//! 同步页落库后写入的已证实区间：连续同步页、游标推进、冷启动尾页、快照行、回填页、
//! 本地清空记录，以及不推进游标的向新页。

use std::sync::Arc;

use flare_proto::common::{
    Message as ProtoMessage, SingleConversationSyncRes, SnapshotConversationRow, SyncRes,
    SyncSliceItem, SyncTombstoneItem, sync_res::Payload as SyncResPayload,
    sync_slice_item::Payload as ItemPayload,
};
use tokio::sync::Mutex;

use super::SyncProtocolAdapter;
use crate::application::notification::{NotificationHandlerRegistry, NotificationInboundPipeline};
use crate::application::services::{EventDeduper, MessageDeduper};
use crate::application::{ConversationLocalLifecycle, LocalConversationVisibility};
use crate::domain::{
    ConversationWriter, MessageWriter, SyncCursorReader, SyncCursorVo, SyncCursorWriter,
};
use crate::infrastructure::persistence::{StoreProvider, in_memory_im_provider};
use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
use crate::kernel::SyncRunContext;
use crate::kernel::event::EventBus;
use crate::model::{Conversation, IMMessage};
use crate::spi::metrics::MetricsRecorder;

const USER: &str = "u1";
const CONVERSATION: &str = "c1";

async fn new_adapter() -> (SyncProtocolAdapter, StoreProvider) {
    let stores = in_memory_im_provider();
    let bus = EventBus::new();
    let pipeline = NotificationInboundPipeline::new(
        Arc::new(NotificationHandlerRegistry::new()),
        MessageDeduper::new(Some(64)),
        bus.clone(),
    );
    let sender = Arc::new(PacketSender::new(
        Arc::new(Mutex::new(None)),
        Arc::new(ProtobufCodec) as Arc<dyn Codec>,
    ));
    let adapter = SyncProtocolAdapter::new(
        sender,
        stores.clone(),
        bus,
        EventDeduper::new(Some(64)),
        pipeline,
        "d1".to_string(),
        MetricsRecorder::disabled(),
    );
    *adapter.active_user_id.lock().await = USER.to_string();
    (adapter, stores)
}

fn message(seq: u64) -> ProtoMessage {
    ProtoMessage {
        conversation_id: CONVERSATION.into(),
        server_id: format!("s{seq}"),
        client_msg_id: format!("m{seq}"),
        sender_id: "u2".into(),
        conversation_seq: seq,
        ..Default::default()
    }
}

fn message_item(seq: u64) -> SyncSliceItem {
    SyncSliceItem {
        conversation_seq: seq,
        created_at: 0,
        payload: Some(ItemPayload::Message(message(seq))),
    }
}

fn tombstone_item(seq: u64) -> SyncSliceItem {
    SyncSliceItem {
        conversation_seq: seq,
        created_at: 0,
        payload: Some(ItemPayload::Tombstone(SyncTombstoneItem {
            tombstone_id: format!("{CONVERSATION}:{seq}"),
            reason: None,
        })),
    }
}

fn page(items: Vec<SyncSliceItem>, max_seq: u64, has_more: bool) -> SingleConversationSyncRes {
    SingleConversationSyncRes {
        conversation_id: CONVERSATION.into(),
        items,
        max_conversation_seq: max_seq,
        next_cursor: String::new(),
        has_more,
        hints: None,
        stale: None,
    }
}

fn response(page: SingleConversationSyncRes) -> SyncRes {
    SyncRes {
        payload: Some(SyncResPayload::SingleConversation(page)),
    }
}

async fn ranges(stores: &StoreProvider) -> Vec<(u64, u64)> {
    stores
        .timeline_coverage
        .coverage(CONVERSATION)
        .await
        .unwrap()
        .ranges()
        .iter()
        .map(|range| (range.from, range.to))
        .collect()
}

async fn cursor(stores: &StoreProvider) -> u64 {
    stores
        .cursors
        .get_conversation_cursor(USER, CONVERSATION)
        .await
        .unwrap()
        .map(|cursor| cursor.last_seq)
        .unwrap_or(0)
}

async fn set_cursor(stores: &StoreProvider, last_seq: u64) {
    stores
        .cursors
        .save_conversation_cursor(&SyncCursorVo {
            user_id: USER.into(),
            conversation_id: CONVERSATION.into(),
            last_seq,
            synced_at: 1,
        })
        .await
        .unwrap();
}

async fn local_rows(stores: &StoreProvider, seqs: impl IntoIterator<Item = u64>) {
    let rows = seqs
        .into_iter()
        .map(|seq| IMMessage::new(message(seq)))
        .collect::<Vec<_>>();
    stores.messages.save_batch(&rows).await.unwrap();
}

/// 连续同步页（after = 0，服务端补了 tombstone）：证明 [1, M]，游标推进同样证明这一段。
#[tokio::test]
async fn a_contiguous_page_from_zero_proves_from_one() {
    let (adapter, stores) = new_adapter().await;
    let items = (1..=5)
        .map(message_item)
        .chain([tombstone_item(6)])
        .collect();
    adapter
        .apply_sync_res_single(
            &SyncRunContext::silent_gap_repair(),
            CONVERSATION,
            &response(page(items, 6, false)),
            0,
            "",
        )
        .await
        .unwrap();
    assert_eq!(ranges(&stores).await, vec![(1, 6)]);
    assert_eq!(cursor(&stores).await, 6);
}

/// 连续同步页（after = A）：只证明从 A+1 起连续到的那一段，缺口之后的不算。
#[tokio::test]
async fn a_page_proves_only_its_contiguous_part() {
    let (adapter, stores) = new_adapter().await;
    local_rows(&stores, 1..=10).await;
    set_cursor(&stores, 10).await;
    let items = [11, 12, 14].into_iter().map(message_item).collect();
    adapter
        .apply_sync_res_single(
            &SyncRunContext::silent_gap_repair(),
            CONVERSATION,
            &response(page(items, 14, false)),
            10,
            "seq:10",
        )
        .await
        .unwrap();
    assert_eq!(ranges(&stores).await, vec![(11, 12)]);
    assert_eq!(cursor(&stores).await, 12);
}

/// 游标从 c 推进到 c'（本地行由实时推送写入）：证明 [c+1, c']。
#[tokio::test]
async fn a_cursor_advance_proves_the_seqs_it_passes() {
    let (adapter, stores) = new_adapter().await;
    set_cursor(&stores, 10).await;
    local_rows(&stores, 11..=15).await;
    adapter
        .save_cursor_with_remote(USER, CONVERSATION, 15, &[])
        .await
        .unwrap();
    assert_eq!(cursor(&stores).await, 15);
    assert_eq!(ranges(&stores).await, vec![(11, 15)]);
}

/// 冷启动尾页（after = 0、无游标、服务端只回最新一段与水位）：证明 [页内最小 seq, 水位]，不从 1 起。
#[tokio::test]
async fn a_cold_start_tail_proves_from_its_oldest_row_not_from_one() {
    let (adapter, stores) = new_adapter().await;
    let items = (980..=1000).map(message_item).collect();
    adapter
        .apply_sync_res_single(
            &SyncRunContext::silent_gap_repair(),
            CONVERSATION,
            &response(page(items, 1003, false)),
            0,
            "",
        )
        .await
        .unwrap();
    assert_eq!(ranges(&stores).await, vec![(980, 1003)]);
    assert_eq!(
        cursor(&stores).await,
        1003,
        "the tail still takes the watermark cursor"
    );
}

/// 快照行（冷启 bundle、gap-too-large 重建）同属尾页：证明行内最小到最大 seq，不延到水位。
#[tokio::test]
async fn a_snapshot_row_proves_between_its_oldest_and_newest_message() {
    let (adapter, stores) = new_adapter().await;
    adapter
        .sync_apply_use_case
        .apply_snapshot_row(
            USER,
            SnapshotConversationRow {
                conversation_id: CONVERSATION.into(),
                messages: (970..=1000).map(message).collect(),
                last_conversation_seq: 1005,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(ranges(&stores).await, vec![(970, 1000)]);
}

/// 回填页：还有更早时证明 [页内最小 seq, B-1]，到起点（含空页）时证明 [1, B-1]。
#[tokio::test]
async fn a_backfill_page_proves_down_to_its_oldest_row_or_to_one() {
    let (adapter, stores) = new_adapter().await;
    let use_case = &adapter.sync_apply_use_case;
    use_case
        .record_backfill_coverage(
            CONVERSATION,
            980,
            &page((940..=979).map(message_item).collect(), 0, true),
        )
        .await;
    assert_eq!(ranges(&stores).await, vec![(940, 979)]);

    use_case
        .record_backfill_coverage(CONVERSATION, 900, &page(Vec::new(), 0, true))
        .await;
    assert_eq!(
        ranges(&stores).await,
        vec![(940, 979)],
        "an empty page with more proves nothing"
    );

    use_case
        .record_backfill_coverage(
            CONVERSATION,
            940,
            &page((3..=939).map(message_item).collect(), 0, false),
        )
        .await;
    assert_eq!(
        ranges(&stores).await,
        vec![(1, 979)],
        "no more history reaches seq 1"
    );

    use_case
        .record_backfill_coverage("c2", 5000, &page(Vec::new(), 0, false))
        .await;
    assert_eq!(
        stores
            .timeline_coverage
            .coverage("c2")
            .await
            .unwrap()
            .ranges()
            .iter()
            .map(|range| (range.from, range.to))
            .collect::<Vec<_>>(),
        vec![(1, 4999)],
        "an empty last page proves everything below the anchor"
    );
}

/// 本地清空记录（可见下限 F）：写入 [1, F]。
#[tokio::test]
async fn clearing_local_history_proves_up_to_the_cleared_seq() {
    let (_adapter, stores) = new_adapter().await;
    stores
        .conversations
        .save_one(&Conversation {
            conversation_id: CONVERSATION.into(),
            channel_id: CONVERSATION.into(),
            max_seq: 30,
            ..Default::default()
        })
        .await
        .unwrap();
    ConversationLocalLifecycle::clear_history_boundary(
        &stores,
        USER,
        CONVERSATION,
        LocalConversationVisibility::Keep,
    )
    .await
    .unwrap()
    .expect("conversation exists");
    assert_eq!(ranges(&stores).await, vec![(1, 30)]);
}

/// 向新同步页（不推进游标）：消息与区间照写，游标留在原处；同一页正常应用会推进游标。
#[tokio::test]
async fn a_detached_page_writes_ranges_but_leaves_the_cursor() {
    let items = || (101..=110).map(message_item).collect::<Vec<_>>();

    let (adapter, stores) = new_adapter().await;
    set_cursor(&stores, 100).await;
    let (max_seq, has_more, _) = adapter
        .apply_sync_res_single(
            &SyncRunContext::detached_page(),
            CONVERSATION,
            &response(page(items(), 500, true)),
            100,
            "",
        )
        .await
        .unwrap();
    assert_eq!((max_seq, has_more), (110, true));
    assert_eq!(ranges(&stores).await, vec![(101, 110)]);
    assert_eq!(
        cursor(&stores).await,
        100,
        "the cursor is not moved by a detached page"
    );
    assert!(
        stores.messages.get("s105").await.unwrap().is_some(),
        "the messages are stored"
    );

    let (adapter, stores) = new_adapter().await;
    set_cursor(&stores, 100).await;
    adapter
        .apply_sync_res_single(
            &SyncRunContext::silent_gap_repair(),
            CONVERSATION,
            &response(page(items(), 500, true)),
            100,
            "",
        )
        .await
        .unwrap();
    assert_eq!(
        cursor(&stores).await,
        110,
        "the same page applied normally moves it"
    );
}

/// 不推进游标的页远在游标之下：从请求位点起算连续，不受游标影响。
#[tokio::test]
async fn a_detached_page_below_the_cursor_is_read_from_its_own_anchor() {
    let (adapter, stores) = new_adapter().await;
    set_cursor(&stores, 5000).await;
    adapter
        .apply_sync_res_single(
            &SyncRunContext::detached_page(),
            CONVERSATION,
            &response(page((201..=240).map(message_item).collect(), 5000, true)),
            200,
            "",
        )
        .await
        .unwrap();
    assert_eq!(ranges(&stores).await, vec![(201, 240)]);
    assert_eq!(cursor(&stores).await, 5000);
}

/// 不推进游标的页落在尾页语义上（after = 0）：尾页区间照写，游标不推到水位。
#[tokio::test]
async fn a_detached_tail_does_not_take_the_watermark_cursor() {
    let (adapter, stores) = new_adapter().await;
    adapter
        .apply_sync_res_single(
            &SyncRunContext::detached_page(),
            CONVERSATION,
            &response(page((980..=1000).map(message_item).collect(), 1003, false)),
            0,
            "",
        )
        .await
        .unwrap();
    assert_eq!(ranges(&stores).await, vec![(980, 1003)]);
    assert_eq!(cursor(&stores).await, 0);
}
