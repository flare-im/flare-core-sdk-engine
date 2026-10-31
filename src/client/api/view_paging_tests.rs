//! 往旧翻页与 hasOlder：只由已证实连续区间回答。
//!
//! 服务端用脚本模拟：回填请求按真实服务端的语义取 `before` 之前最新的一页可见消息、多取一条
//! 判断 `has_more`，落库后按与同步适配器相同的规则写区间。用例分三组（区间、load_older、
//! 不回跳），最后一组逐条读 sdk-spec 的 `golden/view/timeline-paging.json`。

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use flare_proto::common::{
    Message as ProtoMessage, MessageDeleteEvent, MessageRecallEvent, SingleConversationSyncRes,
    SyncSliceItem, sync_slice_item::Payload as ItemPayload,
};
use serde::Deserialize;
use tokio::sync::RwLock;

use super::{ViewApi, view_refresh_plan};
use crate::application::notification::{NotificationHandlerRegistry, NotificationInboundPipeline};
use crate::application::services::{EventDeduper, MessageDeduper};
use crate::application::usecases::{
    ConversationCommandUseCase, ConversationViewAssembler, MessageViewAssembler, SyncApplyUseCase,
};
use crate::client::api::ConversationApi;
use crate::domain::SeqRange;
use crate::infrastructure::persistence::{
    MemoryUserProfileStore, StoreProvider, in_memory_im_provider,
};
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent, SyncNotify, SyncPhase};
use crate::kernel::{
    AttentionRegistry, CurrentUserIdStore, SessionSyncRunner, SyncReason, SyncRunContext,
    SyncScope, SyncTrigger, SyncVisibility,
};
use crate::model::conversation::ConversationType;
use crate::model::{
    Conversation, ConversationParticipant, IMMessage, LoadOlderTimelineViewRequest,
    OpenTimelineViewRequest,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

const CONVERSATION: &str = "c1";

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

fn proto(seq: u64) -> ProtoMessage {
    ProtoMessage {
        conversation_id: CONVERSATION.into(),
        server_id: format!("s{seq}"),
        client_msg_id: format!("m{seq}"),
        sender_id: "u2".into(),
        conversation_seq: seq,
        ..Default::default()
    }
}

fn row(seq: u64) -> IMMessage {
    IMMessage::new(proto(seq))
}

fn expand(ranges: &[[u64; 2]]) -> Vec<u64> {
    ranges.iter().flat_map(|[from, to]| *from..=*to).collect()
}

fn seqs(from: u64, to: u64) -> Vec<u64> {
    (from..=to).collect()
}

/// 模拟服务端：持有可见消息的 seq 段，按真实回填语义回页。
struct ScriptedServer {
    visible: Vec<(u64, u64)>,
    offline: AtomicBool,
    /// 回页时说还有更早、却一条也不给（坏掉的服务端）。
    empty_pages_with_more: AtomicBool,
    requests: StdMutex<Vec<(u64, usize)>>,
    stores: StoreProvider,
    sync_apply: SyncApplyUseCase,
}

impl ScriptedServer {
    /// `before` 之前最新的 `limit` 条可见消息（由新到旧）与是否还有更早的。
    fn page_before(&self, before: u64, limit: usize) -> (Vec<u64>, bool) {
        let mut ranges = self.visible.clone();
        ranges.sort_by_key(|range| std::cmp::Reverse(range.1));
        let mut seqs = Vec::with_capacity(limit + 1);
        'ranges: for (from, to) in ranges {
            let top = to.min(before.saturating_sub(1));
            if top < from {
                continue;
            }
            for seq in (from..=top).rev() {
                seqs.push(seq);
                if seqs.len() > limit {
                    break 'ranges;
                }
            }
        }
        let has_more = seqs.len() > limit;
        seqs.truncate(limit);
        (seqs, has_more)
    }

    fn visible_in(&self, from: u64, to: u64) -> Vec<u64> {
        (from..=to)
            .filter(|seq| self.visible.iter().any(|(a, b)| a <= seq && seq <= b))
            .collect()
    }

    fn take_requests(&self) -> Vec<(u64, usize)> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

impl SessionSyncRunner for ScriptedServer {
    fn request_message_sync(&self, _conversation_id: &str) -> BoxFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request_message_sync_from_seq(
        &self,
        _conversation_id: &str,
        _last_seq: u64,
        _limit: i32,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request_message_backfill_before_seq(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: i32,
    ) -> BoxFuture<'_, bool> {
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let limit = limit.max(1) as usize;
            self.requests.lock().unwrap().push((before_seq, limit));
            if self.offline.load(Ordering::SeqCst) {
                return Err(FlareError::localized(ErrorCode::NetworkError, "offline"));
            }
            let (seqs, has_more) = if self.empty_pages_with_more.load(Ordering::SeqCst) {
                (Vec::new(), true)
            } else {
                self.page_before(before_seq, limit)
            };
            let rows = seqs.iter().copied().map(row).collect::<Vec<_>>();
            if !rows.is_empty() {
                self.stores.messages.save_batch(&rows).await?;
            }
            let page = SingleConversationSyncRes {
                conversation_id: conversation_id.clone(),
                items: seqs
                    .iter()
                    .map(|seq| SyncSliceItem {
                        conversation_seq: *seq,
                        created_at: 0,
                        payload: Some(ItemPayload::Message(proto(*seq))),
                    })
                    .collect(),
                has_more,
                ..Default::default()
            };
            self.sync_apply
                .record_backfill_coverage(&conversation_id, before_seq, &page)
                .await;
            Ok(has_more)
        })
    }

    fn send_read_ack(&self, _conversation_id: &str, _read_seq: u64) -> BoxFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request_participants_sync(
        &self,
        _conversation_id: &str,
        _limit: i32,
    ) -> BoxFuture<'_, Vec<ConversationParticipant>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct Setup<'a> {
    page_size: u32,
    max_seq: u64,
    visible_after_seq: u64,
    server: &'a [[u64; 2]],
    local: &'a [[u64; 2]],
    coverage: &'a [[u64; 2]],
}

struct Harness {
    view: ViewApi,
    stores: StoreProvider,
    server: Arc<ScriptedServer>,
    view_id: StdMutex<String>,
    page_size: u32,
}

impl Harness {
    async fn new(setup: Setup<'_>) -> Self {
        let stores = in_memory_im_provider();
        stores
            .conversations
            .save_one(&Conversation {
                conversation_id: CONVERSATION.into(),
                channel_id: CONVERSATION.into(),
                conversation_type: ConversationType::Group,
                max_seq: setup.max_seq,
                visible_after_seq: setup.visible_after_seq,
                ..Default::default()
            })
            .await
            .unwrap();
        let rows = expand(setup.local).into_iter().map(row).collect::<Vec<_>>();
        if !rows.is_empty() {
            stores.messages.save_batch(&rows).await.unwrap();
        }
        for [from, to] in setup.coverage {
            stores
                .timeline_coverage
                .add(CONVERSATION, SeqRange::new(*from, *to).unwrap())
                .await
                .unwrap();
        }

        let bus = EventBus::new();
        let user: CurrentUserIdStore = Arc::new(RwLock::new("me".to_string()));
        let profiles = Arc::new(MemoryUserProfileStore::new());
        let conversation_api = ConversationApi::new(
            Arc::new(
                ConversationCommandUseCase::new(
                    stores.conversations.clone(),
                    user.clone(),
                    None,
                    Some(stores.cursors.clone()),
                )
                .with_timeline_coverage(stores.timeline_coverage.clone()),
            ),
            Arc::new(ConversationViewAssembler::new(
                stores.conversations.clone(),
                profiles.clone(),
            )),
            Arc::new(MessageViewAssembler::new(stores.messages.clone(), profiles)),
            stores.timeline_coverage.clone(),
            bus.clone(),
            user,
        );
        let sync_apply = SyncApplyUseCase::new(
            stores.clone(),
            bus.clone(),
            EventDeduper::new(Some(64)),
            NotificationInboundPipeline::new(
                Arc::new(NotificationHandlerRegistry::new()),
                MessageDeduper::new(Some(64)),
                bus.clone(),
            ),
        );
        let server = Arc::new(ScriptedServer {
            visible: setup.server.iter().map(|[from, to]| (*from, *to)).collect(),
            offline: AtomicBool::new(false),
            empty_pages_with_more: AtomicBool::new(false),
            requests: StdMutex::new(Vec::new()),
            stores: stores.clone(),
            sync_apply,
        });
        let view = ViewApi::new(
            conversation_api,
            Some(server.clone() as Arc<dyn SessionSyncRunner>),
            bus,
            AttentionRegistry::new(),
        );
        Self {
            view,
            stores,
            server,
            view_id: StdMutex::new(String::new()),
            page_size: setup.page_size,
        }
    }

    async fn open(&self) {
        let response = self
            .view
            .open_timeline(OpenTimelineViewRequest {
                conversation_id: CONVERSATION.into(),
                message_limit: self.page_size,
            })
            .await
            .unwrap();
        *self.view_id.lock().unwrap() = response.view_id;
    }

    async fn load_older(&self) -> Result<u32> {
        let view_id = self.view_id.lock().unwrap().clone();
        self.view
            .load_older_timeline(LoadOlderTimelineViewRequest {
                view_id,
                message_limit: self.page_size,
            })
            .await
            .map(|response| response.loaded_count)
    }

    /// 视图当前持有的窗口（seq 升序）与 hasOlder。
    async fn window(&self) -> (Vec<u64>, bool) {
        let view_id = self.view_id.lock().unwrap().clone();
        let views = self.view.inner.views.read().await;
        let snapshot = &views
            .timelines
            .get(&view_id)
            .expect("view is open")
            .last_snapshot;
        (
            snapshot
                .messages
                .iter()
                .map(|message| message.conversation_seq)
                .collect(),
            snapshot.has_more,
        )
    }

    async fn coverage(&self) -> Vec<[u64; 2]> {
        self.stores
            .timeline_coverage
            .coverage(CONVERSATION)
            .await
            .unwrap()
            .ranges()
            .iter()
            .map(|range| [range.from, range.to])
            .collect()
    }

    async fn refresh(&self, event: SdkEvent) {
        let plan = view_refresh_plan(&event).expect("the event refreshes timelines");
        self.view.refresh_open_views(plan).await;
    }

    /// 一条实时推送：只落库，不写区间，经热路径进窗口。
    async fn live_message(&self, seq: u64) {
        let message = row(seq);
        self.stores
            .messages
            .save_batch(std::slice::from_ref(&message))
            .await
            .unwrap();
        self.refresh(SdkEvent::Message(MessageEvent::Received {
            message: Box::new(message),
        }))
        .await;
    }

    /// 一次同步证明 `[from, to]`：服务端可见的消息落库、写区间，同步完成后视图重读。
    async fn gap_filled(&self, from: u64, to: u64) {
        let rows = self
            .server
            .visible_in(from, to)
            .into_iter()
            .map(row)
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            self.stores.messages.save_batch(&rows).await.unwrap();
        }
        self.stores
            .timeline_coverage
            .add(CONVERSATION, SeqRange::new(from, to).unwrap())
            .await
            .unwrap();
        self.refresh(sync_finished()).await;
    }

    async fn event(&self, kind: &str) {
        let event = match kind {
            "reaction" => SdkEvent::Message(MessageEvent::ReactionChanged {
                conversation_id: CONVERSATION.into(),
                server_msg_id: "s10".into(),
                user_id: "u2".into(),
                emoji: "+1".into(),
                action: 1,
            }),
            "recall" => SdkEvent::Message(MessageEvent::Recalled {
                conversation_id: CONVERSATION.into(),
                event: MessageRecallEvent::default(),
            }),
            "delete" => SdkEvent::Message(MessageEvent::Deleted {
                conversation_id: CONVERSATION.into(),
                event: MessageDeleteEvent::default(),
            }),
            "syncFinished" => sync_finished(),
            other => panic!("unknown event kind {other}"),
        };
        self.refresh(event).await;
    }
}

fn sync_finished() -> SdkEvent {
    SdkEvent::Sync(SyncNotify::Finished {
        run: SyncRunContext::new(
            SyncTrigger::Manual,
            SyncScope::Global,
            SyncVisibility::Silent,
            SyncReason::UserRequested,
        ),
        phase: SyncPhase::Background,
    })
}

// ---- 区间 ---------------------------------------------------------------------------------

/// 单条实时推送不写区间：在游标推进或缺口补拉证明它之前，它是头部未证实行。
#[tokio::test]
async fn a_realtime_push_writes_no_range() {
    use crate::infrastructure::protocol::DownlinkPayload;

    let stores = in_memory_im_provider();
    let bus = EventBus::new();
    let dispatcher = crate::runtime::Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores.clone()),
        Arc::new(RwLock::new("me".to_string())),
        EventDeduper::new(Some(64)),
        NotificationInboundPipeline::new(
            Arc::new(NotificationHandlerRegistry::new()),
            MessageDeduper::new(Some(64)),
            bus,
        ),
        crate::spi::metrics::MetricsRecorder::disabled(),
    );
    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![proto(1001)],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert!(
        stores.messages.get("s1001").await.unwrap().is_some(),
        "the pushed message is stored"
    );
    assert!(
        stores
            .timeline_coverage
            .coverage(CONVERSATION)
            .await
            .unwrap()
            .is_empty()
    );
}

/// 回填写入的区间与已有区间合并；本地删除不改区间。
#[tokio::test]
async fn backfilled_ranges_merge_and_local_deletes_leave_them() {
    let h = Harness::new(Setup {
        page_size: 10,
        max_seq: 50,
        visible_after_seq: 0,
        server: &[[1, 50]],
        local: &[[41, 50]],
        coverage: &[[41, 50]],
    })
    .await;
    h.open().await;
    h.load_older().await.unwrap();
    assert_eq!(h.coverage().await, vec![[31, 50]]);
    h.stores.messages.delete("s45").await.unwrap();
    h.event("delete").await;
    assert_eq!(h.coverage().await, vec![[31, 50]]);
    let (window, has_older) = h.window().await;
    assert!(!window.contains(&45));
    assert!(has_older);
}

// ---- load_older --------------------------------------------------------------------------

#[tokio::test]
async fn load_older_reads_a_full_page_inside_a_range_without_a_request() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[1, 100]],
        coverage: &[[1, 100]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(61, 100), true));
    assert_eq!(h.load_older().await.unwrap(), 40);
    assert_eq!(h.window().await, (seqs(21, 100), true));
    assert_eq!(h.load_older().await.unwrap(), 20);
    assert_eq!(h.window().await, (seqs(1, 100), false));
    assert_eq!(h.server.take_requests(), vec![]);
}

/// 读到区间边界：以边界为锚回填缺的行数，再在扩展后的区间内读满。
#[tokio::test]
async fn load_older_backfills_at_the_range_boundary() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[50, 100]],
        coverage: &[[50, 100]],
    })
    .await;
    h.open().await;
    assert_eq!(h.load_older().await.unwrap(), 40);
    assert_eq!(h.server.take_requests(), vec![(50, 29)]);
    assert_eq!(h.window().await, (seqs(21, 100), true));
    assert_eq!(h.coverage().await, vec![[21, 100]]);
}

/// 窗口紧挨着洞（本地没有任何区间）：以窗口最旧一行为锚回填一整页。
#[tokio::test]
async fn load_older_backfills_before_a_window_next_to_a_hole() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[61, 100]],
        coverage: &[],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(61, 100), true));
    h.load_older().await.unwrap();
    assert_eq!(h.server.take_requests(), vec![(61, 40)]);
    assert_eq!(h.window().await, (seqs(21, 100), true));
}

/// 回填失败：窗口不变、返回错误，缺口下面的本地行不读。
#[tokio::test]
async fn a_failed_backfill_keeps_the_window_and_does_not_cross_the_gap() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 1000,
        visible_after_seq: 0,
        server: &[[1, 1000]],
        local: &[[1, 36], [980, 1000]],
        coverage: &[[1, 36], [980, 1000]],
    })
    .await;
    h.open().await;
    let before = h.window().await;
    assert_eq!(before, (seqs(980, 1000), true));
    h.server.offline.store(true, Ordering::SeqCst);
    let error = h.load_older().await.expect_err("the backfill fails");
    assert_eq!(error.code(), Some(ErrorCode::NetworkError));
    assert_eq!(h.window().await, before, "the window is unchanged");
    assert_eq!(h.server.take_requests(), vec![(980, 40)]);
}

/// 到达起点：回填说没有更早的，写入 [1, before-1]，hasOlder 变为 false。
#[tokio::test]
async fn reaching_the_start_turns_has_older_off() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 12,
        visible_after_seq: 0,
        server: &[[3, 12]],
        local: &[[9, 12]],
        coverage: &[[9, 12]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(9, 12), true));
    h.load_older().await.unwrap();
    assert_eq!(h.server.take_requests(), vec![(9, 40)]);
    assert_eq!(h.window().await, (seqs(3, 12), false));
    assert_eq!(h.coverage().await, vec![[1, 12]]);
}

/// 会话第一条消息是 seq 3：区间已证明到 1 时，打开即知没有更早的，也不再请求。
#[tokio::test]
async fn the_first_message_at_seq_three_is_the_start_when_proven() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 12,
        visible_after_seq: 0,
        server: &[[3, 12]],
        local: &[[3, 12]],
        coverage: &[[1, 12]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(3, 12), false));
    assert_eq!(h.load_older().await.unwrap(), 0);
    assert_eq!(h.window().await, (seqs(3, 12), false));
    assert_eq!(h.server.take_requests(), vec![]);
}

/// 窗口为空、会话有消息：以会话已知最新 seq 之上一位为锚回填一页。
#[tokio::test]
async fn an_empty_window_backfills_above_the_newest_known_seq() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 50,
        visible_after_seq: 0,
        server: &[[1, 50]],
        local: &[],
        coverage: &[],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (vec![], true));
    h.load_older().await.unwrap();
    assert_eq!(h.server.take_requests(), vec![(51, 40)]);
    assert_eq!(h.window().await, (seqs(11, 50), true));
}

/// 服务端回了空页却说还有更早：不重复请求，留给下一次往旧翻页。
#[tokio::test]
async fn a_backfill_that_proves_nothing_is_not_repeated() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[61, 100]],
        coverage: &[[61, 100]],
    })
    .await;
    h.open().await;
    h.server.empty_pages_with_more.store(true, Ordering::SeqCst);
    assert_eq!(h.load_older().await.unwrap(), 0);
    assert_eq!(h.server.take_requests(), vec![(61, 40)]);
    assert_eq!(h.window().await, (seqs(61, 100), true));
}

// ---- 不回跳 --------------------------------------------------------------------------------

/// 翻到起点后，表情、撤回、删除、同步完成都触发重读：hasOlder 保持 false，窗口不缩回。
/// 窗口有 120 行，超过打开时页大小的上限（100）：按打开时的「最新窗口」整窗重读会把它截短。
#[tokio::test]
async fn has_older_stays_off_after_the_start_through_events() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 122,
        visible_after_seq: 0,
        server: &[[3, 122]],
        local: &[[83, 122]],
        coverage: &[[83, 122]],
    })
    .await;
    h.open().await;
    h.load_older().await.unwrap();
    h.load_older().await.unwrap();
    assert_eq!(h.window().await, (seqs(3, 122), false));
    assert_eq!(
        h.server.take_requests(),
        vec![(83, 40), (43, 40)],
        "two backfills reached the start"
    );

    for kind in [
        "reaction",
        "recall",
        "syncFinished",
        "reaction",
        "syncFinished",
    ] {
        h.event(kind).await;
        assert_eq!(h.window().await, (seqs(3, 122), false), "after {kind}");
    }
    h.stores.messages.delete("s60").await.unwrap();
    h.event("delete").await;
    let (window, has_older) = h.window().await;
    assert_eq!(window.len(), 119);
    assert!(!has_older, "a local delete does not bring load-more back");
    assert_eq!(h.load_older().await.unwrap(), 0);
    assert_eq!(h.server.take_requests(), vec![]);
}

// ---- sdk-spec 向量 -------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vectors {
    schema_version: u32,
    older_paging: OlderPaging,
}

#[derive(Deserialize)]
struct OlderPaging {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    boundary_case: String,
    page_size: u32,
    conversation: CaseConversation,
    server: CaseServer,
    local: CaseLocal,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaseConversation {
    max_seq: u64,
    #[serde(default)]
    visible_after_seq: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaseServer {
    visible_messages: Vec<[u64; 2]>,
}

#[derive(Deserialize)]
struct CaseLocal {
    messages: Vec<[u64; 2]>,
    coverage: Vec<[u64; 2]>,
}

#[derive(Deserialize)]
struct Step {
    #[serde(rename = "do")]
    action: String,
    seq: Option<u64>,
    from: Option<u64>,
    to: Option<u64>,
    kind: Option<String>,
    expect: Option<Expect>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expect {
    window: Option<Vec<[u64; 2]>>,
    has_older: Option<bool>,
    requests: Option<Vec<Request>>,
    coverage: Option<Vec<[u64; 2]>>,
    #[serde(default)]
    error: bool,
}

#[derive(Deserialize)]
struct Request {
    before: u64,
    limit: usize,
}

/// 向量在本仓同级的客户端 SDK 规格里。
fn vectors() -> Vectors {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flare-im-core-client-sdk/sdk-spec/golden/view/timeline-paging.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("decode {}: {error}", path.display()))
}

async fn run_step(h: &Harness, step: &Step, at: &str) -> Result<()> {
    match step.action.as_str() {
        "open" => h.open().await,
        "loadOlder" => return h.load_older().await.map(|_| ()),
        "offline" => h.server.offline.store(true, Ordering::SeqCst),
        "online" => h.server.offline.store(false, Ordering::SeqCst),
        "liveMessage" => {
            h.live_message(step.seq.expect("liveMessage has a seq"))
                .await
        }
        "gapFilled" => {
            h.gap_filled(
                step.from.expect("gapFilled has from"),
                step.to.expect("gapFilled has to"),
            )
            .await
        }
        "event" => {
            h.event(step.kind.as_deref().expect("event has a kind"))
                .await
        }
        other => panic!("{at}: unknown step {other}"),
    }
    Ok(())
}

#[tokio::test]
async fn older_paging_follows_the_shared_vectors() {
    let vectors = vectors();
    assert_eq!(vectors.schema_version, 1);
    let cases = vectors.older_paging.cases;
    assert!(
        cases.len() >= 6,
        "every older-paging boundary case is listed"
    );
    for case in cases {
        assert!(!case.boundary_case.is_empty(), "{}", case.name);
        let h = Harness::new(Setup {
            page_size: case.page_size,
            max_seq: case.conversation.max_seq,
            visible_after_seq: case.conversation.visible_after_seq,
            server: &case.server.visible_messages,
            local: &case.local.messages,
            coverage: &case.local.coverage,
        })
        .await;
        for (index, step) in case.steps.iter().enumerate() {
            let at = format!("{} / step {} ({})", case.name, index + 1, step.action);
            let outcome = run_step(&h, step, &at).await;
            let requests = h.server.take_requests();
            let Some(expect) = &step.expect else {
                assert!(outcome.is_ok(), "{at}: {outcome:?}");
                continue;
            };
            assert_eq!(outcome.is_err(), expect.error, "{at}: {outcome:?}");
            let (window, has_older) = h.window().await;
            if let Some(expected) = &expect.window {
                assert_eq!(window, expand(expected), "{at}: window");
            }
            if let Some(expected) = expect.has_older {
                assert_eq!(has_older, expected, "{at}: hasOlder");
            }
            if let Some(expected) = &expect.requests {
                let expected = expected
                    .iter()
                    .map(|request| (request.before, request.limit))
                    .collect::<Vec<_>>();
                assert_eq!(requests, expected, "{at}: requests");
            }
            if let Some(expected) = &expect.coverage {
                assert_eq!(&h.coverage().await, expected, "{at}: coverage");
            }
        }
    }
}
