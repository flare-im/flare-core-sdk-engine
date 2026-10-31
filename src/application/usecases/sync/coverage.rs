//! 同步页落库后写入它们证明的已证实区间（规则见 `domain::timeline`）。

use flare_proto::common::{SingleConversationSyncRes, sync_slice_item::Payload as ItemPayload};

use crate::domain::timeline::backfill_page_range;
use crate::domain::{SeqRange, TimelineCoverageStore};

pub(crate) async fn record_range(
    store: &dyn TimelineCoverageStore,
    conversation_id: &str,
    range: Option<SeqRange>,
) {
    let Some(range) = range else {
        return;
    };
    if let Err(error) = store.add(conversation_id, range).await {
        tracing::warn!(
            conversation_id = %conversation_id,
            from_seq = range.from,
            to_seq = range.to,
            error = %error,
            "写入已证实区间失败（下次往旧翻页会多回填一次）"
        );
    }
}

/// 回填页的每个 item 的 seq（消息行优先取 item 上的 seq）。
fn backfill_item_seqs(page: &SingleConversationSyncRes) -> impl Iterator<Item = u64> + '_ {
    page.items.iter().map(|item| {
        if item.conversation_seq > 0 {
            return item.conversation_seq;
        }
        match &item.payload {
            Some(ItemPayload::Message(message)) => message.conversation_seq,
            Some(ItemPayload::Event(event)) => event.conversation_seq,
            _ => 0,
        }
    })
}

/// 回填页落库之后调用：`has_more` 为真证明 `[页内最小 seq, before - 1]`，为假证明 `[1, before - 1]`。
pub(crate) async fn record_backfill_page(
    store: &dyn TimelineCoverageStore,
    conversation_id: &str,
    before_seq: u64,
    page: &SingleConversationSyncRes,
) {
    record_range(
        store,
        conversation_id,
        backfill_page_range(before_seq, backfill_item_seqs(page), page.has_more),
    )
    .await;
}
