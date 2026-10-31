#[derive(Debug, Clone)]
pub(crate) struct AppliedSingleConversationPage {
    pub has_decoded_items: bool,
    pub max_seq: u64,
    pub remote_max_seq: u64,
    pub has_more: bool,
    pub next_cursor: String,
    pub has_seq_gap: bool,
    /// 服务端以 skip/tombstone 证明「此 seq 没有消息」的位点：本地不会有对应消息行，
    /// 存游标时必须当作已物化，否则游标永远越不过去。
    pub absent_seqs: Vec<u64>,
    /// 页内已落库 item 的最小、最大 seq（没有时为 0）：冷启动尾页按它们写区间。
    pub min_item_seq: u64,
    pub max_item_seq: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedConversationIncremental {
    pub has_more: bool,
    pub message_sync_conversation_ids: Vec<String>,
    pub synced_conversation_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ReplayMode {
    SingleConversation,
    CriticalEvents,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedSingleConversationItems {
    pub messages: Vec<crate::model::IMMessage>,
    pub events: Vec<flare_proto::common::Event>,
    /// 与 `events` 一一对齐的 item seq；事件是否计入游标由 apply 结果决定，不在解码期预判。
    pub event_item_seqs: Vec<u64>,
    /// 落库可保证持久的 item seq（消息/skip/tombstone）：save_batch 失败会让整页出错、游标不动。
    pub covered_item_seqs: Vec<u64>,
    /// `covered_item_seqs` 的子集：skip/tombstone —— 服务端证明其上没有消息行。
    pub absent_item_seqs: Vec<u64>,
    pub has_decoded_items: bool,
}
