use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{Conversation, IMMessage};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TimelineSyncState {
    #[default]
    LocalReady,
    Synced,
    Partial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapHomeTimelineRequest {
    #[serde(default = "default_conversation_limit")]
    pub conversation_limit: u32,
}

impl Default for BootstrapHomeTimelineRequest {
    fn default() -> Self {
        Self {
            conversation_limit: default_conversation_limit(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpenConversationTimelineRequest {
    pub conversation_id: String,
    #[serde(default = "default_message_limit")]
    pub message_limit: u32,
}

impl Default for OpenConversationTimelineRequest {
    fn default() -> Self {
        Self {
            conversation_id: String::new(),
            message_limit: default_message_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HomeTimelineSnapshot {
    pub conversations: Vec<Conversation>,
    /// Unread total across all conversations, not only the returned ones
    /// (the rule of `conversation.total_unread`).
    pub total_unread: u64,
    pub sync_state: TimelineSyncState,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConversationTimelineSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Conversation>,
    pub messages: Vec<IMMessage>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpenTimelineViewRequest {
    pub conversation_id: String,
    #[serde(default = "default_message_limit")]
    pub message_limit: u32,
    /// 围绕某条消息打开（搜索结果、引用、置顶跳转）；不带时打开最新窗口。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<TimelineAnchor>,
}

/// 定位目标：`message_id` 是服务端 id 或客户端 id，本地找得到就用本地的 seq；找不到时用
/// 调用方给的 `seq`（搜索结果、引用、置顶都带 seq）。两者都没有时打开失败
/// （`sdk.view.anchor_not_found`）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimelineAnchor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoadOlderTimelineViewRequest {
    pub view_id: String,
    #[serde(default = "default_message_limit")]
    pub message_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoadNewerTimelineViewRequest {
    pub view_id: String,
    #[serde(default = "default_message_limit")]
    pub message_limit: u32,
}

/// 只带视图 id 的时间线请求：`view.timeline.jump_to_latest` 与 `view.timeline.snapshot`。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimelineViewRequest {
    pub view_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpenConversationListViewRequest {
    #[serde(default = "default_conversation_limit")]
    pub conversation_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CloseViewRequest {
    pub view_id: String,
}

/// 时间线视图的窗口：按时间线顺序排列的一段消息行，以及两个方向还有没有、是否接在最新。
///
/// - `has_older`：窗口最旧一行之下还有可见消息，只由已证实连续区间回答；
/// - `has_newer`：窗口之上还有消息，当且仅当没有接在最新；
/// - `attached_to_latest`：窗口最新一行就是本地最新的已发送行，实时消息会追加进来；
/// - `anchor_key`：围绕某条消息打开时目标行的行键（[`IMMessage::row_key`]），宿主据此滚到并高亮。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimelineViewSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Conversation>,
    pub messages: Vec<IMMessage>,
    pub has_older: bool,
    pub has_newer: bool,
    pub attached_to_latest: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "viewType", content = "data", rename_all = "camelCase")]
#[allow(clippy::large_enum_variant)]
pub enum ViewSnapshot {
    Timeline(TimelineViewSnapshot),
    ConversationList(HomeTimelineSnapshot),
}

/// 打开视图与 `view.timeline.snapshot` 的返回：视图 id、当前版本号与它对应的快照。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ViewOpenResponse {
    pub view_id: String,
    pub revision: u64,
    pub snapshot: ViewSnapshot,
}

/// 往旧、往新翻页与回到最新的返回。窗口的变化只经 `view.updated` 事件推出；这里只说
/// 「包含这一页的是版本几」与这一页并入了几行。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TimelineViewPageResponse {
    pub view_id: String,
    pub loaded_count: u32,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ViewUpdateKind {
    Snapshot,
    Delta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ViewDeltaKind {
    Insert,
    Update,
    Remove,
    Move,
}

/// 会话列表视图增量里的一步（按下标）。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ViewDeltaOp {
    pub op: ViewDeltaKind,
    pub key: String,
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<Value>,
}

/// 视图增量。
///
/// 时间线按行键（[`IMMessage::row_key`]）表达，不带下标，可以幂等重放。宿主按顺序应用：
/// 删 `remove` 里的键；原位替换 `upsert`；`prepend` 放到最前、`append` 放到最后；`reorder`
/// 非空时按它给出的全部键重排；最后更新三个状态字段与 `conversation`（只在变化时带）。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "viewType",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
// 变体大小差异是 serde DTO 的固有形态：Timeline 变体内嵌完整 Conversation。
// 加 Box 虽能收敛栈占用，但会改变各端绑定（wasm/ffi）构造与匹配这个公开类型的
// 写法，代价远大于收益 —— 它是按值传递一次的增量载荷，不在热路径反复拷贝。
#[allow(clippy::large_enum_variant)]
pub enum ViewDelta {
    Timeline {
        /// 更旧的行，按时间线顺序。
        prepend: Vec<IMMessage>,
        /// 更新的行，按时间线顺序。
        append: Vec<IMMessage>,
        /// 已在窗口里的行的新内容，原位替换。
        upsert: Vec<IMMessage>,
        /// 移出窗口的行键。
        remove: Vec<String>,
        /// 窗口的全部行键（新顺序）；只在顺序变化无法用前四步表达时给。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reorder: Option<Vec<String>>,
        has_older: bool,
        has_newer: bool,
        attached_to_latest: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conversation: Option<Conversation>,
    },
    ConversationList {
        ops: Vec<ViewDeltaOp>,
        total_unread: u64,
        sync_state: TimelineSyncState,
    },
}

/// `view.updated` 的载荷。`revision` 按视图严格递增；宿主发现它不等于已应用版本加 1 时，
/// 取 `view.timeline.snapshot` 整体替换（比已应用版本旧的直接丢弃）。
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ViewUpdate {
    pub view_id: String,
    pub revision: u64,
    pub kind: ViewUpdateKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<ViewSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<ViewDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CloseViewResponse {
    pub closed: bool,
}

fn default_conversation_limit() -> u32 {
    100
}

fn default_message_limit() -> u32 {
    40
}

pub fn normalized_conversation_limit(limit: u32) -> u32 {
    limit.clamp(1, 500)
}

pub fn normalized_message_limit(limit: u32) -> u32 {
    limit.clamp(1, 100)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn timeline_delta_carries_rows_by_key_and_omits_absent_fields() {
        let update = ViewUpdate {
            view_id: "timeline-1".to_string(),
            revision: 3,
            kind: ViewUpdateKind::Delta,
            snapshot: None,
            delta: Some(ViewDelta::Timeline {
                prepend: Vec::new(),
                append: Vec::new(),
                upsert: Vec::new(),
                remove: vec!["cm-1".to_string()],
                reorder: None,
                has_older: true,
                has_newer: false,
                attached_to_latest: true,
                conversation: None,
            }),
        };

        let value = serde_json::to_value(update).expect("view update serializes");

        assert_eq!(value["viewId"], json!("timeline-1"));
        assert_eq!(value["revision"], json!(3));
        assert_eq!(value["kind"], json!("delta"));
        assert!(value.get("snapshot").is_none());

        let delta = value
            .get("delta")
            .and_then(Value::as_object)
            .expect("delta object");
        assert_eq!(delta.get("viewType"), Some(&json!("timeline")));
        assert_eq!(delta.get("remove"), Some(&json!(["cm-1"])));
        assert_eq!(delta.get("prepend"), Some(&json!([])));
        assert_eq!(delta.get("hasOlder"), Some(&json!(true)));
        assert_eq!(delta.get("hasNewer"), Some(&json!(false)));
        assert_eq!(delta.get("attachedToLatest"), Some(&json!(true)));
        assert!(delta.get("conversation").is_none());
        assert!(delta.get("reorder").is_none());
        assert!(delta.get("ops").is_none());
    }

    #[test]
    fn open_timeline_view_request_reads_an_optional_anchor() {
        let plain: OpenTimelineViewRequest =
            serde_json::from_value(json!({ "conversationId": "c1" })).expect("decodes");
        assert_eq!(plain.message_limit, 40);
        assert!(plain.anchor.is_none());

        let anchored: OpenTimelineViewRequest = serde_json::from_value(json!({
            "conversationId": "c1",
            "messageLimit": 40,
            "anchor": { "messageId": "srv-123", "seq": 81234 }
        }))
        .expect("decodes");
        assert_eq!(
            anchored.anchor,
            Some(TimelineAnchor {
                message_id: Some("srv-123".to_string()),
                seq: Some(81234),
            })
        );
    }

    #[test]
    fn timeline_view_snapshot_carries_the_window_state() {
        let snapshot = ViewSnapshot::Timeline(TimelineViewSnapshot {
            conversation: None,
            messages: Vec::new(),
            has_older: false,
            has_newer: true,
            attached_to_latest: false,
            anchor_key: Some("cm-7".to_string()),
        });
        let value = serde_json::to_value(snapshot).expect("snapshot serializes");
        assert_eq!(value["viewType"], json!("timeline"));
        assert_eq!(
            value["data"],
            json!({
                "messages": [],
                "hasOlder": false,
                "hasNewer": true,
                "attachedToLatest": false,
                "anchorKey": "cm-7"
            })
        );
    }

    #[test]
    fn timeline_snapshot_omits_absent_conversation() {
        let snapshot = ConversationTimelineSnapshot {
            conversation: None,
            messages: Vec::new(),
            has_more: false,
        };

        let value = serde_json::to_value(snapshot).expect("timeline snapshot serializes");

        assert!(value.get("conversation").is_none());
        assert_eq!(value["messages"], json!([]));
        assert_eq!(value["hasMore"], json!(false));
    }
}
