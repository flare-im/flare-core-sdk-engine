//! SDK 查询对象：客户端只传筛选条件，业务筛选语义集中在 core-sdk。

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::model::conversation::ConversationType;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MessageSearchKind {
    /// 消息记录页签：不额外限制消息类型。
    Message,
    /// 文本类消息：Text / RichText / Quote。
    Text,
    /// 媒体集合：Image / Video / Audio / File / ImageGroup。
    Media,
    Image,
    Video,
    Audio,
    /// 文件附件：支持按 typed 文件名、描述、MIME 与文件 ID 搜索。
    File,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[serde(rename_all = "camelCase")]
pub struct MessageSearchQuery {
    pub keyword: Option<String>,
    pub conversation_id: Option<String>,
    pub sender_id: Option<String>,
    /// 起始消息时间（毫秒，含）。
    pub from_time: Option<u64>,
    /// 截止消息时间（毫秒，含）。
    pub to_time: Option<u64>,
    pub kinds: Vec<MessageSearchKind>,
    pub limit: u32,
    /// 默认排除已撤回消息。
    pub include_recalled: bool,
}

impl Default for MessageSearchQuery {
    fn default() -> Self {
        Self {
            keyword: None,
            conversation_id: None,
            sender_id: None,
            from_time: None,
            to_time: None,
            kinds: Vec::new(),
            limit: 50,
            include_recalled: false,
        }
    }
}

impl MessageSearchQuery {
    pub fn text(keyword: &str, limit: u32) -> Self {
        Self {
            keyword: Some(keyword.to_string()),
            limit,
            ..Self::default()
        }
    }

    pub fn in_conversation(conversation_id: &str, keyword: &str, limit: u32) -> Self {
        Self {
            keyword: Some(keyword.to_string()),
            conversation_id: Some(conversation_id.to_string()),
            limit,
            ..Self::default()
        }
    }

    pub fn normalized_limit(&self) -> u32 {
        self.limit.clamp(1, 200)
    }

    pub fn normalized_keyword(&self) -> Option<String> {
        self.keyword
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[serde(rename_all = "camelCase")]
pub struct ConversationListQuery {
    pub keyword: Option<String>,
    pub include_archived: bool,
    pub unread_only: bool,
    pub mention_me_only: bool,
    pub pinned_only: bool,
    pub muted_only: Option<bool>,
    pub has_draft_only: bool,
    /// 标记消息所在会话。core 当前没有“会话标签”模型，因此只支持消息标记聚合。
    pub has_marked_messages: bool,
    pub conversation_types: Vec<ConversationType>,
    /// cursor 为会话 ID，表示从该会话之后开始。
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl ConversationListQuery {
    pub fn normalized_keyword(&self) -> Option<String> {
        self.keyword
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase())
    }

    pub fn normalized_limit(&self) -> Option<u32> {
        self.limit.map(|limit| limit.clamp(1, 500))
    }
}
