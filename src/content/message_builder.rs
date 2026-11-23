//! 消息构建器 — 构建发送用 Message

use crate::content::BuiltContent;
use crate::model::message::{ConversationType, Message};
use crate::shared::error::Result;
use crate::shared::util::id::now_millis;
use flare_proto::common::{MessageSource, OfflinePushInfo};

/// 消息构建器
#[derive(Clone, Debug)]
pub struct MessageBuilder {
    conversation_id: String,
    sender_id: String,
    sender_name: String,
    sender_avatar: String,
    content: Option<BuiltContent>,
    conversation_type: i32,
    channel_id: String,
    thread_id: Option<String>,
    offline_push_info: Option<OfflinePushInfo>,
    extra: std::collections::HashMap<String, String>,
}

impl MessageBuilder {
    pub fn new(conversation_id: impl Into<String>, sender_id: impl Into<String>) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            sender_id: sender_id.into(),
            sender_name: String::new(),
            sender_avatar: String::new(),
            content: None,
            conversation_type: ConversationType::Unspecified as i32,
            channel_id: String::new(),
            thread_id: None,
            offline_push_info: None,
            extra: std::collections::HashMap::new(),
        }
    }

    /// 设置发送者昵称（展示用）
    pub fn sender_name(mut self, name: impl Into<String>) -> Self {
        self.sender_name = name.into();
        self
    }

    /// 设置发送者头像 URL（展示用）
    pub fn sender_avatar(mut self, url: impl Into<String>) -> Self {
        self.sender_avatar = url.into();
        self
    }

    pub fn content(mut self, content: BuiltContent) -> Self {
        self.content = Some(content);
        self
    }

    pub fn single_chat(mut self) -> Self {
        self.conversation_type = ConversationType::Single as i32;
        self
    }

    pub fn group_chat(mut self) -> Self {
        self.conversation_type = ConversationType::Group as i32;
        self
    }

    pub fn channel(mut self, channel_id: impl Into<String>) -> Self {
        self.channel_id = channel_id.into();
        self
    }

    pub fn thread(mut self, thread_id: impl Into<String>) -> Self {
        let thread_id = thread_id.into();
        self.thread_id = (!thread_id.trim().is_empty()).then_some(thread_id);
        self
    }

    pub fn offline_push(mut self, title: impl Into<String>, body: impl Into<String>) -> Self {
        self.offline_push_info = Some(OfflinePushInfo {
            title: title.into(),
            body: body.into(),
            ..Default::default()
        });
        self
    }

    pub fn extra(mut self, key: &str, value: impl Into<String>) -> Self {
        self.extra.insert(key.to_string(), value.into());
        self
    }

    pub fn attributes(self, key: &str, value: impl Into<String>) -> Self {
        self.extra(key, value)
    }

    /// 便捷：构建纯文本消息
    pub fn text(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl AsRef<str>,
    ) -> Result<Message> {
        let content = super::ContentBuilder::text(text.as_ref()).build();
        Self::new(conversation_id, sender_id)
            .content(content)
            .build()
    }

    pub fn build(self) -> Result<Message> {
        let content = self.content.ok_or_else(|| {
            crate::shared::error::FlareError::localized(
                crate::shared::error::ErrorCode::InvalidParameter,
                "message content required",
            )
        })?;
        let message_type = content.message_type as i32;
        // 未下行前 `timestamp` 为空会导致 `IMMessage::timestamp/client_timestamp` 均为 0，
        // 前端按时间排序时会把待发消息误排到更早的对方消息之前。
        let created_at = now_millis() as i64;
        Ok(Message {
            last_edit_seq: 0,
            reaction_states: Vec::new(),
            server_id: String::new(),
            conversation_id: self.conversation_id,
            client_msg_id: crate::shared::util::id::generate_client_msg_id(),
            sender_id: self.sender_id,
            sender_name: self.sender_name,
            sender_avatar: self.sender_avatar,
            source: MessageSource::User as i32,
            conversation_seq: 0,
            created_at,
            conversation_type: self.conversation_type,
            message_type,
            message_seq: None,
            channel_id: self.channel_id,
            content: Some(content.inner),
            thread_id: self.thread_id,
            status: 0,
            retention_policy: None,
            retention_state: None,
            offline_push_info: self.offline_push_info,
            attributes: self.extra,
            extensions: std::collections::HashMap::new(),
        })
    }
}
