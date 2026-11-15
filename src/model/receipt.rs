//! 消息回执投影 `MessageReceiptView`：核心 SDK 给每条消息算好的回执状态与群聊已读摘要。
//! 宿主与 Kit 只画它，不再从 `status` / `isRead` / `localState` 自己映射。
//!
//! - `status`：pending | sending | sent | read | failed 五态，没有 delivered（没有真实的投递数据，
//!   且与已读只靠颜色区分）。单聊：对方已读（`isRead`，由对端已读回执单调推进）为 read。群聊：
//!   只有已读摘要说全部已读才是 read；没有摘要（超大群、尚未取到）一律 sent，不猜。
//! - `readSummary`：只在群聊里本人发出的消息上，数据来自 Core 的已读摘要查询（见
//!   `application::services::read_summaries`）；单聊、超大群、尚未取到时没有。
//!
//! 摘要只在本机的视图消息上，以 [`READ_SUMMARY_JSON_KEY`] 这个本地属性携带：不入库、不上 wire
//! （收发两个方向都剥离），序列化给宿主时变成 `receipt.readSummary` 并从 `attributes` 里拿掉。
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::message::{ConversationType, IMMessage, MessageStatus};

/// 视图消息上携带已读摘要的本地属性键（JSON：[`MessageReadSummary`]）。
pub(crate) const READ_SUMMARY_JSON_KEY: &str = "readSummaryJson";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MessageReceiptStatus {
    /// 本地已构建、尚未进入发送。
    Pending,
    /// 发送中（含媒体上传中），尚无服务端序号。
    Sending,
    /// 服务端已接受。
    Sent,
    /// 单聊对方已读；群聊全部已读。
    Read,
    /// 发送失败，可重试。
    Failed,
}

/// 群聊里本人一条消息的已读情况（不含发送者本人）。`total = readCount + unreadCount`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageReadSummary {
    pub read_count: u32,
    pub unread_count: u32,
    pub total: u32,
}

impl MessageReadSummary {
    /// 可展示的摘要：总数大于 0 且计数自洽（Core 约定 total 为 0 时不显示）。
    pub fn is_displayable(&self) -> bool {
        self.total > 0 && self.read_count.checked_add(self.unread_count) == Some(self.total)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageReceiptView {
    pub status: MessageReceiptStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_summary: Option<MessageReadSummary>,
}

impl IMMessage {
    fn is_group_message(&self) -> bool {
        self.conversation_type == ConversationType::Group as i32
    }

    /// 视图消息携带的群已读摘要；不是群消息、没有或不可展示时为 `None`。
    pub fn read_summary(&self) -> Option<MessageReadSummary> {
        if !self.is_group_message() {
            return None;
        }
        let raw = self.attributes.get(READ_SUMMARY_JSON_KEY)?;
        crate::shared::util::json::from_str::<MessageReadSummary>(raw)
            .ok()
            .filter(MessageReadSummary::is_displayable)
    }

    /// 写入（或清除）视图消息上的已读摘要；只用于交给宿主的视图副本。
    pub(crate) fn set_read_summary(&mut self, summary: Option<MessageReadSummary>) {
        match summary.filter(MessageReadSummary::is_displayable) {
            Some(summary) => {
                if let Ok(raw) = serde_json::to_string(&summary) {
                    self.attributes
                        .insert(READ_SUMMARY_JSON_KEY.to_owned(), raw);
                }
            }
            None => {
                self.attributes.remove(READ_SUMMARY_JSON_KEY);
            }
        }
    }

    /// 这条消息的回执投影。发送者是不是本人由宿主判断（Kit 对他人消息不画回执）。
    pub fn receipt_view(&self) -> MessageReceiptView {
        let read_summary = self.read_summary();
        let status = if self.local_state.failed || self.status == MessageStatus::Failed as i32 {
            MessageReceiptStatus::Failed
        } else if self.conversation_seq == 0 && self.status <= MessageStatus::Created as i32 {
            if self.status == MessageStatus::Created as i32
                || self.local_state.sending
                || self.local_state.uploading
            {
                MessageReceiptStatus::Sending
            } else {
                MessageReceiptStatus::Pending
            }
        } else {
            let read = if self.is_group_message() {
                read_summary.is_some_and(|summary| summary.unread_count == 0)
            } else {
                self.is_read
            };
            if read {
                MessageReceiptStatus::Read
            } else {
                MessageReceiptStatus::Sent
            }
        };
        MessageReceiptView {
            status,
            read_summary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::message::MessageLocalState;

    fn message(conversation_type: ConversationType, status: MessageStatus, seq: u64) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message {
            server_id: if seq > 0 {
                format!("s{seq}")
            } else {
                String::new()
            },
            client_msg_id: "c1".into(),
            conversation_id: "conv-1".into(),
            conversation_type: conversation_type as i32,
            sender_id: "me".into(),
            conversation_seq: seq,
            status: status as i32,
            ..Default::default()
        });
        message.conversation_seq = seq;
        message.status = status as i32;
        message
    }

    fn summary(read: u32, unread: u32) -> MessageReadSummary {
        MessageReadSummary {
            read_count: read,
            unread_count: unread,
            total: read + unread,
        }
    }

    #[test]
    fn local_states_project_to_pending_sending_and_failed() {
        let built = message(ConversationType::Single, MessageStatus::Unspecified, 0);
        assert_eq!(built.receipt_view().status, MessageReceiptStatus::Pending);
        let created = message(ConversationType::Single, MessageStatus::Created, 0);
        assert_eq!(created.receipt_view().status, MessageReceiptStatus::Sending);
        let mut uploading = message(ConversationType::Group, MessageStatus::Unspecified, 0);
        uploading.local_state = MessageLocalState {
            uploading: true,
            ..Default::default()
        };
        assert_eq!(
            uploading.receipt_view().status,
            MessageReceiptStatus::Sending
        );
        let mut failed = message(ConversationType::Single, MessageStatus::Created, 0);
        failed.local_state.failed = true;
        assert_eq!(failed.receipt_view().status, MessageReceiptStatus::Failed);
        let wire_failed = message(ConversationType::Single, MessageStatus::Failed, 0);
        assert_eq!(
            wire_failed.receipt_view().status,
            MessageReceiptStatus::Failed
        );
        // A server sequence means the server has it, whatever the stale wire status says.
        let acknowledged = message(ConversationType::Single, MessageStatus::Created, 9);
        assert_eq!(
            acknowledged.receipt_view().status,
            MessageReceiptStatus::Sent
        );
    }

    #[test]
    fn single_chats_read_from_the_peer_read_mark_and_ignore_summaries() {
        let mut sent = message(ConversationType::Single, MessageStatus::Persisted, 3);
        assert_eq!(sent.receipt_view().status, MessageReceiptStatus::Sent);
        sent.is_read = true;
        sent.set_read_summary(Some(summary(0, 1)));
        let view = sent.receipt_view();
        assert_eq!(view.status, MessageReceiptStatus::Read);
        assert_eq!(view.read_summary, None, "single chats have no summary");
    }

    #[test]
    fn group_messages_are_read_only_when_the_summary_says_all_read() {
        let mut message = message(ConversationType::Group, MessageStatus::Sent, 5);
        // Someone read it, but without a summary a group message stays sent.
        message.is_read = true;
        assert_eq!(
            message.receipt_view(),
            MessageReceiptView {
                status: MessageReceiptStatus::Sent,
                read_summary: None
            }
        );
        message.set_read_summary(Some(summary(2, 3)));
        assert_eq!(
            message.receipt_view(),
            MessageReceiptView {
                status: MessageReceiptStatus::Sent,
                read_summary: Some(summary(2, 3))
            }
        );
        message.set_read_summary(Some(summary(5, 0)));
        assert_eq!(message.receipt_view().status, MessageReceiptStatus::Read);
        // An empty or inconsistent summary is not shown.
        message.set_read_summary(Some(summary(0, 0)));
        assert_eq!(message.read_summary(), None);
        message.attributes.insert(
            READ_SUMMARY_JSON_KEY.into(),
            r#"{"readCount":1,"unreadCount":1,"total":5}"#.into(),
        );
        assert_eq!(message.read_summary(), None);
        message.set_read_summary(None);
        assert!(!message.attributes.contains_key(READ_SUMMARY_JSON_KEY));
    }

    #[test]
    fn the_summary_rides_as_receipt_never_as_an_attribute_or_on_the_wire() {
        let mut message = message(ConversationType::Group, MessageStatus::Sent, 5);
        message.set_read_summary(Some(summary(1, 2)));
        let json = serde_json::to_value(&message).unwrap();
        assert_eq!(
            json["receipt"],
            serde_json::json!({"status": "sent", "readSummary": {"readCount": 1, "unreadCount": 2, "total": 3}})
        );
        assert!(json["attributes"].get(READ_SUMMARY_JSON_KEY).is_none());
        let wire = message.to_proto();
        assert!(!wire.attributes.contains_key(READ_SUMMARY_JSON_KEY));
        // A summary planted in an incoming message's attributes is dropped.
        let mut incoming = wire.clone();
        incoming.attributes.insert(
            READ_SUMMARY_JSON_KEY.into(),
            r#"{"readCount":9,"unreadCount":0,"total":9}"#.into(),
        );
        assert!(
            !IMMessage::new(incoming)
                .attributes
                .contains_key(READ_SUMMARY_JSON_KEY)
        );
    }
}
