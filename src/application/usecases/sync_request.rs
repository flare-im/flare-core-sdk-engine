use std::sync::Arc;
use std::time::Duration;

use flare_proto::common::{
    ConversationParticipantsSync, ConversationUserSettingsSync, ConversationsSync,
    ConversationsSyncRes, EnsureConversationSync, Event, GetSyncCursorSync,
    GetUserNotificationSettingsSync, MultiConversationSync, QueryEventsSync,
    SingleConversationSync, Sync as SyncWire, SyncRes, SyncSnapshotSync, SyncSnapshotSyncRes,
    UpdateSyncCursorSync, UserNotificationSettingsSync, event::Payload as EventPayload,
    sync::Payload as SyncPayload, sync_res::Payload as SyncResPayload,
    sync_slice_item::Payload as SyncSliceItemPayload,
};

use crate::domain::{
    CONVERSATIONS_SYNC_TIMEOUT_SECS, GET_SYNC_CURSOR_TIMEOUT_SECS, QUERY_EVENTS_TIMEOUT_SECS,
    UPDATE_CURSOR_TIMEOUT_SECS,
};
use crate::infrastructure::protocol::PacketSender;
use crate::shared::error::{FlareError, Result};

const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// 用户级通知设置是一个小对象，读写都不该等太久：设置页上的开关要很快有结果。
const USER_SETTINGS_TIMEOUT_SECS: u64 = 10;

fn sync_wire(device_id: &str, payload: SyncPayload) -> SyncWire {
    SyncWire {
        device_id: device_id.to_string(),
        payload: Some(payload),
    }
}

pub struct SyncRequestUseCase {
    sender: Arc<PacketSender>,
    device_id: String,
}

impl SyncRequestUseCase {
    pub fn new(sender: Arc<PacketSender>, device_id: impl Into<String>) -> Self {
        Self {
            sender,
            device_id: device_id.into(),
        }
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// 推送路径兜底：服务端主动下发 SyncRes 时仍可被消费（主路径为 `send_sync_and_wait`）。
    pub async fn handle_response(&self, resp: SyncRes) -> bool {
        let _ = resp;
        false
    }

    pub async fn request_single_page(
        &self,
        conversation_id: &str,
        last_seq: u64,
        limit: i32,
        cursor: String,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(
            &self.device_id,
            SyncPayload::SingleConversation(SingleConversationSync {
                conversation_id: conversation_id.to_string(),
                after_conversation_seq: last_seq,
                cursor,
                limit,
                before_conversation_seq: 0,
            }),
        );
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    /// 历史回溯页：返回严格小于 `before_seq` 的最近 `limit` 条（方向经 proto 显式字段表达）。
    pub async fn request_backfill_page(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: i32,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(
            &self.device_id,
            SyncPayload::SingleConversation(SingleConversationSync {
                conversation_id: conversation_id.to_string(),
                after_conversation_seq: 0,
                cursor: String::new(),
                limit,
                before_conversation_seq: before_seq,
            }),
        );
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    /// 批量拉取多会话增量（B3 catch-up：把 N 个变化会话合并为一次请求，O(会话)→O(1请求)）。
    pub async fn request_multi_conversation(
        &self,
        conversation_ids: Vec<String>,
        last_conversation_seq_per_conversation: std::collections::HashMap<String, u64>,
        limit_per_conversation: i32,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(
            &self.device_id,
            SyncPayload::MultiConversation(MultiConversationSync {
                conversation_ids,
                last_conversation_seq_per_conversation,
                limit_per_conversation,
            }),
        );
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    pub async fn request_query_events(&self, req: QueryEventsSync) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::QueryEvents(req));
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(QUERY_EVENTS_TIMEOUT_SECS))
            .await
    }

    pub async fn request_conversation_participants(
        &self,
        req: ConversationParticipantsSync,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::ConversationParticipants(req));
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    pub async fn request_get_cursor(&self, req: GetSyncCursorSync) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::GetSyncCursor(req));
        self.sender
            .send_sync_and_wait(
                &sync_event,
                Duration::from_secs(GET_SYNC_CURSOR_TIMEOUT_SECS),
            )
            .await
    }

    pub async fn request_update_cursor(&self, req: UpdateSyncCursorSync) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::UpdateSyncCursor(req));
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(UPDATE_CURSOR_TIMEOUT_SECS))
            .await
    }

    pub async fn request_conversation_user_settings(
        &self,
        req: ConversationUserSettingsSync,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::ConversationUserSettings(req));
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    /// 读当前用户的用户级通知设置。
    pub async fn request_user_notification_settings(&self) -> Result<SyncRes> {
        let sync_event = sync_wire(
            &self.device_id,
            SyncPayload::GetUserNotificationSettings(GetUserNotificationSettingsSync {}),
        );
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(USER_SETTINGS_TIMEOUT_SECS))
            .await
    }

    /// 部分更新当前用户的用户级通知设置。
    pub async fn update_user_notification_settings(
        &self,
        req: UserNotificationSettingsSync,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::UserNotificationSettings(req));
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(USER_SETTINGS_TIMEOUT_SECS))
            .await
    }

    /// 显式建群：把整张成员表一次性交服务端建群,之后消息不再携带成员表。
    pub async fn request_ensure_conversation(
        &self,
        req: EnsureConversationSync,
    ) -> Result<SyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::EnsureConversation(req));
        self.sender
            .send_sync_and_wait(&sync_event, Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .await
    }

    /// 快照重建（I2 gap-too-large 切换）：增量窗口过期/缺口不可回放时，放弃回放改拉状态快照。
    pub async fn request_sync_snapshot(
        &self,
        req: SyncSnapshotSync,
    ) -> Result<SyncSnapshotSyncRes> {
        let sync_event = sync_wire(&self.device_id, SyncPayload::SyncSnapshot(req));
        let resp = self
            .sender
            .send_sync_and_wait(
                &sync_event,
                Duration::from_secs(CONVERSATIONS_SYNC_TIMEOUT_SECS),
            )
            .await?;
        match resp.payload {
            Some(SyncResPayload::SyncSnapshotRes(response)) => Ok(response),
            _ => Err(FlareError::general_error(
                "unexpected sync snapshot response".to_string(),
            )),
        }
    }

    pub async fn request_conversations(
        &self,
        cursor: String,
        limit: i32,
    ) -> Result<ConversationsSyncRes> {
        let sync_event = sync_wire(
            &self.device_id,
            SyncPayload::Conversations(ConversationsSync {
                cursor,
                limit,
                ..Default::default()
            }),
        );
        let resp = self
            .sender
            .send_sync_and_wait(
                &sync_event,
                Duration::from_secs(CONVERSATIONS_SYNC_TIMEOUT_SECS),
            )
            .await?;
        match resp.payload {
            Some(SyncResPayload::Conversations(response)) => Ok(response),
            _ => Err(FlareError::general_error(
                "unexpected conversations sync response".to_string(),
            )),
        }
    }
}

/// 按会话序号读一条消息：严格小于 `seq + 1` 的最近 1 条，即翻历史的回溯页，只取 `seq` 上那条。
/// 插件卡片补读共享层用（Reader 读回的消息 extra 带着最新共享层）。
impl crate::application::services::RemoteMessageLookup for SyncRequestUseCase {
    fn message_at_seq<'a>(
        &'a self,
        conversation_id: &'a str,
        seq: u64,
    ) -> crate::application::services::plugin_state_refetch::BoxFuture<
        'a,
        Result<Option<crate::model::IMMessage>>,
    > {
        Box::pin(async move {
            let response = self
                .request_backfill_page(conversation_id, seq.saturating_add(1), 1)
                .await?;
            let Some(SyncResPayload::SingleConversation(page)) = response.payload else {
                return Ok(None);
            };
            Ok(message_at_seq(page.items, seq))
        })
    }
}

/// The message at `seq` in a page: a message item, or an event item carrying one.
fn message_at_seq(
    items: Vec<flare_proto::common::SyncSliceItem>,
    seq: u64,
) -> Option<crate::model::IMMessage> {
    items.into_iter().find_map(|item| {
        let message = match item.payload? {
            SyncSliceItemPayload::Message(message) => message,
            SyncSliceItemPayload::Event(Event {
                payload: Some(EventPayload::Message(message)),
                ..
            }) => message,
            _ => return None,
        };
        (message.conversation_seq == seq || item.conversation_seq == seq)
            .then(|| crate::model::IMMessage::new(message))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{Message, SyncSkipItem, SyncSliceItem};

    fn message(server_id: &str, seq: u64) -> Message {
        Message {
            server_id: server_id.into(),
            conversation_seq: seq,
            ..Default::default()
        }
    }

    #[test]
    fn the_message_at_the_seq_is_picked_from_message_or_event_items() {
        let page = vec![
            SyncSliceItem {
                conversation_seq: 6,
                payload: Some(SyncSliceItemPayload::Skip(SyncSkipItem::default())),
                ..Default::default()
            },
            SyncSliceItem {
                conversation_seq: 7,
                payload: Some(SyncSliceItemPayload::Message(message("m7", 7))),
                ..Default::default()
            },
        ];
        assert_eq!(message_at_seq(page.clone(), 7).unwrap().server_id, "m7");
        assert!(
            message_at_seq(page, 6).is_none(),
            "a skipped seq has no message"
        );
        let event = vec![SyncSliceItem {
            conversation_seq: 9,
            payload: Some(SyncSliceItemPayload::Event(Event {
                payload: Some(EventPayload::Message(message("m9", 0))),
                ..Default::default()
            })),
            ..Default::default()
        }];
        assert_eq!(message_at_seq(event.clone(), 9).unwrap().server_id, "m9");
        assert!(message_at_seq(event, 8).is_none());
    }
}
