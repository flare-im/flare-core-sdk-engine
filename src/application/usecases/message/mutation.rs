use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::transport_mapper::event_from_transport_action;
use crate::domain::{
    ConversationStore, DELETE_SCOPE_CONVERSATION_GLOBAL, DELETE_SCOPE_USER_PRIVATE,
    DELETE_TYPE_SOFT, MessageActor, MessageLocalUpdate, MessageLocatorService,
    MessageMutationService, MessageStore, MessageTransportAction, OperationApplyResult,
    ResolvedMessage,
};
use crate::infrastructure::protocol::PacketSender;
use crate::kernel::CurrentUserIdStore;
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::model::IMMessage;
use crate::model::message::{MarkType, ReactionAction};
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_proto::common::{
    Ack, MessageDeleteEvent, PinEvent, ReadAck, RealtimeControlPacket, TypingStatePacket,
    UnpinEvent, ack::Payload as AckPayload,
    realtime_control_packet::Payload as RealtimeControlPayload,
};

const REQUEST_TIMEOUT_SECS: u64 = 15;
const RESOLVE_WAIT_STEP_MS: u64 = 100;
const RESOLVE_WAIT_TOTAL_MS: u64 = 3_000;
/// typing 源头节流窗口：每会话 `typing=true` 最多 1 次/此窗口（停止态 stop 不受限，立即上行）。
const TYPING_SOURCE_THROTTLE_MS: u64 = 3_000;

fn timeout() -> Duration {
    Duration::from_secs(REQUEST_TIMEOUT_SECS)
}

pub struct MessageMutationUseCase {
    sender: Arc<PacketSender>,
    store: Arc<dyn MessageStore>,
    conversation_store: Arc<dyn ConversationStore>,
    current_user_id: CurrentUserIdStore,
    device_id: String,
    bus: Option<EventBus>,
    locator_service: MessageLocatorService,
    mutation_service: MessageMutationService,
    /// typing 源头节流：每会话上次发送 `typing=true` 的时刻(UNIX 毫秒,wasm 安全),窗口内抑制重复上行。
    typing_throttle: Arc<Mutex<HashMap<String, u64>>>,
}

impl MessageMutationUseCase {
    pub fn new(
        sender: Arc<PacketSender>,
        store: Arc<dyn MessageStore>,
        conversation_store: Arc<dyn ConversationStore>,
        current_user_id: CurrentUserIdStore,
        device_id: impl Into<String>,
        bus: Option<EventBus>,
    ) -> Self {
        Self {
            sender,
            store,
            conversation_store,
            current_user_id,
            device_id: device_id.into().trim().to_string(),
            bus,
            locator_service: MessageLocatorService,
            mutation_service: MessageMutationService,
            typing_throttle: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn current_user_id(&self) -> Result<String> {
        let uid = self.current_user_id.read().await.clone();
        if uid.is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        Ok(uid)
    }

    async fn actor(&self) -> Result<MessageActor> {
        MessageActor::require(self.current_user_id().await?)
    }

    async fn resolve_message(&self, message_id: &str) -> Result<ResolvedMessage> {
        let mut message = self
            .locator_service
            .require_by_any_id(self.store.as_ref(), message_id)
            .await?;
        if message.server_id.is_empty() {
            let mut waited_ms = 0;
            while waited_ms < RESOLVE_WAIT_TOTAL_MS {
                crate::shared::util::delay(Duration::from_millis(RESOLVE_WAIT_STEP_MS)).await;
                waited_ms += RESOLVE_WAIT_STEP_MS;
                if let Some(updated) = self
                    .locator_service
                    .find_by_any_id(self.store.as_ref(), message_id)
                    .await?
                {
                    message = updated;
                    if !message.server_id.is_empty() {
                        break;
                    }
                }
            }
        }
        if message.server_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::OperationTimeout,
                format!(
                    "message server id not ready yet: {} (try again shortly)",
                    message_id
                ),
            ));
        }
        Ok(ResolvedMessage::new(message))
    }

    fn require_resolved_conversation<'a>(
        conversation_id: &str,
        resolved: &'a ResolvedMessage,
    ) -> Result<&'a str> {
        let requested = conversation_id.trim();
        if requested.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "conversation_id must not be empty",
            ));
        }
        let actual = resolved.conversation_id();
        if requested != actual {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                format!(
                    "conversation_id does not match message: requested={}, actual={}",
                    requested, actual
                ),
            ));
        }
        Ok(actual)
    }

    pub async fn resolve_message_id(&self, message_id: &str) -> Result<(String, String)> {
        let resolved = self.resolve_message(message_id).await?;
        Ok((
            resolved.conversation_id().to_string(),
            resolved.server_id().to_string(),
        ))
    }

    pub async fn recall(&self, message_id: &str) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = resolved.conversation_id().to_string();
        let server_msg_id = resolved.server_id().to_string();
        let plan = self.mutation_service.plan_recall(&actor, &resolved)?;
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.store
            .update_status(
                resolved.server_id(),
                flare_proto::common::MessageStatus::Recalled as i32,
            )
            .await?;
        // 只改存储是不够的：可观测视图靠总线事件刷新。
        //
        // 线上实测——发起撤回的那一端会**一直显示原文**（20s 后仍在），
        // 而对端与全新客户端都正确不显示；重新登录后才好。原因就是这里
        // 少了 recompute + publish：`edit` / `delete_for_self` 都有，唯独 recall 没有。
        self.recompute_conversation_latest(&conversation_id).await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::Recalled {
                conversation_id,
                event: flare_proto::common::MessageRecallEvent {
                    server_msg_id,
                    ..Default::default()
                },
            }));
        }
        Ok(())
    }

    pub async fn edit(
        &self,
        conversation_id: &str,
        message_id: &str,
        new_content: Vec<u8>,
    ) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = Self::require_resolved_conversation(conversation_id, &resolved)?;
        let plan = self.mutation_service.plan_edit(
            &actor,
            conversation_id,
            &resolved,
            new_content.clone(),
        )?;
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.apply_local_update(plan.local_update).await?;
        self.recompute_conversation_latest(conversation_id).await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::Edited {
                conversation_id: conversation_id.to_string(),
                server_msg_id: resolved.server_id().to_string(),
            }));
        }
        Ok(())
    }

    pub async fn delete_for_self(&self, message_id: &str, reason: Option<String>) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = resolved.conversation_id().to_string();
        let server_msg_id = resolved.server_id().to_string();
        let reason_for_event = reason.clone();
        let plan = self
            .mutation_service
            .plan_delete_for_self(&actor, &resolved, reason);
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.apply_local_update(plan.local_update).await?;
        self.recompute_conversation_latest(&conversation_id).await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::Deleted {
                conversation_id,
                event: MessageDeleteEvent {
                    server_msg_id,
                    delete_type: Some(DELETE_TYPE_SOFT),
                    scope: Some(DELETE_SCOPE_USER_PRIVATE),
                    reason: reason_for_event,
                    notify_others: Some(false),
                    target_user_id: Some(actor.user_id),
                },
            }));
        }
        Ok(())
    }

    pub async fn delete_for_everyone(
        &self,
        message_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = resolved.conversation_id().to_string();
        let server_msg_id = resolved.server_id().to_string();
        let reason_for_event = reason.clone();
        let plan = self
            .mutation_service
            .plan_delete_for_everyone(&actor, &resolved, reason)?;
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.apply_local_update(plan.local_update).await?;
        self.recompute_conversation_latest(&conversation_id).await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::Deleted {
                conversation_id,
                event: MessageDeleteEvent {
                    server_msg_id,
                    delete_type: Some(DELETE_TYPE_SOFT),
                    scope: Some(DELETE_SCOPE_CONVERSATION_GLOBAL),
                    reason: reason_for_event,
                    notify_others: Some(true),
                    target_user_id: None,
                },
            }));
        }
        Ok(())
    }

    pub async fn mark_read_and_burn(&self, message_id: &str) -> Result<()> {
        let resolved = self.resolve_message(message_id).await?;
        let ack = read_ack_packet(
            &self.device_id,
            resolved.conversation_id(),
            resolved.message.conversation_seq(),
        )?;
        self.sender.send_ack(&ack).await
    }

    pub async fn typing(&self, conversation_id: &str, typing: bool) -> Result<()> {
        // 源头节流（飞书/Telegram 标准）：每会话 `typing=true` 最多 1 次/窗口（默认 3s），把按键级
        // 调用压成低频上行；`typing=false`(stop) 不受限、立即上行并清窗口（停止态及时）。
        // 与网关侧聚合/去抖互补：源头降上行、网关降下行扇出。
        if !self.should_send_typing(conversation_id, typing) {
            return Ok(());
        }
        let actor = self.actor().await?;
        self.sender
            .send_realtime_control_best_effort(&typing_realtime_control_packet(
                conversation_id,
                &actor,
                &self.device_id,
                typing,
            )?)
            .await
    }

    /// typing 源头节流判定。`typing=false` 总是发送（清窗口）；`typing=true` 窗口内抑制。
    fn should_send_typing(&self, conversation_id: &str, typing: bool) -> bool {
        let mut guard = match self.typing_throttle.lock() {
            Ok(guard) => guard,
            Err(_) => return true, // 锁中毒不影响有损信令语义，放行
        };
        typing_should_send(
            &mut guard,
            conversation_id,
            typing,
            crate::shared::util::time::now_millis(),
            Duration::from_millis(TYPING_SOURCE_THROTTLE_MS),
        )
    }

    pub async fn add_reaction(&self, message_id: &str, emoji: &str) -> Result<()> {
        self.react(message_id, emoji, ReactionAction::Add).await
    }

    pub async fn remove_reaction(&self, message_id: &str, emoji: &str) -> Result<()> {
        self.react(message_id, emoji, ReactionAction::Remove).await
    }

    async fn react(&self, message_id: &str, emoji: &str, action: ReactionAction) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let plan = self.mutation_service.plan_reaction(
            &actor,
            resolved.conversation_id(),
            resolved.server_id(),
            emoji,
            action,
        );
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.store
            .apply_reaction(
                resolved.conversation_id(),
                resolved.server_id(),
                &actor.user_id,
                emoji,
                action as i32,
            )
            .await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::ReactionChanged {
                conversation_id: resolved.conversation_id().to_string(),
                server_msg_id: resolved.server_id().to_string(),
                user_id: actor.user_id.clone(),
                emoji: emoji.to_string(),
                action: action as i32,
            }));
        }
        Ok(())
    }

    pub async fn pin(&self, conversation_id: &str, message_id: &str, scope: i32) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = Self::require_resolved_conversation(conversation_id, &resolved)?;
        let plan =
            self.mutation_service
                .plan_pin(&actor, conversation_id, resolved.server_id(), scope);
        let scope = match &plan.transport_action {
            MessageTransportAction::Pin { scope, .. } => *scope,
            _ => scope,
        };
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.apply_local_update(plan.local_update).await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::Pinned {
                conversation_id: conversation_id.to_string(),
                event: PinEvent {
                    server_msg_id: resolved.server_id().to_string(),
                    pinned_by: actor.user_id.clone(),
                    scope,
                    ..Default::default()
                },
            }));
        }
        Ok(())
    }

    pub async fn unpin(&self, conversation_id: &str, message_id: &str, scope: i32) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = Self::require_resolved_conversation(conversation_id, &resolved)?;
        let plan =
            self.mutation_service
                .plan_unpin(&actor, conversation_id, resolved.server_id(), scope);
        let scope = match &plan.transport_action {
            MessageTransportAction::Unpin { scope, .. } => *scope,
            _ => scope,
        };
        self.dispatch_transport_action(&plan.transport_action)
            .await?;
        self.apply_local_update(plan.local_update).await?;
        if let Some(bus) = &self.bus {
            bus.publish(SdkEvent::Message(MessageEvent::Unpinned {
                conversation_id: conversation_id.to_string(),
                event: UnpinEvent {
                    server_msg_id: resolved.server_id().to_string(),
                    unpinned_by: actor.user_id.clone(),
                    scope,
                },
            }));
        }
        Ok(())
    }

    pub async fn mark(
        &self,
        conversation_id: &str,
        message_id: &str,
        mark_type: MarkType,
        color: &str,
    ) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = Self::require_resolved_conversation(conversation_id, &resolved)?;
        let plan = self.mutation_service.plan_mark(
            &actor,
            conversation_id,
            resolved.server_id(),
            mark_type,
            color,
        );
        self.dispatch_transport_action(&plan.transport_action).await
    }

    pub async fn unmark(
        &self,
        conversation_id: &str,
        message_id: &str,
        mark_type: MarkType,
    ) -> Result<()> {
        let actor = self.actor().await?;
        let resolved = self.resolve_message(message_id).await?;
        let conversation_id = Self::require_resolved_conversation(conversation_id, &resolved)?;
        let plan = self.mutation_service.plan_unmark(
            &actor,
            conversation_id,
            resolved.server_id(),
            mark_type,
        );
        self.dispatch_transport_action(&plan.transport_action).await
    }

    async fn apply_local_update(&self, update: MessageLocalUpdate) -> Result<()> {
        match update {
            MessageLocalUpdate::None => Ok(()),
            MessageLocalUpdate::UpdateContent {
                message_id,
                new_content,
            } => {
                self.store.update_content(&message_id, new_content).await?;
                Ok(())
            }
            MessageLocalUpdate::SetPinned { message_id, pinned } => {
                let applied = self
                    .store
                    .apply_pin_event(&message_id, pinned, None)
                    .await?;
                if !matches!(applied, OperationApplyResult::Applied) {
                    return Err(FlareError::localized(
                        ErrorCode::InvalidParameter,
                        format!("message pin target not found: {message_id}"),
                    ));
                }
                Ok(())
            }
            MessageLocalUpdate::Delete { message_id } => self.store.delete(&message_id).await,
        }
    }

    async fn recompute_conversation_latest(&self, conversation_id: &str) -> Result<()> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(());
        }
        let latest = self
            .store
            .get_by_conversation(conversation_id, 0, 1)
            .await?;
        let Some(message) = latest.first() else {
            return Ok(());
        };
        self.update_conversation_latest_from_message(conversation_id, message)
            .await
    }

    async fn update_conversation_latest_from_message(
        &self,
        conversation_id: &str,
        message: &IMMessage,
    ) -> Result<()> {
        let last_message_id = if message.server_id.trim().is_empty() {
            message.client_msg_id.trim()
        } else {
            message.server_id.trim()
        };
        if last_message_id.is_empty() {
            return Ok(());
        }
        let preview = message.text_for_storage();
        self.conversation_store
            .update_last_message(
                conversation_id,
                last_message_id,
                message.sender_id(),
                message.display_time_ms(),
                preview.as_deref(),
                message.conversation_seq,
            )
            .await
    }

    async fn dispatch_transport_action(&self, action: &MessageTransportAction) -> Result<()> {
        self.sender
            .send_event(&event_from_transport_action(action), timeout())
            .await
    }
}

/// typing 源头节流纯判定（便于单测）：`typing=false` 总放行并清窗口；`typing=true` 窗口内抑制、否则记录并放行。
fn typing_should_send(
    map: &mut HashMap<String, u64>,
    conversation_id: &str,
    typing: bool,
    now: u64,
    window: Duration,
) -> bool {
    if !typing {
        map.remove(conversation_id);
        return true;
    }
    if let Some(last) = map.get(conversation_id)
        && now.saturating_sub(*last) < window.as_millis() as u64
    {
        return false;
    }
    map.insert(conversation_id.to_string(), now);
    true
}

fn typing_realtime_control_packet(
    conversation_id: &str,
    actor: &MessageActor,
    device_id: &str,
    typing: bool,
) -> Result<RealtimeControlPacket> {
    let conversation_id = conversation_id.trim();
    if conversation_id.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "conversation_id must not be empty",
        ));
    }
    let device_id = device_id.trim();
    if device_id.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "typing device_id must not be empty",
        ));
    }
    Ok(RealtimeControlPacket {
        control_type: "typing".to_string(),
        conversation_id: Some(conversation_id.to_string()),
        correlation_id: None,
        attributes: Default::default(),
        payload: Some(RealtimeControlPayload::Typing(TypingStatePacket {
            conversation_id: conversation_id.to_string(),
            user_id: actor.user_id.clone(),
            typing,
            device_id: Some(device_id.to_string()),
            occurred_at: Some(crate::shared::util::now_millis() as i64),
        })),
    })
}

fn read_ack_packet(device_id: &str, conversation_id: &str, read_seq: u64) -> Result<Ack> {
    let conversation_id = conversation_id.trim();
    if conversation_id.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "conversation_id must not be empty",
        ));
    }
    if read_seq == 0 {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "read_seq must be greater than 0",
        ));
    }
    let device_id = device_id.trim();
    if device_id.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "read ack device_id must not be empty",
        ));
    }
    let ack_id = format!("read:{conversation_id}:{read_seq}");
    Ok(Ack {
        ack_id: Some(ack_id.clone()),
        ack_at: Some(crate::shared::util::now_millis() as i64),
        payload: Some(AckPayload::Read(ReadAck {
            conversation_id: conversation_id.to_string(),
            read_seq,
            device_id: Some(device_id.to_string()),
            ack_id: Some(ack_id),
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::MessageMutationUseCase;
    use super::read_ack_packet;
    use super::typing_realtime_control_packet;
    use super::typing_should_send;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn typing_source_throttle_suppresses_within_window_and_passes_stop() {
        let mut map: HashMap<String, u64> = HashMap::new();
        let window = Duration::from_millis(3000);
        let t0: u64 = 1_000_000;
        // 首次 typing → 放行
        assert!(typing_should_send(&mut map, "c", true, t0, window));
        // 窗口内再次 typing → 抑制
        assert!(!typing_should_send(&mut map, "c", true, t0 + 500, window));
        // stop 总是放行（并清窗口）
        assert!(typing_should_send(&mut map, "c", false, t0 + 600, window));
        // stop 清窗口后，再次 typing 立即放行
        assert!(typing_should_send(&mut map, "c", true, t0 + 700, window));
        // 超过窗口后 typing 再次放行
        assert!(typing_should_send(
            &mut map,
            "c",
            true,
            t0 + 700 + window.as_millis() as u64,
            window
        ));
        // 不同会话独立
        assert!(typing_should_send(&mut map, "c2", true, t0, window));
    }
    use crate::content::ContentBuilder;
    use crate::domain::{
        ConversationReader, ConversationStore, ConversationWriter, MessageActor,
        MessageLocalUpdate, MessageReader, MessageStore, MessageWriter, ResolvedMessage,
    };
    use crate::infrastructure::protocol::{PacketSender, ProtobufCodec};
    use crate::kernel::CurrentUserIdStore;
    use crate::kernel::event::EventBus;
    use crate::model::Conversation;
    use crate::model::message::IMMessage;
    use crate::shared::error::ErrorCode;
    use crate::storage::{MemoryConversationStore, MemoryMessageStore};
    use flare_proto::common::ack::Payload as AckPayload;
    use flare_proto::common::realtime_control_packet::Payload as RealtimeControlPayload;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock};

    fn resolved_message(conversation_id: &str) -> ResolvedMessage {
        let proto = flare_proto::common::Message {
            conversation_id: conversation_id.to_string(),
            server_id: "server-1".to_string(),
            ..Default::default()
        };
        ResolvedMessage::new(IMMessage::new(proto))
    }

    fn text_message(
        conversation_id: &str,
        server_id: &str,
        client_msg_id: &str,
        text: &str,
    ) -> IMMessage {
        let content = ContentBuilder::text(text).build();
        IMMessage::new(flare_proto::common::Message {
            server_id: server_id.to_string(),
            client_msg_id: client_msg_id.to_string(),
            conversation_id: conversation_id.to_string(),
            sender_id: "alice".to_string(),
            conversation_seq: 7,
            created_at: 1_700_000_000_000,
            message_type: content.message_type as i32,
            content: Some(content.inner),
            ..Default::default()
        })
    }

    fn encoded_text(text: &str) -> Vec<u8> {
        ContentBuilder::text(text).build().encode()
    }

    fn usecase(
        messages: Arc<dyn MessageStore>,
        conversations: Arc<dyn ConversationStore>,
    ) -> MessageMutationUseCase {
        let sender = Arc::new(PacketSender::new(
            Arc::new(Mutex::new(None)),
            Arc::new(ProtobufCodec),
        ));
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("alice".to_string()));
        MessageMutationUseCase::new(
            sender,
            messages,
            conversations,
            current_user_id,
            "device-a",
            Some(EventBus::new()),
        )
    }

    #[tokio::test]
    async fn unauthorized_recall_is_rejected_before_transport_or_local_change() {
        let messages = Arc::new(MemoryMessageStore::new());
        let mut message = text_message("c-denied", "s-denied", "m-denied", "keep original");
        message.sender_id = "bob".into();
        let original = message.text_for_storage();
        messages.save_batch(&[message]).await.unwrap();
        let case = usecase(messages.clone(), Arc::new(MemoryConversationStore::new()));
        let error = case
            .recall("m-denied")
            .await
            .expect_err("only sender recalls");
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        let stored = messages
            .get_by_client_msg_id("m-denied")
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.is_recalled);
        assert_eq!(stored.text_for_storage(), original);
    }

    #[tokio::test]
    async fn unauthorized_edit_is_rejected_before_transport_or_local_change() {
        let messages = Arc::new(MemoryMessageStore::new());
        let mut message = text_message("c-denied", "s-denied", "m-denied", "keep original");
        message.sender_id = "bob".into();
        let original = message.text_for_storage();
        messages.save_batch(&[message]).await.unwrap();
        let case = usecase(messages.clone(), Arc::new(MemoryConversationStore::new()));
        let error = case
            .edit("c-denied", "m-denied", encoded_text("unauthorized change"))
            .await
            .expect_err("only sender edits");
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        let stored = messages
            .get_by_client_msg_id("m-denied")
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.is_edited);
        assert_eq!(stored.text_for_storage(), original);
    }

    #[test]
    fn require_resolved_conversation_accepts_matching_conversation() {
        let resolved = resolved_message("conv-a");

        let actual =
            MessageMutationUseCase::require_resolved_conversation("conv-a", &resolved).unwrap();

        assert_eq!(actual, "conv-a");
    }

    #[test]
    fn require_resolved_conversation_rejects_empty_conversation() {
        let resolved = resolved_message("conv-a");

        let err = MessageMutationUseCase::require_resolved_conversation(" ", &resolved)
            .expect_err("empty conversation must be rejected");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn require_resolved_conversation_rejects_mismatched_conversation() {
        let resolved = resolved_message("conv-a");

        let err = MessageMutationUseCase::require_resolved_conversation("conv-b", &resolved)
            .expect_err("mismatched conversation must be rejected");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    /// 撤回必须像 edit / delete_for_self 一样，**通知本地视图**。
    ///
    /// 线上实测：只改存储不发总线时，发起撤回的那一端会一直显示原文（20s 后仍在），
    /// 而对端与全新客户端都正确不显示——只有自己看不到撤回效果，重新登录才好。
    #[test]
    fn recall_must_notify_local_views_not_only_the_store() {
        let source = include_str!("mutation.rs");
        let body = source
            .split("pub async fn recall(")
            .nth(1)
            .expect("recall 存在")
            .split("pub async fn edit(")
            .next()
            .expect("下一个函数存在");
        assert!(
            body.contains("MessageEvent::Recalled"),
            "recall 必须发布 Recalled 事件，否则发起端的视图不会刷新"
        );
        assert!(
            body.contains("recompute_conversation_latest"),
            "recall 必须重算会话最新消息，否则会话列表仍显示被撤回的预览"
        );
        assert!(
            body.contains("update_status"),
            "recall 仍须落库状态（这条原本就有，别在补事件时丢掉）"
        );
    }

    #[tokio::test]
    async fn local_edit_refreshes_message_preview_and_conversation_latest() {
        let messages = Arc::new(MemoryMessageStore::new());
        let conversations = Arc::new(MemoryConversationStore::new());
        let message = text_message("conv-edit", "server-edit", "client-edit", "before edit");
        messages
            .save_batch(std::slice::from_ref(&message))
            .await
            .unwrap();
        conversations
            .save_one(&Conversation {
                conversation_id: "conv-edit".to_string(),
                last_message_id: Some("server-edit".to_string()),
                last_sender_id: Some("alice".to_string()),
                last_message_at: Some(message.display_time_ms()),
                last_message_preview: Some("before edit".to_string()),
                max_seq: message.conversation_seq,
                ..Default::default()
            })
            .await
            .unwrap();
        let usecase = usecase(messages.clone(), conversations.clone());

        usecase
            .apply_local_update(MessageLocalUpdate::UpdateContent {
                message_id: "server-edit".to_string(),
                new_content: encoded_text("after edit"),
            })
            .await
            .unwrap();
        usecase
            .recompute_conversation_latest("conv-edit")
            .await
            .unwrap();

        let stored = messages
            .get("server-edit")
            .await
            .unwrap()
            .expect("edited message");
        assert!(stored.is_edited);
        assert!(
            stored.text_preview.contains("after edit"),
            "message preview should contain edited text: {}",
            stored.text_preview
        );
        let conversation = conversations
            .get("conv-edit")
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(conversation.last_message_id.as_deref(), Some("server-edit"));
        let latest_preview = conversation
            .last_message_preview
            .as_deref()
            .expect("latest preview");
        assert!(
            latest_preview.contains("after edit"),
            "conversation preview should contain edited text: {latest_preview}"
        );
    }

    #[test]
    fn typing_realtime_control_packet_includes_current_device_id() {
        let actor = MessageActor::require("alice".to_string()).unwrap();

        let packet = typing_realtime_control_packet("conv-a", &actor, "device-a", true).unwrap();

        assert_eq!(packet.control_type, "typing");
        assert_eq!(packet.conversation_id.as_deref(), Some("conv-a"));
        let Some(RealtimeControlPayload::Typing(payload)) = packet.payload else {
            panic!("typing payload expected");
        };
        assert_eq!(payload.conversation_id, "conv-a");
        assert_eq!(payload.user_id, "alice");
        assert!(payload.typing);
        assert_eq!(payload.device_id.as_deref(), Some("device-a"));
        assert!(payload.occurred_at.is_some());
    }

    #[test]
    fn typing_realtime_control_packet_requires_device_id() {
        let actor = MessageActor::require("alice".to_string()).unwrap();

        let err = typing_realtime_control_packet("conv-a", &actor, " ", true)
            .expect_err("device id is required");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn read_ack_packet_includes_current_device_id() {
        let ack = read_ack_packet("device-a", "conv-a", 42).unwrap();

        assert_eq!(ack.ack_id.as_deref(), Some("read:conv-a:42"));
        assert!(ack.ack_at.is_some());
        let Some(AckPayload::Read(payload)) = ack.payload else {
            panic!("read ack payload expected");
        };
        assert_eq!(payload.conversation_id, "conv-a");
        assert_eq!(payload.read_seq, 42);
        assert_eq!(payload.device_id.as_deref(), Some("device-a"));
        assert_eq!(payload.ack_id.as_deref(), Some("read:conv-a:42"));
    }

    #[test]
    fn read_ack_packet_rejects_empty_device_id() {
        let err = read_ack_packet(" ", "conv-a", 42).expect_err("device id is required");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn read_ack_packet_rejects_zero_read_seq() {
        let err = read_ack_packet("device-a", "conv-a", 0).expect_err("read seq is required");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }
}
