use crate::application::InboundSource;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::domain::{
    ConversationIdRewrite, ConversationIdentityService, ConversationStore,
    IncomingMessageConvergenceDecision, MessageDeliveryService, MessageStore,
};
use crate::kernel::ReliableSendQueuePort;
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::model::IMMessage;
use crate::shared::error::Result;

#[derive(Clone)]
pub(crate) struct IncomingMessageConverger {
    message_store: Arc<dyn MessageStore>,
    conversation_store: Arc<dyn ConversationStore>,
    bus: EventBus,
    reliable_queue: Arc<RwLock<Option<Arc<dyn ReliableSendQueuePort>>>>,
}

impl IncomingMessageConverger {
    pub(crate) fn new(
        message_store: Arc<dyn MessageStore>,
        conversation_store: Arc<dyn ConversationStore>,
        bus: EventBus,
        reliable_queue: Option<Arc<dyn ReliableSendQueuePort>>,
    ) -> Self {
        Self {
            message_store,
            conversation_store,
            bus,
            reliable_queue: Arc::new(RwLock::new(reliable_queue)),
        }
    }

    pub(crate) fn set_reliable_queue(
        &self,
        reliable_queue: Option<Arc<dyn ReliableSendQueuePort>>,
    ) {
        if let Ok(mut guard) = self.reliable_queue.write() {
            *guard = reliable_queue;
        }
    }

    pub(crate) async fn converge_messages(
        &self,
        current_user_id: &str,
        messages: Vec<IMMessage>,
        source: InboundSource,
    ) -> Result<Vec<IMMessage>> {
        if messages.is_empty() {
            return Ok(messages);
        }
        let (mut messages, rewrites) = canonicalize_incoming_messages(messages, current_user_id);
        for rewrite in rewrites {
            self.apply_identity_rewrite(rewrite).await?;
        }
        for message in &mut messages {
            MessageDeliveryService::normalize_incoming_server_state(message);
            crate::domain::enforce_retention_visibility(message);
        }
        let local_by_client = self.local_by_client_msg_id(&messages).await?;
        let local_by_server = self.local_by_server_id(&messages).await?;
        let ids: HashSet<_> = messages
            .iter()
            .map(|m| m.conversation_id.as_str())
            .collect();
        let ids = ids.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let conversations: HashMap<_, _> = self
            .conversation_store
            .get_stored_many(&ids)
            .await?
            .into_iter()
            .map(|c| (c.conversation_id.clone(), c))
            .collect();
        let mut out = Vec::with_capacity(messages.len());
        for message in messages {
            let local_by_client = trimmed_lookup(&local_by_client, &message.client_msg_id);
            let local_by_server = trimmed_lookup(&local_by_server, &message.server_id);
            let conversation = conversations.get(&message.conversation_id);
            let floor = conversation
                .map(crate::domain::sync_visibility_floor)
                .unwrap_or_default();
            let visible = crate::domain::message_visible_after_clear(&message, floor);
            match MessageDeliveryService::decide_incoming_message_convergence(
                current_user_id,
                &message,
                local_by_client,
                local_by_server,
            ) {
                IncomingMessageConvergenceDecision::EmitReceived => {
                    if visible {
                        out.push(message);
                    }
                }
                IncomingMessageConvergenceDecision::MergePendingAndAck => {
                    // Live fanout can precede the archive commit. Keep the pending row
                    // and outbox until a gateway receipt or storage-backed sync confirms it.
                    if source == InboundSource::RealtimePush {
                        continue;
                    }
                    let ack = MessageDeliveryService::synthetic_ack_from_incoming(&message);
                    let reliable_queue = self
                        .reliable_queue
                        .read()
                        .ok()
                        .and_then(|guard| guard.clone());
                    if let Some(queue) = reliable_queue {
                        queue.on_ack(ack).await?;
                    } else {
                        let merged = MessageDeliveryService::merge_incoming_as_sent(
                            local_by_client,
                            &message,
                        );
                        self.message_store
                            .update_after_ack(&message.client_msg_id, &merged)
                            .await?;
                        self.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
                            ack: Box::new(ack),
                        }));
                    }
                }
                IncomingMessageConvergenceDecision::DropDuplicate => {
                    let mut message = message;
                    let projected = match conversation {
                        Some(conversation) => {
                            crate::domain::message_projection_coverage(conversation)?
                                .range_containing(message.conversation_seq)
                                .is_some()
                        }
                        None => false,
                    };
                    if let Some(local) = local_by_server {
                        crate::domain::merge_message_event_state(&mut message, local);
                        message.status = message.status.max(local.status);
                        message.is_read |= local.is_read;
                        message.is_recalled |= local.is_recalled;
                        if message.status != local.status
                            || message.retention_state != local.retention_state
                            || message.retention_policy != local.retention_policy
                            || message.encoded_content != local.encoded_content
                            || message.attributes != local.attributes
                            || message.extensions != local.extensions
                        {
                            self.message_store.save_one(&message).await?;
                            self.bus.publish(SdkEvent::Conversation(
                                crate::kernel::event::ConversationEvent::Synced {
                                    conversation_ids: vec![message.conversation_id.clone()],
                                },
                            ));
                        }
                    }
                    if visible && !projected {
                        out.push(message);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn local_by_client_msg_id(
        &self,
        messages: &[IMMessage],
    ) -> Result<HashMap<String, IMMessage>> {
        let ids = unique_non_empty_ids(
            messages
                .iter()
                .map(|message| message.client_msg_id.as_str()),
        );
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        Ok(self
            .message_store
            .get_by_client_msg_ids(&ids)
            .await?
            .into_iter()
            .filter_map(|message| keyed_message(message.client_msg_id.clone(), message))
            .collect())
    }

    async fn local_by_server_id(
        &self,
        messages: &[IMMessage],
    ) -> Result<HashMap<String, IMMessage>> {
        let ids = unique_non_empty_ids(messages.iter().map(|message| message.server_id.as_str()));
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        Ok(self
            .message_store
            .get_by_message_ids(&ids)
            .await?
            .into_iter()
            .filter_map(|message| keyed_message(message.server_id.clone(), message))
            .collect())
    }

    async fn apply_identity_rewrite(&self, rewrite: ConversationIdRewrite) -> Result<()> {
        if rewrite.from == rewrite.to {
            return Ok(());
        }
        let moved = self
            .message_store
            .rewrite_conversation_id(&rewrite.from, &rewrite.to)
            .await?;
        self.conversation_store
            .merge_conversation_identity(&rewrite.from, &rewrite.to)
            .await?;
        if moved > 0 {
            tracing::info!(
                from = %rewrite.from,
                to = %rewrite.to,
                moved,
                "canonicalized single chat conversation identity"
            );
        }
        Ok(())
    }
}

fn canonicalize_incoming_messages(
    messages: Vec<IMMessage>,
    current_user_id: &str,
) -> (Vec<IMMessage>, Vec<ConversationIdRewrite>) {
    let mut seen = HashSet::new();
    let mut rewrites = Vec::new();
    let mut out: Vec<IMMessage> = Vec::with_capacity(messages.len());
    let mut by_server: HashMap<String, usize> = HashMap::new();
    for mut message in messages {
        if let Some(rewrite) = ConversationIdentityService::canonicalize_single_chat_message(
            &mut message,
            current_user_id,
        ) {
            let key = (rewrite.from.clone(), rewrite.to.clone());
            if seen.insert(key) {
                rewrites.push(rewrite);
            }
        }
        if let Some(&index) = by_server.get(&message.server_id) {
            crate::domain::merge_message_event_state(&mut message, &out[index]);
            out[index] = message;
        } else {
            if !message.server_id.trim().is_empty() {
                by_server.insert(message.server_id.clone(), out.len());
            }
            out.push(message);
        }
    }
    (out, rewrites)
}

fn unique_non_empty_ids<'a>(ids: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut seen = HashSet::new();
    ids.filter_map(|id| {
        let id = id.trim();
        if id.is_empty() || !seen.insert(id.to_string()) {
            None
        } else {
            Some(id.to_string())
        }
    })
    .collect()
}

fn keyed_message(key: String, message: IMMessage) -> Option<(String, IMMessage)> {
    let key = key.trim();
    if key.is_empty() {
        None
    } else {
        Some((key.to_string(), message))
    }
}

fn trimmed_lookup<'a>(map: &'a HashMap<String, IMMessage>, key: &str) -> Option<&'a IMMessage> {
    let key = key.trim();
    if key.is_empty() { None } else { map.get(key) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use flare_proto::common::{ConversationType as ProtoConversationType, Message as ProtoMessage};

    use crate::domain::conversation::generate_single_chat_conversation_id;
    use crate::domain::{ConversationReader, ConversationWriter, MessageReader, MessageWriter};
    use crate::infrastructure::persistence::memory_im::{
        MemoryConversationStore, MemoryMessageStore,
    };
    use crate::kernel::event::EventBus;
    use crate::model::conversation::ConversationType;
    use crate::model::message::MessageStatus;
    use crate::model::{Conversation, IMMessage};

    fn single_message(server_id: &str, conversation_id: &str, peer: &str, seq: u64) -> IMMessage {
        IMMessage::new(ProtoMessage {
            server_id: server_id.to_string(),
            conversation_id: conversation_id.to_string(),
            client_msg_id: format!("client-{server_id}"),
            sender_id: peer.to_string(),
            conversation_seq: seq,
            conversation_type: ProtoConversationType::Single as i32,
            channel_id: peer.to_string(),
            created_at: 1_000 + seq as i64,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn terminal_snapshots_in_one_batch_reach_consumers_only_once() {
        let mut original = IMMessage::new(ProtoMessage {
            server_id: "m".into(),
            conversation_id: "c".into(),
            conversation_type: ProtoConversationType::Group as i32,
            ..Default::default()
        });
        original.encoded_content = crate::content::ContentBuilder::text("private")
            .build()
            .encode();
        let mut purged = original.clone();
        purged.retention_state = Some(flare_proto::common::MessageRetentionState {
            lifecycle: 5,
            content_visibility: 4,
            ..Default::default()
        });
        let messages = Arc::new(MemoryMessageStore::new());
        let converger = IncomingMessageConverger::new(
            messages,
            Arc::new(MemoryConversationStore::new()),
            EventBus::new(),
            None,
        );
        let output = converger
            .converge_messages("me", vec![purged, original], InboundSource::SyncReplay)
            .await
            .unwrap();
        assert_eq!(output.len(), 1);
        assert!(output[0].encoded_content.is_empty());
        assert_eq!(output[0].retention_state.as_ref().unwrap().lifecycle, 5);
    }

    #[tokio::test]
    async fn canonicalizes_single_chat_messages_and_migrates_existing_local_rows() {
        let current_user = "me";
        let peer = "peer";
        let canonical = generate_single_chat_conversation_id(current_user, peer);
        let messages = Arc::new(MemoryMessageStore::new());
        let conversations = Arc::new(MemoryConversationStore::new());

        let old_message = single_message("old-server", peer, peer, 1);
        messages.save_one(&old_message).await.unwrap();

        let mut old_conversation = Conversation::from_conversation_id(peer.to_string());
        old_conversation.conversation_type = ConversationType::Single;
        old_conversation.channel_id = peer.to_string();
        old_conversation.max_seq = 1;
        conversations.save_one(&old_conversation).await.unwrap();

        let converger = IncomingMessageConverger::new(
            messages.clone(),
            conversations.clone(),
            EventBus::new(),
            None,
        );
        let incoming = single_message("new-server", peer, peer, 2);
        let fresh = converger
            .converge_messages(current_user, vec![incoming], InboundSource::SyncReplay)
            .await
            .unwrap();

        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].conversation_id, canonical);
        assert_eq!(fresh[0].channel_id, peer);

        let moved = messages
            .get_by_conversation(&canonical, 0, 20)
            .await
            .unwrap();
        assert_eq!(moved.len(), 1);
        assert_eq!(moved[0].server_id, "old-server");
        assert!(
            messages
                .get_by_conversation(peer, 0, 20)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(conversations.get(peer).await.unwrap().is_none());
        assert!(conversations.get(&canonical).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn an_unprojected_stored_message_refreshes_status_and_stays_replayable() {
        let current_user = "me";
        let peer = "peer";
        let conversation_id = generate_single_chat_conversation_id(current_user, peer);
        let messages = Arc::new(MemoryMessageStore::new());
        let conversations = Arc::new(MemoryConversationStore::new());

        let mut local = single_message("server-1", &conversation_id, peer, 7);
        local.status = MessageStatus::Created as i32;
        messages.save_one(&local).await.unwrap();

        let mut incoming = single_message("server-1", &conversation_id, peer, 7);
        incoming.status = MessageStatus::Created as i32;
        let converger =
            IncomingMessageConverger::new(messages.clone(), conversations, EventBus::new(), None);

        let fresh = converger
            .converge_messages(current_user, vec![incoming], InboundSource::SyncReplay)
            .await
            .unwrap();

        assert_eq!(fresh.len(), 1, "stored alone does not mean projected");
        let stored = messages
            .get_by_message_ids(&["server-1".to_string()])
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].status, MessageStatus::Sent as i32);
    }

    #[tokio::test]
    async fn projected_duplicate_applies_redaction_without_receiving_or_unreading_again() {
        let conversation_id = generate_single_chat_conversation_id("me", "peer");
        let conversations = Arc::new(MemoryConversationStore::new());
        let messages = Arc::new(MemoryMessageStore::with_conversations(
            conversations.clone(),
        ));
        let mut local = single_message("server", &conversation_id, "peer", 7);
        local.encoded_content = crate::content::ContentBuilder::text("private")
            .build()
            .encode();
        local.is_read = true;
        messages.save_one(&local).await.unwrap();
        conversations
            .save_one(&Conversation {
                conversation_id: conversation_id.clone(),
                last_read_seq: 7,
                max_seq: 9,
                unread_count: 2,
                last_message_id: Some("server".into()),
                last_message_preview: Some("private".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let bus = EventBus::new();
        let mut events = bus.subscribe();
        let converger =
            IncomingMessageConverger::new(messages.clone(), conversations.clone(), bus, None);
        let mut snapshot = local.clone();
        snapshot.is_read = false;
        snapshot.encoded_content.clear();
        snapshot.retention_state = Some(flare_proto::common::MessageRetentionState {
            lifecycle: flare_proto::common::MessageRetentionLifecycle::Expired as i32,
            content_visibility: flare_proto::common::ContentVisibility::Redacted as i32,
            ..Default::default()
        });
        assert!(
            converger
                .converge_messages("me", vec![snapshot.clone()], InboundSource::SyncReplay)
                .await
                .unwrap()
                .is_empty()
        );
        let stored = messages.get("server").await.unwrap().unwrap();
        assert!(stored.encoded_content.is_empty());
        assert!(stored.is_read);
        assert_eq!(stored.retention_state, snapshot.retention_state);
        let conversation = conversations.get(&conversation_id).await.unwrap().unwrap();
        assert_eq!(conversation.unread_count, 2);
        assert!(conversation.last_message_preview.is_none());
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
                .await
                .unwrap(),
            Ok(crate::kernel::event::SdkEvent::Conversation(
                crate::kernel::event::ConversationEvent::Synced { .. }
            ))
        ));
        assert!(
            converger
                .converge_messages("me", vec![local], InboundSource::SyncReplay)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            messages
                .get("server")
                .await
                .unwrap()
                .unwrap()
                .encoded_content
                .is_empty()
        );
    }
}
