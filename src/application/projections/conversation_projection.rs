use crate::content::message_elem::MessagePreviewElem;
use crate::domain::conversation::id as conversation_id;
use crate::domain::{ConversationIdentityService, ReadPosition};
use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::event::{ConversationEvent, EventBus, SdkEvent};
use crate::model::conversation::ConversationType;
use crate::model::{Conversation, IMMessage};
use crate::shared::error::Result;

pub(crate) struct ConversationProjectionApplier {
    stores: StoreProvider,
    bus: EventBus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnreadApplyMode {
    RealtimePush,
    SyncReplay,
}

impl ConversationProjectionApplier {
    pub(crate) fn new(stores: StoreProvider, bus: EventBus) -> Self {
        Self { stores, bus }
    }

    pub(crate) async fn apply_messages(
        &self,
        messages: &[IMMessage],
        current_user_id: &str,
    ) -> Result<()> {
        self.apply_messages_with_mode(messages, current_user_id, UnreadApplyMode::RealtimePush)
            .await
    }

    pub(crate) async fn apply_synced_messages(
        &self,
        messages: &[IMMessage],
        current_user_id: &str,
    ) -> Result<()> {
        self.apply_messages_with_mode(messages, current_user_id, UnreadApplyMode::SyncReplay)
            .await
    }

    async fn apply_messages_with_mode(
        &self,
        messages: &[IMMessage],
        current_user_id: &str,
        unread_mode: UnreadApplyMode,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut previous_by_conversation: std::collections::HashMap<String, Option<Conversation>> =
            std::collections::HashMap::new();
        for message in messages {
            if message.conversation_id.trim().is_empty() {
                continue;
            }
            if previous_by_conversation.contains_key(&message.conversation_id) {
                continue;
            }
            let previous = self
                .stores
                .conversations
                .get(&message.conversation_id)
                .await
                .ok()
                .flatten();
            previous_by_conversation.insert(message.conversation_id.clone(), previous);
        }

        let mut latest_per_conversation: std::collections::HashMap<String, &IMMessage> =
            std::collections::HashMap::new();
        for message in messages {
            if message.conversation_id.trim().is_empty() {
                continue;
            }
            match latest_per_conversation.get(&message.conversation_id) {
                Some(previous)
                    if IMMessage::compare_for_latest_window_desc(previous, message).is_le() => {}
                _ => {
                    latest_per_conversation.insert(message.conversation_id.clone(), message);
                }
            }
        }

        for (conversation_id, latest) in latest_per_conversation {
            let previous = previous_by_conversation
                .get(&conversation_id)
                .cloned()
                .flatten();
            let previous_unread = previous.as_ref().map(|c| c.unread_count).unwrap_or(0);
            let previous_read = previous
                .as_ref()
                .map(ReadPosition::from_conversation)
                .unwrap_or_default()
                .normalize_for_unread_delta();
            self.ensure_or_repair_conversation_shell(latest, current_user_id)
                .await?;

            let previous_max_seq = previous
                .as_ref()
                .map(|conversation| conversation.max_seq)
                .unwrap_or(0);
            let previous_materialized_max_seq = self
                .stores
                .conversations
                .get_local_max_seq(&conversation_id)
                .await
                .unwrap_or(0);
            let should_update_summary =
                previous.is_none() || latest.conversation_seq >= previous_materialized_max_seq;
            let conversation_max_seq = previous
                .as_ref()
                .map(|conversation| conversation.max_seq.max(latest.conversation_seq))
                .unwrap_or(latest.conversation_seq);
            if should_update_summary {
                let _ = self
                    .stores
                    .conversations
                    .update_last_message(
                        &conversation_id,
                        latest.server_id(),
                        latest.sender_id(),
                        latest.display_time_ms(),
                        latest.text_for_storage().as_deref(),
                        latest.conversation_seq,
                    )
                    .await;
            }

            if !current_user_id.is_empty() {
                let unread_delta = self.unread_delta_for_conversation(
                    messages,
                    &conversation_id,
                    current_user_id,
                    previous_read,
                    unread_mode,
                );
                if unread_delta > 0 {
                    let next_unread = previous_unread.saturating_add(unread_delta);
                    let next_read_seq = Self::read_seq_for_unread_window(
                        previous_read,
                        conversation_max_seq,
                        next_unread,
                    );
                    let _ = self
                        .stores
                        .conversations
                        .update_unread(&conversation_id, next_unread, next_read_seq)
                        .await;
                    if let Ok(Some(updated)) = self.stores.conversations.get(&conversation_id).await
                        && updated.unread_count != previous_unread
                    {
                        self.bus.publish(SdkEvent::Conversation(
                            ConversationEvent::UnreadCountChanged {
                                conversation_id: conversation_id.clone(),
                                unread_count: updated.unread_count,
                            },
                        ));
                    }
                }
                let mention_delta = Self::mention_delta_for_conversation(
                    messages,
                    &conversation_id,
                    current_user_id,
                    previous_read,
                    unread_mode,
                );
                if mention_delta > 0
                    && let Ok(Some(mut updated)) =
                        self.stores.conversations.get(&conversation_id).await
                {
                    updated.mention_me = true;
                    updated.mention_count = updated.mention_count.saturating_add(mention_delta);
                    self.stores.conversations.save_one(&updated).await?;
                }
            } else {
                let is_new_message = latest.conversation_seq > previous_max_seq;
                let is_self_message = latest.sender_id() == current_user_id;
                if is_new_message && !is_self_message {
                    let last_read_seq = previous.as_ref().map(|c| c.last_read_seq).unwrap_or(0);
                    let unread_count = previous_unread.saturating_add(1);
                    let _ = self
                        .stores
                        .conversations
                        .update_unread(&conversation_id, unread_count, last_read_seq)
                        .await;
                }
            }
        }

        Ok(())
    }

    fn read_seq_for_unread_window(
        previous_read: ReadPosition,
        conversation_max_seq: u64,
        unread_count: u32,
    ) -> u64 {
        let read_upper_bound = conversation_max_seq.saturating_sub(unread_count as u64);
        previous_read.last_read_seq.min(read_upper_bound)
    }

    fn unread_delta_for_conversation(
        &self,
        messages: &[IMMessage],
        conversation_id: &str,
        current_user_id: &str,
        previous_read: ReadPosition,
        unread_mode: UnreadApplyMode,
    ) -> u32 {
        messages
            .iter()
            .filter(|message| message.conversation_id == conversation_id)
            .filter(|message| message.sender_id() != current_user_id)
            .filter(|message| !message.is_recalled)
            .filter(|message| Self::should_count_unread(message, previous_read, unread_mode))
            .count()
            .min(u32::MAX as usize) as u32
    }

    fn mention_delta_for_conversation(
        messages: &[IMMessage],
        conversation_id: &str,
        current_user_id: &str,
        previous_read: ReadPosition,
        unread_mode: UnreadApplyMode,
    ) -> u32 {
        messages
            .iter()
            .filter(|message| message.conversation_id == conversation_id)
            .filter(|message| message.sender_id() != current_user_id)
            .filter(|message| !message.is_recalled)
            .filter(|message| Self::should_count_unread(message, previous_read, unread_mode))
            .filter(|message| Self::mentions_current_user(message, current_user_id))
            .count()
            .min(u32::MAX as usize) as u32
    }

    fn mentions_current_user(message: &IMMessage, current_user_id: &str) -> bool {
        let current_user_id = current_user_id.trim();
        if current_user_id.is_empty() {
            return false;
        }
        message.mention_all
            || message
                .mention_users
                .iter()
                .any(|user_id| user_id.trim() == current_user_id)
    }

    fn should_count_unread(
        message: &IMMessage,
        previous_read: ReadPosition,
        unread_mode: UnreadApplyMode,
    ) -> bool {
        let accounted_through = previous_read
            .last_read_seq
            .saturating_add(previous_read.unread_count as u64);
        match unread_mode {
            UnreadApplyMode::SyncReplay => {
                message.conversation_seq > previous_read.last_read_seq
                    && message.conversation_seq > accounted_through
            }
            UnreadApplyMode::RealtimePush => {
                if message.conversation_seq > previous_read.last_read_seq
                    && message.conversation_seq > accounted_through
                {
                    return true;
                }

                // 历史 delivery/read ACK 混淆可能污染服务端摘要：max_seq/last_read_seq 已推进，
                // 但本地从未见过这条实时消息。对零未读的会话尾部新鲜消息做本地恢复。
                previous_read.unread_count == 0
                    && previous_read.max_seq > 0
                    && message.conversation_seq >= previous_read.max_seq
                    && message.conversation_seq >= previous_read.last_read_seq
            }
        }
    }

    async fn ensure_or_repair_conversation_shell(
        &self,
        message: &IMMessage,
        current_user_id: &str,
    ) -> Result<()> {
        let conversation_id = message.conversation_id.trim();
        if conversation_id.is_empty() || conversation_id.starts_with("sync:") {
            return Ok(());
        }
        if let Some(mut existing) = self.stores.conversations.get(conversation_id).await? {
            if Self::repair_existing_single_chat_channel(&mut existing, message, current_user_id) {
                self.stores.conversations.save_one(&existing).await?;
            }
            return Ok(());
        }

        let conversation_type = ConversationType::from_proto_int(message.conversation_type);
        let preview = message.text_for_storage().unwrap_or_default();
        let mut conversation = Conversation {
            conversation_id: conversation_id.to_string(),
            conversation_type,
            business_type: "chat".to_string(),
            channel_id: message.channel_id.clone(),
            display_name: String::new(),
            last_message_id: if message.server_id.trim().is_empty() {
                None
            } else {
                Some(message.server_id.clone())
            },
            last_sender_id: if message.sender_id.trim().is_empty() {
                None
            } else {
                Some(message.sender_id.clone())
            },
            last_message_at: Some(message.created_at),
            last_message_preview: if preview.is_empty() {
                None
            } else {
                Some(preview.clone())
            },
            last_message: Some(MessagePreviewElem {
                message_id: message.server_id.clone(),
                sender_id: message.sender_id.clone(),
                r#type: message.message_type,
                text: preview,
                time: message.created_at,
            }),
            max_seq: message.conversation_seq,
            updated_at: message.created_at,
            created_at: message.created_at,
            ..Default::default()
        };
        if conversation.channel_id.trim().is_empty()
            && conversation.conversation_type.is_single_chat_conversation()
        {
            let sender = message.sender_id.trim();
            let me = current_user_id.trim();
            if !sender.is_empty() && sender != me {
                conversation.channel_id = sender.to_string();
            }
        }
        ConversationIdentityService::repair_single_chat_channel(
            &mut conversation,
            current_user_id,
            None,
        );
        // 壳会话不带名字：消息里只有 id，名字由业务快照或会话摘要给，标题在读取时投影。
        self.stores.conversations.save_one(&conversation).await?;
        Ok(())
    }

    fn repair_existing_single_chat_channel(
        conversation: &mut Conversation,
        message: &IMMessage,
        current_user_id: &str,
    ) -> bool {
        let is_single = conversation.conversation_type.is_single_chat_conversation()
            || ConversationType::from_proto_int(message.conversation_type)
                .is_single_chat_conversation()
            || conversation_id::is_single_chat_conversation(&conversation.conversation_id);
        if !is_single {
            return false;
        }
        let sender = message.sender_id.trim();
        let me = current_user_id.trim();
        if sender.is_empty() || (!me.is_empty() && sender == me) {
            return false;
        }
        let previous_channel = conversation.channel_id.clone();
        if ConversationIdentityService::repair_single_chat_channel(
            conversation,
            current_user_id,
            Some(sender),
        ) {
            conversation.conversation_type = ConversationType::Single;
            // 修好 channel 后名字列里若是 id（旧 channel、本人或对端），清掉而不是换成另一个 id。
            conversation.display_name = crate::domain::business_name(
                &conversation.display_name,
                &[&previous_channel, me, sender, &conversation.conversation_id],
            );
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::ConversationProjectionApplier;
    use crate::domain::{
        ConversationReader, ConversationWriter, MessageReader, MessageStore, MessageWriter,
        SyncCursorReader, SyncCursorVo, SyncCursorWriter,
    };
    use crate::infrastructure::persistence::StoreProvider;
    use crate::kernel::event::{ConversationEvent, EventBus, SdkEvent};
    use crate::model::message::MessageLocalState;
    use crate::model::{Conversation, ConversationListQuery, IMMessage};
    use crate::shared::error::Result;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio::time::{Duration, timeout};

    struct MemoryConversationStore {
        data: RwLock<HashMap<String, Conversation>>,
        materialized_max_seq: RwLock<HashMap<String, u64>>,
    }

    impl MemoryConversationStore {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
                materialized_max_seq: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl ConversationReader for MemoryConversationStore {
        async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
            Ok(self.data.read().await.get(conversation_id).cloned())
        }

        async fn list(&self) -> Result<Vec<Conversation>> {
            Ok(self.data.read().await.values().cloned().collect())
        }
    }

    #[async_trait]
    impl ConversationWriter for MemoryConversationStore {
        async fn save_batch(&self, conversations: &[Conversation]) -> Result<()> {
            let mut data = self.data.write().await;
            for conversation in conversations {
                data.insert(conversation.conversation_id.clone(), conversation.clone());
            }
            Ok(())
        }

        async fn save_one(&self, conversation: &Conversation) -> Result<()> {
            self.save_batch(std::slice::from_ref(conversation)).await
        }

        async fn update_unread(
            &self,
            conversation_id: &str,
            unread_count: u32,
            last_read_seq: u64,
        ) -> Result<()> {
            let mut data = self.data.write().await;
            if let Some(conversation) = data.get_mut(conversation_id) {
                conversation.unread_count = unread_count;
                conversation.last_read_seq = last_read_seq;
                if unread_count == 0 {
                    conversation.mention_count = 0;
                    conversation.mention_me = false;
                }
            }
            Ok(())
        }

        async fn set_pinned(&self, _conversation_id: &str, _pinned: bool) -> Result<()> {
            Ok(())
        }

        async fn set_muted(&self, _conversation_id: &str, _muted: bool) -> Result<()> {
            Ok(())
        }

        async fn set_archived(&self, _conversation_id: &str, _archived: bool) -> Result<()> {
            Ok(())
        }

        async fn mark_unread(&self, _conversation_id: &str) -> Result<u32> {
            Ok(1)
        }

        async fn update_draft(&self, _conversation_id: &str, _draft: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _conversation_id: &str) -> Result<()> {
            self.materialized_max_seq
                .write()
                .await
                .remove(_conversation_id);
            Ok(())
        }

        async fn merge_conversation_identity(
            &self,
            from_conversation_id: &str,
            to_conversation_id: &str,
        ) -> Result<()> {
            let from = from_conversation_id.trim();
            let to = to_conversation_id.trim();
            if from.is_empty() || to.is_empty() || from == to {
                return Ok(());
            }
            let mut data = self.data.write().await;
            if let Some(mut conversation) = data.remove(from) {
                conversation.conversation_id = to.to_string();
                data.entry(to.to_string()).or_insert(conversation);
            }
            let mut materialized = self.materialized_max_seq.write().await;
            let from_seq = materialized.remove(from).unwrap_or(0);
            let to_seq = materialized.get(to).copied().unwrap_or(0);
            if from_seq > 0 || to_seq > 0 {
                materialized.insert(to.to_string(), from_seq.max(to_seq));
            }
            Ok(())
        }

        async fn clear_local_chat_history(
            &self,
            _conversation_id: &str,
            _cleared_through_seq: u64,
        ) -> Result<()> {
            Ok(())
        }

        async fn update_last_message(
            &self,
            conversation_id: &str,
            last_message_id: &str,
            last_sender_id: &str,
            last_message_at: u64,
            last_message_preview: Option<&str>,
            max_seq: u64,
        ) -> Result<()> {
            let mut data = self.data.write().await;
            if let Some(conversation) = data.get_mut(conversation_id) {
                conversation.last_message_id = Some(last_message_id.to_string());
                conversation.last_sender_id = Some(last_sender_id.to_string());
                conversation.last_message_at = Some(last_message_at);
                conversation.last_message_preview =
                    last_message_preview.map(std::string::ToString::to_string);
                conversation.max_seq = conversation.max_seq.max(max_seq);
            }
            let mut materialized = self.materialized_max_seq.write().await;
            let previous = materialized.get(conversation_id).copied().unwrap_or(0);
            materialized.insert(conversation_id.to_string(), previous.max(max_seq));
            Ok(())
        }

        async fn recompute_unread_for_user(
            &self,
            conversation_id: &str,
            current_user_id: &str,
        ) -> Result<()> {
            let mut data = self.data.write().await;
            if let Some(conversation) = data.get_mut(conversation_id) {
                let is_self_message = conversation
                    .last_sender_id
                    .as_deref()
                    .map(|sender| sender == current_user_id)
                    .unwrap_or(false);
                conversation.unread_count = if is_self_message
                    || conversation.max_seq <= conversation.last_read_seq
                {
                    0
                } else {
                    (conversation
                        .max_seq
                        .saturating_sub(conversation.last_read_seq)) as u32
                };
            }
            Ok(())
        }

        async fn get_local_max_seq(&self, conversation_id: &str) -> Result<u64> {
            Ok(self
                .materialized_max_seq
                .read()
                .await
                .get(conversation_id)
                .copied()
                .unwrap_or(0))
        }
    }

    struct NoopMessageStore;
    struct NoopSyncCursorStore;

    #[async_trait]
    impl MessageReader for NoopMessageStore {
        async fn get(&self, _message_id: &str) -> Result<Option<IMMessage>> {
            Ok(None)
        }

        async fn get_by_client_msg_id(&self, _client_msg_id: &str) -> Result<Option<IMMessage>> {
            Ok(None)
        }

        async fn get_by_conversation(
            &self,
            _conversation_id: &str,
            _before_seq: u64,
            _limit: u32,
        ) -> Result<Vec<IMMessage>> {
            Ok(Vec::new())
        }

        async fn search(&self, _keyword: &str, _limit: u32) -> Result<Vec<IMMessage>> {
            Ok(Vec::new())
        }

        async fn search_in_conversation(
            &self,
            _conversation_id: &str,
            _keyword: &str,
            _limit: u32,
        ) -> Result<Vec<IMMessage>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl MessageWriter for NoopMessageStore {
        async fn save_batch(&self, _messages: &[IMMessage]) -> Result<()> {
            Ok(())
        }

        async fn save_one(&self, _message: &IMMessage) -> Result<()> {
            Ok(())
        }

        async fn update_status(&self, _message_id: &str, _status: i32) -> Result<u64> {
            Ok(0)
        }

        async fn update_content(&self, _message_id: &str, _new_content: Vec<u8>) -> Result<bool> {
            Ok(false)
        }

        async fn delete(&self, _message_id: &str) -> Result<()> {
            Ok(())
        }

        async fn rewrite_conversation_id(
            &self,
            _from_conversation_id: &str,
            _to_conversation_id: &str,
        ) -> Result<u64> {
            Ok(0)
        }

        async fn update_after_ack(&self, _client_msg_id: &str, _message: &IMMessage) -> Result<()> {
            Ok(())
        }
    }

    impl MessageStore for NoopMessageStore {}

    #[async_trait]
    impl SyncCursorReader for NoopSyncCursorStore {
        async fn get_conversation_cursor(
            &self,
            _user_id: &str,
            _conversation_id: &str,
        ) -> Result<Option<SyncCursorVo>> {
            Ok(None)
        }

        async fn get_raw(&self, _key: &str) -> Result<Option<String>> {
            Ok(None)
        }
    }

    #[async_trait]
    impl SyncCursorWriter for NoopSyncCursorStore {
        async fn save_conversation_cursor(&self, _cursor: &SyncCursorVo) -> Result<()> {
            Ok(())
        }

        async fn save_raw(&self, _key: &str, _cursor: &str) -> Result<()> {
            Ok(())
        }
    }

    fn stores() -> (Arc<MemoryConversationStore>, StoreProvider) {
        let conversations = Arc::new(MemoryConversationStore::new());
        let store_provider = StoreProvider {
            plugin_commands: None,
            messages: Arc::new(NoopMessageStore),
            conversations: conversations.clone(),
            conversation_participants: None,
            cursors: Arc::new(NoopSyncCursorStore),
            timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
            pending_send_reader: None,
            pending_send_writer: None,
            upload_manifest_store: None,
            media_cache_store: None,
            media_cache_admin: None,
            user_file_download_store: None,
            user_profiles_reader: None,
            user_profiles_writer: None,
        };
        (conversations, store_provider)
    }

    fn conversation_summary(max_seq: u64, last_read_seq: u64, unread_count: u32) -> Conversation {
        Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq,
            last_read_seq,
            unread_count,
            ..Default::default()
        }
    }

    fn text_message(server_id: &str, conversation_seq: u64, text: &str) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = server_id.to_string();
        message.client_msg_id = format!("client-{server_id}");
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = conversation_seq;
        message.created_at = conversation_seq;
        message.client_created_at = conversation_seq;
        message.content = Some(crate::model::Elem::Text(
            crate::content::message_elem::TextElem {
                text: text.to_string(),
                mentions: Vec::new(),
            },
        ));
        message.materialize_encoded_content_from_elem();
        message
    }

    fn pending_text_message(client_msg_id: &str, sort_ts: u64, text: &str) -> IMMessage {
        let mut message = text_message("", 0, text);
        message.client_msg_id = client_msg_id.to_string();
        message.sender_id = "u1".to_string();
        message.created_at = sort_ts.saturating_sub(10);
        message.client_created_at = sort_ts;
        message.local_state = MessageLocalState {
            sending: true,
            failed: false,
            is_local: true,
            uploading: false,
            upload_progress: 0,
            sort_ts,
        };
        message
    }

    #[tokio::test]
    async fn remote_message_updates_unread_once_and_publishes_change() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let applier = ConversationProjectionApplier::new(stores, bus.clone());

        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-1".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = 5;

        applier.apply_messages(&[message], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 1);

        let event = timeout(Duration::from_millis(200), receiver.recv())
            .await
            .expect("expected unread event")
            .expect("bus closed");
        match event {
            SdkEvent::Conversation(ConversationEvent::UnreadCountChanged {
                conversation_id,
                unread_count,
            }) => {
                assert_eq!(conversation_id, "conv-1");
                assert_eq!(unread_count, 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let second = timeout(Duration::from_millis(80), receiver.recv()).await;
        assert!(second.is_err(), "should publish unread change only once");
    }

    #[tokio::test]
    async fn remote_mention_updates_conversation_projection() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);

        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-mention".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = 5;
        message.mention_users = vec!["u1".to_string()];

        applier.apply_messages(&[message], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 1);
        assert_eq!(updated.mention_count, 1);
        assert!(updated.mention_me);

        let mentioned = conversations
            .list_by_query(&ConversationListQuery {
                mention_me_only: true,
                ..ConversationListQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(mentioned.len(), 1);
        assert_eq!(mentioned[0].conversation_id, "conv-1");

        conversations.update_unread("conv-1", 0, 5).await.unwrap();
        let cleared = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(cleared.unread_count, 0);
        assert_eq!(cleared.mention_count, 0);
        assert!(!cleared.mention_me);
        let mentioned = conversations
            .list_by_query(&ConversationListQuery {
                mention_me_only: true,
                ..ConversationListQuery::default()
            })
            .await
            .unwrap();
        assert!(mentioned.is_empty());
    }

    #[tokio::test]
    async fn self_message_does_not_increment_unread() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let applier = ConversationProjectionApplier::new(stores, bus);

        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-2".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u1".to_string();
        message.conversation_seq = 7;

        applier.apply_messages(&[message], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 0);
        let event = timeout(Duration::from_millis(80), receiver.recv()).await;
        assert!(event.is_err(), "self message should not emit unread change");
    }

    #[tokio::test]
    async fn higher_seq_wins_for_last_message_projection() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);

        let mut older = IMMessage::new(flare_proto::common::Message::default());
        older.server_id = "server-old".to_string();
        older.conversation_id = "conv-1".to_string();
        older.sender_id = "u2".to_string();
        older.conversation_seq = 2;

        let mut newer = IMMessage::new(flare_proto::common::Message::default());
        newer.server_id = "server-new".to_string();
        newer.conversation_id = "conv-1".to_string();
        newer.sender_id = "u2".to_string();
        newer.conversation_seq = 9;

        applier.apply_messages(&[older, newer], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.last_message_id.as_deref(), Some("server-new"));
        assert_eq!(updated.max_seq, 9);
        assert_eq!(updated.unread_count, 2);
    }

    #[tokio::test]
    async fn later_local_pending_message_wins_last_message_projection_when_seq_is_zero() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);

        let first = pending_text_message("client-first", 1_000, "first pending");
        let second = pending_text_message("client-second", 2_000, "second pending");

        applier
            .apply_messages(&[first, second], "u1")
            .await
            .unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert!(
            updated
                .last_message_preview
                .as_deref()
                .unwrap_or_default()
                .contains("second pending")
        );
        assert_eq!(updated.last_message_at, Some(2_000));
        assert_eq!(updated.max_seq, 0);
        assert_eq!(updated.unread_count, 0);
    }

    #[tokio::test]
    async fn projected_preview_uses_materialized_message_waterline_not_remote_summary_max_seq() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(2, 0, 2);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);

        applier
            .apply_synced_messages(&[text_message("server-1", 1, "first preview")], "u1")
            .await
            .unwrap();

        let first = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(first.max_seq, 2);
        assert_eq!(first.last_message_id.as_deref(), Some("server-1"));
        assert!(
            first
                .last_message_preview
                .as_deref()
                .unwrap_or_default()
                .contains("first preview")
        );
        assert_eq!(conversations.get_local_max_seq("conv-1").await.unwrap(), 1);

        applier
            .apply_synced_messages(&[text_message("server-2", 2, "second preview")], "u1")
            .await
            .unwrap();

        let second = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(second.max_seq, 2);
        assert_eq!(second.last_message_id.as_deref(), Some("server-2"));
        assert!(
            second
                .last_message_preview
                .as_deref()
                .unwrap_or_default()
                .contains("second preview")
        );
        assert_eq!(conversations.get_local_max_seq("conv-1").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn incoming_single_message_repairs_existing_wrong_channel_id() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);

        conversations
            .save_one(&Conversation {
                conversation_id: "1A-single".to_string(),
                conversation_type: crate::model::conversation::ConversationType::Single,
                channel_id: "me".to_string(),
                display_name: "me".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-peer".to_string();
        message.conversation_id = "1A-single".to_string();
        message.conversation_type =
            crate::model::conversation::ConversationType::Single.to_proto_int();
        message.sender_id = "peer".to_string();
        message.conversation_seq = 1;
        message.created_at = 12_345;

        applier.apply_messages(&[message], "me").await.unwrap();

        let updated = conversations.get("1A-single").await.unwrap().unwrap();
        assert_eq!(updated.channel_id, "peer");
        assert_eq!(
            updated.conversation_type,
            crate::model::conversation::ConversationType::Single
        );
        assert_eq!(
            updated.display_name, "",
            "the repaired name column must not hold an id"
        );
    }

    /// 首条消息建出的壳会话不带名字：消息里只有 id。
    #[tokio::test]
    async fn first_message_shell_conversations_store_no_name() {
        use crate::model::conversation::ConversationType;
        let (conversations, stores) = stores();
        let applier = ConversationProjectionApplier::new(stores, EventBus::new());
        let message = |conversation_id: &str, kind: ConversationType, channel: &str| {
            let mut message = IMMessage::new(flare_proto::common::Message::default());
            message.server_id = format!("server-{conversation_id}");
            message.conversation_id = conversation_id.to_string();
            message.conversation_type = kind.to_proto_int();
            message.channel_id = channel.to_string();
            message.sender_id = "peer".to_string();
            message.sender_name = "Peer at send time".to_string();
            message.conversation_seq = 1;
            message.created_at = 1_000;
            message
        };
        applier
            .apply_messages(
                &[
                    message("1A-new", ConversationType::Single, "peer"),
                    message("1A-blank", ConversationType::Single, ""),
                    message("2A-new", ConversationType::Group, "g1"),
                ],
                "me",
            )
            .await
            .unwrap();
        for conversation_id in ["1A-new", "1A-blank", "2A-new"] {
            let created = conversations.get(conversation_id).await.unwrap().unwrap();
            assert_eq!(created.display_name, "", "{conversation_id}");
        }
    }

    #[tokio::test]
    async fn realtime_message_recovers_unread_when_summary_advanced_with_zero_unread() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(5, 4, 0);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-5".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = 5;

        applier.apply_messages(&[message], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 1);
        assert_eq!(updated.last_read_seq, 4);
    }

    #[tokio::test]
    async fn realtime_message_recovers_unread_when_read_seq_was_polluted_to_tail() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(5, 5, 0);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-5".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = 5;

        applier.apply_messages(&[message], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 1);
        assert_eq!(updated.last_read_seq, 4);
    }

    #[tokio::test]
    async fn realtime_messages_keep_incrementing_after_read_seq_tail_repair() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(5, 5, 0);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut first = IMMessage::new(flare_proto::common::Message::default());
        first.server_id = "server-5".to_string();
        first.conversation_id = "conv-1".to_string();
        first.sender_id = "u2".to_string();
        first.conversation_seq = 5;

        let mut second = IMMessage::new(flare_proto::common::Message::default());
        second.server_id = "server-6".to_string();
        second.conversation_id = "conv-1".to_string();
        second.sender_id = "u2".to_string();
        second.conversation_seq = 6;

        applier.apply_messages(&[first], "u1").await.unwrap();
        applier.apply_messages(&[second], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 2);
        assert_eq!(updated.last_read_seq, 4);
        assert_eq!(updated.max_seq, 6);
    }

    #[tokio::test]
    async fn realtime_message_does_not_double_count_when_summary_already_has_unread() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(5, 4, 1);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-5".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = 5;

        applier.apply_messages(&[message], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 1);
        assert_eq!(updated.last_read_seq, 4);
    }

    #[tokio::test]
    async fn realtime_messages_increment_after_summary_advanced_beyond_unread_window() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(8, 4, 1);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut messages = Vec::new();
        for seq in 6..=8 {
            let mut message = IMMessage::new(flare_proto::common::Message::default());
            message.server_id = format!("server-{seq}");
            message.conversation_id = "conv-1".to_string();
            message.sender_id = "u2".to_string();
            message.conversation_seq = seq;
            messages.push(message);
        }

        applier.apply_messages(&messages, "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 4);
        assert_eq!(updated.last_read_seq, 4);
    }

    #[tokio::test]
    async fn sync_replay_heals_under_counted_summary_with_fresh_messages() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(8, 4, 1);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut messages = Vec::new();
        for seq in 6..=8 {
            let mut message = IMMessage::new(flare_proto::common::Message::default());
            message.server_id = format!("server-{seq}");
            message.conversation_id = "conv-1".to_string();
            message.sender_id = "u2".to_string();
            message.conversation_seq = seq;
            messages.push(message);
        }

        applier
            .apply_synced_messages(&messages, "u1")
            .await
            .unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 4);
        assert_eq!(updated.last_read_seq, 4);
    }

    #[tokio::test]
    async fn sync_replay_preserves_server_summary_unread_inside_known_tail() {
        let (conversations, stores) = stores();
        let summary = conversation_summary(10, 7, 3);
        conversations.save_one(&summary).await.unwrap();

        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "server-8".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.conversation_seq = 8;

        applier
            .apply_synced_messages(&[message], "u1")
            .await
            .unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.unread_count, 3);
        assert_eq!(updated.last_read_seq, 7);
    }

    #[tokio::test]
    async fn older_message_arriving_later_does_not_roll_back_last_message() {
        let (conversations, stores) = stores();
        let bus = EventBus::new();
        let applier = ConversationProjectionApplier::new(stores, bus);

        let mut newer = IMMessage::new(flare_proto::common::Message::default());
        newer.server_id = "server-new".to_string();
        newer.conversation_id = "conv-1".to_string();
        newer.sender_id = "u2".to_string();
        newer.conversation_seq = 9;

        let mut older = IMMessage::new(flare_proto::common::Message::default());
        older.server_id = "server-old".to_string();
        older.conversation_id = "conv-1".to_string();
        older.sender_id = "u2".to_string();
        older.conversation_seq = 2;

        applier.apply_messages(&[newer], "u1").await.unwrap();
        applier.apply_messages(&[older], "u1").await.unwrap();

        let updated = conversations.get("conv-1").await.unwrap().unwrap();
        assert_eq!(updated.last_message_id.as_deref(), Some("server-new"));
        assert_eq!(updated.max_seq, 9);
    }
}
