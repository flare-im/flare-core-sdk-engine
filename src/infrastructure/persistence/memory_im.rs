//! 浏览器 / 联调用内存 IM 仓储（Message + Conversation + PendingSend）。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::StoreProvider;
use super::empty_stores::MemorySyncCursorStore;
use super::memory::{MemoryPendingSendStore, MemoryUserProfileStore};
#[path = "memory_message_rows.rs"]
mod memory_message_rows;
use crate::domain::{
    ConversationReader, ConversationWriter, EditApplyResult, MessageReader, MessageStore,
    MessageWriter, OperationApplyResult, RetentionEventKind, advance_message_event,
    apply_retention_event, encode_plugin_message_state, enforce_retention_visibility,
    mark_outgoing_message_read, merge_incoming_conversation_summary, merge_message_event_state,
    message_attribute_seq, retention_hides_content, validate_plugin_shared_state,
    write_message_mark, write_plugin_message_state, write_plugin_shared_state,
};
use crate::model::Conversation;
use crate::model::IMMessage;
use crate::model::message::ReactionEntry;
use crate::model::{decode_content_bytes, decoded_content_to_elem};
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_plugin_protocol::state::{SharedState, SharedStateOrder};
use flare_plugin_protocol::ui::PluginMessageState;
use flare_proto::common::{MessageRetentionPolicy, MessageRetentionState, MessageStatus};
use memory_message_rows::MemoryMessageRows;

pub struct MemoryMessageStore {
    data: Arc<RwLock<MemoryMessageRows>>,
    conversations: Arc<MemoryConversationStore>,
}

impl MemoryMessageStore {
    pub fn new() -> Self {
        Self::with_conversations(Arc::new(MemoryConversationStore::new()))
    }

    pub fn with_conversations(conversations: Arc<MemoryConversationStore>) -> Self {
        Self {
            data: conversations.messages.clone(),
            conversations,
        }
    }

    async fn clear_retained_preview(&self, message: &IMMessage) {
        if !message
            .retention_state
            .as_ref()
            .is_some_and(retention_hides_content)
        {
            return;
        }
        let mut conversations = self.conversations.data.write().await;
        if let Some(conversation) = conversations.get_mut(&message.conversation_id) {
            clear_retained_conversation_preview(conversation, message);
        }
    }

    /// Copies only rows that need persistence; the caller owns write serialization.
    pub async fn outgoing_read_updates(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        read_seq: u64,
    ) -> Vec<IMMessage> {
        let data = self.data.read().await;
        data.conversation(conversation_id.trim())
            .filter(|message| {
                message.sender_id == sender_user_id.trim()
                    && message.conversation_seq > 0
                    && message.conversation_seq <= read_seq
            })
            .filter_map(|message| {
                if message.is_read && message.status != MessageStatus::Created as i32 {
                    return None;
                }
                let mut message = message.clone();
                mark_outgoing_message_read(&mut message, conversation_id, sender_user_id, read_seq)
                    .then_some(message)
            })
            .collect()
    }

    fn update_message_content(message: &mut IMMessage, new_content: Vec<u8>) {
        message.encoded_content = new_content;
        message.is_edited = true;
        let decoded = decode_content_bytes(&message.encoded_content).ok();
        message.text_preview = decoded
            .as_ref()
            .map(|content| content.text_preview())
            .unwrap_or_default();
        message.content = decoded.as_ref().and_then(decoded_content_to_elem);
        enforce_retention_visibility(message);
    }

    fn merged_outgoing(
        data: &MemoryMessageRows,
        client_msg_id: &str,
        message: &IMMessage,
    ) -> IMMessage {
        let mut message = message.clone();
        let ack_client_msg_id = client_msg_id.trim();
        if message.client_msg_id.trim().is_empty() && !ack_client_msg_id.is_empty() {
            message.client_msg_id = ack_client_msg_id.to_string();
        }
        // 整行替换乐观写入行；本地插件卡片状态随行带过去。
        if let Some(stored) = data
            .get(&message.server_id)
            .filter(|stored| !message.server_id.is_empty() && stored.server_id == message.server_id)
        {
            merge_message_event_state(&mut message, stored);
        }
        let stored_client_id = message.client_msg_id.clone();
        let other_id = (stored_client_id != ack_client_msg_id).then_some(stored_client_id.as_str());
        let candidates = data
            .client_rows(ack_client_msg_id)
            .chain(other_id.into_iter().flat_map(|id| data.client_rows(id)));
        for (stored_key, stored) in candidates {
            if !message.server_id.is_empty()
                && stored_key == message.server_id
                && stored.server_id == message.server_id
            {
                continue;
            }
            if stored.sender_id == message.sender_id
                && stored.conversation_id == message.conversation_id
            {
                merge_message_event_state(&mut message, stored);
            }
        }
        enforce_retention_visibility(&mut message);
        message
    }

    fn storage_key(message: &IMMessage) -> String {
        if !message.server_id.is_empty() {
            message.server_id.clone()
        } else {
            message.client_msg_id.clone()
        }
    }

    /// Fill gaps from a durable read without reverting changes already applied in this store.
    #[doc(hidden)]
    pub async fn restore_missing_messages(&self, messages: Vec<IMMessage>) {
        if messages.is_empty() {
            return;
        }
        let mut data = self.data.write().await;
        for mut message in messages {
            let key = Self::storage_key(&message);
            if data.get(&key).is_none() {
                enforce_retention_visibility(&mut message);
                data.insert(key, message);
            }
        }
    }

    #[doc(hidden)]
    pub async fn missing_message_ids(&self, ids: &[String]) -> Vec<String> {
        let data = self.data.read().await;
        ids.iter()
            .filter(|id| data.get(id).is_none())
            .cloned()
            .collect()
    }

    fn remove_conflicting_client_msg_rows(
        data: &mut MemoryMessageRows,
        client_msg_id: &str,
        keep_key: &str,
        conversation_id: &str,
        sender_id: &str,
    ) {
        data.remove_client_rows(client_msg_id, |key, stored| {
            key != keep_key
                && stored.conversation_id == conversation_id
                && stored.sender_id == sender_id
        });
    }

    fn remove_conflicting_local_client_msg_rows(
        data: &mut MemoryMessageRows,
        client_msg_id: &str,
        keep_key: &str,
        conversation_id: &str,
        sender_id: &str,
    ) {
        let client_msg_id = client_msg_id.trim();
        if client_msg_id.is_empty() {
            return;
        }
        data.remove_client_rows(client_msg_id, |key, stored| {
            key != keep_key
                && stored.conversation_id == conversation_id
                && stored.sender_id == sender_id
                && stored.local_state.is_local
                && !stored.local_state.failed
                && (stored.local_state.sending || stored.local_state.uploading)
        });
    }

    fn get_mut_by_any_message_id<'a>(
        data: &'a mut MemoryMessageRows,
        message_id: &str,
    ) -> Option<&'a mut IMMessage> {
        if data.get(message_id).is_some() {
            return data.get_mut(message_id);
        }
        let key = {
            let mut matches = data.client_rows(message_id);
            let key = matches.next()?.0;
            matches.next().is_none().then(|| key.to_owned())?
        };
        data.get_mut(&key)
    }
    fn get_mut_by_server_or_local_id<'a>(
        data: &'a mut MemoryMessageRows,
        message_id: &str,
    ) -> Option<&'a mut IMMessage> {
        Self::get_mut_by_any_message_id(data, message_id)
            .filter(|message| message.server_id == message_id || message.local_state.is_local)
    }
}

impl Default for MemoryMessageStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageReader for MemoryMessageStore {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        Ok(self.data.read().await.get(message_id).cloned())
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        let data = self.data.read().await;
        let mut matches = data.client_rows(client_msg_id);
        let message = matches.next().map(|(_, row)| row);
        Ok(if matches.next().is_none() {
            message.cloned()
        } else {
            None
        })
    }

    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let data = self.data.read().await;
        let bound = if before_seq == 0 {
            u64::MAX
        } else {
            before_seq
        };
        let is_latest = before_seq == 0 || before_seq >= i64::MAX as u64;
        let mut rows: Vec<_> = data
            .conversation(conversation_id)
            .filter(|message| {
                message.conversation_seq < bound && (is_latest || message.conversation_seq > 0)
            })
            .collect();
        let compare = |left: &&IMMessage, right: &&IMMessage| {
            if is_latest {
                IMMessage::compare_for_latest_window_desc(left, right)
            } else {
                right.conversation_seq.cmp(&left.conversation_seq)
            }
        };
        let limit = limit as usize;
        // IndexedDB's hot window uses this store. Clone and order only the rows the caller shows.
        if rows.len() > limit {
            rows.select_nth_unstable_by(limit, compare);
            rows.truncate(limit);
        }
        rows.sort_by(compare);
        Ok(rows.into_iter().cloned().collect())
    }

    async fn get_by_conversation_after(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let data = self.data.read().await;
        let mut msgs: Vec<_> = data
            .conversation(conversation_id)
            .filter(|m| m.conversation_seq > after_seq)
            .collect();
        let limit = limit as usize;
        if msgs.len() > limit {
            msgs.select_nth_unstable_by_key(limit, |m| m.conversation_seq);
            msgs.truncate(limit);
        }
        msgs.sort_by_key(|m| m.conversation_seq);
        Ok(msgs.into_iter().cloned().collect())
    }

    async fn oldest_conversation_seq(&self, conversation_id: &str) -> Result<Option<u64>> {
        Ok(self
            .data
            .read()
            .await
            .conversation(conversation_id)
            .filter(|message| message.conversation_seq > 0)
            .map(|message| message.conversation_seq)
            .min())
    }

    async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.search_by_query(&crate::model::MessageSearchQuery::text(keyword, limit))
            .await
    }

    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.search_by_query(&crate::model::MessageSearchQuery::in_conversation(
            conversation_id,
            keyword,
            limit,
        ))
        .await
    }

    async fn search_by_query(
        &self,
        query: &crate::model::MessageSearchQuery,
    ) -> Result<Vec<IMMessage>> {
        self.search_with_history_floors(query, &HashMap::new())
            .await
    }
}

impl MemoryMessageStore {
    /// Search with visibility enforced before applying the requested limit.
    pub async fn search_visible(
        &self,
        query: &crate::model::MessageSearchQuery,
        conversations: &dyn ConversationReader,
    ) -> Result<Vec<IMMessage>> {
        let scope = query
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty());
        let rows = match scope {
            Some(id) => conversations.get_stored_many(&[id.to_owned()]).await?,
            None => conversations.list().await?,
        };
        let floors = rows
            .into_iter()
            .filter_map(|conversation| {
                let floor = crate::domain::sync_visibility_floor(&conversation);
                (floor > 0).then_some((conversation.conversation_id, floor))
            })
            .collect();
        self.search_with_history_floors(query, &floors).await
    }

    async fn search_with_history_floors(
        &self,
        query: &crate::model::MessageSearchQuery,
        floors: &HashMap<String, u64>,
    ) -> Result<Vec<IMMessage>> {
        use super::search::{
            elem_search_text, message_type_values_for_search, search_text_for_content_bytes,
        };
        let keyword = query.normalized_keyword();
        let types = message_type_values_for_search(&query.kinds);
        let time = |m: &IMMessage| {
            if m.created_at > 0 {
                m.created_at
            } else if m.client_created_at > 0 {
                m.client_created_at
            } else {
                m.local_state.sort_ts
            }
        };
        let data = self.data.read().await;
        let scope = query
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty());
        let messages: Box<dyn Iterator<Item = &IMMessage> + Send> = match scope {
            Some(id) => Box::new(data.conversation(id)),
            None => Box::new(data.values()),
        };
        let mut results: Vec<_> = messages
            .filter(|m| {
                let floor = floors.get(&m.conversation_id).copied().unwrap_or(0);
                if !crate::domain::message_visible_after_clear(m, floor) {
                    return false;
                }
                if !query.include_recalled && m.is_recalled {
                    return false;
                }
                if query
                    .sender_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .is_some_and(|id| m.sender_id != id)
                {
                    return false;
                }
                if !types.is_empty() && !types.contains(&m.message_type) {
                    return false;
                }
                if query.from_time.is_some_and(|t| time(m) < t)
                    || query.to_time.is_some_and(|t| time(m) > t)
                {
                    return false;
                }
                keyword.as_ref().is_none_or(|kw| {
                    m.content
                        .as_ref()
                        .and_then(elem_search_text)
                        .or_else(|| search_text_for_content_bytes(&m.encoded_content))
                        .or_else(|| m.text_for_storage())
                        .is_some_and(|text| text.to_lowercase().contains(kw))
                        || m.attributes
                            .get("contentText")
                            .is_some_and(|text| text.to_lowercase().contains(kw))
                })
            })
            .collect();
        let compare = |a: &&IMMessage, b: &&IMMessage| {
            time(b)
                .cmp(&time(a))
                .then_with(|| b.conversation_seq.cmp(&a.conversation_seq))
                .then_with(|| a.server_id.cmp(&b.server_id))
        };
        let limit = query.normalized_limit() as usize;
        if results.len() > limit {
            results.select_nth_unstable_by(limit, compare);
            results.truncate(limit);
        }
        results.sort_by(compare);
        Ok(results.into_iter().cloned().collect())
    }
}

#[async_trait]
impl MessageWriter for MemoryMessageStore {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        let mut data = self.data.write().await;
        for msg in messages {
            let key = Self::storage_key(msg);
            let existing = data
                .get(&msg.server_id)
                .filter(|stored| {
                    !msg.server_id.trim().is_empty() && stored.server_id == msg.server_id
                })
                .or_else(|| {
                    data.client_rows(&msg.client_msg_id)
                        .map(|(_, row)| row)
                        .find(|stored| {
                            !msg.client_msg_id.trim().is_empty()
                                && stored.conversation_id == msg.conversation_id
                                && stored.sender_id == msg.sender_id
                                && stored.local_state.is_local
                        })
                });
            let mut next = msg.clone();
            if let Some(existing) = existing {
                merge_message_event_state(&mut next, existing);
            }
            enforce_retention_visibility(&mut next);
            Self::remove_conflicting_local_client_msg_rows(
                &mut data,
                &msg.client_msg_id,
                &key,
                &msg.conversation_id,
                &msg.sender_id,
            );
            self.clear_retained_preview(&next).await;
            data.insert(key, next);
        }
        Ok(())
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        MessageWriter::save_batch(self, std::slice::from_ref(message)).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id) else {
            return Ok(0);
        };
        message.status = status;
        if status == MessageStatus::Recalled as i32 {
            message.is_recalled = true;
        }
        Ok(1)
    }

    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool> {
        Ok(self.apply_edit_event(message_id, new_content, None).await? == EditApplyResult::Applied)
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id) {
            let key = Self::storage_key(message);
            data.remove(&key);
        }
        Ok(())
    }

    async fn rewrite_conversation_id(
        &self,
        from_conversation_id: &str,
        to_conversation_id: &str,
    ) -> Result<u64> {
        let from = from_conversation_id.trim();
        let to = to_conversation_id.trim();
        if from.is_empty() || to.is_empty() || from == to {
            return Ok(0);
        }
        Ok(self.data.write().await.move_conversation(from, to))
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        let mut data = self.data.write().await;
        let message = Self::merged_outgoing(&data, client_msg_id, message);
        let key = Self::storage_key(&message);
        let ack_client_msg_id = client_msg_id.trim();
        Self::remove_conflicting_client_msg_rows(
            &mut data,
            ack_client_msg_id,
            &key,
            &message.conversation_id,
            &message.sender_id,
        );
        Self::remove_conflicting_client_msg_rows(
            &mut data,
            &message.client_msg_id,
            &key,
            &message.conversation_id,
            &message.sender_id,
        );
        self.clear_retained_preview(&message).await;
        data.insert(key, message);
        Ok(())
    }
}

#[async_trait]
impl MessageStore for MemoryMessageStore {
    async fn apply_edit_event(
        &self,
        message_id: &str,
        new_content: Vec<u8>,
        event_seq: Option<u64>,
    ) -> Result<EditApplyResult> {
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id.trim())
        else {
            return Ok(EditApplyResult::NotFound);
        };
        if !advance_message_event(&mut message.attributes, "lastEditEventSeq", event_seq) {
            return Ok(EditApplyResult::IgnoredStale);
        }
        Self::update_message_content(message, new_content);
        let version =
            message_attribute_seq(&message.attributes, "currentEditVersion").saturating_add(1);
        message
            .attributes
            .insert("currentEditVersion".into(), version.to_string());
        message
            .attributes
            .insert("messageFsmState".into(), "EDITED".into());
        Ok(EditApplyResult::Applied)
    }

    async fn apply_mark_event(
        &self,
        message_id: &str,
        mark_type: i32,
        color: Option<&str>,
        set_mark: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id.trim())
        else {
            return Ok(OperationApplyResult::NotFound);
        };
        if !advance_message_event(&mut message.attributes, "lastMarkEventSeq", event_seq) {
            return Ok(OperationApplyResult::IgnoredStale);
        }
        write_message_mark(&mut message.attributes, mark_type, color, set_mark);
        Ok(OperationApplyResult::Applied)
    }

    async fn set_message_mark(
        &self,
        message_id: &str,
        mark_type: i32,
        color: Option<&str>,
    ) -> Result<()> {
        self.apply_mark_event(message_id, mark_type, color, true, None)
            .await?;
        Ok(())
    }

    async fn clear_message_mark(&self, message_id: &str, mark_type: i32) -> Result<()> {
        self.apply_mark_event(message_id, mark_type, None, false, None)
            .await?;
        Ok(())
    }

    async fn apply_retention_scheduled_event(
        &self,
        message_id: &str,
        policy: &MessageRetentionPolicy,
        state: &MessageRetentionState,
        scheduled_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id.trim())
        else {
            return Ok(OperationApplyResult::NotFound);
        };
        let applied = apply_retention_event(
            message,
            RetentionEventKind::Scheduled,
            Some(policy),
            state,
            scheduled_at,
            event_seq,
        );
        self.clear_retained_preview(message).await;
        Ok(applied)
    }

    async fn apply_retention_expired_event(
        &self,
        message_id: &str,
        state: &MessageRetentionState,
        expired_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id.trim())
        else {
            return Ok(OperationApplyResult::NotFound);
        };
        let applied = apply_retention_event(
            message,
            RetentionEventKind::Expired,
            None,
            state,
            expired_at,
            event_seq,
        );
        self.clear_retained_preview(message).await;
        Ok(applied)
    }

    async fn apply_retention_purged_event(
        &self,
        message_id: &str,
        state: &MessageRetentionState,
        purged_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, message_id.trim())
        else {
            return Ok(OperationApplyResult::NotFound);
        };
        let applied = apply_retention_event(
            message,
            RetentionEventKind::Purged,
            None,
            state,
            purged_at,
            event_seq,
        );
        self.clear_retained_preview(message).await;
        Ok(applied)
    }

    async fn mark_outgoing_read_upto_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        read_seq: u64,
    ) -> Result<()> {
        let mut data = self.data.write().await;
        data.for_each_in_conversation_mut(conversation_id.trim(), |message| {
            mark_outgoing_message_read(message, conversation_id, sender_user_id, read_seq);
        });
        Ok(())
    }

    async fn reconcile_outgoing_read_by_peer_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        peer_read_seq: u64,
    ) -> Result<()> {
        let conversation_id = conversation_id.trim();
        let sender_user_id = sender_user_id.trim();
        if conversation_id.is_empty() || sender_user_id.is_empty() {
            return Ok(());
        }
        // 读态单调:对端已读回执只前进（peer_read_seq 单调增），一旦某条被对方读过就恒为已读。
        // 早先这里会对 seq>peer_read_seq 的消息 is_read=false 回退——冷启/陈旧同步下发的偏低
        // （甚至 0）peer_read_seq 会把已读消息退回未读，表现为「双勾过一下变单勾」。
        // 只前进不回退：由 mark_outgoing_read_upto_seq 推进已读，不再 un-read。
        if peer_read_seq > 0 {
            self.mark_outgoing_read_upto_seq(conversation_id, sender_user_id, peer_read_seq)
                .await?;
        }
        Ok(())
    }

    async fn apply_reaction(
        &self,
        conversation_id: &str,
        message_server_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
    ) -> Result<()> {
        self.apply_reaction_event(
            conversation_id,
            message_server_id,
            user_id,
            emoji,
            action,
            None,
        )
        .await?;
        Ok(())
    }

    async fn apply_reaction_event(
        &self,
        conversation_id: &str,
        message_server_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let (user_id, emoji) = (user_id.trim(), emoji.trim());
        if user_id.is_empty()
            || emoji.is_empty()
            || !matches!(
                flare_proto::common::ReactionAction::try_from(action).ok(),
                Some(
                    flare_proto::common::ReactionAction::Add
                        | flare_proto::common::ReactionAction::Remove
                )
            )
        {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut data = self.data.write().await;
        let Some(message) =
            Self::get_mut_by_server_or_local_id(&mut data, message_server_id.trim())
        else {
            return Ok(OperationApplyResult::NotFound);
        };
        if message.conversation_id != conversation_id {
            return Ok(OperationApplyResult::NotFound);
        }
        if !advance_message_event(
            &mut message.attributes,
            &format!("lastReactionEventSeq:{user_id}:{emoji}"),
            event_seq,
        ) {
            return Ok(OperationApplyResult::IgnoredStale);
        }
        message.apply_reaction_change(user_id, emoji, action);
        Ok(OperationApplyResult::Applied)
    }

    async fn list_reactions(
        &self,
        message_server_ids: &[String],
    ) -> Result<HashMap<String, Vec<ReactionEntry>>> {
        let data = self.data.read().await;
        let mut out = HashMap::new();
        for id in message_server_ids {
            let target = id.trim();
            if target.is_empty() || out.contains_key(target) {
                continue;
            }
            let message = data.get(target).or_else(|| {
                let mut matches = data
                    .client_rows(target)
                    .map(|(_, row)| row)
                    .filter(|message| message.local_state.is_local);
                let first = matches.next()?;
                matches.next().is_none().then_some(first)
            });
            let Some(message) = message else {
                continue;
            };
            if message.reactions.is_empty() {
                continue;
            }
            out.insert(target.to_owned(), message.reactions.clone());
        }
        Ok(out)
    }

    async fn set_message_flag(
        &self,
        message_id: &str,
        flag_key: &str,
        enabled: bool,
    ) -> Result<()> {
        let target = message_id.trim();
        let key = flag_key.trim();
        if target.is_empty() || key.is_empty() {
            return Ok(());
        }
        let value = if enabled { "true" } else { "false" }.to_string();
        let mut data = self.data.write().await;
        if let Some(message) = Self::get_mut_by_any_message_id(&mut data, target) {
            message.attributes.insert(key.to_string(), value);
        }
        Ok(())
    }

    async fn apply_pin_event(
        &self,
        message_id: &str,
        enabled: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let target = message_id.trim();
        if target.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_server_or_local_id(&mut data, target) else {
            return Ok(OperationApplyResult::NotFound);
        };
        if let Some(seq) = event_seq {
            let current_seq = message_attribute_seq(&message.attributes, "lastPinEventSeq");
            if seq < current_seq {
                return Ok(OperationApplyResult::IgnoredStale);
            }
            message
                .attributes
                .insert("lastPinEventSeq".to_string(), seq.to_string());
        }
        message.attributes.insert(
            "pinned".to_string(),
            if enabled { "true" } else { "false" }.to_string(),
        );
        Ok(OperationApplyResult::Applied)
    }

    async fn apply_plugin_message_state(
        &self,
        message_id: &str,
        state: &PluginMessageState,
    ) -> Result<OperationApplyResult> {
        let values_json = encode_plugin_message_state(state)?;
        let target = message_id.trim();
        if target.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_any_message_id(&mut data, target) else {
            return Ok(OperationApplyResult::NotFound);
        };
        Ok(
            if write_plugin_message_state(&mut message.attributes, state, &values_json) {
                OperationApplyResult::Applied
            } else {
                OperationApplyResult::IgnoredStale
            },
        )
    }

    async fn apply_plugin_shared_state(
        &self,
        message_id: &str,
        state: &SharedState,
    ) -> Result<OperationApplyResult> {
        validate_plugin_shared_state(state)?;
        let target = message_id.trim();
        if target.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut data = self.data.write().await;
        let Some(message) = Self::get_mut_by_any_message_id(&mut data, target) else {
            return Ok(OperationApplyResult::NotFound);
        };
        Ok(
            match write_plugin_shared_state(&mut message.attributes, state) {
                SharedStateOrder::Newer => OperationApplyResult::Applied,
                SharedStateOrder::Same | SharedStateOrder::Older => {
                    OperationApplyResult::IgnoredStale
                }
            },
        )
    }
}

fn clear_retained_conversation_preview(conversation: &mut Conversation, message: &IMMessage) {
    if message
        .retention_state
        .as_ref()
        .is_some_and(retention_hides_content)
        && conversation.last_message_id.as_deref()
            == Some(MemoryMessageStore::storage_key(message).as_str())
    {
        conversation.last_message_preview = None;
        if let Some(preview) = &mut conversation.last_message {
            preview.text.clear();
        }
    }
}

pub struct MemoryConversationStore {
    messages: Arc<RwLock<MemoryMessageRows>>,
    data: RwLock<HashMap<String, Conversation>>,
    materialized_max_seq: RwLock<HashMap<String, u64>>,
    member_nicknames: RwLock<HashMap<crate::domain::MemberKey, String>>,
}

impl MemoryConversationStore {
    /// Publish only the preview fields committed with message rows. Applying a remote
    /// summary merge here would also acknowledge settings and adjust unread receipts.
    pub async fn install_staged_previews(&self, committed: Vec<Conversation>) {
        let mut data = self.data.write().await;
        for staged in committed {
            match data.entry(staged.conversation_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(staged);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let current = entry.get_mut();
                    current.max_seq = staged.max_seq;
                    current.last_message_id = staged.last_message_id;
                    current.last_sender_id = staged.last_sender_id;
                    current.last_message_at = staged.last_message_at;
                    current.last_message_preview = staged.last_message_preview;
                    current.last_message = staged.last_message;
                    current.updated_at = staged.updated_at;
                    current.updated_at_ts = staged.updated_at_ts;
                }
            }
        }
    }

    /// Restore trusted local receipts along with the persisted conversation state.
    pub async fn restore_local_snapshot(&self, conversations: &[Conversation]) -> Result<()> {
        self.save_batch(conversations).await?;
        let messages = self.messages.read().await;
        let mut data = self.data.write().await;
        for persisted in conversations {
            if let Some(current) = data.get_mut(&persisted.conversation_id) {
                crate::domain::preserve_message_projection(Some(persisted), current);
                if let Some(message) = current
                    .last_message_id
                    .as_ref()
                    .and_then(|id| messages.get(id))
                {
                    clear_retained_conversation_preview(current, message);
                }
            }
        }
        Ok(())
    }

    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(MemoryMessageRows::default())),
            data: RwLock::new(HashMap::new()),
            materialized_max_seq: RwLock::new(HashMap::new()),
            member_nicknames: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryConversationStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConversationReader for MemoryConversationStore {
    async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        Ok(self.data.read().await.get(conversation_id).cloned())
    }

    async fn get_many(&self, conversation_ids: &[String]) -> Result<Vec<Conversation>> {
        let data = self.data.read().await;
        Ok(conversation_ids
            .iter()
            .filter_map(|id| data.get(id).cloned())
            .collect())
    }

    async fn list(&self) -> Result<Vec<Conversation>> {
        let mut list: Vec<Conversation> = self.data.read().await.values().cloned().collect();
        list.sort_by(|a, b| {
            match (
                b.is_pinned.cmp(&a.is_pinned),
                b.last_message_at.cmp(&a.last_message_at),
            ) {
                (std::cmp::Ordering::Equal, o) => o,
                (p, _) => p,
            }
        });
        Ok(list)
    }

    async fn member_nicknames(
        &self,
        pairs: &[crate::domain::MemberKey],
    ) -> Result<HashMap<crate::domain::MemberKey, String>> {
        let nicknames = self.member_nicknames.read().await;
        Ok(pairs
            .iter()
            .filter_map(|pair| {
                nicknames
                    .get(pair)
                    .map(|nickname| (pair.clone(), nickname.clone()))
            })
            .collect())
    }
}

#[async_trait]
impl ConversationWriter for MemoryConversationStore {
    async fn commit_message_projection(
        &self,
        expected: &Conversation,
        projected: &Conversation,
    ) -> Result<bool> {
        let seq = crate::domain::message_projection_receipt(projected)?.materialized_seq;
        let mut data = self.data.write().await;
        let Some(current) = data.get_mut(&expected.conversation_id) else {
            return Ok(false);
        };
        if !crate::domain::message_projection_matches(current, expected) {
            return Ok(false);
        }
        crate::domain::copy_message_projection(current, projected);
        drop(data);
        let mut materialized = self.materialized_max_seq.write().await;
        let value = materialized
            .entry(projected.conversation_id.clone())
            .or_default();
        *value = (*value).max(seq);
        Ok(true)
    }

    async fn save_batch(&self, conversations: &[Conversation]) -> Result<()> {
        for conversation in conversations {
            if conversation.conversation_id.trim().is_empty() {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "conversationId 不能为空",
                ));
            }
        }

        let messages = self.messages.read().await;
        let mut data = self.data.write().await;
        for conv in conversations {
            let mut merged =
                merge_incoming_conversation_summary(data.get(&conv.conversation_id), conv);
            if let Some(message) = merged
                .last_message_id
                .as_ref()
                .and_then(|id| messages.get(id))
            {
                clear_retained_conversation_preview(&mut merged, message);
            }
            data.insert(merged.conversation_id.clone(), merged);
        }
        Ok(())
    }

    async fn save_one(&self, conversation: &Conversation) -> Result<()> {
        ConversationWriter::save_batch(self, std::slice::from_ref(conversation)).await
    }

    async fn update_unread(
        &self,
        conversation_id: &str,
        unread_count: u32,
        last_read_seq: u64,
    ) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            updated.unread_count = unread_count;
            updated.last_read_seq = last_read_seq;
            if unread_count == 0 {
                updated.mention_count = 0;
                updated.mention_me = false;
            }
            data.insert(conversation_id.to_string(), updated);
        }
        Ok(())
    }

    async fn set_pinned(&self, conversation_id: &str, pinned: bool) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            updated.is_pinned = pinned;
            data.insert(conversation_id.to_string(), updated);
        }
        Ok(())
    }

    async fn set_muted(&self, conversation_id: &str, muted: bool) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            updated.is_muted = muted;
            data.insert(conversation_id.to_string(), updated);
        }
        Ok(())
    }

    async fn set_notify_all(&self, conversation_id: &str, enabled: bool) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            crate::model::set_notify_all(&mut updated, enabled);
            data.insert(conversation_id.to_string(), updated);
        }
        Ok(())
    }

    async fn set_archived(&self, conversation_id: &str, archived: bool) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            updated.is_archived = archived;
            data.insert(conversation_id.to_string(), updated);
        }
        Ok(())
    }

    async fn mark_unread(&self, conversation_id: &str) -> Result<u32> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            if updated.max_seq > 0 {
                updated.last_read_seq = updated.max_seq.saturating_sub(1);
            }
            updated.unread_count = 1;
            data.insert(conversation_id.to_string(), updated.clone());
            return Ok(updated.unread_count);
        }
        Ok(0)
    }

    async fn update_draft(&self, conversation_id: &str, draft: Option<&str>) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get(conversation_id) {
            let mut updated = conv.clone();
            updated.draft = draft.map(String::from);
            data.insert(conversation_id.to_string(), updated);
        }
        Ok(())
    }

    async fn delete(&self, conversation_id: &str) -> Result<()> {
        self.data.write().await.remove(conversation_id);
        self.materialized_max_seq
            .write()
            .await
            .remove(conversation_id);
        self.member_nicknames
            .write()
            .await
            .retain(|(conversation, _), _| conversation != conversation_id);
        Ok(())
    }

    async fn save_member_nicknames(
        &self,
        snapshots: &[crate::domain::ParticipantDisplaySnapshot],
    ) -> Result<Vec<String>> {
        let mut nicknames = self.member_nicknames.write().await;
        let mut changed: Vec<String> = Vec::new();
        for snapshot in snapshots.iter().filter_map(|s| s.normalized()) {
            let key = (snapshot.conversation_id.clone(), snapshot.user_id.clone());
            let differs = if snapshot.nickname.is_empty() {
                nicknames.remove(&key).is_some()
            } else {
                nicknames.insert(key, snapshot.nickname.clone()).as_deref()
                    != Some(snapshot.nickname.as_str())
            };
            if differs && !changed.contains(&snapshot.conversation_id) {
                changed.push(snapshot.conversation_id);
            }
        }
        Ok(changed)
    }

    async fn apply_user_profile_snapshot(
        &self,
        user_id: &str,
        _nickname: &str,
        avatar_url: &str,
    ) -> Result<Vec<String>> {
        let user_id = user_id.trim();
        let mut data = self.data.write().await;
        let mut affected = Vec::new();
        for conversation in data.values_mut() {
            if !crate::domain::conversation_shows_person(conversation, user_id) {
                continue;
            }
            if conversation.conversation_type.is_single_chat_conversation()
                && conversation.channel_id.trim() == user_id
            {
                conversation.avatar_url = avatar_url.to_string();
            }
            if conversation.last_sender_id.as_deref().map(str::trim) == Some(user_id) {
                conversation.last_sender_avatar_url = avatar_url.to_string();
            }
            affected.push(conversation.conversation_id.clone());
        }
        affected.sort();
        Ok(affected)
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
        {
            let messages = self.messages.read().await;
            let mut data = self.data.write().await;
            let Some(mut source) = data.remove(from) else {
                return Ok(());
            };
            // 旧行的名字可能是它自己的 id；换到新 id 下就再也认不出来，所以先按旧 id 清掉。
            source.display_name =
                crate::domain::business_name(&source.display_name, &[&source.channel_id, from]);
            source.conversation_id = to.to_string();
            match data.get_mut(to) {
                Some(target) => {
                    if target.channel_id.trim().is_empty() {
                        target.channel_id = source.channel_id;
                    }
                    if target.display_name.trim().is_empty() {
                        target.display_name = source.display_name;
                    }
                    if target.avatar_url.trim().is_empty() {
                        target.avatar_url = source.avatar_url;
                    }
                    if target.remark.is_none() {
                        target.remark = source.remark;
                    }
                    if target.draft.is_none() {
                        target.draft = source.draft;
                    }
                    if source.max_seq > target.max_seq {
                        target.max_seq = source.max_seq;
                        target.last_message_id = source.last_message_id;
                        target.last_sender_id = source.last_sender_id;
                        target.last_message_at = source.last_message_at;
                        target.last_message_preview = source.last_message_preview;
                    }
                    target.unread_count = target.unread_count.max(source.unread_count);
                    target.last_read_seq = target.last_read_seq.max(source.last_read_seq);
                    target.visible_after_seq =
                        target.visible_after_seq.max(source.visible_after_seq);
                    target.is_pinned |= source.is_pinned;
                    target.is_muted |= source.is_muted;
                    target.is_archived |= source.is_archived;
                    target.version = target.version.max(source.version);
                    target.updated_at = target.updated_at.max(source.updated_at);
                    target.updated_at_ts = target.updated_at_ts.max(source.updated_at_ts);
                    for (key, value) in source.ext {
                        target.ext.entry(key).or_insert(value);
                    }
                }
                None => {
                    data.insert(to.to_string(), source);
                }
            }
            if let Some(target) = data.get_mut(to)
                && let Some(message) = target
                    .last_message_id
                    .as_ref()
                    .and_then(|id| messages.get(id))
            {
                clear_retained_conversation_preview(target, message);
            }
        }
        let mut max_seq = self.materialized_max_seq.write().await;
        let source_seq = max_seq.remove(from).unwrap_or_default();
        if source_seq > 0 {
            let target_seq = max_seq.entry(to.to_string()).or_default();
            *target_seq = (*target_seq).max(source_seq);
        }
        Ok(())
    }

    async fn clear_local_chat_history(
        &self,
        conversation_id: &str,
        cleared_through_seq: u64,
    ) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(conv) = data.get_mut(conversation_id) {
            conv.last_message_id = None;
            conv.last_message_preview = None;
            conv.last_message_at = None;
            conv.unread_count = 0;
            if cleared_through_seq > 0 {
                crate::domain::set_local_cleared_through_seq(&mut conv.ext, cleared_through_seq);
            }
        }
        self.materialized_max_seq
            .write()
            .await
            .remove(conversation_id);
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
        let messages = self.messages.read().await;
        let mut data = self.data.write().await;
        if let Some(c) = data.get_mut(conversation_id) {
            let current_seq = c.max_seq;
            let current_time = c.last_message_at.unwrap_or(0);
            let should_replace = max_seq >= current_seq || last_message_at >= current_time;
            if should_replace {
                c.last_message_id = Some(last_message_id.to_string());
                c.last_sender_id = Some(last_sender_id.to_string());
                c.last_message_at = Some(last_message_at);
                c.last_message_preview = last_message_preview.map(String::from);
            }
            c.max_seq = c.max_seq.max(max_seq);
            if let Some(message) = c.last_message_id.as_ref().and_then(|id| messages.get(id)) {
                clear_retained_conversation_preview(c, message);
            }
        }
        if max_seq > 0 {
            let mut materialized = self.materialized_max_seq.write().await;
            let current = materialized.entry(conversation_id.to_string()).or_default();
            *current = (*current).max(max_seq);
        }
        Ok(())
    }

    async fn recompute_unread_for_user(
        &self,
        _conversation_id: &str,
        _current_user_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_local_max_seq(&self, conversation_id: &str) -> Result<u64> {
        Ok(self
            .materialized_max_seq
            .read()
            .await
            .get(conversation_id)
            .copied()
            .unwrap_or_default())
    }
}

/// 完整内存 IM 仓储（Web production runtime 默认路径）。
/// Shared commit path for the volatile provider and write-through browser storage.
pub struct MemoryOutgoingStore {
    messages: Arc<MemoryMessageStore>,
    pending: Arc<MemoryPendingSendStore>,
}

impl MemoryOutgoingStore {
    pub fn new(messages: Arc<MemoryMessageStore>, pending: Arc<MemoryPendingSendStore>) -> Self {
        Self { messages, pending }
    }

    /// Publish the new cache rows only after the backing transaction has completed.
    pub async fn commit_with<F, Fut>(
        &self,
        message: &IMMessage,
        entry: Option<&crate::domain::PendingSendVo>,
        persist: F,
    ) -> Result<bool>
    where
        F: FnOnce(IMMessage, Conversation) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        if let Some(entry) = entry {
            entry.validate_message(message)?;
        }
        let mut rows = self.messages.data.write().await;
        let mut conversations = self.messages.conversations.data.write().await;
        let mut materialized = self
            .messages
            .conversations
            .materialized_max_seq
            .write()
            .await;
        let mut pending = self.pending.data.write().await;
        let confirmed = rows
            .client_rows(&message.client_msg_id)
            .map(|(_, row)| row)
            .find(|row| {
                row.conversation_id == message.conversation_id
                    && row.sender_id == message.sender_id
                    && !row.local_state.is_local
                    && row.conversation_seq > 0
            });
        if entry.is_some() && confirmed.is_some() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "message already confirmed",
            ));
        }
        let newer = confirmed.filter(|row| {
            message.local_state.failed || row.conversation_seq > message.conversation_seq
        });
        let applied = newer.is_none();
        let msg = newer.cloned().unwrap_or_else(|| {
            MemoryMessageStore::merged_outgoing(&rows, &message.client_msg_id, message)
        });
        let key = MemoryMessageStore::storage_key(&msg);
        let mut conversation = conversations
            .get(&msg.conversation_id)
            .cloned()
            .or_else(|| crate::domain::ConversationIdentityService::outgoing_conversation(&msg))
            .unwrap_or_else(|| Conversation::from_conversation_id(msg.conversation_id.clone()));
        let seq = msg.conversation_seq;
        let ts = if seq > 0 && msg.created_at > 0 {
            msg.created_at
        } else if seq > 0 && msg.client_created_at > 0 {
            msg.client_created_at
        } else {
            msg.timeline_sort_ts()
        };
        if seq > conversation.max_seq
            || ((seq == 0 || seq == conversation.max_seq)
                && ts >= conversation.last_message_at.unwrap_or_default())
            || conversation.last_message_id.as_deref() == Some(&msg.client_msg_id)
        {
            conversation.last_message_id = Some(key.clone());
            conversation.last_message_at = Some(ts);
            conversation.last_sender_id = Some(msg.sender_id.clone());
            conversation.last_message_preview = msg.text_for_storage();
            clear_retained_conversation_preview(&mut conversation, &msg);
        }
        if conversation.business_type.is_empty() {
            conversation.business_type = crate::domain::projected_business_type(&msg).into();
        }
        if !conversations.contains_key(&msg.conversation_id) {
            conversation.created_at = ts;
        }
        conversation.updated_at = conversation.updated_at.max(ts);
        conversation.updated_at_ts = Some(conversation.updated_at_ts.unwrap_or_default().max(ts));
        conversation.max_seq = conversation.max_seq.max(seq);
        persist(msg.clone(), conversation.clone()).await?;
        MemoryMessageStore::remove_conflicting_client_msg_rows(
            &mut rows,
            &msg.client_msg_id,
            &key,
            &msg.conversation_id,
            &msg.sender_id,
        );
        rows.insert(key, msg);
        conversations.insert(message.conversation_id.clone(), conversation);
        if seq > 0 {
            let current = materialized
                .entry(message.conversation_id.clone())
                .or_default();
            *current = (*current).max(seq);
        }
        match entry {
            Some(entry) => pending.insert(entry.clone()),
            None => {
                pending.remove(&message.client_msg_id);
            }
        }
        Ok(applied)
    }
}

#[async_trait]
impl crate::domain::OutgoingSendStore for MemoryOutgoingStore {
    fn survives_restart(&self) -> bool {
        false
    }

    async fn enqueue_send(
        &self,
        message: &IMMessage,
        entry: &crate::domain::PendingSendVo,
    ) -> Result<()> {
        self.commit_with(message, Some(entry), |_, _| async { Ok(()) })
            .await
            .map(|_| ())
    }
    async fn complete_send(&self, message: &IMMessage) -> Result<bool> {
        self.commit_with(message, None, |_, _| async { Ok(()) })
            .await
    }
}

pub fn in_memory_im_provider() -> StoreProvider {
    let pending = Arc::new(MemoryPendingSendStore::new());
    let user_profiles = Arc::new(MemoryUserProfileStore::new());
    let conversations = Arc::new(MemoryConversationStore::new());
    let messages = Arc::new(MemoryMessageStore::with_conversations(
        conversations.clone(),
    ));
    StoreProvider {
        plugin_commands: None,
        plugin_consents: None,
        plugin_bundle_storage: None,
        messages: messages.clone(),
        conversations,
        conversation_participants: None,
        cursors: Arc::new(MemorySyncCursorStore::new()),
        timeline_coverage: Arc::new(super::MemoryTimelineCoverageStore::new()),
        outgoing: Some(Arc::new(MemoryOutgoingStore::new(
            messages,
            pending.clone(),
        ))),
        pending_send_reader: Some(pending.clone()),
        pending_send_writer: Some(pending),
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: Some(user_profiles.clone()),
        user_profiles_writer: Some(user_profiles),
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryConversationStore, MemoryMessageStore};
    use crate::domain::{
        ConversationReader, ConversationWriter, MessageReader, MessageStore, MessageWriter,
        OperationApplyResult,
    };
    use crate::model::{Conversation, IMMessage};
    use crate::shared::error::ErrorCode;
    use flare_proto::common::{MessageRetentionPolicy, MessageRetentionState, MessageStatus};

    fn local_message(server_id: &str, client_msg_id: &str) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = server_id.to_string();
        message.client_msg_id = client_msg_id.to_string();
        message.conversation_id = "conv-memory-dupe".to_string();
        message.sender_id = "u1".to_string();
        message.local_state.sending = true;
        message.local_state.is_local = true;
        message
    }

    #[tokio::test]
    async fn outgoing_commit_only_publishes_after_external_storage_succeeds() {
        use crate::domain::{OutgoingSendStore, PendingSendReader, PendingSendVo};
        let messages = std::sync::Arc::new(MemoryMessageStore::new());
        let pending = std::sync::Arc::new(super::MemoryPendingSendStore::new());
        let store = super::MemoryOutgoingStore::new(messages.clone(), pending.clone());
        let local = local_message("client", "client");
        let entry = PendingSendVo {
            client_msg_id: local.client_msg_id.clone(),
            conversation_id: local.conversation_id.clone(),
            message: local.clone(),
            enqueued_at_ms: 1,
        };
        let fail = |_, _| async { Err(crate::FlareError::system("write failed")) };
        assert!(store.commit_with(&local, Some(&entry), fail).await.is_err());
        assert!(messages.get("client").await.unwrap().is_none());
        assert!(
            messages
                .conversations
                .get(&local.conversation_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(pending.list().await.unwrap().is_empty());
        store.enqueue_send(&local, &entry).await.unwrap();
        let mut ack = local.clone();
        ack.server_id = "server".into();
        ack.conversation_seq = 7;
        ack.local_state = Default::default();
        assert!(store.commit_with(&ack, None, fail).await.is_err());
        assert!(messages.get("client").await.unwrap().is_some());
        assert!(messages.get("server").await.unwrap().is_none());
        assert_eq!(pending.list().await.unwrap().len(), 1);
        store.complete_send(&ack).await.unwrap();
        store.complete_send(&ack).await.unwrap();
        assert!(pending.list().await.unwrap().is_empty());
        assert!(messages.get("client").await.unwrap().is_none());
        assert_eq!(
            messages
                .conversations
                .get(&local.conversation_id)
                .await
                .unwrap()
                .unwrap()
                .last_message_id
                .as_deref(),
            Some("server")
        );
    }

    #[tokio::test]
    async fn indexed_windows_follow_alias_rewrites_ack_replacements_and_deletes() {
        let store = MemoryMessageStore::new();
        let mut pending = local_message("", "shared-client-id");
        pending.conversation_id = "legacy-peer".into();
        let mut unrelated = local_message("other-server", &pending.client_msg_id);
        unrelated.conversation_id = "other-chat".into();
        unrelated.conversation_seq = 9;
        store
            .save_batch(&[pending.clone(), unrelated])
            .await
            .unwrap();
        assert!(
            store
                .get_by_client_msg_id(&pending.client_msg_id)
                .await
                .unwrap()
                .is_none()
        );

        assert_eq!(
            store
                .rewrite_conversation_id("legacy-peer", "canonical-chat")
                .await
                .unwrap(),
            1
        );
        assert!(
            store
                .get_by_conversation("legacy-peer", 0, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .get_by_conversation("canonical-chat", 0, 10)
                .await
                .unwrap()
                .len(),
            1
        );

        let mut confirmed = pending.clone();
        confirmed.conversation_id = "canonical-chat".into();
        confirmed.server_id = "acked-server".into();
        confirmed.conversation_seq = 5;
        confirmed.local_state = Default::default();
        store
            .update_after_ack(&pending.client_msg_id, &confirmed)
            .await
            .unwrap();
        let page = store
            .get_by_conversation_after("canonical-chat", 0, 1)
            .await
            .unwrap();
        assert_eq!(page[0].server_id, "acked-server");
        assert_eq!(
            store
                .get_by_conversation("canonical-chat", 0, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .get_by_conversation("other-chat", 0, 10)
                .await
                .unwrap()[0]
                .server_id,
            "other-server"
        );
        assert!(store.get(&pending.client_msg_id).await.unwrap().is_none());

        store.delete("other-server").await.unwrap();
        assert_eq!(
            store
                .get_by_client_msg_id(&pending.client_msg_id)
                .await
                .unwrap()
                .unwrap()
                .server_id,
            "acked-server"
        );
        store.delete("acked-server").await.unwrap();
        assert!(
            store
                .get_by_client_msg_id(&pending.client_msg_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .oldest_conversation_seq("canonical-chat")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn search_filters_types_before_limit_and_honors_global_keyword() {
        use crate::model::{MessageSearchKind as Kind, MessageSearchQuery, MessageType};
        let store = MemoryMessageStore::new();
        let mut rows = Vec::new();
        for (index, kind) in [
            MessageType::Text,
            MessageType::RichText,
            MessageType::Quote,
            MessageType::Image,
            MessageType::ImageGroup,
            MessageType::Video,
            MessageType::Audio,
            MessageType::File,
        ]
        .into_iter()
        .enumerate()
        {
            let mut m = local_message(&format!("m{index}"), &format!("c{index}"));
            m.message_type = kind as i32;
            m.created_at = 1000 - index as u64;
            m.conversation_seq = 1000 - index as u64;
            m.content = Some(crate::model::Elem::Text(
                crate::content::message_elem::TextElem {
                    text: "needle 11".into(),
                    mentions: vec![],
                },
            ));
            rows.push(m);
        }
        let mut foreign = rows[7].clone();
        foreign.server_id = "foreign".into();
        foreign.client_msg_id = "foreign-client".into();
        foreign.conversation_id = "elsewhere".into();
        rows.push(foreign);
        store.save_batch(&rows).await.unwrap();
        for (kind, expected) in [
            (Kind::Text, vec![0, 1, 2]),
            (Kind::Image, vec![3, 4]),
            (Kind::Video, vec![5]),
            (Kind::Audio, vec![6]),
            (Kind::File, vec![7]),
            (Kind::Media, vec![3, 4, 5, 6, 7]),
            (Kind::Message, vec![0, 1, 2, 3, 4, 5, 6, 7]),
        ] {
            let results = store
                .search_by_query(&MessageSearchQuery {
                    conversation_id: Some("conv-memory-dupe".into()),
                    keyword: Some("NEEDLE".into()),
                    kinds: vec![kind],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(
                results
                    .iter()
                    .map(|m| m.server_id.clone())
                    .collect::<Vec<_>>(),
                expected.iter().map(|i| format!("m{i}")).collect::<Vec<_>>()
            );
        }
        let result = store
            .search_by_query(&MessageSearchQuery {
                conversation_id: Some("conv-memory-dupe".into()),
                keyword: None,
                kinds: vec![Kind::File],
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result[0].server_id, "m7");
        assert!(store.search("absent", 50).await.unwrap().is_empty());
        assert_eq!(store.search("NEEDLE", 50).await.unwrap().len(), 9);
    }

    #[tokio::test]
    async fn save_batch_collapses_existing_client_msg_id() {
        let store = MemoryMessageStore::new();
        let pending = local_message("client-memory-1", "client-memory-1");
        store
            .save_batch(std::slice::from_ref(&pending))
            .await
            .unwrap();

        let mut echoed = pending;
        echoed.server_id = "server-memory-1".to_string();
        echoed.conversation_seq = 10;
        echoed.status = MessageStatus::Persisted as i32;
        echoed.local_state.sending = false;
        echoed.local_state.is_local = false;
        store.save_batch(&[echoed]).await.unwrap();

        assert!(store.get("client-memory-1").await.unwrap().is_none());
        let stored = store
            .get_by_client_msg_id("client-memory-1")
            .await
            .unwrap()
            .expect("canonical message");
        assert_eq!(stored.server_id, "server-memory-1");

        let timeline = store
            .get_by_conversation("conv-memory-dupe", 0, 10)
            .await
            .unwrap();
        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].server_id, "server-memory-1");
    }

    #[tokio::test]
    async fn save_batch_keeps_distinct_server_messages_with_same_client_msg_id() {
        let store = MemoryMessageStore::new();
        let mut first = local_message("server-memory-remote-1", "client-memory-shared");
        first.conversation_seq = 10;
        first.local_state.sending = false;
        first.local_state.is_local = false;
        let mut second = local_message("server-memory-remote-2", "client-memory-shared");
        second.conversation_seq = 11;
        second.local_state.sending = false;
        second.local_state.is_local = false;

        store.save_batch(&[first, second]).await.unwrap();

        let timeline = store
            .get_by_conversation("conv-memory-dupe", 0, 10)
            .await
            .unwrap();
        let server_ids = timeline
            .iter()
            .map(|message| message.server_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(timeline.len(), 2);
        assert!(server_ids.contains("server-memory-remote-1"));
        assert!(server_ids.contains("server-memory-remote-2"));
    }

    #[tokio::test]
    async fn update_after_ack_collapses_existing_client_msg_id() {
        let store = MemoryMessageStore::new();
        let stale = local_message("stale-memory-1", "client-memory-ack-1");
        store
            .save_batch(std::slice::from_ref(&stale))
            .await
            .unwrap();

        let mut acked = stale;
        acked.server_id = "server-memory-ack-1".to_string();
        acked.conversation_seq = 20;
        acked.status = MessageStatus::Sent as i32;
        store
            .update_after_ack("client-memory-ack-1", &acked)
            .await
            .unwrap();

        assert!(store.get("stale-memory-1").await.unwrap().is_none());
        let stored = store.get("server-memory-ack-1").await.unwrap().unwrap();
        assert_eq!(stored.client_msg_id, "client-memory-ack-1");

        let timeline = store
            .get_by_conversation("conv-memory-dupe", 0, 10)
            .await
            .unwrap();
        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].server_id, "server-memory-ack-1");
    }

    #[tokio::test]
    async fn read_receipt_marks_outgoing_messages_read() {
        let store = MemoryMessageStore::new();
        let mut first = local_message("server-memory-read-1", "client-memory-read-1");
        first.conversation_id = "conv-memory-read-receipt".to_string();
        first.conversation_seq = 1;
        first.status = MessageStatus::Created as i32;
        let mut second = local_message("server-memory-read-2", "client-memory-read-2");
        second.conversation_id = "conv-memory-read-receipt".to_string();
        second.conversation_seq = 2;
        second.status = MessageStatus::Persisted as i32;
        let mut unread_tail = local_message("server-memory-read-3", "client-memory-read-3");
        unread_tail.conversation_id = "conv-memory-read-receipt".to_string();
        unread_tail.conversation_seq = 3;
        unread_tail.status = MessageStatus::Sent as i32;

        store
            .save_batch(&[first, second, unread_tail])
            .await
            .unwrap();
        store
            .mark_outgoing_read_upto_seq("conv-memory-read-receipt", "u1", 2)
            .await
            .unwrap();

        let first = store.get("server-memory-read-1").await.unwrap().unwrap();
        let second = store.get("server-memory-read-2").await.unwrap().unwrap();
        let tail = store.get("server-memory-read-3").await.unwrap().unwrap();
        assert!(first.is_read);
        assert_eq!(first.status, MessageStatus::Sent as i32);
        assert!(second.is_read);
        assert_eq!(second.status, MessageStatus::Persisted as i32);
        assert!(!tail.is_read);
    }

    #[tokio::test]
    async fn reconcile_outgoing_read_by_peer_seq_never_downgrades_read() {
        // 读态单调:即便同步下发偏低/陈旧的 peer_read_seq(冷启摘要可能为 0 或落后),
        // 已被对方读过的消息也不回退为未读——否则「双勾过一下变单勾」。
        let store = MemoryMessageStore::new();
        let mut first = local_message("server-memory-peer-read-1", "client-memory-peer-read-1");
        first.conversation_id = "conv-memory-peer-read".to_string();
        first.conversation_seq = 1;
        first.status = MessageStatus::Sent as i32;
        first.is_read = true;
        let mut tail = local_message("server-memory-peer-read-2", "client-memory-peer-read-2");
        tail.conversation_id = "conv-memory-peer-read".to_string();
        tail.conversation_seq = 2;
        tail.status = MessageStatus::Sent as i32;
        tail.is_read = true;

        store.save_batch(&[first, tail]).await.unwrap();
        // 用偏低的 peer_read_seq=1 reconcile：seq=2 的 tail 曾被读,不得因此回退。
        store
            .reconcile_outgoing_read_by_peer_seq("conv-memory-peer-read", "u1", 1)
            .await
            .unwrap();

        let first = store
            .get("server-memory-peer-read-1")
            .await
            .unwrap()
            .unwrap();
        let tail = store
            .get("server-memory-peer-read-2")
            .await
            .unwrap()
            .unwrap();
        assert!(first.is_read);
        assert!(tail.is_read);
    }

    #[tokio::test]
    async fn reaction_updates_are_visible_by_server_and_client_message_id() {
        let store = MemoryMessageStore::new();
        let message = local_message("server-memory-reaction-1", "client-memory-reaction-1");
        store.save_one(&message).await.unwrap();

        store
            .apply_reaction(
                "conv-memory-dupe",
                "server-memory-reaction-1",
                "u2",
                "thumbsup",
                flare_proto::common::ReactionAction::Add as i32,
            )
            .await
            .unwrap();

        let reactions = store
            .list_reactions(&[
                "server-memory-reaction-1".to_string(),
                "client-memory-reaction-1".to_string(),
            ])
            .await
            .unwrap();
        let by_server = reactions
            .get("server-memory-reaction-1")
            .expect("server id reaction");
        let by_client = reactions
            .get("client-memory-reaction-1")
            .expect("client id reaction");
        assert_eq!(by_server[0].emoji, "thumbsup");
        assert_eq!(by_server[0].user_ids, vec!["u2".to_string()]);
        assert_eq!(by_client[0].count, 1);

        store
            .apply_reaction(
                "conv-memory-dupe",
                "server-memory-reaction-1",
                "u2",
                "thumbsup",
                flare_proto::common::ReactionAction::Remove as i32,
            )
            .await
            .unwrap();

        let reactions = store
            .list_reactions(&["server-memory-reaction-1".to_string()])
            .await
            .unwrap();
        assert!(reactions.is_empty());
    }

    #[tokio::test]
    async fn save_batch_preserves_pin_event_attributes() {
        let store = MemoryMessageStore::new();
        let message = local_message("server-memory-pin-1", "client-memory-pin-1");
        store.save_one(&message).await.unwrap();

        let applied = store
            .apply_pin_event("server-memory-pin-1", true, Some(10))
            .await
            .unwrap();
        assert_eq!(applied, OperationApplyResult::Applied);

        let mut snapshot = message.clone();
        snapshot.attributes.clear();
        store.save_one(&snapshot).await.unwrap();

        let after_snapshot = store
            .get("server-memory-pin-1")
            .await
            .unwrap()
            .expect("message");
        assert_eq!(
            after_snapshot.attributes.get("pinned").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            after_snapshot
                .attributes
                .get("lastPinEventSeq")
                .map(String::as_str),
            Some("10")
        );

        let stale = store
            .apply_pin_event("server-memory-pin-1", false, Some(9))
            .await
            .unwrap();
        assert_eq!(stale, OperationApplyResult::IgnoredStale);
        let after_stale = store
            .get("server-memory-pin-1")
            .await
            .unwrap()
            .expect("message");
        assert_eq!(
            after_stale.attributes.get("pinned").map(String::as_str),
            Some("true")
        );

        let newer = store
            .apply_pin_event("server-memory-pin-1", false, Some(11))
            .await
            .unwrap();
        assert_eq!(newer, OperationApplyResult::Applied);
        store.save_one(&snapshot).await.unwrap();
        let after_unpin_snapshot = store
            .get("server-memory-pin-1")
            .await
            .unwrap()
            .expect("message");
        assert_eq!(
            after_unpin_snapshot
                .attributes
                .get("pinned")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            after_unpin_snapshot
                .attributes
                .get("lastPinEventSeq")
                .map(String::as_str),
            Some("11")
        );
    }

    #[tokio::test]
    async fn conversation_save_batch_rejects_blank_conversation_id() {
        let store = MemoryConversationStore::new();
        let conversation = Conversation::from_conversation_id("   ".to_string());

        let err = store
            .save_batch(&[conversation])
            .await
            .expect_err("blank conversation id must be rejected");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[tokio::test]
    async fn conversation_save_batch_does_not_roll_back_local_read_position() {
        let store = MemoryConversationStore::new();
        let mut local = Conversation::from_conversation_id("conv-memory-read".to_string());
        local.max_seq = 310;
        local.last_read_seq = 310;
        local.unread_count = 0;
        store.save_one(&local).await.unwrap();

        let mut stale = Conversation::from_conversation_id("conv-memory-read".to_string());
        stale.max_seq = 310;
        stale.last_read_seq = 20;
        stale.unread_count = 17;
        store.save_one(&stale).await.unwrap();

        let stored = store
            .get("conv-memory-read")
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(stored.max_seq, 310);
        assert_eq!(stored.last_read_seq, 310);
        assert_eq!(stored.unread_count, 0);
    }

    #[tokio::test]
    async fn conversation_save_batch_does_not_clear_local_latest_message_with_empty_summary() {
        let store = MemoryConversationStore::new();
        let mut local = Conversation::from_conversation_id("conv-memory-latest".to_string());
        local.max_seq = 100;
        local.last_message_id = Some("msg-100".to_string());
        local.last_sender_id = Some("u2".to_string());
        local.last_message_at = Some(12_000);
        local.last_message_preview = Some("latest".to_string());
        store.save_one(&local).await.unwrap();

        let mut stale = Conversation::from_conversation_id("conv-memory-latest".to_string());
        stale.max_seq = 100;
        stale.last_read_seq = 90;
        stale.unread_count = 10;
        store.save_one(&stale).await.unwrap();

        let stored = store
            .get("conv-memory-latest")
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(stored.last_message_id.as_deref(), Some("msg-100"));
        assert_eq!(stored.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(stored.last_message_at, Some(12_000));
        assert_eq!(stored.last_message_preview.as_deref(), Some("latest"));
    }

    #[tokio::test]
    async fn local_max_seq_tracks_materialized_messages_not_remote_summary() {
        let store = MemoryConversationStore::new();
        let mut summary = Conversation::from_conversation_id("conv-memory-seq".to_string());
        summary.max_seq = 8;
        summary.unread_count = 8;
        store.save_batch(&[summary]).await.unwrap();

        assert_eq!(
            store.get("conv-memory-seq").await.unwrap().unwrap().max_seq,
            8
        );
        assert_eq!(store.get_local_max_seq("conv-memory-seq").await.unwrap(), 0);

        store
            .update_last_message(
                "conv-memory-seq",
                "server-memory-seq-3",
                "alice",
                1_000,
                Some("latest"),
                3,
            )
            .await
            .unwrap();

        assert_eq!(
            store.get("conv-memory-seq").await.unwrap().unwrap().max_seq,
            8
        );
        assert_eq!(store.get_local_max_seq("conv-memory-seq").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn local_zero_seq_message_updates_preview_without_rolling_back_server_seq() {
        let store = MemoryConversationStore::new();
        let mut conversation =
            Conversation::from_conversation_id("conv-memory-preview".to_string());
        conversation.max_seq = 8;
        conversation.last_message_at = Some(1_000);
        conversation.last_message_preview = Some("server-latest".to_string());
        store.save_one(&conversation).await.unwrap();

        store
            .update_last_message(
                "conv-memory-preview",
                "client-local-1",
                "alice",
                2_000,
                Some("local pending"),
                0,
            )
            .await
            .unwrap();

        let updated = store
            .get("conv-memory-preview")
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(updated.max_seq, 8);
        assert_eq!(updated.last_message_id.as_deref(), Some("client-local-1"));
        assert_eq!(
            updated.last_message_preview.as_deref(),
            Some("local pending")
        );

        store
            .update_last_message(
                "conv-memory-preview",
                "client-stale-1",
                "alice",
                1_500,
                Some("stale local"),
                0,
            )
            .await
            .unwrap();

        let updated = store
            .get("conv-memory-preview")
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(updated.last_message_id.as_deref(), Some("client-local-1"));
        assert_eq!(
            updated.last_message_preview.as_deref(),
            Some("local pending")
        );
    }

    #[tokio::test]
    async fn update_last_message_accepts_newer_time_when_summary_max_seq_is_ahead() {
        let store = MemoryConversationStore::new();
        let mut summary =
            Conversation::from_conversation_id("conv-memory-summary-ahead".to_string());
        summary.max_seq = 99;
        summary.last_message_id = Some("msg-11".to_string());
        summary.last_sender_id = Some("u1".to_string());
        summary.last_message_at = Some(11_000);
        summary.last_message_preview = Some("stale-summary".to_string());
        store.save_one(&summary).await.unwrap();

        store
            .update_last_message(
                "conv-memory-summary-ahead",
                "msg-12",
                "u2",
                12_345,
                Some("111"),
                12,
            )
            .await
            .unwrap();

        let loaded = store
            .get("conv-memory-summary-ahead")
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(loaded.max_seq, 99);
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-12"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(loaded.last_message_at, Some(12_345));
        assert_eq!(loaded.last_message_preview.as_deref(), Some("111"));
    }

    #[tokio::test]
    async fn plugin_message_state_is_monotonic_and_survives_ack_and_sync() {
        let state = |version: u64, status: &str| {
            crate::shared::util::json::from_value::<flare_plugin_protocol::ui::PluginMessageState>(
                serde_json::json!({"version": version, "values": {"status": status}}),
            )
            .unwrap()
        };
        let status = |message: &IMMessage| {
            let state = message.plugin_message_state()?;
            Some((state.version, state.values["status"].as_str()?.to_string()))
        };
        let store = MemoryMessageStore::new();
        let pending = local_message("", "client-plugin-1");
        store.save_one(&pending).await.unwrap();
        assert_eq!(
            store
                .apply_plugin_message_state("missing", &state(1, "open"))
                .await
                .unwrap(),
            OperationApplyResult::NotFound
        );
        assert_eq!(
            store
                .apply_plugin_message_state("client-plugin-1", &state(2, "open"))
                .await
                .unwrap(),
            OperationApplyResult::Applied
        );

        let mut acked = pending;
        acked.server_id = "server-plugin-1".to_string();
        acked.local_state.sending = false;
        acked.local_state.is_local = false;
        store
            .update_after_ack("client-plugin-1", &acked)
            .await
            .unwrap();
        let stored = store.get("server-plugin-1").await.unwrap().unwrap();
        assert_eq!(status(&stored), Some((2, "open".to_string())));

        assert_eq!(
            store
                .apply_plugin_message_state("server-plugin-1", &state(1, "stale"))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        assert_eq!(
            store
                .apply_plugin_message_state("client-plugin-1", &state(2, "claimed"))
                .await
                .unwrap(),
            OperationApplyResult::Applied
        );
        store.save_one(&acked).await.unwrap();
        store.save_one(&stored).await.unwrap();
        let after_sync = store.get("server-plugin-1").await.unwrap().unwrap();
        assert_eq!(status(&after_sync), Some((2, "claimed".to_string())));
    }

    #[tokio::test]
    async fn plugin_shared_state_moves_forward_only_and_survives_ack_and_sync() {
        let shared = |version: u64, count: u64| flare_plugin_protocol::state::SharedState {
            entity: "poll-1".into(),
            version,
            values: serde_json::json!({"voterCount": count})
                .as_object()
                .unwrap()
                .clone(),
        };
        let voters = |message: &IMMessage| {
            let state = message.plugin_shared_state(None)?;
            Some((state.version, state.values["voterCount"].as_u64()?))
        };
        let store = MemoryMessageStore::new();
        let pending = local_message("", "client-shared-1");
        store.save_one(&pending).await.unwrap();
        assert_eq!(
            store
                .apply_plugin_shared_state("missing", &shared(1, 1))
                .await
                .unwrap(),
            OperationApplyResult::NotFound
        );
        assert_eq!(
            store
                .apply_plugin_shared_state("client-shared-1", &shared(2, 3))
                .await
                .unwrap(),
            OperationApplyResult::Applied
        );
        for stale in [shared(2, 9), shared(1, 9)] {
            assert_eq!(
                store
                    .apply_plugin_shared_state("client-shared-1", &stale)
                    .await
                    .unwrap(),
                OperationApplyResult::IgnoredStale,
                "a duplicate or older version changes nothing"
            );
        }
        let mut invalid = shared(3, 1);
        invalid.entity = "bad entity".into();
        assert!(
            store
                .apply_plugin_shared_state("client-shared-1", &invalid)
                .await
                .is_err()
        );

        let mut acked = pending;
        acked.server_id = "server-shared-1".to_string();
        acked.local_state.sending = false;
        acked.local_state.is_local = false;
        store
            .update_after_ack("client-shared-1", &acked)
            .await
            .unwrap();
        let stored = store.get("server-shared-1").await.unwrap().unwrap();
        assert_eq!(voters(&stored), Some((2, 3)));

        // History carries an older snapshot (no entity): the stored, newer one wins.
        let mut synced = acked.clone();
        synced
            .attributes
            .insert("pluginSharedStateJson".into(), r#"{"voterCount":1}"#.into());
        synced
            .attributes
            .insert("pluginSharedStateVersion".into(), "1".into());
        store.save_one(&synced).await.unwrap();
        let after_sync = store.get("server-shared-1").await.unwrap().unwrap();
        assert_eq!(voters(&after_sync), Some((2, 3)));
        // A newer snapshot from history replaces it; the entity stays with the message.
        synced
            .attributes
            .insert("pluginSharedStateJson".into(), r#"{"voterCount":5}"#.into());
        synced
            .attributes
            .insert("pluginSharedStateVersion".into(), "4".into());
        store.save_one(&synced).await.unwrap();
        let newer = store.get("server-shared-1").await.unwrap().unwrap();
        assert_eq!(voters(&newer), Some((4, 5)));
        assert_eq!(newer.attributes["pluginSharedEntity"], "poll-1");
    }
}
