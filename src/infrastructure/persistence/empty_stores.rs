//! 空实现仓储：供无 SQLite 联调组装 [`super::minimal_provider::in_memory_empty_im_provider`]。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::domain::{
    ConversationReader, ConversationWriter, MessageReader, MessageStore, MessageWriter,
};
use crate::model::Conversation;
use crate::model::IMMessage;
use crate::shared::error::Result;

pub struct EmptyMessageStore;

#[async_trait]
impl MessageReader for EmptyMessageStore {
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

    async fn search_by_query(
        &self,
        _query: &crate::model::MessageSearchQuery,
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
impl MessageWriter for EmptyMessageStore {
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

impl MessageStore for EmptyMessageStore {}

pub struct EmptyConversationStore;

#[async_trait]
impl ConversationReader for EmptyConversationStore {
    async fn get(&self, _conversation_id: &str) -> Result<Option<Conversation>> {
        Ok(None)
    }

    async fn list(&self) -> Result<Vec<Conversation>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl ConversationWriter for EmptyConversationStore {
    async fn save_batch(&self, _conversations: &[Conversation]) -> Result<()> {
        Ok(())
    }

    async fn save_one(&self, _conversation: &Conversation) -> Result<()> {
        Ok(())
    }

    async fn update_unread(
        &self,
        _conversation_id: &str,
        _unread_count: u32,
        _last_read_seq: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn set_pinned(&self, _conversation_id: &str, _pinned: bool) -> Result<()> {
        Ok(())
    }

    async fn set_muted(&self, _conversation_id: &str, _muted: bool) -> Result<()> {
        Ok(())
    }

    async fn set_notify_all(&self, _conversation_id: &str, _enabled: bool) -> Result<()> {
        Ok(())
    }

    async fn mark_unread(&self, _conversation_id: &str) -> Result<u32> {
        Ok(1)
    }

    async fn set_archived(&self, _conversation_id: &str, _archived: bool) -> Result<()> {
        Ok(())
    }

    async fn update_draft(&self, _conversation_id: &str, _draft: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _conversation_id: &str) -> Result<()> {
        Ok(())
    }

    async fn merge_conversation_identity(
        &self,
        _from_conversation_id: &str,
        _to_conversation_id: &str,
    ) -> Result<()> {
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
        _conversation_id: &str,
        _last_message_id: &str,
        _last_sender_id: &str,
        _last_message_at: u64,
        _last_message_preview: Option<&str>,
        _max_seq: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn recompute_unread_for_user(
        &self,
        _conversation_id: &str,
        _current_user_id: &str,
    ) -> Result<()> {
        Ok(())
    }
}

pub struct MemorySyncCursorStore {
    data: Arc<RwLock<HashMap<String, String>>>,
}

impl MemorySyncCursorStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemorySyncCursorStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl crate::domain::SyncCursorReader for MemorySyncCursorStore {
    async fn get_raw(&self, key: &str) -> Result<Option<String>> {
        Ok(self.data.read().await.get(key).cloned())
    }

    async fn get_conversation_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<crate::domain::SyncCursorVo>> {
        let key = format!("{user_id}:{conversation_id}");
        let data = self.data.read().await;
        let Some(cursor_str) = data.get(&key) else {
            return Ok(None);
        };
        let Some((seq_str, synced_str)) = cursor_str.split_once(':') else {
            return Ok(None);
        };
        let (Ok(last_seq), Ok(synced_at)) = (seq_str.parse::<u64>(), synced_str.parse::<u64>())
        else {
            return Ok(None);
        };
        Ok(Some(crate::domain::SyncCursorVo {
            user_id: user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            last_seq,
            synced_at,
        }))
    }
}

#[async_trait]
impl crate::domain::SyncCursorWriter for MemorySyncCursorStore {
    async fn save_raw(&self, key: &str, cursor: &str) -> Result<()> {
        self.data
            .write()
            .await
            .insert(key.to_string(), cursor.to_string());
        Ok(())
    }

    async fn save_conversation_cursor(&self, cursor: &crate::domain::SyncCursorVo) -> Result<()> {
        let key = format!("{}:{}", cursor.user_id, cursor.conversation_id);
        let value = format!("{}:{}", cursor.last_seq, cursor.synced_at);
        self.data.write().await.insert(key, value);
        Ok(())
    }
}
