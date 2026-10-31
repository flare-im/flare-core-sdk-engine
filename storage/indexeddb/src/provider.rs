//! WASM store provider: memory hot path + optional JS IndexedDB persistence.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use flare_im_core_sdk::Result;
use flare_im_core_sdk::model::Conversation;
use flare_im_core_sdk::model::IMMessage;
use flare_im_core_sdk::model::MessagePreviewElem;
use flare_im_core_sdk::model::message::ReactionEntry;
use flare_im_core_sdk::plugin::platform::state::SharedState;
use flare_im_core_sdk::plugin::platform::ui::PluginMessageState;
use flare_im_core_sdk::storage::{
    ConversationReader, ConversationWriter, CursorBackedTimelineCoverageStore, MemberKey,
    MessageReader, MessageStore, MessageWriter, OperationApplyResult, ParticipantDisplaySnapshot,
    PendingSendReader, PendingSendVo, PendingSendWriter, SyncCursorReader, SyncCursorWriter,
    TimelineCoverageStore, stored_name_is_an_id,
};
use flare_im_core_sdk::storage::{MemoryConversationStore, MemoryMessageStore};
use flare_im_core_sdk::storage::{MemoryPendingSendStore, MemoryUserProfileStore};
use flare_im_core_sdk::storage::{MemorySyncCursorStore, StoreProvider, in_memory_im_provider};
use wasm_bindgen::JsCast;

use super::host::{
    delete_conversation, delete_message, delete_pending_send, load_snapshot, persist_conversation,
    persist_cursor, persist_message, persist_pending_send, storage_host_configured,
};

fn spawn_persist<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

fn log_storage_error(operation: &str, error: &flare_im_core_sdk::FlareError) {
    log_storage_console("error", operation, &error.to_string());
}

fn log_storage_console(level: &str, operation: &str, detail: &str) {
    let global = js_sys::global();
    let Ok(console) = js_sys::Reflect::get(&global, &wasm_bindgen::JsValue::from_str("console"))
    else {
        return;
    };
    let Ok(error_fn) = js_sys::Reflect::get(&console, &wasm_bindgen::JsValue::from_str(level))
    else {
        return;
    };
    let Some(error_fn) = error_fn.dyn_ref::<js_sys::Function>() else {
        return;
    };
    let _ = error_fn.call2(
        &console,
        &wasm_bindgen::JsValue::from_str("[flare-core-storage]"),
        &wasm_bindgen::JsValue::from_str(&format!("{operation}: {detail}")),
    );
}

fn persist_conversation_spawn(user_id: &str, conversation: Conversation) {
    let user_id = user_id.to_string();
    spawn_persist(async move {
        let _ = persist_conversation(&user_id, &conversation).await;
    });
}

fn persist_cursor_spawn(user_id: &str, key: String, value: String) {
    let user_id = user_id.to_string();
    spawn_persist(async move {
        let _ = persist_cursor(&user_id, &key, &value).await;
    });
}

struct PersistingMessageStore {
    user_id: String,
    inner: Arc<MemoryMessageStore>,
    conversations: Arc<MemoryConversationStore>,
}

struct PersistingConversationStore {
    user_id: String,
    inner: Arc<MemoryConversationStore>,
    messages: Arc<MemoryMessageStore>,
    timeline_coverage: Arc<dyn TimelineCoverageStore>,
}

struct PersistingSyncCursorStore {
    user_id: String,
    inner: Arc<MemorySyncCursorStore>,
}

struct PersistingPendingSendStore {
    user_id: String,
    inner: Arc<MemoryPendingSendStore>,
}

impl PersistingMessageStore {
    fn new(
        user_id: String,
        inner: Arc<MemoryMessageStore>,
        conversations: Arc<MemoryConversationStore>,
    ) -> Self {
        Self {
            user_id,
            inner,
            conversations,
        }
    }

    async fn get_by_any_message_id(&self, message_id: &str) -> Result<Option<IMMessage>> {
        if let Some(message) = self.inner.get(message_id).await? {
            return Ok(Some(message));
        }
        self.inner.get_by_client_msg_id(message_id).await
    }

    fn persist_message_spawn(&self, message: IMMessage) {
        let user_id = self.user_id.clone();
        spawn_persist(async move {
            if let Err(error) = persist_message(&user_id, &message).await {
                log_storage_error("persist_message", &error);
            }
        });
    }

    async fn persist_message_now(&self, message: &IMMessage) -> Result<()> {
        let user_id = self.user_id.clone();
        let message = message.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        spawn_persist(async move {
            let result = persist_message(&user_id, &message).await;
            let _ = tx.send(result);
        });
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                log_storage_error("persist_message", &error);
                Err(error)
            }
            Err(_) => Err(flare_im_core_sdk::FlareError::system(
                "persist message task was cancelled",
            )),
        }
    }

    async fn persist_outgoing_messages_in_conversation(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
    ) -> Result<()> {
        let messages = self
            .inner
            .get_by_conversation(conversation_id, 0, u32::MAX)
            .await?;
        for message in messages {
            if message.sender_id == sender_user_id {
                self.persist_message_spawn(message);
            }
        }
        Ok(())
    }
}

impl PersistingConversationStore {
    fn new(
        user_id: String,
        inner: Arc<MemoryConversationStore>,
        messages: Arc<MemoryMessageStore>,
        timeline_coverage: Arc<dyn TimelineCoverageStore>,
    ) -> Self {
        Self {
            user_id,
            inner,
            messages,
            timeline_coverage,
        }
    }
}

async fn repair_single_chat_message_aliases(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversations: &Arc<MemoryConversationStore>,
) -> Result<u64> {
    let conversations = conversations.list().await?;
    let mut moved = 0_u64;
    for conversation in conversations {
        moved +=
            repair_single_chat_message_alias_for_conversation(user_id, messages, &conversation)
                .await?;
    }
    Ok(moved)
}

async fn repair_single_chat_message_alias_for_conversation(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversation: &Conversation,
) -> Result<u64> {
    if !conversation.conversation_type.is_single_chat_conversation() {
        return Ok(0);
    }
    let to = conversation.conversation_id.trim();
    let from = conversation.channel_id.trim();
    if from.is_empty() || to.is_empty() || from == to {
        return Ok(0);
    }

    let mut moved_messages = messages.get_by_conversation(from, 0, u32::MAX).await?;
    if moved_messages.is_empty() {
        return Ok(0);
    }
    let moved = messages.rewrite_conversation_id(from, to).await?;
    if moved > 0 {
        let user_id = user_id.to_string();
        let to = to.to_string();
        spawn_persist(async move {
            for message in &mut moved_messages {
                message.conversation_id = to.clone();
                let _ = persist_message(&user_id, message).await;
            }
        });
    }
    Ok(moved)
}

fn latest_message_id(message: &IMMessage) -> String {
    if !message.server_id.trim().is_empty() {
        return message.server_id.clone();
    }
    message.client_msg_id.clone()
}

fn latest_message_preview(message: &IMMessage) -> Option<String> {
    message.text_for_storage().or_else(|| {
        let preview = message.text_preview.trim();
        (!preview.is_empty()).then(|| preview.to_string())
    })
}

fn hydrate_conversation_latest(
    conversation: &Conversation,
    latest: &IMMessage,
) -> Option<Conversation> {
    let message_id = latest_message_id(latest);
    if message_id.trim().is_empty() {
        return None;
    }

    let preview = latest_message_preview(latest);
    let time = latest.display_time_ms();
    let sender_id = latest.sender_id.clone();
    let max_seq = latest.conversation_seq;
    let latest_preview = MessagePreviewElem {
        message_id: message_id.clone(),
        sender_id: sender_id.clone(),
        r#type: latest.message_type,
        text: preview.clone().unwrap_or_default(),
        time,
    };

    let unchanged = conversation.last_message_id.as_deref() == Some(message_id.as_str())
        && conversation.last_sender_id.as_deref() == Some(sender_id.as_str())
        && conversation.last_message_at == Some(time)
        && conversation.last_message_preview.as_deref() == preview.as_deref()
        && conversation.last_message.as_ref().is_some_and(|current| {
            current.message_id == latest_preview.message_id
                && current.sender_id == latest_preview.sender_id
                && current.r#type == latest_preview.r#type
                && current.text == latest_preview.text
                && current.time == latest_preview.time
        })
        && conversation.max_seq >= max_seq;

    if unchanged {
        return None;
    }

    let mut updated = conversation.clone();
    updated.last_message_id = Some(message_id);
    updated.last_sender_id = Some(sender_id);
    updated.last_message_at = Some(time);
    updated.last_message_preview = preview;
    updated.last_message = Some(latest_preview);
    updated.max_seq = updated.max_seq.max(max_seq);
    updated.updated_at = updated.updated_at.max(time);
    updated.updated_at_ts = Some(updated.updated_at_ts.unwrap_or(0).max(time));
    Some(updated)
}

async fn hydrate_conversation_latest_from_messages(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversations: &Arc<MemoryConversationStore>,
    conversation: &Conversation,
) -> Result<Option<Conversation>> {
    let conversation_id = conversation.conversation_id.trim();
    if conversation_id.is_empty() {
        return Ok(None);
    }
    let latest = messages.get_by_conversation(conversation_id, 0, 1).await?;
    let Some(latest) = latest.first() else {
        return Ok(None);
    };
    let Some(hydrated) = hydrate_conversation_latest(conversation, latest) else {
        return Ok(None);
    };
    ConversationWriter::save_one(conversations.as_ref(), &hydrated).await?;
    persist_conversation_spawn(user_id, hydrated.clone());
    Ok(Some(hydrated))
}

async fn hydrate_all_conversation_latest_from_messages(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversations: &Arc<MemoryConversationStore>,
) -> Result<()> {
    let list = conversations.list().await?;
    for conversation in list {
        let _ = hydrate_conversation_latest_from_messages(
            user_id,
            messages,
            conversations,
            &conversation,
        )
        .await?;
    }
    Ok(())
}

impl PersistingSyncCursorStore {
    fn new(user_id: String, inner: Arc<MemorySyncCursorStore>) -> Self {
        Self { user_id, inner }
    }
}

impl PersistingPendingSendStore {
    fn new(user_id: String, inner: Arc<MemoryPendingSendStore>) -> Self {
        Self { user_id, inner }
    }
}

#[async_trait]
impl MessageReader for PersistingMessageStore {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        self.inner.get(message_id).await
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        self.inner.get_by_client_msg_id(client_msg_id).await
    }

    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        if let Some(conversation) = self.conversations.get(conversation_id).await? {
            repair_single_chat_message_alias_for_conversation(
                &self.user_id,
                &self.inner,
                &conversation,
            )
            .await?;
        }
        self.inner
            .get_by_conversation(conversation_id, before_seq, limit)
            .await
    }

    async fn get_by_conversation_after(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        if let Some(conversation) = self.conversations.get(conversation_id).await? {
            repair_single_chat_message_alias_for_conversation(
                &self.user_id,
                &self.inner,
                &conversation,
            )
            .await?;
        }
        self.inner
            .get_by_conversation_after(conversation_id, after_seq, limit)
            .await
    }

    async fn search_by_query(
        &self,
        query: &flare_im_core_sdk::model::MessageSearchQuery,
    ) -> Result<Vec<IMMessage>> {
        self.inner.search_by_query(query).await
    }

    async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.inner.search(keyword, limit).await
    }

    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.inner
            .search_in_conversation(conversation_id, keyword, limit)
            .await
    }
}

#[async_trait]
impl MessageWriter for PersistingMessageStore {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        self.inner.save_batch(messages).await?;
        for message in messages {
            let lookup_id = if message.server_id.trim().is_empty() {
                message.client_msg_id.trim()
            } else {
                message.server_id.trim()
            };
            let persisted = self
                .get_by_any_message_id(lookup_id)
                .await?
                .unwrap_or_else(|| message.clone());
            self.persist_message_now(&persisted).await?;
        }
        Ok(())
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        MessageWriter::save_batch(self, std::slice::from_ref(message)).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        let affected = self.inner.update_status(message_id, status).await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_spawn(message);
        }
        Ok(affected)
    }

    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool> {
        let updated = self.inner.update_content(message_id, new_content).await?;
        if updated && let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_spawn(message);
        }
        Ok(updated)
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        self.inner.delete(message_id).await?;
        let user_id = self.user_id.clone();
        let message_id = message_id.to_string();
        spawn_persist(async move {
            let _ = delete_message(&user_id, &message_id).await;
        });
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
        let mut moved_messages = self.inner.get_by_conversation(from, 0, u32::MAX).await?;
        let moved = self.inner.rewrite_conversation_id(from, to).await?;
        if moved > 0 {
            let user_id = self.user_id.clone();
            let to = to.to_string();
            spawn_persist(async move {
                for message in &mut moved_messages {
                    message.conversation_id = to.clone();
                    let _ = persist_message(&user_id, message).await;
                }
            });
        }
        Ok(moved)
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        self.inner.update_after_ack(client_msg_id, message).await?;
        // Persist the merged row so local-only attributes (plugin card state) survive the ACK.
        let lookup_id = if message.server_id.trim().is_empty() {
            client_msg_id.trim()
        } else {
            message.server_id.trim()
        };
        let message = self
            .get_by_any_message_id(lookup_id)
            .await?
            .unwrap_or_else(|| message.clone());
        let user_id = self.user_id.clone();
        let client_msg_id = client_msg_id.to_string();
        spawn_persist(async move {
            let _ = delete_message(&user_id, &client_msg_id).await;
            let _ = persist_message(&user_id, &message).await;
        });
        Ok(())
    }
}

#[async_trait]
impl MessageStore for PersistingMessageStore {
    async fn mark_outgoing_read_upto_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        read_seq: u64,
    ) -> Result<()> {
        self.inner
            .mark_outgoing_read_upto_seq(conversation_id, sender_user_id, read_seq)
            .await?;
        self.persist_outgoing_messages_in_conversation(conversation_id, sender_user_id)
            .await
    }

    async fn reconcile_outgoing_read_by_peer_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        peer_read_seq: u64,
    ) -> Result<()> {
        self.inner
            .reconcile_outgoing_read_by_peer_seq(conversation_id, sender_user_id, peer_read_seq)
            .await?;
        self.persist_outgoing_messages_in_conversation(conversation_id, sender_user_id)
            .await
    }

    async fn apply_reaction(
        &self,
        conversation_id: &str,
        message_server_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
    ) -> Result<()> {
        self.inner
            .apply_reaction(conversation_id, message_server_id, user_id, emoji, action)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_server_id).await? {
            self.persist_message_spawn(message);
        }
        Ok(())
    }

    async fn list_reactions(
        &self,
        message_server_ids: &[String],
    ) -> Result<HashMap<String, Vec<ReactionEntry>>> {
        self.inner.list_reactions(message_server_ids).await
    }

    async fn set_message_flag(
        &self,
        message_id: &str,
        flag_key: &str,
        enabled: bool,
    ) -> Result<()> {
        self.inner
            .set_message_flag(message_id, flag_key, enabled)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_spawn(message);
        }
        Ok(())
    }

    async fn apply_pin_event(
        &self,
        message_id: &str,
        enabled: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let applied = self
            .inner
            .apply_pin_event(message_id, enabled, event_seq)
            .await?;
        if applied == OperationApplyResult::Applied
            && let Some(message) = self.get_by_any_message_id(message_id).await?
        {
            self.persist_message_spawn(message);
        }
        Ok(applied)
    }

    async fn apply_plugin_message_state(
        &self,
        message_id: &str,
        state: &PluginMessageState,
    ) -> Result<OperationApplyResult> {
        let applied = self
            .inner
            .apply_plugin_message_state(message_id, state)
            .await?;
        if applied == OperationApplyResult::Applied
            && let Some(message) = self.get_by_any_message_id(message_id).await?
        {
            self.persist_message_spawn(message);
        }
        Ok(applied)
    }

    async fn apply_plugin_shared_state(
        &self,
        message_id: &str,
        state: &SharedState,
    ) -> Result<OperationApplyResult> {
        let applied = self
            .inner
            .apply_plugin_shared_state(message_id, state)
            .await?;
        if applied == OperationApplyResult::Applied
            && let Some(message) = self.get_by_any_message_id(message_id).await?
        {
            self.persist_message_spawn(message);
        }
        Ok(applied)
    }
}

#[async_trait]
impl ConversationReader for PersistingConversationStore {
    async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            repair_single_chat_message_alias_for_conversation(
                &self.user_id,
                &self.messages,
                &conversation,
            )
            .await?;
            if let Some(hydrated) = hydrate_conversation_latest_from_messages(
                &self.user_id,
                &self.messages,
                &self.inner,
                &conversation,
            )
            .await?
            {
                return Ok(Some(hydrated));
            }
        }
        self.inner.get(conversation_id).await
    }

    async fn list(&self) -> Result<Vec<Conversation>> {
        repair_single_chat_message_aliases(&self.user_id, &self.messages, &self.inner).await?;
        hydrate_all_conversation_latest_from_messages(&self.user_id, &self.messages, &self.inner)
            .await?;
        self.inner.list().await
    }

    async fn member_nicknames(&self, pairs: &[MemberKey]) -> Result<HashMap<MemberKey, String>> {
        self.inner.member_nicknames(pairs).await
    }
}

#[async_trait]
impl ConversationWriter for PersistingConversationStore {
    async fn save_batch(&self, conversations: &[Conversation]) -> Result<()> {
        self.inner.save_batch(conversations).await?;
        for conversation in conversations {
            let user_id = self.user_id.clone();
            let conversation_id = conversation.conversation_id.clone();
            if let Some(conversation) = self.inner.get(&conversation_id).await? {
                spawn_persist(async move {
                    let _ = persist_conversation(&user_id, &conversation).await;
                });
            }
        }
        Ok(())
    }

    async fn save_one(&self, conversation: &Conversation) -> Result<()> {
        self.inner.save_one(conversation).await?;
        let user_id = self.user_id.clone();
        if let Some(conversation) = self.inner.get(&conversation.conversation_id).await? {
            spawn_persist(async move {
                let _ = persist_conversation(&user_id, &conversation).await;
            });
        }
        Ok(())
    }

    async fn update_unread(
        &self,
        conversation_id: &str,
        unread_count: u32,
        last_read_seq: u64,
    ) -> Result<()> {
        self.inner
            .update_unread(conversation_id, unread_count, last_read_seq)
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn set_pinned(&self, conversation_id: &str, pinned: bool) -> Result<()> {
        self.inner.set_pinned(conversation_id, pinned).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn set_muted(&self, conversation_id: &str, muted: bool) -> Result<()> {
        self.inner.set_muted(conversation_id, muted).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn set_archived(&self, conversation_id: &str, archived: bool) -> Result<()> {
        self.inner.set_archived(conversation_id, archived).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn mark_unread(&self, conversation_id: &str) -> Result<u32> {
        let unread = self.inner.mark_unread(conversation_id).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(unread)
    }

    async fn update_draft(&self, conversation_id: &str, draft: Option<&str>) -> Result<()> {
        self.inner.update_draft(conversation_id, draft).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn delete(&self, conversation_id: &str) -> Result<()> {
        self.inner.delete(conversation_id).await?;
        // 宿主删会话时连同它的消息一起删；证明这段历史完整的区间不能留下。
        self.timeline_coverage.clear(conversation_id).await?;
        let user_id = self.user_id.clone();
        let conversation_id = conversation_id.to_string();
        spawn_persist(async move {
            let _ = delete_conversation(&user_id, &conversation_id).await;
        });
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
        self.inner.merge_conversation_identity(from, to).await?;
        let user_id = self.user_id.clone();
        let from = from.to_string();
        let merged = self.inner.get(to).await?;
        spawn_persist(async move {
            let _ = delete_conversation(&user_id, &from).await;
            if let Some(conversation) = merged {
                let _ = persist_conversation(&user_id, &conversation).await;
            }
        });
        Ok(())
    }

    async fn clear_local_chat_history(
        &self,
        conversation_id: &str,
        cleared_through_seq: u64,
    ) -> Result<()> {
        self.inner
            .clear_local_chat_history(conversation_id, cleared_through_seq)
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
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
        self.inner
            .update_last_message(
                conversation_id,
                last_message_id,
                last_sender_id,
                last_message_at,
                last_message_preview,
                max_seq,
            )
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn recompute_unread_for_user(
        &self,
        conversation_id: &str,
        current_user_id: &str,
    ) -> Result<()> {
        self.inner
            .recompute_unread_for_user(conversation_id, current_user_id)
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_spawn(&self.user_id, conversation);
        }
        Ok(())
    }

    async fn get_local_max_seq(&self, conversation_id: &str) -> Result<u64> {
        self.inner.get_local_max_seq(conversation_id).await
    }

    /// Member nicknames stay in memory: business SDKs write them again after a reload.
    async fn save_member_nicknames(
        &self,
        snapshots: &[ParticipantDisplaySnapshot],
    ) -> Result<Vec<String>> {
        self.inner.save_member_nicknames(snapshots).await
    }

    async fn apply_user_profile_snapshot(
        &self,
        user_id: &str,
        nickname: &str,
        avatar_url: &str,
    ) -> Result<Vec<String>> {
        let affected = self
            .inner
            .apply_user_profile_snapshot(user_id, nickname, avatar_url)
            .await?;
        for conversation_id in &affected {
            if let Some(conversation) = self.inner.get(conversation_id).await? {
                persist_conversation_spawn(&self.user_id, conversation);
            }
        }
        Ok(affected)
    }
}

#[async_trait]
impl SyncCursorReader for PersistingSyncCursorStore {
    async fn get_raw(&self, key: &str) -> Result<Option<String>> {
        self.inner.get_raw(key).await
    }

    async fn get_conversation_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<flare_im_core_sdk::storage::SyncCursorVo>> {
        self.inner
            .get_conversation_cursor(user_id, conversation_id)
            .await
    }
}

#[async_trait]
impl SyncCursorWriter for PersistingSyncCursorStore {
    async fn save_raw(&self, key: &str, cursor: &str) -> Result<()> {
        self.inner.save_raw(key, cursor).await?;
        persist_cursor_spawn(&self.user_id, key.to_string(), cursor.to_string());
        Ok(())
    }

    async fn save_conversation_cursor(
        &self,
        cursor: &flare_im_core_sdk::storage::SyncCursorVo,
    ) -> Result<()> {
        self.inner.save_conversation_cursor(cursor).await?;
        let key = format!("{}:{}", cursor.user_id, cursor.conversation_id);
        let value = format!("{}:{}", cursor.last_seq, cursor.synced_at);
        persist_cursor_spawn(&self.user_id, key, value);
        Ok(())
    }
}

#[async_trait]
impl PendingSendReader for PersistingPendingSendStore {
    async fn get(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        self.inner.get(client_msg_id).await
    }

    async fn list(&self) -> Result<Vec<PendingSendVo>> {
        self.inner.list().await
    }

    async fn take_oldest(&self) -> Result<Option<PendingSendVo>> {
        self.inner.take_oldest().await
    }

    async fn list_oldest_excluding(
        &self,
        excluded_client_msg_ids: &[String],
        limit: usize,
    ) -> Result<Vec<PendingSendVo>> {
        self.inner
            .list_oldest_excluding(excluded_client_msg_ids, limit)
            .await
    }
}

#[async_trait]
impl PendingSendWriter for PersistingPendingSendStore {
    async fn push(&self, entry: PendingSendVo) -> Result<()> {
        self.inner.push(entry.clone()).await?;
        let user_id = self.user_id.clone();
        spawn_persist(async move {
            let _ = persist_pending_send(&user_id, &entry).await;
        });
        Ok(())
    }

    async fn pop(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let removed = self.inner.pop(client_msg_id).await?;
        if removed.is_some() {
            let user_id = self.user_id.clone();
            let client_msg_id = client_msg_id.to_string();
            spawn_persist(async move {
                let _ = delete_pending_send(&user_id, &client_msg_id).await;
            });
        }
        Ok(removed)
    }
}

pub async fn build_web_store_provider(user_id: &str) -> StoreProvider {
    if !storage_host_configured() {
        let mut store = in_memory_im_provider();
        store.plugin_commands = Some(Arc::new(
            super::plugin_command::IndexedDbPluginCommandStore {
                user_id: user_id.to_owned(),
            },
        ));
        return store;
    }

    let messages = Arc::new(MemoryMessageStore::new());
    let conversations = Arc::new(MemoryConversationStore::new());
    let cursors = Arc::new(MemorySyncCursorStore::new());
    let pending_inner = Arc::new(MemoryPendingSendStore::new());

    if let Ok(snapshot) = load_snapshot(user_id).await {
        if !snapshot.messages.is_empty() {
            let _ = MessageWriter::save_batch(messages.as_ref(), &snapshot.messages).await;
        }
        if !snapshot.conversations.is_empty() {
            // Stores written before names were projected may hold an id as the name; the
            // memory store clears it on save, and the cleared rows are written back once.
            let id_named: Vec<String> = snapshot
                .conversations
                .iter()
                .filter(|conversation| stored_name_is_an_id(conversation))
                .map(|conversation| conversation.conversation_id.clone())
                .collect();
            let _ = ConversationWriter::save_batch(conversations.as_ref(), &snapshot.conversations)
                .await;
            for conversation_id in id_named {
                if let Ok(Some(conversation)) = conversations.get(&conversation_id).await {
                    persist_conversation_spawn(user_id, conversation);
                }
            }
        }
        for (key, value) in snapshot.cursors {
            let _ = SyncCursorWriter::save_raw(cursors.as_ref(), &key, &value).await;
        }
        for entry in snapshot.pending_sends {
            let _ = PendingSendWriter::push(pending_inner.as_ref(), entry).await;
        }
    }
    let _ = repair_single_chat_message_aliases(user_id, &messages, &conversations).await;
    let _ = hydrate_all_conversation_latest_from_messages(user_id, &messages, &conversations).await;

    let user_id = user_id.to_string();
    let pending = Arc::new(PersistingPendingSendStore::new(
        user_id.clone(),
        pending_inner,
    ));
    let user_profiles = Arc::new(MemoryUserProfileStore::new());
    let cursors = Arc::new(PersistingSyncCursorStore::new(user_id.clone(), cursors));
    // 区间存进同步游标键值：经宿主 saveCursor 持久化，随 loadSnapshot 的游标一起载回。
    let timeline_coverage: Arc<dyn TimelineCoverageStore> =
        Arc::new(CursorBackedTimelineCoverageStore::new(cursors.clone()));

    StoreProvider {
        plugin_commands: Some(Arc::new(
            super::plugin_command::IndexedDbPluginCommandStore {
                user_id: user_id.clone(),
            },
        )),
        messages: Arc::new(PersistingMessageStore::new(
            user_id.clone(),
            messages.clone(),
            conversations.clone(),
        )),
        conversations: Arc::new(PersistingConversationStore::new(
            user_id,
            conversations,
            messages,
            timeline_coverage.clone(),
        )),
        conversation_participants: None,
        cursors,
        timeline_coverage,
        pending_send_reader: Some(pending.clone()),
        pending_send_writer: Some(pending),
        upload_manifest_store: None,
        // Browser WASM: rely on gateway presigned URLs + HTTP cache; skip blob IDB cache.
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: Some(user_profiles.clone()),
        user_profiles_writer: Some(user_profiles),
    }
}
