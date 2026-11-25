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
    ConversationReader, ConversationWriter, CursorBackedTimelineCoverageStore, EditApplyResult,
    MemberKey, MessageReader, MessageRetentionPolicy, MessageRetentionState, MessageStore,
    MessageWriter, OperationApplyResult, ParticipantDisplaySnapshot, PendingSendReader,
    PendingSendVo, PendingSendWriter, SyncCursorReader, SyncCursorWriter, TimelineCoverageStore,
    stored_name_is_an_id,
};
use flare_im_core_sdk::storage::{MemoryConversationStore, MemoryMessageStore};
use flare_im_core_sdk::storage::{MemoryPendingSendStore, MemoryUserProfileStore};
use flare_im_core_sdk::storage::{MemorySyncCursorStore, StoreProvider, in_memory_im_provider};

use super::host::{
    MessageRead, delete_conversation, delete_message, delete_pending_send, load_snapshot,
    loads_messages_on_demand, persist_conversation, persist_cursor, persist_message,
    persist_messages, persist_pending_send, read_messages, storage_host_configured,
};

fn spawn_persist<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

async fn reserve_write(
    writes: &Arc<tokio::sync::Semaphore>,
) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
    writes
        .clone()
        .acquire_owned()
        .await
        .map(Arc::new)
        .map_err(|_| flare_im_core_sdk::FlareError::system("storage closed"))
}

fn await_storage_write<T, F>(
    write: F,
    ownership: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> impl std::future::Future<Output = Result<T>> + Send
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T>> + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    spawn_persist(async move {
        // The host write owns serialization even if its caller is cancelled.
        let _ownership = ownership;
        let _ = tx.send(write.await);
    });
    async move {
        rx.await
            .map_err(|_| flare_im_core_sdk::FlareError::system("storage persistence cancelled"))?
    }
}

fn persist_conversation_now(
    user_id: &str,
    conversation: Conversation,
    ownership: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> impl std::future::Future<Output = Result<()>> + Send {
    let user_id = user_id.to_owned();
    await_storage_write(
        async move { persist_conversation(&user_id, &conversation).await },
        ownership,
    )
}

struct PersistingMessageStore {
    user_id: String,
    inner: Arc<MemoryMessageStore>,
    conversations: Arc<MemoryConversationStore>,
    writes: Arc<tokio::sync::Semaphore>,
}

struct PersistingConversationStore {
    user_id: String,
    inner: Arc<MemoryConversationStore>,
    messages: Arc<MemoryMessageStore>,
    timeline_coverage: Arc<dyn TimelineCoverageStore>,
    writes: Arc<tokio::sync::Semaphore>,
}

struct PersistingSyncCursorStore {
    user_id: String,
    inner: Arc<MemorySyncCursorStore>,
    writes: Arc<tokio::sync::Semaphore>,
}

struct PersistingPendingSendStore {
    user_id: String,
    inner: Arc<MemoryPendingSendStore>,
    writes: Arc<tokio::sync::Semaphore>,
}

async fn restore_message_rows(
    user_id: &str,
    store: &MemoryMessageStore,
    query: MessageRead,
) -> Result<()> {
    if !loads_messages_on_demand() {
        return Ok(());
    }
    // The caller holds the store permit while the WASM thread reads from the host.
    let user_id = user_id.to_owned();
    let rows =
        await_storage_write(async move { read_messages(&user_id, query).await }, None).await?;
    store.restore_missing_messages(rows).await;
    Ok(())
}

impl PersistingMessageStore {
    fn new(
        user_id: String,
        inner: Arc<MemoryMessageStore>,
        conversations: Arc<MemoryConversationStore>,
        writes: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            user_id,
            inner,
            conversations,
            writes,
        }
    }

    async fn get_by_any_message_id(&self, message_id: &str) -> Result<Option<IMMessage>> {
        if let Some(message) = self.inner.get(message_id).await? {
            return Ok(Some(message));
        }
        self.inner.get_by_client_msg_id(message_id).await
    }

    async fn reserve_write(&self) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        reserve_write(&self.writes).await
    }

    async fn restore_ids(&self, ids: Vec<String>) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        restore_message_rows(&self.user_id, &self.inner, MessageRead::Ids { ids }).await
    }

    async fn restore_id(&self, id: &str) -> Result<()> {
        if !loads_messages_on_demand() || id.is_empty() {
            return Ok(());
        }
        self.restore_ids(vec![id.to_owned()]).await
    }

    async fn restore_mutation_target(&self, id: &str) -> Result<()> {
        if loads_messages_on_demand()
            && !self
                .inner
                .get(id)
                .await?
                .is_some_and(|row| row.server_id == id)
        {
            self.restore_id(id).await?;
        }
        Ok(())
    }

    async fn restore_window(
        &self,
        conversation_id: &str,
        before_seq: Option<u64>,
        after_seq: Option<u64>,
        limit: Option<u32>,
        oldest_seq: Option<u64>,
    ) -> Result<()> {
        restore_message_rows(
            &self.user_id,
            &self.inner,
            MessageRead::Timeline {
                conversation_id: conversation_id.to_owned(),
                // Bounds above the largest safely stored JS sequence still delimit
                // the range, including Core's sentinel for confirmed history.
                before_seq: before_seq.map(|seq| seq as f64),
                after_seq: after_seq.map(|seq| seq as f64),
                limit,
                oldest_seq: oldest_seq.map(|seq| seq as f64),
            },
        )
        .await
    }

    async fn persist_message_now(
        &self,
        message: &IMMessage,
        ownership: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<()> {
        let user_id = self.user_id.clone();
        let message = message.clone();
        let conversation = self.retention_preview(&message).await?;
        await_storage_write(
            async move {
                persist_message(&user_id, &message).await?;
                if let Some(conversation) = conversation {
                    persist_conversation(&user_id, &conversation).await?;
                }
                Ok(())
            },
            Some(ownership),
        )
        .await
    }

    async fn retention_preview(&self, message: &IMMessage) -> Result<Option<Conversation>> {
        if message.retention_state.is_none() {
            return Ok(None);
        }
        let latest_id = latest_message_id(message);
        Ok(self
            .conversations
            .get(&message.conversation_id)
            .await?
            .filter(|conversation| conversation.last_message_id.as_deref() == Some(&latest_id)))
    }

    async fn persist_outgoing_read_updates(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        read_seq: u64,
        ownership: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<()> {
        self.restore_window(conversation_id, None, None, None, None)
            .await?;
        for message in self
            .inner
            .outgoing_read_updates(conversation_id, sender_user_id, read_seq)
            .await
        {
            self.persist_message_now(&message, ownership.clone())
                .await?;
            self.inner.save_one(&message).await?;
        }
        Ok(())
    }
}

impl PersistingConversationStore {
    async fn reserve_write(&self) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        reserve_write(&self.writes).await
    }

    fn new(
        user_id: String,
        inner: Arc<MemoryConversationStore>,
        messages: Arc<MemoryMessageStore>,
        timeline_coverage: Arc<dyn TimelineCoverageStore>,
        writes: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            user_id,
            inner,
            messages,
            timeline_coverage,
            writes,
        }
    }
}

async fn repair_single_chat_message_aliases(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversations: &Arc<MemoryConversationStore>,
    writes: &Arc<tokio::sync::Semaphore>,
    ownership: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> Result<u64> {
    let mut moved = 0;
    for conversation in conversations.list().await? {
        moved += repair_single_chat_message_alias_for_conversation(
            user_id,
            messages,
            &conversation,
            writes,
            ownership.clone(),
        )
        .await?;
    }
    Ok(moved)
}

async fn repair_single_chat_message_alias_for_conversation(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversation: &Conversation,
    writes: &Arc<tokio::sync::Semaphore>,
    ownership: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> Result<u64> {
    if !conversation.conversation_type.is_single_chat_conversation() {
        return Ok(0);
    }
    let to = conversation.conversation_id.trim();
    let from = conversation.channel_id.trim();
    if from.is_empty() || to.is_empty() || from == to {
        return Ok(0);
    }
    if !loads_messages_on_demand() && messages.get_by_conversation(from, 0, 1).await?.is_empty() {
        return Ok(0);
    }
    let ownership = match ownership {
        Some(ownership) => ownership,
        None => reserve_write(writes).await?,
    };
    if loads_messages_on_demand() {
        restore_message_rows(
            user_id,
            messages,
            MessageRead::Timeline {
                conversation_id: from.to_owned(),
                before_seq: None,
                after_seq: None,
                limit: Some(1),
                oldest_seq: None,
            },
        )
        .await?;
        if messages.get_by_conversation(from, 0, 1).await?.is_empty() {
            return Ok(0);
        }
        restore_message_rows(
            user_id,
            messages,
            MessageRead::Timeline {
                conversation_id: from.to_owned(),
                before_seq: None,
                after_seq: None,
                limit: None,
                oldest_seq: None,
            },
        )
        .await?;
    }
    let mut moved_messages = messages.get_by_conversation(from, 0, u32::MAX).await?;
    let moved = moved_messages.len() as u64;
    let (user_id, from, to, messages) = (
        user_id.to_owned(),
        from.to_owned(),
        to.to_owned(),
        messages.clone(),
    );
    await_storage_write(
        async move {
            for message in &mut moved_messages {
                message.conversation_id = to.clone();
                persist_message(&user_id, message).await?;
            }
            messages.rewrite_conversation_id(&from, &to).await?;
            Ok(())
        },
        Some(ownership),
    )
    .await?;
    Ok(moved)
}

fn latest_message_id(message: &IMMessage) -> String {
    if !message.server_id.trim().is_empty() {
        return message.server_id.clone();
    }
    message.client_msg_id.clone()
}

fn latest_message_preview(message: &IMMessage) -> Option<String> {
    if message
        .retention_state
        .as_ref()
        .is_some_and(flare_im_core_sdk::storage::retention_hides_content)
    {
        return None;
    }
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

/// A batch stages one visible preview candidate per conversation. Unread and replay
/// receipts are written independently by ApplyInbound after the host commits these rows.
fn batch_previews(rows: &[IMMessage]) -> Vec<Conversation> {
    let mut latest = HashMap::<&str, &IMMessage>::new();
    for row in rows {
        if row.conversation_id.trim().is_empty() || row.conversation_id.starts_with("sync:") {
            continue;
        }
        let current = latest.entry(&row.conversation_id).or_insert(row);
        if IMMessage::compare_for_timeline_asc(row, current).is_ge() {
            *current = row;
        }
    }
    latest
        .into_values()
        .filter_map(|message| {
            let shell = Conversation {
                conversation_id: message.conversation_id.clone(),
                conversation_type:
                    flare_im_core_sdk::model::conversation::ConversationType::from_proto_int(
                        message.conversation_type,
                    ),
                business_type: flare_im_core_sdk::storage::projected_business_type(message).into(),
                channel_id: message.channel_id.clone(),
                created_at: message.display_time_ms(),
                ..Default::default()
            };
            hydrate_conversation_latest(&shell, message)
        })
        .collect()
}

async fn hydrate_conversation_latest_from_messages(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversations: &Arc<MemoryConversationStore>,
    conversation: &Conversation,
    ownership: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
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
    let Some(hydrated) = conversations.get(conversation_id).await? else {
        return Ok(None);
    };
    persist_conversation_now(user_id, hydrated.clone(), ownership).await?;
    Ok(Some(hydrated))
}

async fn hydrate_all_conversation_latest_from_messages(
    user_id: &str,
    messages: &Arc<MemoryMessageStore>,
    conversations: &Arc<MemoryConversationStore>,
    ownership: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> Result<()> {
    let list = conversations.list().await?;
    for conversation in list {
        let _ = hydrate_conversation_latest_from_messages(
            user_id,
            messages,
            conversations,
            &conversation,
            ownership.clone(),
        )
        .await?;
    }
    Ok(())
}

impl PersistingSyncCursorStore {
    fn new(user_id: String, inner: Arc<MemorySyncCursorStore>) -> Self {
        Self {
            user_id,
            inner,
            writes: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

struct PersistingOutgoingStore {
    user_id: String,
    messages: Arc<MemoryMessageStore>,
    memory: Arc<flare_im_core_sdk::storage::MemoryOutgoingStore>,
    writes: Arc<tokio::sync::Semaphore>,
}

impl PersistingOutgoingStore {
    async fn commit(&self, message: IMMessage, entry: Option<PendingSendVo>) -> Result<bool> {
        let permit = reserve_write(&self.writes).await?;
        let user = self.user_id.clone();
        let memory = self.memory.clone();
        let messages = self.messages.clone();
        await_storage_write(
            async move {
                restore_message_rows(
                    &user,
                    &messages,
                    MessageRead::Ids {
                        ids: vec![message.server_id.clone(), message.client_msg_id.clone()],
                    },
                )
                .await?;
                let outbox = entry.clone();
                memory
                    .commit_with(&message, entry.as_ref(), |row, summary| async move {
                        super::host::commit_outgoing(&user, row, summary, outbox).await
                    })
                    .await
            },
            Some(permit),
        )
        .await
    }
}

#[async_trait]
impl flare_im_core_sdk::storage::OutgoingSendStore for PersistingOutgoingStore {
    fn survives_restart(&self) -> bool {
        true
    }

    async fn enqueue_send(&self, message: &IMMessage, entry: &PendingSendVo) -> Result<()> {
        self.commit(message.clone(), Some(entry.clone()))
            .await
            .map(|_| ())
    }
    async fn complete_send(&self, message: &IMMessage) -> Result<bool> {
        self.commit(message.clone(), None).await
    }
}

#[async_trait]
impl MessageReader for PersistingMessageStore {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        if loads_messages_on_demand() {
            let _write = self.reserve_write().await?;
            if let Some(message) = self.inner.get(message_id).await? {
                return Ok(Some(message));
            }
            self.restore_id(message_id).await?;
            return self.inner.get(message_id).await;
        }
        self.inner.get(message_id).await
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        let _write = self.reserve_write().await?;
        self.restore_id(client_msg_id).await?;
        self.inner.get_by_client_msg_id(client_msg_id).await
    }

    async fn get_by_message_ids(&self, message_ids: &[String]) -> Result<Vec<IMMessage>> {
        let _write = self.reserve_write().await?;
        if loads_messages_on_demand() {
            self.restore_ids(self.inner.missing_message_ids(message_ids).await)
                .await?;
        }
        self.inner.get_by_message_ids(message_ids).await
    }

    async fn get_by_client_msg_ids(&self, client_msg_ids: &[String]) -> Result<Vec<IMMessage>> {
        let _write = self.reserve_write().await?;
        if loads_messages_on_demand() {
            self.restore_ids(client_msg_ids.to_vec()).await?;
        }
        self.inner.get_by_client_msg_ids(client_msg_ids).await
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
        let _write = self.reserve_write().await?;
        if let Some(conversation) = self.conversations.get(conversation_id).await? {
            repair_single_chat_message_alias_for_conversation(
                &self.user_id,
                &self.inner,
                &conversation,
                &self.writes,
                Some(_write.clone()),
            )
            .await?;
        }
        let before = (before_seq > 0 && before_seq < i64::MAX as u64).then_some(before_seq);
        self.restore_window(conversation_id, before, None, Some(limit), None)
            .await?;
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
        if limit == 0 {
            return Ok(Vec::new());
        }
        let _write = self.reserve_write().await?;
        if let Some(conversation) = self.conversations.get(conversation_id).await? {
            repair_single_chat_message_alias_for_conversation(
                &self.user_id,
                &self.inner,
                &conversation,
                &self.writes,
                Some(_write.clone()),
            )
            .await?;
        }
        self.restore_window(conversation_id, None, Some(after_seq), Some(limit), None)
            .await?;
        self.inner
            .get_by_conversation_after(conversation_id, after_seq, limit)
            .await
    }

    async fn get_latest_window_through(
        &self,
        conversation_id: &str,
        oldest_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        if !loads_messages_on_demand() {
            return self.get_by_conversation(conversation_id, 0, limit).await;
        }
        let _write = self.reserve_write().await?;
        self.restore_window(conversation_id, None, None, Some(limit), Some(oldest_seq))
            .await?;
        let mut rows = self
            .inner
            .get_by_conversation(conversation_id, 0, limit)
            .await?;
        if let Some(below) = rows
            .iter()
            .position(|row| row.conversation_seq > 0 && row.conversation_seq < oldest_seq)
        {
            rows.truncate(below + 1);
        }
        Ok(rows)
    }

    async fn oldest_conversation_seq(&self, conversation_id: &str) -> Result<Option<u64>> {
        Ok(self
            .get_by_conversation_after(conversation_id, 0, 1)
            .await?
            .first()
            .map(|row| row.conversation_seq))
    }

    async fn search_by_query(
        &self,
        query: &flare_im_core_sdk::model::MessageSearchQuery,
    ) -> Result<Vec<IMMessage>> {
        let _write = self.reserve_write().await?;
        if let Some(conversation_id) = query
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            self.restore_window(conversation_id, None, None, None, None)
                .await?;
        } else {
            restore_message_rows(&self.user_id, &self.inner, MessageRead::All).await?;
        }
        self.inner
            .search_visible(query, self.conversations.as_ref())
            .await
    }

    async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.search_by_query(&flare_im_core_sdk::model::MessageSearchQuery::text(
            keyword, limit,
        ))
        .await
    }

    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.search_by_query(
            &flare_im_core_sdk::model::MessageSearchQuery::in_conversation(
                conversation_id,
                keyword,
                limit,
            ),
        )
        .await
    }
}

#[async_trait]
impl MessageWriter for PersistingMessageStore {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let ownership = self.reserve_write().await?;
        if loads_messages_on_demand() {
            self.restore_ids(
                messages
                    .iter()
                    .flat_map(|message| [message.server_id.clone(), message.client_msg_id.clone()])
                    .collect(),
            )
            .await?;
        }
        self.inner.save_batch(messages).await?;
        let mut rows = Vec::with_capacity(messages.len());
        let mut previews = HashMap::new();
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
            if let Some(conversation) = self.retention_preview(&persisted).await? {
                previews.insert(persisted.conversation_id.clone(), conversation);
            }
            rows.push(persisted);
        }
        let conversations = self.conversations.clone();
        let user_id = self.user_id.clone();
        await_storage_write(
            async move {
                let staged = batch_previews(&rows);
                if let Some(committed) = persist_messages(&user_id, rows, staged).await? {
                    conversations.install_staged_previews(committed).await;
                } else {
                    for conversation in previews.into_values() {
                        persist_conversation(&user_id, &conversation).await?;
                    }
                }
                Ok(())
            },
            Some(ownership),
        )
        .await
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        MessageWriter::save_batch(self, std::slice::from_ref(message)).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let affected = self.inner.update_status(message_id, status).await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(affected)
    }

    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let updated = self.inner.update_content(message_id, new_content).await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(updated)
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let Some(message) = self
            .get_by_any_message_id(message_id)
            .await?
            .filter(|message| message.server_id == message_id || message.local_state.is_local)
        else {
            return Ok(());
        };
        let user_id = self.user_id.clone();
        let message_id = latest_message_id(&message);
        let inner = self.inner.clone();
        await_storage_write(
            async move {
                delete_message(&user_id, &message_id).await?;
                inner.delete(&message_id).await
            },
            Some(ownership),
        )
        .await?;
        Ok(())
    }

    async fn rewrite_conversation_id(
        &self,
        from_conversation_id: &str,
        to_conversation_id: &str,
    ) -> Result<u64> {
        let ownership = self.reserve_write().await?;
        let from = from_conversation_id.trim();
        let to = to_conversation_id.trim();
        if from.is_empty() || to.is_empty() || from == to {
            return Ok(0);
        }
        self.restore_window(from, None, None, None, None).await?;
        let moved = self.inner.rewrite_conversation_id(from, to).await?;
        // Include rows moved by an earlier attempt whose host write failed.
        let moved_messages = self.inner.get_by_conversation(to, 0, u32::MAX).await?;
        for message in moved_messages {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(moved)
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        let ownership = self.reserve_write().await?;
        if loads_messages_on_demand() {
            self.restore_ids(vec![client_msg_id.to_owned(), message.server_id.clone()])
                .await?;
        }
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
        self.persist_message_now(&message, ownership).await?;
        Ok(())
    }
}

#[async_trait]
impl MessageStore for PersistingMessageStore {
    async fn apply_edit_event(
        &self,
        message_id: &str,
        new_content: Vec<u8>,
        event_seq: Option<u64>,
    ) -> Result<EditApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_edit_event(message_id, new_content, event_seq)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership).await?;
        }
        Ok(applied)
    }

    async fn apply_mark_event(
        &self,
        message_id: &str,
        mark_type: i32,
        color: Option<&str>,
        set_mark: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_mark_event(message_id, mark_type, color, set_mark, event_seq)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership).await?;
        }
        Ok(applied)
    }

    async fn apply_reaction_event(
        &self,
        conversation_id: &str,
        message_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_reaction_event(
                conversation_id,
                message_id,
                user_id,
                emoji,
                action,
                event_seq,
            )
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership).await?;
        }
        Ok(applied)
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
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_retention_scheduled_event(message_id, policy, state, scheduled_at, event_seq)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership).await?;
        }
        Ok(applied)
    }

    async fn apply_retention_expired_event(
        &self,
        message_id: &str,
        state: &MessageRetentionState,
        expired_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_retention_expired_event(message_id, state, expired_at, event_seq)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership).await?;
        }
        Ok(applied)
    }

    async fn apply_retention_purged_event(
        &self,
        message_id: &str,
        state: &MessageRetentionState,
        purged_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_retention_purged_event(message_id, state, purged_at, event_seq)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership).await?;
        }
        Ok(applied)
    }

    async fn mark_outgoing_read_upto_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        read_seq: u64,
    ) -> Result<()> {
        let ownership = self.reserve_write().await?;
        self.persist_outgoing_read_updates(conversation_id, sender_user_id, read_seq, ownership)
            .await
    }

    async fn reconcile_outgoing_read_by_peer_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        peer_read_seq: u64,
    ) -> Result<()> {
        let ownership = self.reserve_write().await?;
        self.persist_outgoing_read_updates(
            conversation_id,
            sender_user_id,
            peer_read_seq,
            ownership,
        )
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
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_server_id).await?;
        self.inner
            .apply_reaction(conversation_id, message_server_id, user_id, emoji, action)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_server_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(())
    }

    async fn list_reactions(
        &self,
        message_server_ids: &[String],
    ) -> Result<HashMap<String, Vec<ReactionEntry>>> {
        let _write = self.reserve_write().await?;
        if loads_messages_on_demand() {
            self.restore_ids(self.inner.missing_message_ids(message_server_ids).await)
                .await?;
        }
        self.inner.list_reactions(message_server_ids).await
    }

    async fn set_message_flag(
        &self,
        message_id: &str,
        flag_key: &str,
        enabled: bool,
    ) -> Result<()> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        self.inner
            .set_message_flag(message_id, flag_key, enabled)
            .await?;
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(())
    }

    async fn apply_pin_event(
        &self,
        message_id: &str,
        enabled: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_pin_event(message_id, enabled, event_seq)
            .await?;
        // Retry persistence even when memory already applied this version before a failed host write.
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(applied)
    }

    async fn apply_plugin_message_state(
        &self,
        message_id: &str,
        state: &PluginMessageState,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_plugin_message_state(message_id, state)
            .await?;
        // Retry persistence even when memory already applied this version before a failed host write.
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(applied)
    }

    async fn apply_plugin_shared_state(
        &self,
        message_id: &str,
        state: &SharedState,
    ) -> Result<OperationApplyResult> {
        let ownership = self.reserve_write().await?;
        self.restore_mutation_target(message_id).await?;
        let applied = self
            .inner
            .apply_plugin_shared_state(message_id, state)
            .await?;
        // Retry persistence even when memory already applied this version before a failed host write.
        if let Some(message) = self.get_by_any_message_id(message_id).await? {
            self.persist_message_now(&message, ownership.clone())
                .await?;
        }
        Ok(applied)
    }
}

#[async_trait]
impl ConversationReader for PersistingConversationStore {
    async fn get_stored_many(&self, conversation_ids: &[String]) -> Result<Vec<Conversation>> {
        let _write = self.reserve_write().await?;
        self.inner.get_many(conversation_ids).await
    }

    async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        let _write = self.reserve_write().await?;
        if loads_messages_on_demand() {
            return self.inner.get(conversation_id).await;
        }
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            repair_single_chat_message_alias_for_conversation(
                &self.user_id,
                &self.messages,
                &conversation,
                &self.writes,
                Some(_write.clone()),
            )
            .await?;
            if let Some(hydrated) = hydrate_conversation_latest_from_messages(
                &self.user_id,
                &self.messages,
                &self.inner,
                &conversation,
                Some(_write.clone()),
            )
            .await?
            {
                return Ok(Some(hydrated));
            }
        }
        self.inner.get(conversation_id).await
    }

    async fn list(&self) -> Result<Vec<Conversation>> {
        let _write = self.reserve_write().await?;
        if loads_messages_on_demand() {
            return self.inner.list().await;
        }
        repair_single_chat_message_aliases(
            &self.user_id,
            &self.messages,
            &self.inner,
            &self.writes,
            Some(_write.clone()),
        )
        .await?;
        hydrate_all_conversation_latest_from_messages(
            &self.user_id,
            &self.messages,
            &self.inner,
            Some(_write.clone()),
        )
        .await?;
        self.inner.list().await
    }

    async fn member_nicknames(&self, pairs: &[MemberKey]) -> Result<HashMap<MemberKey, String>> {
        self.inner.member_nicknames(pairs).await
    }
}

#[async_trait]
impl ConversationWriter for PersistingConversationStore {
    async fn commit_message_projection(
        &self,
        expected: &Conversation,
        projected: &Conversation,
    ) -> Result<bool> {
        let ownership = self.reserve_write().await?;
        let inner = self.inner.clone();
        let user_id = self.user_id.clone();
        let expected = expected.clone();
        let projected = projected.clone();
        await_storage_write(
            async move {
                let Some(current) = inner.get(&expected.conversation_id).await? else {
                    return Ok(false);
                };
                if !flare_im_core_sdk::storage::message_projection_matches(&current, &expected) {
                    return Ok(false);
                }
                let mut next = current;
                flare_im_core_sdk::storage::copy_message_projection(&mut next, &projected);
                persist_conversation(&user_id, &next).await?;
                inner.commit_message_projection(&expected, &projected).await
            },
            Some(ownership),
        )
        .await
    }

    async fn save_batch(&self, conversations: &[Conversation]) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.save_batch(conversations).await?;
        for conversation in conversations {
            let user_id = self.user_id.clone();
            let conversation_id = conversation.conversation_id.clone();
            if let Some(conversation) = self.inner.get(&conversation_id).await? {
                persist_conversation_now(&user_id, conversation, Some(_write.clone())).await?;
            }
        }
        Ok(())
    }

    async fn save_one(&self, conversation: &Conversation) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.save_one(conversation).await?;
        let user_id = self.user_id.clone();
        if let Some(conversation) = self.inner.get(&conversation.conversation_id).await? {
            persist_conversation_now(&user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn update_unread(
        &self,
        conversation_id: &str,
        unread_count: u32,
        last_read_seq: u64,
    ) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner
            .update_unread(conversation_id, unread_count, last_read_seq)
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn set_pinned(&self, conversation_id: &str, pinned: bool) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.set_pinned(conversation_id, pinned).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn set_muted(&self, conversation_id: &str, muted: bool) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.set_muted(conversation_id, muted).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn set_notify_all(&self, conversation_id: &str, enabled: bool) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.set_notify_all(conversation_id, enabled).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn set_archived(&self, conversation_id: &str, archived: bool) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.set_archived(conversation_id, archived).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn mark_unread(&self, conversation_id: &str) -> Result<u32> {
        let _write = self.reserve_write().await?;
        let unread = self.inner.mark_unread(conversation_id).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(unread)
    }

    async fn update_draft(&self, conversation_id: &str, draft: Option<&str>) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.update_draft(conversation_id, draft).await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn delete(&self, conversation_id: &str) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner.delete(conversation_id).await?;
        // 宿主删会话时连同它的消息一起删；证明这段历史完整的区间不能留下。
        self.timeline_coverage.clear(conversation_id).await?;
        let user_id = self.user_id.clone();
        let conversation_id = conversation_id.to_string();
        await_storage_write(
            async move { delete_conversation(&user_id, &conversation_id).await },
            Some(_write.clone()),
        )
        .await?;
        Ok(())
    }

    async fn merge_conversation_identity(
        &self,
        from_conversation_id: &str,
        to_conversation_id: &str,
    ) -> Result<()> {
        let _write = self.reserve_write().await?;
        let from = from_conversation_id.trim();
        let to = to_conversation_id.trim();
        if from.is_empty() || to.is_empty() || from == to {
            return Ok(());
        }
        self.inner.merge_conversation_identity(from, to).await?;
        let user_id = self.user_id.clone();
        let from = from.to_string();
        let merged = self.inner.get(to).await?;
        await_storage_write(
            async move {
                delete_conversation(&user_id, &from).await?;
                if let Some(conversation) = merged {
                    persist_conversation(&user_id, &conversation).await?;
                }
                Ok(())
            },
            Some(_write.clone()),
        )
        .await?;
        Ok(())
    }

    async fn clear_local_chat_history(
        &self,
        conversation_id: &str,
        cleared_through_seq: u64,
    ) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner
            .clear_local_chat_history(conversation_id, cleared_through_seq)
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
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
        let _write = self.reserve_write().await?;
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
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
        }
        Ok(())
    }

    async fn recompute_unread_for_user(
        &self,
        conversation_id: &str,
        current_user_id: &str,
    ) -> Result<()> {
        let _write = self.reserve_write().await?;
        self.inner
            .recompute_unread_for_user(conversation_id, current_user_id)
            .await?;
        if let Some(conversation) = self.inner.get(conversation_id).await? {
            persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
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
        let _write = self.reserve_write().await?;
        self.inner.save_member_nicknames(snapshots).await
    }

    async fn apply_user_profile_snapshot(
        &self,
        user_id: &str,
        nickname: &str,
        avatar_url: &str,
    ) -> Result<Vec<String>> {
        let _write = self.reserve_write().await?;
        let affected = self
            .inner
            .apply_user_profile_snapshot(user_id, nickname, avatar_url)
            .await?;
        for conversation_id in &affected {
            if let Some(conversation) = self.inner.get(conversation_id).await? {
                persist_conversation_now(&self.user_id, conversation, Some(_write.clone())).await?;
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
        let ownership = reserve_write(&self.writes).await?;
        let (user_id, key, cursor, inner) = (
            self.user_id.clone(),
            key.to_owned(),
            cursor.to_owned(),
            self.inner.clone(),
        );
        await_storage_write(
            async move {
                persist_cursor(&user_id, &key, &cursor).await?;
                inner.save_raw(&key, &cursor).await
            },
            Some(ownership),
        )
        .await
    }

    async fn save_conversation_cursor(
        &self,
        cursor: &flare_im_core_sdk::storage::SyncCursorVo,
    ) -> Result<()> {
        let ownership = reserve_write(&self.writes).await?;
        let (user_id, cursor, inner) = (self.user_id.clone(), cursor.clone(), self.inner.clone());
        await_storage_write(
            async move {
                let key = format!("{}:{}", cursor.user_id, cursor.conversation_id);
                let value = format!("{}:{}", cursor.last_seq, cursor.synced_at);
                persist_cursor(&user_id, &key, &value).await?;
                inner.save_conversation_cursor(&cursor).await
            },
            Some(ownership),
        )
        .await
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
        let ownership = reserve_write(&self.writes).await?;
        let (user_id, inner) = (self.user_id.clone(), self.inner.clone());
        await_storage_write(
            async move {
                persist_pending_send(&user_id, &entry).await?;
                inner.push(entry).await
            },
            Some(ownership),
        )
        .await
    }

    async fn pop(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let ownership = reserve_write(&self.writes).await?;
        let (user_id, id, inner) = (
            self.user_id.clone(),
            client_msg_id.to_owned(),
            self.inner.clone(),
        );
        await_storage_write(
            async move {
                delete_pending_send(&user_id, &id).await?;
                inner.pop(&id).await
            },
            Some(ownership),
        )
        .await
    }
}

pub async fn build_web_store_provider(
    namespace: &str,
    tenant_id: &str,
    user_id: &str,
) -> Result<StoreProvider> {
    let account_user_id = user_id.trim().to_owned();
    let scope = flare_im_core_sdk::prelude::local_storage_scope(namespace, tenant_id, user_id);
    let user_id = scope.as_str();
    if !storage_host_configured() {
        let mut store = in_memory_im_provider();
        store.plugin_commands = Some(Arc::new(
            super::plugin_command::IndexedDbPluginCommandStore {
                user_id: account_user_id.clone(),
            },
        ));
        store.plugin_consents = Some(Arc::new(
            super::plugin_consent::IndexedDbPluginConsentStore {
                user_id: account_user_id.clone(),
            },
        ));
        store.plugin_bundle_storage = Some(Arc::new(
            super::plugin_bundle_storage::IndexedDbPluginBundleStorage {
                user_id: account_user_id.clone(),
            },
        ));
        return Ok(store);
    }

    let conversations = Arc::new(MemoryConversationStore::new());
    let messages = Arc::new(MemoryMessageStore::with_conversations(
        conversations.clone(),
    ));
    // Message bodies and conversation previews must finish writing in the same order.
    let writes = Arc::new(tokio::sync::Semaphore::new(1));
    let cursors = Arc::new(MemorySyncCursorStore::new());
    let pending_inner = Arc::new(MemoryPendingSendStore::new());

    {
        let snapshot = load_snapshot(user_id).await?;
        if !snapshot.messages.is_empty() {
            let restored: Vec<IMMessage> = snapshot
                .messages
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?;
            MessageWriter::save_batch(messages.as_ref(), &restored).await?;
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
            conversations
                .restore_local_snapshot(&snapshot.conversations)
                .await?;
            if !snapshot.preview_repairs.is_empty() {
                let repaired = snapshot
                    .preview_repairs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<IMMessage>>>()?;
                if let Some(staged) =
                    persist_messages(user_id, vec![], batch_previews(&repaired)).await?
                {
                    conversations.install_staged_previews(staged).await;
                }
            }
            for conversation_id in id_named {
                if let Some(conversation) = conversations.get(&conversation_id).await? {
                    persist_conversation_now(user_id, conversation, None).await?;
                }
            }
        }
        for (key, value) in snapshot.cursors {
            SyncCursorWriter::save_raw(cursors.as_ref(), &key, &value).await?;
        }
        for entry in snapshot.pending_sends {
            PendingSendWriter::push(pending_inner.as_ref(), entry.try_into()?).await?;
        }
    }
    if !loads_messages_on_demand() {
        repair_single_chat_message_aliases(user_id, &messages, &conversations, &writes, None)
            .await?;
        hydrate_all_conversation_latest_from_messages(user_id, &messages, &conversations, None)
            .await?;
    }

    let user_id = user_id.to_string();
    let outgoing = Arc::new(PersistingOutgoingStore {
        user_id: user_id.clone(),
        messages: messages.clone(),
        writes: writes.clone(),
        memory: Arc::new(flare_im_core_sdk::storage::MemoryOutgoingStore::new(
            messages.clone(),
            pending_inner.clone(),
        )),
    });
    let pending = Arc::new(PersistingPendingSendStore {
        user_id: user_id.clone(),
        inner: pending_inner,
        writes: writes.clone(),
    });
    let user_profiles = Arc::new(MemoryUserProfileStore::new());
    let cursors = Arc::new(PersistingSyncCursorStore::new(user_id.clone(), cursors));
    // 区间存进同步游标键值：经宿主 saveCursor 持久化，随 loadSnapshot 的游标一起载回。
    let timeline_coverage: Arc<dyn TimelineCoverageStore> =
        Arc::new(CursorBackedTimelineCoverageStore::new(cursors.clone()));

    Ok(StoreProvider {
        plugin_commands: Some(Arc::new(
            super::plugin_command::IndexedDbPluginCommandStore {
                user_id: account_user_id.clone(),
            },
        )),
        plugin_consents: Some(Arc::new(
            super::plugin_consent::IndexedDbPluginConsentStore {
                user_id: account_user_id.clone(),
            },
        )),
        // Per (tenant, user, plugin) in IndexedDB, removed on sign-out.
        plugin_bundle_storage: Some(Arc::new(
            super::plugin_bundle_storage::IndexedDbPluginBundleStorage {
                user_id: account_user_id.clone(),
            },
        )),
        messages: Arc::new(PersistingMessageStore::new(
            user_id.clone(),
            messages.clone(),
            conversations.clone(),
            writes.clone(),
        )),
        conversations: Arc::new(PersistingConversationStore::new(
            user_id,
            conversations,
            messages,
            timeline_coverage.clone(),
            writes,
        )),
        conversation_participants: None,
        cursors,
        timeline_coverage,
        outgoing: Some(outgoing),
        pending_send_reader: Some(pending.clone()),
        pending_send_writer: Some(pending),
        upload_manifest_store: None,
        // Browser WASM: rely on gateway presigned URLs + HTTP cache; skip blob IDB cache.
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: Some(user_profiles.clone()),
        user_profiles_writer: Some(user_profiles),
    })
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    fn install_regression_host() -> wasm_bindgen::JsValue {
        let host = js_sys::eval(r#"
            globalThis.eventStorage = {messages: [], conversations: [], pendingSends: [], cursors: {}, fail: false, readFail: false, conversationFail: false, writes: 0};
            ({
                loadSnapshot: () => eventStorage.readFail ? Promise.reject('read unavailable') : JSON.stringify(eventStorage),
                saveMessage: ({message}) => {
                    if (eventStorage.fail) return Promise.reject('disk full');
                    eventStorage.writes++;
                    eventStorage.messages = eventStorage.messages.filter(m => m.serverId !== message.serverId);
                    eventStorage.messages.push(message); return Promise.resolve();
                },
                commitOutgoing: () => Promise.reject("unused by this test"),
                savePendingSend: ({entry}) => {eventStorage.pendingSends = [entry]; return Promise.resolve()},
                saveCursor: ({key, value}) => {eventStorage.cursors[key] = value; return Promise.resolve()},
                saveConversation: ({conversation}) => {
                    if (eventStorage.conversationFail) return Promise.reject('conversation disk full');
                    eventStorage.conversations = eventStorage.conversations.filter(c => c.conversationId !== conversation.conversationId);
                    eventStorage.conversations.push(conversation); return Promise.resolve();
                }, deleteMessage: ({id}) => {
                    if (eventStorage.fail) return Promise.reject('disk full');
                    eventStorage.messages = eventStorage.messages.filter(m => m.serverId !== id);
                    return Promise.resolve();
                },
                deleteConversation: () => Promise.resolve(), deletePendingSend: () => Promise.resolve()
            })
        "#).unwrap();
        super::super::host::set_storage_host(&host).unwrap();
        host
    }

    fn regression_message(id: &str, seq: u64) -> IMMessage {
        let mut message = IMMessage::new(Default::default());
        message.server_id = id.into();
        message.client_msg_id = format!("local-{id}");
        message.conversation_id = "c".into();
        message.sender_id = "u".into();
        message.conversation_seq = seq;
        message.status = 2;
        message.encoded_content = flare_im_core_sdk::content::ContentBuilder::text("original")
            .build()
            .encode();
        message
    }

    #[wasm_bindgen_test]
    async fn message_batch_uses_one_host_write_and_restores_all_rows() {
        let host = install_regression_host();
        let batch_write = js_sys::eval(
            r#"(({messages}) => {
                eventStorage.writes++;
                eventStorage.messages.push(...messages);
                return Promise.resolve();
            })"#,
        )
        .unwrap();
        js_sys::Reflect::set(&host, &"saveMessages".into(), &batch_write).unwrap();
        super::super::host::set_storage_host(&host).unwrap();

        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        store
            .messages
            .save_batch(&[
                regression_message("first", 1),
                regression_message("second", 2),
            ])
            .await
            .unwrap();
        assert_eq!(
            js_sys::eval("eventStorage.writes").unwrap().as_f64(),
            Some(1.0)
        );
        let reopened = build_web_store_provider("core", "a", "u").await.unwrap();
        assert!(reopened.messages.get("first").await.unwrap().is_some());
        assert!(reopened.messages.get("second").await.unwrap().is_some());
    }

    #[wasm_bindgen_test]
    async fn lazy_boot_reads_only_metadata_and_fetches_windows_and_unloaded_edits() {
        let host = install_regression_host();
        let seeded = build_web_store_provider("core", "a", "u").await.unwrap();
        seeded
            .conversations
            .save_one(&Conversation::from_conversation_id("c".into()))
            .await
            .unwrap();
        let rows = (1..=10)
            .map(|seq| regression_message(&format!("message-{seq}"), seq))
            .collect::<Vec<_>>();
        seeded.messages.save_batch(&rows).await.unwrap();

        let configure = js_sys::eval(
            r#"((host) => {
                globalThis.messageReads = [];
                host.loadSnapshot = ({ metadataOnly }) => {
                    if (!metadataOnly) throw Error('startup must not load message bodies');
                    return JSON.stringify({conversations: eventStorage.conversations});
                };
                host.readMessages = (query) => {
                    messageReads.push(query);
                    const rows = eventStorage.messages.filter(row => {
                        if (query.kind === 'ids') return query.ids.includes(row.serverId) || query.ids.includes(row.clientMsgId);
                        if (query.kind !== 'timeline') throw Error('unexpected full scan');
                        return row.conversationId === query.conversationId
                            && (query.beforeSeq === undefined || row.conversationSeq < query.beforeSeq)
                            && (query.afterSeq === undefined || row.conversationSeq > query.afterSeq);
                    });
                    const sign = query.afterSeq === undefined ? -1 : 1;
                    rows.sort((left, right) => sign * (left.conversationSeq - right.conversationSeq));
                    return JSON.stringify(rows.slice(0, query.limit ?? rows.length));
                };
            })"#,
        )
        .unwrap();
        let configure = js_sys::Function::from(configure);
        configure
            .call1(&wasm_bindgen::JsValue::NULL, &host)
            .unwrap();
        super::super::host::set_storage_host(&host).unwrap();

        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        assert_eq!(store.conversations.list().await.unwrap().len(), 1);
        assert_eq!(
            js_sys::eval("messageReads.length").unwrap().as_f64(),
            Some(0.0)
        );
        let latest = store.messages.get_by_conversation("c", 0, 2).await.unwrap();
        assert_eq!(latest[0].conversation_seq, 10);
        assert_eq!(latest[1].conversation_seq, 9);
        let older = store.messages.get_by_conversation("c", 9, 2).await.unwrap();
        assert_eq!(older[0].conversation_seq, 8);
        assert_eq!(older[1].conversation_seq, 7);
        assert_eq!(
            store
                .messages
                .get_by_conversation("c", i64::MAX as u64 - 1, 1)
                .await
                .unwrap()[0]
                .conversation_seq,
            10
        );
        assert_eq!(
            store.messages.oldest_conversation_seq("c").await.unwrap(),
            Some(1)
        );
        let content = flare_im_core_sdk::content::ContentBuilder::text("edited while unloaded")
            .build()
            .encode();
        assert_eq!(
            store
                .messages
                .apply_edit_event("message-2", content, Some(50))
                .await
                .unwrap(),
            EditApplyResult::Applied
        );
        let edited = store.messages.get("message-2").await.unwrap().unwrap();
        assert!(edited.is_edited);
        assert_eq!(
            js_sys::eval("messageReads.map(r => r.kind).join(',')")
                .unwrap()
                .as_string()
                .as_deref(),
            Some("timeline,timeline,timeline,timeline,ids")
        );
    }

    #[wasm_bindgen_test]
    async fn authoritative_reaction_removal_and_ambiguous_aliases_survive_restart() {
        install_regression_host();
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        let original = regression_message("reaction", 1);
        store.messages.save_one(&original).await.unwrap();
        store
            .messages
            .apply_reaction_event("c", "reaction", "u", "👍", 1, Some(10))
            .await
            .unwrap();
        let mut snapshot = original.to_proto();
        snapshot.reaction_states.push(Default::default());
        snapshot.reaction_states[0].user_id = "u".into();
        snapshot.reaction_states[0].emoji = "👍".into();
        snapshot.reaction_states[0].event_seq = 20;
        store
            .messages
            .save_one(&IMMessage::new(snapshot))
            .await
            .unwrap();
        let mut other = regression_message("other", 2);
        other.client_msg_id = original.client_msg_id.clone();
        other.conversation_id = "other".into();
        store.messages.save_one(&other).await.unwrap();
        drop(store);
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        let message = store.messages.get("reaction").await.unwrap().unwrap();
        assert!(message.reactions.is_empty());
        assert_eq!(message.attributes["lastReactionEventSeq:u:👍"], "20");
        assert_eq!(message.attributes["reactionsJson"], "[]");
        assert_eq!(
            store
                .messages
                .apply_reaction_event("c", "reaction", "u", "👍", 1, Some(10))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        assert!(
            store
                .messages
                .get_by_client_msg_id(&original.client_msg_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[wasm_bindgen_test]
    async fn authoritative_snapshots_and_scoped_operations_survive_restart() {
        install_regression_host();
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        let original = regression_message("edit", 1);
        store.messages.save_one(&original).await.unwrap();
        store
            .messages
            .apply_edit_event("edit", original.encoded_content.clone(), Some(10))
            .await
            .unwrap();
        let mut newer = original.clone();
        newer.encoded_content.clear();
        newer.content = Some(flare_im_core_sdk::json::from_value(flare_im_core_sdk::serde_json::json!({"contentType":"text","text":"latest","mentions":[]})).unwrap());
        newer.materialize_encoded_content_from_elem();
        let mut wire = newer.to_proto();
        wire.last_edit_seq = 20;
        let latest = IMMessage::new(wire);
        store.messages.save_one(&latest).await.unwrap();
        store.messages.save_one(&original).await.unwrap();
        assert_eq!(
            store
                .messages
                .apply_reaction_event("other", "edit", "u", "👍", 1, Some(30))
                .await
                .unwrap(),
            OperationApplyResult::NotFound
        );
        assert_eq!(
            store
                .messages
                .apply_reaction_event("c", "edit", "u", "👍", 1, Some(30))
                .await
                .unwrap(),
            OperationApplyResult::Applied
        );
        let mut pending = regression_message("local", 0);
        pending.local_state.is_local = true;
        pending.local_state.sending = true;
        store.messages.save_one(&pending).await.unwrap();
        let mut remote = regression_message("remote", 2);
        remote.client_msg_id = pending.client_msg_id.clone();
        remote.conversation_id = "other".into();
        remote.sender_id = "other".into();
        store.messages.save_one(&remote).await.unwrap();
        let reopened = build_web_store_provider("core", "a", "u").await.unwrap();
        assert!(reopened.messages.get("local").await.unwrap().is_some());
        let actual = reopened.messages.get("edit").await.unwrap().unwrap();
        assert_eq!(actual.encoded_content, latest.encoded_content);
        assert_eq!(actual.attributes["lastEditEventSeq"], "20");
        assert_eq!(actual.reactions[0].count, 1);
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn terminal_snapshots_clear_all_copies_and_retry_conversation_persistence() {
        install_regression_host();
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        let mut original = regression_message("m", 1);
        original.content = Some(flare_im_core_sdk::json::from_value(flare_im_core_sdk::serde_json::json!({"contentType":"text","text":"private","mentions":[]})).unwrap());
        original
            .extensions
            .insert("preview".into(), b"private".to_vec());
        store.messages.save_one(&original).await.unwrap();
        store
            .conversations
            .save_one(&Conversation {
                conversation_id: "c".into(),
                last_message_id: Some("m".into()),
                last_message_preview: Some("private".into()),
                max_seq: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .messages
            .apply_retention_scheduled_event(
                "m",
                &MessageRetentionPolicy::default(),
                &MessageRetentionState {
                    lifecycle: 3,
                    content_visibility: 1,
                    ..Default::default()
                },
                10,
                Some(10),
            )
            .await
            .unwrap();
        store
            .messages
            .apply_edit_event("m", original.encoded_content.clone(), Some(20))
            .await
            .unwrap();
        let mut snapshot = original.clone();
        snapshot.content = None;
        snapshot.encoded_content.clear();
        snapshot.retention_state = Some(MessageRetentionState {
            lifecycle: 4,
            content_visibility: 3,
            ..Default::default()
        });
        store.messages.save_one(&snapshot).await.unwrap();
        let actual = store.messages.get("m").await.unwrap().unwrap();
        assert_eq!(actual.retention_state, snapshot.retention_state);
        assert!(actual.encoded_content.is_empty());
        assert!(actual.extensions.is_empty());
        let purged = MessageRetentionState {
            lifecycle: 5,
            content_visibility: 4,
            ..Default::default()
        };
        js_sys::eval("eventStorage.conversationFail = true").unwrap();
        assert!(
            store
                .messages
                .apply_retention_purged_event("m", &purged, 30, Some(30))
                .await
                .is_err()
        );
        js_sys::eval("eventStorage.conversationFail = false").unwrap();
        assert_eq!(
            store
                .messages
                .apply_retention_purged_event("m", &purged, 30, Some(30))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        store
            .messages
            .update_after_ack("local-m", &original)
            .await
            .unwrap();
        let reopened = build_web_store_provider("core", "a", "u").await.unwrap();
        let actual = reopened.messages.get("m").await.unwrap().unwrap();
        assert_eq!(actual.retention_state, Some(purged));
        assert!(actual.encoded_content.is_empty());
        assert!(actual.extensions.is_empty());
        let conversation = reopened.conversations.get("c").await.unwrap().unwrap();
        assert!(
            conversation
                .last_message_preview
                .as_deref()
                .unwrap_or_default()
                .is_empty()
        );
        assert!(
            conversation
                .last_message
                .as_ref()
                .is_none_or(|preview| preview.text.is_empty())
        );
        let mut stale = conversation.clone();
        stale.last_message_preview = Some("private".into());
        reopened.conversations.save_one(&stale).await.unwrap();
        let reloaded = build_web_store_provider("core", "a", "u").await.unwrap();
        assert!(
            reloaded
                .conversations
                .get("c")
                .await
                .unwrap()
                .unwrap()
                .last_message_preview
                .as_deref()
                .unwrap_or_default()
                .is_empty()
        );
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn scoped_message_partitions_keep_real_plugin_account_identity() {
        use flare_im_core_sdk::spi::{PluginCommandScope, PluginConsentScope};
        use flare_im_core_sdk::storage::PluginBundleStorageScope;
        for configured in [false, true] {
            super::super::host::clear_storage_host();
            if configured {
                install_regression_host();
            }
            let a = build_web_store_provider("core", "a", "u").await.unwrap();
            let b = build_web_store_provider("core", "b", "u").await.unwrap();
            assert!(
                a.plugin_commands
                    .as_ref()
                    .unwrap()
                    .pending(&PluginCommandScope {
                        tenant_id: "a".into(),
                        user_id: "u".into(),
                        slot: "test".into(),
                    })
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                a.plugin_consents
                    .as_ref()
                    .unwrap()
                    .get(
                        &PluginConsentScope {
                            tenant_id: "a".into(),
                            user_id: "u".into(),
                        },
                        "test.plugin",
                        "send"
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
            let scope_a = PluginBundleStorageScope {
                tenant_id: "a".into(),
                user_id: "u".into(),
                plugin_id: "test.plugin".into(),
            };
            let scope_b = PluginBundleStorageScope {
                tenant_id: "b".into(),
                user_id: "u".into(),
                plugin_id: "test.plugin".into(),
            };
            let store_a = a.plugin_bundle_storage.as_ref().unwrap();
            let store_b = b.plugin_bundle_storage.as_ref().unwrap();
            store_a
                .set(&scope_a, "key", &serde_json::json!("a"))
                .await
                .unwrap();
            store_b
                .set(&scope_b, "key", &serde_json::json!("b"))
                .await
                .unwrap();
            assert_eq!(
                store_a.get(&scope_a, "key").await.unwrap(),
                Some(serde_json::json!("a"))
            );
            assert_eq!(
                store_b.get(&scope_b, "key").await.unwrap(),
                Some(serde_json::json!("b"))
            );
            store_a.clear_user("a", "u").await.unwrap();
            assert_eq!(
                store_b.get(&scope_b, "key").await.unwrap(),
                Some(serde_json::json!("b"))
            );
            store_b.clear_user("b", "u").await.unwrap();
        }
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn alias_repair_waits_for_storage_and_can_retry_without_changing_hot_identity() {
        install_regression_host();
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        let mut message = regression_message("alias", 1);
        message.conversation_id = "peer".into();
        message.channel_id = "peer".into();
        message.conversation_type = 1;
        store.messages.save_one(&message).await.unwrap();
        store
            .conversations
            .save_one(&Conversation {
                conversation_id: "canonical".into(),
                channel_id: "peer".into(),
                conversation_type: flare_im_core_sdk::model::conversation::ConversationType::Single,
                ..Default::default()
            })
            .await
            .unwrap();
        js_sys::eval("eventStorage.fail = true").unwrap();
        assert!(
            store
                .messages
                .get_by_conversation("canonical", 0, 10)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .messages
                .get("alias")
                .await
                .unwrap()
                .unwrap()
                .conversation_id,
            "peer"
        );
        js_sys::eval("eventStorage.fail = false").unwrap();
        let repaired = store
            .messages
            .get_by_conversation("canonical", 0, 10)
            .await
            .unwrap();
        assert_eq!(repaired.len(), 1);
        assert_eq!(repaired[0].conversation_id, "canonical");
        assert_eq!(repaired[0].encoded_content, message.encoded_content);
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn id_aliases_cannot_cross_messages_after_ack_delete_and_restart() {
        install_regression_host();
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        let first = regression_message("11111111-1111-4111-8111-111111111111", 1);
        let mut other = regression_message("22222222-2222-4222-8222-222222222222", 1);
        other.client_msg_id = first.server_id.clone();
        other.conversation_id = "other-conversation".into();
        store
            .messages
            .save_batch(&[first.clone(), other.clone()])
            .await
            .unwrap();
        let replacement = flare_im_core_sdk::content::ContentBuilder::text("edited first")
            .build()
            .encode();
        store
            .messages
            .apply_edit_event(&first.server_id, replacement, Some(20))
            .await
            .unwrap();
        store
            .messages
            .update_after_ack(&other.client_msg_id, &other)
            .await
            .unwrap();
        js_sys::eval("eventStorage.fail = true").unwrap();
        assert!(store.messages.delete(&first.server_id).await.is_err());
        assert!(
            store
                .messages
                .get(&first.server_id)
                .await
                .unwrap()
                .is_some()
        );
        js_sys::eval("eventStorage.fail = false").unwrap();
        store.messages.delete(&first.server_id).await.unwrap();
        store.messages.delete(&first.server_id).await.unwrap();
        let reopened = build_web_store_provider("core", "a", "u").await.unwrap();
        let actual = reopened
            .messages
            .get(&other.server_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.encoded_content, other.encoded_content);
        assert_eq!(
            reopened
                .messages
                .apply_edit_event(&first.server_id, other.encoded_content.clone(), Some(30))
                .await
                .unwrap(),
            EditApplyResult::NotFound
        );
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn reaction_snapshots_and_cross_type_marks_keep_their_effects_after_restart() {
        install_regression_host();
        let store = build_web_store_provider("core", "a", "u").await.unwrap();
        store
            .messages
            .save_one(&regression_message("ops", 1))
            .await
            .unwrap();
        store
            .messages
            .apply_reaction_event("c", "ops", "u", "👍", 1, Some(10))
            .await
            .unwrap();
        let mut old = store.messages.get("ops").await.unwrap().unwrap();
        old.attributes.remove("lastReactionEventSeq:u:👍");
        old.apply_reaction_change("other", "👍", 1);
        store
            .messages
            .apply_reaction_event("c", "ops", "u", "👍", 2, Some(20))
            .await
            .unwrap();
        store
            .messages
            .apply_mark_event("ops", 2, None, true, Some(10))
            .await
            .unwrap();
        store
            .messages
            .apply_mark_event("ops", 3, Some("green"), true, Some(30))
            .await
            .unwrap();
        assert_eq!(
            store
                .messages
                .apply_mark_event("ops", 2, None, false, Some(20))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        store.messages.save_one(&old).await.unwrap();
        let reopened = build_web_store_provider("core", "a", "u").await.unwrap();
        assert_eq!(
            reopened
                .messages
                .list_reactions(&["ops".into()])
                .await
                .unwrap()["ops"][0]
                .user_ids,
            vec!["other"]
        );
        assert_eq!(
            reopened
                .messages
                .apply_reaction_event("c", "ops", "u", "👍", 2, Some(20))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        let actual = reopened.messages.get("ops").await.unwrap().unwrap();
        assert_eq!(
            actual.attributes.get("markType").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            actual.attributes.get("markColor").map(String::as_str),
            Some("green")
        );
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn rejected_snapshot_is_an_error_and_retry_restores_pending() {
        install_regression_host();
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        stores
            .pending_send_writer
            .as_ref()
            .unwrap()
            .push(PendingSendVo {
                client_msg_id: "local-m".into(),
                conversation_id: "c".into(),
                message: regression_message("m", 1),
                enqueued_at_ms: 1,
            })
            .await
            .unwrap();
        js_sys::eval("eventStorage.readFail = true").unwrap();
        assert!(build_web_store_provider("core", "a", "u").await.is_err());
        js_sys::eval("eventStorage.readFail = false").unwrap();
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        assert_eq!(
            stores
                .pending_send_reader
                .as_ref()
                .unwrap()
                .list()
                .await
                .unwrap()
                .len(),
            1
        );
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn ordered_edits_marks_and_reactions_persist_after_failure_and_restart() {
        use flare_im_core_sdk::content::ContentBuilder;
        install_regression_host();
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        stores
            .messages
            .save_one(&regression_message("m", 1))
            .await
            .unwrap();
        let newer = ContentBuilder::text("newer").build().encode();
        js_sys::eval("eventStorage.fail = true").unwrap();
        assert!(
            stores
                .messages
                .apply_edit_event("m", newer.clone(), Some(20))
                .await
                .is_err()
        );
        js_sys::eval("eventStorage.fail = false").unwrap();
        assert_eq!(
            stores
                .messages
                .apply_edit_event(
                    "m",
                    ContentBuilder::text("older").build().encode(),
                    Some(10)
                )
                .await
                .unwrap(),
            EditApplyResult::IgnoredStale
        );
        stores
            .messages
            .apply_mark_event("m", 1, Some("red"), true, Some(20))
            .await
            .unwrap();
        stores
            .messages
            .apply_reaction_event("c", "m", "u", "👍", 2, Some(20))
            .await
            .unwrap();
        assert_eq!(
            stores
                .messages
                .apply_reaction_event("c", "m", "u", "👍", 1, Some(10))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        let m = stores.messages.get("m").await.unwrap().unwrap();
        assert_eq!(m.encoded_content, newer);
        assert_eq!(m.attributes.get("markType").map(String::as_str), Some("1"));
        assert!(m.reactions.is_empty());
        js_sys::eval("eventStorage.fail = true").unwrap();
        assert!(
            stores
                .messages
                .apply_mark_event("m", 1, None, false, Some(30))
                .await
                .is_err()
        );
        js_sys::eval("eventStorage.fail = false").unwrap();
        assert_eq!(
            stores
                .messages
                .apply_mark_event("m", 1, None, false, Some(30))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        stores
            .messages
            .apply_reaction_event("c", "m", "u", "👍", 1, Some(30))
            .await
            .unwrap();
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        let m = stores.messages.get("m").await.unwrap().unwrap();
        assert!(!m.attributes.contains_key("markType"));
        assert_eq!(m.reactions.len(), 1);
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn receipts_write_only_changes_and_retry_without_losing_read_state() {
        install_regression_host();
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        let messages = (1..=50)
            .map(|seq| regression_message(&format!("m{seq}"), seq))
            .collect::<Vec<_>>();
        stores.messages.save_batch(&messages).await.unwrap();
        js_sys::eval("eventStorage.writes = 0").unwrap();
        stores
            .messages
            .mark_outgoing_read_upto_seq("c", "u", 1)
            .await
            .unwrap();
        assert_eq!(
            js_sys::eval("eventStorage.writes").unwrap().as_f64(),
            Some(1.0)
        );
        stores
            .messages
            .mark_outgoing_read_upto_seq("c", "u", 1)
            .await
            .unwrap();
        assert_eq!(
            js_sys::eval("eventStorage.writes").unwrap().as_f64(),
            Some(1.0)
        );
        js_sys::eval("eventStorage.fail = true").unwrap();
        assert!(
            stores
                .messages
                .mark_outgoing_read_upto_seq("c", "u", 2)
                .await
                .is_err()
        );
        assert!(!stores.messages.get("m2").await.unwrap().unwrap().is_read);
        js_sys::eval("eventStorage.fail = false").unwrap();
        stores
            .messages
            .mark_outgoing_read_upto_seq("c", "u", 2)
            .await
            .unwrap();
        stores
            .messages
            .reconcile_outgoing_read_by_peer_seq("c", "u", 0)
            .await
            .unwrap();
        assert_eq!(
            js_sys::eval("eventStorage.writes").unwrap().as_f64(),
            Some(2.0)
        );
        let stores = build_web_store_provider("core", "a", "u").await.unwrap();
        assert!(stores.messages.get("m2").await.unwrap().unwrap().is_read);
        assert!(!stores.messages.get("m3").await.unwrap().unwrap().is_read);
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn scoped_retention_and_pending_survive_restart_and_rejected_writes() {
        let host = js_sys::eval(r#"
            globalThis.scopedStorage = { owners: {u: {messages: [], pendingSends: [{client_msg_id: 'legacy'}]}}, fail: false };
            ({
                loadSnapshot: ({userId}) => JSON.stringify(scopedStorage.owners[userId] || {}),
                saveMessage: ({userId, message}) => {
                    if (scopedStorage.fail) return Promise.reject('disk full');
                    const owner = scopedStorage.owners[userId] ||= {};
                    owner.messages = [message]; return Promise.resolve();
                },
                commitOutgoing: () => Promise.reject("unused by this test"),
                savePendingSend: ({userId, entry}) => {
                    (scopedStorage.owners[userId] ||= {}).pendingSends = [entry]; return Promise.resolve();
                },
                saveConversation: () => Promise.resolve(), saveCursor: () => Promise.resolve(),
                deleteMessage: () => Promise.resolve(), deleteConversation: () => Promise.resolve(), deletePendingSend: () => Promise.resolve()
            })
        "#).unwrap();
        super::super::host::set_storage_host(&host).unwrap();
        let a = build_web_store_provider("core", "a", "u").await.unwrap();
        assert!(
            a.pending_send_reader
                .as_ref()
                .unwrap()
                .list()
                .await
                .unwrap()
                .is_empty()
        );
        let mut message = IMMessage::new(Default::default());
        message.server_id = "m".into();
        message.client_msg_id = "local".into();
        message.conversation_id = "c".into();
        message.encoded_content = flare_im_core_sdk::content::ContentBuilder::text("private")
            .build()
            .encode();
        a.messages.save_one(&message).await.unwrap();
        a.pending_send_writer
            .as_ref()
            .unwrap()
            .push(PendingSendVo {
                client_msg_id: "local".into(),
                conversation_id: "c".into(),
                message,
                enqueued_at_ms: 1,
            })
            .await
            .unwrap();
        let b = build_web_store_provider("core", "b", "u").await.unwrap();
        assert!(b.messages.get("m").await.unwrap().is_none());
        assert!(
            b.pending_send_reader
                .as_ref()
                .unwrap()
                .list()
                .await
                .unwrap()
                .is_empty()
        );
        let mut state = MessageRetentionState::default();
        state.lifecycle = 4;
        state.content_visibility = 2;
        js_sys::eval("scopedStorage.fail = true").unwrap();
        assert!(
            a.messages
                .apply_retention_expired_event("m", &state, 100, Some(20))
                .await
                .is_err()
        );
        js_sys::eval("scopedStorage.fail = false").unwrap();
        assert_eq!(
            a.messages
                .apply_retention_expired_event("m", &state, 100, Some(20))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        a.messages
            .apply_edit_event("m", regression_message("m", 1).encoded_content, Some(15))
            .await
            .unwrap();
        let a = build_web_store_provider("core", "a", "u").await.unwrap();
        assert!(
            a.messages
                .get("m")
                .await
                .unwrap()
                .unwrap()
                .encoded_content
                .is_empty()
        );
        state.lifecycle = 5;
        state.content_visibility = 4;
        a.messages
            .apply_retention_purged_event("m", &state, 200, Some(30))
            .await
            .unwrap();
        assert_eq!(
            a.messages
                .apply_retention_scheduled_event(
                    "m",
                    &MessageRetentionPolicy::default(),
                    &MessageRetentionState::default(),
                    1,
                    Some(10)
                )
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
        let a = build_web_store_provider("core", "a", "u").await.unwrap();
        assert_eq!(
            a.messages.get("m").await.unwrap().unwrap().retention_state,
            Some(state)
        );
        assert_eq!(
            a.pending_send_reader
                .as_ref()
                .unwrap()
                .list()
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            js_sys::eval("scopedStorage.owners.u.pendingSends.length")
                .unwrap()
                .as_f64(),
            Some(1.0)
        );
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn rejected_host_writes_retry_and_rehydrate_without_advancing_cursors() {
        let host = js_sys::eval(r#"
            globalThis.storageTest = { fail: true, messages: [], pendingSends: [], cursors: {} };
            ({
                loadSnapshot: () => JSON.stringify(globalThis.storageTest),
                saveMessage: ({message}) => {
                    if (storageTest.fail) return Promise.reject('disk full');
                    storageTest.messages = [message];
                    return Promise.resolve();
                },
                commitOutgoing: () => Promise.reject("unused by this test"),
                savePendingSend: ({entry}) => {
                    if (storageTest.fail) return Promise.reject('disk full');
                    storageTest.pendingSends = [entry];
                    return Promise.resolve();
                },
                saveCursor: ({key, value}) => {
                    if (storageTest.fail) return Promise.reject('disk full');
                    storageTest.cursors[key] = value;
                    return Promise.resolve();
                },
                saveConversation: () => Promise.resolve(),
                deleteMessage: () => Promise.resolve(),
                deleteConversation: () => Promise.resolve(),
                deletePendingSend: () => storageTest.fail ? Promise.reject('disk full') : Promise.resolve()
            })
        "#).unwrap();
        super::super::host::set_storage_host(&host).unwrap();
        let inner = Arc::new(MemoryMessageStore::new());
        let mut message = IMMessage::new(Default::default());
        message.server_id = "m".into();
        message.client_msg_id = "local".into();
        message.conversation_id = "c".into();
        inner.save_one(&message).await.unwrap();
        let store = PersistingMessageStore::new(
            "u".into(),
            inner,
            Arc::new(MemoryConversationStore::new()),
            Arc::new(tokio::sync::Semaphore::new(1)),
        );
        let pending =
            PersistingPendingSendStore::new("u".into(), Arc::new(MemoryPendingSendStore::new()));
        let cursors =
            PersistingSyncCursorStore::new("u".into(), Arc::new(MemorySyncCursorStore::new()));
        let entry = PendingSendVo {
            client_msg_id: "local".into(),
            conversation_id: "c".into(),
            message,
            enqueued_at_ms: 1,
        };
        assert!(store.apply_pin_event("m", true, Some(10)).await.is_err());
        let plugin_state = PluginMessageState {
            version: 2,
            based_on_version: None,
            values: Default::default(),
        };
        assert!(
            store
                .apply_plugin_message_state("m", &plugin_state)
                .await
                .is_err()
        );
        assert!(pending.push(entry.clone()).await.is_err());
        assert!(pending.list().await.unwrap().is_empty());
        assert!(cursors.save_raw("event", "10").await.is_err());
        assert!(cursors.get_raw("event").await.unwrap().is_none());
        js_sys::eval("storageTest.fail = false").unwrap();
        // The hot store remembers version 10; the retry must still persist it.
        assert_eq!(
            store.apply_pin_event("m", false, Some(9)).await.unwrap(),
            OperationApplyResult::IgnoredStale
        );
        assert_eq!(
            store
                .apply_plugin_message_state("m", &plugin_state)
                .await
                .unwrap(),
            OperationApplyResult::Applied
        );
        pending.push(entry).await.unwrap();
        cursors.save_raw("event", "10").await.unwrap();
        let snapshot = load_snapshot("u").await.unwrap();
        assert_eq!(snapshot.messages.len(), 1);
        assert_eq!(snapshot.pending_sends.len(), 1);
        assert_eq!(
            snapshot.cursors.get("event").map(String::as_str),
            Some("10")
        );
        js_sys::eval("storageTest.fail = true").unwrap();
        assert!(pending.pop("local").await.is_err());
        assert_eq!(pending.list().await.unwrap().len(), 1);
        super::super::host::clear_storage_host();
    }

    #[wasm_bindgen_test]
    async fn cancelled_caller_keeps_write_ownership_until_host_completion() {
        let writes = Arc::new(tokio::sync::Semaphore::new(1));
        let ownership = reserve_write(&writes).await.unwrap();
        let (finish, wait) = tokio::sync::oneshot::channel::<()>();
        let (done, completed) = tokio::sync::oneshot::channel();
        let future = await_storage_write(
            async move {
                wait.await.unwrap();
                done.send(()).unwrap();
                Ok(())
            },
            Some(ownership),
        );
        drop(future);
        assert!(writes.clone().try_acquire_owned().is_err());
        finish.send(()).unwrap();
        completed.await.unwrap();
        assert!(writes.try_acquire_owned().is_ok());
    }
}
