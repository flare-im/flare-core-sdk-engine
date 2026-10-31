//! 测试公共基础设施
//!
//! - MemoryMessageStore / MemoryConversationStore / MemoryCursorStore：纯内存实现
//! - create_test_client / create_test_client_no_connect：快速创建 IMClient
//! - establish_connection：连接到服务端并等待 Ready
//! - SERIAL_LOCK：串行化集成测试，避免同平台互斥踢下线

#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use flare_im_core_sdk::model::Conversation;
use flare_im_core_sdk::model::IMMessage;
use flare_im_core_sdk::model::{decode_content_bytes, decoded_content_to_elem};
use flare_im_core_sdk::prelude::StoreProvider;
use flare_im_core_sdk::prelude::*;
use flare_im_core_sdk::storage::{
    ConversationReader, ConversationWriter, MessageReader, MessageStore, MessageWriter,
    SyncCursorReader, SyncCursorWriter,
};
use flare_proto::common::MessageStatus;

/// 串行化集成测试，同一用户仅允许一个连接在线
pub static SERIAL_LOCK: Lazy<tokio::sync::Mutex<()>> = Lazy::new(|| tokio::sync::Mutex::new(()));

// =============================================================================
// MemoryMessageStore
// =============================================================================

pub struct MemoryMessageStore {
    data: RwLock<HashMap<String, IMMessage>>,
}

impl MemoryMessageStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl MessageReader for MemoryMessageStore {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        let data = self.data.read().await;
        Ok(data.get(message_id).cloned())
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        let data = self.data.read().await;
        Ok(data
            .values()
            .find(|m| m.client_msg_id == client_msg_id)
            .cloned())
    }

    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        let data = self.data.read().await;
        let bound = if before_seq == 0 {
            u64::MAX
        } else {
            before_seq
        };
        let is_latest = before_seq == 0 || before_seq >= i64::MAX as u64;
        let mut msgs: Vec<_> = if is_latest {
            data.values()
                .filter(|m| m.conversation_id == conversation_id && m.conversation_seq < bound)
                .cloned()
                .collect()
        } else {
            data.values()
                .filter(|m| {
                    m.conversation_id == conversation_id
                        && m.conversation_seq > 0
                        && m.conversation_seq < bound
                })
                .cloned()
                .collect()
        };
        if is_latest {
            msgs.sort_by(IMMessage::compare_for_latest_window_desc);
        } else {
            msgs.sort_by_key(|m| std::cmp::Reverse(m.conversation_seq));
        }
        msgs.truncate(limit as usize);
        Ok(msgs)
    }

    async fn search(&self, _keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        let data = self.data.read().await;
        let results: Vec<_> = data.values().take(limit as usize).cloned().collect();
        Ok(results)
    }

    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        let kw = keyword.trim().to_lowercase();
        let data = self.data.read().await;
        let mut results: Vec<_> = data
            .values()
            .filter(|m| {
                if m.conversation_id != conversation_id {
                    return false;
                }
                let from_extra = m
                    .attributes
                    .get("contentText")
                    .is_some_and(|t| t.to_lowercase().contains(&kw));
                let from_preview = m
                    .text_for_storage()
                    .is_some_and(|t| t.to_lowercase().contains(&kw));
                from_extra || from_preview
            })
            .cloned()
            .collect();
        results.sort_by_key(|m| std::cmp::Reverse(m.conversation_seq));
        results.truncate(limit as usize);
        Ok(results)
    }
}

#[async_trait]
impl MessageWriter for MemoryMessageStore {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        let mut data = self.data.write().await;
        for msg in messages {
            let key = if !msg.server_id.is_empty() {
                msg.server_id.clone()
            } else {
                msg.client_msg_id.clone()
            };
            data.insert(key, msg.clone());
        }
        Ok(())
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        MessageWriter::save_batch(self, std::slice::from_ref(message)).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        let mut data = self.data.write().await;
        let recalled = MessageStatus::Recalled as i32;
        let apply = |msg: &mut IMMessage| {
            msg.status = status;
            if status == recalled {
                msg.is_recalled = true;
            }
        };
        if let Some(msg) = data.get_mut(message_id) {
            apply(msg);
            return Ok(1);
        }
        for msg in data.values_mut() {
            if msg.server_id == message_id || msg.client_msg_id == message_id {
                apply(msg);
                return Ok(1);
            }
        }
        Ok(0)
    }

    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool> {
        let mut data = self.data.write().await;
        let apply = |msg: &mut IMMessage| {
            msg.encoded_content = new_content.clone();
            msg.is_edited = true;
            let decoded = decode_content_bytes(&msg.encoded_content).ok();
            msg.text_preview = decoded
                .as_ref()
                .map(|content| content.text_preview())
                .unwrap_or_default();
            msg.content = decoded.as_ref().and_then(decoded_content_to_elem);
        };
        if let Some(msg) = data.get_mut(message_id) {
            apply(msg);
            return Ok(true);
        }
        for msg in data.values_mut() {
            if msg.server_id == message_id || msg.client_msg_id == message_id {
                apply(msg);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(message_id);
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
        let mut count = 0;
        for msg in self.data.write().await.values_mut() {
            if msg.conversation_id == from {
                msg.conversation_id = to.to_string();
                count += 1;
            }
        }
        Ok(count)
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(client_msg_id);
        let key = if !message.server_id.is_empty() {
            message.server_id.clone()
        } else {
            message.client_msg_id.clone()
        };
        data.insert(key, message.clone());
        Ok(())
    }
}

impl MessageStore for MemoryMessageStore {}

// =============================================================================
// MemoryConversationStore
// =============================================================================

pub struct MemoryConversationStore {
    data: RwLock<HashMap<String, Conversation>>,
}

impl MemoryConversationStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ConversationReader for MemoryConversationStore {
    async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        let data = self.data.read().await;
        Ok(data.get(conversation_id).cloned())
    }

    async fn list(&self) -> Result<Vec<Conversation>> {
        let data = self.data.read().await;
        let mut list: Vec<Conversation> = data.values().cloned().collect();
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
}

#[async_trait]
impl ConversationWriter for MemoryConversationStore {
    async fn save_batch(&self, conversations: &[Conversation]) -> Result<()> {
        let mut data = self.data.write().await;
        for conv in conversations {
            data.insert(conv.conversation_id.clone(), conv.clone());
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
        let mut data = self.data.write().await;
        data.remove(conversation_id);
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
        let Some(mut source) = data.remove(from) else {
            return Ok(());
        };
        source.conversation_id = to.to_string();
        data.entry(to.to_string()).or_insert(source);
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
                flare_im_core_sdk::storage::set_local_cleared_through_seq(
                    &mut conv.ext,
                    cleared_through_seq,
                );
            }
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
        let mut data = self.data.write().await;
        if let Some(c) = data.get_mut(conversation_id) {
            c.last_message_id = Some(last_message_id.to_string());
            c.last_sender_id = Some(last_sender_id.to_string());
            c.last_message_at = Some(last_message_at);
            c.last_message_preview = last_message_preview.map(String::from);
            c.max_seq = max_seq;
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
        let data = self.data.read().await;
        Ok(data
            .get(conversation_id)
            .map(|conv| conv.max_seq)
            .unwrap_or_default())
    }
}

// =============================================================================
// MemoryCursorStore
// =============================================================================

pub struct MemoryCursorStore {
    data: RwLock<HashMap<String, String>>,
}

impl MemoryCursorStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SyncCursorReader for MemoryCursorStore {
    async fn get_raw(&self, key: &str) -> Result<Option<String>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn get_conversation_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<flare_im_core_sdk::storage::SyncCursorVo>> {
        let key = format!("{user_id}:{conversation_id}");
        let data = self.data.read().await;
        if let Some(cursor_str) = data.get(&key)
            && let Some((seq_str, synced_str)) = cursor_str.split_once(':')
            && let (Ok(last_seq), Ok(synced_at)) =
                (seq_str.parse::<u64>(), synced_str.parse::<u64>())
        {
            return Ok(Some(flare_im_core_sdk::storage::SyncCursorVo {
                user_id: user_id.to_string(),
                conversation_id: conversation_id.to_string(),
                last_seq,
                synced_at,
            }));
        }
        Ok(None)
    }
}

#[async_trait]
impl SyncCursorWriter for MemoryCursorStore {
    async fn save_raw(&self, key: &str, cursor: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(key.to_string(), cursor.to_string());
        Ok(())
    }

    async fn save_conversation_cursor(
        &self,
        cursor: &flare_im_core_sdk::storage::SyncCursorVo,
    ) -> Result<()> {
        let key = format!("{}:{}", cursor.user_id, cursor.conversation_id);
        let cursor_str = format!("{}:{}", cursor.last_seq, cursor.synced_at);
        let mut data = self.data.write().await;
        data.insert(key, cursor_str);
        Ok(())
    }
}

// =============================================================================
// Helper functions
// =============================================================================

fn get_ws_url() -> String {
    env::var("FLARE_TEST_SERVER_URL").unwrap_or_else(|_| "ws://localhost:60051".to_string())
}

fn make_stores() -> StoreProvider {
    StoreProvider {
        plugin_commands: None,
        messages: Arc::new(MemoryMessageStore::new()),
        conversations: Arc::new(MemoryConversationStore::new()),
        conversation_participants: None,
        cursors: Arc::new(MemoryCursorStore::new()),
        timeline_coverage: Arc::new(flare_im_core_sdk::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    }
}

/// 创建 IMClient（不连接），用于纯本地测试
pub async fn create_test_client_no_connect() -> IMClient {
    let config = SdkConfig::new(get_ws_url());
    IMClient::builder()
        .config(config)
        .stores(make_stores())
        .build()
        .expect("test IMClient build")
}

/// 创建 IMClient（用于集成测试，需服务端运行）
pub async fn create_test_client() -> IMClient {
    create_test_client_no_connect().await
}

/// 创建带会话/消息/已读同步任务的 IMClient（用于同步集成测试）
/// Builder 已内置注入三任务，与 create_test_client_no_connect 等价。
pub async fn create_test_client_with_sync_tasks() -> IMClient {
    create_test_client_no_connect().await
}

/// 连接到服务端并等待 SDK 进入 Ready 状态
///
/// 包含 connect → bootstrap → 等待 Ready 全流程。
/// 内置重试机制：服务端可能因前次测试的设备冲突清理未完成而暂时拒绝连接，
/// 最多重试 3 次（间隔 2s）以等待服务端清理旧连接状态。
pub async fn establish_connection(client: &mut IMClient, user_id: &str) {
    let token = generate_core_token(user_id);

    let max_retries = 3;
    let mut last_err = String::new();
    for attempt in 1..=max_retries {
        match client.connect(user_id, Some(token.as_str())).await {
            Ok(_) => {
                last_err.clear();
                break;
            }
            Err(e) => {
                last_err = e.to_string();
                if attempt < max_retries {
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }
    }
    if !last_err.is_empty() {
        panic!(
            "连接失败（user={user_id}，已重试 {max_retries} 次）: {last_err}。请确保服务端已启动"
        );
    }

    let max_wait = tokio::time::Duration::from_secs(15);
    let start = tokio::time::Instant::now();
    loop {
        if client.state() == SdkState::Ready {
            break;
        }
        if start.elapsed() > max_wait {
            panic!(
                "SDK 在 {max_wait:?} 内未进入 Ready 状态（当前: {:?}）",
                client.state()
            );
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
}

/// 断开连接 + 等待服务端清理
///
/// 集成测试结束时调用，确保服务端有时间完成设备冲突清理。
pub async fn teardown(client: &mut IMClient) {
    let _ = client.disconnect().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
}

/// 构建可直接发送的单聊文本消息（返回 IMMessage，供 message().send 使用）
///
/// 设置 channel_id（单聊为对方 user_id）和 conversation_type = Single，满足服务端对单聊消息的要求。
pub fn build_single_text(
    conversation_id: &str,
    sender_id: &str,
    channel_id: &str,
    text: &str,
) -> flare_im_core_sdk::model::IMMessage {
    use flare_im_core_sdk::model::{ContentBuilder, MessageBuilder};
    let msg = MessageBuilder::new(conversation_id, sender_id)
        .content(ContentBuilder::text(text).build())
        .channel(channel_id)
        .single_chat()
        .build()
        .expect("build single text message");
    flare_im_core_sdk::model::IMMessage::new(msg)
}

/// 生成 Core JWT token（使用 SDK 内置的 generate_core_token）
///
/// 与 flare-im-core/examples/chatroom_client.rs 保持一致：
/// secret = "insecure-secret"，issuer = "flare-im-core"，tenant_id = "0"
fn generate_core_token(user_id: &str) -> String {
    flare_im_core_sdk::prelude::generate_core_token(&flare_im_core_sdk::prelude::CoreTokenConfig {
        secret: "insecure-secret".to_string(),
        issuer: "flare-im-core".to_string(),
        user_id: user_id.to_string(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some("0".to_string()),
    })
    .expect("generate_core_token should not fail")
}
