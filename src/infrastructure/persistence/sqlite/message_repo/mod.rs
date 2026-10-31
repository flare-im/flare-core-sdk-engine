//! SQLite 消息仓储：与 [schema] 中 messages 表结构一致，按列读写；row 直接映射为 IMMessage（不经 ProtoMessage）。
use crate::infrastructure::persistence::search::{
    elem_search_text, message_type_values_for_search, search_text_for_content_bytes,
};

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use base64::prelude::*;
use flare_proto::common::{MessageRetentionPolicy, MessageRetentionState, ReactionAction};
use prost::Message as ProstMessage;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use tracing::debug;

use crate::content::message_elem::{TextElem, elem_preview_storage_payload};
use crate::domain::{
    EditApplyResult, MessageDeliveryService, MessageReader, MessageStore, MessageWriter,
    OperationApplyResult, carry_plugin_message_state, encode_plugin_message_state,
    local_cleared_through_seq, merge_message_event_attributes, message_visible_after_clear,
    validate_plugin_shared_state, write_plugin_message_state, write_plugin_shared_state,
};
use crate::model::conversation::ConversationType;
use crate::model::message::{
    MessageLocalState, ReactionEntry, has_reaction_snapshot_in_attributes,
    parse_reactions_from_attributes,
};
use crate::model::search::{SqliteKeywordSearch, sqlite_keyword_search};
use crate::model::{
    Elem, IMMessage, MessageSearchKind, MessageSearchQuery, decode_content_bytes,
    decoded_content_to_elem,
};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::time::now_millis;
use flare_plugin_protocol::state::{SharedState, SharedStateOrder};
use flare_plugin_protocol::ui::PluginMessageState;
use flare_proto::common::{ContentVisibility, MessageStatus, MessageType};

use super::identity_repair;

const MESSAGE_SAVE_BATCH_INSERT_CHUNK_SIZE: usize = 20;
const MESSAGE_REACTION_INSERT_CHUNK_SIZE: usize = 100;

struct MessagePersistRow<'a> {
    message: &'a IMMessage,
    attributes_json: String,
    mention_users_json: String,
    extensions_json: String,
    text: Option<String>,
    search_text: Option<String>,
}

impl<'a> MessagePersistRow<'a> {
    fn from_message(message: &'a IMMessage) -> Self {
        Self {
            message,
            attributes_json: serde_json::to_string(&message.attributes).unwrap_or_default(),
            mention_users_json: serde_json::to_string(&message.mention_users).unwrap_or_default(),
            extensions_json: extensions_to_json(&message.extensions),
            text: message_preview_for_storage(message),
            search_text: message_search_text_for_storage(message),
        }
    }
}

fn parse_extra(s: Option<&str>) -> HashMap<String, String> {
    let s = match s {
        Some(x) if !x.is_empty() => x,
        _ => return HashMap::new(),
    };
    serde_json::from_str(s).unwrap_or_default()
}

fn extra_to_json(attributes: &HashMap<String, String>) -> String {
    serde_json::to_string(attributes).unwrap_or_default()
}

fn encode_optional_proto<M: ProstMessage>(message: &Option<M>) -> Vec<u8> {
    message
        .as_ref()
        .map(ProstMessage::encode_to_vec)
        .unwrap_or_default()
}

fn decode_optional_proto<M>(bytes: Vec<u8>) -> Option<M>
where
    M: ProstMessage + Default,
{
    if bytes.is_empty() {
        return None;
    }
    M::decode(bytes.as_slice()).ok()
}

fn retention_hides_content(state: &MessageRetentionState) -> bool {
    matches!(
        ContentVisibility::try_from(state.content_visibility).ok(),
        Some(ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged)
    )
}

fn parse_mention_users(s: Option<&str>) -> Vec<String> {
    let s = match s {
        Some(x) if !x.is_empty() => x,
        _ => return Vec::new(),
    };
    serde_json::from_str(s).unwrap_or_default()
}

fn sqlx_err(e: sqlx::Error) -> FlareError {
    FlareError::localized(ErrorCode::DatabaseError, e.to_string())
}

/// 将分页游标 `before_seq` 绑定为 SQLite INTEGER（`conversation_seq < ?`）。
///
/// - **`0`**：表示客户端刚打开会话、尚无游标，等价于「上界无穷」，取当前库中 **最新一页**（与 Tauri `INITIAL_BEFORE_SEQ` 语义对齐，客户端可直接传 `0`）。
///   最新一页在仓储层按 **本地 seq=0 待发送优先，其余服务端消息按 `conversation_seq DESC`**（见 `get_by_conversation`），**不**伪造服务端 `conversation_seq`。
/// - **`u64::MAX`**：不可直接 `as i64`（会变成 `-1`，导致 `conversation_seq < -1` 恒空），钳制到 `i64::MAX`。
/// - 其它正值：`conversation_seq < before_seq`，用于「加载更早消息」。
fn before_seq_for_sqlite(before_seq: u64) -> i64 {
    if before_seq == 0 || before_seq >= i64::MAX as u64 {
        i64::MAX
    } else {
        before_seq as i64
    }
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// 写入 `messages.sort_ts` 的最终值：仅用于**本地列表**「最新一页」排序，**不**参与多端 `conversation_seq` 同步语义。
///
/// 取 `max(IMMessage::timeline_sort_ts, 墙钟)`，避免仅保留较小入队时间而弱于历史消息、被 `LIMIT` 裁掉。
fn effective_sort_ts_for_persist(message: &IMMessage) -> i64 {
    let wall = now_ms_i64().max(0) as u64;
    let merged = message.timeline_sort_ts().max(wall);
    u64_to_i64_saturating(merged)
}

fn message_preview_for_storage(message: &IMMessage) -> Option<String> {
    message
        .text_for_storage()
        .or_else(|| preview_for_content_bytes(&message.encoded_content))
        .or_else(|| {
            message
                .attributes
                .get("contentText")
                .map(|s| s.trim())
                .filter(|s| !crate::content::preview_storage::is_redundant_content_text_extra(s))
                .map(str::to_string)
        })
}

fn message_search_text_for_storage(message: &IMMessage) -> Option<String> {
    if let Some(content) = &message.content
        && let Some(text) = elem_search_text(content)
    {
        return Some(text);
    }
    if let Some(text) = search_text_for_content_bytes(&message.encoded_content) {
        return Some(text);
    }
    message_preview_for_storage(message)
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
}

async fn delete_message_fts_by_server_id_tx(
    tx: &mut Transaction<'_, Sqlite>,
    server_id: &str,
) -> Result<()> {
    sqlx::query("DELETE FROM messages_fts WHERE server_id = ?")
        .bind(server_id)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

async fn upsert_message_fts_tx(
    tx: &mut Transaction<'_, Sqlite>,
    server_id: &str,
    conversation_id: &str,
    text: Option<&str>,
) -> Result<()> {
    delete_message_fts_by_server_id_tx(tx, server_id).await?;
    let Some(text) = text.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    sqlx::query("INSERT INTO messages_fts(server_id, conversation_id, text) VALUES (?, ?, ?)")
        .bind(server_id)
        .bind(conversation_id)
        .bind(text)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

async fn upsert_message_fts_rows_tx(
    tx: &mut Transaction<'_, Sqlite>,
    rows: &[MessagePersistRow<'_>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut delete_qb =
        QueryBuilder::<Sqlite>::new("DELETE FROM messages_fts WHERE server_id IN (");
    {
        let mut separated = delete_qb.separated(", ");
        for row in rows {
            separated.push_bind(&row.message.server_id);
        }
    }
    delete_qb.push(")");
    delete_qb
        .build()
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;

    let searchable = rows
        .iter()
        .filter_map(|row| {
            row.search_text
                .as_deref()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(|text| (row, text))
        })
        .collect::<Vec<_>>();
    if searchable.is_empty() {
        return Ok(());
    }

    let mut insert_qb =
        QueryBuilder::<Sqlite>::new("INSERT INTO messages_fts(server_id, conversation_id, text) ");
    insert_qb.push_values(searchable, |mut b, (row, text)| {
        b.push_bind(&row.message.server_id)
            .push_bind(&row.message.conversation_id)
            .push_bind(text);
    });
    insert_qb
        .build()
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

async fn merge_reactions_to_server_id_tx(
    tx: &mut Transaction<'_, Sqlite>,
    old_server_id: &str,
    target_server_id: &str,
    target_conversation_id: &str,
) -> Result<()> {
    let old_server_id = old_server_id.trim();
    let target_server_id = target_server_id.trim();
    if old_server_id.is_empty() || target_server_id.is_empty() || old_server_id == target_server_id
    {
        return Ok(());
    }

    let now = now_ms_i64();
    sqlx::query(
        r#"INSERT OR REPLACE INTO message_reactions (
               message_server_id, conversation_id, emoji, user_id, created_at, updated_at
           )
           SELECT ?, ?, emoji, user_id, created_at, ?
           FROM message_reactions
           WHERE message_server_id = ?"#,
    )
    .bind(target_server_id)
    .bind(target_conversation_id)
    .bind(now)
    .bind(old_server_id)
    .execute(&mut **tx)
    .await
    .map_err(sqlx_err)?;

    sqlx::query("DELETE FROM message_reactions WHERE message_server_id = ?")
        .bind(old_server_id)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

async fn remove_conflicting_client_msg_rows_tx(
    tx: &mut Transaction<'_, Sqlite>,
    client_msg_id: &str,
    target_server_id: &str,
    target_conversation_id: &str,
) -> Result<()> {
    let client_msg_id = client_msg_id.trim();
    let target_server_id = target_server_id.trim();
    if client_msg_id.is_empty() || target_server_id.is_empty() {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"SELECT server_id
           FROM messages
           WHERE client_msg_id = ? AND server_id <> ?"#,
    )
    .bind(client_msg_id)
    .bind(target_server_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(sqlx_err)?;

    for row in rows {
        let old_server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
        delete_message_fts_by_server_id_tx(tx, &old_server_id).await?;
        merge_reactions_to_server_id_tx(
            tx,
            &old_server_id,
            target_server_id,
            target_conversation_id,
        )
        .await?;
    }

    sqlx::query("DELETE FROM messages WHERE client_msg_id = ? AND server_id <> ?")
        .bind(client_msg_id)
        .bind(target_server_id)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

async fn remove_conflicting_local_client_msg_rows_tx(
    tx: &mut Transaction<'_, Sqlite>,
    client_msg_id: &str,
    target_server_id: &str,
) -> Result<()> {
    let client_msg_id = client_msg_id.trim();
    let target_server_id = target_server_id.trim();
    if client_msg_id.is_empty() || target_server_id.is_empty() {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"SELECT server_id
           FROM messages
           WHERE client_msg_id = ? AND server_id <> ?
             AND is_local = 1 AND failed = 0 AND sending = 1"#,
    )
    .bind(client_msg_id)
    .bind(target_server_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(sqlx_err)?;

    for row in rows {
        let old_server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
        delete_message_fts_by_server_id_tx(tx, &old_server_id).await?;
    }

    sqlx::query(
        r#"DELETE FROM messages
           WHERE client_msg_id = ? AND server_id <> ?
             AND is_local = 1 AND failed = 0 AND sending = 1"#,
    )
    .bind(client_msg_id)
    .bind(target_server_id)
    .execute(&mut **tx)
    .await
    .map_err(sqlx_err)?;
    Ok(())
}

async fn insert_message_rows_tx(
    tx: &mut Transaction<'_, Sqlite>,
    rows: &[MessagePersistRow<'_>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut merged_attributes_json = Vec::with_capacity(rows.len());
    for row in rows {
        let incoming = parse_extra(Some(&row.attributes_json));
        let existing_attributes = existing_message_attributes_tx(tx, row).await?;
        let merged = match existing_attributes {
            Some(raw) => merge_message_event_attributes(incoming, parse_extra(Some(&raw))),
            None => incoming,
        };
        merged_attributes_json.push(extra_to_json(&merged));
    }

    let mut qb = QueryBuilder::<Sqlite>::new(
        r#"INSERT OR REPLACE INTO messages (
           server_id, conversation_id, client_msg_id, sender_id, source, conversation_seq, created_at, client_created_at,
           conversation_type, message_type, channel_id, sender_name, sender_avatar,
           sender_display_name, encoded_content, status,
           retention_policy, retention_state,
           is_read, is_recalled, is_edited,
           reply_to, quote_preview, thread_id, mention_users, mention_all, attributes, extensions, version, updated_at, text,
           sending, failed, is_local, sort_ts, uploading, upload_progress)
        "#,
    );
    qb.push_values(rows.iter().enumerate(), |mut b, (index, row)| {
        let m = row.message;
        b.push_bind(&m.server_id)
            .push_bind(&m.conversation_id)
            .push_bind(&m.client_msg_id)
            .push_bind(&m.sender_id)
            .push_bind(m.source)
            .push_bind(m.conversation_seq as i64)
            .push_bind(m.created_at as i64)
            .push_bind(m.client_created_at as i64)
            .push_bind(m.conversation_type)
            .push_bind(m.message_type)
            .push_bind(&m.channel_id)
            .push_bind(&m.sender_name)
            .push_bind(&m.sender_avatar)
            .push_bind(&m.sender_display_name)
            .push_bind(&m.encoded_content)
            .push_bind(m.status)
            .push_bind(encode_optional_proto(&m.retention_policy))
            .push_bind(encode_optional_proto(&m.retention_state))
            .push_bind(if m.is_read { 1i32 } else { 0 })
            .push_bind(if m.is_recalled { 1i32 } else { 0 })
            .push_bind(if m.is_edited { 1i32 } else { 0 })
            .push_bind(&m.reply_to)
            .push_bind(&m.quote_preview)
            .push_bind(&m.thread_id)
            .push_bind(&row.mention_users_json)
            .push_bind(if m.mention_all { 1i32 } else { 0 })
            .push_bind(&merged_attributes_json[index])
            .push_bind(&row.extensions_json)
            .push_bind(m.version as i64)
            .push_bind(m.updated_at as i64)
            .push_bind(row.text.as_deref())
            .push_bind(if m.local_state.sending { 1i32 } else { 0 })
            .push_bind(if m.local_state.failed { 1i32 } else { 0 })
            .push_bind(if m.local_state.is_local { 1i32 } else { 0 })
            .push_bind(effective_sort_ts_for_persist(m))
            .push_bind(if m.local_state.uploading { 1i32 } else { 0 })
            .push_bind(m.local_state.upload_progress.min(100) as i64);
    });
    qb.build().execute(&mut **tx).await.map_err(sqlx_err)?;
    Ok(())
}

async fn existing_message_attributes_tx(
    tx: &mut Transaction<'_, Sqlite>,
    row: &MessagePersistRow<'_>,
) -> Result<Option<String>> {
    let server_id = row.message.server_id.trim();
    let client_msg_id = row.message.client_msg_id.trim();
    let existing = if !server_id.is_empty() && !client_msg_id.is_empty() {
        sqlx::query(
            r#"SELECT attributes FROM messages
               WHERE server_id = ? OR client_msg_id = ?
               LIMIT 1"#,
        )
        .bind(server_id)
        .bind(client_msg_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(sqlx_err)?
    } else if !server_id.is_empty() {
        sqlx::query("SELECT attributes FROM messages WHERE server_id = ? LIMIT 1")
            .bind(server_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(sqlx_err)?
    } else if !client_msg_id.is_empty() {
        sqlx::query("SELECT attributes FROM messages WHERE client_msg_id = ? LIMIT 1")
            .bind(client_msg_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(sqlx_err)?
    } else {
        None
    };

    if let Some(row) = existing {
        let raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
        Ok(raw)
    } else {
        Ok(None)
    }
}

async fn delete_message_fts_by_message_id_tx(
    tx: &mut Transaction<'_, Sqlite>,
    message_id: &str,
) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT server_id
           FROM messages
           WHERE server_id = ? OR client_msg_id = ?"#,
    )
    .bind(message_id)
    .bind(message_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(sqlx_err)?;

    for row in rows {
        let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
        delete_message_fts_by_server_id_tx(tx, &server_id).await?;
    }
    Ok(())
}

fn conversation_projection_ts(message: &IMMessage) -> i64 {
    if message.conversation_seq > 0 {
        let server_time = if message.created_at > 0 {
            message.created_at
        } else if message.client_created_at > 0 {
            message.client_created_at
        } else {
            message.timeline_sort_ts()
        };
        return u64_to_i64_saturating(server_time);
    }

    let merged = message.timeline_sort_ts();
    if merged > 0 {
        u64_to_i64_saturating(merged)
    } else {
        effective_sort_ts_for_persist(message)
    }
}

fn should_replace_conversation_projection(prev: &IMMessage, candidate: &IMMessage) -> bool {
    match (prev.conversation_seq > 0, candidate.conversation_seq > 0) {
        (true, true) => {
            candidate.conversation_seq > prev.conversation_seq
                || (candidate.conversation_seq == prev.conversation_seq
                    && effective_sort_ts_for_persist(candidate)
                        >= effective_sort_ts_for_persist(prev))
        }
        _ => {
            let candidate_sort = effective_sort_ts_for_persist(candidate);
            let prev_sort = effective_sort_ts_for_persist(prev);
            candidate_sort > prev_sort
                || (candidate_sort == prev_sort
                    && candidate.conversation_seq >= prev.conversation_seq)
        }
    }
}

fn search_effective_time_sql(prefix: &str) -> String {
    format!(
        "COALESCE(NULLIF({prefix}.created_at, 0), NULLIF({prefix}.client_created_at, 0), NULLIF({prefix}.sort_ts, 0), 0)"
    )
}

/// 从 `MessageContent` 字节解码为 `messages.text` 的标准 preview 载荷。
fn text_for_sqlite_from_content_bytes(bytes: &[u8]) -> Option<String> {
    preview_for_content_bytes(bytes)
}

fn preview_for_content_bytes(bytes: &[u8]) -> Option<String> {
    decode_content_bytes(bytes)
        .ok()
        .and_then(|decoded| decoded_content_to_elem(&decoded))
        .and_then(|elem| {
            let payload = elem_preview_storage_payload(&elem);
            if payload.is_empty_for_last_preview() {
                return None;
            }
            serde_json::to_string(&payload).ok()
        })
}

fn parse_extensions(s: Option<&str>) -> HashMap<String, Vec<u8>> {
    let s = match s {
        Some(x) if !x.is_empty() => x,
        _ => return HashMap::new(),
    };
    let map: HashMap<String, String> = match serde_json::from_str(s) {
        Ok(m) => m,
        Err(_) => return HashMap::new(),
    };
    map.into_iter()
        .filter_map(|(k, v)| BASE64_STANDARD.decode(&v).ok().map(|b| (k, b)))
        .collect()
}

const MESSAGE_SELECT_COLS: &str = r#"server_id, conversation_id, client_msg_id, sender_id, source,
    conversation_seq, created_at, client_created_at, conversation_type, message_type, channel_id,
    sender_name, sender_avatar, sender_display_name, encoded_content, status,
    retention_policy, retention_state,
    is_read, is_recalled, is_edited,
    reply_to, quote_preview, thread_id, mention_users, mention_all, attributes, extensions, version, updated_at, text,
    sending, failed, is_local, sort_ts, uploading, upload_progress"#;

pub struct SqliteMessageRepo {
    pool: SqlitePool,
}

fn parse_conversation_ext_json(s: Option<&str>) -> HashMap<String, String> {
    let s = match s {
        Some(x) if !x.is_empty() => x,
        _ => return HashMap::new(),
    };
    serde_json::from_str(s).unwrap_or_default()
}

impl SqliteMessageRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn local_cleared_floor(&self, conversation_id: &str) -> Result<u64> {
        let row: Option<(Option<String>, i64)> =
            sqlx::query_as(
                "SELECT ext, COALESCE(visible_after_seq, 0) FROM conversations WHERE conversation_id = ? LIMIT 1",
            )
                .bind(conversation_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let Some((ext, visible_after_seq)) = row else {
            return Ok(0);
        };
        Ok(
            local_cleared_through_seq(&parse_conversation_ext_json(ext.as_deref()))
                .max(visible_after_seq.max(0) as u64),
        )
    }

    fn row_to_immessage(&self, row: &sqlx::sqlite::SqliteRow) -> Result<IMMessage> {
        let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
        let conversation_id: String = row.try_get("conversation_id").map_err(sqlx_err)?;
        let client_msg_id: String = row.try_get("client_msg_id").map_err(sqlx_err)?;
        let sender_id: String = row.try_get("sender_id").map_err(sqlx_err)?;
        let source: i32 = row.try_get("source").map_err(sqlx_err)?;
        let conversation_seq: i64 = row.try_get("conversation_seq").map_err(sqlx_err)?;
        let created_at: i64 = row.try_get("created_at").map_err(sqlx_err)?;
        let client_created_at: i64 = row.try_get("client_created_at").map_err(sqlx_err)?;
        let conversation_type: i32 = row.try_get("conversation_type").map_err(sqlx_err)?;
        let message_type: i32 = row.try_get("message_type").map_err(sqlx_err)?;
        let channel_id: String = row
            .try_get::<Option<String>, _>("channel_id")
            .map_err(sqlx_err)?
            .unwrap_or_default();
        let sender_name: String = row.try_get("sender_name").map_err(sqlx_err)?;
        let sender_avatar: String = row.try_get("sender_avatar").map_err(sqlx_err)?;
        let sender_display_name: String = row.try_get("sender_display_name").map_err(sqlx_err)?;
        let encoded_content: Vec<u8> = row.try_get("encoded_content").map_err(sqlx_err)?;
        let status: i32 = row.try_get("status").map_err(sqlx_err)?;
        let retention_policy_bytes: Vec<u8> = row.try_get("retention_policy").map_err(sqlx_err)?;
        let retention_state_bytes: Vec<u8> = row.try_get("retention_state").map_err(sqlx_err)?;
        let is_read: i32 = row.try_get("is_read").map_err(sqlx_err)?;
        let is_recalled: i32 = row.try_get("is_recalled").map_err(sqlx_err)?;
        let is_edited: i32 = row.try_get("is_edited").map_err(sqlx_err)?;
        let reply_to: Option<String> = row.try_get("reply_to").map_err(sqlx_err)?;
        let quote_preview: Option<String> = row.try_get("quote_preview").map_err(sqlx_err)?;
        let thread_id: Option<String> = row
            .try_get::<Option<String>, _>("thread_id")
            .map_err(sqlx_err)?
            .and_then(|thread_id| {
                let trimmed = thread_id.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            });
        let mention_users_json: Option<String> = row.try_get("mention_users").map_err(sqlx_err)?;
        let mention_all: i32 = row.try_get("mention_all").map_err(sqlx_err)?;
        let extra_json: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
        let extensions_json: Option<String> = row.try_get("extensions").map_err(sqlx_err)?;
        let version: i64 = row.try_get("version").map_err(sqlx_err)?;
        let updated_at: i64 = row.try_get("updated_at").map_err(sqlx_err)?;
        let sending: i32 = row.try_get("sending").map_err(sqlx_err)?;
        let failed: i32 = row.try_get("failed").map_err(sqlx_err)?;
        let is_local: i32 = row.try_get("is_local").map_err(sqlx_err)?;
        let sort_ts: i64 = row.try_get("sort_ts").map_err(sqlx_err)?;
        let uploading: i32 = row.try_get("uploading").map_err(sqlx_err)?;
        let upload_progress: i64 = row.try_get("upload_progress").map_err(sqlx_err)?;
        let text_col: Option<String> = row.try_get("text").map_err(sqlx_err)?;

        let mut attributes = parse_extra(extra_json.as_deref());
        let mut content = decode_content_bytes(&encoded_content)
            .ok()
            .and_then(|decoded| decoded_content_to_elem(&decoded));
        if content.is_none()
            && message_type == MessageType::Text as i32
            && let Some(ref t) = text_col
        {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                attributes
                    .entry("contentText".to_string())
                    .or_insert_with(|| trimmed.to_string());
                content = Some(Elem::Text(TextElem {
                    text: trimmed.to_string(),
                    mentions: vec![],
                }));
            }
        }

        let mut ts_u = created_at.max(0) as u64;
        let mut cts_u = client_created_at.max(0) as u64;
        let sort_u = sort_ts.max(0) as u64;
        // 待发/旧数据可能未写 `created_at`，但 `sort_ts` 已在落库时规范化（见 `effective_sort_ts_for_persist`），
        // 读出时回填给前端时间排序，避免 0 被当成「最早」。
        if ts_u == 0 && cts_u == 0 && sort_u > 0 {
            ts_u = sort_u;
            cts_u = sort_u;
        }

        let mut message = IMMessage {
            server_id,
            client_msg_id,
            conversation_id,
            conversation_type,
            channel_id,
            sender_id,
            source,
            conversation_seq: conversation_seq.max(0) as u64,
            created_at: ts_u,
            client_created_at: cts_u,
            message_type,
            content,
            encoded_content,
            text_preview: text_col.unwrap_or_default(),
            sender_name,
            sender_avatar,
            sender_display_name,
            sender: Default::default(),
            reply_to,
            quote_preview,
            thread_id,
            status,
            retention_policy: decode_optional_proto::<MessageRetentionPolicy>(
                retention_policy_bytes,
            ),
            retention_state: decode_optional_proto::<MessageRetentionState>(retention_state_bytes),
            is_read: is_read != 0,
            is_recalled: is_recalled != 0,
            is_edited: is_edited != 0,
            mention_users: parse_mention_users(mention_users_json.as_deref()),
            mention_all: mention_all != 0,
            offline_push_info: None,
            reactions: parse_reactions_from_attributes(&attributes),
            attributes,
            extensions: parse_extensions(extensions_json.as_deref()),
            version: version.max(0) as u64,
            updated_at: updated_at.max(0) as u64,
            local_state: MessageLocalState {
                sending: sending != 0,
                failed: failed != 0,
                is_local: is_local != 0,
                uploading: uploading != 0,
                upload_progress: upload_progress.clamp(0, 100) as u32,
                sort_ts: sort_ts.max(0) as u64,
            },
        };
        message.sender = Some(message.snapshot_sender());
        Ok(message)
    }
}

fn extensions_to_json(ext: &HashMap<String, Vec<u8>>) -> String {
    if ext.is_empty() {
        return String::new();
    }
    let map: HashMap<String, String> = ext
        .iter()
        .map(|(k, v)| (k.clone(), BASE64_STANDARD.encode(v)))
        .collect();
    serde_json::to_string(&map).unwrap_or_default()
}

fn now_ms_i64() -> i64 {
    now_millis().min(i64::MAX as u64) as i64
}

async fn refresh_reactions_json_snapshot(pool: &SqlitePool, message_id: &str) -> Result<()> {
    let id = message_id.trim();
    if id.is_empty() {
        return Ok(());
    }
    let message_row = sqlx::query(
        r#"SELECT server_id, attributes FROM messages
           WHERE server_id = ? OR client_msg_id = ?
           LIMIT 1"#,
    )
    .bind(id)
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(sqlx_err)?;
    let Some(row) = message_row else {
        return Ok(());
    };
    let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
    if server_id.trim().is_empty() {
        return Ok(());
    }

    let reaction_rows = sqlx::query(
        r#"SELECT emoji, user_id
           FROM message_reactions
           WHERE message_server_id = ?
           ORDER BY updated_at ASC"#,
    )
    .bind(server_id.trim())
    .fetch_all(pool)
    .await
    .map_err(sqlx_err)?;

    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for rr in reaction_rows {
        let emoji: String = rr.try_get("emoji").map_err(sqlx_err)?;
        let user_id: String = rr.try_get("user_id").map_err(sqlx_err)?;
        if emoji.trim().is_empty() || user_id.trim().is_empty() {
            continue;
        }
        grouped
            .entry(emoji.trim().to_string())
            .or_default()
            .push(user_id.trim().to_string());
    }
    let mut reactions: Vec<ReactionEntry> = grouped
        .into_iter()
        .map(|(emoji, mut user_ids)| {
            user_ids.sort();
            user_ids.dedup();
            ReactionEntry {
                emoji,
                count: user_ids.len() as u32,
                user_ids,
            }
        })
        .collect();
    reactions.sort_by(|a, b| a.emoji.cmp(&b.emoji));
    debug!(
        message_id = %id,
        server_id = %server_id,
        reaction_group_count = reactions.len(),
        "refresh_reactions_json_snapshot"
    );

    let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
    let mut attributes = parse_extra(extra_raw.as_deref());
    if reactions.is_empty() {
        attributes.remove("reactionsJson");
    } else if let Ok(raw) = serde_json::to_string(&reactions) {
        attributes.insert("reactionsJson".to_string(), raw);
    }
    sqlx::query("UPDATE messages SET attributes = ? WHERE server_id = ?")
        .bind(extra_to_json(&attributes))
        .bind(server_id)
        .execute(pool)
        .await
        .map_err(sqlx_err)?;
    Ok(())
}

async fn upsert_conversation_snapshot_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message: &IMMessage,
) -> Result<()> {
    let conversation_id = message.conversation_id.trim();
    if conversation_id.is_empty() {
        return Ok(());
    }

    let conv_type = ConversationType::from_proto_int(message.conversation_type);
    let conversation_type = conv_type.to_proto_int();
    // 消息里只有 id（对端、群、会话），不是名字；名字列交给业务快照与会话摘要。
    let display_name = "";
    let business_type = if conv_type.is_single_chat_conversation() {
        "single"
    } else {
        "chat"
    };
    let last_message_id = if message.server_id.trim().is_empty() {
        message.client_msg_id.trim()
    } else {
        message.server_id.trim()
    };
    let last_sender_id = message.sender_id.trim();
    let last_message_at = conversation_projection_ts(message);
    let preview = message_preview_for_storage(message).unwrap_or_default();
    let max_seq = message.conversation_seq as i64;
    let now = now_ms_i64();
    let created_at = if last_message_at > 0 {
        last_message_at
    } else {
        now
    };
    let updated_at = if last_message_at > 0 {
        last_message_at
    } else {
        now
    };

    sqlx::query(
        r#"INSERT INTO conversations (
               conversation_id, conversation_type, business_type, channel_id, members_count,
               display_name, avatar_url, remark, description,
               last_message_id, last_sender_id, last_message_at, last_message_preview,
               last_sender_nickname, last_sender_avatar_url,
               unread_count, last_read_seq, max_seq,
               is_pinned, is_muted, is_archived,
               version, updated_at, created_at, updated_at_ts,
               ext, draft, mention_count, mention_me, badge, role
           ) VALUES (
               ?, ?, ?, ?, 0,
               ?, '', NULL, NULL,
               ?, ?, ?, ?,
               '', '',
               0, 0, ?,
               0, 0, 0,
               0, ?, ?, ?,
               '', NULL, 0, 0, NULL, NULL
           )
           ON CONFLICT(conversation_id) DO UPDATE SET
               conversation_type = CASE
                   WHEN conversations.conversation_type = 0 AND excluded.conversation_type != 0 THEN excluded.conversation_type
                   ELSE conversations.conversation_type
               END,
               business_type = CASE
                   WHEN conversations.business_type = '' THEN excluded.business_type
                   ELSE conversations.business_type
               END,
               channel_id = CASE
                   WHEN conversations.channel_id = '' THEN excluded.channel_id
                   ELSE conversations.channel_id
               END,
               display_name = CASE
                   WHEN conversations.display_name = '' THEN excluded.display_name
                   ELSE conversations.display_name
               END,
               avatar_url = CASE
                   WHEN conversations.avatar_url = '' THEN excluded.avatar_url
               ELSE conversations.avatar_url
               END,
               last_message_id = CASE
                   WHEN
                       (COALESCE(excluded.max_seq, 0) > COALESCE(conversations.max_seq, 0))
                    OR (COALESCE(excluded.max_seq, 0) > 0
                        AND COALESCE(excluded.max_seq, 0) = COALESCE(conversations.max_seq, 0)
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                    OR (COALESCE(excluded.max_seq, 0) = 0
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                   THEN excluded.last_message_id
                   ELSE conversations.last_message_id
               END,
               last_sender_id = CASE
                   WHEN
                       (COALESCE(excluded.max_seq, 0) > COALESCE(conversations.max_seq, 0))
                    OR (COALESCE(excluded.max_seq, 0) > 0
                        AND COALESCE(excluded.max_seq, 0) = COALESCE(conversations.max_seq, 0)
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                    OR (COALESCE(excluded.max_seq, 0) = 0
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                   THEN excluded.last_sender_id
                   ELSE conversations.last_sender_id
               END,
               last_message_at = CASE
                   WHEN
                       (COALESCE(excluded.max_seq, 0) > COALESCE(conversations.max_seq, 0))
                    OR (COALESCE(excluded.max_seq, 0) > 0
                        AND COALESCE(excluded.max_seq, 0) = COALESCE(conversations.max_seq, 0)
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                    OR (COALESCE(excluded.max_seq, 0) = 0
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                   THEN excluded.last_message_at
                   ELSE conversations.last_message_at
               END,
               last_message_preview = CASE
                   WHEN
                       (COALESCE(excluded.max_seq, 0) > COALESCE(conversations.max_seq, 0))
                    OR (COALESCE(excluded.max_seq, 0) > 0
                        AND COALESCE(excluded.max_seq, 0) = COALESCE(conversations.max_seq, 0)
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                    OR (COALESCE(excluded.max_seq, 0) = 0
                        AND COALESCE(excluded.last_message_at, 0) >= COALESCE(conversations.last_message_at, 0))
                   THEN excluded.last_message_preview
                   ELSE conversations.last_message_preview
               END,
               max_seq = MAX(COALESCE(conversations.max_seq, 0), COALESCE(excluded.max_seq, 0)),
               updated_at = MAX(COALESCE(conversations.updated_at, 0), COALESCE(excluded.updated_at, 0)),
               updated_at_ts = MAX(COALESCE(conversations.updated_at_ts, 0), COALESCE(excluded.updated_at_ts, 0))
        "#,
    )
    .bind(conversation_id)
    .bind(conversation_type)
    .bind(business_type)
    .bind(&message.channel_id)
    .bind(display_name)
    .bind(last_message_id)
    .bind(last_sender_id)
    .bind(last_message_at)
    .bind(&preview)
    .bind(max_seq)
    .bind(updated_at)
    .bind(created_at)
    .bind(updated_at)
    .execute(&mut **tx)
    .await
    .map_err(sqlx_err)?;
    Ok(())
}

async fn replace_reaction_snapshot_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message: &IMMessage,
) -> Result<()> {
    if message.server_id.is_empty() {
        return Ok(());
    }
    let has_snapshot =
        has_reaction_snapshot_in_attributes(&message.attributes) || !message.reactions.is_empty();
    if !has_snapshot {
        // 下行消息通常不携带 reactions 快照，不能把已落库的反应误删。
        return Ok(());
    }
    sqlx::query("DELETE FROM message_reactions WHERE message_server_id = ?")
        .bind(&message.server_id)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    if message.reactions.is_empty() {
        return Ok(());
    }
    let now = now_ms_i64();
    for reaction in &message.reactions {
        if reaction.emoji.trim().is_empty() {
            continue;
        }
        for uid in &reaction.user_ids {
            if uid.trim().is_empty() {
                continue;
            }
            sqlx::query(
                r#"INSERT OR REPLACE INTO message_reactions
                   (message_server_id, conversation_id, emoji, user_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)"#,
            )
            .bind(&message.server_id)
            .bind(&message.conversation_id)
            .bind(&reaction.emoji)
            .bind(uid)
            .bind(now)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(sqlx_err)?;
        }
    }
    Ok(())
}

async fn replace_reaction_snapshot_rows_tx(
    tx: &mut Transaction<'_, Sqlite>,
    rows: &[MessagePersistRow<'_>],
) -> Result<()> {
    let snapshot_rows = rows
        .iter()
        .filter(|row| {
            let message = row.message;
            !message.server_id.is_empty()
                && (has_reaction_snapshot_in_attributes(&message.attributes)
                    || !message.reactions.is_empty())
        })
        .collect::<Vec<_>>();
    if snapshot_rows.is_empty() {
        return Ok(());
    }

    let mut delete_qb =
        QueryBuilder::<Sqlite>::new("DELETE FROM message_reactions WHERE message_server_id IN (");
    {
        let mut separated = delete_qb.separated(", ");
        for row in &snapshot_rows {
            separated.push_bind(&row.message.server_id);
        }
    }
    delete_qb.push(")");
    delete_qb
        .build()
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;

    let now = now_ms_i64();
    let mut entries = Vec::new();
    for row in snapshot_rows {
        let message = row.message;
        for reaction in &message.reactions {
            if reaction.emoji.trim().is_empty() {
                continue;
            }
            for uid in &reaction.user_ids {
                if uid.trim().is_empty() {
                    continue;
                }
                entries.push((
                    message.server_id.as_str(),
                    message.conversation_id.as_str(),
                    reaction.emoji.as_str(),
                    uid.as_str(),
                ));
            }
        }
    }

    for chunk in entries.chunks(MESSAGE_REACTION_INSERT_CHUNK_SIZE) {
        let mut insert_qb = QueryBuilder::<Sqlite>::new(
            r#"INSERT OR REPLACE INTO message_reactions
               (message_server_id, conversation_id, emoji, user_id, created_at, updated_at) "#,
        );
        insert_qb.push_values(chunk, |mut b, entry| {
            b.push_bind(entry.0)
                .push_bind(entry.1)
                .push_bind(entry.2)
                .push_bind(entry.3)
                .push_bind(now)
                .push_bind(now);
        });
        insert_qb
            .build()
            .execute(&mut **tx)
            .await
            .map_err(sqlx_err)?;
    }

    Ok(())
}

async fn refresh_conversation_snapshot_after_message_delete_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    conversation_id: &str,
) -> Result<()> {
    if conversation_id.trim().is_empty() {
        return Ok(());
    }

    let row = sqlx::query(
        r#"SELECT m.server_id, m.client_msg_id, m.sender_id,
                  CASE
                    WHEN m.conversation_seq > 0 THEN COALESCE(NULLIF(m.created_at, 0), NULLIF(m.client_created_at, 0), NULLIF(m.sort_ts, 0), 0)
                    ELSE max(max(COALESCE(m.sort_ts, 0), COALESCE(m.created_at, 0)), COALESCE(m.client_created_at, 0))
                  END AS message_at,
                  text
           FROM messages m
           LEFT JOIN conversations c ON c.conversation_id = m.conversation_id
           WHERE m.conversation_id = ?
             AND TRIM(COALESCE(m.text, '')) != ''
             AND (
                 m.conversation_seq = 0
                 OR m.conversation_seq > COALESCE(c.visible_after_seq, 0)
             )
           ORDER BY
             CASE
               WHEN m.conversation_seq = 0
                AND max(max(COALESCE(m.sort_ts, 0), COALESCE(m.created_at, 0)), COALESCE(m.client_created_at, 0)) >
                    COALESCE((
                      SELECT max(COALESCE(NULLIF(s.created_at, 0), NULLIF(s.client_created_at, 0), NULLIF(s.sort_ts, 0), 0))
                      FROM messages s
                      WHERE s.conversation_id = m.conversation_id
                        AND s.conversation_seq > COALESCE(c.visible_after_seq, 0)
                        AND s.conversation_seq > 0
                        AND TRIM(COALESCE(s.text, '')) != ''
                    ), 0)
               THEN 1 ELSE 0
             END DESC,
             CASE WHEN m.conversation_seq > 0 THEN m.conversation_seq ELSE 0 END DESC,
             CASE
               WHEN m.conversation_seq > 0 THEN COALESCE(NULLIF(m.created_at, 0), NULLIF(m.client_created_at, 0), NULLIF(m.sort_ts, 0), 0)
               ELSE max(max(COALESCE(m.sort_ts, 0), COALESCE(m.created_at, 0)), COALESCE(m.client_created_at, 0))
             END DESC
           LIMIT 1"#,
    )
    .bind(conversation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(sqlx_err)?;

    if let Some(row) = row {
        let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
        let client_msg_id: String = row.try_get("client_msg_id").map_err(sqlx_err)?;
        let sender_id: String = row.try_get("sender_id").map_err(sqlx_err)?;
        let message_at: i64 = row.try_get("message_at").map_err(sqlx_err)?;
        let text: Option<String> = row.try_get("text").map_err(sqlx_err)?;
        let message_id = if server_id.trim().is_empty() {
            client_msg_id
        } else {
            server_id
        };
        sqlx::query(
            r#"UPDATE conversations
               SET last_message_id = ?,
                   last_sender_id = ?,
                   last_message_at = ?,
                   last_message_preview = ?,
                   updated_at = MAX(COALESCE(updated_at, 0), ?),
                   updated_at_ts = MAX(COALESCE(updated_at_ts, 0), ?)
               WHERE conversation_id = ?"#,
        )
        .bind(message_id)
        .bind(sender_id)
        .bind(message_at)
        .bind(text.as_deref())
        .bind(message_at)
        .bind(message_at)
        .bind(conversation_id)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    } else {
        sqlx::query(
            r#"UPDATE conversations
               SET last_message_id = NULL,
                   last_sender_id = NULL,
                   last_message_at = NULL,
                   last_message_preview = NULL
               WHERE conversation_id = ?"#,
        )
        .bind(conversation_id)
        .execute(&mut **tx)
        .await
        .map_err(sqlx_err)?;
    }

    Ok(())
}

fn is_stale_operation(
    attributes: &HashMap<String, String>,
    seq_key: &str,
    incoming_event_seq: Option<u64>,
) -> bool {
    let Some(incoming_event_seq) =
        incoming_event_seq.filter(|conversation_seq| *conversation_seq > 0)
    else {
        return false;
    };
    let current_event_seq = attributes
        .get(seq_key)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    current_event_seq > 0 && incoming_event_seq <= current_event_seq
}

async fn apply_message_extra_with_seq<F>(
    pool: &SqlitePool,
    message_id: &str,
    seq_key: &str,
    incoming_event_seq: Option<u64>,
    mut apply: F,
) -> Result<OperationApplyResult>
where
    F: FnMut(&mut HashMap<String, String>),
{
    // 读改写必须同事务：seq 判定（is_stale_operation）与 UPDATE 交错会让旧事件覆盖新事件。
    let mut tx = pool.begin().await.map_err(sqlx_err)?;
    let rows = sqlx::query(
        r#"SELECT server_id, attributes FROM messages
           WHERE server_id = ? OR client_msg_id = ?"#,
    )
    .bind(message_id)
    .bind(message_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(sqlx_err)?;

    if rows.is_empty() {
        return Ok(OperationApplyResult::NotFound);
    }

    let mut applied_any = false;
    for row in rows {
        let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
        let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
        let mut attributes = parse_extra(extra_raw.as_deref());
        if is_stale_operation(&attributes, seq_key, incoming_event_seq) {
            continue;
        }
        if let Some(conversation_seq) =
            incoming_event_seq.filter(|conversation_seq| *conversation_seq > 0)
        {
            attributes.insert(seq_key.to_string(), conversation_seq.to_string());
        }
        apply(&mut attributes);
        sqlx::query("UPDATE messages SET attributes = ? WHERE server_id = ?")
            .bind(extra_to_json(&attributes))
            .bind(server_id)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        applied_any = true;
    }
    tx.commit().await.map_err(sqlx_err)?;

    Ok(if applied_any {
        OperationApplyResult::Applied
    } else {
        OperationApplyResult::IgnoredStale
    })
}

mod operations;
mod reader;
#[cfg(test)]
mod tests;
mod writer;
