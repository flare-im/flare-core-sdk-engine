//! SQLite 会话仓储：与 [schema] 中 conversations 表结构一致，按列读写，无 data BLOB。
//! 排序与 idx_conversations_sort 一致：is_archived → is_pinned DESC → last_message_at DESC。

use std::collections::HashMap;

use async_trait::async_trait;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use tokio::sync::OnceCell;

use crate::content::message_elem::{
    MessagePreviewElem, decoded_content_to_elem, elem_preview_storage_payload,
};
use crate::domain::{
    ConversationReader, ConversationWriter, MemberKey, ParticipantDisplaySnapshot, ReadPosition,
    business_name, local_cleared_through_seq, preserve_local_remark,
    preserve_local_single_chat_channel, set_local_cleared_through_seq,
};
use crate::model::conversation::{ConversationLocalState, ConversationType};
use crate::model::search::escaped_like_contains;
use crate::model::{Conversation, ConversationListQuery, decode_content_bytes};
use crate::shared::error::{ErrorCode, FlareError, Result};

use super::identity_repair;

/// 与 schema 中 conversations 表列顺序一致的 i32 枚举（由 SDK 会话类型 policy 统一维护）。
fn conversation_type_to_i32(t: &ConversationType) -> i32 {
    t.to_proto_int()
}

fn i32_to_conversation_type(v: i32) -> ConversationType {
    ConversationType::from_proto_int(v)
}

fn sqlx_err(e: sqlx::Error) -> FlareError {
    FlareError::localized(ErrorCode::DatabaseError, e.to_string())
}

fn parse_ext(s: Option<&str>) -> HashMap<String, String> {
    let s = match s {
        Some(x) if !x.is_empty() => x,
        _ => return HashMap::new(),
    };
    serde_json::from_str(s).unwrap_or_default()
}

fn merge_identity_conversation(mut target: Conversation, mut source: Conversation) -> Conversation {
    source.conversation_id = target.conversation_id.clone();
    if target.channel_id.trim().is_empty() {
        target.channel_id = source.channel_id;
    }
    if target.business_type.trim().is_empty() {
        target.business_type = source.business_type;
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
    if target.description.is_none() {
        target.description = source.description;
    }
    if target.draft.is_none() {
        target.draft = source.draft;
    }
    let source_is_newer = source.max_seq > target.max_seq
        || (source.max_seq == target.max_seq
            && source.last_message_at.unwrap_or_default()
                > target.last_message_at.unwrap_or_default());
    if source_is_newer {
        target.last_message_id = source.last_message_id;
        target.last_sender_id = source.last_sender_id;
        target.last_message_at = source.last_message_at;
        target.last_message_preview = source.last_message_preview;
        target.last_message = source.last_message;
        target.last_sender_nickname = source.last_sender_nickname;
        target.last_sender_avatar_url = source.last_sender_avatar_url;
        target.max_seq = target.max_seq.max(source.max_seq);
    }
    target.members_count = target.members_count.max(source.members_count);
    target.unread_count = target.unread_count.max(source.unread_count);
    target.last_read_seq = target.last_read_seq.max(source.last_read_seq);
    target.visible_after_seq = target.visible_after_seq.max(source.visible_after_seq);
    target.is_pinned |= source.is_pinned;
    target.is_muted |= source.is_muted;
    target.is_archived |= source.is_archived;
    target.version = target.version.max(source.version);
    target.updated_at = target.updated_at.max(source.updated_at);
    target.created_at = match (target.created_at, source.created_at) {
        (0, value) => value,
        (value, 0) => value,
        (left, right) => left.min(right),
    };
    target.updated_at_ts = target.updated_at_ts.max(source.updated_at_ts);
    target.mention_count = target.mention_count.max(source.mention_count);
    target.mention_me |= source.mention_me;
    if target.badge.is_none() {
        target.badge = source.badge;
    }
    if target.role.is_none() {
        target.role = source.role;
    }
    for (key, value) in source.ext {
        target.ext.entry(key).or_insert(value);
    }
    target
}

pub struct SqliteConversationRepo {
    pool: SqlitePool,
    legacy_repair_once: OnceCell<()>,
}

impl SqliteConversationRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            legacy_repair_once: OnceCell::new(),
        }
    }

    async fn delete_invalid_conversation_rows(&self) -> Result<()> {
        sqlx::query("DELETE FROM conversations WHERE TRIM(COALESCE(conversation_id, '')) = ''")
            .execute(&self.pool)
            .await
            .map_err(sqlx_err)?;
        Ok(())
    }

    async fn repair_missing_message_previews_from_content(&self) -> Result<()> {
        let rows = sqlx::query(
            r#"SELECT rowid, encoded_content
               FROM messages
               WHERE TRIM(COALESCE(text, '')) = ''
                 AND length(COALESCE(encoded_content, X'')) > 0"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(sqlx_err)?;

        for row in rows {
            let rowid: i64 = row.try_get("rowid").map_err(sqlx_err)?;
            let encoded_content: Vec<u8> = row.try_get("encoded_content").map_err(sqlx_err)?;
            let Some(preview) = preview_from_encoded_content(&encoded_content) else {
                continue;
            };
            sqlx::query("UPDATE messages SET text = ? WHERE rowid = ?")
                .bind(preview)
                .bind(rowid)
                .execute(&self.pool)
                .await
                .map_err(sqlx_err)?;
        }

        Ok(())
    }

    async fn repair_missing_conversations_from_messages(&self) -> Result<()> {
        self.delete_invalid_conversation_rows().await?;
        self.repair_missing_message_previews_from_content().await?;
        identity_repair::repair_single_chat_message_aliases(&self.pool).await?;
        sqlx::query(
            r#"WITH latest_messages AS (
                   SELECT
                       m.conversation_id,
                       CASE
                           WHEN COALESCE(m.conversation_type, 0) > 0 THEN m.conversation_type
                           ELSE 1
                       END AS conversation_type,
                       m.channel_id,
                       m.sender_id,
                       m.sender_name,
                       m.sender_display_name,
                       m.server_id,
                       m.client_msg_id,
                       m.conversation_seq,
                       COALESCE(
                           NULLIF(TRIM(m.text), ''),
                           NULLIF(TRIM(CASE WHEN json_valid(COALESCE(m.attributes, '')) THEN json_extract(m.attributes, '$.contentText') ELSE NULL END), ''),
                           ''
                       ) AS preview,
                       CASE
                           WHEN m.conversation_seq > 0 THEN COALESCE(NULLIF(m.created_at, 0), NULLIF(m.client_created_at, 0), NULLIF(m.sort_ts, 0), 0)
                           ELSE max(max(COALESCE(m.sort_ts, 0), COALESCE(m.created_at, 0)), COALESCE(m.client_created_at, 0))
                       END AS effective_at
                   FROM messages m
                   WHERE TRIM(COALESCE(m.conversation_id, '')) != ''
                     AND COALESCE(m.is_recalled, 0) = 0
                     AND NOT EXISTS (
                         SELECT 1
                         FROM conversations c
                         WHERE c.conversation_id = m.conversation_id
                     )
                     AND m.rowid = (
                         SELECT s.rowid
                         FROM messages s
                         WHERE s.conversation_id = m.conversation_id
                           AND COALESCE(s.is_recalled, 0) = 0
                         ORDER BY
                             CASE
                                 WHEN s.conversation_seq = 0
                                  AND max(max(COALESCE(s.sort_ts, 0), COALESCE(s.created_at, 0)), COALESCE(s.client_created_at, 0)) >
                                      COALESCE((
                                          SELECT max(COALESCE(NULLIF(x.created_at, 0), NULLIF(x.client_created_at, 0), NULLIF(x.sort_ts, 0), 0))
                                          FROM messages x
                                          WHERE x.conversation_id = s.conversation_id
                                            AND x.conversation_seq > 0
                                            AND COALESCE(x.is_recalled, 0) = 0
                                      ), 0)
                                 THEN 3
                                 WHEN s.conversation_seq > 0
                                 THEN 2
                                 ELSE 0
                             END DESC,
                             CASE WHEN s.conversation_seq > 0 THEN s.conversation_seq ELSE 0 END DESC,
                             CASE
                                 WHEN s.conversation_seq > 0 THEN COALESCE(NULLIF(s.created_at, 0), NULLIF(s.client_created_at, 0), NULLIF(s.sort_ts, 0), 0)
                                 ELSE max(max(COALESCE(s.sort_ts, 0), COALESCE(s.created_at, 0)), COALESCE(s.client_created_at, 0))
                             END DESC,
                             s.rowid DESC
                         LIMIT 1
                     )
               )
               INSERT OR IGNORE INTO conversations (
                   conversation_id, conversation_type, business_type, channel_id, members_count,
                   display_name, avatar_url, remark, description,
                   last_message_id, last_sender_id, last_message_at, last_message_preview,
                   last_sender_nickname, last_sender_avatar_url,
                   unread_count, last_read_seq, max_seq,
                   is_pinned, is_muted, is_archived,
                   version, updated_at, created_at, updated_at_ts,
                   ext, draft, mention_count, mention_me, badge, role
               )
               SELECT
                   lm.conversation_id,
                   lm.conversation_type,
                   CASE
                       WHEN lm.conversation_type = 1 THEN 'single'
                       WHEN lm.conversation_type = 2 THEN 'group'
                       WHEN lm.conversation_type = 3 THEN 'ai'
                       WHEN lm.conversation_type = 4 THEN 'system'
                       WHEN lm.conversation_type = 5 THEN 'customer'
                       WHEN lm.conversation_type = 6 THEN 'temp'
                       WHEN lm.conversation_type = 7 THEN 'channel'
                       WHEN lm.conversation_type = 8 THEN 'broadcast'
                       ELSE 'chat'
                   END,
                   COALESCE(
                       NULLIF(TRIM(lm.channel_id), ''),
                       CASE
                           WHEN lm.conversation_type = 1 THEN NULLIF(TRIM(lm.sender_id), '')
                           ELSE NULL
                       END,
                       lm.conversation_id
                   ),
                   0,
                   '',
                   '',
                   NULL,
                   NULL,
                   COALESCE(NULLIF(TRIM(lm.server_id), ''), NULLIF(TRIM(lm.client_msg_id), '')),
                   NULLIF(TRIM(lm.sender_id), ''),
                   CASE
                       WHEN COALESCE(lm.effective_at, 0) > 0 THEN lm.effective_at
                       ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000
                   END,
                   lm.preview,
                   '',
                   '',
                   0,
                   0,
                   COALESCE((
                       SELECT MAX(COALESCE(mm.conversation_seq, 0))
                       FROM messages mm
                       WHERE mm.conversation_id = lm.conversation_id
                         AND COALESCE(mm.is_recalled, 0) = 0
                   ), 0),
                   0,
                   0,
                   0,
                   0,
                   CASE
                       WHEN COALESCE(lm.effective_at, 0) > 0 THEN lm.effective_at
                       ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000
                   END,
                   CASE
                       WHEN COALESCE(lm.effective_at, 0) > 0 THEN lm.effective_at
                       ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000
                   END,
                   CASE
                       WHEN COALESCE(lm.effective_at, 0) > 0 THEN lm.effective_at
                       ELSE CAST(strftime('%s', 'now') AS INTEGER) * 1000
                   END,
                   '',
                   NULL,
                   0,
                   0,
                   NULL,
                   NULL
               FROM latest_messages lm"#,
        )
        .execute(&self.pool)
        .await
        .map_err(sqlx_err)?;
        Ok(())
    }

    async fn ensure_legacy_repair(&self) -> Result<()> {
        self.legacy_repair_once
            .get_or_try_init(|| async { self.repair_missing_conversations_from_messages().await })
            .await
            .map(|_| ())
    }

    fn select_with_latest_visible_message(where_clause: &str) -> String {
        format!(
            r#"SELECT c.conversation_id, c.conversation_type, c.business_type, c.channel_id,
                      c.members_count, c.display_name, c.avatar_url, c.remark, c.description,
                      CASE
                          WHEN lm.rowid IS NOT NULL
                              THEN COALESCE(NULLIF(lm.server_id, ''), NULLIF(lm.client_msg_id, ''), NULLIF(TRIM(c.last_message_id), ''))
                          ELSE NULLIF(TRIM(c.last_message_id), '')
                      END AS last_message_id,
                      CASE
                          WHEN lm.rowid IS NOT NULL
                              THEN COALESCE(NULLIF(lm.sender_id, ''), NULLIF(TRIM(c.last_sender_id), ''))
                          ELSE NULLIF(TRIM(c.last_sender_id), '')
                      END AS last_sender_id,
                      CASE
                          WHEN lm.rowid IS NOT NULL
                              THEN COALESCE(NULLIF(lm.effective_at, 0), c.last_message_at)
                          ELSE c.last_message_at
                      END AS last_message_at,
                      CASE
                          WHEN lm.rowid IS NOT NULL THEN lm.text
                          ELSE c.last_message_preview
                      END AS last_message_preview,
                      c.last_sender_nickname, c.last_sender_avatar_url,
                      c.unread_count, c.last_read_seq, c.max_seq, c.visible_after_seq,
                      c.is_pinned, c.is_muted, c.is_mention_only, c.is_archived, c.version, c.updated_at,
                      c.created_at, c.updated_at_ts, c.ext, c.draft,
                      c.mention_count, c.mention_me, c.badge, c.role, c.member_preview
               FROM conversations c
               LEFT JOIN (
                   SELECT rowid, server_id, client_msg_id, sender_id, conversation_seq,
                          COALESCE(
                              NULLIF(TRIM(text), ''),
                              NULLIF(TRIM(CASE WHEN json_valid(COALESCE(attributes, '')) THEN json_extract(attributes, '$.contentText') ELSE NULL END), '')
                          ) AS text,
                          CASE
                              WHEN conversation_seq > 0 THEN COALESCE(NULLIF(created_at, 0), NULLIF(client_created_at, 0), NULLIF(sort_ts, 0), 0)
                              ELSE max(max(COALESCE(sort_ts, 0), COALESCE(created_at, 0)), COALESCE(client_created_at, 0))
                          END AS effective_at
                   FROM messages
               ) lm ON lm.rowid = (
                   SELECT m.rowid
                   FROM messages m
                   WHERE m.conversation_id = c.conversation_id
                     AND (m.conversation_seq = 0 OR m.conversation_seq > COALESCE(c.visible_after_seq, 0))
                     AND COALESCE(
                         NULLIF(TRIM(m.text), ''),
                         NULLIF(TRIM(CASE WHEN json_valid(COALESCE(m.attributes, '')) THEN json_extract(m.attributes, '$.contentText') ELSE NULL END), '')
                     ) IS NOT NULL
                   ORDER BY
                            CASE
                                WHEN m.conversation_seq = 0
                                 AND max(max(COALESCE(m.sort_ts, 0), COALESCE(m.created_at, 0)), COALESCE(m.client_created_at, 0)) >
                                     COALESCE((
                                         SELECT max(COALESCE(NULLIF(s.created_at, 0), NULLIF(s.client_created_at, 0), NULLIF(s.sort_ts, 0), 0))
                                         FROM messages s
                                         WHERE s.conversation_id = m.conversation_id
                                           AND s.conversation_seq > 0
                                           AND COALESCE(
                                               NULLIF(TRIM(s.text), ''),
                                               NULLIF(TRIM(CASE WHEN json_valid(COALESCE(s.attributes, '')) THEN json_extract(s.attributes, '$.contentText') ELSE NULL END), '')
                                           ) IS NOT NULL
                                     ), 0)
                                THEN 3
                                WHEN m.conversation_seq > 0
                                THEN 2
                                ELSE 0
                            END DESC,
                            CASE WHEN m.conversation_seq > 0 THEN m.conversation_seq ELSE 0 END DESC,
                            CASE
                                WHEN m.conversation_seq > 0 THEN COALESCE(NULLIF(m.created_at, 0), NULLIF(m.client_created_at, 0), NULLIF(m.sort_ts, 0), 0)
                                ELSE max(max(COALESCE(m.sort_ts, 0), COALESCE(m.created_at, 0)), COALESCE(m.client_created_at, 0))
                            END DESC
                   LIMIT 1
               )
               {where_clause}"#
        )
    }

    fn row_to_conversation(&self, row: &sqlx::sqlite::SqliteRow) -> Result<Conversation> {
        let conversation_id: String = row.try_get("conversation_id").map_err(sqlx_err)?;
        let conversation_type: i32 = row.try_get("conversation_type").map_err(sqlx_err)?;
        let business_type: String = row.try_get("business_type").map_err(sqlx_err)?;
        let channel_id: String = row.try_get("channel_id").map_err(sqlx_err)?;
        let members_count: i64 = row.try_get("members_count").map_err(sqlx_err)?;
        let display_name: String = row.try_get("display_name").map_err(sqlx_err)?;
        let avatar_url: String = row.try_get("avatar_url").map_err(sqlx_err)?;
        let remark: Option<String> = row.try_get("remark").map_err(sqlx_err)?;
        let description: Option<String> = row.try_get("description").map_err(sqlx_err)?;
        let last_message_id: Option<String> = row.try_get("last_message_id").map_err(sqlx_err)?;
        let last_sender_id: Option<String> = row.try_get("last_sender_id").map_err(sqlx_err)?;
        let last_message_at: Option<i64> = row.try_get("last_message_at").map_err(sqlx_err)?;
        let last_message_preview: Option<String> =
            row.try_get("last_message_preview").map_err(sqlx_err)?;
        let last_sender_nickname: String = row.try_get("last_sender_nickname").map_err(sqlx_err)?;
        let last_sender_avatar_url: String =
            row.try_get("last_sender_avatar_url").map_err(sqlx_err)?;
        let unread_count: i32 = row.try_get("unread_count").map_err(sqlx_err)?;
        let last_read_seq: i64 = row.try_get("last_read_seq").map_err(sqlx_err)?;
        let max_seq: i64 = row.try_get("max_seq").map_err(sqlx_err)?;
        let visible_after_seq: i64 = row.try_get("visible_after_seq").map_err(sqlx_err)?;
        let is_pinned: i32 = row.try_get("is_pinned").map_err(sqlx_err)?;
        let is_muted: i32 = row.try_get("is_muted").map_err(sqlx_err)?;
        let is_mention_only: Option<i32> = row.try_get("is_mention_only").map_err(sqlx_err)?;
        let is_archived: i32 = row.try_get("is_archived").map_err(sqlx_err)?;
        let version: i64 = row.try_get("version").map_err(sqlx_err)?;
        let updated_at: i64 = row.try_get("updated_at").map_err(sqlx_err)?;
        let created_at: i64 = row.try_get("created_at").map_err(sqlx_err)?;
        let updated_at_ts: Option<i64> = row.try_get("updated_at_ts").map_err(sqlx_err)?;
        let ext_json: Option<String> = row.try_get("ext").map_err(sqlx_err)?;
        let draft: Option<String> = row.try_get("draft").map_err(sqlx_err)?;
        let mention_count: i32 = row.try_get("mention_count").map_err(sqlx_err)?;
        let mention_me: i32 = row.try_get("mention_me").map_err(sqlx_err)?;
        let badge: Option<String> = row.try_get("badge").map_err(sqlx_err)?;
        let role: Option<String> = row.try_get("role").map_err(sqlx_err)?;
        let ext = parse_ext(ext_json.as_deref());
        let peer_read_seq = ext
            .get("peer_read_seq")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_default();

        let last_message = last_message_id.as_ref().map(|id| MessagePreviewElem {
            message_id: id.clone(),
            sender_id: last_sender_id.clone().unwrap_or_default(),
            r#type: 0,
            text: last_message_preview.clone().unwrap_or_default(),
            time: last_message_at.map(|ts| ts.max(0) as u64).unwrap_or(0),
        });

        Ok(Conversation {
            conversation_id,
            conversation_type: i32_to_conversation_type(conversation_type),
            business_type,
            channel_id,
            members_count: members_count.max(0) as u32,
            display_name,
            title: Default::default(),
            avatar_url,
            remark,
            description,
            last_message_id,
            last_sender_id,
            last_message_at: last_message_at.map(|t| t as u64),
            last_message_preview,
            last_message,
            last_sender_nickname,
            last_sender: None,
            last_sender_avatar_url,
            unread_count: unread_count.max(0) as u32,
            last_read_seq: last_read_seq.max(0) as u64,
            peer_read_seq,
            max_seq: max_seq.max(0) as u64,
            visible_after_seq: visible_after_seq.max(0) as u64,
            is_pinned: is_pinned != 0,
            is_muted: is_muted != 0,
            is_mention_only: is_mention_only.map(|value| value != 0),
            notify_all: false,
            is_archived: is_archived != 0,
            version: version.max(0) as u64,
            updated_at: updated_at.max(0) as u64,
            created_at: created_at.max(0) as u64,
            updated_at_ts: updated_at_ts.map(|t| t as u64),
            ext,
            participant_version: 0,
            member_preview: row
                .try_get::<Option<String>, _>("member_preview")
                .ok()
                .flatten()
                .and_then(|raw| serde_json::from_str(&raw).ok())
                .unwrap_or_default(),
            draft,
            mention_count: mention_count.max(0) as u32,
            mention_me: mention_me != 0,
            badge,
            role,
            participants: Vec::new(),
            local_state: ConversationLocalState::default(),
        })
    }
}

fn preview_from_encoded_content(bytes: &[u8]) -> Option<String> {
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

#[async_trait]
impl ConversationReader for SqliteConversationRepo {
    async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        if conversation_id.trim().is_empty() {
            return Ok(None);
        }
        self.ensure_legacy_repair().await?;
        let sql = Self::select_with_latest_visible_message("WHERE c.conversation_id = ?");
        let row = sqlx::query(&sql)
            .bind(conversation_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        row.map(|r| self.row_to_conversation(&r)).transpose()
    }

    /// 存在性探测：启动分类无需整张 JOIN 列表。
    async fn has_any(&self) -> Result<bool> {
        let row: (i64,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM conversations WHERE TRIM(COALESCE(conversation_id, '')) != '' LIMIT 1)",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row.0 != 0)
    }

    /// I7：单条 IN 查询批量点查（分块守 SQLite 绑定变量上限）。
    async fn get_many(&self, conversation_ids: &[String]) -> Result<Vec<Conversation>> {
        let ids: Vec<&String> = conversation_ids
            .iter()
            .filter(|id| !id.trim().is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_legacy_repair().await?;
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(super::SQLITE_IN_CHUNK) {
            let placeholders = super::in_placeholders(chunk.len());
            let sql = Self::select_with_latest_visible_message(&format!(
                "WHERE c.conversation_id IN ({placeholders})"
            ));
            let mut query = sqlx::query(&sql);
            for conversation_id in chunk {
                query = query.bind(conversation_id.as_str());
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            for row in rows {
                out.push(self.row_to_conversation(&row)?);
            }
        }
        Ok(out)
    }

    /// 列表：与 idx_conversations_sort 一致 — is_archived → is_pinned DESC → last_message_at DESC
    async fn list(&self) -> Result<Vec<Conversation>> {
        self.ensure_legacy_repair().await?;
        let sql = Self::select_with_latest_visible_message(
            "WHERE TRIM(COALESCE(c.conversation_id, '')) != ''
             ORDER BY c.is_archived ASC, c.is_pinned DESC, COALESCE(last_message_at, 0) DESC",
        );
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.row_to_conversation(&row)?);
        }
        Ok(out)
    }

    async fn list_by_query(&self, query: &ConversationListQuery) -> Result<Vec<Conversation>> {
        self.ensure_legacy_repair().await?;
        let base = Self::select_with_latest_visible_message(
            "WHERE TRIM(COALESCE(c.conversation_id, '')) != ''",
        );
        let mut qb = QueryBuilder::<Sqlite>::new(base);

        if !query.include_archived {
            qb.push(" AND COALESCE(c.is_archived, 0) = 0");
        }
        if query.unread_only {
            qb.push(" AND COALESCE(c.unread_count, 0) > 0");
        }
        if query.mention_me_only {
            qb.push(" AND (COALESCE(c.mention_me, 0) != 0 OR COALESCE(c.mention_count, 0) > 0)");
        }
        if query.pinned_only {
            qb.push(" AND COALESCE(c.is_pinned, 0) != 0");
        }
        if let Some(muted) = query.muted_only {
            qb.push(" AND COALESCE(c.is_muted, 0) = ");
            qb.push_bind(if muted { 1i32 } else { 0i32 });
        }
        if query.has_draft_only {
            qb.push(" AND TRIM(COALESCE(c.draft, '')) != ''");
        }
        if !query.conversation_types.is_empty() {
            qb.push(" AND c.conversation_type IN (");
            let mut separated = qb.separated(", ");
            for conversation_type in &query.conversation_types {
                separated.push_bind(conversation_type_to_i32(conversation_type));
            }
            separated.push_unseparated(")");
        }
        if query.has_marked_messages {
            qb.push(
                r#" AND EXISTS (
                    SELECT 1
                    FROM messages mm
                    WHERE mm.conversation_id = c.conversation_id
                      AND (mm.conversation_seq = 0 OR mm.conversation_seq > COALESCE(c.visible_after_seq, 0))
                      AND COALESCE(mm.is_recalled, 0) = 0
                      AND COALESCE(mm.attributes, '') LIKE '%markType%'
                    LIMIT 1
                )"#,
            );
        }
        if let Some(keyword) = query.normalized_keyword() {
            let like = escaped_like_contains(&keyword);
            qb.push(" AND (LOWER(COALESCE(c.conversation_id, '')) LIKE ");
            qb.push_bind(like.clone());
            qb.push(" ESCAPE '\\' OR LOWER(COALESCE(c.channel_id, '')) LIKE ");
            qb.push_bind(like.clone());
            qb.push(" ESCAPE '\\' OR LOWER(COALESCE(c.display_name, '')) LIKE ");
            qb.push_bind(like.clone());
            qb.push(" ESCAPE '\\' OR LOWER(COALESCE(c.remark, '')) LIKE ");
            qb.push_bind(like.clone());
            qb.push(" ESCAPE '\\' OR LOWER(COALESCE(c.description, '')) LIKE ");
            qb.push_bind(like.clone());
            qb.push(" ESCAPE '\\' OR LOWER(COALESCE(c.last_message_preview, '')) LIKE ");
            qb.push_bind(like);
            qb.push(" ESCAPE '\\')");
        }

        qb.push(" ORDER BY c.is_archived ASC, c.is_pinned DESC, COALESCE(last_message_at, 0) DESC");

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.row_to_conversation(&row)?);
        }
        Ok(out)
    }

    async fn member_nicknames(&self, pairs: &[MemberKey]) -> Result<HashMap<MemberKey, String>> {
        let mut out = HashMap::new();
        // 两个变量一对，按 SQLite 变量上限分块。
        for chunk in pairs.chunks(super::SQLITE_IN_CHUNK / 2) {
            if chunk.is_empty() {
                continue;
            }
            let mut builder = QueryBuilder::<Sqlite>::new(
                "SELECT conversation_id, user_id, nickname FROM conversation_member_nicknames WHERE ",
            );
            for (index, (conversation_id, user_id)) in chunk.iter().enumerate() {
                if index > 0 {
                    builder.push(" OR ");
                }
                builder.push("(conversation_id = ");
                builder.push_bind(conversation_id);
                builder.push(" AND user_id = ");
                builder.push_bind(user_id);
                builder.push(")");
            }
            let rows = builder
                .build()
                .fetch_all(&self.pool)
                .await
                .map_err(sqlx_err)?;
            for row in rows {
                let conversation_id: String = row.try_get("conversation_id").map_err(sqlx_err)?;
                let user_id: String = row.try_get("user_id").map_err(sqlx_err)?;
                let nickname: String = row.try_get("nickname").map_err(sqlx_err)?;
                out.insert((conversation_id, user_id), nickname);
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl ConversationWriter for SqliteConversationRepo {
    async fn save_batch(&self, conversations: &[Conversation]) -> Result<()> {
        for conversation in conversations {
            if conversation.conversation_id.trim().is_empty() {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "conversationId 不能为空",
                ));
            }
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        for c in conversations {
            let existing = sqlx::query(
                r#"SELECT last_read_seq, max_seq, unread_count,
                          last_message_id, last_sender_id, last_message_at, last_message_preview,
                          is_pinned, is_muted, is_mention_only, is_archived, visible_after_seq, ext, draft,
                          remark, channel_id, conversation_type, member_preview
                   FROM conversations
                   WHERE conversation_id = ?"#,
            )
            .bind(&c.conversation_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

            // 在 existing 被 move 进下面的 if let 之前先取出来
            let prev_member_preview: Option<String> = existing
                .as_ref()
                .and_then(|row| row.try_get::<Option<String>, _>("member_preview").ok())
                .flatten();

            let mut merged = c.clone();
            if let Some(row) = existing {
                let prev_max_seq = row.try_get::<i64, _>("max_seq").unwrap_or(0).max(0) as u64;
                let prev_unread = row.try_get::<i64, _>("unread_count").unwrap_or(0).max(0) as u32;
                let raw_prev_last_read =
                    row.try_get::<i64, _>("last_read_seq").unwrap_or(0).max(0) as u64;
                let local_read =
                    ReadPosition::from_parts(prev_max_seq, raw_prev_last_read, prev_unread);
                let merged_read = ReadPosition::merge_with_incoming_summary(
                    local_read,
                    ReadPosition::from_conversation(c),
                );
                merged.last_read_seq = merged_read.last_read_seq;
                merged.unread_count = merged_read.unread_count;
                merged.max_seq = merged.max_seq.max(merged_read.max_seq);
                let prev_last_message_id = row
                    .try_get::<Option<String>, _>("last_message_id")
                    .unwrap_or(None);
                let prev_last_sender_id = row
                    .try_get::<Option<String>, _>("last_sender_id")
                    .unwrap_or(None);
                let prev_last_message_at = row
                    .try_get::<Option<i64>, _>("last_message_at")
                    .unwrap_or(None)
                    .map(|t| t.max(0) as u64);
                let prev_last_message_preview = row
                    .try_get::<Option<String>, _>("last_message_preview")
                    .unwrap_or(None);
                let prev_is_pinned = row.try_get::<i32, _>("is_pinned").unwrap_or(0) != 0;
                let prev_is_muted = row.try_get::<i32, _>("is_muted").unwrap_or(0) != 0;
                let prev_mention_only = row
                    .try_get::<Option<i32>, _>("is_mention_only")
                    .map_err(sqlx_err)?
                    .map(|value| value != 0);
                merged.is_mention_only = merged.is_mention_only.or(prev_mention_only);
                let prev_is_archived = row.try_get::<i32, _>("is_archived").unwrap_or(0) != 0;
                let prev_draft = row.try_get::<Option<String>, _>("draft").unwrap_or(None);
                let prev_ext_json = row.try_get::<Option<String>, _>("ext").unwrap_or(None);
                let prev_ext: std::collections::HashMap<String, String> = prev_ext_json
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                let prev_clear_floor = local_cleared_through_seq(&prev_ext);
                let prev_settings_version = prev_ext
                    .get(crate::model::EXT_USER_SETTINGS_VERSION)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                let incoming_settings_version = merged
                    .ext
                    .get(crate::model::EXT_USER_SETTINGS_VERSION)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                let prev_dirty = prev_ext
                    .get(crate::model::EXT_SETTINGS_DIRTY)
                    .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));

                if prev_dirty && incoming_settings_version <= prev_settings_version {
                    merged.is_pinned = prev_is_pinned;
                    merged.is_muted = prev_is_muted;
                    merged.is_mention_only = prev_mention_only;
                    merged.is_archived = prev_is_archived;
                    merged.draft = prev_draft;
                    merged.ext = prev_ext;
                } else if incoming_settings_version >= prev_settings_version {
                    for (k, v) in prev_ext {
                        merged.ext.entry(k).or_insert(v);
                    }
                    merged.ext.insert(
                        crate::model::EXT_SETTINGS_DIRTY.to_string(),
                        "0".to_string(),
                    );
                } else {
                    merged.is_pinned = prev_is_pinned;
                    merged.is_muted = prev_is_muted;
                    merged.is_mention_only = prev_mention_only;
                    merged.is_archived = prev_is_archived;
                    merged.draft = prev_draft;
                    merged.ext = prev_ext;
                }

                let confirmed_floor = local_cleared_through_seq(&c.ext).max(prev_clear_floor);
                set_local_cleared_through_seq(&mut merged.ext, confirmed_floor);

                let prev_has_last_message = prev_last_message_id
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|v| !v.is_empty())
                    || prev_last_message_preview
                        .as_deref()
                        .map(str::trim)
                        .is_some_and(|v| !v.is_empty())
                    || prev_last_message_at.unwrap_or_default() > 0;
                let incoming_has_last_message = c
                    .last_message_id
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|v| !v.is_empty())
                    || c.last_message_preview
                        .as_deref()
                        .map(str::trim)
                        .is_some_and(|v| !v.is_empty())
                    || c.last_message_at.unwrap_or_default() > 0;
                let incoming_preview_is_empty = c
                    .last_message_preview
                    .as_deref()
                    .map(str::trim)
                    .is_none_or(str::is_empty);
                // 会话摘要只是服务端投影；本地消息表同步出来的最新消息更权威。
                // 避免服务端摘要滞后时把会话列表预览回滚到旧消息或空消息。
                if prev_has_last_message
                    && (c.max_seq <= prev_max_seq
                        || !incoming_has_last_message
                        || incoming_preview_is_empty)
                {
                    merged.last_message_id = prev_last_message_id;
                    merged.last_sender_id = prev_last_sender_id;
                    merged.last_message_at = prev_last_message_at;
                    merged.last_message_preview = prev_last_message_preview;
                }

                let prev_remark = row.try_get::<Option<String>, _>("remark").unwrap_or(None);
                merged.remark =
                    preserve_local_remark(merged.remark.as_deref(), prev_remark.as_deref());

                let prev_channel_id = row
                    .try_get::<Option<String>, _>("channel_id")
                    .unwrap_or(None);
                let prev_conv_type = row.try_get::<i32, _>("conversation_type").unwrap_or(0);
                merged.channel_id = preserve_local_single_chat_channel(
                    i32_to_conversation_type(prev_conv_type),
                    &merged.channel_id,
                    prev_channel_id.as_deref().unwrap_or_default(),
                );
            } else {
                let merged_read = ReadPosition::merge_with_incoming_summary(
                    ReadPosition::default(),
                    ReadPosition::from_conversation(c),
                );
                merged.last_read_seq = merged_read.last_read_seq;
                merged.unread_count = merged_read.unread_count;
                merged.max_seq = merged.max_seq.max(merged_read.max_seq);
            }

            // Server-visible history boundary is authoritative from the incoming summary.
            // Local clear is preserved by its explicit local_cleared_through_seq marker.
            let local_cleared_floor = crate::domain::sync_visibility_floor(&merged);
            merged.visible_after_seq = local_cleared_floor;
            if local_cleared_floor > 0 {
                merged.last_read_seq = merged.last_read_seq.max(local_cleared_floor);
                merged.max_seq = merged.max_seq.max(local_cleared_floor);
                if c.max_seq <= local_cleared_floor {
                    merged.last_message_id = None;
                    merged.last_sender_id = None;
                    merged.last_message_at = None;
                    merged.last_message_preview = None;
                    merged.last_message = None;
                    merged.unread_count = 0;
                }
            }
            // unread 上界：最多为“最大 seq - 已读 seq”
            merged.unread_count = merged
                .unread_count
                .min(ReadPosition::from_conversation(&merged).unread_upper_bound());

            // 名字列只存业务名：等于自己 id（对端、群或会话 id）的值不是名字。
            merged.display_name = business_name(
                &merged.display_name,
                &[&merged.channel_id, &merged.conversation_id],
            );

            let ext_json = serde_json::to_string(&merged.ext).unwrap_or_default();
            // 成员预览：来源不带就沿用库里已有的，绝不清空。
            // 并非每条会话更新都携带 member_preview（未读变化、草稿、置顶等
            // 局部更新就不带），若照写就会把它抹掉——端上单聊标题会随机
            // 退化成「会话」，且只在特定操作后复现，极难定位。
            let member_preview_json: Option<String> = if merged.member_preview.is_empty() {
                prev_member_preview
            } else {
                serde_json::to_string(&merged.member_preview).ok()
            };

            sqlx::query(
                r#"INSERT OR REPLACE INTO conversations (
                   conversation_id, conversation_type, business_type, channel_id, members_count,
                   display_name, avatar_url, remark, description, last_message_id, last_sender_id,
                   last_message_at, last_message_preview, last_sender_nickname, last_sender_avatar_url,
                   unread_count, last_read_seq, max_seq, visible_after_seq, is_pinned, is_muted, is_mention_only, is_archived,
                   version, updated_at, created_at, updated_at_ts, ext, draft,
                   mention_count, mention_me, badge, role, member_preview)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(&merged.conversation_id)
            .bind(conversation_type_to_i32(&merged.conversation_type))
            .bind(&merged.business_type)
            .bind(&merged.channel_id)
            .bind(merged.members_count as i64)
            .bind(&merged.display_name)
            .bind(&merged.avatar_url)
            .bind(&merged.remark)
            .bind(&merged.description)
            .bind(&merged.last_message_id)
            .bind(&merged.last_sender_id)
            .bind(merged.last_message_at.map(|t| t as i64))
            .bind(&merged.last_message_preview)
            .bind(&merged.last_sender_nickname)
            .bind(&merged.last_sender_avatar_url)
            .bind(merged.unread_count as i32)
            .bind(merged.last_read_seq as i64)
            .bind(merged.max_seq as i64)
            .bind(merged.visible_after_seq as i64)
            .bind(if merged.is_pinned { 1i32 } else { 0 })
            .bind(if merged.is_muted { 1i32 } else { 0 })
            .bind(merged.is_mention_only.map(i32::from))
            .bind(if merged.is_archived { 1i32 } else { 0 })
            .bind(merged.version as i64)
            .bind(merged.updated_at as i64)
            .bind(merged.created_at as i64)
            .bind(merged.updated_at_ts.map(|t| t as i64))
            .bind(&ext_json)
            .bind(&merged.draft)
            .bind(merged.mention_count as i32)
            .bind(if merged.mention_me { 1i32 } else { 0 })
            .bind(&merged.badge)
            .bind(&merged.role)
            .bind(&member_preview_json)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
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
        sqlx::query(
            r#"UPDATE conversations
               SET
                 last_read_seq = MAX(0, ?),
                 max_seq = MAX(COALESCE(max_seq, 0), ?),
                 unread_count = MAX(0, ?),
                 mention_count = CASE WHEN ? <= 0 THEN 0 ELSE mention_count END,
                 mention_me = CASE WHEN ? <= 0 THEN 0 ELSE mention_me END
               WHERE conversation_id = ?"#,
        )
        .bind(last_read_seq as i64)
        .bind(last_read_seq as i64)
        .bind(unread_count as i64)
        .bind(unread_count as i64)
        .bind(unread_count as i64)
        .bind(conversation_id)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn set_pinned(&self, conversation_id: &str, pinned: bool) -> Result<()> {
        sqlx::query(r#"UPDATE conversations SET is_pinned = ? WHERE conversation_id = ?"#)
            .bind(if pinned { 1i32 } else { 0 })
            .bind(conversation_id)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn set_muted(&self, conversation_id: &str, muted: bool) -> Result<()> {
        sqlx::query(r#"UPDATE conversations SET is_muted = ? WHERE conversation_id = ?"#)
            .bind(if muted { 1i32 } else { 0 })
            .bind(conversation_id)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn set_notify_all(&self, conversation_id: &str, enabled: bool) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let ext_row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT ext FROM conversations WHERE conversation_id = ?")
                .bind(conversation_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let Some((raw_ext,)) = ext_row else {
            return Ok(());
        };
        let mut ext = parse_ext(raw_ext.as_deref());
        crate::model::set_notify_all_in(&mut ext, enabled);
        let ext_json = serde_json::to_string(&ext)
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("UPDATE conversations SET ext = ? WHERE conversation_id = ?")
            .bind(ext_json)
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn set_archived(&self, conversation_id: &str, archived: bool) -> Result<()> {
        sqlx::query(r#"UPDATE conversations SET is_archived = ? WHERE conversation_id = ?"#)
            .bind(if archived { 1i32 } else { 0 })
            .bind(conversation_id)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn mark_unread(&self, conversation_id: &str) -> Result<u32> {
        sqlx::query(
            r#"UPDATE conversations
               SET last_read_seq = CASE WHEN max_seq > 0 THEN max_seq - 1 ELSE 0 END,
                   unread_count = CASE WHEN max_seq > 0 THEN 1 ELSE 1 END
               WHERE conversation_id = ?"#,
        )
        .bind(conversation_id)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let row =
            sqlx::query(r#"SELECT unread_count FROM conversations WHERE conversation_id = ?"#)
                .bind(conversation_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row
            .and_then(|r| r.try_get::<i64, _>("unread_count").ok())
            .unwrap_or(1)
            .max(0) as u32)
    }

    async fn update_draft(&self, conversation_id: &str, draft: Option<&str>) -> Result<()> {
        sqlx::query(r#"UPDATE conversations SET draft = ? WHERE conversation_id = ?"#)
            .bind(draft)
            .bind(conversation_id)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
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
        sqlx::query(
            r#"UPDATE conversations SET
               last_message_id = ?, last_sender_id = ?, last_message_at = ?, last_message_preview = ?,
               max_seq = MAX(COALESCE(max_seq, 0), ?)
               WHERE conversation_id = ?
                 AND (
                   COALESCE(max_seq, 0) <= ?
                   OR COALESCE(last_message_at, 0) <= ?
                 )"#,
        )
        .bind(last_message_id)
        .bind(last_sender_id)
        .bind(last_message_at as i64)
        .bind(last_message_preview.unwrap_or(""))
        .bind(max_seq as i64)
        .bind(conversation_id)
        .bind(max_seq as i64)
        .bind(last_message_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn recompute_unread_for_user(
        &self,
        conversation_id: &str,
        current_user_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"UPDATE conversations
               SET unread_count = (
                   SELECT COUNT(1)
                   FROM messages m
                   WHERE m.conversation_id = conversations.conversation_id
                     AND COALESCE(m.conversation_seq, 0) > COALESCE(conversations.last_read_seq, 0)
                     AND COALESCE(m.sender_id, '') <> ?
                     AND COALESCE(m.is_recalled, 0) = 0
               )
               WHERE conversation_id = ?"#,
        )
        .bind(current_user_id)
        .bind(conversation_id)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn clear_local_chat_history(
        &self,
        conversation_id: &str,
        cleared_through_seq: u64,
    ) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        if cleared_through_seq > 0 {
            let ext_row: Option<(Option<String>,)> =
                sqlx::query_as("SELECT ext FROM conversations WHERE conversation_id = ?")
                    .bind(conversation_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            let mut ext = parse_ext(ext_row.and_then(|(e,)| e).as_deref());
            set_local_cleared_through_seq(&mut ext, cleared_through_seq);
            let ext_json = serde_json::to_string(&ext)
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            sqlx::query("UPDATE conversations SET ext = ? WHERE conversation_id = ?")
                .bind(ext_json)
                .bind(conversation_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        }

        sqlx::query("DELETE FROM message_reactions WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM messages WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM pending_sends WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query(
            r#"UPDATE conversations SET
               last_message_id = NULL,
               last_sender_id = NULL,
               last_message_at = NULL,
               last_message_preview = NULL,
               unread_count = 0,
               last_read_seq = MAX(COALESCE(last_read_seq, 0), ?),
               max_seq = MAX(COALESCE(max_seq, 0), ?),
               visible_after_seq = MAX(COALESCE(visible_after_seq, 0), ?)
               WHERE conversation_id = ?"#,
        )
        .bind(cleared_through_seq as i64)
        .bind(cleared_through_seq as i64)
        .bind(cleared_through_seq as i64)
        .bind(conversation_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, conversation_id: &str) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        sqlx::query("DELETE FROM message_reactions WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM messages WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM pending_sends WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM conversation_participants WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM conversation_member_nicknames WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM sync_conversation_cursors WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        // 消息删掉了，证明这段历史完整的区间也要删掉。
        sqlx::query("DELETE FROM timeline_coverage WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM conversations WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
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

        let Some(mut source) = self.get(from).await? else {
            return Ok(());
        };
        // 旧行的名字可能是它自己的 id；换到新 id 下就再也认不出来，所以先按旧 id 清掉。
        source.display_name = business_name(&source.display_name, &[&source.channel_id, from]);
        source.conversation_id = to.to_string();
        let merged = match self.get(to).await? {
            Some(target) => merge_identity_conversation(target, source),
            None => source,
        };
        self.save_one(&merged).await?;

        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        sqlx::query("DELETE FROM conversation_participants WHERE conversation_id = ?")
            .bind(from)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        sqlx::query("DELETE FROM sync_conversation_cursors WHERE conversation_id = ?")
            .bind(from)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        sqlx::query("DELETE FROM pending_sends WHERE conversation_id = ?")
            .bind(from)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        sqlx::query("DELETE FROM conversations WHERE conversation_id = ?")
            .bind(from)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        tx.commit().await.map_err(sqlx_err)?;
        Ok(())
    }

    async fn get_local_max_seq(&self, conversation_id: &str) -> Result<u64> {
        let row = sqlx::query(
            r#"SELECT COALESCE(MAX(conversation_seq), 0) AS max_seq
               FROM messages
               WHERE conversation_id = ?"#,
        )
        .bind(conversation_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let max_seq = row.try_get::<i64, _>("max_seq").unwrap_or(0).max(0) as u64;
        Ok(max_seq)
    }

    /// I7：GROUP BY 一次查询批量取本地最大 seq（分块守 SQLite 绑定变量上限）。
    async fn get_local_max_seqs(
        &self,
        conversation_ids: &[String],
    ) -> Result<std::collections::HashMap<String, u64>> {
        let mut out = std::collections::HashMap::with_capacity(conversation_ids.len());
        for chunk in conversation_ids.chunks(super::SQLITE_IN_CHUNK) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = super::in_placeholders(chunk.len());
            let sql = format!(
                "SELECT conversation_id, COALESCE(MAX(conversation_seq), 0) AS max_seq
                 FROM messages
                 WHERE conversation_id IN ({placeholders})
                 GROUP BY conversation_id"
            );
            let mut query = sqlx::query(&sql);
            for conversation_id in chunk {
                query = query.bind(conversation_id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            for row in rows {
                let conversation_id: String = row.try_get("conversation_id").unwrap_or_default();
                let max_seq = row.try_get::<i64, _>("max_seq").unwrap_or(0).max(0) as u64;
                out.insert(conversation_id, max_seq);
            }
        }
        // 无消息的会话补 0，保证返回表覆盖全部入参。
        for conversation_id in conversation_ids {
            out.entry(conversation_id.clone()).or_insert(0);
        }
        Ok(out)
    }

    async fn save_member_nicknames(
        &self,
        snapshots: &[ParticipantDisplaySnapshot],
    ) -> Result<Vec<String>> {
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let now = crate::shared::util::id::now_millis() as i64;
        let mut changed: Vec<String> = Vec::new();
        for snapshot in snapshots
            .iter()
            .filter_map(ParticipantDisplaySnapshot::normalized)
        {
            let previous: Option<String> = sqlx::query_scalar(
                r#"SELECT nickname FROM conversation_member_nicknames
                   WHERE conversation_id = ? AND user_id = ?"#,
            )
            .bind(&snapshot.conversation_id)
            .bind(&snapshot.user_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(sqlx_err)?;
            if snapshot.nickname.is_empty() {
                if previous.is_none() {
                    continue;
                }
                sqlx::query(
                    r#"DELETE FROM conversation_member_nicknames
                       WHERE conversation_id = ? AND user_id = ?"#,
                )
                .bind(&snapshot.conversation_id)
                .bind(&snapshot.user_id)
                .execute(&mut *tx)
                .await
                .map_err(sqlx_err)?;
            } else {
                if previous.as_deref() == Some(snapshot.nickname.as_str()) {
                    continue;
                }
                sqlx::query(
                    r#"INSERT INTO conversation_member_nicknames
                           (conversation_id, user_id, nickname, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(conversation_id, user_id) DO UPDATE SET
                           nickname = excluded.nickname,
                           updated_at = excluded.updated_at"#,
                )
                .bind(&snapshot.conversation_id)
                .bind(&snapshot.user_id)
                .bind(&snapshot.nickname)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(sqlx_err)?;
            }
            if !changed.contains(&snapshot.conversation_id) {
                changed.push(snapshot.conversation_id);
            }
        }
        tx.commit().await.map_err(sqlx_err)?;
        Ok(changed)
    }

    async fn apply_user_profile_snapshot(
        &self,
        user_id: &str,
        _nickname: &str,
        avatar_url: &str,
    ) -> Result<Vec<String>> {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return Ok(Vec::new());
        }
        // 成员预览按 JSON 存，`"userId":"<id>"` 用 instr 精确找（不像 LIKE 有通配符）。
        let preview_needle = format!(
            "\"userId\":{}",
            serde_json::to_string(user_id).unwrap_or_default()
        );
        let rows = sqlx::query(
            r#"SELECT conversation_id
               FROM conversations
               WHERE (conversation_type = 1 AND channel_id = ?)
                  OR last_sender_id = ?
                  OR (conversation_type = 2
                      AND TRIM(COALESCE(remark, '')) IN ('', TRIM(channel_id), TRIM(conversation_id))
                      AND TRIM(display_name) IN ('', TRIM(channel_id), TRIM(conversation_id))
                      AND instr(COALESCE(member_preview, ''), ?) > 0)
               ORDER BY conversation_id"#,
        )
        .bind(user_id)
        .bind(user_id)
        .bind(&preview_needle)
        .fetch_all(&self.pool)
        .await
        .map_err(sqlx_err)?;

        let mut affected = Vec::with_capacity(rows.len());
        let now = crate::shared::util::id::now_millis() as i64;
        for row in rows {
            let conversation_id: String = row.try_get("conversation_id").map_err(sqlx_err)?;
            // 名字在读取时投影，这里只同步头像，不回写名字列。
            sqlx::query(
                r#"UPDATE conversations SET
                   avatar_url = CASE
                       WHEN conversation_type = 1 AND channel_id = ? THEN ?
                       ELSE avatar_url
                   END,
                   last_sender_avatar_url = CASE
                       WHEN last_sender_id = ? THEN ?
                       ELSE last_sender_avatar_url
                   END,
                   updated_at = ?
                   WHERE conversation_id = ?"#,
            )
            .bind(user_id)
            .bind(avatar_url)
            .bind(user_id)
            .bind(avatar_url)
            .bind(now)
            .bind(&conversation_id)
            .execute(&self.pool)
            .await
            .map_err(sqlx_err)?;
            affected.push(conversation_id);
        }
        Ok(affected)
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteConversationRepo;
    use crate::content::ContentBuilder;
    use crate::domain::{ConversationReader, ConversationWriter};
    use crate::model::{Conversation, ConversationListQuery, MessagePreviewElem};
    use crate::shared::error::ErrorCode;
    use sqlx::{Row, SqlitePool};

    async fn repo() -> SqliteConversationRepo {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        super::super::schema::init_schema(&pool).await.unwrap();
        SqliteConversationRepo::new(pool)
    }

    fn conversation(conversation_id: &str, conversation_seq: u64, text: &str) -> Conversation {
        Conversation {
            conversation_id: conversation_id.to_string(),
            business_type: "chat".to_string(),
            channel_id: conversation_id.to_string(),
            display_name: conversation_id.to_string(),
            last_message_id: Some(format!("msg-{conversation_seq}")),
            last_sender_id: Some("u1".to_string()),
            last_message_at: Some(conversation_seq * 1000),
            last_message_preview: Some(text.to_string()),
            last_message: Some(MessagePreviewElem {
                message_id: format!("msg-{conversation_seq}"),
                sender_id: "u1".to_string(),
                r#type: 1,
                text: text.to_string(),
                time: conversation_seq * 1000,
            }),
            max_seq: conversation_seq,
            updated_at: conversation_seq * 1000,
            created_at: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn mention_only_survives_partial_summary_in_upgraded_database() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        super::super::schema::init_schema(&pool).await.unwrap();
        sqlx::query("ALTER TABLE conversations DROP COLUMN is_mention_only")
            .execute(&pool)
            .await
            .unwrap();
        super::super::schema::init_schema(&pool).await.unwrap();
        let repo = SqliteConversationRepo::new(pool);
        assert!(repo.list().await.unwrap().is_empty());

        let mut row = conversation("group", 1, "first");
        row.is_mention_only = Some(true);
        repo.save_one(&row).await.unwrap();
        let mut incoming = conversation("group", 2, "next");
        repo.save_one(&incoming).await.unwrap();
        assert_eq!(
            repo.get("group").await.unwrap().unwrap().is_mention_only,
            Some(true)
        );

        incoming.is_mention_only = Some(false);
        repo.save_one(&incoming).await.unwrap();
        assert_eq!(
            repo.get("group").await.unwrap().unwrap().is_mention_only,
            Some(false)
        );
    }

    #[tokio::test]
    async fn list_repairs_missing_conversation_from_local_messages() {
        let repo = repo().await;

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, sender_name, sender_display_name,
                   encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
        )
        .bind("msg-orphan-1")
        .bind("conv-orphan")
        .bind("client-orphan-1")
        .bind("peer-1")
        .bind(1_i64)
        .bind(10_000_i64)
        .bind(10_000_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("peer-1")
        .bind("Peer One")
        .bind("Peer One")
        .bind("older")
        .bind(10_000_i64)
        .bind(10_000_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, sender_name, sender_display_name,
                   encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
        )
        .bind("msg-orphan-2")
        .bind("conv-orphan")
        .bind("client-orphan-2")
        .bind("peer-1")
        .bind(2_i64)
        .bind(20_000_i64)
        .bind(20_000_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("peer-1")
        .bind("Peer One")
        .bind("Peer One")
        .bind("latest")
        .bind(20_000_i64)
        .bind(20_000_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let listed = repo.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].conversation_id, "conv-orphan");
        assert_eq!(listed[0].channel_id, "peer-1");
        assert_eq!(
            listed[0].display_name, "",
            "a conversation rebuilt from messages has only ids, no name"
        );
        assert_eq!(listed[0].last_message_id.as_deref(), Some("msg-orphan-2"));
        assert_eq!(listed[0].last_sender_id.as_deref(), Some("peer-1"));
        assert_eq!(listed[0].last_message_at, Some(20_000));
        assert_eq!(listed[0].last_message_preview.as_deref(), Some("latest"));
        assert_eq!(listed[0].max_seq, 2);

        let loaded = repo.get("conv-orphan").await.unwrap().unwrap();
        assert_eq!(loaded.last_message_preview.as_deref(), Some("latest"));
    }

    #[tokio::test]
    async fn save_batch_keeps_local_latest_message_when_summary_is_stale() {
        let repo = repo().await;
        repo.save_one(&conversation("conv-1", 10, "new-local"))
            .await
            .unwrap();

        repo.save_one(&conversation("conv-1", 8, "old-server"))
            .await
            .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.max_seq, 10);
        assert_eq!(loaded.last_message_preview.as_deref(), Some("new-local"));
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-10"));
    }

    #[tokio::test]
    async fn save_batch_does_not_restore_messages_before_local_clear_floor() {
        let repo = repo().await;
        repo.save_one(&conversation("conv-1", 10, "before-delete"))
            .await
            .unwrap();
        repo.clear_local_chat_history("conv-1", 10).await.unwrap();

        repo.save_one(&conversation("conv-1", 10, "old-server"))
            .await
            .unwrap();
        let hidden = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(hidden.last_read_seq, 10);
        assert_eq!(hidden.unread_count, 0);
        assert!(hidden.last_message_id.is_none());
        assert!(hidden.last_message_preview.is_none());

        repo.save_one(&conversation("conv-1", 11, "after-readd"))
            .await
            .unwrap();
        let visible = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(visible.max_seq, 11);
        assert_eq!(visible.last_message_preview.as_deref(), Some("after-readd"));
        assert_eq!(visible.last_message_id.as_deref(), Some("msg-11"));
    }

    #[tokio::test]
    async fn save_batch_does_not_treat_previous_sync_cursor_as_visible_floor() {
        let repo = repo().await;
        let mut polluted = conversation("conv-1", 10, "cursor-polluted");
        polluted.visible_after_seq = 10;
        repo.save_one(&polluted).await.unwrap();

        repo.save_one(&conversation("conv-1", 10, "server-summary"))
            .await
            .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.visible_after_seq, 0);
        assert_eq!(
            loaded.last_message_preview.as_deref(),
            Some("server-summary")
        );
    }

    #[tokio::test]
    async fn save_batch_keeps_local_remark_when_summary_has_none() {
        let repo = repo().await;
        let mut local = conversation("conv-1", 10, "hello");
        local.conversation_type = crate::model::conversation::ConversationType::Single;
        local.channel_id = "peer-u2".to_string();
        local.remark = Some("我的备注".to_string());
        repo.save_one(&local).await.unwrap();

        let mut summary = conversation("conv-1", 10, "hello");
        summary.conversation_type = crate::model::conversation::ConversationType::Single;
        summary.channel_id = "123456".to_string();
        summary.remark = None;
        repo.save_batch(&[summary]).await.unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.remark.as_deref(), Some("我的备注"));
        assert_eq!(loaded.channel_id, "peer-u2");
    }

    /// 名字列只存业务名：写入时等于自己 id 的名字一律存空串，真名字照存。
    #[tokio::test]
    async fn save_batch_never_stores_an_id_as_the_name() {
        let repo = repo().await;
        let mut by_channel = conversation("1A_peer", 1, "hi");
        by_channel.channel_id = "u_peer".to_string();
        by_channel.display_name = "u_peer".to_string();
        let mut by_id = conversation("2A_self", 1, "hi");
        by_id.channel_id = "g_1".to_string();
        by_id.display_name = " 2A_self ".to_string();
        let mut named = conversation("2A_named", 1, "hi");
        named.channel_id = "g_2".to_string();
        named.display_name = "0401".to_string();
        repo.save_batch(&[by_channel, by_id, named]).await.unwrap();
        assert_eq!(repo.get("1A_peer").await.unwrap().unwrap().display_name, "");
        assert_eq!(repo.get("2A_self").await.unwrap().unwrap().display_name, "");
        assert_eq!(
            repo.get("2A_named").await.unwrap().unwrap().display_name,
            "0401"
        );
    }

    /// 资料变化：返回对端是此人的单聊、最后一条由此人发出的会话、此人在成员预览里的无名群；
    /// 只同步头像，名字列与旧的最后发送者名都不回写。
    #[tokio::test]
    async fn a_profile_change_reports_the_conversations_showing_the_person_and_writes_no_name() {
        use crate::model::ConversationParticipant;
        use crate::model::conversation::ConversationType;
        let repo = repo().await;
        let member = |id: &str| ConversationParticipant {
            user_id: id.to_string(),
            ..Default::default()
        };
        let mut single = conversation("1A_bob", 1, "hi");
        single.conversation_type = ConversationType::Single;
        single.channel_id = "u_bob".to_string();
        single.last_sender_id = Some("me".to_string());
        if let Some(last) = single.last_message.as_mut() {
            last.sender_id = "me".to_string();
        }
        let mut sent_by_bob = conversation("2A_named", 2, "hi");
        sent_by_bob.conversation_type = ConversationType::Group;
        sent_by_bob.channel_id = "g_named".to_string();
        sent_by_bob.display_name = "Design team".to_string();
        sent_by_bob.last_sender_id = Some("u_bob".to_string());
        sent_by_bob.last_sender_nickname = "Bob before".to_string();
        let mut unnamed = conversation("2A_unnamed", 3, "hi");
        unnamed.conversation_type = ConversationType::Group;
        unnamed.channel_id = "g_unnamed".to_string();
        unnamed.member_preview = vec![member("me"), member("u_bob")];
        let mut named_with_bob = conversation("2A_named_bob", 4, "hi");
        named_with_bob.conversation_type = ConversationType::Group;
        named_with_bob.channel_id = "g_nb".to_string();
        named_with_bob.display_name = "Ride".to_string();
        named_with_bob.member_preview = vec![member("me"), member("u_bob")];
        let mut unrelated = conversation("2A_other", 5, "hi");
        unrelated.conversation_type = ConversationType::Group;
        unrelated.channel_id = "g_other".to_string();
        unrelated.member_preview = vec![member("me"), member("u_bobby")];
        repo.save_batch(&[single, sent_by_bob, unnamed, named_with_bob, unrelated])
            .await
            .unwrap();

        let affected = repo
            .apply_user_profile_snapshot("u_bob", "Bob", "https://flare.test/bob.png")
            .await
            .unwrap();
        assert_eq!(affected, vec!["1A_bob", "2A_named", "2A_unnamed"]);
        let single = repo.get("1A_bob").await.unwrap().unwrap();
        assert_eq!(
            single.display_name, "",
            "the name column stays a business name"
        );
        assert_eq!(single.avatar_url, "https://flare.test/bob.png");
        let sent = repo.get("2A_named").await.unwrap().unwrap();
        assert_eq!(sent.last_sender_nickname, "Bob before");
        assert_eq!(sent.last_sender_avatar_url, "https://flare.test/bob.png");
    }

    #[tokio::test]
    async fn member_nicknames_are_stored_apart_from_members_and_read_in_one_batch() {
        use crate::domain::ParticipantDisplaySnapshot;
        let repo = repo().await;
        let changed = repo
            .save_member_nicknames(&[
                ParticipantDisplaySnapshot::new("2A_g", "u_a", "Group Ann"),
                ParticipantDisplaySnapshot::new("2A_g", "u_b", "u_b"),
                ParticipantDisplaySnapshot::new("2A_h", "u_a", "Other Ann"),
            ])
            .await
            .unwrap();
        assert_eq!(changed, vec!["2A_g", "2A_h"]);
        let again = repo
            .save_member_nicknames(&[ParticipantDisplaySnapshot::new("2A_g", "u_a", "Group Ann")])
            .await
            .unwrap();
        assert!(again.is_empty(), "an unchanged nickname changes nothing");
        let pairs = vec![
            ("2A_g".to_string(), "u_a".to_string()),
            ("2A_g".to_string(), "u_b".to_string()),
            ("2A_h".to_string(), "u_a".to_string()),
        ];
        let found = repo.member_nicknames(&pairs).await.unwrap();
        assert_eq!(
            found.len(),
            2,
            "a nickname equal to the user id is not stored"
        );
        assert_eq!(found.get(&pairs[0]).map(String::as_str), Some("Group Ann"));
        assert_eq!(found.get(&pairs[2]).map(String::as_str), Some("Other Ann"));

        let cleared = repo
            .save_member_nicknames(&[ParticipantDisplaySnapshot::new("2A_g", "u_a", "")])
            .await
            .unwrap();
        assert_eq!(cleared, vec!["2A_g"]);
        assert!(repo.member_nicknames(&pairs[..1]).await.unwrap().is_empty());
        repo.delete("2A_h").await.unwrap();
        assert!(repo.member_nicknames(&pairs[2..]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn save_batch_rejects_blank_conversation_id() {
        let repo = repo().await;
        let summary = conversation("   ", 1, "bad");

        let err = repo
            .save_batch(&[summary])
            .await
            .expect_err("blank conversation id must be rejected");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[tokio::test]
    async fn list_prunes_legacy_blank_conversation_id_rows() {
        let repo = repo().await;
        sqlx::query(
            r#"INSERT INTO conversations (
                   conversation_id, conversation_type, business_type, display_name,
                   avatar_url, updated_at, created_at
               )
               VALUES ('', 1, 'single', 'bad', '', 0, 0)"#,
        )
        .execute(&repo.pool)
        .await
        .unwrap();

        let conversations = repo.list().await.unwrap();

        assert!(conversations.is_empty());
    }

    #[tokio::test]
    async fn update_last_message_does_not_roll_back_newer_projection() {
        let repo = repo().await;
        repo.save_one(&conversation("conv-1", 10, "new-local"))
            .await
            .unwrap();

        repo.update_last_message("conv-1", "msg-8", "u2", 8000, Some("old-server"), 8)
            .await
            .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.max_seq, 10);
        assert_eq!(loaded.last_message_preview.as_deref(), Some("new-local"));
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-10"));
    }

    #[tokio::test]
    async fn update_last_message_accepts_newer_time_when_summary_max_seq_is_ahead() {
        let repo = repo().await;
        let mut summary = conversation("conv-1", 99, "stale-summary");
        summary.last_message_id = Some("msg-11".to_string());
        summary.last_sender_id = Some("u1".to_string());
        summary.last_message_at = Some(11_000);
        summary.last_message_preview = Some("stale-summary".to_string());
        repo.save_one(&summary).await.unwrap();

        repo.update_last_message("conv-1", "msg-12", "u2", 12_345, Some("111"), 12)
            .await
            .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.max_seq, 99);
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-12"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(loaded.last_message_at, Some(12_345));
        assert_eq!(loaded.last_message_preview.as_deref(), Some("111"));
    }

    #[tokio::test]
    async fn save_batch_does_not_erase_local_preview_with_empty_newer_summary() {
        let repo = repo().await;
        repo.save_one(&conversation("conv-1", 10, "local-preview"))
            .await
            .unwrap();

        let mut summary = conversation("conv-1", 11, "");
        summary.last_message_id = Some("msg-11".to_string());
        summary.last_sender_id = Some("u2".to_string());
        summary.last_message_at = Some(11_000);
        summary.last_message_preview = None;
        summary.last_message = None;
        repo.save_one(&summary).await.unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.max_seq, 11);
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-10"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u1"));
        assert_eq!(loaded.last_message_at, Some(10_000));
        assert_eq!(
            loaded.last_message_preview.as_deref(),
            Some("local-preview")
        );
    }

    /// member_preview 必须落库、且不被后续不带它的局部更新抹掉。
    ///
    /// 单聊的 channel_id / display_name 在服务端恒为空（同一行被会话两端共用，
    /// 装不下各自的对端），member_preview 是端上认出「对端是谁」的唯一通道。
    ///
    /// 这条判据同时守两个坑，两个都会静默失效：
    /// 1. 不落库 → 冷启内存里有、标题正确，但任何一次从库重读就退化成「会话」；
    /// 2. 落了库但读旧值的 SELECT 忘记带上这一列 → 沿用逻辑永远读到 None，
    ///    于是第一条不带 preview 的局部更新就把它清空。实测就是栽在第 2 点上。
    #[tokio::test]
    async fn save_batch_persists_member_preview_and_keeps_it_on_partial_update() {
        let repo = repo().await;

        let mut first = conversation("conv-1", 10, "hello");
        first.member_preview = vec![
            crate::model::ConversationParticipant {
                user_id: "me".to_string(),
                ..Default::default()
            },
            crate::model::ConversationParticipant {
                user_id: "peer".to_string(),
                ..Default::default()
            },
        ];
        repo.save_one(&first).await.unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(
            loaded.member_preview.len(),
            2,
            "member_preview 没有落库，端上刷新一次就认不出对端了"
        );

        // 局部更新（未读/新消息等）不带 member_preview——绝不能把它清空
        let partial = conversation("conv-1", 11, "world");
        assert!(partial.member_preview.is_empty());
        repo.save_one(&partial).await.unwrap();

        let after = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(after.max_seq, 11, "局部更新本身应当生效");
        assert_eq!(
            after
                .member_preview
                .iter()
                .map(|p| p.user_id.as_str())
                .collect::<Vec<_>>(),
            vec!["me", "peer"],
            "不带 member_preview 的局部更新把已有成员预览抹掉了"
        );
    }

    #[tokio::test]
    async fn list_by_query_filters_unread_type_keyword_and_marked_messages() {
        let repo = repo().await;
        let mut single = conversation("conv-single", 10, "needle marked");
        single.conversation_type = crate::model::conversation::ConversationType::Single;
        single.unread_count = 2;
        repo.save_one(&single).await.unwrap();

        let mut group = conversation("conv-group", 8, "needle plain");
        group.conversation_type = crate::model::conversation::ConversationType::Group;
        group.unread_count = 1;
        repo.save_one(&group).await.unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, attributes, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?, ?)"#,
        )
        .bind("marked-msg")
        .bind("conv-single")
        .bind("client-marked-msg")
        .bind("u2")
        .bind(10_i64)
        .bind(10_000_i64)
        .bind(10_000_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("u2")
        .bind("needle marked")
        .bind(r#"{"markType":"1"}"#)
        .bind(10_000_i64)
        .bind(10_000_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let filtered = repo
            .list_by_query(&ConversationListQuery {
                keyword: Some("needle".to_string()),
                unread_only: true,
                has_marked_messages: true,
                conversation_types: vec![crate::model::conversation::ConversationType::Single],
                ..ConversationListQuery::default()
            })
            .await
            .unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].conversation_id, "conv-single");
    }

    #[tokio::test]
    async fn update_unread_to_zero_clears_mention_badge() {
        let repo = repo().await;
        let mut conversation = conversation("conv-mention", 10, "mention");
        conversation.unread_count = 2;
        conversation.mention_count = 1;
        conversation.mention_me = true;
        repo.save_one(&conversation).await.unwrap();

        repo.update_unread("conv-mention", 0, 10).await.unwrap();

        let loaded = repo.get("conv-mention").await.unwrap().unwrap();
        assert_eq!(loaded.unread_count, 0);
        assert_eq!(loaded.mention_count, 0);
        assert!(!loaded.mention_me);

        let filtered = repo
            .list_by_query(&ConversationListQuery {
                mention_me_only: true,
                ..ConversationListQuery::default()
            })
            .await
            .unwrap();
        assert!(filtered.is_empty());
    }

    #[tokio::test]
    async fn read_model_uses_latest_visible_message_when_summary_preview_is_missing() {
        let repo = repo().await;
        let mut summary = conversation("conv-1", 12, "");
        summary.last_message_id = None;
        summary.last_sender_id = None;
        summary.last_message_at = Some(12_000);
        summary.last_message_preview = None;
        summary.last_message = None;
        summary.visible_after_seq = 10;
        repo.save_one(&summary).await.unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
        )
        .bind("msg-12")
        .bind("conv-1")
        .bind("client-12")
        .bind("u2")
        .bind(12_i64)
        .bind(12_345_i64)
        .bind(12_300_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("u2")
        .bind("latest-visible")
        .bind(12_345_i64)
        .bind(12_345_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-12"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(loaded.last_message_at, Some(12_345));
        assert_eq!(
            loaded.last_message_preview.as_deref(),
            Some("latest-visible")
        );

        let listed = repo.list().await.unwrap();
        assert_eq!(
            listed[0].last_message_preview.as_deref(),
            Some("latest-visible")
        );
    }

    #[tokio::test]
    async fn read_model_uses_latest_visible_message_when_summary_preview_is_stale() {
        let repo = repo().await;
        let mut summary = conversation("conv-1", 12, "stale-summary");
        summary.last_message_id = Some("msg-11".to_string());
        summary.last_sender_id = Some("u1".to_string());
        summary.last_message_at = Some(11_000);
        summary.max_seq = 12;
        repo.save_one(&summary).await.unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
        )
        .bind("msg-12")
        .bind("conv-1")
        .bind("client-12")
        .bind("u2")
        .bind(12_i64)
        .bind(12_345_i64)
        .bind(12_300_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("u2")
        .bind("latest-visible")
        .bind(12_345_i64)
        .bind(12_345_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-12"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(loaded.last_message_at, Some(12_345));
        assert_eq!(
            loaded.last_message_preview.as_deref(),
            Some("latest-visible")
        );
    }

    #[tokio::test]
    async fn read_model_uses_newer_local_message_when_summary_max_seq_is_ahead() {
        let repo = repo().await;
        let mut summary = conversation("conv-1", 99, "stale-summary");
        summary.last_message_id = Some("msg-11".to_string());
        summary.last_sender_id = Some("u1".to_string());
        summary.last_message_at = Some(11_000);
        summary.last_message_preview = Some("stale-summary".to_string());
        summary.max_seq = 99;
        repo.save_one(&summary).await.unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
        )
        .bind("msg-12")
        .bind("conv-1")
        .bind("client-12")
        .bind("u2")
        .bind(12_i64)
        .bind(12_345_i64)
        .bind(12_300_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("u2")
        .bind("111")
        .bind(12_345_i64)
        .bind(12_345_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.max_seq, 99);
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-12"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(loaded.last_message_at, Some(12_345));
        assert_eq!(loaded.last_message_preview.as_deref(), Some("111"));
    }

    #[tokio::test]
    async fn read_model_uses_extra_content_text_when_message_text_is_missing() {
        let repo = repo().await;
        let mut summary = conversation("conv-1", 99, "stale-summary");
        summary.last_message_id = Some("msg-11".to_string());
        summary.last_sender_id = Some("u1".to_string());
        summary.last_message_at = Some(11_000);
        summary.last_message_preview = Some("stale-summary".to_string());
        summary.max_seq = 99;
        repo.save_one(&summary).await.unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, attributes, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', NULL, ?, ?, ?)"#,
        )
        .bind("msg-12")
        .bind("conv-1")
        .bind("client-12")
        .bind("u2")
        .bind(12_i64)
        .bind(12_345_i64)
        .bind(12_300_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("u2")
        .bind(r#"{"contentText":"111"}"#)
        .bind(12_345_i64)
        .bind(12_345_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        assert_eq!(loaded.max_seq, 99);
        assert_eq!(loaded.last_message_id.as_deref(), Some("msg-12"));
        assert_eq!(loaded.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(loaded.last_message_at, Some(12_345));
        assert_eq!(loaded.last_message_preview.as_deref(), Some("111"));
    }

    #[tokio::test]
    async fn read_model_repairs_message_preview_from_encoded_content() {
        let repo = repo().await;
        let mut summary = conversation("conv-1", 31, "");
        summary.last_message_id = Some("msg-31".to_string());
        summary.last_sender_id = Some("u2".to_string());
        summary.last_message_at = Some(31_000);
        summary.last_message_preview = None;
        summary.last_message = None;
        summary.unread_count = 31;
        repo.save_one(&summary).await.unwrap();

        let encoded = ContentBuilder::text("encoded-body").build().encode();
        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, attributes, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '{}', ?, ?)"#,
        )
        .bind("msg-31")
        .bind("conv-1")
        .bind("client-31")
        .bind("u2")
        .bind(31_i64)
        .bind(31_000_i64)
        .bind(31_000_i64)
        .bind(1_i32)
        .bind(1_i32)
        .bind("u2")
        .bind(encoded)
        .bind(31_000_i64)
        .bind(31_000_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let loaded = repo.get("conv-1").await.unwrap().unwrap();
        let preview = loaded.last_message_preview.as_deref().unwrap_or_default();
        assert!(preview.contains("im.preview.user_text"));
        assert!(preview.contains("encoded-body"));

        let row = sqlx::query("SELECT text FROM messages WHERE server_id = 'msg-31'")
            .fetch_one(&repo.pool)
            .await
            .unwrap();
        let repaired_text: Option<String> = row.try_get("text").unwrap();
        assert_eq!(
            repaired_text.as_deref(),
            loaded.last_message_preview.as_deref()
        );
    }

    #[tokio::test]
    async fn list_repairs_single_chat_channel_alias_messages_into_canonical_conversation() {
        let repo = repo().await;
        let mut canonical = conversation("cid-canonical", 17, "");
        canonical.conversation_type = crate::model::conversation::ConversationType::Single;
        canonical.channel_id = "peer-12".to_string();
        canonical.display_name = "peer-12".to_string();
        canonical.last_message_id = None;
        canonical.last_sender_id = None;
        canonical.last_message_at = None;
        canonical.last_message_preview = None;
        canonical.last_message = None;
        canonical.unread_count = 17;
        repo.save_one(&canonical).await.unwrap();

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, sender_name, sender_display_name,
                   encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
        )
        .bind("server-alias-latest")
        .bind("peer-12")
        .bind("client-alias-latest")
        .bind("peer-12")
        .bind(17_i64)
        .bind(17_000_i64)
        .bind(17_000_i64)
        .bind(crate::model::conversation::ConversationType::Single.to_proto_int())
        .bind(1_i32)
        .bind("peer-12")
        .bind("Peer 12")
        .bind("Peer 12")
        .bind("hello-from-peer")
        .bind(17_000_i64)
        .bind(17_000_i64)
        .execute(&repo.pool)
        .await
        .unwrap();

        let loaded = repo.get("cid-canonical").await.unwrap().unwrap();
        assert_eq!(loaded.unread_count, 17);
        assert_eq!(
            loaded.last_message_preview.as_deref(),
            Some("hello-from-peer")
        );
        assert_eq!(
            loaded.last_message_id.as_deref(),
            Some("server-alias-latest")
        );

        let message_conversation_id: String =
            sqlx::query_scalar("SELECT conversation_id FROM messages WHERE server_id = ?")
                .bind("server-alias-latest")
                .fetch_one(&repo.pool)
                .await
                .unwrap();
        assert_eq!(message_conversation_id, "cid-canonical");
    }
}
