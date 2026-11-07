//! SQLite 表结构初始化，供 `create_pool` 后调用。

use crate::shared::error::{ErrorCode, FlareError, Result};
use sqlx::{Row, SqlitePool};

const CURRENT_SCHEMA_VERSION: i64 = 4;

fn db_err(e: sqlx::Error) -> FlareError {
    FlareError::localized(ErrorCode::DatabaseError, e.to_string())
}

async fn current_user_version(pool: &SqlitePool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("PRAGMA user_version")
        .fetch_one(pool)
        .await
        .map_err(db_err)
}

async fn set_user_version(pool: &SqlitePool, version: i64) -> Result<()> {
    sqlx::query(&format!("PRAGMA user_version = {version}"))
        .execute(pool)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn column_exists(pool: &SqlitePool, table: &str, column: &str) -> Result<bool> {
    let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await
        .map_err(db_err)?;
    Ok(rows
        .iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .any(|name| name == column))
}

async fn ensure_column(
    pool: &SqlitePool,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    if column_exists(pool, table, column).await? {
        return Ok(());
    }
    sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {definition}"))
        .execute(pool)
        .await
        .map_err(db_err)?;
    Ok(())
}

async fn create_messages_fts(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            server_id UNINDEXED,
            conversation_id UNINDEXED,
            text,
            tokenize='trigram'
        )"#,
    )
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn ensure_current_message_columns(pool: &SqlitePool) -> Result<()> {
    for (column, definition) in [
        ("client_msg_id", "client_msg_id TEXT NOT NULL DEFAULT ''"),
        ("sender_id", "sender_id TEXT NOT NULL DEFAULT ''"),
        ("source", "source INTEGER NOT NULL DEFAULT 0"),
        (
            "conversation_seq",
            "conversation_seq INTEGER NOT NULL DEFAULT 0",
        ),
        ("created_at", "created_at INTEGER NOT NULL DEFAULT 0"),
        (
            "client_created_at",
            "client_created_at INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "conversation_type",
            "conversation_type INTEGER NOT NULL DEFAULT 0",
        ),
        ("message_type", "message_type INTEGER NOT NULL DEFAULT 0"),
        ("channel_id", "channel_id TEXT NOT NULL DEFAULT ''"),
        ("sender_name", "sender_name TEXT NOT NULL DEFAULT ''"),
        ("sender_avatar", "sender_avatar TEXT NOT NULL DEFAULT ''"),
        (
            "sender_display_name",
            "sender_display_name TEXT NOT NULL DEFAULT ''",
        ),
        (
            "encoded_content",
            "encoded_content BLOB NOT NULL DEFAULT X''",
        ),
        ("status", "status INTEGER NOT NULL DEFAULT 0"),
        (
            "retention_policy",
            "retention_policy BLOB NOT NULL DEFAULT X''",
        ),
        (
            "retention_state",
            "retention_state BLOB NOT NULL DEFAULT X''",
        ),
        ("is_read", "is_read INTEGER NOT NULL DEFAULT 0"),
        ("is_recalled", "is_recalled INTEGER NOT NULL DEFAULT 0"),
        ("is_edited", "is_edited INTEGER NOT NULL DEFAULT 0"),
        ("reply_to", "reply_to TEXT"),
        ("quote_preview", "quote_preview TEXT"),
        ("thread_id", "thread_id TEXT"),
        ("mention_users", "mention_users TEXT"),
        ("mention_all", "mention_all INTEGER NOT NULL DEFAULT 0"),
        ("attributes", "attributes TEXT"),
        ("extensions", "extensions TEXT"),
        ("version", "version INTEGER NOT NULL DEFAULT 0"),
        ("updated_at", "updated_at INTEGER NOT NULL DEFAULT 0"),
        ("text", "text TEXT"),
        ("sending", "sending INTEGER NOT NULL DEFAULT 0"),
        ("failed", "failed INTEGER NOT NULL DEFAULT 0"),
        ("is_local", "is_local INTEGER NOT NULL DEFAULT 0"),
        ("sort_ts", "sort_ts INTEGER NOT NULL DEFAULT 0"),
        // 本地媒体上传中与进度（0..=100）：发送方时间线要画进度条。以前只存了 sending/failed/is_local，
        // 读回时这两项恒为 false/0，原生端（SQLite）上传期间看不到进度，web（IndexedDB 存整条）却能。
        ("uploading", "uploading INTEGER NOT NULL DEFAULT 0"),
        (
            "upload_progress",
            "upload_progress INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        ensure_column(pool, "messages", column, definition).await?;
    }
    Ok(())
}

async fn ensure_current_conversation_columns(pool: &SqlitePool) -> Result<()> {
    for (column, definition) in [
        (
            "conversation_type",
            "conversation_type INTEGER NOT NULL DEFAULT 1",
        ),
        (
            "business_type",
            "business_type TEXT NOT NULL DEFAULT 'chat'",
        ),
        ("channel_id", "channel_id TEXT NOT NULL DEFAULT ''"),
        ("members_count", "members_count INTEGER NOT NULL DEFAULT 0"),
        ("display_name", "display_name TEXT NOT NULL DEFAULT ''"),
        ("avatar_url", "avatar_url TEXT NOT NULL DEFAULT ''"),
        ("remark", "remark TEXT"),
        ("description", "description TEXT"),
        ("last_message_id", "last_message_id TEXT"),
        ("last_sender_id", "last_sender_id TEXT"),
        ("last_message_at", "last_message_at INTEGER"),
        ("last_message_preview", "last_message_preview TEXT"),
        (
            "last_sender_nickname",
            "last_sender_nickname TEXT NOT NULL DEFAULT ''",
        ),
        (
            "last_sender_avatar_url",
            "last_sender_avatar_url TEXT NOT NULL DEFAULT ''",
        ),
        ("unread_count", "unread_count INTEGER NOT NULL DEFAULT 0"),
        ("last_read_seq", "last_read_seq INTEGER NOT NULL DEFAULT 0"),
        ("max_seq", "max_seq INTEGER NOT NULL DEFAULT 0"),
        (
            "visible_after_seq",
            "visible_after_seq INTEGER NOT NULL DEFAULT 0",
        ),
        ("is_pinned", "is_pinned INTEGER NOT NULL DEFAULT 0"),
        ("is_muted", "is_muted INTEGER NOT NULL DEFAULT 0"),
        ("is_mention_only", "is_mention_only INTEGER"),
        ("is_archived", "is_archived INTEGER NOT NULL DEFAULT 0"),
        ("version", "version INTEGER NOT NULL DEFAULT 0"),
        ("updated_at", "updated_at INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "created_at INTEGER NOT NULL DEFAULT 0"),
        ("updated_at_ts", "updated_at_ts INTEGER"),
        ("ext", "ext TEXT"),
        ("draft", "draft TEXT"),
        ("mention_count", "mention_count INTEGER NOT NULL DEFAULT 0"),
        ("mention_me", "mention_me INTEGER NOT NULL DEFAULT 0"),
        ("badge", "badge TEXT"),
        ("role", "role TEXT"),
        // 随会话摘要下发的成员预览（JSON 数组）。必须持久化：单聊的
        // channel_id / display_name 装不下各自的对端，端上认出「对端是谁」
        // 只能靠它。不存的话，冷启时内存里有、标题正确，但任何一次从本地库
        // 重读（收到新消息刷新列表等）都会退化成「会话」。
        ("member_preview", "member_preview TEXT"),
    ] {
        ensure_column(pool, "conversations", column, definition).await?;
    }
    Ok(())
}

async fn backfill_messages_fts(pool: &SqlitePool) -> Result<()> {
    sqlx::query("DELETE FROM messages_fts")
        .execute(pool)
        .await
        .map_err(db_err)?;

    sqlx::query(
        r#"INSERT INTO messages_fts(server_id, conversation_id, text)
           SELECT server_id, conversation_id, search_text
           FROM (
               SELECT
                   server_id,
                   conversation_id,
                   COALESCE(
                       NULLIF(TRIM(text), ''),
                       CASE
                           WHEN json_valid(attributes)
                           THEN NULLIF(TRIM(json_extract(attributes, '$.contentText')), '')
                           ELSE NULL
                       END,
                       ''
                   ) AS search_text
               FROM messages
           )
           WHERE search_text <> ''"#,
    )
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn migrate_to_v1(pool: &SqlitePool) -> Result<()> {
    ensure_current_message_columns(pool).await?;
    ensure_current_conversation_columns(pool).await?;
    create_messages_fts(pool).await?;
    backfill_messages_fts(pool).await
}

async fn migrate_to_v2(pool: &SqlitePool) -> Result<()> {
    ensure_current_message_columns(pool).await?;
    ensure_current_conversation_columns(pool).await?;
    create_messages_fts(pool).await?;
    backfill_messages_fts(pool).await
}

async fn migrate_to_v3(pool: &SqlitePool) -> Result<()> {
    ensure_current_message_columns(pool).await?;
    ensure_current_conversation_columns(pool).await?;
    create_messages_fts(pool).await?;
    backfill_messages_fts(pool).await
}

/// v4：名字列只存业务名。之前的版本会把对端 id、群 id 或会话 id 写进 `display_name`；
/// 这是本地缓存，名字随会话摘要同步回来，所以这里一次性清空，不在读侧猜「像不像 id」。
async fn migrate_to_v4(pool: &SqlitePool) -> Result<()> {
    clear_id_display_names(pool).await.map(|_| ())
}

pub(crate) async fn clear_id_display_names(pool: &SqlitePool) -> Result<u64> {
    let done = sqlx::query(
        r#"UPDATE conversations SET display_name = ''
           WHERE display_name != ''
             AND (TRIM(display_name) = TRIM(channel_id)
                  OR TRIM(display_name) = TRIM(conversation_id))"#,
    )
    .execute(pool)
    .await
    .map_err(db_err)?;
    Ok(done.rows_affected())
}

async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    let mut version = current_user_version(pool).await?;
    if version > CURRENT_SCHEMA_VERSION {
        tracing::warn!(
            current = version,
            supported = CURRENT_SCHEMA_VERSION,
            "SQLite schema version is newer than this SDK"
        );
        return Ok(());
    }

    let upgraded_from_empty = version < 1;
    if upgraded_from_empty {
        migrate_to_v1(pool).await?;
        set_user_version(pool, 1).await?;
        version = 1;
    }

    if version < 2 {
        if !upgraded_from_empty {
            migrate_to_v2(pool).await?;
        }
        set_user_version(pool, 2).await?;
        version = 2;
    }

    if version < 3 {
        if !upgraded_from_empty {
            migrate_to_v3(pool).await?;
        }
        set_user_version(pool, 3).await?;
        version = 3;
    }

    if version < 4 {
        if !upgraded_from_empty {
            migrate_to_v4(pool).await?;
        }
        set_user_version(pool, 4).await?;
        version = 4;
    }

    debug_assert_eq!(version, CURRENT_SCHEMA_VERSION);
    Ok(())
}

/// 创建所有仓储所需表（messages / conversations / pending_sends / user_profiles / sync_cursors / sync_conversation_cursors / timeline_coverage）
pub async fn init_schema(pool: &SqlitePool) -> Result<()> {
    super::plugin_command_repo::init_schema(pool).await?;
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS messages (
            server_id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            client_msg_id TEXT NOT NULL DEFAULT '',
            sender_id TEXT NOT NULL DEFAULT '',
            source INTEGER NOT NULL DEFAULT 0,
            conversation_seq INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL DEFAULT 0,
            client_created_at INTEGER NOT NULL DEFAULT 0,
            conversation_type INTEGER NOT NULL DEFAULT 0,
            message_type INTEGER NOT NULL DEFAULT 0,
            channel_id TEXT NOT NULL DEFAULT '',
            sender_name TEXT NOT NULL DEFAULT '',
            sender_avatar TEXT NOT NULL DEFAULT '',
            sender_display_name TEXT NOT NULL DEFAULT '',
            encoded_content BLOB NOT NULL,
            status INTEGER NOT NULL DEFAULT 0,
            retention_policy BLOB NOT NULL DEFAULT X'',
            retention_state BLOB NOT NULL DEFAULT X'',
            is_read INTEGER NOT NULL DEFAULT 0,
            is_recalled INTEGER NOT NULL DEFAULT 0,
            is_edited INTEGER NOT NULL DEFAULT 0,
            reply_to TEXT,
            quote_preview TEXT,
            thread_id TEXT,
            mention_users TEXT,
            mention_all INTEGER NOT NULL DEFAULT 0,
            attributes TEXT,
            extensions TEXT,
            version INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0,
            text TEXT,
            sending INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            is_local INTEGER NOT NULL DEFAULT 0,
            sort_ts INTEGER NOT NULL DEFAULT 0,
            uploading INTEGER NOT NULL DEFAULT 0,
            upload_progress INTEGER NOT NULL DEFAULT 0
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    ensure_current_message_columns(pool).await?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_messages_conv_seq
           ON messages(conversation_id, conversation_seq DESC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_messages_client_msg_id
           ON messages(client_msg_id)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS message_reactions (
            message_server_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL DEFAULT '',
            emoji TEXT NOT NULL,
            user_id TEXT NOT NULL,
            created_at INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (message_server_id, emoji, user_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_message_reactions_message
           ON message_reactions(message_server_id, updated_at DESC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_message_reactions_conversation
           ON message_reactions(conversation_id, updated_at DESC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS conversations (
            conversation_id TEXT PRIMARY KEY,
            conversation_type INTEGER NOT NULL,
            business_type TEXT NOT NULL,
            channel_id TEXT NOT NULL DEFAULT '',
            members_count INTEGER NOT NULL DEFAULT 0,
            display_name TEXT NOT NULL,
            avatar_url TEXT NOT NULL,
            remark TEXT,
            description TEXT,
            last_message_id TEXT,
            last_sender_id TEXT,
            last_message_at INTEGER,
            last_message_preview TEXT,
            last_sender_nickname TEXT NOT NULL DEFAULT '',
            last_sender_avatar_url TEXT NOT NULL DEFAULT '',
            unread_count INTEGER NOT NULL DEFAULT 0,
            last_read_seq INTEGER NOT NULL DEFAULT 0,
            max_seq INTEGER NOT NULL DEFAULT 0,
            visible_after_seq INTEGER NOT NULL DEFAULT 0,
            is_pinned INTEGER NOT NULL DEFAULT 0,
            is_muted INTEGER NOT NULL DEFAULT 0,
            is_mention_only INTEGER,
            is_archived INTEGER NOT NULL DEFAULT 0,
            version INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at_ts INTEGER,
            ext TEXT,
            draft TEXT,
            mention_count INTEGER NOT NULL DEFAULT 0,
            mention_me INTEGER NOT NULL DEFAULT 0,
            badge TEXT,
            role TEXT,
            member_preview TEXT
        );"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    ensure_current_conversation_columns(pool).await?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_conversations_sort
                ON conversations (
                    is_archived,
                    is_pinned DESC,
                    last_message_at DESC
                );"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_conversations_channel
           ON conversations (channel_id)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_conversations_last_message_id
           ON conversations (last_message_id)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS conversation_participants (
            conversation_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            roles TEXT NOT NULL DEFAULT '[]',
            muted INTEGER NOT NULL DEFAULT 0,
            pinned INTEGER NOT NULL DEFAULT 0,
            attributes TEXT NOT NULL DEFAULT '{}',
            joined_at INTEGER NOT NULL DEFAULT 0,
            nickname TEXT NOT NULL DEFAULT '',
            participant_version INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (conversation_id, user_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_conversation_participants_list
           ON conversation_participants (conversation_id, joined_at ASC, user_id ASC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS pending_sends (
            client_msg_id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            enqueued_at_ms INTEGER NOT NULL,
            data BLOB NOT NULL
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_pending_sends_enqueued_at_ms
           ON pending_sends (enqueued_at_ms ASC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    // 覆盖 (enqueued_at_ms, client_msg_id)，便于「队首」子查询尽量走索引、再按主键取 BLOB。
    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_pending_sends_queue_head
           ON pending_sends (enqueued_at_ms ASC, client_msg_id ASC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query("ANALYZE pending_sends")
        .execute(pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS user_profiles (
            user_id TEXT PRIMARY KEY,
            nickname TEXT NOT NULL DEFAULT '',
            avatar_url TEXT NOT NULL DEFAULT '',
            alias TEXT NOT NULL DEFAULT ''
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
    ensure_column(
        pool,
        "user_profiles",
        "alias",
        "alias TEXT NOT NULL DEFAULT ''",
    )
    .await?;

    // 账号在某个会话里的名字（社交业务里的群昵称），由业务 SDK 写入；与成员同步分开存，
    // 成员整页替换时不会把它冲掉。
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS conversation_member_nicknames (
            conversation_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            nickname TEXT NOT NULL,
            updated_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (conversation_id, user_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS sync_cursors (
            key TEXT PRIMARY KEY,
            cursor TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS sync_conversation_cursors (
            user_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            last_seq INTEGER NOT NULL DEFAULT 0,
            synced_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, conversation_id)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    // 时间线已证实连续区间：每行一段 [from_seq, to_seq]，同一会话的行互不重叠、互不相邻。
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS timeline_coverage (
            conversation_id TEXT NOT NULL,
            from_seq INTEGER NOT NULL,
            to_seq INTEGER NOT NULL,
            PRIMARY KEY (conversation_id, from_seq)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS media_upload_manifest (
            local_upload_id TEXT PRIMARY KEY,
            remote_upload_id TEXT,
            file_id TEXT,
            storage_upload_id TEXT,
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_locator TEXT NOT NULL,
            file_name TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            part_size INTEGER NOT NULL,
            total_parts INTEGER NOT NULL,
            transport_kind TEXT,
            bucket TEXT,
            object_key TEXT,
            upload_url TEXT,
            file_fingerprint TEXT NOT NULL,
            head_tail_sha256 TEXT NOT NULL,
            full_sha256 TEXT,
            upload_state TEXT NOT NULL,
            last_error_code TEXT,
            last_error_message TEXT,
            expires_at_ms INTEGER,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_media_upload_manifest_active
           ON media_upload_manifest (source_locator, file_fingerprint, upload_state, updated_at_ms DESC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS media_local_cache (
            file_id TEXT PRIMARY KEY,
            local_path TEXT NOT NULL,
            mime_type TEXT NOT NULL DEFAULT '',
            size_bytes INTEGER NOT NULL DEFAULT 0,
            updated_at_ms INTEGER NOT NULL DEFAULT 0
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_media_local_cache_updated
           ON media_local_cache (updated_at_ms DESC)"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS media_cache_settings (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            max_bytes INTEGER NOT NULL DEFAULT 0,
            cache_root TEXT NOT NULL DEFAULT ''
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"INSERT OR IGNORE INTO media_cache_settings (singleton, max_bytes, cache_root) VALUES (1, 0, '')"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS media_upload_part (
            local_upload_id TEXT NOT NULL,
            part_number INTEGER NOT NULL,
            offset_bytes INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            etag TEXT,
            uploaded INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (local_upload_id, part_number)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS user_file_download (
            download_key TEXT PRIMARY KEY,
            local_path TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            updated_at_ms INTEGER NOT NULL DEFAULT 0
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS file_download_settings (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            download_subfolder TEXT NOT NULL DEFAULT 'flare',
            download_directory TEXT NOT NULL DEFAULT ''
        )"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
    // 用户自选的下载目录（'' = 平台默认）；早期库没有这一列。
    ensure_column(
        pool,
        "file_download_settings",
        "download_directory",
        "download_directory TEXT NOT NULL DEFAULT ''",
    )
    .await?;

    sqlx::query(
        r#"INSERT OR IGNORE INTO file_download_settings (singleton, download_subfolder) VALUES (1, 'flare')"#,
    )
    .execute(pool)
    .await
    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

    run_migrations(pool).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{column_exists, current_user_version, init_schema};
    use crate::domain::ConversationReader;
    use sqlx::SqlitePool;

    /// v3 的库里名字列可能存着 id（对端、群、会话 id）；升到 v4 一次清空，真名字不动。
    #[tokio::test]
    async fn v4_clears_names_that_are_ids_and_keeps_real_ones() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_schema(&pool).await.unwrap();
        for (conversation_id, conversation_type, channel_id, display_name) in [
            ("1A_peer", 1, "u_peer", "u_peer"),
            ("2A_group", 2, "g_1", " g_1 "),
            ("2A_self", 2, "g_2", "2A_self"),
            ("2A_named", 2, "g_3", "Design team"),
            ("1A_digits", 1, "u_9", "0401"),
        ] {
            sqlx::query(
                r#"INSERT INTO conversations (
                       conversation_id, conversation_type, business_type, channel_id, members_count,
                       display_name, avatar_url, updated_at, created_at
                   ) VALUES (?, ?, 'chat', ?, 0, ?, '', 1, 1)"#,
            )
            .bind(conversation_id)
            .bind(conversation_type)
            .bind(channel_id)
            .bind(display_name)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query("PRAGMA user_version = 3")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE user_profiles DROP COLUMN alias")
            .execute(&pool)
            .await
            .unwrap();

        init_schema(&pool).await.unwrap();

        assert_eq!(current_user_version(&pool).await.unwrap(), 4);
        assert!(
            column_exists(&pool, "user_profiles", "alias")
                .await
                .unwrap()
        );
        let names: Vec<(String, String)> = sqlx::query_as(
            "SELECT conversation_id, display_name FROM conversations ORDER BY conversation_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            names,
            vec![
                ("1A_digits".to_string(), "0401".to_string()),
                ("1A_peer".to_string(), String::new()),
                ("2A_group".to_string(), String::new()),
                ("2A_named".to_string(), "Design team".to_string()),
                ("2A_self".to_string(), String::new()),
            ]
        );
    }

    #[tokio::test]
    async fn init_schema_migrates_legacy_v1_messages_without_conversation_seq() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            r#"CREATE TABLE messages (
                server_id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                encoded_content BLOB NOT NULL DEFAULT X'',
                attributes TEXT,
                text TEXT
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("PRAGMA user_version = 1")
            .execute(&pool)
            .await
            .unwrap();

        init_schema(&pool).await.unwrap();

        assert!(
            column_exists(&pool, "messages", "conversation_seq")
                .await
                .unwrap()
        );
        assert_eq!(current_user_version(&pool).await.unwrap(), 4);

        sqlx::query(
            r#"INSERT INTO messages (
                   server_id, conversation_id, sender_id, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind("legacy-msg-1")
        .bind("legacy-conv")
        .bind("peer-1")
        .bind("legacy preview")
        .bind(1_000_i64)
        .bind(1_000_i64)
        .execute(&pool)
        .await
        .unwrap();

        let repo = super::super::conversation_repo::SqliteConversationRepo::new(pool);
        let conversations = repo.list().await.unwrap();
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].conversation_id, "legacy-conv");
        assert_eq!(
            conversations[0].last_message_preview.as_deref(),
            Some("legacy preview")
        );
    }
}
