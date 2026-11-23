use sqlx::{Row, SqlitePool};

use crate::model::conversation::ConversationType;
use crate::shared::error::{ErrorCode, FlareError, Result};

fn sqlx_err(error: sqlx::Error) -> FlareError {
    FlareError::localized(ErrorCode::DatabaseError, error.to_string())
}

pub(super) async fn repair_single_chat_message_aliases(pool: &SqlitePool) -> Result<u64> {
    let rows = sqlx::query(
        r#"SELECT c.conversation_id, c.channel_id
           FROM conversations c
           WHERE c.conversation_type = ?
             AND TRIM(COALESCE(c.conversation_id, '')) != ''
             AND TRIM(COALESCE(c.channel_id, '')) != ''
             AND TRIM(c.conversation_id) != TRIM(c.channel_id)
             AND (EXISTS(SELECT 1 FROM messages WHERE conversation_id = TRIM(c.channel_id))
               OR EXISTS(SELECT 1 FROM message_reactions WHERE conversation_id = TRIM(c.channel_id)))"#,
    )
    .bind(ConversationType::Single.to_proto_int())
    .fetch_all(pool)
    .await
    .map_err(sqlx_err)?;

    let mut moved = 0_u64;
    for row in rows {
        let conversation_id: String = row.try_get("conversation_id").map_err(sqlx_err)?;
        let channel_id: String = row.try_get("channel_id").map_err(sqlx_err)?;
        moved += rewrite_message_conversation_alias(pool, &channel_id, &conversation_id).await?;
    }
    Ok(moved)
}

pub(super) async fn repair_single_chat_message_alias_for_conversation(
    pool: &SqlitePool,
    conversation_id: &str,
) -> Result<u64> {
    let conversation_id = conversation_id.trim();
    if conversation_id.is_empty() {
        return Ok(0);
    }
    let Some(row) = sqlx::query(
        r#"SELECT c.channel_id
           FROM conversations c
           WHERE c.conversation_id = ?
             AND c.conversation_type = ?
             AND TRIM(COALESCE(c.channel_id, '')) != ''
             AND TRIM(c.channel_id) != TRIM(c.conversation_id)
             AND (EXISTS(SELECT 1 FROM messages WHERE conversation_id = TRIM(c.channel_id))
               OR EXISTS(SELECT 1 FROM message_reactions WHERE conversation_id = TRIM(c.channel_id)))"#,
    )
    .bind(conversation_id)
    .bind(ConversationType::Single.to_proto_int())
    .fetch_optional(pool)
    .await
    .map_err(sqlx_err)?
    else {
        return Ok(0);
    };

    let channel_id: String = row.try_get("channel_id").map_err(sqlx_err)?;
    rewrite_message_conversation_alias(pool, &channel_id, conversation_id).await
}

async fn rewrite_message_conversation_alias(
    pool: &SqlitePool,
    from_conversation_id: &str,
    to_conversation_id: &str,
) -> Result<u64> {
    let from = from_conversation_id.trim();
    let to = to_conversation_id.trim();
    if from.is_empty() || to.is_empty() || from == to {
        return Ok(0);
    }

    let mut tx = pool.begin().await.map_err(sqlx_err)?;
    let result = sqlx::query("UPDATE messages SET conversation_id = ? WHERE conversation_id = ?")
        .bind(to)
        .bind(from)
        .execute(&mut *tx)
        .await
        .map_err(sqlx_err)?;
    sqlx::query("UPDATE messages_fts SET conversation_id = ? WHERE conversation_id = ?")
        .bind(to)
        .bind(from)
        .execute(&mut *tx)
        .await
        .map_err(sqlx_err)?;
    sqlx::query("UPDATE message_reactions SET conversation_id = ? WHERE conversation_id = ?")
        .bind(to)
        .bind(from)
        .execute(&mut *tx)
        .await
        .map_err(sqlx_err)?;
    tx.commit().await.map_err(sqlx_err)?;

    let moved = result.rows_affected();
    if moved > 0 {
        tracing::info!(
            from,
            to,
            moved,
            "repaired single chat message conversation alias"
        );
    }
    Ok(moved)
}
