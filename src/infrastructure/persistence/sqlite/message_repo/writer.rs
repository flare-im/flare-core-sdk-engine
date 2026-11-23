use super::*;
use crate::shared::util::spawn_background;

const INLINE_FTS_BATCH_LIMIT: usize = 32;

#[async_trait]
impl MessageWriter for SqliteMessageRepo {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        let mut cleared_floors: HashMap<String, u64> = HashMap::new();
        let mut persistable: Vec<&IMMessage> = Vec::new();
        for m in messages {
            let cid = m.conversation_id.trim();
            if cid.is_empty() {
                continue;
            }
            let floor = if let Some(v) = cleared_floors.get(cid) {
                *v
            } else {
                let v = self.local_cleared_floor(cid).await?;
                cleared_floors.insert(cid.to_string(), v);
                v
            };
            if message_visible_after_clear(m, floor) {
                persistable.push(m);
            }
        }
        if persistable.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let has_pending_reactions: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM message_reactions WHERE event_seq > 0)",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(sqlx_err)?;
        let mut merged_messages: Vec<IMMessage> = Vec::with_capacity(persistable.len());
        let mut batch_indices: HashMap<String, usize> = HashMap::new();
        for incoming in persistable {
            let mut message = incoming.clone();
            // Uploads reach SQLite before the reliable queue assigns a local row ID.
            // The empty server ID would make every unacknowledged upload share a primary key.
            if message.local_state.is_local
                && message.conversation_seq == 0
                && message.server_id.is_empty()
                && !message.client_msg_id.is_empty()
            {
                message.server_id.clone_from(&message.client_msg_id);
            }
            let batch_index = batch_indices.get(&message.server_id).copied();
            if let Some(index) = batch_index {
                merge_message_event_state(&mut message, &merged_messages[index]);
            } else if let Some(row) = sqlx::query("SELECT * FROM messages WHERE (server_id = ? AND server_id != '') OR (client_msg_id = ? AND client_msg_id != '' AND is_local = 1 AND conversation_id = ? AND sender_id = ?) ORDER BY is_local ASC LIMIT 1")
                .bind(&message.server_id).bind(&message.client_msg_id).bind(&message.conversation_id).bind(&message.sender_id).fetch_optional(&mut *tx).await.map_err(sqlx_err)? {
                merge_message_event_state(&mut message, &self.row_to_immessage(&row)?);
            }
            if has_pending_reactions {
                merge_orphan_reactions_tx(&mut tx, &mut message).await?;
            }
            enforce_retention_visibility(&mut message);
            remove_conflicting_local_client_msg_rows_tx(
                &mut tx,
                &message.client_msg_id,
                &message.server_id,
                &message.conversation_id,
                &message.sender_id,
            )
            .await?;
            if let Some(index) = batch_index {
                merged_messages[index] = message;
            } else {
                if !message.server_id.trim().is_empty() {
                    batch_indices.insert(message.server_id.clone(), merged_messages.len());
                }
                merged_messages.push(message);
            }
        }
        let rows = merged_messages
            .iter()
            .map(MessagePersistRow::from_message)
            .collect::<Vec<_>>();

        let mut latest_per_conversation: HashMap<&str, &IMMessage> = HashMap::new();
        let should_update_fts_inline = rows.len() <= INLINE_FTS_BATCH_LIMIT;
        for chunk in rows.chunks(MESSAGE_SAVE_BATCH_INSERT_CHUNK_SIZE) {
            insert_message_rows_tx(&mut tx, chunk).await?;
            if should_update_fts_inline {
                upsert_message_fts_rows_tx(&mut tx, chunk).await?;
            }
            replace_reaction_snapshot_rows_tx(&mut tx, chunk).await?;
            for row in chunk {
                let m = row.message;
                let conv_id = m.conversation_id.trim();
                if conv_id.is_empty() {
                    continue;
                }
                match latest_per_conversation.get(conv_id) {
                    Some(prev) if !should_replace_conversation_projection(prev, m) => {}
                    _ => {
                        latest_per_conversation.insert(conv_id, m);
                    }
                }
            }
        }

        for (_, latest) in latest_per_conversation {
            upsert_conversation_snapshot_tx(&mut tx, latest).await?;
        }
        let deferred_fts_rows = if should_update_fts_inline {
            Vec::new()
        } else {
            rows.iter()
                .map(|row| row.message.server_id.clone())
                .collect()
        };
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        if !deferred_fts_rows.is_empty() {
            schedule_message_fts_backfill(self.pool.clone(), deferred_fts_rows);
        }
        Ok(())
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        MessageWriter::save_batch(self, std::slice::from_ref(message)).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        let recalled = MessageStatus::Recalled as i32;
        let result = if status == recalled {
            sqlx::query(&format!(
                r#"UPDATE messages SET status = ?, is_recalled = 1
                   WHERE {MESSAGE_ID_MATCH}"#
            ))
            .bind(status)
            .bind(message_id)
            .bind(message_id)
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(&format!(
                "UPDATE messages SET status = ? WHERE {MESSAGE_ID_MATCH}"
            ))
            .bind(status)
            .bind(message_id)
            .bind(message_id)
            .execute(&self.pool)
            .await
        }
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool> {
        Ok(self.apply_edit_event(message_id, new_content, None).await? == EditApplyResult::Applied)
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let deleted_conversation_id = sqlx::query(&format!(
            r#"SELECT conversation_id
               FROM messages
               WHERE {MESSAGE_ID_MATCH}
               LIMIT 1"#
        ))
        .bind(message_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?
        .and_then(|row| row.try_get::<String, _>("conversation_id").ok());
        delete_message_fts_by_message_id_tx(&mut tx, message_id).await?;
        sqlx::query(&format!(
            r#"DELETE FROM message_reactions
               WHERE message_server_id = ?
                  OR message_server_id IN (
                      SELECT server_id
                      FROM messages
                      WHERE {MESSAGE_ID_MATCH}
                  )"#
        ))
        .bind(message_id)
        .bind(message_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query(&format!("DELETE FROM messages WHERE {MESSAGE_ID_MATCH}"))
            .bind(message_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        if let Some(conversation_id) = deleted_conversation_id {
            refresh_conversation_snapshot_after_message_delete_tx(&mut tx, &conversation_id)
                .await?;
        }
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
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
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let result =
            sqlx::query("UPDATE messages SET conversation_id = ? WHERE conversation_id = ?")
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
        Ok(result.rows_affected())
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        let mut message = MessageDeliveryService::sanitize_send_ack_message(message);
        let ack_client_msg_id = client_msg_id.trim();
        if message.client_msg_id.trim().is_empty() && !ack_client_msg_id.is_empty() {
            message.client_msg_id = ack_client_msg_id.to_string();
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        // 下面整行替换乐观写入行（以及可能已同步落库的同 server_id 行）；本地插件卡片状态随行带过去。
        let rows = sqlx::query("SELECT * FROM messages WHERE (server_id = ? AND server_id != '') OR (client_msg_id IN (?, ?) AND conversation_id = ? AND sender_id = ?)")
            .bind(&message.server_id).bind(ack_client_msg_id).bind(&message.client_msg_id).bind(&message.conversation_id)
            .bind(&message.sender_id)
            .fetch_all(&mut *tx).await.map_err(sqlx_err)?;
        for row in rows {
            merge_message_event_state(&mut message, &self.row_to_immessage(&row)?);
        }
        remove_conflicting_client_msg_rows_tx(
            &mut tx,
            ack_client_msg_id,
            &message.server_id,
            &message.conversation_id,
            &message.sender_id,
        )
        .await?;
        if message.client_msg_id.trim() != ack_client_msg_id {
            remove_conflicting_client_msg_rows_tx(
                &mut tx,
                &message.client_msg_id,
                &message.server_id,
                &message.conversation_id,
                &message.sender_id,
            )
            .await?;
        }
        merge_orphan_reactions_tx(&mut tx, &mut message).await?;
        let rows = [MessagePersistRow::from_message(&message)];
        insert_message_rows_tx(&mut tx, &rows).await?;
        upsert_message_fts_rows_tx(&mut tx, &rows).await?;
        replace_reaction_snapshot_tx(&mut tx, &message).await?;
        upsert_conversation_snapshot_tx(&mut tx, &message).await?;
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }
}

fn schedule_message_fts_backfill(pool: SqlitePool, rows: Vec<String>) {
    spawn_background(async move {
        let row_count = rows.len();
        if let Err(error) = backfill_message_fts_rows(pool, rows).await {
            tracing::warn!(
                %error,
                row_count,
                "message FTS backfill failed after large batch save"
            );
        }
    });
}

async fn backfill_message_fts_rows(pool: SqlitePool, rows: Vec<String>) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.map_err(sqlx_err)?;
    let repo = SqliteMessageRepo::new(pool.clone());
    for chunk in rows.chunks(MESSAGE_SAVE_BATCH_INSERT_CHUNK_SIZE) {
        upsert_message_fts_backfill_rows_tx(&repo, &mut tx, chunk).await?;
        tokio::task::yield_now().await;
    }
    tx.commit().await.map_err(sqlx_err)?;
    Ok(())
}

async fn upsert_message_fts_backfill_rows_tx(
    repo: &SqliteMessageRepo,
    tx: &mut Transaction<'_, Sqlite>,
    rows: &[String],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // Deferred work must use the current row, never a pre-expiry/edit body snapshot.
    let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM messages WHERE server_id IN (");
    let mut ids = query.separated(", ");
    for id in rows {
        ids.push_bind(id);
    }
    query.push(")");
    let current = query.build().fetch_all(&mut **tx).await.map_err(sqlx_err)?;
    let messages = current
        .iter()
        .map(|row| repo.row_to_immessage(row))
        .collect::<Result<Vec<_>>>()?;
    let current_rows = messages
        .iter()
        .map(MessagePersistRow::from_message)
        .collect::<Vec<_>>();
    upsert_message_fts_rows_tx(tx, &current_rows).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delayed_fts_work_cannot_restore_purged_deleted_or_edited_text() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        crate::infrastructure::persistence::sqlite_init_schema(&pool)
            .await
            .unwrap();
        let repo = SqliteMessageRepo::new(pool.clone());
        let mut message = IMMessage::new(Default::default());
        message.server_id = "purged".into();
        message.conversation_id = "c".into();
        message.encoded_content = crate::content::ContentBuilder::text("old-private")
            .build()
            .encode();
        for id in ["purged", "deleted", "edited"] {
            message.server_id = id.into();
            message.client_msg_id = format!("local-{id}");
            repo.save_one(&message).await.unwrap();
        }
        repo.apply_retention_purged_event(
            "purged",
            &MessageRetentionState {
                lifecycle: 5,
                content_visibility: 4,
                ..Default::default()
            },
            30,
            Some(30),
        )
        .await
        .unwrap();
        repo.delete("deleted").await.unwrap();
        repo.apply_edit_event(
            "edited",
            crate::content::ContentBuilder::text("new-index")
                .build()
                .encode(),
            Some(20),
        )
        .await
        .unwrap();
        backfill_message_fts_rows(
            pool.clone(),
            vec!["purged".into(), "deleted".into(), "edited".into()],
        )
        .await
        .unwrap();
        let old: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM messages_fts WHERE text LIKE '%old-private%'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(old, 0);
        assert!(repo.search("old-private", 10).await.unwrap().is_empty());
        assert_eq!(
            repo.search("new-index", 10).await.unwrap()[0].server_id,
            "edited"
        );
        let stale: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM messages_fts WHERE server_id IN ('purged', 'deleted')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stale, 0);
    }
}
