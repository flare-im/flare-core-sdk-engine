use async_trait::async_trait;
use sqlx::SqlitePool;

use flare_im_core_sdk::storage::{SyncCursorReader, SyncCursorVo, SyncCursorWriter};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};

pub struct SqliteSyncCursorRepo {
    pool: SqlitePool,
}

impl SqliteSyncCursorRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SyncCursorReader for SqliteSyncCursorRepo {
    async fn get_raw(&self, key: &str) -> Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT cursor FROM sync_cursors WHERE key = ?")
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row.map(|(c,)| c))
    }

    async fn get_conversation_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<SyncCursorVo>> {
        let row: Option<(String, String, i64, i64)> = sqlx::query_as(
            r#"SELECT user_id, conversation_id, last_seq, synced_at
               FROM sync_conversation_cursors
               WHERE user_id = ? AND conversation_id = ?"#,
        )
        .bind(user_id)
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row.map(|(uid, cid, seq, synced_at)| SyncCursorVo {
            user_id: uid,
            conversation_id: cid,
            last_seq: seq.max(0) as u64,
            synced_at: synced_at.max(0) as u64,
        }))
    }

    /// 批量 raw 读取：单条 IN 查询（分块守 SQLite 绑定变量上限）。
    async fn get_raws(&self, keys: &[String]) -> Result<std::collections::HashMap<String, String>> {
        let mut out = std::collections::HashMap::with_capacity(keys.len());
        for chunk in keys.chunks(super::SQLITE_IN_CHUNK) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = super::in_placeholders(chunk.len());
            let sql = format!("SELECT key, cursor FROM sync_cursors WHERE key IN ({placeholders})");
            let mut query = sqlx::query_as::<_, (String, String)>(&sql);
            for key in chunk {
                query = query.bind(key);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            out.extend(rows);
        }
        Ok(out)
    }

    /// I7：单条 IN 查询批量取游标（分块守 SQLite 绑定变量上限）。
    async fn get_conversation_cursors(
        &self,
        user_id: &str,
        conversation_ids: &[String],
    ) -> Result<std::collections::HashMap<String, SyncCursorVo>> {
        let mut out = std::collections::HashMap::with_capacity(conversation_ids.len());
        for chunk in conversation_ids.chunks(super::SQLITE_IN_CHUNK) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = super::in_placeholders(chunk.len());
            let sql = format!(
                "SELECT user_id, conversation_id, last_seq, synced_at
                 FROM sync_conversation_cursors
                 WHERE user_id = ? AND conversation_id IN ({placeholders})"
            );
            let mut query = sqlx::query_as::<_, (String, String, i64, i64)>(&sql).bind(user_id);
            for conversation_id in chunk {
                query = query.bind(conversation_id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            for (uid, cid, seq, synced_at) in rows {
                out.insert(
                    cid.clone(),
                    SyncCursorVo {
                        user_id: uid,
                        conversation_id: cid,
                        last_seq: seq.max(0) as u64,
                        synced_at: synced_at.max(0) as u64,
                    },
                );
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl SyncCursorWriter for SqliteSyncCursorRepo {
    async fn save_raw(&self, key: &str, cursor: &str) -> Result<()> {
        sqlx::query(
            r#"INSERT OR REPLACE INTO sync_cursors (key, cursor, updated_at)
               VALUES (?, ?, datetime('now'))"#,
        )
        .bind(key)
        .bind(cursor)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn save_conversation_cursor(&self, cursor: &SyncCursorVo) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO sync_conversation_cursors
               (user_id, conversation_id, last_seq, synced_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, conversation_id) DO UPDATE SET
                   last_seq = MAX(sync_conversation_cursors.last_seq, excluded.last_seq),
                   synced_at = MAX(sync_conversation_cursors.synced_at, excluded.synced_at)"#,
        )
        .bind(&cursor.user_id)
        .bind(&cursor.conversation_id)
        .bind(cursor.last_seq as i64)
        .bind(cursor.synced_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn save_conversation_cursors(&self, cursors: &[SyncCursorVo]) -> Result<()> {
        if cursors.is_empty() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        for cursor in cursors {
            sqlx::query(
                r#"INSERT INTO sync_conversation_cursors
                   (user_id, conversation_id, last_seq, synced_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(user_id, conversation_id) DO UPDATE SET
                       last_seq = MAX(sync_conversation_cursors.last_seq, excluded.last_seq),
                       synced_at = MAX(sync_conversation_cursors.synced_at, excluded.synced_at)"#,
            )
            .bind(&cursor.user_id)
            .bind(&cursor.conversation_id)
            .bind(cursor.last_seq as i64)
            .bind(cursor.synced_at as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteSyncCursorRepo;
    use flare_im_core_sdk::storage::{SyncCursorReader, SyncCursorVo, SyncCursorWriter};
    use sqlx::SqlitePool;

    async fn repo() -> SqliteSyncCursorRepo {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        super::super::schema::init_schema(&pool).await.unwrap();
        SqliteSyncCursorRepo::new(pool)
    }

    #[tokio::test]
    async fn batch_cursor_read_matches_single_reads() {
        // I7：批量 IN 查询与逐条读取语义等价；无游标会话不出现在返回表。
        let repo = repo().await;
        for (conversation_id, seq) in [("c1", 5u64), ("c2", 9u64)] {
            repo.save_conversation_cursor(&SyncCursorVo {
                user_id: "u1".to_string(),
                conversation_id: conversation_id.to_string(),
                last_seq: seq,
                synced_at: 100,
            })
            .await
            .unwrap();
        }

        let ids = vec!["c1".to_string(), "c2".to_string(), "c-missing".to_string()];
        let batch = repo.get_conversation_cursors("u1", &ids).await.unwrap();

        assert_eq!(batch.len(), 2);
        assert_eq!(batch.get("c1").map(|c| c.last_seq), Some(5));
        assert_eq!(batch.get("c2").map(|c| c.last_seq), Some(9));
        assert!(!batch.contains_key("c-missing"));
        // 与单条读取一致。
        let single = repo
            .get_conversation_cursor("u1", "c1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(single.last_seq, batch.get("c1").unwrap().last_seq);
    }
}
