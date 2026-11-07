use async_trait::async_trait;
use prost::Message as ProstMessage;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::domain::{PendingSendReader, PendingSendVo, PendingSendWriter};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// 待发队列（SQLite）。
///
/// 发送顺序按入队时刻（发件箱保证它在一个队列里严格递增，跨重启也续得上）；入队时刻相同的旧条目
/// 按写入先后（`rowid`），不看 `client_msg_id`（随机值，按它排会打乱会话内顺序）。
pub struct SqlitePendingSendRepo {
    pool: SqlitePool,
}

impl SqlitePendingSendRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn encode_entry(vo: &PendingSendVo) -> Vec<u8> {
    vo.message.to_proto().encode_to_vec()
}

fn decode_entry(data: &[u8], enqueued_at_ms: u64) -> Result<PendingSendVo> {
    let proto = crate::model::Message::decode(data).map_err(|e| {
        FlareError::localized(
            crate::shared::error::ErrorCode::DatabaseError,
            e.to_string(),
        )
    })?;
    let message = crate::model::IMMessage::from(proto);
    Ok(PendingSendVo {
        client_msg_id: message.client_msg_id.clone(),
        conversation_id: message.conversation_id.clone(),
        message,
        enqueued_at_ms,
    })
}

#[async_trait]
impl PendingSendReader for SqlitePendingSendRepo {
    async fn get(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
            "SELECT data, enqueued_at_ms FROM pending_sends WHERE client_msg_id = ?",
        )
        .bind(client_msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        match row {
            Some((data, enqueued_at_ms)) => Ok(Some(decode_entry(&data, enqueued_at_ms as u64)?)),
            None => Ok(None),
        }
    }

    async fn list(&self) -> Result<Vec<PendingSendVo>> {
        let rows: Vec<(Vec<u8>, i64)> = sqlx::query_as(
            "SELECT data, enqueued_at_ms FROM pending_sends ORDER BY enqueued_at_ms ASC, rowid ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for (data, enqueued_at_ms) in rows {
            out.push(decode_entry(&data, enqueued_at_ms as u64)?);
        }
        Ok(out)
    }

    async fn take_oldest(&self) -> Result<Option<PendingSendVo>> {
        // 先按索引解析队首 client_msg_id，再主键取 BLOB，避免单条语句对大表排序+拉全行。
        let row: Option<(Vec<u8>, i64)> = sqlx::query_as(
            r#"SELECT data, enqueued_at_ms FROM pending_sends
               WHERE client_msg_id = (
                 SELECT client_msg_id FROM pending_sends
                 ORDER BY enqueued_at_ms ASC, rowid ASC LIMIT 1
               )"#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row
            .map(|(data, enqueued_at_ms)| decode_entry(&data, enqueued_at_ms as u64))
            .transpose()?)
    }

    async fn list_oldest_excluding(
        &self,
        excluded_client_msg_ids: &[String],
        limit: usize,
    ) -> Result<Vec<PendingSendVo>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut builder =
            QueryBuilder::<Sqlite>::new("SELECT data, enqueued_at_ms FROM pending_sends");
        let excluded = excluded_client_msg_ids
            .iter()
            .filter(|id| !id.is_empty())
            .collect::<Vec<_>>();
        if !excluded.is_empty() {
            builder.push(" WHERE client_msg_id NOT IN (");
            let mut separated = builder.separated(", ");
            for id in excluded {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");
        }
        builder.push(" ORDER BY enqueued_at_ms ASC, rowid ASC LIMIT ");
        builder.push_bind(limit as i64);

        let rows: Vec<(Vec<u8>, i64)> = builder
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for (data, enqueued_at_ms) in rows {
            out.push(decode_entry(&data, enqueued_at_ms as u64)?);
        }
        Ok(out)
    }
}

#[async_trait]
impl PendingSendWriter for SqlitePendingSendRepo {
    async fn push(&self, entry: PendingSendVo) -> Result<()> {
        let data = encode_entry(&entry);
        sqlx::query(
            r#"INSERT OR REPLACE INTO pending_sends (client_msg_id, conversation_id, enqueued_at_ms, data)
               VALUES (?, ?, ?, ?)"#,
        )
        .bind(&entry.client_msg_id)
        .bind(&entry.conversation_id)
        .bind(entry.enqueued_at_ms as i64)
        .bind(&data)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn pop(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let vo = PendingSendReader::get(self, client_msg_id).await?;
        if vo.is_some() {
            sqlx::query("DELETE FROM pending_sends WHERE client_msg_id = ?")
                .bind(client_msg_id)
                .execute(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        }
        Ok(vo)
    }
}

#[cfg(test)]
mod tests {
    use super::SqlitePendingSendRepo;
    use crate::domain::{PendingSendReader, PendingSendVo, PendingSendWriter};
    use crate::infrastructure::persistence::sqlite::init_schema;
    use crate::model::IMMessage;
    use sqlx::SqlitePool;

    fn pending(client_msg_id: &str, enqueued_at_ms: u64) -> PendingSendVo {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = client_msg_id.to_string();
        message.conversation_id = "conv".to_string();
        PendingSendVo {
            client_msg_id: client_msg_id.to_string(),
            conversation_id: "conv".to_string(),
            message,
            enqueued_at_ms,
        }
    }

    #[tokio::test]
    async fn list_oldest_excluding_filters_in_flight_and_limits() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_schema(&pool).await.unwrap();
        let repo = SqlitePendingSendRepo::new(pool);
        for entry in [
            pending("client-1", 10),
            pending("client-2", 20),
            pending("client-3", 30),
            pending("client-4", 40),
        ] {
            repo.push(entry).await.unwrap();
        }

        let excluded = vec!["client-1".to_string(), "client-3".to_string()];
        let rows = repo.list_oldest_excluding(&excluded, 2).await.unwrap();

        let ids = rows
            .into_iter()
            .map(|entry| entry.client_msg_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["client-2".to_string(), "client-4".to_string()]);
    }

    /// 入队时刻相同（升级前写下的旧条目）时按写入先后，不按 client_msg_id：会话内顺序不被随机 ID 打乱。
    #[tokio::test]
    async fn equal_enqueue_times_keep_the_write_order() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_schema(&pool).await.unwrap();
        let repo = SqlitePendingSendRepo::new(pool);
        for id in ["zz-first", "mm-second", "aa-third"] {
            repo.push(pending(id, 7)).await.unwrap();
        }
        repo.push(pending("00-later", 8)).await.unwrap();

        let ids = |rows: Vec<PendingSendVo>| {
            rows.into_iter()
                .map(|entry| entry.client_msg_id)
                .collect::<Vec<_>>()
        };
        let order = vec!["zz-first", "mm-second", "aa-third", "00-later"];
        assert_eq!(
            ids(repo.list_oldest_excluding(&[], 10).await.unwrap()),
            order
        );
        assert_eq!(ids(repo.list().await.unwrap()), order);
        assert_eq!(
            repo.take_oldest().await.unwrap().unwrap().client_msg_id,
            "zz-first"
        );
    }
}
