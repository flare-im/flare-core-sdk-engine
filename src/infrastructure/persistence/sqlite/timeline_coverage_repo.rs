use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::{SeqRange, TimelineCoverage, TimelineCoverageStore};
use crate::shared::error::{ErrorCode, FlareError, Result};

fn db_err(error: sqlx::Error) -> FlareError {
    FlareError::localized(ErrorCode::DatabaseError, error.to_string())
}

fn seq_value(seq: u64) -> i64 {
    i64::try_from(seq).unwrap_or(i64::MAX)
}

/// `timeline_coverage(conversation_id, from_seq, to_seq)`：一行一段，写入时在同一事务里
/// 删掉与它重叠、相邻的行，插入合并后的一段。
pub struct SqliteTimelineCoverageRepo {
    pool: SqlitePool,
}

impl SqliteTimelineCoverageRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TimelineCoverageStore for SqliteTimelineCoverageRepo {
    async fn coverage(&self, conversation_id: &str) -> Result<TimelineCoverage> {
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            r#"SELECT from_seq, to_seq FROM timeline_coverage
               WHERE conversation_id = ? ORDER BY from_seq"#,
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(TimelineCoverage::from_ranges(rows.into_iter().filter_map(
            |(from, to)| SeqRange::new(from.max(0) as u64, to.max(0) as u64),
        )))
    }

    async fn add(&self, conversation_id: &str, range: SeqRange) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let touching: Vec<(i64, i64)> = sqlx::query_as(
            r#"SELECT from_seq, to_seq FROM timeline_coverage
               WHERE conversation_id = ? AND from_seq <= ? AND to_seq >= ?"#,
        )
        .bind(conversation_id)
        .bind(seq_value(range.to.saturating_add(1)))
        .bind(seq_value(range.from.saturating_sub(1)))
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
        let mut merged = range;
        for (from, to) in &touching {
            let (from, to) = ((*from).max(0) as u64, (*to).max(0) as u64);
            if from <= range.from && range.to <= to {
                // 已被一段覆盖：不用写。
                return Ok(());
            }
            merged.from = merged.from.min(from);
            merged.to = merged.to.max(to);
        }
        sqlx::query(
            r#"DELETE FROM timeline_coverage
               WHERE conversation_id = ? AND from_seq <= ? AND to_seq >= ?"#,
        )
        .bind(conversation_id)
        .bind(seq_value(range.to.saturating_add(1)))
        .bind(seq_value(range.from.saturating_sub(1)))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query(
            r#"INSERT OR REPLACE INTO timeline_coverage (conversation_id, from_seq, to_seq)
               VALUES (?, ?, ?)"#,
        )
        .bind(conversation_id)
        .bind(seq_value(merged.from))
        .bind(seq_value(merged.to))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn clear(&self, conversation_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM timeline_coverage WHERE conversation_id = ?")
            .bind(conversation_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteTimelineCoverageRepo;
    use crate::domain::{ConversationReader, ConversationWriter, SeqRange, TimelineCoverageStore};
    use crate::infrastructure::persistence::sqlite::{SqliteConversationRepo, init_schema};
    use crate::infrastructure::persistence::timeline_coverage::contract;
    use crate::model::Conversation;
    use sqlx::SqlitePool;

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_schema(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn the_sqlite_store_follows_the_contract() {
        contract::assert_store_contract(&SqliteTimelineCoverageRepo::new(pool().await)).await;
    }

    #[tokio::test]
    async fn ranges_survive_reopening_the_repository() {
        let pool = pool().await;
        let repo = SqliteTimelineCoverageRepo::new(pool.clone());
        repo.add("c1", SeqRange::new(980, 1000).unwrap())
            .await
            .unwrap();
        repo.add("c1", SeqRange::new(1, 36).unwrap()).await.unwrap();
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT conversation_id, from_seq, to_seq FROM timeline_coverage ORDER BY from_seq",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![("c1".into(), 1, 36), ("c1".into(), 980, 1000)],
            "one row per merged range"
        );
        let reopened = SqliteTimelineCoverageRepo::new(pool);
        assert_eq!(
            reopened.coverage("c1").await.unwrap(),
            repo.coverage("c1").await.unwrap()
        );
    }

    /// 删除会话会删掉它的本地消息，区间必须一起删掉，不然会证明一段已经没有行的历史。
    #[tokio::test]
    async fn deleting_a_conversation_drops_its_ranges() {
        let pool = pool().await;
        let conversations = SqliteConversationRepo::new(pool.clone());
        conversations
            .save_one(&Conversation {
                conversation_id: "c1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let coverage = SqliteTimelineCoverageRepo::new(pool);
        coverage
            .add("c1", SeqRange::new(1, 40).unwrap())
            .await
            .unwrap();
        coverage
            .add("c2", SeqRange::new(1, 4).unwrap())
            .await
            .unwrap();
        conversations.delete("c1").await.unwrap();
        assert!(conversations.get("c1").await.unwrap().is_none());
        assert!(coverage.coverage("c1").await.unwrap().is_empty());
        assert!(!coverage.coverage("c2").await.unwrap().is_empty());
    }
}
