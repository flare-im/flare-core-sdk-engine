//! 原生 SQLite 实现：`{data_root}/access/access.db`，单表键值，独立于按用户分库的 IM 库。

use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};

use super::{AccessStore, StoreError, StoreOp};

fn backend(error: sqlx::Error) -> StoreError {
    StoreError::Backend(error.to_string())
}

pub struct SqliteAccessStore {
    pool: SqlitePool,
    path: PathBuf,
}

impl SqliteAccessStore {
    /// 在 `{data_root}/access/access.db` 打开（不存在则创建目录与文件）。
    pub async fn open(data_root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let dir = data_root.as_ref().join("access");
        std::fs::create_dir_all(&dir)
            .map_err(|error| StoreError::Backend(format!("create {}: {error}", dir.display())))?;
        Self::open_file(dir.join("access.db")).await
    }

    /// 打开指定文件（测试或宿主自定义位置）。
    pub async fn open_file(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        // 单连接：写入量极小（清单、批量健康记录），串行化最简单，也避免与自己抢写锁。
        // WAL + NORMAL：事务仍然原子；掉电最多丢最后一次提交，对 maxSerial 只意味着稍旧的下界。
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(backend)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS access_kv (key TEXT PRIMARY KEY NOT NULL, value BLOB NOT NULL) WITHOUT ROWID",
        )
        .execute(&pool)
        .await
        .map_err(backend)?;
        Ok(Self { pool, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

const UPSERT: &str = "INSERT INTO access_kv (key, value) VALUES (?1, ?2) \
                      ON CONFLICT(key) DO UPDATE SET value = excluded.value";
const DELETE: &str = "DELETE FROM access_kv WHERE key = ?1";

#[async_trait::async_trait]
impl AccessStore for SqliteAccessStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let row = sqlx::query("SELECT value FROM access_kv WHERE key = ?1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        row.map(|row| row.try_get::<Vec<u8>, _>(0).map_err(backend))
            .transpose()
    }

    async fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        sqlx::query(UPSERT)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        sqlx::query(DELETE)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn apply(&self, ops: &[StoreOp]) -> Result<(), StoreError> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(backend)?;
        for op in ops {
            match op {
                StoreOp::Put { key, value } => {
                    sqlx::query(UPSERT)
                        .bind(key.as_str())
                        .bind(value.as_slice())
                        .execute(&mut *tx)
                        .await
                        .map_err(backend)?;
                }
                StoreOp::Delete { key } => {
                    sqlx::query(DELETE)
                        .bind(key.as_str())
                        .execute(&mut *tx)
                        .await
                        .map_err(backend)?;
                }
            }
        }
        // 任何一步出错时 tx 被丢弃即回滚，保证整批原子。
        tx.commit().await.map_err(backend)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let rows = sqlx::query(
            "SELECT key, value FROM access_kv WHERE substr(key, 1, length(?1)) = ?1 ORDER BY key",
        )
        .bind(prefix)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>(0).map_err(backend)?,
                    row.try_get::<Vec<u8>, _>(1).map_err(backend)?,
                ))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_across_reopen_and_scans_by_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteAccessStore::open(dir.path()).await.unwrap();
        assert!(store.path().ends_with("access/access.db"));
        store.put("serial/access/x", b"10").await.unwrap();
        store
            .apply(&[
                StoreOp::Put {
                    key: "health/ap/a/1".into(),
                    value: b"a".to_vec(),
                },
                StoreOp::Put {
                    key: "health/net/1".into(),
                    value: b"n".to_vec(),
                },
                StoreOp::Put {
                    key: "healthy".into(),
                    value: b"no".to_vec(),
                },
                StoreOp::Delete {
                    key: "serial/access/x".into(),
                },
            ])
            .await
            .unwrap();
        store.put("health/ap/a/1", b"b").await.unwrap();
        store.close().await;

        let reopened = SqliteAccessStore::open(dir.path()).await.unwrap();
        assert_eq!(reopened.get("serial/access/x").await.unwrap(), None);
        let health = reopened.scan_prefix("health/").await.unwrap();
        assert_eq!(
            health,
            vec![
                ("health/ap/a/1".to_owned(), b"b".to_vec()),
                ("health/net/1".to_owned(), b"n".to_vec()),
            ]
        );
        reopened.delete("health/net/1").await.unwrap();
        assert_eq!(reopened.scan_prefix("health/net/").await.unwrap(), vec![]);
    }
}
