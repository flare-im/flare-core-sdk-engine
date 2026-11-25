//! SQLite：`user_file_download` + `file_download_settings`

use std::path::PathBuf;

use async_trait::async_trait;
use sqlx::SqlitePool;

use flare_im_core_sdk::storage::UserFileDownloadStore;
use flare_im_core_sdk::storage::now_millis;
use flare_im_core_sdk::{ErrorCode, FlareError, Result};

const DEFAULT_SUBFOLDER: &str = "flare";

fn now_ms() -> i64 {
    now_millis().min(i64::MAX as u64) as i64
}

pub struct SqliteUserFileDownloadRepo {
    pool: SqlitePool,
    fallback_root: Option<PathBuf>,
}

impl SqliteUserFileDownloadRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            fallback_root: None,
        }
    }

    /// 平台下载目录都不可用时的兜底根目录（通常是本地库同级的 `downloads`）。
    pub fn with_fallback_root(mut self, root: PathBuf) -> Self {
        self.fallback_root = Some(root);
        self
    }
}

fn sanitize_subfolder(name: &str) -> String {
    let t = name.trim();
    if t.is_empty() {
        return DEFAULT_SUBFOLDER.to_string();
    }
    let cleaned: String = t
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if cleaned.is_empty() || cleaned.chars().all(|c| c == '_') {
        DEFAULT_SUBFOLDER.to_string()
    } else {
        cleaned
    }
}

#[async_trait]
impl UserFileDownloadStore for SqliteUserFileDownloadRepo {
    async fn get_saved_path(&self, download_key: &str) -> Result<Option<String>> {
        let k = download_key.trim();
        if k.is_empty() {
            return Ok(None);
        }
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT local_path FROM user_file_download WHERE download_key = ? LIMIT 1",
        )
        .bind(k)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row.map(|r| r.0))
    }

    async fn save_download_record(
        &self,
        download_key: &str,
        local_path: &str,
        display_name: &str,
    ) -> Result<()> {
        let k = download_key.trim();
        if k.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "empty download_key",
            ));
        }
        let p = local_path.trim();
        if p.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "empty local_path",
            ));
        }
        let t = now_ms();
        sqlx::query(
            r#"INSERT INTO user_file_download (download_key, local_path, display_name, updated_at_ms)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(download_key) DO UPDATE SET
                 local_path = excluded.local_path,
                 display_name = excluded.display_name,
                 updated_at_ms = excluded.updated_at_ms"#,
        )
        .bind(k)
        .bind(p)
        .bind(display_name)
        .bind(t)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn get_download_subfolder(&self) -> Result<String> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT download_subfolder FROM file_download_settings WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row
            .map(|r| sanitize_subfolder(&r.0))
            .unwrap_or_else(|| DEFAULT_SUBFOLDER.to_string()))
    }

    async fn set_download_subfolder(&self, name: &str) -> Result<()> {
        let s = if name.trim().is_empty() {
            DEFAULT_SUBFOLDER.to_string()
        } else {
            sanitize_subfolder(name)
        };
        sqlx::query("UPDATE file_download_settings SET download_subfolder = ? WHERE singleton = 1")
            .bind(&s)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn get_download_directory(&self) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT download_directory FROM file_download_settings WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row
            .map(|r| r.0.trim().to_string())
            .filter(|dir| !dir.is_empty()))
    }

    async fn set_download_directory(&self, directory: Option<&str>) -> Result<()> {
        let value = directory.map(str::trim).unwrap_or("");
        sqlx::query("UPDATE file_download_settings SET download_directory = ? WHERE singleton = 1")
            .bind(value)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    fn fallback_download_root(&self) -> Option<PathBuf> {
        self.fallback_root.clone()
    }

    async fn delete_download_record(&self, download_key: &str) -> Result<()> {
        let k = download_key.trim();
        if k.is_empty() {
            return Ok(());
        }
        sqlx::query("DELETE FROM user_file_download WHERE download_key = ?")
            .bind(k)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }
}
