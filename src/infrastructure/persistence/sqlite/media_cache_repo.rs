use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use tokio::sync::{Mutex, RwLock};

use crate::domain::{
    DEFAULT_MEDIA_CACHE_MAX_BYTES, MediaCacheAdmin, MediaCacheEntryVo, MediaCacheStatsVo,
    MediaCacheStore,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// 读命中时刷新最近使用时间的最小间隔：列表滚动会反复读同一批图，不必每次都写库。
const TOUCH_INTERVAL_MS: i64 = 60_000;

#[derive(Debug, Clone)]
struct MediaCacheState {
    /// 用户设置的上限；0 表示没设置过，用 [`DEFAULT_MEDIA_CACHE_MAX_BYTES`]。
    max_bytes: u64,
    root_override: Option<PathBuf>,
}

impl MediaCacheState {
    fn effective_max_bytes(&self) -> u64 {
        if self.max_bytes == 0 {
            DEFAULT_MEDIA_CACHE_MAX_BYTES
        } else {
            self.max_bytes
        }
    }
}

pub struct SqliteMediaCacheRepo {
    pool: SqlitePool,
    /// 与 `flare_im_sdk.db` 同目录下的 `media_cache`（未自定义目录时生效）
    default_root: PathBuf,
    state: Arc<RwLock<MediaCacheState>>,
    format_paths: Mutex<()>,
}

impl SqliteMediaCacheRepo {
    /// 从库中读取 `media_cache_settings` 并完成初始化。
    pub async fn create(pool: SqlitePool, default_root: PathBuf) -> Result<Self> {
        let _ = tokio::fs::create_dir_all(&default_root).await;

        let row = sqlx::query(
            "SELECT max_bytes, cache_root FROM media_cache_settings WHERE singleton = 1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        let (max_bytes, root_override) = if let Some(row) = row {
            let mb: i64 = row.get("max_bytes");
            let root_s: String = row.get("cache_root");
            let t = root_s.trim();
            let ov = if t.is_empty() {
                None
            } else {
                Some(PathBuf::from(t))
            };
            (mb.max(0) as u64, ov)
        } else {
            (0u64, None)
        };

        Ok(Self {
            pool,
            default_root,
            format_paths: Mutex::new(()),
            state: Arc::new(RwLock::new(MediaCacheState {
                max_bytes,
                root_override,
            })),
        })
    }

    async fn effective_root(&self) -> PathBuf {
        let s = self.state.read().await;
        s.root_override
            .clone()
            .unwrap_or_else(|| self.default_root.clone())
    }

    /// 按 MIME 主类型分目录：图片 / 音频 / 视频 / 其余（对应 `image`、`audio`、`video`、`other`）。
    fn media_kind_dir(mime_type: &str) -> &'static str {
        fn starts_with_icase(hay: &str, needle: &str) -> bool {
            hay.len() >= needle.len() && hay[..needle.len()].eq_ignore_ascii_case(needle)
        }
        let t = mime_type.trim();
        if starts_with_icase(t, "image/") {
            "image"
        } else if starts_with_icase(t, "audio/") {
            "audio"
        } else if starts_with_icase(t, "video/") {
            "video"
        } else {
            "other"
        }
    }

    fn blob_path(root: &Path, file_id: &str, mime_type: &str) -> PathBuf {
        let kind = Self::media_kind_dir(mime_type);
        let mut hasher = Sha256::new();
        hasher.update(file_id.as_bytes());
        let hex = format!("{:x}", hasher.finalize());
        let dir = &hex[..2];
        let name = &hex[2..];
        let path = root.join(kind).join(dir).join(name);
        match Self::playback_extension(mime_type) {
            Some(extension) => path.with_extension(extension),
            None => path,
        }
    }

    // AVFoundation does not identify extensionless AVURLAssets even when the
    // bytes are valid (e.g. PCM WAVE). Use a fixed MIME allowlist, never a
    // sender-supplied filename or arbitrary path suffix.
    fn playback_extension(mime_type: &str) -> Option<&'static str> {
        match mime_type
            .split(';')
            .next()?
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "audio/wav" | "audio/x-wav" | "audio/wave" => Some("wav"),
            "audio/mpeg" => Some("mp3"),
            "audio/aac" => Some("aac"),
            "audio/mp4" | "audio/x-m4a" => Some("m4a"),
            "audio/ogg" => Some("ogg"),
            "video/mp4" => Some("mp4"),
            "video/quicktime" => Some("mov"),
            "video/webm" => Some("webm"),
            _ => None,
        }
    }

    async fn trim_to_max_bytes(&self) -> Result<()> {
        let max = { self.state.read().await.effective_max_bytes() };
        let max_i64 = i64::try_from(max).unwrap_or(i64::MAX);
        loop {
            let sum: i64 =
                sqlx::query_scalar("SELECT COALESCE(SUM(size_bytes), 0) FROM media_local_cache")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            if sum <= max_i64 {
                break;
            }
            if !self.remove_oldest_entry().await? {
                break;
            }
        }
        Ok(())
    }

    async fn remove_oldest_entry(&self) -> Result<bool> {
        let row =
            sqlx::query("SELECT file_id FROM media_local_cache ORDER BY updated_at_ms ASC LIMIT 1")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let Some(row) = row else {
            return Ok(false);
        };
        let fid: String = row.get("file_id");
        self.remove(&fid).await?;
        Ok(true)
    }

    async fn clear_all_entries(&self) -> Result<()> {
        let rows = sqlx::query("SELECT local_path FROM media_local_cache")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM media_local_cache")
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        for row in rows {
            let p: String = row.get("local_path");
            let _ = tokio::fs::remove_file(Path::new(&p)).await;
        }
        // 中断的写入留下的暂存文件（没有行指向它们）。
        let _ = tokio::fs::remove_dir_all(self.effective_root().await.join(".staging")).await;
        Ok(())
    }
}

#[async_trait]
impl MediaCacheStore for SqliteMediaCacheRepo {
    async fn get_cached(&self, file_id: &str) -> Result<Option<MediaCacheEntryVo>> {
        let _format_guard = self.format_paths.lock().await;
        let fid = file_id.trim();
        if fid.is_empty() {
            return Ok(None);
        }
        let row = sqlx::query(
            r#"SELECT file_id, local_path, mime_type, size_bytes, updated_at_ms
               FROM media_local_cache WHERE file_id = ?"#,
        )
        .bind(fid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        let Some(row) = row else {
            return Ok(None);
        };

        let mut path: String = row.get("local_path");
        if tokio::fs::metadata(&path).await.is_err() {
            let _ = sqlx::query("DELETE FROM media_local_cache WHERE file_id = ?")
                .bind(fid)
                .execute(&self.pool)
                .await;
            return Ok(None);
        }

        let mime_type: String = row.get("mime_type");
        let cached = Path::new(&path);
        if cached.extension().is_none()
            && let Some(extension) = Self::playback_extension(&mime_type)
        {
            let playable = cached.with_extension(extension);
            // This moves only a disposable SDK-owned cache blob. Source
            // uploads, user downloads, message state and authorization stay
            // untouched. An open POSIX descriptor remains valid.
            tokio::fs::rename(cached, &playable)
                .await
                .map_err(|_| FlareError::system("media cache format path unavailable"))?;
            let next = playable.to_string_lossy().to_string();
            let updated = sqlx::query(
                "UPDATE media_local_cache SET local_path = ? WHERE file_id = ? AND local_path = ?",
            )
            .bind(&next)
            .bind(fid)
            .bind(&path)
            .execute(&self.pool)
            .await;
            if let Err(error) = updated {
                let _ = tokio::fs::rename(&playable, cached).await;
                return Err(FlareError::localized(
                    ErrorCode::DatabaseError,
                    error.to_string(),
                ));
            }
            path = next;
        }

        // 淘汰按最近使用：命中时刷新时间，常看的图不会因为「写得早」被先删。
        let mut updated_at_ms: i64 = row.get("updated_at_ms");
        let now = chrono::Utc::now().timestamp_millis();
        if now - updated_at_ms >= TOUCH_INTERVAL_MS {
            let _ = sqlx::query("UPDATE media_local_cache SET updated_at_ms = ? WHERE file_id = ?")
                .bind(now)
                .bind(fid)
                .execute(&self.pool)
                .await;
            updated_at_ms = now;
        }

        Ok(Some(MediaCacheEntryVo {
            file_id: row.get("file_id"),
            local_path: path,
            mime_type,
            size_bytes: row.get("size_bytes"),
            updated_at_ms,
        }))
    }

    async fn put_bytes(
        &self,
        file_id: &str,
        data: &[u8],
        mime_type: &str,
    ) -> Result<MediaCacheEntryVo> {
        let staged = self
            .staging_dir()
            .await?
            .join(format!("{}.part", uuid::Uuid::new_v4().simple()));
        tokio::fs::write(&staged, data)
            .await
            .map_err(|e| FlareError::system(format!("media cache write: {e}")))?;
        let result = self.put_file(file_id, &staged, mime_type, true).await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&staged).await;
        }
        result
    }

    async fn staging_dir(&self) -> Result<PathBuf> {
        let dir = self.effective_root().await.join(".staging");
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| FlareError::system(format!("media cache mkdir: {e}")))?;
        Ok(dir)
    }

    async fn put_file(
        &self,
        file_id: &str,
        source: &Path,
        mime_type: &str,
        move_source: bool,
    ) -> Result<MediaCacheEntryVo> {
        let fid = file_id.trim();
        if fid.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "media cache: empty file_id",
            ));
        }

        let root = self.effective_root().await;
        let dest = Self::blob_path(&root, fid, mime_type);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FlareError::system(format!("media cache mkdir: {e}")))?;
        }

        // 先到同目录的临时名再改名：读者要么看到旧文件，要么看到完整的新文件。
        let tmp = dest.with_extension("part");
        let placed = if move_source {
            match tokio::fs::rename(source, &tmp).await {
                Ok(()) => Ok(()),
                // 跨文件系统改名会失败，退回复制后删源。
                Err(_) => tokio::fs::copy(source, &tmp).await.map(|_| {
                    let _ = std::fs::remove_file(source);
                }),
            }
        } else {
            tokio::fs::copy(source, &tmp).await.map(|_| ())
        };
        placed.map_err(|e| FlareError::system(format!("media cache write: {e}")))?;
        if let Err(e) = tokio::fs::rename(&tmp, &dest).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(FlareError::system(format!("media cache rename: {e}")));
        }

        let size = tokio::fs::metadata(&dest)
            .await
            .map_err(|e| FlareError::system(format!("media cache stat: {e}")))?
            .len();
        let local_path = dest.to_string_lossy().to_string();
        let size_bytes = i64::try_from(size)
            .map_err(|_| FlareError::general_error("media cache: payload too large"))?;
        let now = chrono::Utc::now().timestamp_millis();

        sqlx::query(
            r#"INSERT INTO media_local_cache (file_id, local_path, mime_type, size_bytes, updated_at_ms)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(file_id) DO UPDATE SET
                 local_path = excluded.local_path,
                 mime_type = excluded.mime_type,
                 size_bytes = excluded.size_bytes,
                 updated_at_ms = excluded.updated_at_ms"#,
        )
        .bind(fid)
        .bind(&local_path)
        .bind(mime_type)
        .bind(size_bytes)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        self.trim_to_max_bytes().await?;

        Ok(MediaCacheEntryVo {
            file_id: fid.to_string(),
            local_path,
            mime_type: mime_type.to_string(),
            size_bytes,
            updated_at_ms: now,
        })
    }

    async fn remove(&self, file_id: &str) -> Result<()> {
        let fid = file_id.trim();
        if fid.is_empty() {
            return Ok(());
        }
        let row = sqlx::query("SELECT local_path FROM media_local_cache WHERE file_id = ?")
            .bind(fid)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        if let Some(row) = row {
            let path: String = row.get("local_path");
            let _ = tokio::fs::remove_file(Path::new(&path)).await;
        }
        sqlx::query("DELETE FROM media_local_cache WHERE file_id = ?")
            .bind(fid)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl MediaCacheAdmin for SqliteMediaCacheRepo {
    async fn media_cache_stats(&self) -> Result<MediaCacheStatsVo> {
        let effective = self.effective_root().await;
        let default_s = self.default_root.to_string_lossy().to_string();
        let (max_bytes, max_bytes_is_default) = {
            let state = self.state.read().await;
            (state.effective_max_bytes(), state.max_bytes == 0)
        };
        let total: i64 =
            sqlx::query_scalar("SELECT COALESCE(SUM(size_bytes), 0) FROM media_local_cache")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let entry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_local_cache")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(MediaCacheStatsVo {
            effective_root: effective.to_string_lossy().to_string(),
            default_root: default_s,
            max_bytes,
            max_bytes_is_default,
            total_bytes: total,
            entry_count,
        })
    }

    async fn set_media_cache_max_bytes(&self, max_bytes: u64) -> Result<()> {
        let mb_i64 = i64::try_from(max_bytes).unwrap_or(i64::MAX);
        sqlx::query("UPDATE media_cache_settings SET max_bytes = ? WHERE singleton = 1")
            .bind(mb_i64)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        self.state.write().await.max_bytes = max_bytes;
        self.trim_to_max_bytes().await
    }

    async fn set_media_cache_root(&self, absolute_path: Option<&str>) -> Result<()> {
        self.clear_all_entries().await?;

        let stored = match absolute_path.map(str::trim).filter(|s| !s.is_empty()) {
            None => String::new(),
            Some(p) => {
                let pb = PathBuf::from(p);
                if !pb.is_absolute() {
                    return Err(FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "media cache root must be an absolute path",
                    ));
                }
                let _ = tokio::fs::create_dir_all(&pb)
                    .await
                    .map_err(|e| FlareError::system(format!("media cache root not usable: {e}")))?;
                p.to_string()
            }
        };

        sqlx::query("UPDATE media_cache_settings SET cache_root = ? WHERE singleton = 1")
            .bind(&stored)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;

        let mut w = self.state.write().await;
        w.root_override = if stored.is_empty() {
            None
        } else {
            Some(PathBuf::from(stored))
        };
        Ok(())
    }

    async fn clear_media_cache(&self) -> Result<()> {
        self.clear_all_entries().await
    }
}

#[cfg(test)]
mod playback_path_tests {
    use super::*;

    #[test]
    fn playback_suffix_is_a_fixed_mime_mapping_not_a_filename() {
        let root = Path::new("/cache");
        for (mime, suffix) in [
            ("audio/wav", "wav"),
            ("Audio/MP4; codecs=mp4a", "m4a"),
            ("video/mp4", "mp4"),
        ] {
            let path = SqliteMediaCacheRepo::blob_path(root, "../../sender-id", mime);
            assert!(path.starts_with(root));
            assert_eq!(path.extension().and_then(|s| s.to_str()), Some(suffix));
        }
        for mime in [
            "audio/../../secret",
            "application/octet-stream",
            "image/png",
        ] {
            assert!(
                SqliteMediaCacheRepo::blob_path(root, "id", mime)
                    .extension()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn playback_cache_path_keeps_bytes_and_concurrent_hits_consistent() {
        let temp = std::env::temp_dir().join(format!(
            "flare-media-playback-test-{}",
            uuid::Uuid::new_v4()
        ));
        assert!(!temp.exists());
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        super::super::schema::init_schema(&pool).await.unwrap();
        let repo = SqliteMediaCacheRepo::create(pool.clone(), temp.clone())
            .await
            .unwrap();
        let original = repo
            .put_bytes("voice", b"owned-cache-bytes", "audio/wav")
            .await
            .unwrap();
        assert!(original.local_path.ends_with(".wav"));
        // Exercise a previously cached blob too; no source upload is moved.
        let raw_path = Path::new(&original.local_path).with_extension("");
        tokio::fs::rename(&original.local_path, &raw_path)
            .await
            .unwrap();
        sqlx::query("UPDATE media_local_cache SET local_path = ? WHERE file_id = 'voice'")
            .bind(raw_path.to_string_lossy().as_ref())
            .execute(&pool)
            .await
            .unwrap();
        let (a, b) = tokio::join!(repo.get_cached("voice"), repo.get_cached("voice"));
        for hit in [a.unwrap().unwrap(), b.unwrap().unwrap()] {
            assert_eq!(hit.local_path, original.local_path);
            assert_eq!(
                tokio::fs::read(hit.local_path).await.unwrap(),
                b"owned-cache-bytes"
            );
        }
        assert!(!raw_path.exists());
        assert_eq!(repo.media_cache_stats().await.unwrap().entry_count, 1);
        pool.close().await;
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }
}
