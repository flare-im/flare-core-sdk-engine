use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use flare_im_core_sdk::storage::{
    MediaUploadManifestVo, MediaUploadPartVo, UploadManifestState, UploadManifestStore,
    UploadSourceKind,
};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};

pub struct SqliteUploadManifestRepo {
    pool: SqlitePool,
}

impl SqliteUploadManifestRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn source_kind_to_str(value: &UploadSourceKind) -> &'static str {
    match value {
        UploadSourceKind::StableFile => "stable_file",
        UploadSourceKind::SpoolFile => "spool_file",
    }
}

fn state_to_str(value: &UploadManifestState) -> &'static str {
    match value {
        UploadManifestState::Initiating => "initiating",
        UploadManifestState::Uploading => "uploading",
        UploadManifestState::Completing => "completing",
        UploadManifestState::Completed => "completed",
        UploadManifestState::Failed => "failed",
        UploadManifestState::Aborted => "aborted",
    }
}

fn parse_source_kind(value: &str) -> Result<UploadSourceKind> {
    match value {
        "stable_file" => Ok(UploadSourceKind::StableFile),
        "spool_file" => Ok(UploadSourceKind::SpoolFile),
        other => Err(FlareError::localized(
            ErrorCode::DatabaseError,
            format!("unknown upload source kind: {other}"),
        )),
    }
}

fn parse_state(value: &str) -> Result<UploadManifestState> {
    match value {
        "initiating" => Ok(UploadManifestState::Initiating),
        "uploading" => Ok(UploadManifestState::Uploading),
        "completing" => Ok(UploadManifestState::Completing),
        "completed" => Ok(UploadManifestState::Completed),
        "failed" => Ok(UploadManifestState::Failed),
        "aborted" => Ok(UploadManifestState::Aborted),
        other => Err(FlareError::localized(
            ErrorCode::DatabaseError,
            format!("unknown upload manifest state: {other}"),
        )),
    }
}

fn row_to_manifest(row: &sqlx::sqlite::SqliteRow) -> Result<MediaUploadManifestVo> {
    Ok(MediaUploadManifestVo {
        local_upload_id: row.get("local_upload_id"),
        remote_upload_id: row.get("remote_upload_id"),
        file_id: row.get("file_id"),
        storage_upload_id: row.get("storage_upload_id"),
        tenant_id: row.get("tenant_id"),
        user_id: row.get("user_id"),
        source_kind: parse_source_kind(row.get::<String, _>("source_kind").as_str())?,
        source_locator: row.get("source_locator"),
        file_name: row.get("file_name"),
        mime_type: row.get("mime_type"),
        file_size: row.get::<i64, _>("file_size") as u64,
        part_size: row.get::<i64, _>("part_size") as u32,
        total_parts: row.get::<i64, _>("total_parts") as u32,
        transport_kind: row.get::<Option<String>, _>("transport_kind").map(|value| {
            match value.as_str() {
                "single_put" => flare_im_core_sdk::storage::DirectUploadTransportKindVo::SinglePut,
                _ => flare_im_core_sdk::storage::DirectUploadTransportKindVo::MultipartPut,
            }
        }),
        bucket: row.get("bucket"),
        object_key: row.get("object_key"),
        upload_url: row.get("upload_url"),
        file_fingerprint: row.get("file_fingerprint"),
        head_tail_sha256: row.get("head_tail_sha256"),
        full_sha256: row.get("full_sha256"),
        state: parse_state(row.get::<String, _>("upload_state").as_str())?,
        last_error_code: row.get("last_error_code"),
        last_error_message: row.get("last_error_message"),
        expires_at_ms: row.get("expires_at_ms"),
        created_at_ms: row.get("created_at_ms"),
        updated_at_ms: row.get("updated_at_ms"),
    })
}

fn row_to_part(row: &sqlx::sqlite::SqliteRow) -> MediaUploadPartVo {
    MediaUploadPartVo {
        local_upload_id: row.get("local_upload_id"),
        part_number: row.get::<i64, _>("part_number") as u32,
        offset: row.get::<i64, _>("offset_bytes") as u64,
        size: row.get::<i64, _>("size_bytes") as u64,
        sha256: row.get("sha256"),
        etag: row.get("etag"),
        uploaded: row.get::<i64, _>("uploaded") != 0,
    }
}

#[async_trait]
impl UploadManifestStore for SqliteUploadManifestRepo {
    async fn get_manifest(&self, local_upload_id: &str) -> Result<Option<MediaUploadManifestVo>> {
        let row = sqlx::query("SELECT * FROM media_upload_manifest WHERE local_upload_id = ?")
            .bind(local_upload_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        row.map(|row| row_to_manifest(&row)).transpose()
    }

    async fn find_active_manifest(
        &self,
        source_locator: &str,
        file_fingerprint: &str,
    ) -> Result<Option<MediaUploadManifestVo>> {
        let row = sqlx::query(
            r#"SELECT * FROM media_upload_manifest
               WHERE source_locator = ?
                 AND file_fingerprint = ?
                 AND upload_state IN ('initiating', 'uploading', 'completing')
               ORDER BY updated_at_ms DESC
               LIMIT 1"#,
        )
        .bind(source_locator)
        .bind(file_fingerprint)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        row.map(|row| row_to_manifest(&row)).transpose()
    }

    async fn upsert_manifest(&self, manifest: &MediaUploadManifestVo) -> Result<()> {
        sqlx::query(
            r#"INSERT OR REPLACE INTO media_upload_manifest (
                local_upload_id, remote_upload_id, file_id, storage_upload_id, tenant_id, user_id,
                source_kind, source_locator, file_name, mime_type, file_size, part_size, total_parts,
                transport_kind, bucket, object_key, upload_url, file_fingerprint, head_tail_sha256,
                full_sha256, upload_state, last_error_code, last_error_message, expires_at_ms,
                created_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&manifest.local_upload_id)
        .bind(&manifest.remote_upload_id)
        .bind(&manifest.file_id)
        .bind(&manifest.storage_upload_id)
        .bind(&manifest.tenant_id)
        .bind(&manifest.user_id)
        .bind(source_kind_to_str(&manifest.source_kind))
        .bind(&manifest.source_locator)
        .bind(&manifest.file_name)
        .bind(&manifest.mime_type)
        .bind(manifest.file_size as i64)
        .bind(manifest.part_size as i64)
        .bind(manifest.total_parts as i64)
        .bind(manifest.transport_kind.as_ref().map(|kind| match kind {
            flare_im_core_sdk::storage::DirectUploadTransportKindVo::SinglePut => "single_put",
            flare_im_core_sdk::storage::DirectUploadTransportKindVo::MultipartPut => "multipart_put",
        }))
        .bind(&manifest.bucket)
        .bind(&manifest.object_key)
        .bind(&manifest.upload_url)
        .bind(&manifest.file_fingerprint)
        .bind(&manifest.head_tail_sha256)
        .bind(&manifest.full_sha256)
        .bind(state_to_str(&manifest.state))
        .bind(&manifest.last_error_code)
        .bind(&manifest.last_error_message)
        .bind(manifest.expires_at_ms)
        .bind(manifest.created_at_ms)
        .bind(manifest.updated_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn delete_manifest(&self, local_upload_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM media_upload_manifest WHERE local_upload_id = ?")
            .bind(local_upload_id)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        self.delete_parts(local_upload_id).await
    }

    async fn list_parts(&self, local_upload_id: &str) -> Result<Vec<MediaUploadPartVo>> {
        let rows = sqlx::query(
            r#"SELECT * FROM media_upload_part
               WHERE local_upload_id = ?
               ORDER BY part_number ASC"#,
        )
        .bind(local_upload_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(rows.into_iter().map(|row| row_to_part(&row)).collect())
    }

    async fn replace_parts(
        &self,
        local_upload_id: &str,
        parts: &[MediaUploadPartVo],
    ) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        sqlx::query("DELETE FROM media_upload_part WHERE local_upload_id = ?")
            .bind(local_upload_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        for part in parts {
            sqlx::query(
                r#"INSERT OR REPLACE INTO media_upload_part (
                    local_upload_id, part_number, offset_bytes, size_bytes, sha256, etag, uploaded
                ) VALUES (?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(&part.local_upload_id)
            .bind(part.part_number as i64)
            .bind(part.offset as i64)
            .bind(part.size as i64)
            .bind(&part.sha256)
            .bind(&part.etag)
            .bind(if part.uploaded { 1_i64 } else { 0_i64 })
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn upsert_part(&self, part: &MediaUploadPartVo) -> Result<()> {
        sqlx::query(
            r#"INSERT OR REPLACE INTO media_upload_part (
                local_upload_id, part_number, offset_bytes, size_bytes, sha256, etag, uploaded
            ) VALUES (?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&part.local_upload_id)
        .bind(part.part_number as i64)
        .bind(part.offset as i64)
        .bind(part.size as i64)
        .bind(&part.sha256)
        .bind(&part.etag)
        .bind(if part.uploaded { 1_i64 } else { 0_i64 })
        .execute(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }

    async fn delete_parts(&self, local_upload_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM media_upload_part WHERE local_upload_id = ?")
            .bind(local_upload_id)
            .execute(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }
}
