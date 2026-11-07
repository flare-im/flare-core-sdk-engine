//! Browser media service: gateway HTTP direct upload + file URL APIs.
//!
//! WASM intentionally does **not** enable [`MediaCacheStore`]: browsers already cache HTTP
//! responses, and persisting blobs in IndexedDB duplicates storage without a usable `file://`
//! path. Use [`MediaService::get_file_url`] / [`MediaService::resolve_media_access`] instead.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use super::upload_shared::{
    build_control_headers, build_upload_metadata, build_upload_parts, compute_bytes_fingerprints,
    infer_file_type, random_upload_id, single_put_progress, upload_file_to_uploaded_media,
};
use crate::application::callbacks::{UploadPhase, UploadProgress, UploadProgressCallback};
use crate::domain::DirectUploadTransportKindVo;
use crate::infrastructure::transport::{
    CommitDirectUploadPartsHttpRequest, CommitDirectUploadPartsHttpResponse,
    CompleteDirectUploadHttpRequest, DeleteFileHttpRequest, DeleteFileHttpResponse,
    DirectUploadTransportKindHttp, GetDirectUploadStatusHttpResponse, GetFileUrlHttpRequest,
    GetFileUrlHttpResponse, HttpApiResponse, HttpClient, InitiateDirectUploadHttpRequest,
    InitiateDirectUploadHttpResponse, PresignDirectUploadPartsHttpRequest,
    PresignDirectUploadPartsHttpResponse, UploadFileHttpResponse, UploadedPartInfoHttp,
    unwrap_api_response,
};
use crate::model::{MediaAccessUrl, MediaResolvedAccess, UploadOptions, UploadedMedia};
use crate::platform::ports::media::{
    MediaMetadata, MediaProcessorPort, MediaServicePort, MediaSourceDescriptor, MediaUploaderPort,
    ProcessedMedia, UploadProgressSink,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Clone)]
pub struct MediaService {
    http: HttpClient,
    current_user_id: Arc<RwLock<String>>,
}

impl MediaService {
    pub fn new(
        http: HttpClient,
        current_user_id: Arc<RwLock<String>>,
        _upload_manifest_store: Option<Arc<dyn crate::domain::UploadManifestStore>>,
        _media_cache_store: Option<Arc<dyn crate::domain::MediaCacheStore>>,
        _media_cache_admin: Option<Arc<dyn crate::domain::MediaCacheAdmin>>,
        _user_file_download_store: Option<Arc<dyn crate::domain::UserFileDownloadStore>>,
    ) -> Self {
        Self {
            http,
            current_user_id,
        }
    }

    pub async fn delete_file(&self, file_id: &str, hard_delete: bool) -> Result<bool> {
        let req = DeleteFileHttpRequest {
            file_id: file_id.to_string(),
            hard_delete,
        };
        let body: HttpApiResponse<DeleteFileHttpResponse> = self
            .http
            .delete_with_body("/api/v1/medias/file", &req)
            .await?;
        let data = unwrap_api_response(body, "delete file")?;
        Ok(data.success)
    }

    pub async fn get_file_url(&self, file_id: &str, expires_in: i32) -> Result<MediaAccessUrl> {
        self.file_url(file_id, expires_in, false, None).await
    }

    /// 附件直链（`download: true`：服务端可带 `Content-Disposition: attachment`），web 端「另存为」用。
    pub async fn get_temp_url_for_file_download(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaAccessUrl> {
        let fid = file_id.trim();
        if fid.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "get_temp_url_for_file_download: empty file_id",
            ));
        }
        self.file_url(fid, expires_in, true, None).await
    }

    async fn file_url(
        &self,
        file_id: &str,
        expires_in: i32,
        download: bool,
        reference: Option<&crate::platform::ports::media::MessageMediaReference>,
    ) -> Result<MediaAccessUrl> {
        let req = GetFileUrlHttpRequest {
            file_id: file_id.to_string(),
            expires_in,
            download,
            response_headers: HashMap::new(),
            conversation_id: reference
                .map(|r| r.conversation_id())
                .unwrap_or_default()
                .to_owned(),
            message_seq: reference.map(|r| r.message_seq()).unwrap_or_default(),
        };
        let body: HttpApiResponse<GetFileUrlHttpResponse> =
            self.http.post("/api/v1/medias/file-url", &req).await?;
        let data = unwrap_api_response(body, "get file url")?;
        Ok(MediaAccessUrl {
            url: data.url,
            cdn_url: data.cdn_url,
        })
    }

    pub async fn resolve_media_access(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaResolvedAccess> {
        let remote = self.get_file_url(file_id, expires_in).await?;
        Ok(MediaResolvedAccess {
            source: "remote".to_string(),
            local_path: None,
            remote: Some(remote),
        })
    }

    async fn upload_bytes_direct(
        &self,
        bytes: &[u8],
        file_name: String,
        mime_type: String,
        options: UploadOptions,
        on_progress: Option<&UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let user_id = self.current_user_id.read().await.clone();
        let size = i64::try_from(bytes.len())
            .map_err(|_| FlareError::general_error("upload payload too large"))?;
        let file_type = infer_file_type(&mime_type);
        let (file_fingerprint, head_tail_sha256, full_sha256) = compute_bytes_fingerprints(bytes);
        let local_upload_id = random_upload_id("web");
        let _source_locator = format!("wasm://{local_upload_id}");

        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.clone(),
                upload_id: local_upload_id.clone(),
                phase: UploadPhase::Preparing,
                uploaded_bytes: 0,
                total_bytes: bytes.len() as u64,
                chunk_index: None,
                total_chunks: None,
            },
        );

        let headers = build_control_headers(&local_upload_id);
        let req = InitiateDirectUploadHttpRequest {
            metadata: build_upload_metadata(
                file_name.clone(),
                mime_type.clone(),
                size,
                file_type,
                local_upload_id.clone(),
                user_id,
            ),
            desired_part_size: i64::try_from(options.chunk_size)
                .map_err(|_| FlareError::general_error("invalid part size"))?,
            file_fingerprint,
            head_tail_sha256,
            full_sha256: full_sha256.unwrap_or_default(),
        };
        let body: HttpApiResponse<InitiateDirectUploadHttpResponse> = self
            .http
            .post_with_headers("/api/v1/medias/uploads/initiate", &req, &headers)
            .await?;
        let init = unwrap_api_response(body, "initiate direct upload")?;
        if !init.success {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                init.error_message
                    .unwrap_or_else(|| "initiate direct upload failed".to_string()),
            ));
        }

        let upload_id = init.upload_id.clone();
        let transport = match init.transport_kind {
            DirectUploadTransportKindHttp::SinglePut => DirectUploadTransportKindVo::SinglePut,
            DirectUploadTransportKindHttp::MultipartPut => {
                DirectUploadTransportKindVo::MultipartPut
            }
        };

        match transport {
            DirectUploadTransportKindVo::SinglePut => {
                let upload_url = init.upload_url.ok_or_else(|| {
                    FlareError::localized(ErrorCode::GeneralError, "single put upload_url missing")
                })?;
                emit_progress(
                    on_progress,
                    UploadProgress {
                        file_name: file_name.clone(),
                        upload_id: upload_id.clone(),
                        phase: UploadPhase::Uploading,
                        uploaded_bytes: 0,
                        total_bytes: bytes.len() as u64,
                        chunk_index: Some(0),
                        total_chunks: Some(1),
                    },
                );
                let mut put_headers = HashMap::new();
                put_headers.insert("Content-Type".to_string(), mime_type.clone());
                let on_sent =
                    single_put_progress(on_progress, &file_name, &upload_id, bytes.len() as u64);
                self.http
                    .put_bytes_full_url_with_progress(&upload_url, bytes, &put_headers, on_sent)
                    .await?;
            }
            DirectUploadTransportKindVo::MultipartPut => {
                upload_multipart_bytes(
                    &self.http,
                    bytes,
                    &upload_id,
                    &file_name,
                    init.part_size.max(1) as u64,
                    init.total_parts.max(1),
                    on_progress,
                )
                .await?;
            }
        }

        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.clone(),
                upload_id: upload_id.clone(),
                phase: UploadPhase::Completing,
                uploaded_bytes: bytes.len() as u64,
                total_bytes: bytes.len() as u64,
                chunk_index: None,
                total_chunks: Some(init.total_parts.max(1)),
            },
        );

        let complete_body: HttpApiResponse<UploadFileHttpResponse> = self
            .http
            .post_with_headers(
                "/api/v1/medias/uploads/complete",
                &CompleteDirectUploadHttpRequest {
                    upload_id: upload_id.clone(),
                },
                &build_control_headers(&upload_id),
            )
            .await?;
        let data = unwrap_api_response(complete_body, "complete direct upload")?;
        if !data.success {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                data.error_message
                    .unwrap_or_else(|| "complete direct upload failed".to_string()),
            ));
        }

        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.clone(),
                upload_id,
                phase: UploadPhase::Finished,
                uploaded_bytes: bytes.len() as u64,
                total_bytes: bytes.len() as u64,
                chunk_index: None,
                total_chunks: Some(init.total_parts.max(1)),
            },
        );

        Ok(upload_file_to_uploaded_media(
            data, file_name, mime_type, size,
        ))
    }
}

async fn upload_multipart_bytes(
    http: &HttpClient,
    bytes: &[u8],
    upload_id: &str,
    file_name: &str,
    part_size: u64,
    total_parts: u32,
    on_progress: Option<&UploadProgressCallback>,
) -> Result<()> {
    let headers = build_control_headers(upload_id);
    let status_body: HttpApiResponse<GetDirectUploadStatusHttpResponse> = http
        .get_with_headers(
            "/api/v1/medias/uploads/status",
            Some(&HashMap::from([(
                "upload_id".to_string(),
                upload_id.to_string(),
            )])),
            &headers,
        )
        .await?;
    let status = unwrap_api_response(status_body, "get direct upload status")?;
    let mut parts = build_upload_parts(bytes.len() as u64, part_size, total_parts, upload_id);
    for server_part in status.uploaded_parts {
        if let Some(part) = parts
            .iter_mut()
            .find(|part| part.part_number == server_part.part_number)
        {
            part.uploaded = true;
            part.etag = Some(server_part.etag);
        }
    }

    let missing: Vec<u32> = parts
        .iter()
        .filter(|part| !part.uploaded)
        .map(|part| part.part_number)
        .collect();
    if missing.is_empty() {
        return Ok(());
    }

    let presign_body: HttpApiResponse<PresignDirectUploadPartsHttpResponse> = http
        .post_with_headers(
            "/api/v1/medias/uploads/presign-parts",
            &PresignDirectUploadPartsHttpRequest {
                upload_id: upload_id.to_string(),
                part_numbers: missing.clone(),
                expires_in: 3600,
            },
            &headers,
        )
        .await?;
    let presigned = unwrap_api_response(presign_body, "presign direct upload parts")?;
    let presigned_map = presigned
        .parts
        .into_iter()
        .map(|part| (part.part_number, part))
        .collect::<HashMap<_, _>>();

    let mut uploaded_bytes = parts
        .iter()
        .filter(|part| part.uploaded)
        .map(|part| part.size)
        .sum::<u64>();

    for part in parts.iter_mut().filter(|part| !part.uploaded) {
        let presigned_part = presigned_map
            .get(&part.part_number)
            .ok_or_else(|| FlareError::general_error("missing presigned url for upload part"))?;
        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.to_string(),
                upload_id: upload_id.to_string(),
                phase: UploadPhase::Uploading,
                uploaded_bytes,
                total_bytes: bytes.len() as u64,
                chunk_index: Some(part.part_number - 1),
                total_chunks: Some(total_parts),
            },
        );
        let end = (part.offset + part.size).min(bytes.len() as u64) as usize;
        let start = part.offset as usize;
        let data = bytes
            .get(start..end)
            .ok_or_else(|| FlareError::general_error("upload part slice out of bounds"))?;
        part.sha256 = hex::encode(Sha256::digest(data));
        let headers_map = http
            .put_bytes_full_url(&presigned_part.upload_url, data, &presigned_part.headers)
            .await?;
        let etag = headers_map
            .get("etag")
            .cloned()
            .or_else(|| headers_map.get("ETag").cloned())
            .ok_or_else(|| FlareError::general_error("object storage response missing ETag"))?;
        let commit_body: HttpApiResponse<CommitDirectUploadPartsHttpResponse> = http
            .post_with_headers(
                "/api/v1/medias/uploads/commit-parts",
                &CommitDirectUploadPartsHttpRequest {
                    upload_id: upload_id.to_string(),
                    parts: vec![UploadedPartInfoHttp {
                        part_number: part.part_number,
                        etag: etag.clone(),
                        size: part.size as i64,
                        sha256: Some(part.sha256.clone()),
                    }],
                },
                &headers,
            )
            .await?;
        let _ = unwrap_api_response(commit_body, "commit direct upload part")?;
        part.uploaded = true;
        part.etag = Some(etag);
        uploaded_bytes = uploaded_bytes.saturating_add(part.size);
    }
    Ok(())
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MediaProcessorPort for MediaService {
    async fn inspect(&self, source: &MediaSourceDescriptor) -> Result<MediaMetadata> {
        if let Some(metadata) = &source.metadata {
            return Ok(metadata.clone());
        }
        Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "web media source requires metadata",
        ))
    }

    async fn prepare_upload(
        &self,
        source: MediaSourceDescriptor,
        _options: Option<UploadOptions>,
    ) -> Result<ProcessedMedia> {
        let metadata = self.inspect(&source).await?;
        Ok(ProcessedMedia {
            source,
            metadata,
            payload: None,
        })
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MediaUploaderPort for MediaService {
    async fn upload(
        &self,
        media: ProcessedMedia,
        options: Option<UploadOptions>,
        progress: Option<UploadProgressSink>,
    ) -> Result<UploadedMedia> {
        let bytes = media.payload.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                "web media upload requires in-memory payload bytes",
            )
        })?;
        let options = options.unwrap_or_default();
        let progress_cb: Option<UploadProgressCallback> =
            progress.map(|sink| Arc::new(sink) as UploadProgressCallback);
        self.upload_bytes_direct(
            &bytes,
            media.metadata.file_name,
            media.metadata.mime_type,
            options,
            progress_cb.as_ref(),
        )
        .await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MediaServicePort for MediaService {
    async fn upload(
        &self,
        media: ProcessedMedia,
        options: Option<UploadOptions>,
        progress: Option<UploadProgressSink>,
    ) -> Result<UploadedMedia> {
        <Self as MediaUploaderPort>::upload(self, media, options, progress).await
    }

    async fn delete_file(&self, file_id: &str, hard_delete: bool) -> Result<bool> {
        MediaService::delete_file(self, file_id, hard_delete).await
    }

    async fn get_file_url(&self, file_id: &str, expires_in: i32) -> Result<MediaAccessUrl> {
        MediaService::get_file_url(self, file_id, expires_in).await
    }

    async fn get_file_url_with_reference(
        &self,
        file_id: &str,
        expires_in: i32,
        reference: Option<&crate::platform::ports::media::MessageMediaReference>,
    ) -> Result<MediaAccessUrl> {
        self.file_url(file_id, expires_in, false, reference).await
    }

    async fn get_temp_url_for_file_download(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaAccessUrl> {
        MediaService::get_temp_url_for_file_download(self, file_id, expires_in).await
    }

    async fn resolve_media_access(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaResolvedAccess> {
        MediaService::resolve_media_access(self, file_id, expires_in).await
    }

    async fn cache_remote_media(
        &self,
        _file_id: &str,
        _expires_in: i32,
    ) -> Result<crate::domain::MediaCacheEntryVo> {
        Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "media cache is disabled on web/wasm; use resolve_media_access with gateway URLs",
        ))
    }

    async fn media_cache_stats(&self) -> Result<crate::domain::MediaCacheStatsVo> {
        Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "media cache is disabled on web/wasm",
        ))
    }

    async fn set_media_cache_max_bytes(&self, _max_bytes: u64) -> Result<()> {
        Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "media cache is disabled on web/wasm",
        ))
    }

    async fn set_media_cache_root(&self, _absolute_path: Option<&str>) -> Result<()> {
        Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "media cache is disabled on web/wasm",
        ))
    }

    async fn clear_media_cache(&self) -> Result<()> {
        Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "media cache is disabled on web/wasm",
        ))
    }
}

fn emit_progress(on_progress: Option<&UploadProgressCallback>, progress: UploadProgress) {
    if let Some(cb) = on_progress {
        cb(progress);
    }
}
