//! 媒体上传与下载：直传分片、网关取链、本地缓存、附件下载到用户目录并落库。

use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashSet;
use std::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use super::native_download::{
    AUTO_CACHE_MAX_ENTRY_BYTES, WrittenFile, copy_file_to, ensure_writable_dir, extension_for_mime,
    fill_cache_from_url, platform_default_download_root, read_head, stream_http_to_file,
    temp_sibling,
};
use super::upload_shared::{
    build_control_headers as shared_build_control_headers, build_upload_metadata,
    build_upload_parts, build_upload_parts_from_manifest, compute_bytes_fingerprints,
    infer_file_type, random_upload_id, single_put_progress, upload_file_to_uploaded_media,
};
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::RwLock;

use crate::application::callbacks::{
    UploadPhase, UploadProgress, UploadProgressCallback, UserFileDownloadRequest,
};
use crate::domain::{
    DirectUploadTransportKindVo, MediaCacheAdmin, MediaCacheEntryVo, MediaCacheStore,
    MediaUploadManifestVo, UploadManifestState, UploadManifestStore, UploadSourceKind,
    UserDownloadDirectoryVo, UserFileDownloadResultVo, UserFileDownloadStore,
};
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
    MediaMetadata, MediaProcessorPort, MediaServicePort, MediaSourceDescriptor, MediaSourceKind,
    MediaUploaderPort, ProcessedMedia, UploadProgressSink,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

const MAX_CONCURRENT_DIRECT_UPLOAD_PARTS: usize = 4;

#[derive(Clone)]
pub struct MediaService {
    http: HttpClient,
    current_user_id: Arc<RwLock<String>>,
    upload_manifest_store: Option<Arc<dyn UploadManifestStore>>,
    media_cache_store: Option<Arc<dyn MediaCacheStore>>,
    media_cache_admin: Option<Arc<dyn MediaCacheAdmin>>,
    user_file_download_store: Option<Arc<dyn UserFileDownloadStore>>,
    /// 与 `download_key` 对应；`false` 表示取消下载。
    download_cancel_flags: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    /// 正在后台自动缓存的 `file_id`（同一张图同时只拉一次）。
    #[cfg(not(target_arch = "wasm32"))]
    auto_cache_inflight: Arc<Mutex<HashSet<String>>>,
}

struct NewUploadManifest<'a> {
    user_id: &'a str,
    source_locator: &'a str,
    file_name: &'a str,
    mime_type: &'a str,
    file_size: u64,
    file_fingerprint: &'a str,
    head_tail_sha256: &'a str,
    full_sha256: Option<String>,
}

struct UploadedDirectPart {
    part_number: u32,
    size: u64,
    sha256: String,
    etag: String,
}

#[cfg(not(target_arch = "wasm32"))]
const MAX_USER_DOWNLOAD_BYTES: u64 = 2 * 1024 * 1024 * 1024;

impl MediaService {
    pub fn new(
        http: HttpClient,
        current_user_id: Arc<RwLock<String>>,
        upload_manifest_store: Option<Arc<dyn UploadManifestStore>>,
        media_cache_store: Option<Arc<dyn MediaCacheStore>>,
        media_cache_admin: Option<Arc<dyn MediaCacheAdmin>>,
        user_file_download_store: Option<Arc<dyn UserFileDownloadStore>>,
    ) -> Self {
        Self {
            http,
            current_user_id,
            upload_manifest_store,
            media_cache_store,
            media_cache_admin,
            user_file_download_store,
            download_cancel_flags: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(not(target_arch = "wasm32"))]
            auto_cache_inflight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn http(&self) -> &HttpClient {
        &self.http
    }

    pub async fn upload_file_from_path_with_progress(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let path = path.as_ref();
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| FlareError::localized(ErrorCode::InvalidParameter, "invalid file name"))?
            .to_string();
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| FlareError::general_error(format!("read file metadata failed: {e}")))?;
        let size = i64::try_from(metadata.len())
            .map_err(|_| FlareError::general_error("file too large"))?;
        let mime = infer_mime_type(&file_name);
        let options = options.unwrap_or_default();
        let uploaded = self
            .upload_via_direct_session(path, file_name, mime, size, options, on_progress.as_ref())
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        self.cache_uploaded_file(&uploaded, path, metadata.len())
            .await;
        Ok(uploaded)
    }

    pub async fn upload_image_from_path_with_progress(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        validate_path_mime_prefix(path.as_ref(), "image/")?;
        let media = self
            .upload_file_from_path_with_progress(path, options, on_progress)
            .await?;
        Ok(media)
    }

    pub async fn upload_video_from_path_with_progress(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        validate_path_mime_prefix(path.as_ref(), "video/")?;
        let media = self
            .upload_file_from_path_with_progress(path, options, on_progress)
            .await?;
        Ok(media)
    }

    pub async fn upload_bytes_with_progress(
        &self,
        bytes: Vec<u8>,
        file_name: String,
        mime_type: String,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let options = options.unwrap_or_default();
        let uploaded = self
            .upload_bytes_direct(&bytes, file_name, mime_type, options, on_progress.as_ref())
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        self.cache_uploaded_bytes(&uploaded, &bytes).await;
        Ok(uploaded)
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
        self.get_access_url(file_id, expires_in, false, None, None)
            .await
    }

    /// 向网关申请短时直链，`download: true` 时服务端可返回 `Content-Disposition: attachment` 等（附件下载场景）。
    pub async fn get_temp_url_for_file_download(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaAccessUrl> {
        self.get_message_download_url(file_id, expires_in, None, None)
            .await
    }

    async fn get_message_download_url(
        &self,
        file_id: &str,
        expires_in: i32,
        conversation_id: Option<&str>,
        message_seq: Option<i64>,
    ) -> Result<MediaAccessUrl> {
        self.get_access_url(file_id, expires_in, true, conversation_id, message_seq)
            .await
    }

    async fn get_access_url(
        &self,
        file_id: &str,
        expires_in: i32,
        download: bool,
        conversation_id: Option<&str>,
        message_seq: Option<i64>,
    ) -> Result<MediaAccessUrl> {
        let fid = file_id.trim();
        if fid.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "get_temp_url_for_file_download: empty file_id",
            ));
        }
        let req = GetFileUrlHttpRequest {
            file_id: fid.to_string(),
            expires_in,
            download,
            response_headers: HashMap::new(),
            conversation_id: conversation_id.unwrap_or_default().to_owned(),
            message_seq: message_seq.unwrap_or_default(),
        };
        let body: HttpApiResponse<GetFileUrlHttpResponse> =
            self.http.post("/api/v1/medias/file-url", &req).await?;
        let data = unwrap_api_response(body, "get file url (download)")?;
        Ok(MediaAccessUrl {
            url: data.url,
            cdn_url: data.cdn_url,
        })
    }

    /// 解析媒体访问方式：**优先** SQLite 对照表中且仍存在的本地文件，否则向网关请求短时 URL。
    pub async fn resolve_media_access(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaResolvedAccess> {
        let fid = file_id.trim();
        if fid.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "resolve_media_access: empty file_id",
            ));
        }

        if let Some(cache) = &self.media_cache_store
            && let Some(entry) = cache.get_cached(fid).await?
        {
            return Ok(MediaResolvedAccess {
                source: "local".to_string(),
                local_path: Some(entry.local_path),
                remote: None,
            });
        }

        let remote = self.get_file_url(fid, expires_in).await?;
        Ok(MediaResolvedAccess {
            source: "remote".to_string(),
            local_path: None,
            remote: Some(remote),
        })
    }

    /// 从网关取直链并下载落盘，写入 `media_local_cache` 对照表（供「点击后缓存」等场景）。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn cache_remote_media(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaCacheEntryVo> {
        let cache = self.media_cache_store.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "media cache store is not configured",
            )
        })?;

        let fid = file_id.trim();
        if fid.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "cache_remote_media: empty file_id",
            ));
        }

        if let Some(hit) = cache.get_cached(fid).await? {
            return Ok(hit);
        }

        let access = self.get_file_url(fid, expires_in).await?;
        let url = pick_download_url(&access);
        if url.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                "cache_remote_media: empty download url",
            ));
        }

        fill_cache_from_url(
            &self.http,
            cache,
            fid,
            url,
            MAX_USER_DOWNLOAD_BYTES,
            media_mime_of,
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn cache_remote_media(
        &self,
        _file_id: &str,
        _expires_in: i32,
    ) -> Result<MediaCacheEntryVo> {
        Err(FlareError::system(
            "cache_remote_media is not supported on wasm",
        ))
    }

    pub async fn media_cache_stats(&self) -> Result<crate::domain::MediaCacheStatsVo> {
        let admin = self.media_cache_admin.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "media cache is not configured",
            )
        })?;
        admin.media_cache_stats().await
    }

    pub async fn set_media_cache_max_bytes(&self, max_bytes: u64) -> Result<()> {
        let admin = self.media_cache_admin.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "media cache is not configured",
            )
        })?;
        admin.set_media_cache_max_bytes(max_bytes).await
    }

    pub async fn set_media_cache_root(&self, absolute_path: Option<&str>) -> Result<()> {
        let admin = self.media_cache_admin.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "media cache is not configured",
            )
        })?;
        admin.set_media_cache_root(absolute_path).await
    }

    pub async fn clear_media_cache(&self) -> Result<()> {
        let admin = self.media_cache_admin.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "media cache is not configured",
            )
        })?;
        admin.clear_media_cache().await
    }

    async fn upload_via_direct_session(
        &self,
        path: &Path,
        file_name: String,
        mime_type: String,
        size: i64,
        options: UploadOptions,
        on_progress: Option<&UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let user_id = self.current_user_id.read().await.clone();
        let file_type = infer_file_type(&mime_type);
        let source_locator = path.to_string_lossy().to_string();
        let (file_fingerprint, head_tail_sha256, full_sha256) =
            compute_file_fingerprints(path).await?;

        let mut manifest = if let Some(store) = &self.upload_manifest_store {
            if let Some(existing) = store
                .find_active_manifest(&source_locator, &file_fingerprint)
                .await?
            {
                existing
            } else {
                self.new_manifest(NewUploadManifest {
                    user_id: &user_id,
                    source_locator: &source_locator,
                    file_name: &file_name,
                    mime_type: &mime_type,
                    file_size: size as u64,
                    file_fingerprint: &file_fingerprint,
                    head_tail_sha256: &head_tail_sha256,
                    full_sha256: full_sha256.clone(),
                })
            }
        } else {
            self.new_manifest(NewUploadManifest {
                user_id: &user_id,
                source_locator: &source_locator,
                file_name: &file_name,
                mime_type: &mime_type,
                file_size: size as u64,
                file_fingerprint: &file_fingerprint,
                head_tail_sha256: &head_tail_sha256,
                full_sha256: full_sha256.clone(),
            })
        };

        if manifest.remote_upload_id.is_none() {
            emit_progress(
                on_progress,
                UploadProgress {
                    file_name: file_name.clone(),
                    upload_id: manifest.local_upload_id.clone(),
                    phase: UploadPhase::Preparing,
                    uploaded_bytes: 0,
                    total_bytes: size as u64,
                    chunk_index: None,
                    total_chunks: None,
                },
            );
            let headers = self.build_control_headers(&manifest.local_upload_id);
            let req = InitiateDirectUploadHttpRequest {
                metadata: build_upload_metadata(
                    file_name.clone(),
                    mime_type.clone(),
                    size,
                    file_type,
                    manifest.local_upload_id.clone(),
                    user_id.clone(),
                ),
                desired_part_size: i64::try_from(options.chunk_size)
                    .map_err(|_| FlareError::general_error("invalid part size"))?,
                file_fingerprint: file_fingerprint.clone(),
                head_tail_sha256: head_tail_sha256.clone(),
                full_sha256: full_sha256.clone().unwrap_or_default(),
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
            manifest.remote_upload_id = Some(init.upload_id.clone());
            manifest.file_id = Some(init.file_id.clone());
            manifest.storage_upload_id = init.storage_upload_id.clone();
            manifest.transport_kind = Some(match init.transport_kind {
                DirectUploadTransportKindHttp::SinglePut => DirectUploadTransportKindVo::SinglePut,
                DirectUploadTransportKindHttp::MultipartPut => {
                    DirectUploadTransportKindVo::MultipartPut
                }
            });
            manifest.bucket = Some(init.bucket.clone());
            manifest.object_key = Some(init.object_key.clone());
            manifest.upload_url = init.upload_url.clone();
            manifest.part_size = u32::try_from(init.part_size.max(1)).unwrap_or(u32::MAX);
            manifest.total_parts = init.total_parts.max(1);
            manifest.state = UploadManifestState::Uploading;
            manifest.updated_at_ms = now_ms();
            if let Some(store) = &self.upload_manifest_store {
                store.upsert_manifest(&manifest).await?;
                if manifest.transport_kind == Some(DirectUploadTransportKindVo::MultipartPut) {
                    let parts = build_upload_parts_from_manifest(&manifest);
                    store
                        .replace_parts(&manifest.local_upload_id, &parts)
                        .await?;
                }
            }
        }

        match manifest
            .transport_kind
            .clone()
            .unwrap_or(DirectUploadTransportKindVo::SinglePut)
        {
            DirectUploadTransportKindVo::SinglePut => {
                let upload_url = manifest.upload_url.clone().ok_or_else(|| {
                    FlareError::localized(
                        ErrorCode::GeneralError,
                        "single put upload_url missing in upload manifest",
                    )
                })?;
                emit_progress(
                    on_progress,
                    UploadProgress {
                        file_name: file_name.clone(),
                        upload_id: manifest.remote_upload_id.clone().unwrap_or_default(),
                        phase: UploadPhase::Uploading,
                        uploaded_bytes: 0,
                        total_bytes: size as u64,
                        chunk_index: Some(0),
                        total_chunks: Some(1),
                    },
                );
                let mut put_headers = HashMap::new();
                put_headers.insert("Content-Type".to_string(), mime_type.clone());
                let on_sent = single_put_progress(
                    on_progress,
                    &file_name,
                    &manifest.remote_upload_id.clone().unwrap_or_default(),
                    size as u64,
                );
                let _ = self
                    .http
                    .put_file_full_url_with_progress(
                        &upload_url,
                        path,
                        size as u64,
                        &put_headers,
                        on_sent,
                    )
                    .await?;
                emit_progress(
                    on_progress,
                    UploadProgress {
                        file_name: file_name.clone(),
                        upload_id: manifest.remote_upload_id.clone().unwrap_or_default(),
                        phase: UploadPhase::Completing,
                        uploaded_bytes: size as u64,
                        total_bytes: size as u64,
                        chunk_index: Some(0),
                        total_chunks: Some(1),
                    },
                );
            }
            DirectUploadTransportKindVo::MultipartPut => {
                let upload_id = manifest.remote_upload_id.clone().ok_or_else(|| {
                    FlareError::localized(ErrorCode::GeneralError, "remote_upload_id missing")
                })?;
                let headers = self.build_control_headers(&upload_id);
                let status_body: HttpApiResponse<GetDirectUploadStatusHttpResponse> = self
                    .http
                    .get_with_headers(
                        "/api/v1/medias/uploads/status",
                        Some(&HashMap::from([(
                            "upload_id".to_string(),
                            upload_id.clone(),
                        )])),
                        &headers,
                    )
                    .await?;
                let status = unwrap_api_response(status_body, "get direct upload status")?;

                let mut parts = if let Some(store) = &self.upload_manifest_store {
                    let existing = store.list_parts(&manifest.local_upload_id).await?;
                    if existing.is_empty() {
                        let generated = build_upload_parts_from_manifest(&manifest);
                        store
                            .replace_parts(&manifest.local_upload_id, &generated)
                            .await?;
                        generated
                    } else {
                        existing
                    }
                } else {
                    build_upload_parts_from_manifest(&manifest)
                };

                for server_part in status.uploaded_parts {
                    if let Some(part) = parts
                        .iter_mut()
                        .find(|part| part.part_number == server_part.part_number)
                    {
                        part.uploaded = true;
                        part.etag = Some(server_part.etag);
                    }
                }

                let missing_parts = parts
                    .iter()
                    .filter(|part| !part.uploaded)
                    .map(|part| part.part_number)
                    .collect::<Vec<_>>();

                if !missing_parts.is_empty() {
                    let presign_body: HttpApiResponse<PresignDirectUploadPartsHttpResponse> = self
                        .http
                        .post_with_headers(
                            "/api/v1/medias/uploads/presign-parts",
                            &PresignDirectUploadPartsHttpRequest {
                                upload_id: upload_id.clone(),
                                part_numbers: missing_parts.clone(),
                                expires_in: 3600,
                            },
                            &headers,
                        )
                        .await?;
                    let presigned =
                        unwrap_api_response(presign_body, "presign direct upload parts")?;
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

                    let upload_path = Arc::new(path.to_path_buf());
                    let upload_jobs = parts
                        .iter()
                        .filter(|part| !part.uploaded)
                        .map(|part| {
                            let presigned_part = presigned_map
                                .get(&part.part_number)
                                .cloned()
                                .ok_or_else(|| {
                                    FlareError::general_error(
                                        "missing presigned url for upload part",
                                    )
                                })?;
                            Ok((part.clone(), presigned_part))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut upload_stream = futures_util::stream::iter(
                        upload_jobs.into_iter().map(|(part, presigned_part)| {
                            let http = self.http.clone();
                            let upload_path = Arc::clone(&upload_path);
                            async move {
                                let data =
                                    read_part_bytes(upload_path.as_path(), part.offset, part.size)
                                        .await?;
                                let sha256 = hex::encode(Sha256::digest(&data));
                                let headers_map = http
                                    .put_bytes_full_url(
                                        &presigned_part.upload_url,
                                        &data,
                                        &presigned_part.headers,
                                    )
                                    .await?;
                                let etag = headers_map
                                    .get("etag")
                                    .cloned()
                                    .or_else(|| headers_map.get("ETag").cloned())
                                    .ok_or_else(|| {
                                        FlareError::general_error(
                                            "object storage response missing ETag",
                                        )
                                    })?;
                                Ok::<UploadedDirectPart, FlareError>(UploadedDirectPart {
                                    part_number: part.part_number,
                                    size: part.size,
                                    sha256,
                                    etag,
                                })
                            }
                        }),
                    )
                    .buffer_unordered(MAX_CONCURRENT_DIRECT_UPLOAD_PARTS);

                    let mut uploaded_parts = Vec::new();
                    while let Some(result) = upload_stream.next().await {
                        let uploaded_part = result?;
                        uploaded_bytes = uploaded_bytes.saturating_add(uploaded_part.size);
                        emit_progress(
                            on_progress,
                            UploadProgress {
                                file_name: file_name.clone(),
                                upload_id: upload_id.clone(),
                                phase: UploadPhase::Uploading,
                                uploaded_bytes,
                                total_bytes: size as u64,
                                chunk_index: Some(uploaded_part.part_number - 1),
                                total_chunks: Some(manifest.total_parts),
                            },
                        );
                        uploaded_parts.push(uploaded_part);
                    }

                    if !uploaded_parts.is_empty() {
                        let commit_parts = uploaded_parts
                            .iter()
                            .map(|part| UploadedPartInfoHttp {
                                part_number: part.part_number,
                                etag: part.etag.clone(),
                                size: part.size as i64,
                                sha256: Some(part.sha256.clone()),
                            })
                            .collect::<Vec<_>>();
                        let commit_body: HttpApiResponse<CommitDirectUploadPartsHttpResponse> =
                            self.http
                                .post_with_headers(
                                    "/api/v1/medias/uploads/commit-parts",
                                    &CommitDirectUploadPartsHttpRequest {
                                        upload_id: upload_id.clone(),
                                        parts: commit_parts,
                                    },
                                    &headers,
                                )
                                .await?;
                        let _ = unwrap_api_response(commit_body, "commit direct upload parts")?;

                        let uploaded_map = uploaded_parts
                            .into_iter()
                            .map(|part| (part.part_number, part))
                            .collect::<HashMap<_, _>>();
                        for part in parts.iter_mut().filter(|part| !part.uploaded) {
                            if let Some(uploaded) = uploaded_map.get(&part.part_number) {
                                part.uploaded = true;
                                part.sha256 = uploaded.sha256.clone();
                                part.etag = Some(uploaded.etag.clone());
                            }
                        }
                    }
                }
                if let Some(store) = &self.upload_manifest_store {
                    store
                        .replace_parts(&manifest.local_upload_id, &parts)
                        .await?;
                }
            }
        }

        let upload_id = manifest.remote_upload_id.clone().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::GeneralError,
                "remote_upload_id missing for complete",
            )
        })?;
        let headers = self.build_control_headers(&upload_id);
        let complete_body: HttpApiResponse<UploadFileHttpResponse> = self
            .http
            .post_with_headers(
                "/api/v1/medias/uploads/complete",
                &CompleteDirectUploadHttpRequest {
                    upload_id: upload_id.clone(),
                },
                &headers,
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

        if let Some(store) = &self.upload_manifest_store {
            store.delete_manifest(&manifest.local_upload_id).await?;
        }

        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.clone(),
                upload_id,
                phase: UploadPhase::Finished,
                uploaded_bytes: size as u64,
                total_bytes: size as u64,
                chunk_index: None,
                total_chunks: Some(manifest.total_parts.max(1)),
            },
        );

        Ok(upload_file_to_uploaded_media(
            data, file_name, mime_type, size,
        ))
    }

    async fn upload_bytes_direct(
        &self,
        bytes: &[u8],
        file_name: String,
        mime_type: String,
        options: UploadOptions,
        on_progress: Option<&UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        if file_name.trim().is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "upload_bytes requires file_name",
            ));
        }
        if mime_type.trim().is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "upload_bytes requires mime_type",
            ));
        }
        let user_id = self.current_user_id.read().await.clone();
        let size = i64::try_from(bytes.len())
            .map_err(|_| FlareError::general_error("upload payload too large"))?;
        let file_type = infer_file_type(&mime_type);
        let (file_fingerprint, head_tail_sha256, full_sha256) = compute_bytes_fingerprints(bytes);
        let local_upload_id = random_upload_id("native-bytes");

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

        let headers = self.build_control_headers(&local_upload_id);
        let body: HttpApiResponse<InitiateDirectUploadHttpResponse> = self
            .http
            .post_with_headers(
                "/api/v1/medias/uploads/initiate",
                &InitiateDirectUploadHttpRequest {
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
                },
                &headers,
            )
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
        match init.transport_kind {
            DirectUploadTransportKindHttp::SinglePut => {
                let upload_url = init.upload_url.clone().ok_or_else(|| {
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
            DirectUploadTransportKindHttp::MultipartPut => {
                self.upload_multipart_bytes(
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
                &self.build_control_headers(&upload_id),
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

    async fn upload_multipart_bytes(
        &self,
        bytes: &[u8],
        upload_id: &str,
        file_name: &str,
        part_size: u64,
        total_parts: u32,
        on_progress: Option<&UploadProgressCallback>,
    ) -> Result<()> {
        let headers = self.build_control_headers(upload_id);
        let status_body: HttpApiResponse<GetDirectUploadStatusHttpResponse> = self
            .http
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

        let missing = parts
            .iter()
            .filter(|part| !part.uploaded)
            .map(|part| part.part_number)
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }

        let presign_body: HttpApiResponse<PresignDirectUploadPartsHttpResponse> = self
            .http
            .post_with_headers(
                "/api/v1/medias/uploads/presign-parts",
                &PresignDirectUploadPartsHttpRequest {
                    upload_id: upload_id.to_string(),
                    part_numbers: missing,
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
        let upload_jobs = parts
            .iter()
            .filter(|part| !part.uploaded)
            .map(|part| {
                let presigned_part =
                    presigned_map
                        .get(&part.part_number)
                        .cloned()
                        .ok_or_else(|| {
                            FlareError::general_error("missing presigned url for upload part")
                        })?;
                Ok((part.clone(), presigned_part))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut upload_stream =
            futures_util::stream::iter(upload_jobs.into_iter().map(|(part, presigned_part)| {
                let http = self.http.clone();
                let data = bytes[part.offset as usize..(part.offset + part.size) as usize].to_vec();
                async move {
                    let sha256 = hex::encode(Sha256::digest(&data));
                    let headers_map = http
                        .put_bytes_full_url(
                            &presigned_part.upload_url,
                            &data,
                            &presigned_part.headers,
                        )
                        .await?;
                    let etag = headers_map
                        .get("etag")
                        .cloned()
                        .or_else(|| headers_map.get("ETag").cloned())
                        .ok_or_else(|| {
                            FlareError::general_error("object storage response missing ETag")
                        })?;
                    Ok::<UploadedDirectPart, FlareError>(UploadedDirectPart {
                        part_number: part.part_number,
                        size: part.size,
                        sha256,
                        etag,
                    })
                }
            }))
            .buffer_unordered(MAX_CONCURRENT_DIRECT_UPLOAD_PARTS);

        let mut uploaded_parts = Vec::new();
        while let Some(result) = upload_stream.next().await {
            let uploaded_part = result?;
            uploaded_bytes = uploaded_bytes.saturating_add(uploaded_part.size);
            emit_progress(
                on_progress,
                UploadProgress {
                    file_name: file_name.to_string(),
                    upload_id: upload_id.to_string(),
                    phase: UploadPhase::Uploading,
                    uploaded_bytes,
                    total_bytes: bytes.len() as u64,
                    chunk_index: Some(uploaded_part.part_number - 1),
                    total_chunks: Some(total_parts),
                },
            );
            uploaded_parts.push(uploaded_part);
        }

        if uploaded_parts.is_empty() {
            return Ok(());
        }
        let commit_parts = uploaded_parts
            .iter()
            .map(|part| UploadedPartInfoHttp {
                part_number: part.part_number,
                etag: part.etag.clone(),
                size: part.size as i64,
                sha256: Some(part.sha256.clone()),
            })
            .collect::<Vec<_>>();
        let commit_body: HttpApiResponse<CommitDirectUploadPartsHttpResponse> = self
            .http
            .post_with_headers(
                "/api/v1/medias/uploads/commit-parts",
                &CommitDirectUploadPartsHttpRequest {
                    upload_id: upload_id.to_string(),
                    parts: commit_parts,
                },
                &headers,
            )
            .await?;
        let _ = unwrap_api_response(commit_body, "commit direct upload parts")?;
        Ok(())
    }

    fn build_control_headers(&self, trace_seed: &str) -> HashMap<String, String> {
        shared_build_control_headers(trace_seed)
    }

    fn new_manifest(&self, input: NewUploadManifest<'_>) -> MediaUploadManifestVo {
        let now = now_ms();
        MediaUploadManifestVo {
            local_upload_id: random_upload_id("direct"),
            remote_upload_id: None,
            file_id: None,
            storage_upload_id: None,
            tenant_id: String::new(),
            user_id: input.user_id.to_string(),
            source_kind: UploadSourceKind::StableFile,
            source_locator: input.source_locator.to_string(),
            file_name: input.file_name.to_string(),
            mime_type: input.mime_type.to_string(),
            file_size: input.file_size,
            part_size: 0,
            total_parts: 0,
            transport_kind: None,
            bucket: None,
            object_key: None,
            upload_url: None,
            file_fingerprint: input.file_fingerprint.to_string(),
            head_tail_sha256: input.head_tail_sha256.to_string(),
            full_sha256: input.full_sha256,
            state: UploadManifestState::Initiating,
            last_error_code: None,
            last_error_message: None,
            expires_at_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }
}

#[async_trait]
impl MediaProcessorPort for MediaService {
    async fn inspect(&self, source: &MediaSourceDescriptor) -> Result<MediaMetadata> {
        if let Some(metadata) = &source.metadata {
            return Ok(metadata.clone());
        }

        let local_path = local_path_from_media_source(source)?;
        let path = Path::new(&local_path);
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| FlareError::localized(ErrorCode::InvalidParameter, "invalid file name"))?
            .to_string();
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| FlareError::general_error(format!("read file metadata failed: {e}")))?;

        Ok(MediaMetadata {
            mime_type: infer_mime_type(&file_name),
            file_name,
            size: metadata.len(),
            width: None,
            height: None,
            duration_ms: None,
            extra: HashMap::new(),
        })
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

#[async_trait]
impl MediaUploaderPort for MediaService {
    async fn upload(
        &self,
        media: ProcessedMedia,
        options: Option<UploadOptions>,
        progress: Option<UploadProgressSink>,
    ) -> Result<UploadedMedia> {
        if let Some(bytes) = media.payload {
            let progress = progress.map(|sink| Arc::new(sink) as UploadProgressCallback);
            return self
                .upload_bytes_with_progress(
                    bytes,
                    media.metadata.file_name,
                    media.metadata.mime_type,
                    options,
                    progress,
                )
                .await;
        }
        let local_path = local_path_from_media_source(&media.source)?;
        let progress = progress.map(|sink| Arc::new(sink) as UploadProgressCallback);
        self.upload_file_from_path_with_progress(Path::new(&local_path), options, progress)
            .await
    }
}

#[async_trait]
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
        self.get_access_url(
            file_id,
            expires_in,
            false,
            reference.map(|r| r.conversation_id()),
            reference.map(|r| r.message_seq()),
        )
        .await
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
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaCacheEntryVo> {
        MediaService::cache_remote_media(self, file_id, expires_in).await
    }

    async fn media_cache_stats(&self) -> Result<crate::domain::MediaCacheStatsVo> {
        MediaService::media_cache_stats(self).await
    }

    async fn set_media_cache_max_bytes(&self, max_bytes: u64) -> Result<()> {
        MediaService::set_media_cache_max_bytes(self, max_bytes).await
    }

    async fn set_media_cache_root(&self, absolute_path: Option<&str>) -> Result<()> {
        MediaService::set_media_cache_root(self, absolute_path).await
    }

    async fn clear_media_cache(&self) -> Result<()> {
        MediaService::clear_media_cache(self).await
    }

    fn cancel_user_file_download(&self, download_key: &str) -> bool {
        MediaService::cancel_user_file_download(self, download_key)
    }

    async fn user_download_get_subfolder(&self) -> Result<String> {
        MediaService::user_download_get_subfolder(self).await
    }

    async fn user_download_set_subfolder(&self, name: &str) -> Result<()> {
        MediaService::user_download_set_subfolder(self, name).await
    }

    async fn user_download_get_saved_path(&self, download_key: &str) -> Result<Option<String>> {
        MediaService::user_download_get_saved_path(self, download_key).await
    }

    async fn user_download_delete_record(&self, download_key: &str) -> Result<()> {
        MediaService::user_download_delete_record(self, download_key).await
    }

    async fn download_file_to_user_downloads_folder(
        &self,
        request: UserFileDownloadRequest,
    ) -> Result<String> {
        MediaService::download_file_to_user_downloads_folder(self, request).await
    }

    async fn download_to_user_directory(
        &self,
        request: UserFileDownloadRequest,
    ) -> Result<UserFileDownloadResultVo> {
        MediaService::download_to_user_directory(self, request).await
    }

    async fn user_download_get_directory(&self) -> Result<UserDownloadDirectoryVo> {
        MediaService::user_download_get_directory(self).await
    }

    async fn user_download_set_directory(
        &self,
        directory: Option<&str>,
    ) -> Result<UserDownloadDirectoryVo> {
        MediaService::user_download_set_directory(self, directory).await
    }

    async fn resolve_media_access_opts(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
    ) -> Result<MediaResolvedAccess> {
        MediaService::resolve_media_access_opts(self, file_id, expires_in, auto_cache).await
    }

    async fn resolve_media_access_opts_with_reference(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
        reference: Option<&crate::platform::ports::media::MessageMediaReference>,
    ) -> Result<MediaResolvedAccess> {
        let Some(reference) = reference else {
            return MediaService::resolve_media_access_opts(self, file_id, expires_in, auto_cache)
                .await;
        };
        // Authorize the exact visible attachment before using a file-id cache hit.
        let remote = MediaServicePort::get_file_url_with_reference(
            self,
            file_id,
            expires_in,
            Some(reference),
        )
        .await?;
        if let Some(cache) = &self.media_cache_store {
            if let Some(hit) = cache.get_cached(file_id).await? {
                return Ok(MediaResolvedAccess {
                    source: "local".into(),
                    local_path: Some(hit.local_path),
                    remote: None,
                });
            }
            if auto_cache {
                // Use the SDK's configured TLS client, not the UI's system trust.
                let entry = fill_cache_from_url(
                    &self.http,
                    cache,
                    file_id,
                    pick_download_url(&remote),
                    AUTO_CACHE_MAX_ENTRY_BYTES,
                    media_mime_of,
                )
                .await?;
                return Ok(MediaResolvedAccess {
                    source: "local".into(),
                    local_path: Some(entry.local_path),
                    remote: None,
                });
            }
        }
        Ok(MediaResolvedAccess {
            source: "remote".into(),
            local_path: None,
            remote: Some(remote),
        })
    }
}

fn local_path_from_media_source(source: &MediaSourceDescriptor) -> Result<String> {
    match &source.kind {
        MediaSourceKind::Path => Ok(source.locator.clone()),
        MediaSourceKind::Uri => Ok(source
            .locator
            .strip_prefix("file://")
            .unwrap_or(&source.locator)
            .to_string()),
        other => Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            format!("media source kind is not supported by native MediaService: {other:?}"),
        )),
    }
}

async fn compute_file_fingerprints(path: &Path) -> Result<(String, String, Option<String>)> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|e| FlareError::general_error(format!("read file metadata failed: {e}")))?;
    let file_size = metadata.len();

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| FlareError::general_error(format!("open file failed: {e}")))?;

    let mut head = vec![0_u8; usize::try_from(file_size.min(1024 * 1024)).unwrap_or(0)];
    if !head.is_empty() {
        file.read_exact(&mut head)
            .await
            .map_err(|e| FlareError::general_error(format!("read file head failed: {e}")))?;
    }

    let tail_len = usize::try_from(file_size.min(1024 * 1024)).unwrap_or(0);
    let mut tail = vec![0_u8; tail_len];
    if tail_len > 0 {
        file.seek(SeekFrom::Start(file_size.saturating_sub(tail_len as u64)))
            .await
            .map_err(|e| FlareError::general_error(format!("seek file tail failed: {e}")))?;
        file.read_exact(&mut tail)
            .await
            .map_err(|e| FlareError::general_error(format!("read file tail failed: {e}")))?;
    }

    let head_hash = hex::encode(Sha256::digest(&head));
    let tail_hash = hex::encode(Sha256::digest(&tail));
    let head_tail_sha256 = hex::encode(Sha256::digest(
        format!("{head_hash}:{tail_hash}:{file_size}").as_bytes(),
    ));
    let fingerprint = hex::encode(Sha256::digest(
        format!("{file_size}:{head_hash}:{tail_hash}").as_bytes(),
    ));

    Ok((fingerprint, head_tail_sha256, None))
}

async fn read_part_bytes(path: &Path, offset: u64, size: u64) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| FlareError::general_error(format!("open file failed: {e}")))?;
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(|e| FlareError::general_error(format!("seek file failed: {e}")))?;
    let mut data =
        vec![0_u8; usize::try_from(size).map_err(|_| FlareError::general_error("part too large"))?];
    file.read_exact(&mut data)
        .await
        .map_err(|e| FlareError::general_error(format!("read file part failed: {e}")))?;
    Ok(data)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn emit_progress(on_progress: Option<&UploadProgressCallback>, progress: UploadProgress) {
    if let Some(cb) = on_progress {
        cb(progress);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MediaService {
    /// 取消进行中的「下载到用户下载目录」任务（与 `download_key` 对应）。
    pub fn cancel_user_file_download(&self, download_key: &str) -> bool {
        let k = download_key.trim();
        if k.is_empty() {
            return false;
        }
        let Ok(g) = self.download_cancel_flags.lock() else {
            return false;
        };
        g.get(k).map(|f| f.store(false, Ordering::SeqCst)).is_some()
    }

    pub async fn user_download_get_subfolder(&self) -> Result<String> {
        let store = self.user_file_download_store.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "user file download store is not configured",
            )
        })?;
        store.get_download_subfolder().await
    }

    pub async fn user_download_set_subfolder(&self, name: &str) -> Result<()> {
        let store = self.user_file_download_store.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "user file download store is not configured",
            )
        })?;
        store.set_download_subfolder(name).await
    }

    /// 这个 key 保存过的文件路径。记录指向的文件已被用户删掉（或挪走）时删记录、返回 `None`：
    /// 宿主据此把「在文件夹中显示」换回「下载」。
    pub async fn user_download_get_saved_path(&self, download_key: &str) -> Result<Option<String>> {
        let store = self.user_file_download_store.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "user file download store is not configured",
            )
        })?;
        let Some(path) = store.get_saved_path(download_key).await? else {
            return Ok(None);
        };
        let present = tokio::fs::metadata(&path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false);
        if present {
            return Ok(Some(path));
        }
        store.delete_download_record(download_key).await?;
        Ok(None)
    }

    pub async fn user_download_delete_record(&self, download_key: &str) -> Result<()> {
        let store = self.user_file_download_store.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "user file download store is not configured",
            )
        })?;
        store.delete_download_record(download_key).await
    }

    fn download_store(&self) -> Result<&Arc<dyn UserFileDownloadStore>> {
        self.user_file_download_store.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "user file download store is not configured",
            )
        })
    }

    /// 平台默认下载目录（含子文件夹）：平台约定的「下载」根 → 不可用时退到本地库同级的 `downloads`。
    async fn default_download_directory(
        &self,
        store: &Arc<dyn UserFileDownloadStore>,
    ) -> Result<(PathBuf, String)> {
        let subfolder = store.get_download_subfolder().await?;
        let fallback = store
            .fallback_download_root()
            .unwrap_or_else(|| std::env::temp_dir().join("flare-downloads"));
        let root = platform_default_download_root().unwrap_or_else(|| fallback.clone());
        Ok((root.join(subfolder.trim()), subfolder))
    }

    /// 「下载位置」：实际生效目录、平台默认目录、用户自选目录。
    pub async fn user_download_get_directory(&self) -> Result<UserDownloadDirectoryVo> {
        let store = self.download_store()?;
        let (default_dir, subfolder) = self.default_download_directory(store).await?;
        let custom = store.get_download_directory().await?;
        let directory = custom
            .clone()
            .unwrap_or_else(|| default_dir.to_string_lossy().into_owned());
        Ok(UserDownloadDirectoryVo {
            directory,
            default_directory: default_dir.to_string_lossy().into_owned(),
            is_custom: custom.is_some(),
            custom_directory: custom,
            subfolder,
        })
    }

    /// 设置用户自选的下载目录（绝对路径，必须可写）；`None` 或空串回到平台默认目录。
    pub async fn user_download_set_directory(
        &self,
        directory: Option<&str>,
    ) -> Result<UserDownloadDirectoryVo> {
        let store = self.download_store()?;
        match directory.map(str::trim).filter(|d| !d.is_empty()) {
            None => store.set_download_directory(None).await?,
            Some(raw) => {
                let dir = resolve_user_download_source_path(raw);
                ensure_writable_dir(&dir).await?;
                store
                    .set_download_directory(Some(&dir.to_string_lossy()))
                    .await?;
            }
        }
        self.user_download_get_directory().await
    }

    /// 这次保存实际写入的目录：自选目录（必须可写，否则报错让用户重选）；
    /// 否则平台默认目录，默认目录不可写时退到兜底目录。
    async fn effective_download_directory(
        &self,
        store: &Arc<dyn UserFileDownloadStore>,
    ) -> Result<PathBuf> {
        if let Some(custom) = store.get_download_directory().await? {
            let dir = PathBuf::from(custom);
            ensure_writable_dir(&dir).await?;
            return Ok(dir);
        }
        let (default_dir, subfolder) = self.default_download_directory(store).await?;
        if ensure_writable_dir(&default_dir).await.is_ok() {
            return Ok(default_dir);
        }
        let fallback = store
            .fallback_download_root()
            .unwrap_or_else(|| std::env::temp_dir().join("flare-downloads"))
            .join(subfolder.trim());
        ensure_writable_dir(&fallback).await?;
        Ok(fallback)
    }

    /// 将文件保存到「下载位置」，并写入 SQLite `user_file_download`。返回保存后的路径。
    pub async fn download_file_to_user_downloads_folder(
        &self,
        request: UserFileDownloadRequest,
    ) -> Result<String> {
        self.download_to_user_directory(request)
            .await
            .map(|saved| saved.path)
    }

    /// 将文件保存到「下载位置」（用户自选目录或平台默认目录），并写入 SQLite `user_file_download`。
    ///
    /// 来源优先级：`source_path` → 本地媒体缓存（按 `remote_file_id`）→ `source_http_url`
    /// → `remote_file_id`（经网关取附件直链）。先写同目录下的隐藏临时文件，完成后改名；
    /// 失败或取消时不留半截文件。远端图片保存后顺带进媒体缓存。
    pub async fn download_to_user_directory(
        &self,
        request: UserFileDownloadRequest,
    ) -> Result<UserFileDownloadResultVo> {
        let UserFileDownloadRequest {
            download_key,
            display_file_name,
            source_path,
            source_http_url,
            remote_file_id,
            message_conversation_id,
            message_seq,
            expires_in,
            on_progress,
        } = request;
        let non_empty =
            |v: Option<String>| v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        let source_path = non_empty(source_path);
        let source_http_url = non_empty(source_http_url);
        let remote_file_id = non_empty(remote_file_id);
        if message_conversation_id.is_some() || message_seq.is_some() {
            let valid = matches!((&message_conversation_id, message_seq), (Some(conversation), Some(seq))
                if !conversation.is_empty() && conversation.len() <= 128 && conversation.trim() == conversation && (1..i64::MAX).contains(&seq));
            if !valid
                || remote_file_id.is_none()
                || source_path.is_some()
                || source_http_url.is_some()
            {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "invalid message-bound file download reference",
                ));
            }
        }
        if source_path.is_none() && source_http_url.is_none() && remote_file_id.is_none() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "provide source_path, source_http_url, or remote_file_id",
            ));
        }
        if let Some(url) = &source_http_url
            && !(url.starts_with("http://") || url.starts_with("https://"))
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "source_http_url must be http(s)",
            ));
        }
        let key = non_empty(Some(download_key))
            .or_else(|| remote_file_id.clone())
            .or_else(|| source_http_url.clone())
            .or_else(|| source_path.clone())
            .unwrap_or_default();
        let wanted_name = non_empty(Some(display_file_name)).unwrap_or_else(|| {
            source_path
                .as_deref()
                .or(source_http_url
                    .as_deref()
                    .map(|u| u.split('?').next().unwrap_or(u)))
                .and_then(|s| s.rsplit(['/', '\\']).find(|seg| !seg.is_empty()))
                .map(str::to_string)
                .or_else(|| remote_file_id.clone())
                .unwrap_or_else(|| "download".to_string())
        });

        let store = self.download_store()?.clone();
        let run_flag = Arc::new(AtomicBool::new(true));
        {
            let mut m = self.download_cancel_flags.lock().map_err(|_| {
                FlareError::localized(ErrorCode::InternalError, "download cancel map lock failed")
            })?;
            m.insert(key.clone(), run_flag.clone());
        }

        let result = async {
            let dir = self.effective_download_directory(&store).await?;
            let safe_name = sanitize_user_download_file_name(&wanted_name);
            let mut dest = checked_user_download_destination(&dir, &safe_name)?;
            let tmp = temp_sibling(&dest);
            let flag = Some(run_flag.as_ref());
            let progress = on_progress.as_ref();

            let cached = match (&remote_file_id, &self.media_cache_store) {
                (Some(fid), Some(cache)) if source_path.is_none() => cache.get_cached(fid).await?,
                _ => None,
            };
            let mut from_cache = false;
            let mut fetched_url: Option<String> = None;
            let written: WrittenFile = if let Some(p) = &source_path {
                let src = resolve_user_download_source_path(p);
                copy_file_to(&src, &tmp, MAX_USER_DOWNLOAD_BYTES, flag, progress).await?
            } else if let Some(hit) = &cached {
                from_cache = true;
                copy_file_to(
                    Path::new(&hit.local_path),
                    &tmp,
                    MAX_USER_DOWNLOAD_BYTES,
                    flag,
                    progress,
                )
                .await?
            } else {
                let url = match (&source_http_url, &remote_file_id) {
                    (Some(u), _) => u.clone(),
                    (None, Some(fid)) => {
                        let access = self
                            .get_message_download_url(
                                fid,
                                expires_in,
                                message_conversation_id.as_deref(),
                                message_seq,
                            )
                            .await?;
                        let u = pick_download_url(&access).to_string();
                        if u.is_empty() {
                            return Err(FlareError::localized(
                                ErrorCode::GeneralError,
                                "empty download url from gateway",
                            ));
                        }
                        u
                    }
                    (None, None) => unreachable!("checked above"),
                };
                let w = stream_http_to_file(
                    &self.http,
                    &url,
                    &tmp,
                    MAX_USER_DOWNLOAD_BYTES,
                    flag,
                    progress,
                )
                .await?;
                fetched_url = Some(url);
                w
            };

            // 文件的类型：缓存记录 → 响应头 / 来源地址 → 文件头。
            let head = read_head(&tmp, 16).await;
            let mime = cached
                .as_ref()
                .map(|hit| hit.mime_type.clone())
                .filter(|m| !m.is_empty() && m != "application/octet-stream")
                .unwrap_or_else(|| {
                    let origin = fetched_url
                        .as_deref()
                        .or(source_path.as_deref())
                        .unwrap_or("");
                    media_mime_of(origin, written.content_type.as_deref(), &head)
                });
            // 图片、视频通常没有文件名：名字没扩展名就按类型补上，否则存下来的文件打不开。
            let final_name = match extension_for_mime(&mime) {
                Some(ext) if Path::new(&safe_name).extension().is_none() => {
                    format!("{safe_name}.{ext}")
                }
                _ => safe_name.clone(),
            };
            // 等待期间同名文件可能被别处写出来了：改名前再挑一次不冲突的名字。
            if final_name != safe_name || dest.exists() {
                dest = checked_user_download_destination(&dir, &final_name)?;
            }
            if let Err(e) = tokio::fs::rename(&tmp, &dest).await {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(FlareError::general_error(format!(
                    "save downloaded file failed: {e}"
                )));
            }

            // 远端来的图片顺带进缓存：之后显示与再次保存都不再走网络。
            if let (Some(_), Some(fid), Some(cache)) =
                (&fetched_url, &remote_file_id, &self.media_cache_store)
                && written.size <= AUTO_CACHE_MAX_ENTRY_BYTES
                && mime.starts_with("image/")
            {
                let _ = cache.put_file(fid, &dest, &mime, false).await;
            }

            let path_str = dest.to_string_lossy().into_owned();
            let file_name = dest
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| safe_name.clone());
            store
                .save_download_record(&key, &path_str, &wanted_name)
                .await?;
            Ok(UserFileDownloadResultVo {
                path: path_str,
                directory: dir.to_string_lossy().into_owned(),
                file_name,
                size_bytes: written.size,
                from_cache,
                download_key: key.clone(),
            })
        }
        .await;

        if let Ok(mut m) = self.download_cancel_flags.lock() {
            m.remove(&key);
        }

        result
    }

    /// 解析媒体访问方式：本地缓存命中返回本地文件，否则返回短时 URL。
    /// `auto_cache` 为真时，未命中的文件在后台拉进缓存（单个 ≤ 32 MiB），下次直接用本地文件。
    pub async fn resolve_media_access_opts(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
    ) -> Result<MediaResolvedAccess> {
        self.resolve_media_access_caching(file_id, expires_in, auto_cache)
            .await
            .map(|(resolved, _)| resolved)
    }

    /// [`Self::resolve_media_access_opts`] plus the background cache task it started
    /// (`None` when it started none), for a caller that waits for the cache.
    async fn resolve_media_access_caching(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
    ) -> Result<(MediaResolvedAccess, Option<tokio::task::JoinHandle<()>>)> {
        let resolved = self.resolve_media_access(file_id, expires_in).await?;
        let task = match &resolved.remote {
            Some(remote) if auto_cache && resolved.local_path.is_none() => {
                self.spawn_auto_cache(file_id.trim(), pick_download_url(remote))
            }
            _ => None,
        };
        Ok((resolved, task))
    }

    /// Fills the cache with `file_id` in the background, at most one task per file at a time.
    fn spawn_auto_cache(&self, file_id: &str, url: &str) -> Option<tokio::task::JoinHandle<()>> {
        let cache = self.media_cache_store.clone()?;
        if file_id.is_empty() || url.is_empty() {
            return None;
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        {
            let mut inflight = self.auto_cache_inflight.lock().ok()?;
            if !inflight.insert(file_id.to_string()) {
                return None;
            }
        }
        let http = self.http.clone();
        let inflight = self.auto_cache_inflight.clone();
        let fid = file_id.to_string();
        let url = url.to_string();
        Some(handle.spawn(async move {
            // The caller saw no cached file, then asked the gateway for a URL: a task for the
            // same file can finish in between and give its slot back. A task writes the cache
            // before it gives the slot back, so holding the slot, a second look sees that
            // file and does not fetch it again.
            let cached = matches!(cache.get_cached(&fid).await, Ok(Some(_)));
            if !cached
                && let Err(e) = fill_cache_from_url(
                    &http,
                    &cache,
                    &fid,
                    &url,
                    AUTO_CACHE_MAX_ENTRY_BYTES,
                    media_mime_of,
                )
                .await
            {
                tracing::debug!(file_id = %fid, error = %e, "media auto cache skipped");
            }
            if let Ok(mut set) = inflight.lock() {
                set.remove(&fid);
            }
        }))
    }

    /// 上传成功后把发送方手里的源文件按上传得到的 `file_id` **复制**进媒体缓存（原文件留在原处）。
    ///
    /// 消息内容上传后只剩 `file_id`：不进缓存的话，气泡解析时拿到的是远端地址，还会在后台把
    /// 发送方磁盘上本来就有的文件再下载一遍 —— 慢网下自己刚发的图要空白好几分钟。
    ///
    /// 规则与显示时自动缓存一致：任何类型、单个不超过 [`AUTO_CACHE_MAX_ENTRY_BYTES`]；
    /// 更大的（长视频、大附件）不进缓存，免得一个文件把缓存里的图片整批挤出去。
    /// 写缓存失败只记日志，绝不让上传 / 发送失败。
    async fn cache_uploaded_file(&self, uploaded: &UploadedMedia, path: &Path, size: u64) {
        let Some((cache, fid)) = self.uploaded_cache_target(uploaded, size) else {
            return;
        };
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let head = read_head(path, 16).await;
        let mime = uploaded_media_mime(&uploaded.mime_type, name, &head);
        if let Err(e) = cache.put_file(fid, path, &mime, false).await {
            tracing::warn!(file_id = %fid, error = %e, "caching uploaded media failed; upload unaffected");
        }
    }

    /// [`Self::cache_uploaded_file`] 的字节版（`data:` 来源、`upload_bytes`）。
    async fn cache_uploaded_bytes(&self, uploaded: &UploadedMedia, bytes: &[u8]) {
        let Some((cache, fid)) = self.uploaded_cache_target(uploaded, bytes.len() as u64) else {
            return;
        };
        let head = &bytes[..bytes.len().min(16)];
        let mime = uploaded_media_mime(&uploaded.mime_type, &uploaded.file_name, head);
        if let Err(e) = cache.put_bytes(fid, bytes, &mime).await {
            tracing::warn!(file_id = %fid, error = %e, "caching uploaded media failed; upload unaffected");
        }
    }

    fn uploaded_cache_target<'a>(
        &'a self,
        uploaded: &'a UploadedMedia,
        size: u64,
    ) -> Option<(&'a Arc<dyn MediaCacheStore>, &'a str)> {
        let cache = self.media_cache_store.as_ref()?;
        let fid = uploaded.file_id.trim();
        if fid.is_empty() {
            return None;
        }
        if size > AUTO_CACHE_MAX_ENTRY_BYTES {
            tracing::debug!(file_id = %fid, size, "uploaded media over the per-entry cache limit; not cached");
            return None;
        }
        Some((cache, fid))
    }
}

/// 上传后进缓存时记下的 MIME：上传结果里的类型（非通用二进制时）→ 文件名后缀 → 文件头。
#[cfg(not(target_arch = "wasm32"))]
fn uploaded_media_mime(declared: &str, file_name: &str, head: &[u8]) -> String {
    media_mime_of(&format!("/{file_name}"), Some(declared.trim()), head)
}

#[cfg(not(target_arch = "wasm32"))]
fn sanitize_user_download_file_name(name: &str) -> String {
    let base = name
        .trim()
        .rsplit(['/', '\\'])
        .find(|segment| !segment.trim().is_empty())
        .map(str::trim)
        .unwrap_or("");
    if base.is_empty() {
        return "download".to_string();
    }
    let sanitized = base
        .chars()
        .map(|ch| {
            if ch.is_control() || matches!(ch, '?' | '%' | '*' | ':' | '|' | '"' | '<' | '>') {
                '_'
            } else {
                ch
            }
        })
        .take(200)
        .collect::<String>();
    let sanitized = sanitized.trim_matches([' ', '.']).to_string();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        return "download".to_string();
    }
    if is_windows_reserved_file_name(&sanitized) {
        return format!("{sanitized}_");
    }
    sanitized
}

#[cfg(not(target_arch = "wasm32"))]
fn is_windows_reserved_file_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).to_ascii_lowercase();
    matches!(
        stem.as_str(),
        "con"
            | "prn"
            | "aux"
            | "nul"
            | "com1"
            | "com2"
            | "com3"
            | "com4"
            | "com5"
            | "com6"
            | "com7"
            | "com8"
            | "com9"
            | "lpt1"
            | "lpt2"
            | "lpt3"
            | "lpt4"
            | "lpt5"
            | "lpt6"
            | "lpt7"
            | "lpt8"
            | "lpt9"
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn resolve_user_download_source_path(raw: &str) -> std::path::PathBuf {
    let t = raw.trim();
    if t.to_lowercase().starts_with("file:")
        && let Ok(u) = url::Url::parse(t)
        && let Ok(pb) = u.to_file_path()
    {
        return pb;
    }
    PathBuf::from(t)
}

#[cfg(not(target_arch = "wasm32"))]
fn checked_user_download_destination(dir: &Path, file_name: &str) -> Result<PathBuf> {
    let dest = unique_user_download_destination(dir, file_name);
    ensure_user_download_destination_in_dir(dir, dest)
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_user_download_destination_in_dir(dir: &Path, dest: PathBuf) -> Result<PathBuf> {
    if dest.starts_with(dir) && dest.parent().is_some_and(|parent| parent == dir) {
        Ok(dest)
    } else {
        Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "download destination escapes user download directory",
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn unique_user_download_destination(dir: &Path, file_name: &str) -> PathBuf {
    let dest = dir.join(file_name);
    if !dest.exists() {
        return dest;
    }
    let stem = Path::new(file_name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file");
    let ext = Path::new(file_name)
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| format!(".{e}"))
        .unwrap_or_default();
    for i in 1..10_000 {
        let candidate = dir.join(format!("{stem} ({i}){ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    dir.join(format!("{stem}_{t}{ext}"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod user_download_policy_tests {
    use super::*;

    use super::super::native_download::check_budget;

    #[test]
    fn rejects_content_length_over_user_download_budget() {
        let err = check_budget(
            Some(MAX_USER_DOWNLOAD_BYTES + 1),
            0,
            0,
            MAX_USER_DOWNLOAD_BYTES,
        )
        .expect_err("content length beyond budget must fail");

        assert_eq!(err.code(), Some(ErrorCode::ResourceExhausted));
    }

    #[test]
    fn rejects_chunked_download_when_accumulated_bytes_exceed_budget() {
        let err = check_budget(None, MAX_USER_DOWNLOAD_BYTES, 1, MAX_USER_DOWNLOAD_BYTES)
            .expect_err("chunked response beyond budget must fail");

        assert_eq!(err.code(), Some(ErrorCode::ResourceExhausted));
    }

    #[test]
    fn sanitizes_user_download_file_name_to_safe_basename() {
        assert_eq!(
            sanitize_user_download_file_name("../../etc/passwd"),
            "passwd"
        );
        assert_eq!(
            sanitize_user_download_file_name(r"C:\tmp\payload?.txt"),
            "payload_.txt"
        );
        assert_eq!(sanitize_user_download_file_name(".."), "download");
        assert_eq!(sanitize_user_download_file_name(".hidden."), "hidden");
        assert_eq!(sanitize_user_download_file_name("CON.txt"), "CON.txt_");
        assert_eq!(
            sanitize_user_download_file_name("bad\u{0000}\u{001f}name.txt"),
            "bad__name.txt"
        );
    }

    #[test]
    fn rejects_download_destination_outside_user_download_dir() {
        let dir = Path::new("/tmp/flare-downloads");

        assert!(
            ensure_user_download_destination_in_dir(dir, dir.join("safe.txt")).is_ok(),
            "direct child path should be accepted"
        );
        assert!(
            ensure_user_download_destination_in_dir(dir, PathBuf::from("/tmp/evil.txt")).is_err(),
            "sibling path must be rejected"
        );
        assert!(
            ensure_user_download_destination_in_dir(dir, dir.join("nested").join("evil.txt"))
                .is_err(),
            "nested path must be rejected because downloads are saved as basenames"
        );
    }
}

/// 选择用于下载/展示的 HTTP 地址。
///
/// `flare-media` 对私有对象：`url` 为 S3 预签名链接，`cdn_url` 常为 `cdn_base + object_path`（无签名）。
/// 因此 `url` 是权威访问地址，`cdn_url` 只作为后备展示 hint。
fn pick_download_url(access: &MediaAccessUrl) -> &str {
    let u = access.url.trim();
    if !u.is_empty() {
        return u;
    }
    access.cdn_url.as_deref().unwrap_or("").trim()
}

#[cfg(test)]
mod media_access_url_selection_tests {
    use super::*;

    #[test]
    fn pick_download_url_prefers_core_media_url_over_cdn_hint() {
        let access = MediaAccessUrl {
            url: "http://127.0.0.1:29000/flare-media/private.png?X-Amz-Signature=ok".to_string(),
            cdn_url: Some("http://127.0.0.1:29000/flare-media/private.png".to_string()),
        };

        assert_eq!(
            pick_download_url(&access),
            "http://127.0.0.1:29000/flare-media/private.png?X-Amz-Signature=ok"
        );
    }

    #[test]
    fn pick_download_url_uses_cdn_hint_only_when_core_url_is_absent() {
        let access = MediaAccessUrl {
            url: " ".to_string(),
            cdn_url: Some("http://127.0.0.1:29000/flare-media/public.png".to_string()),
        };

        assert_eq!(
            pick_download_url(&access),
            "http://127.0.0.1:29000/flare-media/public.png"
        );
    }
}

/// 下载来的文件的 MIME：响应头（非通用二进制时）→ URL 后缀 → 文件头。
#[cfg(not(target_arch = "wasm32"))]
fn media_mime_of(url: &str, content_type: Option<&str>, head: &[u8]) -> String {
    if let Some(ct) = content_type.filter(|ct| {
        !ct.is_empty() && *ct != "application/octet-stream" && *ct != "binary/octet-stream"
    }) {
        return ct.to_string();
    }
    infer_mime_from_url_or_octet_stream(url, head)
}

fn infer_mime_from_url_or_octet_stream(url: &str, bytes: &[u8]) -> String {
    let path = url.split('?').next().unwrap_or(url);
    if let Some(idx) = path.rfind('/') {
        let name = &path[idx + 1..];
        if name.contains('.') {
            return infer_mime_type(name);
        }
    }
    if bytes.len() >= 3 && bytes[..3] == [0xFF, 0xD8, 0xFF] {
        return "image/jpeg".to_string();
    }
    if bytes.len() >= 8 && bytes[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] {
        return "image/png".to_string();
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return "image/webp".to_string();
    }
    if bytes.len() >= 4 && &bytes[..4] == b"GIF8" {
        return "image/gif".to_string();
    }
    if bytes.len() >= 4 && &bytes[..4] == b"%PDF" {
        return "application/pdf".to_string();
    }
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        return if &bytes[8..10] == b"qt" {
            "video/quicktime".to_string()
        } else {
            "video/mp4".to_string()
        };
    }
    if bytes.len() >= 4 && bytes[..4] == [0x1A, 0x45, 0xDF, 0xA3] {
        return "video/webm".to_string();
    }
    "application/octet-stream".to_string()
}

fn validate_path_mime_prefix(path: &Path, expected_prefix: &str) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| FlareError::localized(ErrorCode::InvalidParameter, "invalid file name"))?;
    let mime = infer_mime_type(file_name);
    if !mime.starts_with(expected_prefix) {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            format!("expected {expected_prefix} mime type, got {mime}"),
        ));
    }
    Ok(())
}

fn infer_mime_type(file_name: &str) -> String {
    let lower = file_name.to_ascii_lowercase();
    if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        "image/jpeg".to_string()
    } else if lower.ends_with(".png") {
        "image/png".to_string()
    } else if lower.ends_with(".gif") {
        "image/gif".to_string()
    } else if lower.ends_with(".webp") {
        "image/webp".to_string()
    } else if lower.ends_with(".mp4") {
        "video/mp4".to_string()
    } else if lower.ends_with(".webm") {
        "video/webm".to_string()
    } else if lower.ends_with(".mp3") {
        "audio/mpeg".to_string()
    } else if lower.ends_with(".wav") {
        "audio/wav".to_string()
    } else if lower.ends_with(".aac") {
        "audio/aac".to_string()
    } else if lower.ends_with(".pdf") {
        "application/pdf".to_string()
    } else {
        "application/octet-stream".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_upload_parts_respects_part_boundaries() {
        let manifest = MediaUploadManifestVo {
            local_upload_id: "local-upload-1".to_string(),
            remote_upload_id: None,
            file_id: None,
            storage_upload_id: None,
            tenant_id: String::new(),
            user_id: "u1".to_string(),
            source_kind: UploadSourceKind::StableFile,
            source_locator: "/tmp/demo.bin".to_string(),
            file_name: "demo.bin".to_string(),
            mime_type: "application/octet-stream".to_string(),
            file_size: 10,
            part_size: 4,
            total_parts: 3,
            transport_kind: Some(DirectUploadTransportKindVo::MultipartPut),
            bucket: None,
            object_key: None,
            upload_url: None,
            file_fingerprint: "fp".to_string(),
            head_tail_sha256: "ht".to_string(),
            full_sha256: None,
            state: UploadManifestState::Uploading,
            last_error_code: None,
            last_error_message: None,
            expires_at_ms: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        };

        let parts = build_upload_parts_from_manifest(&manifest);
        assert_eq!(parts.len(), 3);
        assert_eq!(
            (parts[0].part_number, parts[0].offset, parts[0].size),
            (1, 0, 4)
        );
        assert_eq!(
            (parts[1].part_number, parts[1].offset, parts[1].size),
            (2, 4, 4)
        );
        assert_eq!(
            (parts[2].part_number, parts[2].offset, parts[2].size),
            (3, 8, 2)
        );
    }

    #[test]
    fn infer_mime_and_file_type_cover_common_media() {
        assert_eq!(infer_mime_type("photo.png"), "image/png");
        assert_eq!(infer_mime_type("clip.mp4"), "video/mp4");
        assert_eq!(infer_mime_type("voice.mp3"), "audio/mpeg");
        assert_eq!(infer_mime_type("report.pdf"), "application/pdf");
    }
}

#[cfg(all(test, feature = "storage-sqlite", not(target_arch = "wasm32")))]
mod download_and_cache_tests {
    use super::*;
    use crate::domain::{DEFAULT_MEDIA_CACHE_MAX_BYTES, MediaCacheAdmin};
    use crate::infrastructure::persistence::sqlite::{
        SqliteMediaCacheRepo, SqliteUserFileDownloadRepo, init_schema,
    };
    use sqlx::SqlitePool;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncBufReadExt, BufReader};

    const PNG: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
    ];

    /// 假网关：`POST /api/v1/medias/file-url` 返回指向自己的 `/blob/<fileId>`；
    /// `GET /blob/ok-*` 返回 PNG 字节，`GET /blob/broken-*` 返回 500。记录 blob 被取的次数。
    /// 直传三步（initiate → `PUT /put/<fileId>` → complete）按单次 PUT 应答，fileId 为 `ok-up-<本地上传 id>`。
    async fn fake_gateway() -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let (base_c, hits_c) = (base.clone(), hits.clone());
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let (base, hits) = (base_c.clone(), hits_c.clone());
                tokio::spawn(async move {
                    let mut reader = BufReader::new(socket);
                    let mut request_line = String::new();
                    reader.read_line(&mut request_line).await.unwrap();
                    let mut len = 0usize;
                    loop {
                        let mut line = String::new();
                        reader.read_line(&mut line).await.unwrap();
                        let line = line.trim_end();
                        if line.is_empty() {
                            break;
                        }
                        if let Some((k, v)) = line.split_once(':')
                            && k.eq_ignore_ascii_case("content-length")
                        {
                            len = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; len];
                    reader.read_exact(&mut body).await.unwrap();
                    let path = request_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("/")
                        .to_string();
                    let (status, ctype, payload): (&str, &str, Vec<u8>) =
                        if path.starts_with("/api/v1/medias/file-url") {
                            let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                            let fid = req["file_id"].as_str().unwrap_or_default();
                            if fid == "ok-message-reference" || fid == "ok-message-preview" {
                                assert_eq!(req["conversation_id"], "chat-1");
                                assert_eq!(req["message_seq"], 7);
                                assert_eq!(req["download"], fid == "ok-message-reference");
                            }
                            let json = serde_json::json!({
                                "code": 0,
                                "data": { "url": format!("{base}/blob/{fid}") }
                            });
                            ("200 OK", "application/json", json.to_string().into_bytes())
                        } else if path.starts_with("/api/v1/medias/uploads/initiate") {
                            let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                            let local = req["metadata"]["upload_id"].as_str().unwrap_or_default();
                            let fid = format!("ok-up-{local}");
                            let json = serde_json::json!({
                                "code": 0,
                                "data": {
                                    "upload_id": fid,
                                    "file_id": fid,
                                    "transport_kind": "single_put",
                                    "bucket": "flare-media",
                                    "object_key": fid,
                                    "storage_upload_id": null,
                                    "part_size": req["metadata"]["file_size"],
                                    "total_parts": 1,
                                    "upload_url": format!("{base}/put/{fid}"),
                                    "success": true,
                                    "error_message": null
                                }
                            });
                            ("200 OK", "application/json", json.to_string().into_bytes())
                        } else if path.starts_with("/put/") {
                            ("200 OK", "text/plain", Vec::new())
                        } else if path.starts_with("/api/v1/medias/uploads/complete") {
                            let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                            let fid = req["upload_id"].as_str().unwrap_or_default();
                            let json = serde_json::json!({
                                "code": 0,
                                "data": {
                                    "file_id": fid,
                                    "url": null,
                                    "cdn_url": null,
                                    "success": true,
                                    "error_message": null,
                                    "info": null
                                }
                            });
                            ("200 OK", "application/json", json.to_string().into_bytes())
                        } else if path.starts_with("/blob/broken") {
                            hits.fetch_add(1, Ordering::SeqCst);
                            ("500 Internal Server Error", "text/plain", b"boom".to_vec())
                        } else {
                            hits.fetch_add(1, Ordering::SeqCst);
                            ("200 OK", "image/png", PNG.to_vec())
                        };
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        payload.len()
                    );
                    let socket = reader.get_mut();
                    socket.write_all(head.as_bytes()).await.unwrap();
                    socket.write_all(&payload).await.unwrap();
                });
            }
        });
        (base, hits)
    }

    struct Fixture {
        service: MediaService,
        cache_admin: Arc<SqliteMediaCacheRepo>,
        root: PathBuf,
        hits: Arc<AtomicUsize>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    async fn fixture() -> Fixture {
        let (base, hits) = fake_gateway().await;
        let root =
            std::env::temp_dir().join(format!("flare-media-{}", uuid::Uuid::new_v4().simple()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_schema(&pool).await.unwrap();
        let cache = Arc::new(
            SqliteMediaCacheRepo::create(pool.clone(), root.join("media_cache"))
                .await
                .unwrap(),
        );
        let downloads = Arc::new(
            SqliteUserFileDownloadRepo::new(pool).with_fallback_root(root.join("downloads")),
        );
        let service = MediaService::new(
            HttpClient::new(base),
            Arc::new(RwLock::new("u1".to_string())),
            None,
            Some(cache.clone() as Arc<dyn MediaCacheStore>),
            Some(cache.clone() as Arc<dyn MediaCacheAdmin>),
            Some(downloads as Arc<dyn UserFileDownloadStore>),
        );
        Fixture {
            service,
            cache_admin: cache,
            root,
            hits,
        }
    }

    fn request(file_id: &str, name: &str) -> UserFileDownloadRequest {
        UserFileDownloadRequest {
            download_key: String::new(),
            display_file_name: name.to_string(),
            source_path: None,
            source_http_url: None,
            remote_file_id: Some(file_id.to_string()),
            message_conversation_id: None,
            message_seq: None,
            expires_in: 600,
            on_progress: None,
        }
    }

    #[tokio::test]
    async fn message_media_preview_forwards_reference_without_attachment_disposition() {
        let f = fixture().await;
        let reference =
            crate::platform::ports::media::MessageMediaReference::new("chat-1".into(), 7).unwrap();
        let url = MediaServicePort::get_file_url_with_reference(
            &f.service,
            "ok-message-preview",
            600,
            Some(&reference),
        )
        .await
        .unwrap();
        assert!(!url.url.is_empty());
        assert_eq!(f.hits.load(Ordering::SeqCst), 0);
        f.cache_admin.clear_media_cache().await.unwrap();
        let resolved = MediaServicePort::resolve_media_access_opts_with_reference(
            &f.service,
            "ok-message-preview",
            600,
            true,
            Some(&reference),
        )
        .await
        .unwrap();
        assert_eq!(
            tokio::fs::read(resolved.local_path.unwrap()).await.unwrap(),
            PNG
        );
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn message_bound_download_forwards_the_exact_reference_and_rejects_partial_hints() {
        let f = fixture().await;
        let mut valid = request("ok-message-reference", "attachment.txt");
        valid.message_conversation_id = Some("chat-1".into());
        valid.message_seq = Some(7);
        let saved = f
            .service
            .download_to_user_directory(valid.clone())
            .await
            .expect("scoped download");
        assert_eq!(tokio::fs::read(&saved.path).await.unwrap(), PNG);
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);
        for mode in 0..4 {
            let mut bad = valid.clone();
            match mode {
                0 => bad.message_seq = None,
                1 => bad.message_seq = Some(0),
                2 => bad.message_conversation_id = Some(" chat-1".into()),
                _ => bad.source_http_url = Some("https://other.test/file".into()),
            }
            assert!(f.service.download_to_user_directory(bad).await.is_err());
        }
        assert_eq!(
            f.hits.load(Ordering::SeqCst),
            1,
            "invalid hints must fail before HTTP/cache writes"
        );
    }

    fn leftovers(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|it| {
                it.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.ends_with(".flaredownload"))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn saves_into_chosen_directory_and_reuses_the_cache() {
        let f = fixture().await;
        let chosen = f.root.join("我的下载");
        let info = f
            .service
            .user_download_set_directory(Some(&chosen.to_string_lossy()))
            .await
            .unwrap();
        assert!(info.is_custom);
        assert_eq!(PathBuf::from(&info.directory), chosen);

        let first = f
            .service
            .download_to_user_directory(request("ok-1", "截图.png"))
            .await
            .unwrap();
        assert_eq!(PathBuf::from(&first.path), chosen.join("截图.png"));
        assert_eq!(std::fs::read(&first.path).unwrap(), PNG);
        assert!(!first.from_cache);
        assert_eq!(first.download_key, "ok-1");
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);
        assert!(leftovers(&chosen).is_empty());

        // 远端图片保存时顺带进了缓存：再保存一次不走网络，同名文件带序号。
        let second = f
            .service
            .download_to_user_directory(request("ok-1", "截图.png"))
            .await
            .unwrap();
        assert!(second.from_cache);
        assert_eq!(second.file_name, "截图 (1).png");
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.service
                .user_download_get_saved_path("ok-1")
                .await
                .unwrap(),
            Some(second.path.clone())
        );
    }

    #[tokio::test]
    async fn a_saved_file_deleted_by_the_user_is_forgotten() {
        let f = fixture().await;
        let chosen = f.root.join("gone");
        f.service
            .user_download_set_directory(Some(&chosen.to_string_lossy()))
            .await
            .unwrap();
        let saved = f
            .service
            .download_to_user_directory(request("ok-gone", "合同.pdf"))
            .await
            .unwrap();
        assert_eq!(
            f.service
                .user_download_get_saved_path("ok-gone")
                .await
                .unwrap(),
            Some(saved.path.clone())
        );

        std::fs::remove_file(&saved.path).unwrap();
        assert_eq!(
            f.service
                .user_download_get_saved_path("ok-gone")
                .await
                .unwrap(),
            None
        );

        // 再下载一次回到原名（旧文件已不在），记录指向新文件。
        let again = f
            .service
            .download_to_user_directory(request("ok-gone", "合同.pdf"))
            .await
            .unwrap();
        assert_eq!(again.file_name, "合同.pdf");
        assert_eq!(
            f.service
                .user_download_get_saved_path("ok-gone")
                .await
                .unwrap(),
            Some(again.path)
        );
    }

    #[tokio::test]
    async fn a_name_without_extension_gets_one_from_the_media_type() {
        let f = fixture().await;
        let chosen = f.root.join("ext");
        f.service
            .user_download_set_directory(Some(&chosen.to_string_lossy()))
            .await
            .unwrap();
        let saved = f
            .service
            .download_to_user_directory(request("ok-ext", "IMG_20260927"))
            .await
            .unwrap();
        assert_eq!(saved.file_name, "IMG_20260927.png");
        // 名字自带扩展名时原样保留。
        let named = f
            .service
            .download_to_user_directory(request("ok-ext2", "报告.pdf"))
            .await
            .unwrap();
        assert_eq!(named.file_name, "报告.pdf");
        assert!(leftovers(&chosen).is_empty());
    }

    #[tokio::test]
    async fn default_directory_and_reset() {
        let f = fixture().await;
        let info = f.service.user_download_get_directory().await.unwrap();
        assert!(!info.is_custom);
        assert_eq!(info.custom_directory, None);
        assert_eq!(info.directory, info.default_directory);
        assert!(info.directory.ends_with("flare"), "{}", info.directory);

        assert!(
            f.service
                .user_download_set_directory(Some("relative/dir"))
                .await
                .is_err(),
            "相对路径必须拒绝"
        );
        let chosen = f.root.join("picked");
        f.service
            .user_download_set_directory(Some(&chosen.to_string_lossy()))
            .await
            .unwrap();
        let reset = f.service.user_download_set_directory(None).await.unwrap();
        assert!(!reset.is_custom);
        assert_eq!(reset.directory, info.default_directory);
    }

    #[tokio::test]
    async fn failed_download_leaves_nothing_behind() {
        let f = fixture().await;
        let chosen = f.root.join("out");
        f.service
            .user_download_set_directory(Some(&chosen.to_string_lossy()))
            .await
            .unwrap();
        let err = f
            .service
            .download_to_user_directory(request("broken-1", "坏文件.pdf"))
            .await;
        assert!(err.is_err());
        assert!(!chosen.join("坏文件.pdf").exists());
        assert!(leftovers(&chosen).is_empty(), "{:?}", leftovers(&chosen));
        assert_eq!(
            f.service
                .user_download_get_saved_path("broken-1")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn displayed_images_are_cached_in_the_background() {
        let f = fixture().await;
        let (first, task) = f
            .service
            .resolve_media_access_caching("ok-auto", 600, true)
            .await
            .unwrap();
        assert!(first.local_path.is_none());
        task.expect("an image shown before it is cached is cached in the background")
            .await
            .unwrap();
        let (again, none) = f
            .service
            .resolve_media_access_caching("ok-auto", 600, true)
            .await
            .unwrap();
        let local = again.local_path.expect("自动缓存应在后台完成");
        assert!(none.is_none(), "a cached image starts no fetch");
        assert_eq!(std::fs::read(local).unwrap(), PNG);
        // 后台只拉一次；之后都读本地。
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);

        // 不开自动缓存则从不落盘。
        let (plain, none) = f
            .service
            .resolve_media_access_caching("ok-plain", 600, false)
            .await
            .unwrap();
        assert!(plain.local_path.is_none());
        assert!(none.is_none(), "without auto cache nothing is fetched");
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);
    }

    /// 显示方先看到「没缓存」、再去网关要地址，这期间同一张图的上一次后台缓存可能刚好写完
    /// 缓存并让出名额。此刻再排后台任务，缓存里已经有了：不能再拉一遍。
    #[tokio::test]
    async fn an_image_cached_while_it_was_resolved_is_not_fetched_again() {
        let f = fixture().await;
        let remote = f
            .service
            .resolve_media_access("ok-race", 600)
            .await
            .unwrap()
            .remote
            .expect("not cached yet");
        // The earlier background task: it finished and gave its slot back.
        f.service.cache_remote_media("ok-race", 600).await.unwrap();
        assert_eq!(f.hits.load(Ordering::SeqCst), 1);

        f.service
            .spawn_auto_cache("ok-race", pick_download_url(&remote))
            .expect("no task for the file is in flight")
            .await
            .unwrap();
        assert_eq!(
            f.hits.load(Ordering::SeqCst),
            1,
            "the cached image was fetched again"
        );
        // The slot is given back, and a file that is not cached is still fetched.
        f.service
            .spawn_auto_cache("ok-other", pick_download_url(&remote))
            .expect("the slot is free again")
            .await
            .unwrap();
        assert_eq!(f.hits.load(Ordering::SeqCst), 2);
        assert!(
            f.cache_admin
                .get_cached("ok-other")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cache_has_a_default_cap_and_clears_staging() {
        let f = fixture().await;
        let stats = f.cache_admin.media_cache_stats().await.unwrap();
        assert_eq!(stats.max_bytes, DEFAULT_MEDIA_CACHE_MAX_BYTES);
        assert!(stats.max_bytes_is_default);

        let entry = f.service.cache_remote_media("ok-2", 600).await.unwrap();
        assert_eq!(entry.mime_type, "image/png");
        assert_eq!(entry.size_bytes, PNG.len() as i64);
        let staging = f.root.join("media_cache").join(".staging");
        std::fs::write(staging.join("orphan.part"), b"x").unwrap();
        f.cache_admin.clear_media_cache().await.unwrap();
        assert!(!staging.exists());
        assert_eq!(
            f.cache_admin.media_cache_stats().await.unwrap().entry_count,
            0
        );

        f.cache_admin.set_media_cache_max_bytes(10).await.unwrap();
        let stats = f.cache_admin.media_cache_stats().await.unwrap();
        assert_eq!(stats.max_bytes, 10);
        assert!(!stats.max_bytes_is_default);
    }

    /// 发送方选中的一张图：PNG 头 + 一段可区分的字节（与假网关 blob 返回的不同）。
    fn picked_image(dir: &Path, name: &str) -> (PathBuf, Vec<u8>) {
        std::fs::create_dir_all(dir).unwrap();
        let mut bytes = PNG[..8].to_vec();
        bytes.extend((0..4096u32).map(|i| (i % 251) as u8));
        let path = dir.join(name);
        std::fs::write(&path, &bytes).unwrap();
        (path, bytes)
    }

    /// `message.send` 上传本地来源的方式：路径来源、不带载荷。
    fn from_path(path: &Path) -> ProcessedMedia {
        ProcessedMedia {
            source: MediaSourceDescriptor::path(path.to_string_lossy().into_owned()),
            metadata: MediaMetadata::default(),
            payload: None,
        }
    }

    /// `data:` 来源解码后的上传方式：字节载荷。
    fn from_payload(name: &str, bytes: &[u8]) -> ProcessedMedia {
        let metadata = MediaMetadata {
            file_name: name.to_string(),
            mime_type: "image/png".to_string(),
            size: bytes.len() as u64,
            ..Default::default()
        };
        ProcessedMedia {
            source: MediaSourceDescriptor::bytes("data:image/png;base64,", metadata.clone()),
            metadata,
            payload: Some(bytes.to_vec()),
        }
    }

    #[tokio::test]
    async fn a_sent_local_image_resolves_to_a_local_copy() {
        let f = fixture().await;
        let (src, bytes) = picked_image(&f.root.join("相册"), "IMG_0001.png");

        // message.send 上传本地来源走的入口。
        let uploaded =
            <MediaService as MediaServicePort>::upload(&f.service, from_path(&src), None, None)
                .await
                .unwrap();
        assert!(
            uploaded.file_id.starts_with("ok-up-"),
            "{}",
            uploaded.file_id
        );

        let (access, fetch) = f
            .service
            .resolve_media_access_caching(&uploaded.file_id, 600, true)
            .await
            .unwrap();
        assert_eq!(access.source, "local");
        assert!(fetch.is_none(), "a cached image starts no fetch");
        let local = PathBuf::from(
            access
                .local_path
                .expect("发送方自己刚发的图应直接命中本地缓存"),
        );
        assert_ne!(local, src, "缓存里是一份副本，不是用户的原文件");
        assert_eq!(std::fs::read(&local).unwrap(), bytes);
        // 原文件原样留在原处。
        assert_eq!(std::fs::read(&src).unwrap(), bytes);
        let entry = f
            .cache_admin
            .get_cached(&uploaded.file_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.mime_type, "image/png");
        assert_eq!(entry.size_bytes, bytes.len() as i64);

        // 直接调 media.upload_image 也一样。
        let (src2, bytes2) = picked_image(&f.root.join("相册"), "IMG_0002.png");
        let direct = f
            .service
            .upload_image_from_path_with_progress(&src2, None, None)
            .await
            .unwrap();
        let access2 = f
            .service
            .resolve_media_access(&direct.file_id, 600)
            .await
            .unwrap();
        assert_eq!(std::fs::read(access2.local_path.unwrap()).unwrap(), bytes2);
        assert_eq!(
            f.cache_admin.media_cache_stats().await.unwrap().entry_count,
            2
        );

        // 没有为自己刚发出的图再从网络下载一遍。
        assert_eq!(f.hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_uploaded_payload_resolves_to_a_local_copy() {
        let f = fixture().await;
        let mut bytes = PNG[..8].to_vec();
        bytes.extend_from_slice(b"pasted screenshot");
        let uploaded = <MediaService as MediaServicePort>::upload(
            &f.service,
            from_payload("paste.png", &bytes),
            None,
            None,
        )
        .await
        .unwrap();
        let access = f
            .service
            .resolve_media_access(&uploaded.file_id, 600)
            .await
            .unwrap();
        let local = access.local_path.expect("上传的字节应进缓存");
        assert_eq!(std::fs::read(local).unwrap(), bytes);
        assert_eq!(f.hits.load(Ordering::SeqCst), 0);
    }

    /// 每次写入都失败的缓存（磁盘满、目录不可写）：记下写入被尝试的次数。
    #[derive(Default)]
    struct FailingCache {
        writes: AtomicUsize,
    }

    #[async_trait]
    impl MediaCacheStore for FailingCache {
        async fn get_cached(&self, _file_id: &str) -> Result<Option<MediaCacheEntryVo>> {
            Ok(None)
        }

        async fn put_bytes(
            &self,
            _file_id: &str,
            _data: &[u8],
            _mime_type: &str,
        ) -> Result<MediaCacheEntryVo> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Err(FlareError::system("media cache write: disk full"))
        }

        async fn remove(&self, _file_id: &str) -> Result<()> {
            Ok(())
        }

        async fn staging_dir(&self) -> Result<PathBuf> {
            Err(FlareError::system("media cache mkdir: disk full"))
        }

        async fn put_file(
            &self,
            _file_id: &str,
            _source: &Path,
            _mime_type: &str,
            _move_source: bool,
        ) -> Result<MediaCacheEntryVo> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Err(FlareError::system("media cache write: disk full"))
        }
    }

    #[tokio::test]
    async fn a_cache_failure_never_fails_the_upload() {
        let (base, _hits) = fake_gateway().await;
        let cache = Arc::new(FailingCache::default());
        let service = MediaService::new(
            HttpClient::new(base),
            Arc::new(RwLock::new("u1".to_string())),
            None,
            Some(cache.clone() as Arc<dyn MediaCacheStore>),
            None,
            None,
        );
        let dir =
            std::env::temp_dir().join(format!("flare-media-{}", uuid::Uuid::new_v4().simple()));
        let (src, bytes) = picked_image(&dir, "IMG_0003.png");

        let uploaded =
            <MediaService as MediaServicePort>::upload(&service, from_path(&src), None, None)
                .await
                .expect("缓存写不进去也不能让发送失败");
        assert!(
            uploaded.file_id.starts_with("ok-up-"),
            "{}",
            uploaded.file_id
        );
        assert_eq!(
            cache.writes.load(Ordering::SeqCst),
            1,
            "上传成功后应尝试写缓存"
        );
        assert_eq!(std::fs::read(&src).unwrap(), bytes);

        <MediaService as MediaServicePort>::upload(
            &service,
            from_payload("paste.png", &bytes),
            None,
            None,
        )
        .await
        .expect("字节上传同理");
        assert_eq!(cache.writes.load(Ordering::SeqCst), 2);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn uploads_over_the_per_entry_limit_are_not_cached() {
        let f = fixture().await;
        let (src, _) = picked_image(&f.root.join("相册"), "big.png");
        let uploaded = UploadedMedia {
            file_id: "ok-up-big".to_string(),
            file_name: "big.png".to_string(),
            mime_type: "image/png".to_string(),
            size: 0,
            url: None,
            cdn_url: None,
        };
        f.service
            .cache_uploaded_file(&uploaded, &src, AUTO_CACHE_MAX_ENTRY_BYTES + 1)
            .await;
        assert!(
            f.cache_admin
                .get_cached("ok-up-big")
                .await
                .unwrap()
                .is_none()
        );
        f.service
            .cache_uploaded_file(&uploaded, &src, AUTO_CACHE_MAX_ENTRY_BYTES)
            .await;
        assert!(
            f.cache_admin
                .get_cached("ok-up-big")
                .await
                .unwrap()
                .is_some()
        );
        // 用户的原文件始终留在原处。
        assert!(src.exists());
    }
}
