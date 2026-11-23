//! Browser media service: gateway HTTP direct upload + file URL APIs.
//!
//! WASM intentionally does **not** enable [`MediaCacheStore`]: browsers already cache HTTP
//! responses, and persisting blobs in IndexedDB duplicates storage without a usable `file://`
//! path. Use [`MediaService::get_file_url`] / [`MediaService::resolve_media_access`] instead.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use js_sys::Uint8Array;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, Request, RequestCredentials, RequestInit, RequestRedirect, Response};

use super::upload_shared::{
    bind_upload_purpose, build_control_headers, build_upload_metadata, build_upload_parts,
    compute_bytes_fingerprints, compute_head_tail_fingerprints, infer_file_type, random_upload_id,
    single_put_progress, upload_file_to_uploaded_media,
};
use super::web_file_range::{MAX_WEB_FILE_SIZE, WEB_FILE_PART_SIZE, WebFileRange};
use crate::application::callbacks::{UploadPhase, UploadProgress, UploadProgressCallback};
use crate::domain::DirectUploadTransportKindVo;
use crate::infrastructure::transport::http::http_client::is_core_bearer_expired;
use crate::infrastructure::transport::{
    CommitDirectUploadPartsHttpRequest, CommitDirectUploadPartsHttpResponse,
    CompleteDirectUploadHttpRequest, DeleteFileHttpRequest, DeleteFileHttpResponse,
    DirectUploadTransportKindHttp, FileInfoHttpResponse, GetDirectUploadStatusHttpResponse,
    GetFileUrlHttpRequest, GetFileUrlHttpResponse, HttpApiResponse, HttpClient,
    InitiateDirectUploadHttpRequest, InitiateDirectUploadHttpResponse,
    PresignDirectUploadPartsHttpRequest, PresignDirectUploadPartsHttpResponse,
    PresignedUploadPartHttp, UploadFileHttpResponse, UploadedPartInfoHttp, unwrap_api_response,
};
use crate::model::{MediaAccessUrl, MediaResolvedAccess, UploadOptions, UploadedMedia};
use crate::platform::ports::media::{
    MediaMetadata, MediaProcessorPort, MediaServicePort, MediaSourceDescriptor, MediaSourceKind,
    MediaUploaderPort, ProcessedMedia, UploadProgressSink,
};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::delay;

const PRESIGN_BATCH_SIZE: usize = 8;
const FINGERPRINT_SAMPLE_SIZE: u64 = 1024 * 1024;
const BROWSER_FILE_DESIRED_PART_SIZE: i64 = 8 * 1024 * 1024;
const UPLOAD_RETRY_DELAYS: [u64; 7] = [1, 2, 4, 8, 16, 32, 60];

type BrowserFileKey = (String, String, &'static str);
type BrowserFileUploads = HashMap<BrowserFileKey, Arc<Mutex<BrowserFileUpload>>>;

#[derive(Default)]
struct BrowserFileUpload {
    session: Option<InitiateDirectUploadHttpResponse>,
    completing: bool,
    completed: Option<UploadedMedia>,
}

impl BrowserFileUpload {
    fn forget_expired(&mut self, error: &FlareError) {
        if upload_http_status(error) == Some(404) {
            self.session = None;
            self.completing = false;
        }
    }
}

fn upload_http_status(error: &FlareError) -> Option<u16> {
    error
        .as_localized()?
        .params?
        .get("httpStatus")?
        .parse()
        .ok()
}

fn retryable_upload_error(error: &FlareError) -> bool {
    if let Some(status) = upload_http_status(error) {
        return matches!(
            status,
            408 | 429 | 500 | 502 | 503 | 504 | 520 | 522 | 523 | 524
        ) || is_core_bearer_expired(error);
    }
    matches!(error, FlareError::System(_))
        || matches!(
            error.code(),
            Some(
                ErrorCode::NetworkUnreachable
                    | ErrorCode::NetworkUncertain
                    | ErrorCode::NetworkTimeout
                    | ErrorCode::ConnectionTimeout
                    | ErrorCode::OperationTimeout
                    | ErrorCode::ServiceUnavailable
            )
        )
}

// Replay the same upload operation so Core can attempt renewal again after a transient refresh failure.
async fn retry_upload<T>(enabled: bool, mut attempt: impl AsyncFnMut() -> Result<T>) -> Result<T> {
    for seconds in UPLOAD_RETRY_DELAYS {
        match attempt().await {
            Err(error) if enabled && retryable_upload_error(&error) => {
                delay(Duration::from_secs(seconds)).await;
            }
            result => return result,
        }
    }
    attempt().await
}

enum WebUploadSource<'a> {
    Bytes(&'a [u8]),
    Blob { locator: &'a str, size: u64 },
}

impl WebUploadSource<'_> {
    fn size(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::Blob { size, .. } => *size,
        }
    }

    async fn read_part(&self, offset: u64, len: u64) -> Result<Cow<'_, [u8]>> {
        match self {
            Self::Bytes(bytes) => {
                let range = offset.checked_add(len).and_then(|end| {
                    Some(usize::try_from(offset).ok()?..usize::try_from(end).ok()?)
                });
                range
                    .and_then(|range| bytes.get(range))
                    .map(Cow::Borrowed)
                    .ok_or_else(|| FlareError::general_error("upload part slice out of bounds"))
            }
            Self::Blob { locator, size } => read_blob_range(locator, offset, len, *size)
                .await
                .map(Cow::Owned),
        }
    }

    async fn fingerprints(&self) -> Result<(String, String, Option<String>)> {
        match self {
            Self::Bytes(bytes) => Ok(compute_bytes_fingerprints(bytes)),
            Self::Blob { size, .. } => {
                let sample = (*size).min(FINGERPRINT_SAMPLE_SIZE);
                let head = self.read_part(0, sample).await?;
                let tail = self.read_part(*size - sample, sample).await?;
                let (fingerprint, _) = compute_head_tail_fingerprints(*size, &head, &tail);
                // The server uses a supplied head/tail digest as the public checksum when no
                // whole-file digest is present. Submit only the fingerprint for sampled files.
                Ok((fingerprint, String::new(), None))
            }
        }
    }
}

async fn read_blob_range(locator: &str, offset: u64, len: u64, size: u64) -> Result<Vec<u8>> {
    let invalid =
        || FlareError::localized(ErrorCode::InvalidParameter, "invalid browser file range");
    let unavailable = || {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            "browser file is no longer available",
        )
    };
    let range = WebFileRange::new(offset, len, size).ok_or_else(invalid)?;
    if !locator.starts_with("blob:") {
        return Err(invalid());
    }
    let window = web_sys::window().ok_or_else(unavailable)?;
    let headers = Headers::new().map_err(|_| invalid())?;
    headers
        .set("Range", &range.request_header())
        .map_err(|_| invalid())?;
    let init = RequestInit::new();
    init.set_headers(&headers);
    init.set_credentials(RequestCredentials::Omit);
    init.set_redirect(RequestRedirect::Error);
    let request = Request::new_with_str_and_init(locator, &init).map_err(|_| invalid())?;
    let response: Response = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|_| unavailable())?
        .dyn_into()
        .map_err(|_| unavailable())?;
    let response_headers = response.headers();
    let content_range = response_headers
        .get("Content-Range")
        .map_err(|_| invalid())?;
    let content_length = response_headers
        .get("Content-Length")
        .map_err(|_| invalid())?;
    if !range.matches_response(
        response.status(),
        content_range.as_deref(),
        content_length.as_deref(),
    ) {
        return Err(invalid());
    }
    let buffer = JsFuture::from(response.array_buffer().map_err(|_| unavailable())?)
        .await
        .map_err(|_| unavailable())?;
    let bytes = Uint8Array::new(&buffer);
    if u64::from(bytes.length()) != len {
        return Err(invalid());
    }
    Ok(bytes.to_vec())
}

#[derive(Clone)]
pub struct MediaService {
    http: HttpClient,
    current_user_id: Arc<RwLock<String>>,
    browser_uploads: Arc<Mutex<BrowserFileUploads>>,
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
            browser_uploads: Arc::default(),
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

    async fn upload_direct(
        &self,
        source: WebUploadSource<'_>,
        file_name: String,
        mime_type: String,
        options: UploadOptions,
        on_progress: Option<&UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let user_id = self.current_user_id.read().await.clone();
        let total_bytes = source.size();
        let size = i64::try_from(total_bytes)
            .map_err(|_| FlareError::general_error("upload payload too large"))?;
        // The object URL identifies an immutable browser File. Each new send gets its own URL;
        // resend of the retained message keeps that URL and can recover the same signed session.
        let browser_key = match &source {
            WebUploadSource::Blob { locator, .. } => Some((
                user_id.clone(),
                (*locator).to_owned(),
                options.purpose.as_str(),
            )),
            WebUploadSource::Bytes(_) => None,
        };
        let slot = if let Some(key) = &browser_key {
            Some(
                self.browser_uploads
                    .lock()
                    .await
                    .entry(key.clone())
                    .or_default()
                    .clone(),
            )
        } else {
            None
        };
        let mut saved = if let Some(slot) = &slot {
            Some(slot.lock().await)
        } else {
            None
        };
        self.ensure_upload_user(&user_id).await?;
        if let Some(uploaded) = saved.as_ref().and_then(|state| state.completed.as_ref()) {
            return Ok(uploaded.clone());
        }
        let init = if let Some(init) = saved.as_ref().and_then(|state| state.session.as_ref()) {
            init.clone()
        } else {
            let file_type = infer_file_type(&mime_type);
            let (file_fingerprint, head_tail_sha256, full_sha256) = source.fingerprints().await?;
            let file_fingerprint = bind_upload_purpose(file_fingerprint, options.purpose);
            let local_upload_id = random_upload_id("web");

            emit_progress(
                on_progress,
                UploadProgress {
                    file_name: file_name.clone(),
                    upload_id: local_upload_id.clone(),
                    phase: UploadPhase::Preparing,
                    uploaded_bytes: 0,
                    total_bytes,
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
                    user_id.clone(),
                    options.purpose,
                ),
                desired_part_size: if matches!(source, WebUploadSource::Blob { .. }) {
                    BROWSER_FILE_DESIRED_PART_SIZE
                } else {
                    i64::try_from(options.chunk_size)
                        .map_err(|_| FlareError::general_error("invalid part size"))?
                },
                file_fingerprint,
                head_tail_sha256,
                full_sha256: full_sha256.unwrap_or_default(),
            };
            // A lost successful initiate response cannot recover its server-generated ID. Only
            // retry a start that could not be dispatched, so a transient error never starts twins.
            let mut attempts = UPLOAD_RETRY_DELAYS.into_iter();
            let body: HttpApiResponse<InitiateDirectUploadHttpResponse> = loop {
                self.ensure_upload_user(&user_id).await?;
                match self
                    .http
                    .post_with_headers("/api/v1/medias/uploads/initiate", &req, &headers)
                    .await
                {
                    Err(error)
                        if browser_key.is_some()
                            && error.code() == Some(ErrorCode::NetworkUnreachable) =>
                    {
                        if let Some(seconds) = attempts.next() {
                            delay(Duration::from_secs(seconds)).await;
                        } else {
                            return Err(error);
                        }
                    }
                    result => break result?,
                }
            };
            let init = unwrap_api_response(body, "initiate direct upload")?;
            if !init.success {
                return Err(FlareError::localized(
                    ErrorCode::GeneralError,
                    init.error_message
                        .unwrap_or_else(|| "initiate direct upload failed".to_string()),
                ));
            }
            if let Some(state) = saved.as_mut()
                && matches!(
                    init.transport_kind,
                    DirectUploadTransportKindHttp::MultipartPut
                )
            {
                state.session = Some(init.clone());
            }
            init
        };

        let upload_id = init.upload_id.clone();
        let transport = match &init.transport_kind {
            DirectUploadTransportKindHttp::SinglePut => DirectUploadTransportKindVo::SinglePut,
            DirectUploadTransportKindHttp::MultipartPut => {
                DirectUploadTransportKindVo::MultipartPut
            }
        };

        match transport {
            DirectUploadTransportKindVo::SinglePut => {
                let upload_url = init.upload_url.as_deref().ok_or_else(|| {
                    FlareError::localized(ErrorCode::GeneralError, "single put upload_url missing")
                })?;
                emit_progress(
                    on_progress,
                    UploadProgress {
                        file_name: file_name.clone(),
                        upload_id: upload_id.clone(),
                        phase: UploadPhase::Uploading,
                        uploaded_bytes: 0,
                        total_bytes,
                        chunk_index: Some(0),
                        total_chunks: Some(1),
                    },
                );
                let mut put_headers = HashMap::new();
                put_headers.insert("Content-Type".to_string(), mime_type.clone());
                let on_sent = single_put_progress(on_progress, &file_name, &upload_id, total_bytes);
                let bytes = source.read_part(0, total_bytes).await?;
                self.http
                    .put_bytes_full_url_with_progress(upload_url, &bytes, &put_headers, on_sent)
                    .await?;
            }
            DirectUploadTransportKindVo::MultipartPut => {
                if !saved.as_ref().is_some_and(|state| state.completing) {
                    if let Err(error) = upload_multipart_bytes(
                        self,
                        &source,
                        &init,
                        &user_id,
                        &file_name,
                        on_progress,
                    )
                    .await
                    {
                        if let Some(state) = saved.as_mut() {
                            state.forget_expired(&error);
                        }
                        return Err(error);
                    }
                    if let Some(state) = saved.as_mut() {
                        state.completing = true;
                    }
                }
            }
        }

        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.clone(),
                upload_id: upload_id.clone(),
                phase: UploadPhase::Completing,
                uploaded_bytes: total_bytes,
                total_bytes,
                chunk_index: None,
                total_chunks: Some(init.total_parts.max(1)),
            },
        );

        let data = match retry_upload(browser_key.is_some(), async || {
            self.ensure_upload_user(&user_id).await?;
            self.complete_direct_upload(
                &init,
                size,
                saved.as_ref().is_some_and(|state| state.completing),
            )
            .await
        })
        .await
        {
            Ok(data) => data,
            Err(error) => {
                if let Some(state) = saved.as_mut() {
                    state.forget_expired(&error);
                }
                return Err(error);
            }
        };
        if !data.success {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                data.error_message
                    .unwrap_or_else(|| "complete direct upload failed".to_string()),
            ));
        }
        if browser_key.is_some()
            && (data.file_id != init.file_id
                || data.info.as_ref().is_some_and(|info| info.size != size))
        {
            return Err(FlareError::general_error(
                "completed upload metadata does not match the upload session",
            ));
        }

        emit_progress(
            on_progress,
            UploadProgress {
                file_name: file_name.clone(),
                upload_id,
                phase: UploadPhase::Finished,
                uploaded_bytes: total_bytes,
                total_bytes,
                chunk_index: None,
                total_chunks: Some(init.total_parts.max(1)),
            },
        );

        let uploaded = upload_file_to_uploaded_media(data, file_name, mime_type, size);
        if let Some(state) = saved.as_mut() {
            state.completed = Some(uploaded.clone());
        }
        if let Some(key) = &browser_key {
            self.browser_uploads.lock().await.remove(key);
        }
        Ok(uploaded)
    }

    async fn ensure_upload_user(&self, user_id: &str) -> Result<()> {
        if self.current_user_id.read().await.as_str() == user_id {
            Ok(())
        } else {
            Err(FlareError::localized(
                ErrorCode::NotConnected,
                "upload belongs to a different session",
            ))
        }
    }

    async fn complete_direct_upload(
        &self,
        init: &InitiateDirectUploadHttpResponse,
        size: i64,
        recover: bool,
    ) -> Result<UploadFileHttpResponse> {
        let mut headers = build_control_headers(&init.upload_id);
        if recover {
            headers.insert(
                "Idempotency-Key".to_string(),
                format!("{}-complete", init.upload_id),
            );
        }
        let complete: Result<HttpApiResponse<UploadFileHttpResponse>> = self
            .http
            .post_with_headers(
                "/api/v1/medias/uploads/complete",
                &CompleteDirectUploadHttpRequest {
                    upload_id: init.upload_id.clone(),
                },
                &headers,
            )
            .await;
        match complete {
            Err(error)
                if recover
                    && upload_http_status(&error) == Some(404)
                    && !init.file_id.is_empty() =>
            {
                // Successful finalization deletes its session. A lost response can be recovered
                // by confirming the exact file ID originally assigned to this upload by Media.
                let body: HttpApiResponse<FileInfoHttpResponse> = self
                    .http
                    .get(
                        "/api/v1/medias/file-info",
                        Some(&HashMap::from([(
                            "file_id".to_string(),
                            init.file_id.clone(),
                        )])),
                    )
                    .await?;
                let info = unwrap_api_response(body, "confirm previously completed direct upload")?;
                if info.file_id != init.file_id || info.size != size {
                    return Err(FlareError::general_error(
                        "recovered file does not match the upload session",
                    ));
                }
                Ok(UploadFileHttpResponse {
                    file_id: init.file_id.clone(),
                    url: info.url.clone(),
                    cdn_url: info.cdn_url.clone(),
                    success: true,
                    error_message: None,
                    info: Some(info),
                })
            }
            result => unwrap_api_response(result?, "complete direct upload"),
        }
    }
}

async fn upload_multipart_bytes(
    service: &MediaService,
    source: &WebUploadSource<'_>,
    init: &InitiateDirectUploadHttpResponse,
    user_id: &str,
    file_name: &str,
    on_progress: Option<&UploadProgressCallback>,
) -> Result<()> {
    let upload_id = &init.upload_id;
    let part_size = u64::try_from(init.part_size).unwrap_or_default();
    let total_parts = init.total_parts;
    let total_bytes = source.size();
    let browser_file = matches!(source, WebUploadSource::Blob { .. });
    if part_size == 0
        || total_bytes.div_ceil(part_size) != u64::from(total_parts)
        || (browser_file && part_size > WEB_FILE_PART_SIZE)
    {
        return Err(FlareError::general_error(
            "invalid direct upload part layout",
        ));
    }
    let http = &service.http;
    let headers = build_control_headers(upload_id);
    let status_query = HashMap::from([("upload_id".to_string(), upload_id.to_string())]);
    let status_body: HttpApiResponse<GetDirectUploadStatusHttpResponse> =
        retry_upload(browser_file, async || {
            service.ensure_upload_user(user_id).await?;
            http.get_with_headers(
                "/api/v1/medias/uploads/status",
                Some(&status_query),
                &headers,
            )
            .await
        })
        .await?;
    let status = unwrap_api_response(status_body, "get direct upload status")?;
    if browser_file
        && (!status.success
            || status.upload_id != *upload_id
            || status.file_id != init.file_id
            || status.part_size != init.part_size
            || u64::try_from(status.total_size).ok() != Some(total_bytes)
            || status.total_parts != total_parts
            || !matches!(
                status.transport_kind,
                DirectUploadTransportKindHttp::MultipartPut
            ))
    {
        return Err(FlareError::general_error(
            "saved browser upload does not match the server session",
        ));
    }
    let mut parts = build_upload_parts(total_bytes, part_size, total_parts, upload_id);
    for server_part in status.uploaded_parts {
        if let Some(part) = parts
            .iter_mut()
            .find(|part| part.part_number == server_part.part_number)
        {
            if browser_file
                && (part.uploaded
                    || server_part.etag.trim().is_empty()
                    || u64::try_from(server_part.size).ok() != Some(part.size)
                    || !server_part.sha256.as_deref().is_some_and(|digest| {
                        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                    }))
            {
                return Err(FlareError::general_error(
                    "invalid committed browser file part",
                ));
            }
            part.uploaded = true;
            part.etag = Some(server_part.etag);
        } else if browser_file {
            return Err(FlareError::general_error(
                "invalid committed browser file part number",
            ));
        }
    }

    let missing: Vec<_> = parts.iter().filter(|part| !part.uploaded).collect();

    let mut uploaded_bytes = parts
        .iter()
        .filter(|part| part.uploaded)
        .map(|part| part.size)
        .sum::<u64>();

    for batch in missing.chunks(PRESIGN_BATCH_SIZE) {
        let presigned_map = if browser_file {
            HashMap::new()
        } else {
            presign_upload_parts(
                http,
                upload_id,
                batch.iter().map(|part| part.part_number).collect(),
                &headers,
            )
            .await?
        };

        for part in batch {
            emit_progress(
                on_progress,
                UploadProgress {
                    file_name: file_name.to_string(),
                    upload_id: upload_id.to_string(),
                    phase: UploadPhase::Uploading,
                    uploaded_bytes,
                    total_bytes,
                    chunk_index: Some(part.part_number - 1),
                    total_chunks: Some(total_parts),
                },
            );
            let data = source.read_part(part.offset, part.size).await?;
            let sha256 = hex::encode(Sha256::digest(&data));
            let missing_url = || FlareError::general_error("missing presigned url for upload part");
            let headers_map = if browser_file {
                retry_upload(true, async || {
                    service.ensure_upload_user(user_id).await?;
                    // Sign immediately before each attempt. Other slow parts cannot age this URL.
                    let mut presigned =
                        presign_upload_parts(http, upload_id, vec![part.part_number], &headers)
                            .await?;
                    let part = presigned
                        .remove(&part.part_number)
                        .ok_or_else(missing_url)?;
                    http.put_browser_file_part(&part.upload_url, &data, &part.headers)
                        .await
                })
                .await?
            } else {
                let part = presigned_map
                    .get(&part.part_number)
                    .ok_or_else(missing_url)?;
                http.put_bytes_full_url(&part.upload_url, &data, &part.headers)
                    .await?
            };
            drop(data);
            let etag = headers_map
                .get("etag")
                .cloned()
                .or_else(|| headers_map.get("ETag").cloned())
                .ok_or_else(|| FlareError::general_error("object storage response missing ETag"))?;
            let mut commit_headers = headers.clone();
            if browser_file {
                let etag_key = hex::encode(Sha256::digest(etag.as_bytes()));
                commit_headers.insert(
                    "Idempotency-Key".into(),
                    format!("{upload_id}-{}-{etag_key}", part.part_number),
                );
            }
            let commit = CommitDirectUploadPartsHttpRequest {
                upload_id: upload_id.to_string(),
                parts: vec![UploadedPartInfoHttp {
                    part_number: part.part_number,
                    etag,
                    size: part.size as i64,
                    sha256: Some(sha256),
                }],
            };
            let commit_body: HttpApiResponse<CommitDirectUploadPartsHttpResponse> =
                retry_upload(browser_file, async || {
                    service.ensure_upload_user(user_id).await?;
                    http.post_with_headers(
                        "/api/v1/medias/uploads/commit-parts",
                        &commit,
                        &commit_headers,
                    )
                    .await
                })
                .await?;
            let committed = unwrap_api_response(commit_body, "commit direct upload part")?;
            if browser_file
                && (!committed.success || !committed.committed_parts.contains(&part.part_number))
            {
                return Err(FlareError::general_error(
                    "server did not commit the browser file part",
                ));
            }
            uploaded_bytes = uploaded_bytes.saturating_add(part.size);
        }
    }
    Ok(())
}

async fn presign_upload_parts(
    http: &HttpClient,
    upload_id: &str,
    part_numbers: Vec<u32>,
    headers: &HashMap<String, String>,
) -> Result<HashMap<u32, PresignedUploadPartHttp>> {
    let body: HttpApiResponse<PresignDirectUploadPartsHttpResponse> = http
        .post_with_headers(
            "/api/v1/medias/uploads/presign-parts",
            &PresignDirectUploadPartsHttpRequest {
                upload_id: upload_id.to_string(),
                part_numbers,
                expires_in: 3600,
            },
            headers,
        )
        .await?;
    Ok(unwrap_api_response(body, "presign direct upload parts")?
        .parts
        .into_iter()
        .map(|part| (part.part_number, part))
        .collect())
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
        let source = if let Some(bytes) = media.payload.as_deref() {
            WebUploadSource::Bytes(bytes)
        } else if matches!(media.source.kind, MediaSourceKind::Blob)
            && (1..=MAX_WEB_FILE_SIZE).contains(&media.metadata.size)
        {
            WebUploadSource::Blob {
                locator: &media.source.locator,
                size: media.metadata.size,
            }
        } else {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "web media upload requires bytes or a browser file of up to 10 GiB",
            ));
        };
        let options = options.unwrap_or_default();
        let progress_cb: Option<UploadProgressCallback> =
            progress.map(|sink| Arc::new(sink) as UploadProgressCallback);
        self.upload_direct(
            source,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::transport::http::http_error_from_response_status;

    #[test]
    fn retries_network_failures_without_retrying_hard_rejections() {
        for status in [408, 429, 500, 502, 503, 504, 520, 522, 523, 524] {
            assert!(retryable_upload_error(&http_error_from_response_status(
                status, ""
            )));
        }
        for status in [400, 401, 403, 404, 413, 422] {
            assert!(!retryable_upload_error(&http_error_from_response_status(
                status, ""
            )));
        }
        for code in [
            ErrorCode::NetworkUnreachable,
            ErrorCode::NetworkUncertain,
            ErrorCode::OperationTimeout,
        ] {
            assert!(retryable_upload_error(&FlareError::localized(code, "test")));
        }
        assert!(!retryable_upload_error(&FlareError::localized(
            ErrorCode::InvalidParameter,
            "test"
        )));
    }

    #[test]
    fn retries_only_expired_core_credentials() {
        for (reason, should_retry) in [
            ("TOKEN_EXPIRED", true),
            ("TOKEN_REVOKED", false),
            ("UNAUTHORIZED", false),
        ] {
            let body = serde_json::json!({"code": 401, "reason": reason, "message": ""});
            let error = http_error_from_response_status(401, &body.to_string());
            assert_eq!(retryable_upload_error(&error), should_retry, "{reason}");
        }
    }
}
