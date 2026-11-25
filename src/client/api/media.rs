//! 媒体 Facade — 委托 [`crate::platform::ports::media::MediaServicePort`]（上传与下载）。

use futures::future::{AbortHandle, Abortable};
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::RwLock;

use super::session_guard::SessionGuard;
#[cfg(not(target_arch = "wasm32"))]
pub use crate::application::UserFileDownloadRequest;
pub use crate::application::{
    FileDownloadProgress, FileDownloadProgressCallback, UploadPhase, UploadProgress,
    UploadProgressCallback,
};
use crate::domain::{
    MediaCacheAdmin, MediaCacheStatsVo, MediaCacheStore, UploadManifestStore, UserFileDownloadStore,
};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::domain::{UserDownloadDirectoryVo, UserFileDownloadResultVo};
use crate::infrastructure::transport::HttpClient;
use crate::model::{
    MediaAccessUrl, MediaCacheEntryVo, MediaResolvedAccess, RenderableMedia, UploadOptions,
    UploadedMedia,
};
use crate::platform::adapters::media::MediaService;
pub use crate::platform::ports::media::MessageMediaReference;
use crate::platform::ports::media::{
    MediaMetadata, MediaServicePort, MediaSourceDescriptor, MediaSourceKind, ProcessedMedia,
    UploadProgressSink,
};
use crate::shared::error::Result;

#[derive(Clone)]
pub struct MediaApi {
    handler: Arc<dyn MediaServicePort>,
    transfers: Arc<Mutex<HashMap<String, AbortHandle>>>,
    session_guard: SessionGuard,
}

struct ActiveUpload<'a> {
    transfers: &'a Mutex<HashMap<String, AbortHandle>>,
    id: String,
}

impl Drop for ActiveUpload<'_> {
    fn drop(&mut self) {
        self.transfers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
    }
}

impl MediaApi {
    pub async fn upload_recovery(
        &self,
        source_kind: &str,
    ) -> Result<crate::model::media::MediaUploadRecovery> {
        self.session_guard.ensure_active().await?;
        let source = match source_kind {
            "path" => MediaSourceKind::Path,
            "blob" => MediaSourceKind::Blob,
            "bytes" => MediaSourceKind::Bytes,
            "uri" => MediaSourceKind::Uri,
            "asset" => MediaSourceKind::Asset,
            other => MediaSourceKind::Custom(other.into()),
        };
        Ok(self.handler.upload_recovery(&source))
    }

    pub async fn resolve_attachment(
        &self,
        request: crate::model::media::ResolveAttachmentRequest,
    ) -> Result<MediaAccessUrl> {
        let media = &request.media;
        // The port and the wire carry seq as int64.
        let seq = i64::try_from(media.message_seq).map_err(|_| {
            crate::FlareError::localized(
                crate::ErrorCode::InvalidParameter,
                "message seq out of range",
            )
        })?;
        let message = MessageMediaReference::new(media.conversation_id.clone(), seq)?;
        self.get_file_url_with_reference(
            &media.file_id,
            request.expires_in.unwrap_or(3600),
            Some(&message),
        )
        .await
    }

    /// The same handle may be reused after the previous attempt returns.
    pub async fn cancel_upload(&self, transfer_id: &str) -> Result<bool> {
        self.session_guard.ensure_active().await?;
        let transfers = self.transfers.lock().unwrap_or_else(|p| p.into_inner());
        Ok(transfers.get(transfer_id).is_some_and(|handle| {
            handle.abort();
            true
        }))
    }

    /// One cancel for any transfer: an upload's `UploadOptions.transfer_id`, or a saved
    /// download's `download_key`. A cancelled download removes its temporary file and
    /// never publishes a partial file. Returns whether a running transfer was found.
    pub async fn cancel_transfer(&self, transfer_id: &str) -> Result<bool> {
        if self.cancel_upload(transfer_id).await? {
            return Ok(true);
        }
        #[cfg(not(target_arch = "wasm32"))]
        let download = self.cancel_user_file_download(transfer_id);
        #[cfg(target_arch = "wasm32")]
        let download = false; // browsers save through the host, not a core transfer
        Ok(download)
    }

    async fn upload(
        &self,
        media: ProcessedMedia,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let progress: Option<UploadProgressSink> = self
            .session_guard
            .guard_callback(on_progress)
            .map(|callback| Box::new(move |progress| callback(progress)) as _);
        let id = options.as_ref().and_then(|o| o.transfer_id.clone());
        let upload = self.handler.upload(media, options, progress);
        self.run_session_bound(async {
            let Some(id) = id else {
                return upload.await;
            };
            if id.trim() != id || id.is_empty() || id.len() > 128 {
                return Err(crate::FlareError::localized(
                    crate::ErrorCode::InvalidParameter,
                    "invalid transfer id",
                ));
            }
            let (handle, cancel) = AbortHandle::new_pair();
            {
                use std::collections::hash_map::Entry;
                let mut transfers = self.transfers.lock().unwrap_or_else(|p| p.into_inner());
                match transfers.entry(id.clone()) {
                    Entry::Vacant(slot) => {
                        slot.insert(handle);
                    }
                    Entry::Occupied(_) => {
                        return Err(crate::FlareError::localized(
                            crate::ErrorCode::InvalidParameter,
                            "upload already active",
                        ));
                    }
                }
            }
            let _active = ActiveUpload {
                transfers: &self.transfers,
                id,
            };
            Abortable::new(upload, cancel).await.map_err(|_| {
                crate::FlareError::localized(crate::ErrorCode::OperationFailed, "upload cancelled")
            })?
        })
        .await
    }

    pub(crate) fn for_session(mut self, watch: crate::client::im_client::SessionEndWatch) -> Self {
        self.session_guard.bind(watch);
        self
    }

    pub fn from_handler(handler: Arc<dyn MediaServicePort>) -> Self {
        Self {
            handler,
            transfers: Default::default(),
            session_guard: SessionGuard::disabled("media"),
        }
    }

    pub fn from_session_handler(
        handler: Arc<dyn MediaServicePort>,
        current_user_id: Arc<RwLock<String>>,
    ) -> Self {
        Self {
            handler,
            transfers: Default::default(),
            session_guard: SessionGuard::new(current_user_id, "media"),
        }
    }

    pub fn new(
        http: HttpClient,
        current_user_id: Arc<RwLock<String>>,
        upload_manifest_store: Option<Arc<dyn UploadManifestStore>>,
        media_cache_store: Option<Arc<dyn MediaCacheStore>>,
        media_cache_admin: Option<Arc<dyn MediaCacheAdmin>>,
        user_file_download_store: Option<Arc<dyn UserFileDownloadStore>>,
    ) -> Self {
        let handler: Arc<dyn MediaServicePort> = Arc::new(MediaService::new(
            http.with_core_bearer(),
            current_user_id.clone(),
            upload_manifest_store,
            media_cache_store,
            media_cache_admin,
            user_file_download_store,
        ));
        Self {
            handler,
            transfers: Default::default(),
            session_guard: SessionGuard::new(current_user_id, "media"),
        }
    }

    async fn ensure_session_active(&self) -> Result<()> {
        self.session_guard.ensure_active().await
    }

    async fn run_session_bound<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.session_guard.run(operation).await
    }

    pub async fn upload_file_from_path(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
    ) -> Result<UploadedMedia> {
        self.ensure_session_active().await?;
        self.upload_file_from_path_with_progress(path, options, None)
            .await
    }

    pub async fn upload_file_from_path_with_progress(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        self.ensure_session_active().await?;
        let source = MediaSourceDescriptor::path(path.as_ref().to_string_lossy().to_string());
        self.upload_source_with_progress(source, options, on_progress)
            .await
    }

    pub async fn upload_source(
        &self,
        source: MediaSourceDescriptor,
        options: Option<UploadOptions>,
    ) -> Result<UploadedMedia> {
        self.ensure_session_active().await?;
        self.upload_source_with_progress(source, options, None)
            .await
    }

    pub async fn upload_source_with_progress(
        &self,
        source: MediaSourceDescriptor,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let metadata = source.metadata.clone().unwrap_or_default();
        let media = ProcessedMedia {
            source,
            metadata,
            payload: None,
        };
        self.upload(media, options, on_progress).await
    }

    pub async fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        file_name: impl Into<String>,
        mime_type: impl Into<String>,
        options: Option<UploadOptions>,
    ) -> Result<UploadedMedia> {
        self.ensure_session_active().await?;
        self.upload_bytes_with_progress(bytes, file_name, mime_type, options, None)
            .await
    }

    pub async fn upload_bytes_with_progress(
        &self,
        bytes: Vec<u8>,
        file_name: impl Into<String>,
        mime_type: impl Into<String>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let metadata = MediaMetadata {
            file_name: file_name.into(),
            mime_type: mime_type.into(),
            size: bytes.len() as u64,
            ..Default::default()
        };
        let source = MediaSourceDescriptor {
            kind: MediaSourceKind::Bytes,
            locator: "memory".to_string(),
            metadata: Some(metadata.clone()),
        };
        let media = ProcessedMedia {
            source,
            metadata,
            payload: Some(bytes),
        };
        self.upload(media, options, on_progress).await
    }

    pub async fn upload_image_from_path_with_progress(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        self.ensure_session_active().await?;
        self.upload_file_from_path_with_progress(path, options, on_progress)
            .await
    }

    pub async fn upload_video_from_path_with_progress(
        &self,
        path: impl AsRef<Path>,
        options: Option<UploadOptions>,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        self.ensure_session_active().await?;
        self.upload_file_from_path_with_progress(path, options, on_progress)
            .await
    }

    pub async fn delete_file(&self, file_id: &str, hard_delete: bool) -> Result<bool> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.delete_file(file_id, hard_delete).await })
            .await
    }

    pub async fn get_file_url(&self, file_id: &str, expires_in: i32) -> Result<MediaAccessUrl> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.get_file_url(file_id, expires_in).await })
            .await
    }

    pub async fn get_file_url_with_reference(
        &self,
        file_id: &str,
        expires_in: i32,
        reference: Option<&crate::platform::ports::media::MessageMediaReference>,
    ) -> Result<MediaAccessUrl> {
        let handler = self.handler.clone();
        self.run_session_bound(async move {
            handler
                .get_file_url_with_reference(file_id, expires_in, reference)
                .await
        })
        .await
    }

    pub async fn resolve_media_access_opts_with_reference(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
        reference: Option<&crate::platform::ports::media::MessageMediaReference>,
    ) -> Result<MediaResolvedAccess> {
        let handler = self.handler.clone();
        self.run_session_bound(async move {
            handler
                .resolve_media_access_opts_with_reference(
                    file_id, expires_in, auto_cache, reference,
                )
                .await
        })
        .await
    }

    /// 网关临时直链（`download: true`，适合附件另存为）。
    pub async fn get_temp_url_for_file_download(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaAccessUrl> {
        let handler = self.handler.clone();
        self.run_session_bound(async move {
            handler
                .get_temp_url_for_file_download(file_id, expires_in)
                .await
        })
        .await
    }

    /// 取消进行中的「下载到用户下载目录」任务。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn cancel_user_file_download(&self, download_key: &str) -> bool {
        !self.session_guard.retired() && self.handler.cancel_user_file_download(download_key)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn user_download_get_subfolder(&self) -> Result<String> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.user_download_get_subfolder().await })
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn user_download_set_subfolder(&self, name: &str) -> Result<()> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.user_download_set_subfolder(name).await })
            .await
    }

    /// SQLite 中已记录的本地绝对路径（文件是否仍存在需上层校验）。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn user_download_get_saved_path(&self, download_key: &str) -> Result<Option<String>> {
        let handler = self.handler.clone();
        self.run_session_bound(
            async move { handler.user_download_get_saved_path(download_key).await },
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn user_download_delete_record(&self, download_key: &str) -> Result<()> {
        let handler = self.handler.clone();
        self.run_session_bound(
            async move { handler.user_download_delete_record(download_key).await },
        )
        .await
    }

    /// 下载到「系统下载目录 / 子目录」并写入 SQLite；进度经 `on_progress` 回调。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn download_file_to_user_downloads_folder(
        &self,
        mut request: UserFileDownloadRequest,
    ) -> Result<String> {
        request.on_progress = self.session_guard.guard_callback(request.on_progress);
        let handler = self.handler.clone();
        self.run_session_bound(async move {
            handler
                .download_file_to_user_downloads_folder(request)
                .await
        })
        .await
    }

    /// 保存到「下载位置」（用户自选目录或平台默认目录）并写入 SQLite；返回保存结果。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn download_to_user_directory(
        &self,
        mut request: UserFileDownloadRequest,
    ) -> Result<UserFileDownloadResultVo> {
        request.on_progress = self.session_guard.guard_callback(request.on_progress);
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.download_to_user_directory(request).await })
            .await
    }

    /// 「下载位置」：实际生效目录、平台默认目录、用户自选目录。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn user_download_get_directory(&self) -> Result<UserDownloadDirectoryVo> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.user_download_get_directory().await })
            .await
    }

    /// 设置用户自选的下载目录（绝对路径，须可写）；`None` 回到平台默认目录。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn user_download_set_directory(
        &self,
        directory: Option<&str>,
    ) -> Result<UserDownloadDirectoryVo> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.user_download_set_directory(directory).await })
            .await
    }

    /// 同 [`Self::resolve_media_access`]；`auto_cache` 为真时未命中的文件在后台进本地缓存。
    pub async fn resolve_media_access_opts(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
    ) -> Result<MediaResolvedAccess> {
        let handler = self.handler.clone();
        self.run_session_bound(async move {
            handler
                .resolve_media_access_opts(file_id, expires_in, auto_cache)
                .await
        })
        .await
    }

    /// 优先返回本地缓存路径，否则返回网关短时 URL。
    pub async fn resolve_media_access(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaResolvedAccess> {
        let handler = self.handler.clone();
        self.run_session_bound(
            async move { handler.resolve_media_access(file_id, expires_in).await },
        )
        .await
    }

    /// Resolve media for inline chat display without assuming a filesystem.
    pub async fn resolve_for_display(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<RenderableMedia> {
        let handler = self.handler.clone();
        self.run_session_bound(
            async move { handler.resolve_for_display(file_id, expires_in).await },
        )
        .await
    }

    /// 下载远程媒体并写入本地缓存与 SQLite 对照表（点击预览等时机调用）。
    pub async fn cache_remote_media(
        &self,
        file_id: &str,
        expires_in: i32,
    ) -> Result<MediaCacheEntryVo> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.cache_remote_media(file_id, expires_in).await })
            .await
    }

    pub async fn media_cache_stats(&self) -> Result<MediaCacheStatsVo> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.media_cache_stats().await })
            .await
    }

    pub async fn set_media_cache_max_bytes(&self, max_bytes: u64) -> Result<()> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.set_media_cache_max_bytes(max_bytes).await })
            .await
    }

    pub async fn set_media_cache_root(&self, absolute_path: Option<&str>) -> Result<()> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.set_media_cache_root(absolute_path).await })
            .await
    }

    pub async fn clear_media_cache(&self) -> Result<()> {
        let handler = self.handler.clone();
        self.run_session_bound(async move { handler.clear_media_cache().await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MediaDestinationKind, RenderableMediaKind};
    use crate::shared::error::ErrorCode;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    struct BlockingUploadService {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl MediaServicePort for BlockingUploadService {
        async fn upload(
            &self,
            _media: ProcessedMedia,
            _options: Option<UploadOptions>,
            _progress: Option<UploadProgressSink>,
        ) -> Result<UploadedMedia> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(UploadedMedia {
                file_id: "file_after_logout".to_string(),
                file_name: "after_logout.txt".to_string(),
                mime_type: "text/plain".to_string(),
                size: 5,
                url: None,
                cdn_url: None,
            })
        }
    }

    struct ResolvingMediaService;

    #[async_trait::async_trait]
    impl MediaServicePort for ResolvingMediaService {
        async fn upload(
            &self,
            _media: ProcessedMedia,
            _options: Option<UploadOptions>,
            _progress: Option<UploadProgressSink>,
        ) -> Result<UploadedMedia> {
            Err(crate::platform::ports::media::unsupported_media_operation(
                "upload",
            ))
        }

        async fn resolve_media_access(
            &self,
            file_id: &str,
            expires_in: i32,
        ) -> Result<MediaResolvedAccess> {
            assert_eq!(file_id, "file-1");
            assert_eq!(expires_in, 300);
            Ok(MediaResolvedAccess {
                source: "remote".to_string(),
                local_path: None,
                remote: Some(MediaAccessUrl {
                    url: "https://media.example/file-1".to_string(),
                    cdn_url: None,
                }),
            })
        }
    }

    #[tokio::test]
    async fn cancel_targets_only_the_admitted_upload() {
        let started = Arc::new(Notify::new());
        let api = MediaApi::from_handler(Arc::new(BlockingUploadService {
            started: started.clone(),
            release: Arc::new(Notify::new()),
        }));
        let options = UploadOptions {
            transfer_id: Some("file-1".into()),
            ..Default::default()
        };
        let inflight = api.upload_bytes(
            vec![1],
            "a",
            "application/octet-stream",
            Some(options.clone()),
        );
        let cancel = async {
            started.notified().await;
            assert!(!api.cancel_upload("file-2").await.unwrap());
            assert!(
                api.upload_bytes(vec![2], "b", "application/octet-stream", Some(options))
                    .await
                    .is_err()
            );
            assert!(api.cancel_upload("file-1").await.unwrap());
        };
        let (result, ()) = tokio::join!(inflight, cancel);
        assert!(result.is_err());
        assert!(!api.cancel_upload("file-1").await.unwrap());
    }

    #[tokio::test]
    async fn in_flight_upload_fails_when_session_changes_before_completion() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let current_user_id = Arc::new(RwLock::new("alice".to_string()));
        let api = MediaApi::from_session_handler(
            Arc::new(BlockingUploadService {
                started: started.clone(),
                release: release.clone(),
            }),
            current_user_id.clone(),
        );

        let upload = tokio::spawn(async move {
            api.upload_bytes(b"hello".to_vec(), "hello.txt", "text/plain", None)
                .await
        });

        started.notified().await;
        *current_user_id.write().await = String::new();

        let result = timeout(Duration::from_secs(1), upload)
            .await
            .expect("session change should abort in-flight media operation");
        let err = result
            .expect("upload task should not panic")
            .expect_err("session change must cancel successful media result");
        assert_eq!(err.code(), Some(ErrorCode::NotConnected));
    }

    #[tokio::test]
    async fn resolve_for_display_maps_remote_access_to_renderable_media() {
        let api = MediaApi::from_handler(Arc::new(ResolvingMediaService));

        let media = api
            .resolve_for_display("file-1", 300)
            .await
            .expect("display media");

        assert_eq!(media.file_id, "file-1");
        assert_eq!(media.destination.kind, MediaDestinationKind::InlineDisplay);
        assert_eq!(media.kind, RenderableMediaKind::RemoteUrl);
        assert_eq!(
            media.render_url.as_deref(),
            Some("https://media.example/file-1")
        );
    }
}
