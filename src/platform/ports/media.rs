//! Media ports.
//!
//! Media is platform-specific at the edges: browsers provide File/Blob,
//! mobile platforms provide URI or asset identifiers, and desktop/native
//! platforms often provide filesystem paths. The core only depends on this
//! normalized contract.

use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

use crate::application::callbacks::{UploadProgress, UserFileDownloadRequest};
use crate::domain::{MediaCacheStatsVo, UserDownloadDirectoryVo, UserFileDownloadResultVo};
use crate::model::{
    MediaAccessUrl, MediaCacheEntryVo, MediaDestinationDescriptor, MediaDestinationKind,
    MediaResolvedAccess, RenderableMedia, UploadOptions, UploadedMedia,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// A stored message attachment read; authorization remains with the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMediaReference {
    conversation_id: String,
    message_seq: i64,
}

impl MessageMediaReference {
    pub fn new(conversation_id: String, message_seq: i64) -> Result<Self> {
        if conversation_id.trim().is_empty()
            || conversation_id != conversation_id.trim()
            || conversation_id.len() > 128
            || message_seq <= 0
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "invalid message media reference",
            ));
        }
        Ok(Self {
            conversation_id,
            message_seq,
        })
    }
    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }
    pub fn message_seq(&self) -> i64 {
        self.message_seq
    }
}

#[derive(Debug, Clone, Default)]
pub struct MediaMetadata {
    pub file_name: String,
    pub mime_type: String,
    pub size: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum MediaSourceKind {
    Path,
    Uri,
    Blob,
    Bytes,
    Asset,
    Custom(String),
}

#[derive(Debug, Clone)]
pub struct MediaSourceDescriptor {
    pub kind: MediaSourceKind,
    pub locator: String,
    pub metadata: Option<MediaMetadata>,
}

impl MediaSourceDescriptor {
    pub fn path(locator: impl Into<String>) -> Self {
        Self {
            kind: MediaSourceKind::Path,
            locator: locator.into(),
            metadata: None,
        }
    }

    pub fn uri(locator: impl Into<String>) -> Self {
        Self {
            kind: MediaSourceKind::Uri,
            locator: locator.into(),
            metadata: None,
        }
    }

    pub fn blob(locator: impl Into<String>) -> Self {
        Self {
            kind: MediaSourceKind::Blob,
            locator: locator.into(),
            metadata: None,
        }
    }

    pub fn bytes(locator: impl Into<String>, metadata: MediaMetadata) -> Self {
        Self {
            kind: MediaSourceKind::Bytes,
            locator: locator.into(),
            metadata: Some(metadata),
        }
    }

    pub fn asset(locator: impl Into<String>) -> Self {
        Self {
            kind: MediaSourceKind::Asset,
            locator: locator.into(),
            metadata: None,
        }
    }

    pub fn custom(kind: impl Into<String>, locator: impl Into<String>) -> Self {
        Self {
            kind: MediaSourceKind::Custom(kind.into()),
            locator: locator.into(),
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: MediaMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

#[derive(Debug, Clone)]
pub struct ProcessedMedia {
    pub source: MediaSourceDescriptor,
    pub metadata: MediaMetadata,
    pub payload: Option<Vec<u8>>,
}

#[derive(Clone, Default)]
pub struct MediaHost {
    pub reader: Option<Arc<dyn MediaSourceReader>>,
    pub sink: Option<Arc<dyn MediaSink>>,
    pub http: Option<Arc<dyn MediaHttp>>,
    pub transcoder: Option<Arc<dyn MediaTranscoder>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaSinkCapabilities {
    pub inline_display: bool,
    pub save_to_device: bool,
    pub path: bool,
    pub bytes: bool,
}

impl MediaSinkCapabilities {
    pub fn save_to_device() -> Self {
        Self {
            inline_display: false,
            save_to_device: true,
            path: false,
            bytes: false,
        }
    }

    pub fn supports(&self, destination: &MediaDestinationDescriptor) -> bool {
        match destination.kind {
            MediaDestinationKind::InlineDisplay => self.inline_display,
            MediaDestinationKind::SaveToDevice => self.save_to_device,
            MediaDestinationKind::Path => self.path,
            MediaDestinationKind::Bytes => self.bytes,
        }
    }
}

pub struct MediaByteStream {
    pub chunks: Vec<Vec<u8>>,
}

pub struct MediaDeliverMeta {
    pub file_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub destination: MediaDestinationDescriptor,
}

#[derive(Debug, Clone)]
pub struct MediaDeliveryResult {
    pub destination: MediaDestinationDescriptor,
    pub render_url: Option<String>,
    pub saved_path: Option<String>,
    pub bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct MediaHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct MediaHttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MediaProfile {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct TranscodedMedia {
    pub media: ProcessedMedia,
    pub thumbnail: Option<Vec<u8>>,
    pub blurhash: Option<String>,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaSourceReader: Send + Sync {
    async fn inspect(&self, source: &MediaSourceDescriptor) -> Result<MediaMetadata>;
    async fn read_part(
        &self,
        source: &MediaSourceDescriptor,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaSink: Send + Sync {
    async fn deliver(
        &self,
        stream: MediaByteStream,
        meta: MediaDeliverMeta,
    ) -> Result<MediaDeliveryResult>;

    fn capabilities(&self) -> MediaSinkCapabilities;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaHttp: Send + Sync {
    async fn send(&self, request: MediaHttpRequest) -> Result<MediaHttpResponse>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaTranscoder: Send + Sync {
    async fn process(
        &self,
        source: MediaSourceDescriptor,
        profile: MediaProfile,
    ) -> Result<TranscodedMedia>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaProcessorPort: Send + Sync {
    async fn inspect(&self, source: &MediaSourceDescriptor) -> Result<MediaMetadata>;

    async fn prepare_upload(
        &self,
        source: MediaSourceDescriptor,
        options: Option<UploadOptions>,
    ) -> Result<ProcessedMedia>;
}

pub type UploadProgressSink = Box<dyn Fn(UploadProgress) + Send + Sync>;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaUploaderPort: Send + Sync {
    async fn upload(
        &self,
        media: ProcessedMedia,
        options: Option<UploadOptions>,
        progress: Option<UploadProgressSink>,
    ) -> Result<UploadedMedia>;
}

/// High-level media service port consumed by client facades and message
/// use-cases.
///
/// Platform adapters may implement only upload and return the default
/// unsupported errors for cache/download management, but the core never needs
/// to know whether the implementation is native files, Web Blob, RN URI,
/// uni-app temp file, Android content URI, iOS asset, or Flutter plugin IO.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MediaServicePort: Send + Sync {
    async fn upload(
        &self,
        media: ProcessedMedia,
        options: Option<UploadOptions>,
        progress: Option<UploadProgressSink>,
    ) -> Result<UploadedMedia>;

    async fn delete_file(&self, _file_id: &str, _hard_delete: bool) -> Result<bool> {
        Err(unsupported_media_operation("delete_file"))
    }

    async fn get_file_url(&self, _file_id: &str, _expires_in: i32) -> Result<MediaAccessUrl> {
        Err(unsupported_media_operation("get_file_url"))
    }

    async fn get_file_url_with_reference(
        &self,
        file_id: &str,
        expires_in: i32,
        reference: Option<&MessageMediaReference>,
    ) -> Result<MediaAccessUrl> {
        if reference.is_some() {
            return Err(unsupported_media_operation("message media read"));
        }
        self.get_file_url(file_id, expires_in).await
    }

    async fn get_temp_url_for_file_download(
        &self,
        _file_id: &str,
        _expires_in: i32,
    ) -> Result<MediaAccessUrl> {
        Err(unsupported_media_operation(
            "get_temp_url_for_file_download",
        ))
    }

    async fn resolve_media_access(
        &self,
        _file_id: &str,
        _expires_in: i32,
    ) -> Result<MediaResolvedAccess> {
        Err(unsupported_media_operation("resolve_media_access"))
    }

    /// 同 [`Self::resolve_media_access`]；`auto_cache` 为真且未命中时，实现可在后台把文件拉进本地缓存。
    async fn resolve_media_access_opts(
        &self,
        file_id: &str,
        expires_in: i32,
        _auto_cache: bool,
    ) -> Result<MediaResolvedAccess> {
        self.resolve_media_access(file_id, expires_in).await
    }

    async fn resolve_media_access_opts_with_reference(
        &self,
        file_id: &str,
        expires_in: i32,
        auto_cache: bool,
        reference: Option<&MessageMediaReference>,
    ) -> Result<MediaResolvedAccess> {
        if reference.is_none() {
            return self
                .resolve_media_access_opts(file_id, expires_in, auto_cache)
                .await;
        }
        Ok(MediaResolvedAccess {
            source: "remote".into(),
            local_path: None,
            remote: Some(
                self.get_file_url_with_reference(file_id, expires_in, reference)
                    .await?,
            ),
        })
    }

    async fn resolve_for_display(&self, file_id: &str, expires_in: i32) -> Result<RenderableMedia> {
        let resolved = self.resolve_media_access(file_id, expires_in).await?;
        Ok(RenderableMedia::from_resolved_access(file_id, resolved))
    }

    async fn cache_remote_media(
        &self,
        _file_id: &str,
        _expires_in: i32,
    ) -> Result<MediaCacheEntryVo> {
        Err(unsupported_media_operation("cache_remote_media"))
    }

    async fn media_cache_stats(&self) -> Result<MediaCacheStatsVo> {
        Err(unsupported_media_operation("media_cache_stats"))
    }

    async fn set_media_cache_max_bytes(&self, _max_bytes: u64) -> Result<()> {
        Err(unsupported_media_operation("set_media_cache_max_bytes"))
    }

    async fn set_media_cache_root(&self, _absolute_path: Option<&str>) -> Result<()> {
        Err(unsupported_media_operation("set_media_cache_root"))
    }

    async fn clear_media_cache(&self) -> Result<()> {
        Err(unsupported_media_operation("clear_media_cache"))
    }

    fn cancel_user_file_download(&self, _download_key: &str) -> bool {
        false
    }

    async fn user_download_get_subfolder(&self) -> Result<String> {
        Err(unsupported_media_operation("user_download_get_subfolder"))
    }

    async fn user_download_set_subfolder(&self, _name: &str) -> Result<()> {
        Err(unsupported_media_operation("user_download_set_subfolder"))
    }

    async fn user_download_get_saved_path(&self, _download_key: &str) -> Result<Option<String>> {
        Err(unsupported_media_operation("user_download_get_saved_path"))
    }

    async fn user_download_delete_record(&self, _download_key: &str) -> Result<()> {
        Err(unsupported_media_operation("user_download_delete_record"))
    }

    async fn download_file_to_user_downloads_folder(
        &self,
        _request: UserFileDownloadRequest,
    ) -> Result<String> {
        Err(unsupported_media_operation(
            "download_file_to_user_downloads_folder",
        ))
    }

    async fn download_to_user_directory(
        &self,
        _request: UserFileDownloadRequest,
    ) -> Result<UserFileDownloadResultVo> {
        Err(unsupported_media_operation("download_to_user_directory"))
    }

    async fn user_download_get_directory(&self) -> Result<UserDownloadDirectoryVo> {
        Err(unsupported_media_operation("user_download_get_directory"))
    }

    async fn user_download_set_directory(
        &self,
        _directory: Option<&str>,
    ) -> Result<UserDownloadDirectoryVo> {
        Err(unsupported_media_operation("user_download_set_directory"))
    }
}

pub fn unsupported_media_operation(operation: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::OperationNotSupported,
        format!("{operation} is not supported by the configured media adapter"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MediaDestinationDescriptor;

    #[test]
    fn sink_capabilities_match_destination_descriptors() {
        let save_only = MediaSinkCapabilities::save_to_device();

        assert!(save_only.supports(&MediaDestinationDescriptor::save_to_device()));
        assert!(!save_only.supports(&MediaDestinationDescriptor::inline_display()));
        assert!(!save_only.supports(&MediaDestinationDescriptor::bytes()));
    }
}
