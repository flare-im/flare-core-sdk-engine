//! SDK 上传/下载等进度回调类型（供 application 服务、client Facade、绑定层共用）。

use std::sync::Arc;

// --- 上传进度 ---

#[derive(Debug, Clone)]
pub enum UploadPhase {
    Preparing,
    Uploading,
    Completing,
    Finished,
}

#[derive(Debug, Clone)]
pub struct UploadProgress {
    pub file_name: String,
    pub upload_id: String,
    pub phase: UploadPhase,
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
    pub chunk_index: Option<u32>,
    pub total_chunks: Option<u32>,
}

pub type UploadProgressCallback = Arc<dyn Fn(UploadProgress) + Send + Sync>;

// --- 用户文件下载进度 ---

/// 已下载字节数；`total` 来自 Content-Length 或本地文件大小（可能为 `None`）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileDownloadProgress {
    pub downloaded: u64,
    pub total: Option<u64>,
}

pub type FileDownloadProgressCallback = Arc<dyn Fn(FileDownloadProgress) + Send + Sync>;

#[derive(Clone)]
pub struct UserFileDownloadRequest {
    pub download_key: String,
    pub display_file_name: String,
    pub source_path: Option<String>,
    pub source_http_url: Option<String>,
    pub remote_file_id: Option<String>,
    /// Optional server message reference for a recipient renewing a private
    /// attachment URL. This is a hint; the server rechecks real visibility.
    pub message_conversation_id: Option<String>,
    pub message_seq: Option<i64>,
    pub expires_in: i32,
    pub on_progress: Option<FileDownloadProgressCallback>,
}
