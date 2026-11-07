use std::collections::HashMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploadFileMetadataHttp {
    pub file_name: String,
    pub mime_type: String,
    pub file_size: i64,
    pub file_type: i32,
    pub upload_id: String,
    pub metadata: HashMap<String, String>,
    pub user_id: String,
    pub trace_id: String,
    pub namespace: String,
    pub business_tag: String,
    pub bucket: String,
    pub object_key: String,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileInfoHttpResponse {
    pub file_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub size: i64,
    pub url: Option<String>,
    pub cdn_url: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct GetFileUrlHttpRequest {
    pub file_id: String,
    pub expires_in: i32,
    pub download: bool,
    pub response_headers: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub conversation_id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub message_seq: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetFileUrlHttpResponse {
    pub url: String,
    pub cdn_url: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploadFileHttpResponse {
    pub file_id: String,
    pub url: Option<String>,
    pub cdn_url: Option<String>,
    pub success: bool,
    pub error_message: Option<String>,
    pub info: Option<FileInfoHttpResponse>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeleteFileHttpRequest {
    pub file_id: String,
    pub hard_delete: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeleteFileHttpResponse {
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectUploadTransportKindHttp {
    SinglePut,
    MultipartPut,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InitiateDirectUploadHttpRequest {
    pub metadata: UploadFileMetadataHttp,
    pub desired_part_size: i64,
    pub file_fingerprint: String,
    pub head_tail_sha256: String,
    pub full_sha256: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InitiateDirectUploadHttpResponse {
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: DirectUploadTransportKindHttp,
    pub bucket: String,
    pub object_key: String,
    pub storage_upload_id: Option<String>,
    pub part_size: i64,
    pub total_parts: u32,
    pub upload_url: Option<String>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploadedPartInfoHttp {
    pub part_number: u32,
    pub etag: String,
    pub size: i64,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetDirectUploadStatusHttpResponse {
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: DirectUploadTransportKindHttp,
    pub bucket: String,
    pub object_key: String,
    pub storage_upload_id: Option<String>,
    pub part_size: i64,
    pub total_size: i64,
    pub total_parts: u32,
    pub uploaded_parts: Vec<UploadedPartInfoHttp>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PresignDirectUploadPartsHttpRequest {
    pub upload_id: String,
    pub part_numbers: Vec<u32>,
    pub expires_in: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PresignedUploadPartHttp {
    pub part_number: u32,
    pub upload_url: String,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PresignDirectUploadPartsHttpResponse {
    pub parts: Vec<PresignedUploadPartHttp>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitDirectUploadPartsHttpRequest {
    pub upload_id: String,
    pub parts: Vec<UploadedPartInfoHttp>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitDirectUploadPartsHttpResponse {
    pub committed_parts: Vec<u32>,
    pub uploaded_size: i64,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompleteDirectUploadHttpRequest {
    pub upload_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AbortDirectUploadHttpRequest {
    pub upload_id: String,
}
