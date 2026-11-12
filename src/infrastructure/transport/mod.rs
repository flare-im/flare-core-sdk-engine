//! 网络层（Network）— IO 与逻辑分离，仅负责连接与帧收发。

pub mod http;
pub mod socket;

pub use http::{
    AbortDirectUploadHttpRequest, CommitDirectUploadPartsHttpRequest,
    CommitDirectUploadPartsHttpResponse, CompleteDirectUploadHttpRequest, DeleteFileHttpRequest,
    DeleteFileHttpResponse, DirectUploadTransportKindHttp, FileInfoHttpResponse,
    GetDirectUploadStatusHttpResponse, GetFileUrlHttpRequest, GetFileUrlHttpResponse,
    HttpApiResponse, HttpClient, HttpCredentialLease, HttpCredentialOwner, HttpCredentialSource,
    HttpCredentials, HttpRequestContext, InitiateDirectUploadHttpRequest,
    InitiateDirectUploadHttpResponse, PresignDirectUploadPartsHttpRequest,
    PresignDirectUploadPartsHttpResponse, PresignedUploadPartHttp, SentBytesCallback,
    UploadFileHttpResponse, UploadFileMetadataHttp, UploadedPartInfoHttp, unwrap_api_response,
    unwrap_optional_api_response, unwrap_void_api_response,
};
pub use socket::{SocketHandler, SocketTransport};
