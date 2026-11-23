//! Media adapters.
//!
//! Native platforms upload filesystem paths through `NativeFileMediaService`.
//! Web, React Native, and uni-app provide File/Blob/URI upload through their
//! host adapter and should inject a `MediaUploaderPort`.

#[cfg(not(target_arch = "wasm32"))]
mod native_download;
#[cfg(not(target_arch = "wasm32"))]
mod native_file_service;
mod profile;
mod upload_only;
mod upload_shared;
#[cfg(any(target_arch = "wasm32", test))]
mod web_file_range;
#[cfg(target_arch = "wasm32")]
mod web_http_service;

#[cfg(not(target_arch = "wasm32"))]
pub use native_file_service::MediaService;
#[cfg(not(target_arch = "wasm32"))]
pub use native_file_service::MediaService as NativeFileMediaService;
pub use profile::{MediaAdapterProfile, MediaSourceSupport};
pub use upload_only::UploadOnlyMediaService;
#[cfg(target_arch = "wasm32")]
pub use web_http_service::MediaService;
#[cfg(target_arch = "wasm32")]
pub use web_http_service::MediaService as WebMediaService;
