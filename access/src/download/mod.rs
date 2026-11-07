//! 原生端的制品下载（§8.8）：多地址故障转移、Range 续传、流式 SHA-256。
//!
//! 浏览器不下载安装包，这个模块在 wasm 下不编译。网络能力经 [`ArtifactTransport`] 由引擎注入。
//! [`VerifiedArtifact`] 只能由这里构造：下载并校验通过才有，安装计划也只能从它产生。

mod resumable;
mod space;
mod verify;

pub use resumable::{
    ArtifactStream, ArtifactTransport, DownloadError, DownloadObserver, MIN_PROGRESS_INTERVAL_MS,
    ProgressGate, SPACE_FACTOR, TransportError, download,
};
pub use space::available_bytes;
pub use verify::{VerifiedArtifact, verify_existing};
