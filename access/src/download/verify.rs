//! 校验通过的制品。

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use super::resumable::{DownloadError, READ_CHUNK_BYTES};
use crate::release::{ArtifactRecord, DownloadSpec, PackagePlan, UpdateInstallPlan};

/// 下载完成并通过哈希校验的文件。字段私有，也没有公开构造函数：平台安装器只接受这一种东西。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArtifact {
    path: PathBuf,
    sha256: String,
    signing_cert_sha256: Option<String>,
    build: u64,
    size_bytes: u64,
}

impl VerifiedArtifact {
    pub(super) fn new(path: PathBuf, spec: &DownloadSpec) -> Self {
        Self {
            path,
            sha256: spec.sha256.clone(),
            signing_cert_sha256: spec.signing_cert_sha256.clone(),
            build: spec.build,
            size_bytes: spec.size_bytes,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn build(&self) -> u64 {
        self.build
    }

    /// 存进 `update/state` 的记录。读回后要经 [`verify_existing`] 重新校验才算数。
    pub fn record(&self) -> ArtifactRecord {
        ArtifactRecord {
            path: self.path.to_string_lossy().into_owned(),
            sha256: self.sha256.clone(),
            signing_cert_sha256: self.signing_cert_sha256.clone(),
            build: self.build,
            size_bytes: self.size_bytes,
        }
    }

    /// 给平台包的安装计划。
    pub fn install_plan(&self) -> UpdateInstallPlan {
        UpdateInstallPlan::InstallPackage(PackagePlan {
            path: self.path.to_string_lossy().into_owned(),
            sha256: self.sha256.clone(),
            signing_cert_sha256: self.signing_cert_sha256.clone(),
            build: self.build,
            size_bytes: self.size_bytes,
        })
    }
}

/// 整个文件的 SHA-256（小写十六进制）。
pub(super) async fn file_sha256(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; READ_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            return Ok(hex(&hasher.finalize()));
        }
        hasher.update(&buffer[..read]);
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// 重新校验已下载的文件。下载和安装之间可能隔了几天，这期间文件可能被改过或删了。
pub async fn verify_existing(record: &ArtifactRecord) -> Result<VerifiedArtifact, DownloadError> {
    let path = PathBuf::from(&record.path);
    let actual = file_sha256(&path)
        .await
        .map_err(|error| DownloadError::Io(error.to_string()))?;
    if actual != record.sha256 {
        return Err(DownloadError::HashMismatch);
    }
    Ok(VerifiedArtifact {
        path,
        sha256: record.sha256.clone(),
        signing_cert_sha256: record.signing_cert_sha256.clone(),
        build: record.build,
        size_bytes: record.size_bytes,
    })
}
