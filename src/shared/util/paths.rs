//! 数据目录、`dataUrl` 解析、按用户落库路径等 **无状态工具**（不依赖 `IMClient`）。

use std::path::PathBuf;

use sha2::{Digest, Sha256};

use crate::FlareError;
use crate::shared::error::ErrorCode;
use crate::shared::error::Result;

/// 将 `user_id` 转为可安全用作目录名的片段。
pub fn sanitize_user_id_for_dir(user_id: &str) -> String {
    let s = user_id
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ if c.is_control() => '_',
            _ => c,
        })
        .collect::<String>();
    if s.is_empty() {
        "default".to_string()
    } else {
        s
    }
}

/// 开发时相对工作目录的数据根（`temp-data`），供示例 / Tauri dev 使用。
pub fn dev_data_dir_relative_to_cwd() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let base = if cwd
        .file_name()
        .is_some_and(|n| n == std::ffi::OsStr::new("src-tauri"))
    {
        cwd.join("..").join("..")
    } else {
        cwd.join("..")
    };
    base.join("temp-data")
}

/// SDK 默认数据根。宿主未显式传入 `dataUrl` 时使用。
pub fn default_sdk_data_root() -> PathBuf {
    let base = default_system_data_dir();
    base.join("flare-im-core-engine")
}

#[cfg(not(target_arch = "wasm32"))]
fn default_system_data_dir() -> PathBuf {
    dirs::data_dir()
        .or_else(|| dirs::home_dir().map(|home| home.join(".local").join("share")))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

#[cfg(target_arch = "wasm32")]
fn default_system_data_dir() -> PathBuf {
    PathBuf::from(".")
}

/// 将 `init` 传入的 `dataUrl`（如 `file:///path`）解析为本地路径。
pub fn parse_data_url_to_path(data_url: &str) -> Result<PathBuf> {
    let t = data_url.trim();
    if t.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "data_url is empty",
        ));
    }
    if let Some(rest) = t.strip_prefix("file://") {
        #[cfg(not(target_arch = "wasm32"))]
        if let Ok(url) = url::Url::parse(t)
            && url.scheme() == "file"
            && let Ok(path) = url.to_file_path()
        {
            return Ok(path);
        }

        let rest = rest.trim();
        #[cfg(windows)]
        {
            let s = rest.trim_start_matches('/');
            if s.len() >= 2 && s.as_bytes().get(1) == Some(&b':') {
                return Ok(PathBuf::from(s));
            }
        }
        return Ok(PathBuf::from(rest));
    }
    Ok(PathBuf::from(t))
}

/// 解析 SDK 数据根：宿主传入 base path 则使用；未传或为空则使用系统默认应用数据目录。
pub fn resolve_sdk_data_root(data_url: Option<&str>) -> Result<PathBuf> {
    if let Some(url) = data_url.map(str::trim).filter(|s| !s.is_empty()) {
        parse_data_url_to_path(url)
    } else {
        Ok(default_sdk_data_root())
    }
}

/// Opaque partition for local data; length prefixes prevent ambiguous identities.
pub fn local_storage_scope(namespace: &str, tenant_id: &str, user_id: &str) -> String {
    let mut digest = Sha256::new();
    let tenant_id = super::normalize_tenant_id(tenant_id);
    for segment in [namespace.trim(), tenant_id.as_str(), user_id.trim()] {
        digest.update((segment.len() as u64).to_be_bytes());
        digest.update(segment.as_bytes());
    }
    format!("v2-{}", hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_storage_partition_separates_namespace_tenant_and_user() {
        let a = local_storage_scope("core", "app-a", "same-user");
        assert_ne!(a, local_storage_scope("core", "app-b", "same-user"));
        assert_ne!(a, local_storage_scope("social", "app-a", "same-user"));
        assert_ne!(a, local_storage_scope("core", "app-a", "other-user"));
        assert_ne!(
            local_storage_scope("a:b", "c", "u"),
            local_storage_scope("a", "b:c", "u")
        );
        assert_ne!(
            local_storage_scope("core", "a/b", "u"),
            local_storage_scope("core", "a_b", "u")
        );
        assert_eq!(a, local_storage_scope(" core ", "app-a", " same-user "));
        assert_eq!(
            local_storage_scope("core", "default", "u"),
            local_storage_scope("core", "0", "u")
        );
    }

    #[test]
    fn parse_file_data_url_decodes_encoded_path() {
        let path = parse_data_url_to_path("file:///tmp/Application%20Support/flare_im_core_sdk")
            .expect("parse file url");

        assert_eq!(path, PathBuf::from("/tmp/Application Support/flare_im_core_sdk"));
    }
}
