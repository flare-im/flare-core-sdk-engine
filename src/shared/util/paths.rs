//! 数据目录、`dataUrl` 解析、按用户落库路径等 **无状态工具**（不依赖 `IMClient`）。

use std::path::PathBuf;

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
    base.join("flare-core-sdk-engine")
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

/// `data_root` 下按用户隔离的 SQLite 文件路径（`users/{sanitized_user_id}/flare_im_sdk.db`）。
///
/// 用户目录统一放在 `users/` 命名空间下，避免用户 ID 与 SDK 根目录下的其他文件或旧异常文件冲突。
pub fn resolve_user_db_path(base: &std::path::Path, user_id: &str) -> PathBuf {
    let user_dir = sanitize_user_id_for_dir(user_id);
    let user_data_dir = base.join("users").join(user_dir);
    user_data_dir.join("flare_im_sdk.db")
}

/// 用户媒体缓存目录（`{user_data}/media_cache`），与 [`resolve_user_db_path`] 同级。
pub fn resolve_user_media_cache_dir(base: &std::path::Path, user_id: &str) -> PathBuf {
    let db = resolve_user_db_path(base, user_id);
    resolve_media_cache_dir_next_to_db(&db)
}

/// 与 SQLite 数据库文件**同目录**下的 `media_cache`（默认缓存根，与库文件并列）。
pub fn resolve_media_cache_dir_next_to_db(db_file: &std::path::Path) -> PathBuf {
    let parent = db_file
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    parent.join("media_cache")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_file_data_url_decodes_encoded_path() {
        let path = parse_data_url_to_path("file:///tmp/Application%20Support/flare_im_sdk")
            .expect("parse file url");

        assert_eq!(path, PathBuf::from("/tmp/Application Support/flare_im_sdk"));
    }
}
