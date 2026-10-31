//! SQLite `StoreProvider` 构建（`feature = "lifecycle-sqlite"`）。
//!
//! 与 [`crate::client::IMClient::login`] 配合：在 `LoginDbKind::Sqlite` 分支由 `im_client` 调用 [`open_sqlite_store_for_user`]；
//! 也可直接用 [`open_sqlite_store_provider`] 传入任意 `database_url`。

use std::sync::{Arc, OnceLock};

use flare_im_core_sdk_storage_sqlite::{
    SqliteSecurityConfig, database_url_from_path, open_pool, open_pool_with_security,
    register_schema_init_with,
};

use crate::FlareError;
use crate::domain::{MediaCacheAdmin, MediaCacheStore, UserFileDownloadStore};
use crate::infrastructure::persistence::{
    SqliteConversationParticipantRepo, SqliteConversationRepo, SqliteMediaCacheRepo,
    SqliteMessageRepo, SqlitePendingSendRepo, SqliteSyncCursorRepo, SqliteTimelineCoverageRepo,
    SqliteUploadManifestRepo, SqliteUserFileDownloadRepo, SqliteUserProfileRepo, StoreProvider,
    sqlite_init_schema,
};
use crate::platform::ports::storage::{
    SecureKeyDescriptor, SecureKeyStore, load_or_create_local_database_key,
};
use crate::shared::error::ErrorCode;
use crate::shared::error::Result;
use crate::shared::util::paths::{resolve_media_cache_dir_next_to_db, resolve_user_db_path};

static CORE_SCHEMA: OnceLock<()> = OnceLock::new();

/// 注册 IM 核心 SQLite 表结构（与社交 schema 一并注册后，共享同一 `open_pool` 连接）。
pub fn ensure_core_sqlite_schema_registered() {
    CORE_SCHEMA.get_or_init(|| {
        register_schema_init_with("flare_im_core_schema", |pool| {
            let pool = pool.clone();
            async move {
                sqlite_init_schema(&pool)
                    .await
                    .map_err(|e| anyhow::anyhow!("{}", e))
            }
        });
    });
}

/// `sqlite:` 连接串，供 [`open_sqlite_store_provider`]。
#[inline]
pub fn sqlite_database_url_from_path(path: &std::path::Path) -> String {
    database_url_from_path(path)
}

/// 打开连接池并组装核心 SDK 所需的 SQLite 仓储集合。
///
/// `media_cache_dir`：媒体文件落盘根目录；为 `None` 时不启用本地媒体缓存（`resolve_media_access` 始终走网关）。
pub async fn open_sqlite_store_provider(
    database_url: &str,
    media_cache_dir: Option<&std::path::Path>,
) -> Result<StoreProvider> {
    open_sqlite_store_provider_with_security(
        database_url,
        media_cache_dir,
        SqliteSecurityConfig::default(),
    )
    .await
}

/// 打开连接池、应用 SQLite 安全配置并组装核心 SDK 所需的 SQLite 仓储集合。
pub async fn open_sqlite_store_provider_with_security(
    database_url: &str,
    media_cache_dir: Option<&std::path::Path>,
    security: SqliteSecurityConfig,
) -> Result<StoreProvider> {
    ensure_core_sqlite_schema_registered();
    let pool = if security.is_encryption_required() {
        open_pool_with_security(database_url, security).await
    } else {
        open_pool(database_url).await
    }
    .map_err(|e| {
        FlareError::localized(
            ErrorCode::ConfigurationError,
            format!("open_pool failed: {}", e),
        )
    })?;

    let pending_repo = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
    let upload_manifest_repo = Arc::new(SqliteUploadManifestRepo::new(pool.clone()));
    let mut user_download_repo = SqliteUserFileDownloadRepo::new(pool.clone());
    if let Some(user_dir) = media_cache_dir.and_then(|dir| dir.parent()) {
        user_download_repo = user_download_repo.with_fallback_root(user_dir.join("downloads"));
    }
    let user_download_repo = Arc::new(user_download_repo);
    let user_repo = Arc::new(SqliteUserProfileRepo::new(pool.clone()));
    let (media_cache_store, media_cache_admin) = if let Some(root) = media_cache_dir {
        let repo = Arc::new(SqliteMediaCacheRepo::create(pool.clone(), root.to_path_buf()).await?);
        (
            Some(repo.clone() as Arc<dyn MediaCacheStore>),
            Some(repo as Arc<dyn MediaCacheAdmin>),
        )
    } else {
        (None, None)
    };
    Ok(StoreProvider {
        plugin_commands: Some(Arc::new(
            crate::infrastructure::persistence::sqlite::SqlitePluginCommandRepo::new(pool.clone()),
        )),
        messages: Arc::new(SqliteMessageRepo::new(pool.clone())),
        conversations: Arc::new(SqliteConversationRepo::new(pool.clone())),
        conversation_participants: Some(Arc::new(SqliteConversationParticipantRepo::new(
            pool.clone(),
        ))),
        cursors: Arc::new(SqliteSyncCursorRepo::new(pool.clone())),
        timeline_coverage: Arc::new(SqliteTimelineCoverageRepo::new(pool.clone())),
        pending_send_reader: Some(pending_repo.clone()),
        pending_send_writer: Some(pending_repo),
        upload_manifest_store: Some(upload_manifest_repo),
        media_cache_store,
        media_cache_admin,
        user_file_download_store: Some(user_download_repo as Arc<dyn UserFileDownloadStore>),
        user_profiles_reader: Some(user_repo.clone()),
        user_profiles_writer: Some(user_repo),
    })
}

/// Builds SQLCipher security config from the platform secure key store.
pub async fn sqlite_security_config_from_secure_key_store(
    key_store: &dyn SecureKeyStore,
    descriptor: &SecureKeyDescriptor,
) -> Result<SqliteSecurityConfig> {
    let secret = load_or_create_local_database_key(key_store, descriptor).await?;
    Ok(SqliteSecurityConfig::new().with_encryption_key(hex::encode(secret.expose_secret())))
}

/// 在 `data_root` 下按用户目录打开库（路径规则见 [`crate::shared::util::paths::resolve_user_db_path`]）。
pub async fn open_sqlite_store_for_user(
    base_data_dir: &std::path::Path,
    user_id: &str,
) -> Result<StoreProvider> {
    let db_path = resolve_user_db_path(base_data_dir, user_id);
    let cache_dir = resolve_media_cache_dir_next_to_db(&db_path);
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                format!(
                    "sqlite database directory create_dir_all failed: path={}, error={}",
                    parent.display(),
                    e
                ),
            )
        })?;
    }
    tokio::fs::create_dir_all(&cache_dir).await.map_err(|e| {
        FlareError::localized(
            ErrorCode::ConfigurationError,
            format!(
                "media cache directory create_dir_all failed: path={}, error={}",
                cache_dir.display(),
                e
            ),
        )
    })?;
    tracing::info!(db = %db_path.display(), cache = %cache_dir.display(), "Opening SQLite store");
    let database_url = sqlite_database_url_from_path(&db_path);
    open_sqlite_store_provider(&database_url, Some(&cache_dir)).await
}

/// 在 `data_root` 下按用户目录打开加密 SQLite 库。
pub async fn open_sqlite_store_for_user_with_security(
    base_data_dir: &std::path::Path,
    user_id: &str,
    security: SqliteSecurityConfig,
) -> Result<StoreProvider> {
    let db_path = resolve_user_db_path(base_data_dir, user_id);
    let cache_dir = resolve_media_cache_dir_next_to_db(&db_path);
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                format!(
                    "sqlite database directory create_dir_all failed: path={}, error={}",
                    parent.display(),
                    e
                ),
            )
        })?;
    }
    tokio::fs::create_dir_all(&cache_dir).await.map_err(|e| {
        FlareError::localized(
            ErrorCode::ConfigurationError,
            format!(
                "media cache directory create_dir_all failed: path={}, error={}",
                cache_dir.display(),
                e
            ),
        )
    })?;
    tracing::info!(
        db = %db_path.display(),
        cache = %cache_dir.display(),
        encrypted = security.is_encryption_required(),
        "Opening SQLite store"
    );
    let database_url = sqlite_database_url_from_path(&db_path);
    open_sqlite_store_provider_with_security(&database_url, Some(&cache_dir), security).await
}

/// 在 `data_root` 下按用户目录打开加密 SQLite 库，并从平台 `SecureKeyStore` 首启生成/后续复用 DB key。
pub async fn open_sqlite_store_for_user_with_secure_key_store(
    base_data_dir: &std::path::Path,
    user_id: &str,
    key_namespace: &str,
    tenant_id: &str,
    key_name: Option<&str>,
    key_store: &dyn SecureKeyStore,
) -> Result<StoreProvider> {
    let descriptor = SecureKeyDescriptor::new(
        key_namespace,
        tenant_id,
        user_id,
        key_name.unwrap_or("local_database.v1"),
    );
    let security = sqlite_security_config_from_secure_key_store(key_store, &descriptor).await?;
    open_sqlite_store_for_user_with_security(base_data_dir, user_id, security).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::ports::storage::{LOCAL_DATABASE_KEY_BYTES, VolatileSecureKeyStore};

    #[tokio::test]
    async fn open_sqlite_store_for_user_supports_data_root_with_spaces() {
        let root =
            std::env::temp_dir().join(format!("flare im sdk data root {}", std::process::id()));
        {
            let _stores = open_sqlite_store_for_user(&root, "hugo")
                .await
                .expect("open store");
        }

        assert!(
            root.join("users")
                .join("hugo")
                .join("flare_im_sdk.db")
                .exists()
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn sqlite_security_config_loads_or_creates_secure_key() {
        let store = VolatileSecureKeyStore::new();
        let descriptor = SecureKeyDescriptor::local_database("flare", "0", "hugo");

        let first = sqlite_security_config_from_secure_key_store(&store, &descriptor)
            .await
            .expect("first security config");
        let second = sqlite_security_config_from_secure_key_store(&store, &descriptor)
            .await
            .expect("second security config");
        let stored = store
            .get_secret(&descriptor)
            .await
            .expect("load stored key")
            .expect("stored key");

        assert!(first.is_encryption_required());
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
        assert_eq!(stored.expose_secret().len(), LOCAL_DATABASE_KEY_BYTES);
    }
}
