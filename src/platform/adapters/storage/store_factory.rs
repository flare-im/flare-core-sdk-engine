//! Storage adapter factory.

use std::sync::Arc;

use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::persistence::in_memory_empty_im_provider;
use crate::platform::ports::storage::SecureKeyStore;
use crate::platform::runtime::{RuntimeConfig, StorageKind};
use crate::shared::error::{ErrorCode, FlareError, Result};

pub async fn open_store_from_runtime_config(config: &RuntimeConfig) -> Result<StoreProvider> {
    open_store_from_runtime_config_with_secure_key_store(config, None).await
}

pub async fn open_store_from_runtime_config_with_secure_key_store(
    config: &RuntimeConfig,
    secure_key_store: Option<Arc<dyn SecureKeyStore>>,
) -> Result<StoreProvider> {
    match config.storage.kind {
        StorageKind::Memory => Ok(in_memory_empty_im_provider()),
        StorageKind::Sqlite => open_sqlite_stores(config, secure_key_store).await,
        StorageKind::IndexedDb => Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "IndexedDB storage must be injected by the Web storage adapter",
        )),
        StorageKind::Custom => Err(FlareError::localized(
            ErrorCode::ConfigurationError,
            "custom storage requires an injected StoreProvider",
        )),
    }
}

#[cfg(feature = "lifecycle-sqlite")]
async fn open_sqlite_stores(
    config: &RuntimeConfig,
    secure_key_store: Option<Arc<dyn SecureKeyStore>>,
) -> Result<StoreProvider> {
    use crate::platform::ports::storage::SecureKeyDescriptor;
    use crate::shared::util::sqlite_store::sqlite_security_config_from_secure_key_store;
    use std::path::PathBuf;

    use crate::shared::util::{
        default_sdk_data_root, open_sqlite_store_for_user, open_sqlite_store_for_user_with_security,
    };

    let user_id = config.storage.user_id.trim();
    if user_id.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::ConfigurationError,
            "SQLite storage requires storage.user_id",
        ));
    }
    let base_data_dir = config
        .storage
        .path
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(default_sdk_data_root);
    if config.storage.encryption.is_enabled() {
        let secure_key_store = secure_key_store.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "SQLite encryption requires a SecureKeyStore",
            )
        })?;
        let descriptor = SecureKeyDescriptor::new(
            config
                .storage
                .encryption
                .key_namespace
                .as_deref()
                .unwrap_or(&config.storage.namespace),
            &config.storage.tenant_id,
            user_id,
            config
                .storage
                .encryption
                .key_name
                .as_deref()
                .unwrap_or("local_database.v1"),
        );
        let security =
            sqlite_security_config_from_secure_key_store(secure_key_store.as_ref(), &descriptor)
                .await?;
        return open_sqlite_store_for_user_with_security(
            &base_data_dir,
            &config.storage.namespace,
            &config.storage.tenant_id,
            user_id,
            security,
        )
        .await;
    }
    open_sqlite_store_for_user(
        &base_data_dir,
        &config.storage.namespace,
        &config.storage.tenant_id,
        user_id,
    )
    .await
}

#[cfg(not(feature = "lifecycle-sqlite"))]
async fn open_sqlite_stores(
    _config: &RuntimeConfig,
    _secure_key_store: Option<Arc<dyn SecureKeyStore>>,
) -> Result<StoreProvider> {
    Err(FlareError::localized(
        ErrorCode::ConfigurationError,
        "SQLite storage requires the lifecycle-sqlite feature",
    ))
}

#[cfg(all(test, feature = "lifecycle-sqlite"))]
mod tests {
    use super::*;
    use crate::client::SdkConfig;
    use crate::platform::runtime::{
        MediaRuntimeConfig, MediaRuntimeKind, PlatformKind, StorageConfig, StorageEncryptionConfig,
    };

    fn encrypted_sqlite_config() -> RuntimeConfig {
        RuntimeConfig {
            platform: PlatformKind::Native,
            sdk: SdkConfig::default(),
            storage: StorageConfig {
                kind: StorageKind::Sqlite,
                namespace: "flare".to_string(),
                tenant_id: "0".to_string(),
                user_id: "alice".to_string(),
                path: None,
                encryption: StorageEncryptionConfig::required(),
            },
            media: MediaRuntimeConfig {
                kind: MediaRuntimeKind::Native,
                cache_namespace: None,
            },
        }
    }

    #[tokio::test]
    async fn encrypted_sqlite_runtime_requires_secure_key_store() {
        let err = match open_store_from_runtime_config_with_secure_key_store(
            &encrypted_sqlite_config(),
            None,
        )
        .await
        {
            Ok(_) => panic!("missing secure key store must fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), Some(ErrorCode::ConfigurationError));
    }

    #[tokio::test]
    async fn encryption_key_namespace_is_independent_of_data_partition_and_reuses_the_key() {
        use crate::platform::ports::storage::{
            SecureKeyDescriptor, VolatileSecureKeyStore, load_or_create_local_database_key,
        };
        for key_namespace in [None, Some("configured-vault")] {
            let root =
                std::env::temp_dir().join(format!("flare-key-namespace-{}", uuid::Uuid::new_v4()));
            let mut config = encrypted_sqlite_config();
            config.storage.path = Some(root.to_string_lossy().into());
            config.storage.namespace = "data-scope".into();
            config.storage.encryption.key_namespace = key_namespace.map(str::to_owned);
            config.storage.encryption.key_name = Some("named-key".into());
            let keys = Arc::new(VolatileSecureKeyStore::new());
            let descriptor = SecureKeyDescriptor::new(
                key_namespace.unwrap_or("data-scope"),
                "0",
                "alice",
                "named-key",
            );
            let original = load_or_create_local_database_key(keys.as_ref(), &descriptor)
                .await
                .unwrap();
            let opened =
                open_store_from_runtime_config_with_secure_key_store(&config, Some(keys.clone()))
                    .await;
            let actual = keys.get_secret(&descriptor).await.unwrap().unwrap();
            assert_eq!(actual.expose_secret(), original.expose_secret());
            let data_path =
                crate::shared::util::paths::resolve_user_db_path(&root, "data-scope", "0", "alice");
            assert!(data_path.parent().unwrap().is_dir());
            if key_namespace.is_some() {
                assert!(
                    keys.get_secret(&SecureKeyDescriptor::new(
                        "data-scope",
                        "0",
                        "alice",
                        "named-key"
                    ))
                    .await
                    .unwrap()
                    .is_none()
                );
                assert!(
                    !crate::shared::util::paths::resolve_user_db_path(
                        &root,
                        "configured-vault",
                        "0",
                        "alice"
                    )
                    .parent()
                    .unwrap()
                    .exists()
                );
            }
            drop(opened);
            tokio::fs::remove_dir_all(root).await.unwrap();
        }
    }
}
