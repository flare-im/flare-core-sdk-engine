//! Runtime assembly.
//!
//! A runtime selects platform adapters and injects them into the stable core.
//! The public IM APIs remain stable; only runtime assembly changes by platform.

use async_trait::async_trait;
use std::sync::Arc;

mod native;

pub use native::NativeRuntimeAssembler;

use crate::client::SdkConfig;
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::transport::SocketTransport;
use crate::platform::ports::crypto::CryptoPort;
use crate::platform::ports::media::{MediaProcessorPort, MediaServicePort, MediaUploaderPort};
use crate::platform::ports::runtime::{RuntimeClock, TaskSpawner};
use crate::shared::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlatformKind {
    Web,
    Android,
    Ios,
    HarmonyArkTs,
    HarmonyCangjie,
    ReactNative,
    UniApp,
    Electron,
    Tauri,
    Flutter,
    Native,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    Memory,
    Sqlite,
    IndexedDb,
    Custom,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageEncryptionConfig {
    pub enabled: bool,
    pub key_namespace: Option<String>,
    pub key_name: Option<String>,
}

impl StorageEncryptionConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn required() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageConfig {
    pub kind: StorageKind,
    pub namespace: String,
    pub tenant_id: String,
    pub user_id: String,
    pub path: Option<String>,
    #[serde(default)]
    pub encryption: StorageEncryptionConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaRuntimeKind {
    Web,
    Native,
    Harmony,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MediaRuntimeConfig {
    pub kind: MediaRuntimeKind,
    pub cache_namespace: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConfig {
    pub platform: PlatformKind,
    pub sdk: SdkConfig,
    pub storage: StorageConfig,
    pub media: MediaRuntimeConfig,
}

pub struct RuntimeComponents {
    pub stores: StoreProvider,
    pub transport: SocketTransport,
    pub clock: Option<Arc<dyn RuntimeClock>>,
    pub spawner: Option<Arc<dyn TaskSpawner>>,
    pub media_service: Option<Arc<dyn MediaServicePort>>,
    pub media_processor: Option<Arc<dyn MediaProcessorPort>>,
    pub media_uploader: Option<Arc<dyn MediaUploaderPort>>,
    pub crypto: Option<Arc<dyn CryptoPort>>,
}

impl RuntimeComponents {
    pub fn native_default(config: SdkConfig, stores: StoreProvider) -> Self {
        Self {
            transport: SocketTransport::new(config),
            stores,
            clock: None,
            spawner: None,
            media_service: None,
            media_processor: None,
            media_uploader: None,
            crypto: None,
        }
    }

    pub fn with_media_service(mut self, media_service: Arc<dyn MediaServicePort>) -> Self {
        self.media_service = Some(media_service);
        self
    }

    pub fn with_media_uploader(mut self, media_uploader: Arc<dyn MediaUploaderPort>) -> Self {
        self.media_uploader = Some(media_uploader);
        self
    }

    pub fn with_media_processor(mut self, media_processor: Arc<dyn MediaProcessorPort>) -> Self {
        self.media_processor = Some(media_processor);
        self
    }
}

#[async_trait]
pub trait RuntimeAssembler: Send + Sync {
    async fn assemble(&self, config: RuntimeConfig) -> Result<RuntimeComponents>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_config_deserializes_without_encryption_for_existing_json() {
        let config: StorageConfig = crate::shared::util::json::from_value(serde_json::json!({
            "kind": "sqlite",
            "namespace": "flare",
            "tenant_id": "0",
            "user_id": "alice",
            "path": null
        }))
        .expect("storage config");

        assert_eq!(config.kind, StorageKind::Sqlite);
        assert!(!config.encryption.is_enabled());
    }

    #[test]
    fn storage_encryption_config_follows_runtime_snake_case_json() {
        let config = StorageConfig {
            kind: StorageKind::Sqlite,
            namespace: "flare".to_string(),
            tenant_id: "0".to_string(),
            user_id: "alice".to_string(),
            path: None,
            encryption: StorageEncryptionConfig {
                enabled: true,
                key_namespace: Some("secure namespace".to_string()),
                key_name: Some("local db key".to_string()),
            },
        };

        let json = serde_json::to_value(&config).expect("serialize");
        assert!(json.get("encryption").is_some());
        assert!(json["encryption"].get("key_namespace").is_some());
        assert!(json["encryption"].get("keyNamespace").is_none());
    }
}
