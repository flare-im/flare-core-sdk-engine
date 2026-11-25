//! Session storage is supplied by the host, either already opened or through an account-scoped factory.

pub use crate::shared::util::paths::{
    default_sdk_data_root, dev_data_dir_relative_to_cwd, parse_data_url_to_path,
    resolve_sdk_data_root, sanitize_user_id_for_dir,
};

use crate::client::config::SdkAppConfig;
use crate::client::{SdkConfig, SdkResourceProfile};
use crate::platform::ports::storage::SecureKeyStore;
use crate::shared::error::Result;
use crate::shared::error::{ErrorCode, FlareError};
use std::sync::Arc;

/// 上层可选覆盖项（JSON 字段为 SDK canonical camelCase）。
///
/// 宿主使用 `endpoints` 提供 Core HTTP 和 WebSocket 地址，QUIC 与媒体地址可选。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkConfigOverlay {
    pub data_url: Option<String>,
    pub endpoints: Option<super::CoreEndpoints>,
    /// 宿主应用信息：版本、构建号、分发渠道、更新通道。给了就整体替换构建期的值。
    pub app: Option<SdkAppConfig>,
    pub tenant_id: Option<String>,
    /// Stable client device id for multi-device delivery and sync cursors.
    pub device_id: Option<String>,
    /// 单次握手（含 CONNECT_ACK）时限的本机覆盖（秒），默认 8 秒。
    pub connect_timeout_secs: Option<u64>,
    /// 运行资源预算 profile（`desktop` / `mobile`）。
    pub resource_profile: Option<SdkResourceProfile>,
    pub sync_batch_size: Option<u32>,
    /// Init/重连后会话消息补拉并发数（默认 4）。
    pub init_message_sync_concurrency: Option<u32>,
    pub event_bus_capacity: Option<usize>,
    pub event_dedupe_capacity: Option<usize>,
    pub message_dedupe_capacity: Option<usize>,
    pub ack_timeout_secs: Option<u64>,
    pub ack_max_retries: Option<u32>,
    pub ack_max_in_flight: Option<usize>,
    /// 私有部署的信任根（全局）。证书钉扎按接入点来自接入清单，没有全局配置项。
    pub tls_ca_cert_path: Option<String>,
    /// 内联信任 CA（PEM 或 base64 DER），见 [`crate::client::config::SdkConfig::tls_ca_cert`]。
    pub tls_ca_cert: Option<String>,
    pub enable_metrics: Option<bool>,
    /// 接入 token 来源（见 [`SdkAuthConfig`]）。
    pub auth: Option<crate::client::config::SdkAuthConfig>,
}

/// 将上层 overlay 合并进默认 [`SdkConfig`]。仅覆盖 `Some(...)` 字段。
pub fn merge_sdk_config(overlay: Option<&SdkConfigOverlay>) -> SdkConfig {
    merge_sdk_config_onto(SdkConfig::default(), overlay)
}

/// 把 overlay 叠到**已有的**配置上（登录重建会话时用构建期配置作底）。
///
/// 只拿默认值作底的后果实测过：宿主在构建期配好的项在登录那一刻被默认值顶掉。
pub fn merge_sdk_config_onto(
    mut config: SdkConfig,
    overlay: Option<&SdkConfigOverlay>,
) -> SdkConfig {
    let Some(o) = overlay else {
        return config;
    };
    if o.endpoints.is_some() {
        config.endpoints = o.endpoints.clone();
    }
    if let Some(app) = &o.app {
        config.app = app.clone();
    }
    if o.tenant_id.is_some() {
        config.tenant_id = o.tenant_id.clone();
    }
    if o.device_id.is_some() {
        config.device_id = o.device_id.clone();
    }
    if o.connect_timeout_secs.is_some() {
        config.connect_timeout_secs = o.connect_timeout_secs;
    }
    if let Some(profile) = o.resource_profile {
        config.resource_profile = profile;
    }
    if o.sync_batch_size.is_some() {
        config.sync_batch_size = o.sync_batch_size;
    }
    if o.init_message_sync_concurrency.is_some() {
        config.init_message_sync_concurrency = o.init_message_sync_concurrency;
    }
    if o.event_bus_capacity.is_some() {
        config.event_bus_capacity = o.event_bus_capacity;
    }
    if o.event_dedupe_capacity.is_some() {
        config.event_dedupe_capacity = o.event_dedupe_capacity;
    }
    if o.message_dedupe_capacity.is_some() {
        config.message_dedupe_capacity = o.message_dedupe_capacity;
    }
    if o.ack_timeout_secs.is_some() {
        config.ack_timeout_secs = o.ack_timeout_secs;
    }
    if o.auth.is_some() {
        config.auth = o.auth.clone();
    }
    if o.ack_max_retries.is_some() {
        config.ack_max_retries = o.ack_max_retries;
    }
    if o.ack_max_in_flight.is_some() {
        config.ack_max_in_flight = o.ack_max_in_flight;
    }
    if o.tls_ca_cert_path.is_some() {
        config.tls_ca_cert_path = o.tls_ca_cert_path.clone();
    }
    if o.tls_ca_cert.is_some() {
        config.tls_ca_cert = o.tls_ca_cert.clone();
    }
    if let Some(b) = o.enable_metrics {
        config.enable_metrics = b;
    }
    config
}

/// 解析连接 token。
///
/// 优先级：
/// 1. 显式参数 `explicit_token`
/// 2. 环境变量 `FLARE_IM_TOKEN`
/// 3. 环境变量 `TOKEN`
/// 4. 开发态自动生成临时 token（仅用于开发/测试）
pub fn resolve_connect_token(user_id: &str, explicit_token: Option<&str>) -> Result<String> {
    if let Some(token) = explicit_token {
        let t = token.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    if let Ok(token) = std::env::var("FLARE_IM_TOKEN") {
        let t = token.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    if let Ok(token) = std::env::var("TOKEN") {
        let t = token.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    let _ = user_id;
    // 不再有本地签发回退：token 要么显式传入，要么配 `auth` 由 SDK 向 Core API 签发，要么应用自己拿。
    // 客户端本地签发意味着签名密钥进客户端。
    Err(FlareError::localized(
        ErrorCode::ConfigurationError,
        "connect token required: pass an explicit token, or set sdk_config.auth so the SDK issues one from the Core API",
    ))
}

/// A composition root opens its backend for the account selected by prepare.
#[async_trait::async_trait]
pub trait AccountStoreOpener: std::any::Any + Send + Sync {
    async fn open(
        &self,
        data_root: &std::path::Path,
        tenant: &str,
        user: &str,
    ) -> Result<crate::storage::StoreProvider>;

    /// Optional host credential vault and its namespace/tenant, shared across reconnects.
    fn credential_store(&self, _tenant: &str) -> Option<(Arc<dyn SecureKeyStore>, String, String)> {
        None
    }
}

pub enum LoginDbKind {
    Open(Arc<dyn AccountStoreOpener>),
    Provided(crate::storage::StoreProvider),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_init_keeps_full_runtime_configuration() {
        let overlay: SdkConfigOverlay = serde_json::from_value(serde_json::json!({
            "endpoints": {"baseUrl": "https://core.example", "websocketUrl": "wss://core.example/ws"},
            "dataUrl": "file:///tmp/flare",
            "tenantId": "tenant-a",
            "deviceId": "device-a",
            "app": {"version": "1.2.3"},
            "auth": {"tokenEndpoint": "/auth/issue", "refreshLeadSecs": 90},
            "connectTimeoutSecs": 4,
            "resourceProfile": "mobile",
            "syncBatchSize": 40,
            "initMessageSyncConcurrency": 2,
            "eventBusCapacity": 128,
            "eventDedupeCapacity": 256,
            "messageDedupeCapacity": 512,
            "ackTimeoutSecs": 6,
            "ackMaxRetries": 3,
            "ackMaxInFlight": 12,
            "tlsCaCertPath": "ca.pem",
            "tlsCaCert": "inline-ca",
            "enableMetrics": false
        })).unwrap();
        let config = merge_sdk_config(Some(&overlay));
        assert_eq!(config.endpoints, overlay.endpoints);
        assert_eq!(overlay.data_url.as_deref(), Some("file:///tmp/flare"));
        assert_eq!(config.tenant_id, overlay.tenant_id);
        assert_eq!(config.device_id, overlay.device_id);
        assert_eq!(Some(config.app), overlay.app);
        assert_eq!(config.auth, overlay.auth);
        assert_eq!(config.connect_timeout_secs, Some(4));
        assert_eq!(config.resource_profile, SdkResourceProfile::Mobile);
        assert_eq!(config.sync_batch_size, Some(40));
        assert_eq!(config.init_message_sync_concurrency, Some(2));
        assert_eq!(config.event_bus_capacity, Some(128));
        assert_eq!(config.event_dedupe_capacity, Some(256));
        assert_eq!(config.message_dedupe_capacity, Some(512));
        assert_eq!(config.ack_timeout_secs, Some(6));
        assert_eq!(config.ack_max_retries, Some(3));
        assert_eq!(config.ack_max_in_flight, Some(12));
        assert_eq!(config.tls_ca_cert_path, overlay.tls_ca_cert_path);
        assert_eq!(config.tls_ca_cert, overlay.tls_ca_cert);
        assert!(!config.enable_metrics);
    }

    /// 应用信息走 camelCase 的 `app` 对象（各端 init 都这么传）；给了就整体替换构建期的值，
    /// 没给保留构建期的，两边都没有就保持为空——绝不回退成假版本号。
    #[test]
    fn app_overlay_merges_onto_configured_config() {
        let overlay: SdkConfigOverlay = crate::shared::util::json::from_value(serde_json::json!({
            "app": { "version": "3.2.1", "build": 30201, "distribution": "appstore", "channel": "stable" }
        }))
        .expect("camelCase app object");
        let app = overlay.app.clone().unwrap();
        assert_eq!(app.version.as_deref(), Some("3.2.1"));
        assert_eq!(app.build, Some(30201));
        assert_eq!(app.distribution.as_deref(), Some("appstore"));
        assert_eq!(app.channel.as_deref(), Some("stable"));

        let configured = SdkConfig::builder()
            .app(SdkAppConfig {
                version: Some("3.0.0".into()),
                build: Some(30000),
                distribution: Some("direct".into()),
                channel: None,
                ..Default::default()
            })
            .build();
        let merged = merge_sdk_config_onto(configured.clone(), Some(&overlay));
        assert_eq!(merged.app, app);
        let merged = merge_sdk_config_onto(configured.clone(), Some(&SdkConfigOverlay::default()));
        assert_eq!(merged.app, configured.app);
        assert_eq!(merge_sdk_config(None).app, SdkAppConfig::default());
    }

    #[test]
    fn overlay_preserves_the_endpoint_group() {
        let overlay: SdkConfigOverlay = serde_json::from_value(serde_json::json!({
            "endpoints": { "baseUrl": "https://example.com/api", "websocketUrl": "wss://example.com/ws" },
            "connectTimeoutSecs": 8
        })).unwrap();
        let merged = merge_sdk_config(Some(&overlay));
        assert_eq!(merged.endpoints, overlay.endpoints);
        assert_eq!(merged.connect_timeout_secs, Some(8));
    }
}

#[cfg(test)]
mod auth_overlay_tests {
    use super::*;

    /// 五端只传 JSON overlay：`auth` 在就是 SDK 托管，按 camelCase 进来并落到 SdkConfig.auth；
    /// `tokenEndpoint` 是 Core API 上的签发路径，不给用标准路径，绝对地址是配置错误。
    #[test]
    fn auth_overlay_round_trips_and_merges() {
        let overlay: SdkConfigOverlay =
            serde_json::from_str(r#"{"auth":{"refreshLeadSecs":120}}"#).unwrap();
        let auth = overlay.auth.as_ref().unwrap();
        assert_eq!(auth.refresh_lead().as_secs(), 120);
        assert_eq!(auth.token_path().unwrap(), "/api/v1/auth/tokens");
        let merged = merge_sdk_config(Some(&overlay));
        assert_eq!(merged.auth.as_ref(), Some(auth));

        let custom: SdkConfigOverlay =
            serde_json::from_str(r#"{"auth":{"tokenEndpoint":"/auth/v2/tokens/"}}"#).unwrap();
        assert_eq!(
            custom.auth.unwrap().token_path().unwrap(),
            "/auth/v2/tokens"
        );
        for absolute in ["http://h/api", "//h/api", "api/v1/auth/tokens"] {
            let auth = crate::client::config::SdkAuthConfig {
                token_endpoint: Some(absolute.into()),
                ..Default::default()
            };
            assert_eq!(
                auth.token_path().unwrap_err().code(),
                Some(ErrorCode::ConfigurationError),
                "{absolute}"
            );
        }

        let none: SdkConfigOverlay = serde_json::from_str(r#"{}"#).unwrap();
        assert!(none.auth.is_none());
        assert!(merge_sdk_config(Some(&none)).auth.is_none());
    }

    /// 设备只来自宿主：overlay 没给就一直不给，登录重建（以构建期配置作底再叠 overlay）也
    /// 不会凭空多出一台设备；给了就原样带过去，构建期的值也不被没有设备的 overlay 抹掉。
    #[test]
    fn a_device_comes_only_from_the_host() {
        let none: SdkConfigOverlay = serde_json::from_str(r#"{}"#).unwrap();
        let first = merge_sdk_config(Some(&none));
        let rebuilt = merge_sdk_config_onto(first.clone(), Some(&none));
        assert_eq!(first.device_id, None);
        assert_eq!(rebuilt.device_id, None);
        assert_eq!(
            first.effective_device_id(),
            rebuilt.effective_device_id(),
            "one process without a device of the host's is one device"
        );

        let named: SdkConfigOverlay = serde_json::from_str(r#"{"deviceId":"device-a"}"#).unwrap();
        assert_eq!(
            merge_sdk_config(Some(&named)).device_id.as_deref(),
            Some("device-a")
        );
        let built = SdkConfig::builder().device_id("device-b").build();
        assert_eq!(
            merge_sdk_config_onto(built, Some(&none))
                .device_id
                .as_deref(),
            Some("device-b")
        );
    }

    /// 没有显式 token、没配网关、没有环境变量：明确报配置错误，绝不本地签发。
    #[test]
    fn no_token_source_is_a_configuration_error_not_a_local_mint() {
        // SAFETY: 测试内清理可能影响结果的环境变量（进程内）。
        unsafe {
            std::env::remove_var("FLARE_IM_TOKEN");
            std::env::remove_var("TOKEN");
        }
        let err = resolve_connect_token("u", None).unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::ConfigurationError));
        assert!(err.to_string().contains("sdk_config.auth"), "{err}");
    }
}
