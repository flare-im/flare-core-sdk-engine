//! 配置合并、连接 token、登录存储选型；路径类工具在 [`crate::shared::util::paths`]。
//!
//! - **SQLite 仓储构建**：[`crate::shared::util::sqlite_store`]（`feature = "lifecycle-sqlite"`），由 [`super::IMClient::login`] 在 `LoginDbKind::Sqlite` 时调用。
//! - **IndexedDB（Web）**：宿主实现 [`crate::infrastructure::persistence`] 后 [`LoginDbKind::IndexedDb`] 传入 [`super::IMClient::login`]。

pub use crate::shared::util::paths::{
    default_sdk_data_root, dev_data_dir_relative_to_cwd, parse_data_url_to_path,
    resolve_sdk_data_root, resolve_user_db_path, sanitize_user_id_for_dir,
};

use crate::client::config::SdkAppConfig;
use crate::client::{SdkConfig, SdkResourceProfile};
#[cfg(feature = "lifecycle-sqlite")]
use crate::platform::ports::storage::SecureKeyStore;
use crate::shared::error::Result;
use crate::shared::error::{ErrorCode, FlareError};
#[cfg(feature = "lifecycle-sqlite")]
use std::sync::Arc;

/// 上层可选覆盖项（JSON 字段为 SDK canonical camelCase）。
///
/// 没有任何地址字段：连哪里只由 `access`（接入配置）决定，开发联调用
/// `access.trustMode = "development"` + `access.developmentEndpoints`。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkConfigOverlay {
    pub data_url: Option<String>,
    /// 接入配置：信任根、构建期种子、信任模式；开发模式下的开发端点。
    pub access: Option<flare_access::AccessConfig>,
    /// 宿主应用信息：版本、构建号、分发渠道、更新通道。给了就整体替换构建期的值。
    pub app: Option<SdkAppConfig>,
    pub tenant_id: Option<String>,
    /// Stable client device id for multi-device delivery and sync cursors.
    pub device_id: Option<String>,
    /// 单次握手（含 CONNECT_ACK）时限的本机覆盖（秒）；不设时用清单 `connectTimeoutMs`（默认 8 秒）。
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
    if o.access.is_some() {
        config.access = o.access.clone();
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

/// 登录时存储选型，配合 [`super::IMClient::login`]。
pub enum LoginDbKind {
    #[cfg(feature = "lifecycle-sqlite")]
    Sqlite,
    #[cfg(feature = "lifecycle-sqlite")]
    EncryptedSqlite {
        key_store: Arc<dyn SecureKeyStore>,
        key_namespace: Option<String>,
        tenant_id: Option<String>,
        key_name: Option<String>,
    },
    IndexedDb(crate::infrastructure::persistence::StoreProvider),
}

#[cfg(feature = "lifecycle-sqlite")]
impl LoginDbKind {
    pub fn encrypted_sqlite(key_store: Arc<dyn SecureKeyStore>) -> Self {
        Self::EncryptedSqlite {
            key_store,
            key_namespace: None,
            tenant_id: None,
            key_name: None,
        }
    }

    pub fn encrypted_sqlite_with_descriptor(
        key_store: Arc<dyn SecureKeyStore>,
        key_namespace: impl Into<String>,
        tenant_id: impl Into<String>,
        key_name: impl Into<String>,
    ) -> Self {
        Self::EncryptedSqlite {
            key_store,
            key_namespace: Some(key_namespace.into()),
            tenant_id: Some(tenant_id.into()),
            key_name: Some(key_name.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_access::{AccessConfig, DevelopmentEndpoints, TrustMode};

    /// 应用信息走 camelCase 的 `app` 对象（各端 init 都这么传）；给了就整体替换构建期的值，
    /// 没给保留构建期的，两边都没有就保持为空——绝不回退成假版本号。
    #[test]
    fn app_overlay_merges_onto_configured_config() {
        let overlay: SdkConfigOverlay = serde_json::from_value(serde_json::json!({
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

    /// 开发模式的接入配置原样带过去；旧的单地址字段与全局钉扎不再被认（一条路径）。
    #[test]
    fn access_overlay_is_the_only_way_to_say_where_to_connect() {
        let overlay: SdkConfigOverlay = serde_json::from_value(serde_json::json!({
            "access": {
                "trustMode": "development",
                "developmentEndpoints": {
                    "wsUrl": "ws://127.0.0.1:60051/ws",
                    "coreApiUrl": "http://127.0.0.1:60061"
                }
            },
            "connectTimeoutSecs": 8,
            "tlsSpkiSha256Pins": ["spki-sha256/current"]
        }))
        .expect("camelCase overlay");
        let access = overlay.access.clone().unwrap();
        assert_eq!(access.trust_mode, TrustMode::Development);
        assert_eq!(
            access.development_endpoints,
            Some(DevelopmentEndpoints {
                ws_url: Some("ws://127.0.0.1:60051/ws".into()),
                core_api_url: Some("http://127.0.0.1:60061".into()),
                ..Default::default()
            })
        );
        let merged = merge_sdk_config(Some(&overlay));
        assert_eq!(merged.access, Some(access));
        assert_eq!(merged.connect_timeout_secs, Some(8));

        let json = serde_json::to_value(&overlay).expect("serialize overlay");
        assert!(json.get("access").is_some());
        for removed in [
            "wsUrl",
            "httpUrl",
            "quicUrl",
            "transportPolicy",
            "reconnectIntervalSecs",
            "tlsSpkiSha256Pins",
            "tlsCertificateSha256Pins",
        ] {
            assert!(json.get(removed).is_none(), "{removed} must be gone");
        }
        assert!(
            serde_json::from_value::<SdkConfigOverlay>(serde_json::json!({ "wsUrl": "ws://x" }))
                .is_ok_and(|overlay| overlay.access.is_none()),
            "unknown legacy keys are ignored, never mapped"
        );
        let production = AccessConfig::production("b7f3c9a1e2d4b6a8", vec![], None);
        assert_eq!(production.trust_mode, TrustMode::Production);
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
