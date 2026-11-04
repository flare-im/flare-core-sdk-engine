//! 配置合并、连接 token、登录存储选型；路径类工具在 [`crate::shared::util::paths`]。
//!
//! - **SQLite 仓储构建**：[`crate::shared::util::sqlite_store`]（`feature = "lifecycle-sqlite"`），由 [`super::IMClient::login`] 在 `LoginDbKind::Sqlite` 时调用。
//! - **IndexedDB（Web）**：宿主实现 [`crate::infrastructure::persistence`] 后 [`LoginDbKind::IndexedDb`] 传入 [`super::IMClient::login`]。

pub use crate::shared::util::paths::{
    default_sdk_data_root, dev_data_dir_relative_to_cwd, parse_data_url_to_path,
    resolve_sdk_data_root, resolve_user_db_path, sanitize_user_id_for_dir,
};

use crate::client::{SdkConfig, SdkResourceProfile, TransportKind, TransportPolicy};
#[cfg(feature = "lifecycle-sqlite")]
use crate::platform::ports::storage::SecureKeyStore;
use crate::shared::error::Result;
use crate::shared::error::{ErrorCode, FlareError};
#[cfg(feature = "lifecycle-sqlite")]
use std::sync::Arc;

/// 上层可选覆盖项（JSON 字段为 SDK canonical camelCase）。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkConfigOverlay {
    pub data_url: Option<String>,
    pub ws_url: Option<String>,
    pub quic_url: Option<String>,
    pub http_url: Option<String>,
    pub media_storage_proxy_prefix: Option<String>,
    pub media_storage_proxy_targets: Option<Vec<String>>,
    pub capability_url: Option<String>,
    pub online_url: Option<String>,
    pub tenant_id: Option<String>,
    /// Stable client device id for multi-device delivery and sync cursors.
    pub device_id: Option<String>,
    /// 宿主应用版本名（`appVersion`，如 `3.2.1`），CONNECT 时作为 `app_version` 上报。
    pub app_version: Option<String>,
    /// 宿主应用构建号（`appBuild`，单调整数），服务端版本比较只用它。
    pub app_build: Option<u64>,
    /// 分发渠道（`distribution`）：`appstore` / `play` / `appgallery` / `direct` / `web` / `tauri` 等。
    pub distribution: Option<String>,
    pub connect_timeout_secs: Option<u64>,
    pub reconnect_interval_secs: Option<u64>,
    pub max_reconnect_attempts: Option<u32>,
    pub transport_policy: Option<TransportPolicy>,
    /// 非竞速模式下的默认传输（`websocket` / `quic`）。
    pub default_transport: Option<TransportKind>,
    /// 协议竞速优先级（从高到低），如 `["quic", "websocket"]`。
    pub protocol_race_order: Option<Vec<TransportKind>>,
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
    pub tls_ca_cert_path: Option<String>,
    pub tls_spki_sha256_pins: Option<Vec<String>>,
    pub tls_certificate_sha256_pins: Option<Vec<String>>,
    /// 内联信任 CA（PEM 或 base64 DER），见 [`crate::client::config::SdkConfig::tls_ca_cert`]。
    pub tls_ca_cert: Option<String>,
    pub enable_metrics: Option<bool>,
    /// 接入 token 来源（见 [`SdkAuthConfig`]）。
    pub auth: Option<crate::client::config::SdkAuthConfig>,
}

/// 解析默认 WebSocket 地址。
///
/// 优先级：
/// 1. `overlay.wsUrl`
/// 2. 环境变量 `FLARE_IM_SERVER_URL`
/// 3. 内置默认 `ws://localhost:60051`
pub fn default_ws_url(overlay: Option<&SdkConfigOverlay>) -> String {
    let from_overlay = overlay.and_then(|c| c.ws_url.as_deref()).map(String::from);
    let from_env = std::env::var("FLARE_IM_SERVER_URL").ok();
    let url = from_overlay
        .clone()
        .or_else(|| from_env.clone())
        .unwrap_or_else(|| "ws://localhost:60051".to_string());
    // 生效地址必须可见：连不上时第一个要确认的就是"到底连的哪里"，
    // 而这条链路有三档来源（overlay / 环境变量 / 内置默认），靠猜代价很高。
    tracing::info!(
        ws_url = %url,
        from_overlay = from_overlay.is_some(),
        from_env = from_env.is_some(),
        "IM WebSocket 地址已确定"
    );
    url
}

/// 解析登录时子客户端要用的 WebSocket 地址。
///
/// 优先级：**overlay > 构建期配置 > 兜底默认值**。
///
/// 中间那一档是 2026-08-26 补的。此前登录时 `prepare` 只看 overlay，
/// 父客户端 `IMClientBuilder::config` 里配好的 ws 地址在重建子客户端那一刻被丢掉，
/// 直接落到 `ws://localhost:60051`。
///
/// 这个缺陷**只在非本机部署上显形**：本机开发时兜底地址恰好是对的，所以配置传递
/// 断了也看不出来。线上实测撞到过——web 客户端业务接口全通、页面正常，
/// 唯独 IM 长连接去连访问者自己的电脑，控制台一句 `ERR_CONNECTION_REFUSED`。
pub fn resolve_ws_url(overlay: Option<&SdkConfigOverlay>, configured: Option<&str>) -> String {
    overlay
        .and_then(|c| c.ws_url.clone())
        .or_else(|| configured.map(String::from))
        .unwrap_or_else(|| default_ws_url(overlay))
}

/// 将上层 overlay 合并进基础 [`SdkConfig`]。
///
/// 仅覆盖 `Some(...)` 字段，未提供的字段保持默认值。
pub fn merge_sdk_config(ws_url: &str, overlay: Option<&SdkConfigOverlay>) -> SdkConfig {
    merge_sdk_config_onto(SdkConfig::new(ws_url), ws_url, overlay)
}

/// 把 overlay 叠到**已有的**配置上（登录重建会话时用构建期配置作底）。
///
/// 只拿默认值作底的后果实测过：宿主在构建期配好的 IM HTTP 地址（媒体、在线状态）在登录
/// 那一刻被 `SdkConfig::new` 的默认值 `http://localhost:50050` 顶掉——部署后浏览器把
/// 图片上传、在线状态请求发到访问者自己的电脑，被直接拦下。
pub fn merge_sdk_config_onto(
    mut config: SdkConfig,
    ws_url: &str,
    overlay: Option<&SdkConfigOverlay>,
) -> SdkConfig {
    config.ws_url = Some(ws_url.to_string());
    if let Some(o) = overlay {
        if let Some(u) = &o.ws_url {
            config.ws_url = Some(u.clone());
        }
        if o.quic_url.is_some() {
            config.quic_url = o.quic_url.clone();
        }
        if o.http_url.is_some() {
            config.http_url = o.http_url.clone();
        }
        if o.media_storage_proxy_prefix.is_some() {
            config.media_storage_proxy_prefix = o.media_storage_proxy_prefix.clone();
        }
        if let Some(targets) = &o.media_storage_proxy_targets {
            config.media_storage_proxy_targets = targets.clone();
        }
        if o.capability_url.is_some() {
            config.capability_url = o.capability_url.clone();
        }
        if o.online_url.is_some() {
            config.online_url = o.online_url.clone();
        }
        if o.tenant_id.is_some() {
            config.tenant_id = o.tenant_id.clone();
        }
        if o.device_id.is_some() {
            config.device_id = o.device_id.clone();
        }
        if o.app_version.is_some() {
            config.app_version = o.app_version.clone();
        }
        if o.app_build.is_some() {
            config.app_build = o.app_build;
        }
        if o.distribution.is_some() {
            config.distribution = o.distribution.clone();
        }
        if o.connect_timeout_secs.is_some() {
            config.connect_timeout_secs = o.connect_timeout_secs;
        }
        if o.reconnect_interval_secs.is_some() {
            config.reconnect_interval_secs = o.reconnect_interval_secs;
        }
        if o.max_reconnect_attempts.is_some() {
            config.max_reconnect_attempts = o.max_reconnect_attempts;
        }
        if let Some(policy) = o.transport_policy {
            config.transport_policy = policy;
        }
        if o.default_transport.is_some() {
            config.default_transport = o.default_transport;
        }
        if o.protocol_race_order.is_some() {
            config.protocol_race_order = o.protocol_race_order.clone();
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
        if let Some(auth) = &o.auth {
            config.auth = auth.clone();
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
        if let Some(pins) = &o.tls_spki_sha256_pins {
            config.tls_spki_sha256_pins = pins.clone();
        }
        if let Some(pins) = &o.tls_certificate_sha256_pins {
            config.tls_certificate_sha256_pins = pins.clone();
        }
        if o.tls_ca_cert.is_some() {
            config.tls_ca_cert = o.tls_ca_cert.clone();
        }
        if let Some(b) = o.enable_metrics {
            config.enable_metrics = b;
        }
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
    // 不再有本地签发回退：token 要么显式传入，要么由 SDK 按 `auth.token_endpoint` 向网关签发，
    // 要么应用自己拿。客户端本地签发意味着签名密钥进客户端。
    Err(FlareError::localized(
        ErrorCode::ConfigurationError,
        "connect token required: pass an explicit token, or set sdk_config.auth.token_endpoint so the SDK issues one from the gateway",
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

    /// ws 地址的优先级必须是 overlay > 构建期配置 > 兜底默认值。
    ///
    /// 中间那一档曾经不存在，导致 `IMClientBuilder::config` 配好的地址在登录时被丢掉。
    /// 本机开发发现不了——兜底的 `ws://localhost:60051` 在本机恰好是对的。
    #[test]
    fn ws_url_priority_overlay_then_configured_then_default() {
        let overlay = SdkConfigOverlay {
            ws_url: Some("wss://from-overlay/im".into()),
            ..Default::default()
        };

        // overlay 最高
        assert_eq!(
            resolve_ws_url(Some(&overlay), Some("wss://from-builder/im")),
            "wss://from-overlay/im"
        );

        // 没有 overlay 时用构建期配置 —— 这一档就是修复的核心
        assert_eq!(
            resolve_ws_url(None, Some("wss://from-builder/im")),
            "wss://from-builder/im",
            "构建期配置必须能传到登录时重建的子客户端，否则远程部署连不上 IM"
        );

        // overlay 存在但没给 ws_url，同样该回落到构建期配置
        let empty_overlay = SdkConfigOverlay::default();
        assert_eq!(
            resolve_ws_url(Some(&empty_overlay), Some("wss://from-builder/im")),
            "wss://from-builder/im"
        );
    }

    /// 两者都没有时才用兜底 —— 保持本机开发的既有行为不变。
    #[test]
    fn ws_url_falls_back_to_default_when_nothing_configured() {
        // 环境变量可能被别的测试设过，这里只断言「非空且是个 ws 地址」
        let got = resolve_ws_url(None, None);
        assert!(
            got.starts_with("ws://") || got.starts_with("wss://"),
            "兜底值必须仍是可用的 ws 地址，实际：{got}"
        );
    }
    use super::*;

    /// 应用版本三项走 camelCase JSON（各端 init 都这么传），overlay 显式给的值覆盖构建期配置，
    /// 没给的项保留构建期配置，两边都没有就保持为空——绝不回退成假版本号。
    #[test]
    fn app_version_overlay_merges_onto_configured_config() {
        let overlay: SdkConfigOverlay = serde_json::from_value(serde_json::json!({
            "appVersion": "3.2.1",
            "appBuild": 30201,
            "distribution": "appstore"
        }))
        .expect("camelCase app version fields");
        assert_eq!(overlay.app_version.as_deref(), Some("3.2.1"));
        assert_eq!(overlay.app_build, Some(30201));
        assert_eq!(overlay.distribution.as_deref(), Some("appstore"));

        let configured = SdkConfig::builder()
            .app_version("3.0.0")
            .app_build(30000)
            .distribution("direct")
            .build();
        let merged = merge_sdk_config_onto(configured.clone(), "ws://h/ws", Some(&overlay));
        assert_eq!(merged.app_version.as_deref(), Some("3.2.1"));
        assert_eq!(merged.app_build, Some(30201));
        assert_eq!(merged.distribution.as_deref(), Some("appstore"));

        let partial = SdkConfigOverlay {
            app_build: Some(30202),
            ..Default::default()
        };
        let merged = merge_sdk_config_onto(configured, "ws://h/ws", Some(&partial));
        assert_eq!(merged.app_version.as_deref(), Some("3.0.0"));
        assert_eq!(merged.app_build, Some(30202));
        assert_eq!(merged.distribution.as_deref(), Some("direct"));

        let unset = merge_sdk_config("ws://h/ws", Some(&SdkConfigOverlay::default()));
        assert_eq!(unset.app_version, None);
        assert_eq!(unset.app_build, None);
        assert_eq!(unset.distribution, None);
    }

    #[test]
    fn sdk_config_overlay_uses_canonical_camel_case_json() {
        let overlay: SdkConfigOverlay = serde_json::from_value(serde_json::json!({
            "wsUrl": "ws://localhost:60051",
            "quicUrl": "quic://localhost:60052",
            "transportPolicy": "protocol_race",
            "defaultTransport": "websocket",
            "protocolRaceOrder": ["quic", "websocket"],
            "connectTimeoutSecs": 30,
            "tlsCaCertPath": "/tmp/flare-ca.crt",
            "tlsSpkiSha256Pins": ["spki-sha256/current", "spki-sha256/next"],
            "tlsCertificateSha256Pins": ["sha256/legacy"],
            "enableMetrics": true
        }))
        .expect("camelCase overlay");

        assert_eq!(overlay.ws_url.as_deref(), Some("ws://localhost:60051"));
        assert_eq!(overlay.quic_url.as_deref(), Some("quic://localhost:60052"));
        assert_eq!(
            overlay.transport_policy,
            Some(TransportPolicy::ProtocolRace)
        );
        assert_eq!(overlay.default_transport, Some(TransportKind::WebSocket));
        assert_eq!(
            overlay.protocol_race_order,
            Some(vec![TransportKind::Quic, TransportKind::WebSocket])
        );
        assert_eq!(overlay.connect_timeout_secs, Some(30));
        assert_eq!(
            overlay.tls_ca_cert_path.as_deref(),
            Some("/tmp/flare-ca.crt")
        );
        assert_eq!(
            overlay.tls_spki_sha256_pins,
            Some(vec![
                "spki-sha256/current".to_string(),
                "spki-sha256/next".to_string()
            ])
        );
        assert_eq!(
            overlay.tls_certificate_sha256_pins,
            Some(vec!["sha256/legacy".to_string()])
        );
        assert_eq!(overlay.enable_metrics, Some(true));

        let merged = merge_sdk_config("ws://fallback", Some(&overlay));
        assert_eq!(
            merged.tls_ca_cert_path.as_deref(),
            Some("/tmp/flare-ca.crt")
        );

        let json = serde_json::to_value(&overlay).expect("serialize overlay");
        assert!(json.get("wsUrl").is_some());
        assert!(json.get("ws_url").is_none());
        assert!(json.get("transportPolicy").is_some());
        assert!(json.get("transport_policy").is_none());
        assert!(json.get("tlsCaCertPath").is_some());
        assert!(json.get("tls_ca_cert_path").is_none());
        assert!(json.get("tlsSpkiSha256Pins").is_some());
        assert!(json.get("tls_spki_sha256_pins").is_none());
        assert!(json.get("tlsCertificateSha256Pins").is_some());
    }
}

#[cfg(test)]
mod auth_overlay_tests {
    use super::*;

    /// 五端只传 JSON overlay；`auth.tokenEndpoint` 必须按 camelCase 进来并落到 SdkConfig.auth。
    #[test]
    fn auth_overlay_round_trips_and_merges() {
        let overlay: SdkConfigOverlay = serde_json::from_str(
            r#"{"wsUrl":"ws://h/ws","auth":{"tokenEndpoint":"http://h/api","refreshLeadSecs":120}}"#,
        )
        .unwrap();
        let auth = overlay.auth.as_ref().unwrap();
        assert!(auth.sdk_managed());
        assert_eq!(auth.refresh_lead().as_secs(), 120);
        let merged = merge_sdk_config("ws://x", Some(&overlay));
        assert_eq!(merged.auth.token_endpoint.as_deref(), Some("http://h/api"));

        let none: SdkConfigOverlay = serde_json::from_str(r#"{"wsUrl":"ws://h/ws"}"#).unwrap();
        assert!(none.auth.is_none());
        assert!(!merge_sdk_config("ws://x", Some(&none)).auth.sdk_managed());
    }

    /// 设备只来自宿主：overlay 没给就一直不给，登录重建（以构建期配置作底再叠 overlay）也
    /// 不会凭空多出一台设备；给了就原样带过去，构建期的值也不被没有设备的 overlay 抹掉。
    #[test]
    fn a_device_comes_only_from_the_host() {
        let none: SdkConfigOverlay = serde_json::from_str(r#"{"wsUrl":"ws://h/ws"}"#).unwrap();
        let first = merge_sdk_config("ws://x", Some(&none));
        let rebuilt = merge_sdk_config_onto(first.clone(), "ws://x", Some(&none));
        assert_eq!(first.device_id, None);
        assert_eq!(rebuilt.device_id, None);
        assert_eq!(
            first.effective_device_id(),
            rebuilt.effective_device_id(),
            "one process without a device of the host's is one device"
        );

        let named: SdkConfigOverlay =
            serde_json::from_str(r#"{"wsUrl":"ws://h/ws","deviceId":"device-a"}"#).unwrap();
        assert_eq!(
            merge_sdk_config("ws://x", Some(&named))
                .device_id
                .as_deref(),
            Some("device-a")
        );
        let built = SdkConfig::builder().device_id("device-b").build();
        assert_eq!(
            merge_sdk_config_onto(built, "ws://x", Some(&none))
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
        assert!(err.to_string().contains("auth.token_endpoint"), "{err}");
    }
}
