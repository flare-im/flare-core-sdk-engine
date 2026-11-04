use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use flare_core::common::config_types::{
    TlsConfig as CoreTlsConfig, TransportProtocol as CoreTransport,
};

use crate::shared::util::RELIABLE_QUEUE_MAX_IN_FLIGHT;
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

fn new_default_device_id() -> String {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("sdk-device-{}-{millis:x}", std::process::id())
    }
    #[cfg(target_arch = "wasm32")]
    {
        format!("sdk-web-{}", uuid::Uuid::new_v4())
    }
}

/// 未配置 `device_id` 时的进程级兜底标识（**同一进程内恒定**）。
///
/// 曾经每次调用都新生成一个值，后果有二：
/// 1. 每次重连都以「新设备」身份上报 → 服务端设备表被无限刷入僵尸设备；
/// 2. 与登录时签进 token 的 device_id 必然不等 → 网关按设备绑定判定拒绝连接，
///    这正是「device_id 只能传空」这一绕过手法的由来（多端互踢语义因此做不了）。
///
/// 进程级恒定只解决 (1) 和进程内一致性：**跨重启仍会变**。要拿到 token 绑定与
/// 多端互踢，宿主必须提供按平台持久化的稳定值（iOS keychain / Android SharedPreferences /
/// Web localStorage / 桌面配置文件），经 `SdkConfigOverlay.device_id` 传入，
/// 并把**同一个值**用于社交登录——见 `SdkConfig::effective_device_id`。
fn process_fallback_device_id() -> &'static str {
    static FALLBACK: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    FALLBACK.get_or_init(|| {
        let id = new_default_device_id();
        tracing::warn!(
            device_id = %id,
            "no device_id configured; using an ephemeral per-process id. \
             Token device binding and multi-device kick are unavailable until the host \
             supplies a persisted SdkConfigOverlay.device_id and uses the same value at login."
        );
        id
    })
}

/// Wire transport kind for init overlay and protocol race ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    #[serde(rename = "websocket")]
    WebSocket,
    Quic,
}

impl TransportKind {
    pub fn to_core(self) -> CoreTransport {
        match self {
            Self::WebSocket => CoreTransport::WebSocket,
            Self::Quic => CoreTransport::QUIC,
        }
    }

    pub fn parse_list(values: &[String]) -> Option<Vec<Self>> {
        let mut out = Vec::with_capacity(values.len());
        for raw in values {
            let kind = match raw.trim().to_ascii_lowercase().as_str() {
                "websocket" | "ws" => Self::WebSocket,
                "quic" => Self::Quic,
                _ => return None,
            };
            out.push(kind);
        }
        if out.is_empty() { None } else { Some(out) }
    }
}

/// Transport selection policy.
///
/// Browser/WASM must use WebSocket because QUIC/native protocol racing is not
/// available in the browser sandbox. Native targets can keep protocol racing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransportPolicy {
    #[default]
    Auto,
    #[serde(rename = "websocket_only")]
    WebSocketOnly,
    ProtocolRace,
}

/// Runtime resource budget selected by the host app.
///
/// The profile only supplies defaults. Explicit numeric config fields still win,
/// so production apps can tune one knob without forking the whole profile.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SdkResourceProfile {
    /// Balanced defaults for desktop and server-like hosts.
    #[default]
    Desktop,
    /// Conservative defaults for mobile devices and memory-constrained hosts.
    Mobile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SdkRuntimeResources {
    pub sync_batch_size: u32,
    pub init_message_sync_concurrency: u32,
    pub event_bus_capacity: usize,
    pub event_dedupe_capacity: usize,
    pub message_dedupe_capacity: usize,
}

impl SdkResourceProfile {
    fn defaults(self) -> SdkRuntimeResources {
        match self {
            Self::Desktop => SdkRuntimeResources {
                sync_batch_size: 200,
                init_message_sync_concurrency: 4,
                event_bus_capacity: 2048,
                event_dedupe_capacity: 4096,
                message_dedupe_capacity: 8192,
            },
            Self::Mobile => SdkRuntimeResources {
                sync_batch_size: 80,
                init_message_sync_concurrency: 2,
                event_bus_capacity: 512,
                event_dedupe_capacity: 1024,
                message_dedupe_capacity: 2048,
            },
        }
    }
}

/// SDK 配置
///
/// ```ignore
/// let config = SdkConfig::builder()
///     .endpoint("wss://im.example.com")
///     .connect_timeout_secs(15)
///     .build();
/// ```
/// 接入 token 的来源（core-only 形态：SDK 托管，向网关签发/刷新）。
///
/// 客户端不再本地签发 token：`token_endpoint` 配了就是 SDK 托管——`login(user_id)` 不传 token 时
/// SDK 去 `{token_endpoint}/api/v1/auth/tokens` 签发，到期前 `refresh_lead_secs` 秒用
/// `/api/v1/auth/tokens/refresh` 换新并 `update_access_token`。没配则必须显式传 token
/// （业务应用由宿主自己拿 token、自己刷新）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkAuthConfig {
    /// 网关基址（不含 `/api/v1/auth/...`），如 `http://host/api`。缺省不托管。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_endpoint: Option<String>,
    /// 到期前多少秒刷新，默认 300。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_lead_secs: Option<u64>,
}

impl SdkAuthConfig {
    pub const DEFAULT_REFRESH_LEAD_SECS: u64 = 300;

    pub fn sdk_managed(&self) -> bool {
        self.token_endpoint
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
    }

    pub fn refresh_lead(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.refresh_lead_secs
                .unwrap_or(Self::DEFAULT_REFRESH_LEAD_SECS)
                .max(5),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkConfig {
    pub ws_url: Option<String>,
    pub quic_url: Option<String>,
    pub http_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_storage_proxy_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_storage_proxy_targets: Vec<String>,
    pub capability_url: Option<String>,
    /// No longer used: presence goes through `http_url` (the Core API gateway) on
    /// every target. Accepted so existing configurations still parse.
    pub online_url: Option<String>,
    pub tenant_id: Option<String>,
    /// Stable client device id used by connection metadata and sync cursors.
    ///
    /// Only the host's own value: `None` (the default) means the host named no
    /// device, and [`SdkConfig::effective_device_id`] then reports the one id of
    /// the current process or web page. Push and token binding need the host's
    /// persisted value and read this field, so a default never stands in for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    /// 宿主应用的版本名（如 `3.2.1`），CONNECT 时作为 `app_version` 上报。
    ///
    /// 只用宿主（或平台包代采）给的真实值；没给就不上报，服务端看到缺省即知「未知版本」，
    /// 不能用一个假值冒充。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_version: Option<String>,
    /// 宿主应用的构建号：单调递增的整数，服务端做版本比较（强制升级、灰度）只看它，
    /// 版本名只用于展示。CONNECT 时以十进制字符串放进 `app_build`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_build: Option<u64>,
    /// 分发渠道：`appstore`、`play`、`appgallery`、`direct`、`web`、`tauri` 等，
    /// CONNECT 时作为 `distribution` 上报。浏览器（wasm）未配置时取 `web`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    pub connect_timeout_secs: Option<u64>,
    pub reconnect_interval_secs: Option<u64>,
    pub max_reconnect_attempts: Option<u32>,
    pub transport_policy: TransportPolicy,
    /// 非竞速时的首选传输；未设置时以 WebSocket 为主入口。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_transport: Option<TransportKind>,
    /// 协议竞速顺序（前项优先），如 `["quic", "websocket"]`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_race_order: Option<Vec<TransportKind>>,
    #[serde(default)]
    pub resource_profile: SdkResourceProfile,
    pub sync_batch_size: Option<u32>,
    pub init_message_sync_concurrency: Option<u32>,
    pub event_bus_capacity: Option<usize>,
    pub event_dedupe_capacity: Option<usize>,
    pub message_dedupe_capacity: Option<usize>,
    pub ack_timeout_secs: Option<u64>,
    pub ack_max_retries: Option<u32>,
    pub ack_max_in_flight: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_cert_path: Option<String>,
    #[serde(default)]
    pub tls_spki_sha256_pins: Vec<String>,
    #[serde(default)]
    pub tls_certificate_sha256_pins: Vec<String>,
    /// 内联的信任 CA 证书（PEM 文本或 base64 DER）。移动端没有可靠的文件路径可给 `tls_ca_cert_path`，
    /// 用它把自建 CA 直接塞进配置；与 `tls_ca_cert_path` 同时给时以本项为准。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_cert: Option<String>,
    pub enable_metrics: bool,
    #[serde(default, skip_serializing_if = "SdkAuthConfig::is_default")]
    pub auth: SdkAuthConfig,
}

impl SdkAuthConfig {
    fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

impl SdkConfig {
    /// 以给定 WebSocket 地址创建配置。
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: Some(ws_url.into()),
            ..Self::default()
        }
    }

    /// 返回配置构建器。
    pub fn builder() -> SdkConfigBuilder {
        SdkConfigBuilder::default()
    }

    /// 获取连接超时时间（秒），未配置时使用默认值。
    pub fn connect_timeout_secs(&self) -> u64 {
        self.connect_timeout_secs.unwrap_or(30)
    }
    /// 获取同步批大小，未配置时使用默认值。
    pub fn sync_batch_size(&self) -> u32 {
        self.runtime_resources().sync_batch_size
    }

    /// Init/重连阶段按会话补拉消息的并发上限（默认 4，最小 1）。
    pub fn init_message_sync_concurrency(&self) -> u32 {
        self.runtime_resources().init_message_sync_concurrency
    }

    pub fn runtime_resources(&self) -> SdkRuntimeResources {
        let defaults = self.resource_profile.defaults();
        SdkRuntimeResources {
            sync_batch_size: self
                .sync_batch_size
                .unwrap_or(defaults.sync_batch_size)
                .max(1),
            init_message_sync_concurrency: self
                .init_message_sync_concurrency
                .unwrap_or(defaults.init_message_sync_concurrency)
                .max(1),
            event_bus_capacity: self
                .event_bus_capacity
                .unwrap_or(defaults.event_bus_capacity)
                .max(1),
            event_dedupe_capacity: self
                .event_dedupe_capacity
                .unwrap_or(defaults.event_dedupe_capacity)
                .max(1),
            message_dedupe_capacity: self
                .message_dedupe_capacity
                .unwrap_or(defaults.message_dedupe_capacity)
                .max(1),
        }
    }

    /// Effective transport policy for the current compilation target.
    pub fn effective_transport_policy(&self) -> TransportPolicy {
        if cfg!(target_arch = "wasm32") {
            TransportPolicy::WebSocketOnly
        } else {
            self.transport_policy
        }
    }

    /// 协议竞速列表；需同时配置 `ws_url` 与 `quic_url`，且策略为 Auto / ProtocolRace。
    pub fn effective_protocol_race_order(&self) -> Option<Vec<CoreTransport>> {
        if cfg!(target_arch = "wasm32") {
            return None;
        }
        let policy = self.effective_transport_policy();
        if !matches!(
            policy,
            TransportPolicy::Auto | TransportPolicy::ProtocolRace
        ) {
            return None;
        }
        self.ws_url.as_ref()?;
        self.quic_url.as_ref()?;
        let order = self
            .protocol_race_order
            .clone()
            .unwrap_or_else(|| vec![TransportKind::Quic, TransportKind::WebSocket]);
        Some(order.into_iter().map(TransportKind::to_core).collect())
    }

    /// 建立长连接时的主 URL（竞速时仍作为 builder 入口，具体协议由 flare-core 选择）。
    pub fn primary_connect_url(&self) -> Option<String> {
        let ws = self.ws_url.clone()?;
        match self.default_transport {
            Some(TransportKind::Quic) => self.quic_url.clone().or(Some(ws)),
            _ => Some(ws),
        }
    }

    /// The configured private CA for other HTTPS endpoints of the same deployment
    /// (Social gateway, business APIs). Certificate pins stay with the IM
    /// connection: another endpoint may present a different certificate, and a
    /// trust anchor only widens what verifies. `None` without a configured CA.
    pub fn ca_trust_tls_config(&self) -> Option<CoreTlsConfig> {
        let mut tls = self.core_tls_config();
        if tls.ca_cert_path.is_none() && tls.ca_cert_data.is_none() {
            return None;
        }
        tls.spki_sha256_pins.clear();
        tls.certificate_sha256_pins.clear();
        Some(tls)
    }

    pub fn core_tls_config(&self) -> CoreTlsConfig {
        let mut tls = CoreTlsConfig::none()
            .with_spki_sha256_pins(self.tls_spki_sha256_pins.clone())
            .with_certificate_sha256_pins(self.tls_certificate_sha256_pins.clone());
        if let Some(path) = self
            .tls_ca_cert_path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
        {
            tls = tls.with_ca_cert(PathBuf::from(path));
        }
        if let Some(inline) = self
            .tls_ca_cert
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            match inline_ca_cert_der(inline) {
                Ok(der) => tls.ca_cert_data = Some(der),
                Err(err) => tracing::warn!(%err, "ignoring unparseable tls_ca_cert"),
            }
        }
        tls
    }

    /// 本次连接上报的设备标识。
    ///
    /// 优先用宿主配置的稳定值；缺省时回退到进程级恒定的临时值
    /// （见 [`process_fallback_device_id`]，会打一次 warn）。
    pub fn effective_device_id(&self) -> String {
        self.device_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| process_fallback_device_id().to_string())
    }

    /// CONNECT 上报的应用版本名：宿主给的非空值，没有就是 `None`（不上报）。
    pub fn effective_app_version(&self) -> Option<String> {
        non_blank(self.app_version.as_deref())
    }

    /// CONNECT 上报的分发渠道：宿主给的非空值优先；浏览器里的 wasm 构建只可能是 Web，
    /// 未配置时取 `web`；其余平台没给就不上报（安装来源 SDK 猜不准，不替宿主编）。
    pub fn effective_distribution(&self) -> Option<String> {
        non_blank(self.distribution.as_deref()).or_else(|| {
            if cfg!(target_arch = "wasm32") {
                Some("web".to_string())
            } else {
                None
            }
        })
    }
}

fn non_blank(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 默认配置不编造应用版本：版本、构建号、渠道都留空，由宿主或平台包填。
    #[test]
    fn a_default_config_names_no_app_version() {
        let config = SdkConfig::default();
        assert_eq!(config.app_version, None);
        assert_eq!(config.app_build, None);
        assert_eq!(config.distribution, None);
        assert_eq!(config.effective_app_version(), None);
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(config.effective_distribution(), None);
        let json = serde_json::to_value(&config).unwrap();
        for key in ["app_version", "app_build", "distribution"] {
            assert!(json.get(key).is_none(), "{key} in {json}");
        }
    }

    #[test]
    fn app_version_fields_ignore_blank_values() {
        let config = SdkConfig::builder()
            .app_version("  ")
            .distribution("")
            .build();
        assert_eq!(config.effective_app_version(), None);
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(config.effective_distribution(), None);

        let config = SdkConfig::builder()
            .app_version(" 3.2.1 ")
            .app_build(30201)
            .distribution("play")
            .build();
        assert_eq!(config.effective_app_version().as_deref(), Some("3.2.1"));
        assert_eq!(config.app_build, Some(30201));
        assert_eq!(config.effective_distribution().as_deref(), Some("play"));
    }

    #[test]
    fn protocol_race_order_respects_overlay() {
        let config = SdkConfig {
            ws_url: Some("ws://a".into()),
            quic_url: Some("quic://b".into()),
            transport_policy: TransportPolicy::ProtocolRace,
            protocol_race_order: Some(vec![TransportKind::WebSocket, TransportKind::Quic]),
            ..SdkConfig::default()
        };
        let order = config.effective_protocol_race_order().expect("race");
        assert_eq!(order[0], CoreTransport::WebSocket);
        assert_eq!(order[1], CoreTransport::QUIC);
    }

    #[test]
    fn mobile_resource_profile_uses_conservative_sync_budget() {
        let config = SdkConfig {
            resource_profile: SdkResourceProfile::Mobile,
            ..SdkConfig::default()
        };

        assert_eq!(config.runtime_resources().sync_batch_size, 80);
        assert_eq!(config.runtime_resources().init_message_sync_concurrency, 2);
        assert_eq!(config.runtime_resources().event_bus_capacity, 512);
        assert_eq!(config.runtime_resources().event_dedupe_capacity, 1024);
        assert_eq!(config.runtime_resources().message_dedupe_capacity, 2048);
        assert_eq!(config.sync_batch_size(), 80);
        assert_eq!(config.init_message_sync_concurrency(), 2);
    }

    #[test]
    fn explicit_resource_overrides_win_over_profile_defaults() {
        let config = SdkConfig {
            resource_profile: SdkResourceProfile::Mobile,
            sync_batch_size: Some(32),
            init_message_sync_concurrency: Some(1),
            event_bus_capacity: Some(128),
            event_dedupe_capacity: Some(256),
            message_dedupe_capacity: Some(512),
            ..SdkConfig::default()
        };

        assert_eq!(config.runtime_resources().sync_batch_size, 32);
        assert_eq!(config.runtime_resources().init_message_sync_concurrency, 1);
        assert_eq!(config.runtime_resources().event_bus_capacity, 128);
        assert_eq!(config.runtime_resources().event_dedupe_capacity, 256);
        assert_eq!(config.runtime_resources().message_dedupe_capacity, 512);
    }

    #[test]
    fn ca_trust_carries_the_ca_without_pins() {
        assert!(SdkConfig::default().ca_trust_tls_config().is_none());
        let pinned_only = SdkConfig {
            tls_spki_sha256_pins: vec!["spki-sha256/current".to_string()],
            ..SdkConfig::default()
        };
        assert!(pinned_only.ca_trust_tls_config().is_none());

        let config = SdkConfig {
            tls_ca_cert_path: Some("/tmp/flare-ca.crt".to_string()),
            tls_spki_sha256_pins: vec!["spki-sha256/current".to_string()],
            tls_certificate_sha256_pins: vec!["sha256/legacy".to_string()],
            ..SdkConfig::default()
        };
        let tls = config.ca_trust_tls_config().expect("CA configured");
        assert_eq!(
            tls.ca_cert_path.as_deref(),
            Some(std::path::Path::new("/tmp/flare-ca.crt"))
        );
        assert!(!tls.has_certificate_pins());
        assert!(tls.requires_custom_client_tls());
    }

    #[test]
    fn core_tls_config_carries_ca_cert_path_and_pins() {
        let config = SdkConfig {
            tls_ca_cert_path: Some("/tmp/flare-ca.crt".to_string()),
            tls_spki_sha256_pins: vec!["spki-sha256/current".to_string()],
            tls_certificate_sha256_pins: vec!["sha256/legacy".to_string()],
            ..SdkConfig::default()
        };

        let tls = config.core_tls_config();

        assert_eq!(
            tls.ca_cert_path.as_deref(),
            Some(std::path::Path::new("/tmp/flare-ca.crt"))
        );
        assert_eq!(tls.spki_sha256_pins, vec!["spki-sha256/current"]);
        assert_eq!(tls.certificate_sha256_pins, vec!["sha256/legacy"]);
        assert!(tls.has_certificate_pins());
    }

    #[test]
    fn effective_device_id_prefers_explicit_value_and_has_default() {
        let default_id = SdkConfig::default().effective_device_id();
        assert!(!default_id.trim().is_empty());

        let explicit = SdkConfig {
            device_id: Some("device-42".to_string()),
            ..SdkConfig::default()
        };
        assert_eq!(explicit.effective_device_id(), "device-42");
    }

    /// 兜底 device_id 必须在进程内恒定。此前每次调用新生成 → 每次重连都以「新设备」
    /// 上报（服务端设备表被刷入僵尸设备），且与登录时签进 token 的值必然不等
    /// （网关设备绑定校验拒连，逼出「device_id 传空」的绕过手法）。
    #[test]
    fn fallback_device_id_is_stable_within_the_process() {
        let first = SdkConfig::default().effective_device_id();
        let second = SdkConfig::default().effective_device_id();
        assert_eq!(
            first, second,
            "a device is one device: the fallback must not change between calls or reconnects"
        );
        assert_eq!(first, process_fallback_device_id());
        let blank = SdkConfig {
            device_id: Some("   ".to_string()),
            ..SdkConfig::default()
        };
        assert_eq!(blank.effective_device_id(), first);
    }

    /// 默认配置不替宿主编造设备：`device_id` 留空，连接取进程级兜底。默认值若自己铸一个 id，
    /// 每份默认配置就是一台新设备（兜底被绕开），推送与 token 绑定还会把这个临时 id 当成宿主
    /// 持久化的稳定值。
    #[test]
    fn a_default_config_names_no_device() {
        for config in [
            SdkConfig::default(),
            SdkConfig::new("ws://h/ws"),
            SdkConfig::builder().endpoint("ws://h/ws").build(),
        ] {
            assert_eq!(config.device_id, None);
            assert_eq!(config.effective_device_id(), process_fallback_device_id());
            let json = serde_json::to_value(&config).unwrap();
            assert!(json.get("device_id").is_none(), "{json}");
        }
        assert_eq!(
            SdkConfig::builder()
                .device_id("device-42")
                .build()
                .effective_device_id(),
            "device-42"
        );
    }
}

impl Default for SdkConfig {
    fn default() -> Self {
        Self {
            // The Core signaling gateway, as `lifecycle::default_ws_url` falls back to.
            ws_url: Some("ws://localhost:60051".into()),
            quic_url: None,
            http_url: Some("http://localhost:50050".into()),
            media_storage_proxy_prefix: None,
            media_storage_proxy_targets: Vec::new(),
            capability_url: Some("http://localhost:50110".into()),
            online_url: Some("http://localhost:50061".into()),
            tenant_id: Some("0".into()),
            // No device of the host's: the connection falls back to the process's id
            // (`effective_device_id`). Minting one here made every default config a new
            // device and passed a throwaway id off as the host's stable one.
            device_id: None,
            // 版本信息只取宿主的真实值；以前这里写死 "1.0.0"，服务端据此做的版本判断全是错的。
            app_version: None,
            app_build: None,
            distribution: None,
            connect_timeout_secs: Some(30),
            reconnect_interval_secs: Some(5),
            max_reconnect_attempts: None,
            transport_policy: TransportPolicy::Auto,
            default_transport: None,
            protocol_race_order: None,
            resource_profile: SdkResourceProfile::Desktop,
            sync_batch_size: None,
            init_message_sync_concurrency: None,
            event_bus_capacity: None,
            event_dedupe_capacity: None,
            message_dedupe_capacity: None,
            ack_timeout_secs: Some(10),
            ack_max_retries: Some(3),
            ack_max_in_flight: Some(RELIABLE_QUEUE_MAX_IN_FLIGHT),
            tls_ca_cert_path: None,
            tls_spki_sha256_pins: Vec::new(),
            tls_certificate_sha256_pins: Vec::new(),
            tls_ca_cert: None,
            enable_metrics: false,
            auth: SdkAuthConfig::default(),
        }
    }
}

#[derive(Default)]
pub struct SdkConfigBuilder {
    config: SdkConfig,
}

impl SdkConfigBuilder {
    /// 设置 WebSocket 入口地址。
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.config.ws_url = Some(url.into());
        self
    }
    /// 设置 QUIC 入口地址。
    pub fn quic_endpoint(mut self, url: impl Into<String>) -> Self {
        self.config.quic_url = Some(url.into());
        self
    }
    /// 设置 HTTP 入口地址（用于 REST/上传等扩展能力）。
    pub fn http_endpoint(mut self, url: impl Into<String>) -> Self {
        self.config.http_url = Some(url.into());
        self
    }
    /// 设置 capability gRPC 端点（`http://host:port`，与 `flare-capability` 或网关后端一致）。
    pub fn capability_endpoint(mut self, url: impl Into<String>) -> Self {
        self.config.capability_url = Some(url.into());
        self
    }
    /// 设置 signaling-online gRPC 端点（`http://host:port`）。
    pub fn online_endpoint(mut self, url: impl Into<String>) -> Self {
        self.config.online_url = Some(url.into());
        self
    }
    /// 设置默认租户 ID（能力授权 gRPC 与编排器默认 `0` 对齐）。
    pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.config.tenant_id = Some(tenant_id.into());
        self
    }

    /// 设置稳定设备 ID，用于多端在线、同步游标与服务端设备态。
    pub fn device_id(mut self, device_id: impl Into<String>) -> Self {
        self.config.device_id = Some(device_id.into());
        self
    }
    /// 设置宿主应用版本名（CONNECT 元数据 `app_version`）。
    pub fn app_version(mut self, version: impl Into<String>) -> Self {
        self.config.app_version = Some(version.into());
        self
    }
    /// 设置宿主应用构建号（单调整数，CONNECT 元数据 `app_build`）。
    pub fn app_build(mut self, build: u64) -> Self {
        self.config.app_build = Some(build);
        self
    }
    /// 设置分发渠道（CONNECT 元数据 `distribution`），如 `appstore` / `play` / `direct`。
    pub fn distribution(mut self, distribution: impl Into<String>) -> Self {
        self.config.distribution = Some(distribution.into());
        self
    }
    /// 设置连接超时（秒）。
    pub fn connect_timeout_secs(mut self, s: u64) -> Self {
        self.config.connect_timeout_secs = Some(s);
        self
    }
    /// 设置可靠发送 ACK 超时（秒）。
    pub fn ack_timeout_secs(mut self, s: u64) -> Self {
        self.config.ack_timeout_secs = Some(s);
        self
    }
    /// 设置可靠发送 ACK 最大重试次数。
    pub fn ack_max_retries(mut self, n: u32) -> Self {
        self.config.ack_max_retries = Some(n);
        self
    }
    /// 设置重连间隔（秒）。
    pub fn reconnect_interval_secs(mut self, s: u64) -> Self {
        self.config.reconnect_interval_secs = Some(s);
        self
    }
    /// 设置最大重连次数。
    pub fn max_reconnect_attempts(mut self, n: u32) -> Self {
        self.config.max_reconnect_attempts = Some(n);
        self
    }
    /// 设置传输策略。
    pub fn transport_policy(mut self, policy: TransportPolicy) -> Self {
        self.config.transport_policy = policy;
        self
    }
    /// 设置单次同步批大小。
    pub fn sync_batch_size(mut self, n: u32) -> Self {
        self.config.sync_batch_size = Some(n);
        self
    }
    /// 设置运行资源预算 profile。
    pub fn resource_profile(mut self, profile: SdkResourceProfile) -> Self {
        self.config.resource_profile = profile;
        self
    }
    /// 设置 Init/重连阶段按会话补拉消息的并发上限。
    pub fn init_message_sync_concurrency(mut self, n: u32) -> Self {
        self.config.init_message_sync_concurrency = Some(n);
        self
    }
    /// 设置 EventBus 有界广播容量。
    pub fn event_bus_capacity(mut self, n: usize) -> Self {
        self.config.event_bus_capacity = Some(n);
        self
    }
    /// 设置事件去重内存窗口容量。
    pub fn event_dedupe_capacity(mut self, n: usize) -> Self {
        self.config.event_dedupe_capacity = Some(n);
        self
    }
    /// 设置消息去重内存窗口容量。
    pub fn message_dedupe_capacity(mut self, n: usize) -> Self {
        self.config.message_dedupe_capacity = Some(n);
        self
    }
    /// 设置可靠发送队列最大在途消息数。
    pub fn ack_max_in_flight(mut self, n: usize) -> Self {
        self.config.ack_max_in_flight = Some(n);
        self
    }
    /// 设置客户端用于验证服务端证书的 CA/自签名证书路径。
    pub fn tls_ca_cert_path(mut self, path: impl Into<String>) -> Self {
        self.config.tls_ca_cert_path = Some(path.into());
        self
    }
    /// 设置 SPKI SHA-256 pins；可同时传当前 pin 与轮换 pin。
    pub fn tls_spki_sha256_pins(mut self, pins: Vec<String>) -> Self {
        self.config.tls_spki_sha256_pins = pins;
        self
    }
    /// 设置旧整证书 SHA-256 pins；新接入优先使用 [`Self::tls_spki_sha256_pins`]。
    pub fn tls_certificate_sha256_pins(mut self, pins: Vec<String>) -> Self {
        self.config.tls_certificate_sha256_pins = pins;
        self
    }
    /// 是否开启 SDK 指标采集。
    /// SDK 托管 token：登录不传 token 时向该网关签发，到期前自动刷新。
    pub fn auth_token_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.auth.token_endpoint = Some(endpoint.into());
        self
    }

    pub fn enable_metrics(mut self, b: bool) -> Self {
        self.config.enable_metrics = b;
        self
    }
    /// 产出最终配置对象。
    pub fn build(self) -> SdkConfig {
        self.config
    }
}

/// 内联 CA：PEM 文本原样交给 flare-core 转 DER；否则按 base64 DER 解码。
pub(crate) fn inline_ca_cert_der(value: &str) -> crate::shared::error::Result<Vec<u8>> {
    use base64::Engine as _;
    let trimmed = value.trim();
    if trimmed.starts_with("-----BEGIN") {
        // PEM→DER 依赖 flare-core 的 cert 模块，它按 target 只在非 wasm 编译
        // （浏览器走自身信任链、且 wasm 端不用 QUIC，本就不需要内联 CA）。
        #[cfg(not(target_arch = "wasm32"))]
        {
            return flare_core::common::cert::cert_bytes_to_der(trimmed.as_bytes().to_vec());
        }
        #[cfg(target_arch = "wasm32")]
        {
            return Err(crate::shared::error::FlareError::localized(
                crate::shared::error::ErrorCode::ConfigurationError,
                "PEM CA 证书在 wasm 端不支持内联转换；浏览器使用自身信任库，请勿在 web 端配置 tls_ca_cert",
            ));
        }
    }
    let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(compact.as_bytes())
        .map_err(|e| {
            crate::shared::error::FlareError::localized(
                crate::shared::error::ErrorCode::ConfigurationError,
                format!("tls_ca_cert is neither PEM nor base64 DER: {e}"),
            )
        })
}

#[cfg(test)]
mod inline_ca_tests {
    use super::*;

    #[test]
    fn inline_ca_accepts_pem_and_base64_der() {
        use base64::Engine as _;
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let der = cert.cert.der().to_vec();
        assert_eq!(inline_ca_cert_der(&cert.cert.pem()).unwrap(), der);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&der);
        assert_eq!(inline_ca_cert_der(&b64).unwrap(), der);
        assert!(inline_ca_cert_der("!!! not a cert !!!").is_err());

        let mut config = SdkConfig::new("ws://h/ws");
        config.tls_ca_cert = Some(b64);
        assert_eq!(
            config.core_tls_config().ca_cert_data.as_deref(),
            Some(der.as_slice())
        );
    }
}
