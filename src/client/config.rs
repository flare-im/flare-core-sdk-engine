use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use flare_core::common::config_types::TlsConfig as CoreTlsConfig;

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

/// 宿主应用信息（`app`）：CONNECT 时上报，更新中心按它判断渠道与版本。
///
/// 只用宿主（或平台包代采）给的真实值；没给的字段不上报，服务端看到缺省即知「未知」，
/// 不能用一个假值冒充。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkAppConfig {
    /// 版本名（如 `3.2.1`），CONNECT 时作为 `app_version` 上报，只用于展示。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// 构建号：单调递增的整数，服务端做版本比较（强制升级、灰度）只看它。CONNECT 时以十进制字符串
    /// 放进 `app_build`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<u64>,
    /// 分发渠道：`appstore`、`play`、`appgallery`、`direct`、`web`、`tauri` 等，CONNECT 时作为
    /// `distribution` 上报。浏览器（wasm）未配置时取 `web`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    /// 更新通道（如 `stable`、`beta`）：更新中心按它挑发布清单，不上报到 CONNECT。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Web 运行中的构建标识（如 `20261105-abc`）：Web 的 `build` 不单调可比，是否更新看它。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    /// 桌面目标（如 `darwin-aarch64`）：发布清单按它挑桌面制品。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// 界面语言（BCP-47，如 `zh-Hant-TW`）：更新说明按它选，回退链见 `flare_access::release::locale`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
}

impl SdkAppConfig {
    fn is_default(value: &Self) -> bool {
        *value == Self::default()
    }
}

/// SDK 配置
///
/// ```ignore
/// let config = SdkConfig::builder()
///     .access(AccessConfig::development(DevelopmentEndpoints {
///         ws_url: Some("ws://127.0.0.1:60051/ws".into()),
///         core_api_url: Some("http://127.0.0.1:60061".into()),
///         ..Default::default()
///     }))
///     .build();
/// ```
/// SDK 托管连接令牌（core-only 形态）：配了 `auth` 就是 SDK 托管——`login(user_id)` 不传 token 时，
/// 链路在每次建连前向 Core API 的签发接口签发（经 HttpRouter，换接入点与重试照 §3.9），到期前
/// `refresh_lead_secs` 秒用 `{签发路径}/refresh` 换新并 `update_access_token`。没配则必须显式传 token
/// （业务应用由宿主自己拿 token、自己刷新）。客户端永远不持有签名密钥。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SdkAuthConfig {
    /// 签发接口在 Core API 上的路径（以 `/` 开头的相对路径）；不给用 [`Self::DEFAULT_TOKEN_PATH`]。
    /// 基址就是接入清单里接入点的 Core API 入口（开发模式即 `developmentEndpoints.coreApiUrl`），
    /// 不接受绝对地址。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_endpoint: Option<String>,
    /// 到期前多少秒刷新，默认 300。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_lead_secs: Option<u64>,
}

impl SdkAuthConfig {
    pub const DEFAULT_REFRESH_LEAD_SECS: u64 = 300;
    /// Core 的标准签发路径（`flare-api-gateway`）。
    pub const DEFAULT_TOKEN_PATH: &'static str = "/api/v1/auth/tokens";

    pub fn refresh_lead(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.refresh_lead_secs
                .unwrap_or(Self::DEFAULT_REFRESH_LEAD_SECS)
                .max(5),
        )
    }

    /// 签发路径：配置的相对路径（去掉末尾斜杠），没配用标准路径。绝对地址或不以 `/` 开头的写法是
    /// 配置错误——签发只走 Core API。
    pub fn token_path(&self) -> crate::shared::error::Result<String> {
        let Some(path) = self
            .token_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
        else {
            return Ok(Self::DEFAULT_TOKEN_PATH.to_owned());
        };
        if !path.starts_with('/') || path.starts_with("//") || path.contains("://") {
            return Err(crate::shared::error::FlareError::localized(
                crate::shared::error::ErrorCode::ConfigurationError,
                format!(
                    "auth.tokenEndpoint must be a path on the Core API such as {} (got {path}); \
                     the address comes from the access configuration",
                    Self::DEFAULT_TOKEN_PATH
                ),
            ));
        }
        let path = path.trim_end_matches('/');
        Ok(if path.is_empty() {
            Self::DEFAULT_TOKEN_PATH.to_owned()
        } else {
            path.to_owned()
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkConfig {
    /// 接入配置：信任根、构建期种子与信任模式；开发模式下给出 `developmentEndpoints`。
    /// 连哪里、怎么切换全部由它装配出的目录与健康簿决定，SDK 里没有单地址配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<flare_access::AccessConfig>,
    /// 宿主应用信息（版本、构建号、分发渠道、更新通道）。
    #[serde(default, skip_serializing_if = "SdkAppConfig::is_default")]
    pub app: SdkAppConfig,
    pub tenant_id: Option<String>,
    /// Stable client device id used by connection metadata and sync cursors.
    ///
    /// Only the host's own value: `None` (the default) means the host named no
    /// device, and [`SdkConfig::effective_device_id`] then reports the one id of
    /// the current process or web page. Push and token binding need the host's
    /// persisted value and read this field, so a default never stands in for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    /// 单次握手（传输 + TLS + CONNECT_ACK）时限的本机覆盖（秒）。不设时用清单
    /// `clientPolicy.connectTimeoutMs`（默认 8 秒）；只在联调慢网络或测试时设置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_secs: Option<u64>,
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
    /// 私有部署的信任根（全局）：自建 CA 的证书路径。证书钉扎不在这里，按接入点来自接入清单
    /// （`spkiPins`），建连、REST 请求与探活时只对选中的那个接入点生效。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_cert_path: Option<String>,
    /// 内联的信任 CA 证书（PEM 文本或 base64 DER）。移动端没有可靠的文件路径可给 `tls_ca_cert_path`，
    /// 用它把自建 CA 直接塞进配置；与 `tls_ca_cert_path` 同时给时以本项为准。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca_cert: Option<String>,
    pub enable_metrics: bool,
    /// SDK 托管连接令牌；`None` 表示令牌由宿主给。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<SdkAuthConfig>,
}

impl SdkConfig {
    /// 以接入配置创建配置（其余取默认）。
    pub fn with_access(access: flare_access::AccessConfig) -> Self {
        Self {
            access: Some(access),
            ..Self::default()
        }
    }

    /// 返回配置构建器。
    pub fn builder() -> SdkConfigBuilder {
        SdkConfigBuilder::default()
    }

    /// 单次握手时限的本机覆盖（毫秒）；没设置时为 `None`，用清单的 `connectTimeoutMs`。
    pub fn connect_timeout_override_ms(&self) -> Option<u32> {
        self.connect_timeout_secs
            .map(|secs| (secs.clamp(2, 30) * 1_000) as u32)
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

    /// The configured private CA, for HTTPS endpoints outside the access
    /// manifest (a host-owned business gateway). `None` without a configured CA.
    pub fn ca_trust_tls_config(&self) -> Option<CoreTlsConfig> {
        let tls = self.core_tls_config();
        (tls.ca_cert_path.is_some() || tls.ca_cert_data.is_some()).then_some(tls)
    }

    /// 部署的信任根：系统根证书，加上配置的私有 CA（如有）。不含钉扎：接入点的 SPKI 钉扎来自接入
    /// 清单，由建连、REST 路由与探活在选中接入点后叠加上去。
    pub fn core_tls_config(&self) -> CoreTlsConfig {
        let mut tls = CoreTlsConfig::none();
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
        non_blank(self.app.version.as_deref())
    }

    /// 更新中心的发布渠道：宿主给的非空值，没有就是 `None`（用 `stable`）。
    pub fn effective_update_channel(&self) -> Option<String> {
        non_blank(self.app.channel.as_deref())
    }

    /// 运行中的 Web 构建：宿主给的非空值。
    pub fn effective_build_id(&self) -> Option<String> {
        non_blank(self.app.build_id.as_deref())
    }

    /// 桌面目标：宿主给的非空值。
    pub fn effective_update_target(&self) -> Option<String> {
        non_blank(self.app.target.as_deref())
    }

    /// 界面语言：宿主给的非空值。
    pub fn effective_locale(&self) -> Option<String> {
        non_blank(self.app.locale.as_deref())
    }

    /// CONNECT 上报的分发渠道：宿主给的非空值优先；浏览器里的 wasm 构建只可能是 Web，
    /// 未配置时取 `web`；其余平台没给就不上报（安装来源 SDK 猜不准，不替宿主编）。
    pub fn effective_distribution(&self) -> Option<String> {
        non_blank(self.app.distribution.as_deref()).or_else(|| {
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

    /// 默认配置不编造应用版本：版本、构建号、渠道都留空，由宿主或平台包填；也没有任何地址。
    #[test]
    fn a_default_config_names_no_app_version_and_no_address() {
        let config = SdkConfig::default();
        assert_eq!(config.app, SdkAppConfig::default());
        assert_eq!(config.effective_app_version(), None);
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(config.effective_distribution(), None);
        assert!(config.access.is_none());
        let json = serde_json::to_value(&config).unwrap();
        for key in ["app", "access", "ws_url", "http_url", "quic_url"] {
            assert!(json.get(key).is_none(), "{key} in {json}");
        }
    }

    #[test]
    fn app_fields_ignore_blank_values() {
        let config = SdkConfig::builder()
            .app(SdkAppConfig {
                version: Some("  ".into()),
                distribution: Some(String::new()),
                ..Default::default()
            })
            .build();
        assert_eq!(config.effective_app_version(), None);
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(config.effective_distribution(), None);

        let config = SdkConfig::builder()
            .app(SdkAppConfig {
                version: Some(" 3.2.1 ".into()),
                build: Some(30201),
                distribution: Some("play".into()),
                channel: Some("beta".into()),
                ..Default::default()
            })
            .build();
        assert_eq!(config.effective_app_version().as_deref(), Some("3.2.1"));
        assert_eq!(config.app.build, Some(30201));
        assert_eq!(config.effective_distribution().as_deref(), Some("play"));
    }

    /// 握手时限的本机覆盖夹在清单同样的上下限里；不设时用清单。
    #[test]
    fn connect_timeout_override_is_clamped() {
        assert_eq!(SdkConfig::default().connect_timeout_override_ms(), None);
        let config = SdkConfig {
            connect_timeout_secs: Some(1),
            ..SdkConfig::default()
        };
        assert_eq!(config.connect_timeout_override_ms(), Some(2_000));
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
    fn ca_trust_is_only_present_with_a_configured_ca() {
        assert!(SdkConfig::default().ca_trust_tls_config().is_none());

        let config = SdkConfig {
            tls_ca_cert_path: Some("/tmp/flare-ca.crt".to_string()),
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

    /// 全局配置只给信任根；钉扎按接入点来自清单，配置里没有入口。
    #[test]
    fn core_tls_config_carries_the_ca_and_never_pins() {
        let config = SdkConfig {
            tls_ca_cert_path: Some("/tmp/flare-ca.crt".to_string()),
            ..SdkConfig::default()
        };

        let tls = config.core_tls_config();

        assert_eq!(
            tls.ca_cert_path.as_deref(),
            Some(std::path::Path::new("/tmp/flare-ca.crt"))
        );
        assert!(!tls.has_certificate_pins());
        assert!(
            !SdkConfig::default()
                .core_tls_config()
                .requires_custom_client_tls()
        );
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
            SdkConfig::with_access(flare_access::AccessConfig::development(
                flare_access::DevelopmentEndpoints {
                    ws_url: Some("ws://h/ws".into()),
                    ..Default::default()
                },
            )),
            SdkConfig::builder().tenant_id("0").build(),
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
            // 没有内置地址：连哪里只由接入配置决定（开发模式给 developmentEndpoints）。
            access: None,
            // 版本信息只取宿主的真实值；以前这里写死 "1.0.0"，服务端据此做的版本判断全是错的。
            app: SdkAppConfig::default(),
            tenant_id: Some("0".into()),
            // No device of the host's: the connection falls back to the process's id
            // (`effective_device_id`). Minting one here made every default config a new
            // device and passed a throwaway id off as the host's stable one.
            device_id: None,
            connect_timeout_secs: None,
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
            tls_ca_cert: None,
            enable_metrics: false,
            auth: None,
        }
    }
}

#[derive(Default)]
pub struct SdkConfigBuilder {
    config: SdkConfig,
}

impl SdkConfigBuilder {
    /// 设置接入配置（信任根、种子、信任模式；开发模式下的开发端点）。
    pub fn access(mut self, access: flare_access::AccessConfig) -> Self {
        self.config.access = Some(access);
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
    /// 设置宿主应用信息（版本、构建号、分发渠道、更新通道）。
    pub fn app(mut self, app: SdkAppConfig) -> Self {
        self.config.app = app;
        self
    }
    /// 单次握手（含 CONNECT_ACK）时限的本机覆盖（秒，夹在 2–30）；不设时用清单的 `connectTimeoutMs`。
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
    /// SDK 托管连接令牌：登录不传 token 时向 Core API 签发，到期前自动换新（见 [`SdkAuthConfig`]）。
    pub fn auth(mut self, auth: SdkAuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }
    /// 是否开启 SDK 指标采集。
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

        let config = SdkConfig {
            tls_ca_cert: Some(b64),
            ..SdkConfig::default()
        };
        assert_eq!(
            config.core_tls_config().ca_cert_data.as_deref(),
            Some(der.as_slice())
        );
    }
}
