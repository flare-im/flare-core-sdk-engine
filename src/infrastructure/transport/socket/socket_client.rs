use std::sync::Arc;
use std::time::Duration;

use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
#[cfg(not(target_arch = "wasm32"))]
use flare_core::common::config_types::TransportProtocol as CoreTransport;
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::protocol::SerializationFormat;
use flare_core::common::{HeartbeatAppState, HeartbeatConfig};
use tokio::sync::{Mutex, Notify};
use tracing::{info, warn};

use crate::client::config::SdkConfig;
use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::timeout;

#[cfg(not(target_arch = "wasm32"))]
fn native_device_platform(target_os: &str) -> DevicePlatform {
    // PlatformExclusive compares this value across installations. Classifying mobile
    // builds as PC would kick a different native platform when it signs in.
    match target_os {
        "ios" => DevicePlatform::IOS,
        "android" => DevicePlatform::Android,
        _ => DevicePlatform::PC,
    }
}

fn sdk_device_info(config: &SdkConfig) -> DeviceInfo {
    let device_id = config.effective_device_id();
    #[cfg(not(target_arch = "wasm32"))]
    let platform = native_device_platform(std::env::consts::OS);
    #[cfg(not(target_arch = "wasm32"))]
    let model = "FlareSDK".to_string();
    #[cfg(target_arch = "wasm32")]
    let platform = DevicePlatform::Web;
    #[cfg(target_arch = "wasm32")]
    let model = "FlareSDK-Web".to_string();

    let mut device = DeviceInfo::new(device_id, platform)
        .with_model(model)
        .with_metadata(
            "desired_conflict_strategy".to_string(),
            "platform_exclusive".to_string(),
        )
        // 引擎自身的版本，与宿主应用版本分开：服务端按它判断协议/能力兼容。
        .with_metadata(
            CONNECT_META_SDK_VERSION.to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );
    // 应用版本只报宿主给的真实值。以前这里写死 "1.0.0"，服务端拿到的版本全是假的，
    // 没法按版本做兼容判断、强制升级或排查问题；没给就不报，服务端据缺省即知「未知」。
    if let Some(version) = config.effective_app_version() {
        device = device.with_app_version(version);
    }
    if let Some(build) = config.app_build {
        device = device.with_metadata(CONNECT_META_APP_BUILD.to_string(), build.to_string());
    }
    if let Some(distribution) = config.effective_distribution() {
        device = device.with_metadata(CONNECT_META_DISTRIBUTION.to_string(), distribution);
    }
    device
}

/// CONNECT 元数据键（经 `DeviceInfo.metadata` 平铺进 CONNECT 帧的 metadata）。
/// 服务端按这几个键名读取并记日志，改名要两端同步。
const CONNECT_META_APP_BUILD: &str = "app_build";
const CONNECT_META_DISTRIBUTION: &str = "distribution";
const CONNECT_META_SDK_VERSION: &str = "sdk_version";

/// 建连失败时保留下来的错误码：服务端明确拒绝的类别原样上报，其余都是连接失败。
///
/// `SERVICE_UNAVAILABLE` 是服务端的暂时性拒绝（例如网关读不到租户状态）：既不是令牌问题，
/// 也不是租户被停用，应用与重连循环都应当退避重试，而不是提示重新登录或终止会话。
fn connect_failure_code(error: &FlareError) -> ErrorCode {
    match error.code() {
        Some(
            code @ (ErrorCode::AuthenticationFailed
            | ErrorCode::AuthenticationExpired
            | ErrorCode::AuthenticationInvalid
            | ErrorCode::TokenInvalid
            | ErrorCode::TokenExpired
            | ErrorCode::PermissionDenied
            | ErrorCode::ServiceUnavailable),
        ) => code,
        _ => ErrorCode::ConnectionFailed,
    }
}

/// 对当前长连接做一次即时验活：立即发一枚 ping，验活窗口（8 s）内收不到 PONG 就按心跳超时
/// 断开，之后由会话的重连监视器接手。
///
/// 用来戳穿「看起来连着、其实早已死掉」的半开连接：网络切换后旧路径上的连接常被中间设备静默
/// 丢弃，本端收不到任何关闭信号，只靠周期心跳要等一个完整的心跳超时（前台 90 s）才发现。
pub trait LinkProbe: Send + Sync {
    /// 请求一次验活，立即返回、不等结果；没有在用的连接时什么也不做。
    fn request_probe(&self);
}

/// 与 [`SocketTransport`] 共享同一客户端槽位的验活句柄：重连换了新客户端，它探的也是新连接。
#[derive(Clone)]
pub struct SocketLinkProbe {
    client: Arc<Mutex<Option<FlareClient>>>,
}

impl SocketLinkProbe {
    /// 对在用的客户端发起验活；没有客户端时返回 `false`，不做任何事。
    ///
    /// 只在取客户端快照时短暂持有槽位锁，验活本身不拿任何锁：发送等回执期间 flare-core 的
    /// 客户端锁一直被占着（`is_connected_async` 也要排在它后面），而那正是最需要验活的时候。
    /// 所以这里不再问一遍「是否已连」——调用方（引擎的 Ready 判断、发件箱的「帧确实发出去了」）
    /// 已经给出了理由；客户端其实没连上时，唤醒只会留到它下次心跳启动时探一次，无害。
    pub async fn probe_now(&self) -> bool {
        let Some(client) = self.client.lock().await.as_ref().cloned() else {
            return false;
        };
        client.probe_heartbeat();
        true
    }
}

impl LinkProbe for SocketLinkProbe {
    fn request_probe(&self) {
        let probe = self.clone();
        let _ = crate::shared::util::spawn_background_task(async move {
            if probe.probe_now().await {
                info!("link probe requested: waiting for PONG on the current connection");
            }
        });
    }
}

/// Socket 传输层 — 基于 flare-core 的多协议长连接封装
pub struct SocketTransport {
    config: SdkConfig,
    sender: Arc<PacketSender>,
    client: Arc<Mutex<Option<FlareClient>>>,
}

impl SocketTransport {
    /// 共享同一客户端槽位的验活句柄（发件箱用它在「发出后迟迟等不到回执」时验活）。
    pub fn link_probe(&self) -> SocketLinkProbe {
        SocketLinkProbe {
            client: self.client.clone(),
        }
    }

    /// 对当前连接做一次即时验活，见 [`LinkProbe`]；返回是否有客户端可探。
    pub async fn probe_link(&self) -> bool {
        self.link_probe().probe_now().await
    }

    pub fn new(config: SdkConfig) -> Self {
        Self::with_codec(config, Arc::new(ProtobufCodec))
    }

    pub fn with_codec(config: SdkConfig, codec: Arc<dyn Codec>) -> Self {
        let client: Arc<Mutex<Option<FlareClient>>> = Arc::new(Mutex::new(None));
        let sender = Arc::new(PacketSender::new(client.clone(), codec));
        Self {
            config,
            sender,
            client,
        }
    }

    pub fn sender(&self) -> &Arc<PacketSender> {
        &self.sender
    }

    pub async fn connect(
        &self,
        user_id: &str,
        token: &str,
        listener: Arc<dyn MessageListener>,
        ready_notify: Arc<Notify>,
    ) -> Result<()> {
        let device = sdk_device_info(&self.config);

        let ws_url = self.config.ws_url.as_deref().ok_or_else(|| {
            FlareError::localized(ErrorCode::ConfigurationError, "ws_url not configured")
        })?;
        let primary_url = self
            .config
            .primary_connect_url()
            .unwrap_or_else(|| ws_url.to_string());

        let mut builder = FlareClientBuilder::new(&primary_url)
            .with_user_id(user_id.to_string())
            .with_token(token.to_string())
            .with_listener(listener)
            .with_device_info(device)
            .with_format(SerializationFormat::Protobuf)
            .with_tls(self.config.core_tls_config())
            .with_connect_timeout(Duration::from_secs(self.config.connect_timeout_secs()))
            // 重连只走会话的重连监视器（新建 FlareClient）。flare-core 客户端默认会在断开后的下一次
            // 发送时自己重连：两条路径并行时，旧客户端带着建连时的令牌连上，监视器又新建一个，同一
            // 用户出现两条连接（网关按设备冲突处理），且旧客户端连上后没有重连后的同步。
            .with_reconnect_on_send(false);

        if let Some(secs) = self.config.reconnect_interval_secs {
            builder = builder.with_reconnect_interval(Duration::from_secs(secs));
        }
        if let Some(max) = self.config.max_reconnect_attempts {
            builder = builder.with_max_reconnect_attempts(Some(max));
        }

        let policy = self.config.effective_transport_policy();
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(race_order), Some(quic_url)) = (
            self.config.effective_protocol_race_order(),
            self.config.quic_url.as_ref(),
        ) {
            for protocol in &race_order {
                let url = match protocol {
                    CoreTransport::WebSocket => ws_url.to_string(),
                    CoreTransport::QUIC => quic_url.clone(),
                    _ => continue,
                };
                builder = builder.with_protocol_url(*protocol, url);
            }
            builder = builder.with_protocol_race(race_order.clone());
            info!(
                ws = ws_url,
                quic = %quic_url,
                race_order = ?race_order,
                policy = ?policy,
                "protocol race enabled"
            );
        } else {
            info!(
                ws = ws_url,
                primary = %primary_url,
                policy = ?policy,
                default_transport = ?self.config.default_transport,
                "single transport (WebSocket entry)"
            );
        }
        #[cfg(target_arch = "wasm32")]
        {
            info!(
                ws = ws_url,
                primary = %primary_url,
                policy = ?policy,
                "wasm websocket transport (flare-core FlareClientBuilder)"
            );
        }

        // Close the previous socket before opening the next one. Hot relogin and
        // reconnect paths must not briefly overlap two same-user connections,
        // because the gateway can legitimately delay the new CONNACK while it
        // waits for the old connection to expire.
        if let Some(old_client) = self.client.lock().await.take()
            && let Err(error) = old_client.disconnect().await
        {
            warn!(error = %error, "closing stale socket client before reconnect failed");
        }

        let flare_client = builder
            .build_with_race()
            .await
            .map_err(|e| {
                let detail = format!(
                    "connect failed primary={primary_url} ws={ws_url} policy={policy:?} default_transport={:?}: {e}",
                    self.config.default_transport
                );
                // 网关拒绝 CONNECT 时传输层已经按服务端给的原因分好类：令牌问题是
                // AUTHENTICATION_FAILED / TOKEN_*，租户不可用是 PERMISSION_DENIED。这里不能再一律
                // 降级成 CONNECTION_FAILED，否则应用层只能把「密钥填错」「租户被停用」都显示成
                // 「连不上服务器」，重连逻辑也分不清该换令牌还是该停手。
                FlareError::localized(connect_failure_code(&e), detail)
            })?;

        *self.client.lock().await = Some(flare_client);

        let wait = Duration::from_secs(self.config.connect_timeout_secs().max(1));
        if timeout(wait, ready_notify.notified()).await.is_err() {
            let timed_out_client = self.client.lock().await.take();
            if let Some(client) = timed_out_client
                && let Err(error) = client.disconnect().await
            {
                warn!(error = %error, "closing socket after CONNACK timeout failed");
            }
            return Err(FlareError::connection_failed(format!(
                "CONNACK not received within {wait:?}"
            )));
        }

        info!(user_id, "socket connected (CONNACK received)");
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<()> {
        if let Some(client) = self.client.lock().await.take()
            && let Err(error) = client.disconnect().await
        {
            warn!(error = %error, "socket client disconnect failed");
        }
        info!("socket disconnected");
        Ok(())
    }

    pub async fn update_heartbeat_config(&self, config: HeartbeatConfig) -> Result<()> {
        if let Some(client) = self.client.lock().await.as_ref() {
            client.update_heartbeat_config(config).await;
        }
        Ok(())
    }

    pub async fn set_heartbeat_app_state(&self, state: HeartbeatAppState) -> Result<()> {
        if let Some(client) = self.client.lock().await.as_ref() {
            client.set_heartbeat_app_state(state).await;
        }
        Ok(())
    }

    pub async fn set_heartbeat_nat_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        if let Some(client) = self.client.lock().await.as_ref() {
            client.set_heartbeat_nat_timeout(timeout).await;
        }
        Ok(())
    }

    pub async fn heartbeat_effective_interval(&self) -> Option<Duration> {
        match self.client.lock().await.as_ref() {
            Some(client) => Some(client.heartbeat_effective_interval().await),
            None => None,
        }
    }

    pub async fn is_connected(&self) -> bool {
        match self.client.lock().await.as_ref() {
            None => false,
            // 全平台走 async 版本。这里曾经在 native 分支调同步版 `is_connected()`，
            // 而那个同步版内部是 `block_in_place` + `block_on` ——
            // 在**已经是 async 的函数里**再阻塞一次：current-thread runtime 上直接
            // panic，多线程上则要为读一个布尔交接整个 worker 的任务队列。
            // 我们本来就在 async 上下文里，`.await` 是唯一没有代价的写法。
            Some(client) => client.is_connected_async().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 服务端给出的拒绝类别要一路带到应用层；只有真正的连不上才是 CONNECTION_FAILED。
    #[test]
    fn connect_failure_keeps_the_rejection_class() {
        let cases = [
            (
                FlareError::localized(
                    ErrorCode::PermissionDenied,
                    "TENANT_UNAVAILABLE: server rejected CONNECT",
                ),
                ErrorCode::PermissionDenied,
            ),
            (
                FlareError::authentication_failed("AUTHENTICATION_FAILED: server rejected CONNECT"),
                ErrorCode::AuthenticationFailed,
            ),
            // 服务端暂时放不了行（如读不到租户状态）：可重试，不能降级成连不上，也不是鉴权失败
            (
                FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "SERVICE_UNAVAILABLE: server rejected CONNECT",
                ),
                ErrorCode::ServiceUnavailable,
            ),
            (
                FlareError::connection_failed(
                    "server closed the connection before CONNECT_ACK without a reason",
                ),
                ErrorCode::ConnectionFailed,
            ),
            (
                FlareError::connection_timeout("Negotiation timeout"),
                ErrorCode::ConnectionFailed,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(connect_failure_code(&error), expected, "{error}");
        }
    }

    /// 连接状态查询绝不能在 async 上下文里再阻塞一次。
    ///
    /// 这条钉的是**源码形态**而不是运行时行为：真正的失败要连上网关、跑在
    /// current-thread runtime 上、等一个心跳周期才会暴露（表现为任务被 panic 打死、
    /// 心跳记账停更、连接自我判超时），单测里复现代价过高。
    /// 而形态判据是确定的：这个函数已经是 async，就必须用 `_async` 版本。
    #[test]
    fn is_connected_never_blocks_inside_async() {
        let source = include_str!("socket_client.rs");
        let start = source
            .find("pub async fn is_connected(")
            .expect("is_connected 必须存在");
        let body = &source[start..start + 700];
        assert!(
            body.contains("is_connected_async().await"),
            "async 上下文里必须 await 异步版本"
        );
        assert!(
            !body.contains("client.is_connected(),"),
            "同步版内部是 block_in_place + block_on，在 async 里调用会 panic 或占住 worker"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_device_platform_separates_mobile_platforms() {
        assert_eq!(native_device_platform("ios"), DevicePlatform::IOS);
        assert_eq!(native_device_platform("android"), DevicePlatform::Android);
        for os in ["macos", "windows", "linux", "freebsd"] {
            assert_eq!(native_device_platform(os), DevicePlatform::PC);
        }
        assert_eq!(
            sdk_device_info(&SdkConfig::default()).platform,
            native_device_platform(std::env::consts::OS)
        );
    }

    #[test]
    fn sdk_device_info_requests_platform_exclusive_sessions() {
        let info = sdk_device_info(&SdkConfig::default());

        assert_eq!(
            info.metadata.get("desired_conflict_strategy"),
            Some(&"platform_exclusive".to_string())
        );
    }

    /// 没配应用版本就不报：以前写死的 "1.0.0" 不能再出现；引擎版本始终报。
    #[test]
    fn sdk_device_info_reports_no_app_version_unless_configured() {
        let info = sdk_device_info(&SdkConfig::default());

        assert_eq!(info.app_version, None);
        assert!(!info.metadata.contains_key("app_build"));
        #[cfg(not(target_arch = "wasm32"))]
        assert!(!info.metadata.contains_key("distribution"));
        assert_eq!(
            info.metadata.get("sdk_version").map(String::as_str),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    /// 服务端按 `app_version` / `app_build` / `distribution` / `sdk_version` 这几个键读取，
    /// 构建号以十进制字符串上报。
    #[test]
    fn sdk_device_info_carries_the_configured_app_version() {
        let config = SdkConfig::builder()
            .app_version("3.2.1")
            .app_build(30201)
            .distribution("appstore")
            .build();
        let info = sdk_device_info(&config);

        assert_eq!(info.app_version.as_deref(), Some("3.2.1"));
        assert_eq!(
            info.metadata.get("app_build").map(String::as_str),
            Some("30201")
        );
        assert_eq!(
            info.metadata.get("distribution").map(String::as_str),
            Some("appstore")
        );
        assert_eq!(
            info.metadata.get("sdk_version").map(String::as_str),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            info.metadata
                .get("desired_conflict_strategy")
                .map(String::as_str),
            Some("platform_exclusive")
        );
    }

    /// 没有在用的连接时验活什么也不做，也不报「探过了」。
    #[tokio::test]
    async fn link_probe_is_a_no_op_without_a_connection() {
        let transport = SocketTransport::new(SdkConfig::default());
        assert!(!transport.probe_link().await);
        transport.link_probe().request_probe();
    }
}
