use std::sync::Arc;
use std::time::Duration;

use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
use flare_core::client::link::{ConnectFailure, ConnectReport};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::protocol::SerializationFormat;
use flare_core::common::{HeartbeatAppState, HeartbeatConfig};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::client::config::SdkConfig;
use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
use crate::runtime::link::OpenRequest;
use crate::shared::error::Result;

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
    if let Some(build) = config.app.build {
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

/// 对当前长连接做一次即时验活：立即发一枚 ping，验活窗口（接入策略 `probeWakeTimeoutMs`，缺省 5 s）
/// 内收不到 PONG 就按心跳超时断开，之后由链路状态机换线路重连。
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

/// Socket 传输层：持有当前长连接的客户端槽位。建连参数（接入点、协议顺序、交错间隔、时限、代数）
/// 每次由链路状态机给出；这里只负责「按给定参数建一条连接」，从不自己重连。
#[derive(Clone)]
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

    pub fn config(&self) -> &SdkConfig {
        &self.config
    }

    /// 按链路状态机给出的目标建一条连接（交错竞速），返回即已收到 CONNECT_ACK。
    ///
    /// 先关掉旧连接再建新的：同设备两条 CONNECT 会被网关按「同设备替换」互踢，新连接的 CONNECT_ACK
    /// 也可能被拖到旧连接过期之后。
    pub(crate) async fn open(
        &self,
        request: &OpenRequest,
        listener: Arc<dyn MessageListener>,
    ) -> std::result::Result<ConnectReport, ConnectFailure> {
        let target = &request.target;
        let Some((_, primary_url)) = target.protocols.first() else {
            // 接入点没有本端可用的 IM 入口：不是网络失败，没有任何尝试。
            return Err(ConnectFailure {
                attempts: Vec::new(),
                skipped: Vec::new(),
                elapsed: Duration::ZERO,
            });
        };
        // 信任根（系统根 + 配置的私有 CA）加这个接入点自己的 SPKI 钉扎；清单没给钉扎时只做 CA 校验。
        // 钉扎不符在握手阶段失败，报成 TLS 失败计入该接入点的健康记录。
        // wasm 上 WebSocket 由浏览器建立、证书由浏览器校验，这里的 TLS 配置不生效，也就没有钉扎。
        let tls = self
            .config
            .core_tls_config()
            .with_spki_sha256_pins(target.spki_pins.clone());
        let app_state = if request.foreground {
            HeartbeatAppState::Foreground
        } else {
            HeartbeatAppState::Background
        };
        let mut builder = FlareClientBuilder::new(primary_url)
            .with_user_id(request.user_id.clone())
            .with_token(request.token.clone())
            .with_listener(listener)
            .with_device_info(sdk_device_info(&self.config))
            .with_format(SerializationFormat::Protobuf)
            .with_tls(tls)
            .with_heartbeat(link_heartbeat_config(
                target.heartbeat,
                target.probe_timeout,
                app_state,
            ))
            .with_connect_timeout(
                self.config
                    .connect_timeout_override_ms()
                    .map(|ms| Duration::from_millis(u64::from(ms)))
                    .unwrap_or(target.connect_timeout),
            )
            .with_race_stagger(target.stagger)
            .with_connection_epoch(request.epoch)
            .with_access_serial(target.access_serial)
            .with_ip_hints(target.ip_hints.clone());
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(doh) = &request.doh {
            builder = builder.with_doh(doh.clone());
        }
        for (protocol, url) in &target.protocols {
            builder = builder.with_protocol_url(*protocol, url.clone());
        }
        builder = builder.with_protocol_race(
            target
                .protocols
                .iter()
                .map(|(protocol, _)| *protocol)
                .collect(),
        );
        info!(
            access_point = %target.access_point_id,
            epoch = request.epoch,
            protocols = ?target.protocols.iter().map(|(protocol, _)| *protocol).collect::<Vec<_>>(),
            "connecting"
        );

        if let Some(old_client) = self.client.lock().await.take()
            && let Err(error) = old_client.disconnect().await
        {
            warn!(error = %error, "closing the previous socket before connecting failed");
        }
        let client = builder.build_with_race().await?;
        let report = client.connect_report().clone();
        *self.client.lock().await = Some(client);
        Ok(report)
    }

    /// 最近一次心跳往返（没有连接或还没有被应答的 ping 时为 `None`）。
    pub async fn heartbeat_round_trip(&self) -> Option<Duration> {
        let client = self.client.lock().await.as_ref().cloned()?;
        client.heartbeat_round_trip().await
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

/// 一条连接的心跳：节奏只来自生效的接入策略（`clientPolicy.heartbeat` 与 `probeWakeTimeoutMs`），宿主没有
/// 别的入口。自适应上限放到后台间隔，否则传输层的默认上限（120 s）会把更长的后台间隔压回去；下限沿用
/// `min(前台间隔, 15 s)`（NAT 超时较短时往下收敛的底）。
fn link_heartbeat_config(
    heartbeat: crate::runtime::link::LinkHeartbeat,
    probe_timeout: Duration,
    app_state: HeartbeatAppState,
) -> HeartbeatConfig {
    let floor = heartbeat.interval.min(Duration::from_secs(15));
    let ceiling = heartbeat.background_interval.max(heartbeat.interval);
    HeartbeatConfig::default()
        .with_interval(heartbeat.interval)
        .with_timeout(heartbeat.timeout)
        .with_background_interval(heartbeat.background_interval)
        .with_interval_bounds(floor, ceiling)
        .with_probe_timeout(probe_timeout)
        .with_app_state(app_state)
}

#[cfg(test)]
mod tests {
    /// 清单给了非默认心跳：前台、后台间隔、判死超时、验活窗口都按它，前后台切换换节奏。
    #[test]
    fn the_heartbeat_comes_from_the_access_policy() {
        let policy = flare_access::ClientPolicy::resolve(&serde_json::json!({
            "heartbeat": { "intervalSec": 45, "timeoutSec": 150, "backgroundIntervalSec": 300 },
            "probeWakeTimeoutMs": 7_000
        }));
        let heartbeat = crate::runtime::link::LinkHeartbeat::from_policy(&policy);
        let config = link_heartbeat_config(
            heartbeat,
            Duration::from_millis(u64::from(policy.probe_wake_timeout_ms)),
            HeartbeatAppState::Foreground,
        );
        assert_eq!(config.effective_interval(), Duration::from_secs(45));
        assert_eq!(config.timeout, Duration::from_secs(150));
        assert_eq!(config.probe_timeout, Duration::from_secs(7));
        assert_eq!(
            config.clone().background().effective_interval(),
            Duration::from_secs(300),
            "the background interval is not capped at the transport's default 120 s"
        );

        // 越界的值按 POLICY_BOUNDS 夹紧（间隔 ≥ 10 s、验活窗口 ≥ 2 s），超时至少两倍间隔。
        let clamped = flare_access::ClientPolicy::resolve(&serde_json::json!({
            "heartbeat": { "intervalSec": 1, "timeoutSec": 1, "backgroundIntervalSec": 1 },
            "probeWakeTimeoutMs": 10
        }));
        let config = link_heartbeat_config(
            crate::runtime::link::LinkHeartbeat::from_policy(&clamped),
            Duration::from_millis(u64::from(clamped.probe_wake_timeout_ms)),
            HeartbeatAppState::Foreground,
        );
        assert_eq!(config.effective_interval(), Duration::from_secs(10));
        assert!(config.timeout >= Duration::from_secs(20));
        assert_eq!(config.probe_timeout, Duration::from_secs(2));
    }

    use super::*;

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
            .app(crate::client::config::SdkAppConfig {
                version: Some("3.2.1".into()),
                build: Some(30201),
                distribution: Some("appstore".into()),
                channel: Some("stable".into()),
                ..Default::default()
            })
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
