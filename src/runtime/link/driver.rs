//! 链路状态机对引擎的要求：建连、关连接、验活、上线后的发件箱与同步。由引擎实现，
//! 状态机只决定「何时、连哪里」，这样状态机可以用假驱动单测，引擎改动也不碰决策。

use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::time::Duration;

use flare_access::ApId;
use flare_core::client::link::{ConnectFailure, ConnectReport};
use flare_core::common::config_types::TransportProtocol;

use super::MaybeSendSync;

/// 跨平台的装箱 future：原生要能跨线程，浏览器 wasm 单线程不要求。
#[cfg(not(target_arch = "wasm32"))]
pub type LinkFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
#[cfg(target_arch = "wasm32")]
pub type LinkFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

#[cfg(not(target_arch = "wasm32"))]
pub fn boxed<T, F>(future: F) -> LinkFuture<T>
where
    F: Future<Output = T> + Send + 'static,
{
    Box::pin(future)
}

#[cfg(target_arch = "wasm32")]
pub fn boxed<T, F>(future: F) -> LinkFuture<T>
where
    F: Future<Output = T> + 'static,
{
    Box::pin(future)
}

/// 一条连接的心跳节奏，来自生效的 `clientPolicy.heartbeat`（开发模式即 `developmentEndpoints.clientPolicy`）。
/// 已由 [`flare_access::ClientPolicy::resolve`] 按 POLICY_BOUNDS 夹紧，并保证超时 ≥ 2 × 前台间隔、
/// 后台间隔 ≥ 前台间隔。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkHeartbeat {
    /// 前台心跳间隔（`intervalSec`）。
    pub interval: Duration,
    /// 多久收不到任何回应即判死（`timeoutSec`）。
    pub timeout: Duration,
    /// 后台心跳间隔（`backgroundIntervalSec`）：前后台切换时传输层按它换节奏。
    pub background_interval: Duration,
}

impl LinkHeartbeat {
    pub fn from_policy(policy: &flare_access::ClientPolicy) -> Self {
        let heartbeat = &policy.heartbeat;
        Self {
            interval: Duration::from_secs(u64::from(heartbeat.interval_sec)),
            timeout: Duration::from_secs(u64::from(heartbeat.timeout_sec)),
            background_interval: Duration::from_secs(u64::from(heartbeat.background_interval_sec)),
        }
    }
}

/// 一次建连的目标：一个接入点与它的协议入口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkTarget {
    pub access_point_id: ApId,
    /// 协议优先序与各自入口（第一个立即发起，交错间隔后再发起下一个）。
    pub protocols: Vec<(TransportProtocol, String)>,
    pub stagger: Duration,
    /// 单次握手（传输 + TLS + CONNECT_ACK）时限。
    pub connect_timeout: Duration,
    /// 这条连接上一次即时验活等 PONG 的窗口（`clientPolicy.probeWakeTimeoutMs`），窗口内没有 PONG 即判死。
    pub probe_timeout: Duration,
    /// 这条连接的心跳节奏（`clientPolicy.heartbeat`）。
    pub heartbeat: LinkHeartbeat,
    /// 该接入点的 SPKI 钉扎（来自接入清单；空表示只做 CA 校验）。
    pub spki_pins: Vec<String>,
    /// 该接入点的 `ipHints`：主机名 DNS 失败或结果可疑时才直连，TLS 仍按主机名。浏览器不能指定 IP，不起作用。
    pub ip_hints: Vec<IpAddr>,
    /// 客户端当前清单 serial（CONNECT 元数据）。
    pub access_serial: u64,
}

#[derive(Debug, Clone)]
pub struct OpenRequest {
    pub epoch: u64,
    pub user_id: String,
    pub token: String,
    pub target: LinkTarget,
    /// 新连接的心跳按当前前后台状态启动（后台心跳间隔更长），不必等宿主再报一次。
    pub foreground: bool,
    /// DoH 备用解析，和 REST、引导拉取共用一份。
    #[cfg(not(target_arch = "wasm32"))]
    pub doh: Option<std::sync::Arc<flare_core::client::dial::DohCache>>,
}

/// 新连接上线的方式：决定发件箱怎么恢复、同步怎么跑。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkUp {
    /// 本会话第一条连接：冷/热启动分类的首次同步。
    First,
    /// 断开后的新连接（切换线路或重连）：静默补同步，补发断链前发出的 ACK。
    Reconnected { lost_at_ms: Option<i64> },
    /// 连接没断（网络恢复后验活通过）：只恢复发件箱，非阻塞补齐。
    Resumed,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub(crate) trait LinkDriver: MaybeSendSync {
    /// 建一条新连接（交错竞速）。先关掉旧连接；监听器带 `request.epoch`。返回即已收到 CONNECT_ACK。
    async fn open(&self, request: OpenRequest) -> Result<ConnectReport, ConnectFailure>;

    /// 关闭当前连接（若有）。
    async fn close(&self);

    /// 请求对当前连接立即验活（不等结果）：验活窗口内没有 PONG，连接按心跳超时结束。
    fn probe(&self);

    /// 最近一次心跳往返。
    async fn heartbeat_round_trip(&self) -> Option<Duration>;

    /// 新连接已收到 CONNECT_ACK：恢复发件箱，在途与待发以原 `client_msg_id` 立即重发。
    ///
    /// 状态机在自己的执行线上等它完成，它与 [`Self::link_down`] 的先后就是事件的先后。放进与命令并发的
    /// 操作里，断网时的暂停可能排到它前面，发件箱就在离线时恢复了。
    async fn resume_outbox(&self);

    /// 新连接已就绪：补发 ACK、启动补同步（连接没断的 [`LinkUp::Resumed`] 还负责恢复发件箱）。
    /// 返回「补同步就绪」的等待（自带上限，不会无限挂住）。
    async fn link_up(&self, up: LinkUp) -> LinkFuture<()>;

    /// 连接不可用（判死、切换、无网络）：暂停发件箱，冻结在途超时计时。
    async fn link_down(&self);

    /// 会话的链路结束（断开、登出）：停止同步编排。
    async fn stop(&self);

    /// 最近一次发送或同步活动（Unix 毫秒），恢复切回据此判断自然边界。
    fn last_activity_ms(&self) -> i64;

    /// CONNECT_ACK 带回的服务端时间：修正时钟偏差，供令牌提前续期判断。
    fn observe_server_time(&self, server_time_ms: u64);
}
