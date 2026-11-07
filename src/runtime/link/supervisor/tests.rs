//! 链路状态机单测：用假驱动（脚本化的建连结果）驱动真实的选路与健康簿（开发模式双接入点）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_access::{AccessConfig, DevelopmentAccessPoint, DevelopmentEndpoints};
use flare_core::client::link::{
    AttemptFailure, AttemptReport, CloseKind, ConnectAckInfo, ConnectFailure, ConnectReport,
    DisconnectReason, ServerClose, ServerRejection,
};
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::error::ErrorCode;

use super::*;
use crate::kernel::event::{EventBus, SdkEventKind};
use crate::runtime::link::{
    AccessStoreSource, ConnectionPhase, LinkFuture, LinkQuality, LinkUp, boxed,
};

type OpenResult = std::result::Result<ConnectReport, ConnectFailure>;

#[derive(Default)]
struct Script {
    /// 每个接入点依次返回的建连结果；没有脚本时成功。
    opens: HashMap<String, VecDeque<OpenResult>>,
    opened: Vec<(String, u64, String)>,
    /// 最近一次建连目标带的 `ipHints`。
    ip_hints: Vec<std::net::IpAddr>,
    link_ups: Vec<LinkUp>,
    link_downs: usize,
    probes: usize,
    /// 发件箱收到的暂停与恢复，按到达顺序：最后一条决定它现在是否在派发。
    outbox: Vec<&'static str>,
    /// 非空时 `link_up` 要等到许可才往下走（模拟它排在别的事件之后）。
    link_up_gate: Option<Arc<tokio::sync::Semaphore>>,
}

#[derive(Clone, Default)]
struct FakeDriver(Arc<Mutex<Script>>);

impl FakeDriver {
    fn script(&self, ap: &str, results: impl IntoIterator<Item = OpenResult>) {
        self.0
            .lock()
            .unwrap()
            .opens
            .entry(ap.to_owned())
            .or_default()
            .extend(results);
    }

    fn opened(&self) -> Vec<(String, u64, String)> {
        self.0.lock().unwrap().opened.clone()
    }

    fn ip_hints(&self) -> Vec<std::net::IpAddr> {
        self.0.lock().unwrap().ip_hints.clone()
    }

    fn link_ups(&self) -> Vec<LinkUp> {
        self.0.lock().unwrap().link_ups.clone()
    }

    fn link_downs(&self) -> usize {
        self.0.lock().unwrap().link_downs
    }

    fn probes(&self) -> usize {
        self.0.lock().unwrap().probes
    }

    fn outbox_running(&self) -> bool {
        self.0.lock().unwrap().outbox.last() == Some(&"resume")
    }

    fn hold_link_up(&self) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        self.0.lock().unwrap().link_up_gate = Some(gate.clone());
        gate
    }
}

fn ok_report() -> ConnectReport {
    ConnectReport {
        protocol: TransportProtocol::WebSocket,
        handshake: Duration::from_millis(40),
        elapsed: Duration::from_millis(40),
        ack: ConnectAckInfo {
            server_time_ms: Some(1_700_000_000_000),
            ..ConnectAckInfo::default()
        },
        failed: vec![],
        cancelled: vec![],
    }
}

fn unreachable() -> ConnectFailure {
    ConnectFailure {
        attempts: vec![AttemptReport {
            protocol: TransportProtocol::WebSocket,
            elapsed: Duration::from_millis(5),
            failure: AttemptFailure::Unreachable {
                detail: "refused".into(),
            },
        }],
        skipped: vec![],
        elapsed: Duration::from_millis(5),
    }
}

fn rejected(code: ErrorCode, reason: &str, retry_after_ms: Option<u64>) -> ConnectFailure {
    ConnectFailure {
        attempts: vec![AttemptReport {
            protocol: TransportProtocol::WebSocket,
            elapsed: Duration::from_millis(5),
            failure: AttemptFailure::Rejected(ServerRejection {
                code,
                reason: reason.into(),
                retry_after_ms,
                min_build: None,
                release_hint: None,
            }),
        }],
        skipped: vec![],
        elapsed: Duration::from_millis(5),
    }
}

#[async_trait::async_trait]
impl LinkDriver for FakeDriver {
    async fn open(
        &self,
        request: OpenRequest,
    ) -> std::result::Result<ConnectReport, ConnectFailure> {
        let mut script = self.0.lock().unwrap();
        let ap = request.target.access_point_id.to_string();
        script
            .opened
            .push((ap.clone(), request.epoch, request.token.clone()));
        script.ip_hints = request.target.ip_hints.clone();
        script
            .opens
            .get_mut(&ap)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| Ok(ok_report()))
    }

    async fn close(&self) {}

    fn probe(&self) {
        self.0.lock().unwrap().probes += 1;
    }

    async fn heartbeat_round_trip(&self) -> Option<Duration> {
        None
    }

    async fn resume_outbox(&self) {
        self.0.lock().unwrap().outbox.push("resume");
    }

    async fn link_up(&self, up: LinkUp) -> LinkFuture<()> {
        let gate = self.0.lock().unwrap().link_up_gate.clone();
        if let Some(gate) = gate {
            let _ = gate.acquire().await;
        }
        let mut script = self.0.lock().unwrap();
        script.link_ups.push(up);
        if up == LinkUp::Resumed {
            script.outbox.push("resume");
        }
        boxed(async {})
    }

    async fn link_down(&self) {
        let mut script = self.0.lock().unwrap();
        script.link_downs += 1;
        script.outbox.push("pause");
    }

    async fn stop(&self) {}

    fn last_activity_ms(&self) -> i64 {
        0
    }

    fn observe_server_time(&self, _server_time_ms: u64) {}
}

/// 令牌来源：给出 `token`；被拒后按 `renewals` 依次给新令牌。
struct Tokens {
    token: String,
    renewals: Mutex<VecDeque<Option<String>>>,
    renew_calls: Mutex<Vec<String>>,
}

impl Tokens {
    fn new(token: &str, renewals: impl IntoIterator<Item = Option<String>>) -> Arc<Self> {
        Arc::new(Self {
            token: token.into(),
            renewals: Mutex::new(renewals.into_iter().collect()),
            renew_calls: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait::async_trait]
impl LinkTokenSource for Tokens {
    async fn token_for_connect(&self) -> ConnectToken {
        ConnectToken::Ready(self.token.clone())
    }

    async fn renew_after_rejection(&self, rejected: &str) -> Option<String> {
        self.renew_calls.lock().unwrap().push(rejected.to_owned());
        self.renewals.lock().unwrap().pop_front().flatten()
    }
}

fn two_access_points() -> AccessConfig {
    AccessConfig::development(DevelopmentEndpoints {
        ws_url: Some("ws://127.0.0.1:1/ws".into()),
        access_points: vec![DevelopmentAccessPoint {
            id: "ap-b".into(),
            ws_url: Some("ws://127.0.0.1:2/ws".into()),
            ..Default::default()
        }],
        // 演练用的短退避（下限 500 ms）。
        client_policy: Some(serde_json::json!({
            "backoff": { "baseMs": 500, "capForegroundMs": 5_000 },
            "probeWakeTimeoutMs": 2_000
        })),
        ..Default::default()
    })
}

struct Harness {
    driver: FakeDriver,
    link: LinkHandle,
    bus: EventBus,
    rx: crate::kernel::event::EventReceiver,
}

fn harness() -> Harness {
    let driver = FakeDriver::default();
    let bus = EventBus::new();
    let rx = bus.subscribe_filter(SdkEventKind::Connection);
    let access = Arc::new(AccessRuntime::new(
        Some(two_access_points()),
        AccessStoreSource::memory(),
    ));
    let link = LinkHandle::spawn(Arc::new(driver.clone()), access, bus.clone(), true);
    Harness {
        driver,
        link,
        bus,
        rx,
    }
}

impl Harness {
    /// 等到出现某个对外阶段（最多 `within`），返回那次的状态。
    async fn wait_phase(&mut self, phase: ConnectionPhase, within: Duration) -> ConnectionStatus {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let event = tokio::time::timeout(remaining, self.rx.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "no {phase:?} within {within:?}; last status {:?}",
                        self.link.status()
                    )
                })
                .expect("bus open");
            if let SdkEvent::Connection(ConnectionEvent::Status(status)) = event
                && status.phase == phase
            {
                return status;
            }
        }
    }

    async fn next_switched(&mut self, within: Duration) -> AccessSwitched {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let event = tokio::time::timeout(remaining, self.rx.recv())
                .await
                .expect("access://switched")
                .expect("bus open");
            if let SdkEvent::Connection(ConnectionEvent::AccessSwitched(switched)) = event {
                return switched;
            }
        }
    }

    /// 等到 `link_up` 被调用过至少 `count` 次。
    async fn wait_link_ups(&self, count: usize, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        while self.driver.link_ups().len() < count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "link_up called {} times, wanted {count}; status {:?}",
                self.driver.link_ups().len(),
                self.link.status()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn lose(&self, epoch: u64, reason: DisconnectReason) {
        self.link.signals().send(LinkSignal::Lost { epoch, reason });
    }

    fn current_epoch(&self) -> u64 {
        self.link.signals().epoch().current()
    }
}

const SHORT: Duration = Duration::from_secs(3);

mod recovery;

#[tokio::test]
async fn start_goes_online_and_announces_the_directory() {
    let mut h = harness();
    let tokens = Tokens::new("t1", []);
    h.link.start("alice", tokens).await.expect("online");
    let online = h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert!(online.development_mode);
    assert!(online.access_point_id.is_some());
    assert_eq!(online.protocol.as_deref(), Some("ws"));
    assert_eq!(h.driver.link_ups(), vec![LinkUp::First]);
    assert_eq!(h.link.sdk_state(), SdkState::Ready);
    let _ = &h.bus;
}

/// 判死：立即换另一个接入点（不等退避、不重登），切换完成后发 access://switched；
/// 旧连接迟到的断开（旧代数）一律丢弃。
#[tokio::test]
async fn a_dead_link_switches_to_the_other_access_point_without_relogin() {
    let mut h = harness();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    let online = h.wait_phase(ConnectionPhase::Online, SHORT).await;
    let first_ap = online.access_point_id.clone().unwrap();
    let first_epoch = h.current_epoch();

    h.lose(first_epoch, DisconnectReason::HeartbeatTimeout);
    let switched = h.next_switched(SHORT).await;
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(switched.from.ap, first_ap);
    assert_ne!(switched.to.ap, first_ap);
    assert_eq!(switched.reason, SwitchReason::Failure);
    assert!(h.current_epoch() > first_epoch);
    assert!(h.driver.link_downs() >= 1, "发件箱在切换期间暂停");
    assert!(matches!(
        h.driver.link_ups().last(),
        Some(LinkUp::Reconnected {
            lost_at_ms: Some(_)
        })
    ));

    // 旧连接迟到的断开不能把新连接当成断了。
    let opened = h.driver.opened().len();
    h.lose(
        first_epoch,
        DisconnectReason::PeerClosed {
            detail: "late".into(),
        },
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.driver.opened().len(), opened);
    assert_eq!(h.link.status().phase, ConnectionPhase::Online);
}

/// 过载：同一个接入点等服务端给的窗口再试，不换入口。
#[tokio::test]
async fn overload_waits_on_the_same_access_point() {
    let mut h = harness();
    h.driver.script(
        "development",
        [Err(rejected(
            ErrorCode::Overloaded,
            "OVERLOADED",
            Some(600),
        ))],
    );
    h.driver.script(
        "ap-b",
        [Err(rejected(
            ErrorCode::Overloaded,
            "OVERLOADED",
            Some(600),
        ))],
    );
    h.link
        .start("alice", Tokens::new("t1", []))
        .await
        .expect("过载时链路保留会话、原地等，登录照常成功");
    let backoff = h.wait_phase(ConnectionPhase::Reconnecting, SHORT).await;
    assert!(backoff.next_retry_at.is_some());
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    let opened = h.driver.opened();
    assert_eq!(opened.len(), 2);
    assert_eq!(opened[0].0, opened[1].0, "过载后原地重试同一个接入点");
}

/// 计划性关闭：在窗口内重连，优先服务端建议的接入点。
#[tokio::test]
async fn drain_reconnects_inside_the_window_preferring_the_suggested_access_point() {
    let mut h = harness();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    let online = h.wait_phase(ConnectionPhase::Online, SHORT).await;
    let current = online.access_point_id.unwrap();
    let other = if current == "ap-b" {
        "development"
    } else {
        "ap-b"
    };
    let close = ServerClose::new(CloseKind::Drain)
        .with_retry_window(Duration::from_millis(200), Duration::from_millis(400))
        .with_prefer_ap([other]);
    let lost_at = tokio::time::Instant::now();
    h.lose(h.current_epoch(), DisconnectReason::ServerClose(close));
    let switched = h.next_switched(SHORT).await;
    assert!(
        lost_at.elapsed() >= Duration::from_millis(200),
        "不早于窗口"
    );
    assert_eq!(switched.to.ap, other);
    assert_eq!(switched.reason, SwitchReason::Drain);
}

/// 鉴权被拒：续期一次，换到不同令牌就用它重连；再被拒即终止（只能重新登录）。
#[tokio::test]
async fn auth_rejection_renews_once_then_terminates() {
    let mut h = harness();
    for ap in ["development", "ap-b"] {
        h.driver.script(
            ap,
            [
                Err(rejected(ErrorCode::TokenExpired, "TOKEN_EXPIRED", None)),
                Err(rejected(ErrorCode::TokenExpired, "TOKEN_EXPIRED", None)),
            ],
        );
    }
    let tokens = Tokens::new("old", [Some("new".to_owned())]);
    let result = h.link.start("alice", tokens.clone()).await;
    assert!(result.is_err());
    let terminated = h.wait_phase(ConnectionPhase::ActionRequired, SHORT).await;
    assert_eq!(terminated.reason.as_deref(), Some("expired"));
    assert_eq!(*tokens.renew_calls.lock().unwrap(), vec!["old".to_owned()]);
    let used: Vec<String> = h
        .driver
        .opened()
        .into_iter()
        .map(|(_, _, token)| token)
        .collect();
    assert_eq!(used, vec!["old".to_owned(), "new".to_owned()]);

    // 终止后网络变化、立即重试都不会再连。
    h.link.network_changed(true, true).await;
    h.link.reconnect_now().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(h.driver.opened().len(), 2);
}

#[tokio::test]
async fn upgrade_required_and_session_ending_kicks_terminate() {
    let mut h = harness();
    h.driver.script(
        "development",
        [Err(rejected(
            ErrorCode::UpgradeRequired,
            "UPGRADE_REQUIRED",
            None,
        ))],
    );
    h.driver.script(
        "ap-b",
        [Err(rejected(
            ErrorCode::UpgradeRequired,
            "UPGRADE_REQUIRED",
            None,
        ))],
    );
    let error = h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::UpgradeRequired));
    let status = h.wait_phase(ConnectionPhase::ActionRequired, SHORT).await;
    assert_eq!(status.reason.as_deref(), Some("upgradeRequired"));

    let mut h = harness();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    h.lose(
        h.current_epoch(),
        DisconnectReason::Kicked {
            reason: "DEVICE_CONFLICT".into(),
            ends_session: true,
        },
    );
    let status = h.wait_phase(ConnectionPhase::ActionRequired, SHORT).await;
    assert_eq!(status.reason.as_deref(), Some("kicked"));
}

/// 所有接入点都连不上：退避（去相关抖动），连续两轮后进入 Recovering（serviceUnavailable）。
#[tokio::test]
async fn exhausted_rounds_back_off_then_recover() {
    let mut h = harness();
    for ap in ["development", "ap-b"] {
        h.driver
            .script(ap, (0..4).map(|_| Err(unreachable())).collect::<Vec<_>>());
    }
    h.link
        .start("alice", Tokens::new("t1", []))
        .await
        .expect("连不上时链路保留会话、按退避重试，登录照常成功");
    let first = h.wait_phase(ConnectionPhase::Reconnecting, SHORT).await;
    assert_eq!(first.attempt, 1);
    let next = first.next_retry_at.unwrap() - crate::runtime::link::access::now_ms();
    assert!(next <= 1_600, "首次退避在 [base, 3·base]：{next} ms");
    let recovering = h
        .wait_phase(ConnectionPhase::ServiceUnavailable, Duration::from_secs(5))
        .await;
    assert_eq!(recovering.attempt, 2);
}

/// 补同步的启动排在断网之后：发件箱不能被它恢复。网络回来验活通过后才恢复。
#[tokio::test]
async fn a_late_link_up_does_not_resume_the_outbox_after_the_network_went_away() {
    let mut h = harness();
    let gate = h.driver.hold_link_up();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Syncing, SHORT).await;

    h.link.network_changed(false, false).await;
    h.wait_phase(ConnectionPhase::WaitingForNetwork, SHORT)
        .await;
    gate.add_permits(16);
    h.wait_link_ups(1, SHORT).await;
    assert!(
        !h.driver.outbox_running(),
        "offline, the outbox stays paused: {:?}",
        h.driver.0.lock().unwrap().outbox
    );

    h.link.network_changed(true, false).await;
    h.wait_link_ups(2, Duration::from_secs(5)).await;
    assert_eq!(h.driver.link_ups().last(), Some(&LinkUp::Resumed));
    assert!(h.driver.outbox_running(), "the probe passed: sending again");
}

/// 网络回来、验活窗口还没过又断了：到点不能恢复发件箱，等下一次网络回来。
#[tokio::test]
async fn the_network_dropping_inside_the_probe_window_keeps_the_outbox_paused() {
    let mut h = harness();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    h.link.network_changed(false, false).await;
    h.wait_phase(ConnectionPhase::WaitingForNetwork, SHORT)
        .await;

    h.link.network_changed(true, false).await;
    h.link.network_changed(false, false).await;
    // 验活窗口（2 s）加余量（1 s）过去。
    tokio::time::sleep(Duration::from_millis(3_300)).await;
    assert!(
        !h.driver.outbox_running(),
        "offline, the outbox stays paused: {:?}",
        h.driver.0.lock().unwrap().outbox
    );

    h.link.network_changed(true, false).await;
    h.wait_link_ups(2, Duration::from_secs(5)).await;
    assert!(h.driver.outbox_running(), "the probe passed: sending again");
}

/// 挂起时用户点了重试且真的连上了：设备仍报告没有网络，发件箱等网络回来。
#[tokio::test]
async fn connecting_while_the_device_reports_no_network_keeps_the_outbox_paused() {
    let mut h = harness();
    h.link.network_changed(false, false).await;
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::WaitingForNetwork, SHORT)
        .await;

    h.link.reconnect_now().await;
    h.wait_link_ups(1, SHORT).await;
    assert!(
        !h.driver.outbox_running(),
        "offline, the outbox stays paused: {:?}",
        h.driver.0.lock().unwrap().outbox
    );

    h.link.network_changed(true, false).await;
    h.wait_link_ups(2, Duration::from_secs(5)).await;
    assert!(h.driver.outbox_running(), "the probe passed: sending again");
}

/// 无网络：挂起、暂停发件箱；网络回来且连接还在：先验活，窗口内没判死就回到在线。
#[tokio::test]
async fn network_loss_suspends_and_recovery_probes_the_surviving_link() {
    let mut h = harness();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    let opened = h.driver.opened().len();

    h.link.network_changed(false, false).await;
    h.wait_phase(ConnectionPhase::WaitingForNetwork, SHORT)
        .await;
    assert_eq!(h.driver.link_downs(), 1);

    h.link.network_changed(true, false).await;
    let degraded = h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(
        degraded.quality,
        Some(LinkQuality::Poor),
        "验活期间链路标为较差"
    );
    assert_eq!(h.driver.probes(), 1);
    let good = h
        .wait_phase(ConnectionPhase::Online, Duration::from_secs(5))
        .await;
    assert_eq!(good.quality, Some(LinkQuality::Good));
    assert_eq!(h.driver.opened().len(), opened, "验活通过不重建连接");
    assert_eq!(h.driver.link_ups().last(), Some(&LinkUp::Resumed));
}

#[tokio::test]
async fn stop_returns_to_idle_and_ignores_the_old_connection() {
    let mut h = harness();
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    let epoch = h.current_epoch();
    h.link.stop().await;
    h.wait_phase(ConnectionPhase::Idle, SHORT).await;
    let opened = h.driver.opened().len();
    h.lose(epoch, DisconnectReason::HeartbeatTimeout);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(h.driver.opened().len(), opened);
    assert_eq!(h.link.status().phase, ConnectionPhase::Idle);
}

/// 过载拒绝转成的错误带着网关给的等待时间（日志与最后一次错误里能看到；登录本身照常成功）。
#[test]
fn an_overload_rejection_carries_the_retry_wait() {
    let error = connect_error(&rejected(ErrorCode::Overloaded, "OVERLOADED", Some(600)));
    assert_eq!(error.code(), Some(ErrorCode::Overloaded));
    assert_eq!(crate::shared::error::retry_after_ms(&error), Some(600));
    let other = connect_error(&rejected(
        ErrorCode::ServiceUnavailable,
        "SERVICE_UNAVAILABLE",
        Some(600),
    ));
    assert_eq!(crate::shared::error::retry_after_ms(&other), None);
}
