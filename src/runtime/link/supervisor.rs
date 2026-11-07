//! 链路状态机（§3.11）：会话里唯一能创建、关闭长连接的地方。
//!
//! 一个 actor、一条有界命令通道。所有「何时连、连哪里、失败后怎么办」都在这里串行决定：
//! 宿主的重连请求、网络变化、前后台、连接断开都只是送进来的命令或信号，不再有各自的重连路径
//! （以前的重连监视器、网络单飞重连、终止监视器里的重连分支并行时会出现两条连接、旧令牌复活连接）。
//!
//! 异步工作（装配目录、取令牌、建连、等补同步）放在单一的「进行中操作」槽里与命令并发等待，
//! 所以建连的 8 秒里也能立即响应断开、网络变化；换一个操作即取消上一个（丢弃 future 即取消竞速）。

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use flare_access::probe::ProbeTrigger;
use flare_access::select::{FailbackDecision, FailbackHold};
use flare_access::{
    ApId, ApStatus, AttemptKind, DirectoryChange, NeutralReason, Observation, Outcome, Protocol,
    ServiceClass, ServiceKind,
};
use flare_core::client::link::{ConnectFailure, ConnectReport, DisconnectReason};
use rand::Rng;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::kernel::SdkState;
use crate::kernel::event::{ConnectionEvent, EventBus, SdkEvent};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::{BackgroundTask, delay, spawn_background, spawn_background_task};

use super::access::{AccessRuntime, AccessUnavailable, ConnectAckSummary, CurrentLink, now_ms};
use super::driver::{
    LinkDriver, LinkFuture, LinkHeartbeat, LinkTarget, LinkUp, OpenRequest, boxed,
};
use super::epoch::ConnectionEpoch;
use super::failover::{
    Backoff, FailoverAction, FailoverEvent, LinkLoss, RejectionKind, ReportRecord, RetryContext,
    attempt_observation, between, classify_loss, classify_rejection, decide, duration_ms,
    from_transport, plan_retry, report_records, to_transport,
};
use super::status::{
    AccessEndpoint, AccessSwitched, ActionRequiredReason, ConnectionStatus, LinkFacts, LinkPhase,
    SwitchReason, project,
};
use super::token::{ConnectToken, LinkTokenSource};

/// 命令通道容量：命令来自宿主 API（重连、网络、前后台），量小；满了说明 actor 卡住，调用方等待即可。
const COMMAND_CAPACITY: usize = 32;
/// 连接信号通道容量：每条连接至多一次断开，外加低频的 RTT 采样。
const SIGNAL_CAPACITY: usize = 32;
/// 在线时的例行检查：采样心跳 RTT、健康簿落盘、恢复切回判定。
const MAINTENANCE_INTERVAL_MS: i64 = 10_000;
/// 两次恢复切回判定的最小间隔（判定本身还要求距上次切换 ≥ 10 分钟）。
const FAILBACK_CHECK_INTERVAL_MS: i64 = 60_000;
/// 心跳往返超过它视为链路较差（只影响 `quality`，不显示横幅）。
const POOR_RTT_MS: u32 = 1_500;
/// 验活窗口之外再留的余量：窗口内没有判死就认为收到了 PONG。
const PROBE_GRACE_MS: i64 = 1_000;
/// 新清单把当前接入点标为 draining / disabled：在这个窗口里随机时刻迁走，别让全网同时重连。
const MANIFEST_DRAIN_WINDOW_MS: (u64, u64) = (1_000, 60_000);

/// 送给状态机的命令。
pub(crate) enum LinkCommand {
    Start {
        user_id: String,
        tokens: Arc<dyn LinkTokenSource>,
        reply: oneshot::Sender<Result<()>>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
    /// 横幅「立即重试」：重置退避，立即一轮。
    ReconnectNow,
    Network {
        available: bool,
        /// 网络指纹变了（切了 Wi-Fi / 蜂窝）。
        changed: bool,
    },
    AppState {
        foreground: bool,
    },
    /// 手动固定变了：当前接入点不是固定的那个就切过去。
    Repin,
}

/// 连接送来的信号（监听器、采样任务）。带连接代数：旧连接的一律丢弃。
#[derive(Debug, Clone)]
pub(crate) enum LinkSignal {
    Lost {
        epoch: u64,
        reason: DisconnectReason,
    },
    RoundTrip {
        epoch: u64,
        rtt: Duration,
    },
}

/// 监听器持有的信号出口：发送永不阻塞传输层的读循环。
#[derive(Clone)]
pub(crate) struct LinkSignals {
    tx: mpsc::Sender<LinkSignal>,
    epoch: ConnectionEpoch,
}

impl LinkSignals {
    pub fn epoch(&self) -> &ConnectionEpoch {
        &self.epoch
    }

    /// 测试用：一个独立的信号通道与代数。
    #[cfg(test)]
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<LinkSignal>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                epoch: ConnectionEpoch::new(),
            },
            rx,
        )
    }

    /// 发一个信号。通道满时转到后台任务排队，绝不在传输层的读循环里等：
    /// 关旧连接时传输层会等读循环收尾，而读循环若卡在这里等状态机，状态机又在等关连接，就互相等死。
    pub fn send(&self, signal: LinkSignal) {
        if let Err(mpsc::error::TrySendError::Full(signal)) = self.tx.try_send(signal) {
            let tx = self.tx.clone();
            spawn_background(async move {
                let _ = tx.send(signal).await;
            });
        }
    }
}

/// actor 的任务；最后一个持有者释放时中止。
struct WorkerGuard(BackgroundTask);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// 状态机句柄。
#[derive(Clone)]
pub(crate) struct LinkHandle {
    commands: mpsc::Sender<LinkCommand>,
    signals: LinkSignals,
    status: Arc<ArcSwap<ConnectionStatus>>,
    /// 状态机用的接入运行时（宿主的状态、诊断、固定接入点 API 必须读写同一份）。
    access: Arc<AccessRuntime>,
    _worker: Arc<WorkerGuard>,
}

impl LinkHandle {
    /// 启动状态机。`bus` 是会话的事件总线（`connection://status` 等从这里发出）。
    ///
    /// `network_available` 是宿主此前报过的网络可用性（没报过为 `true`）：平台常在登录前首报、之后对
    /// 相同状态去重，新建的状态机必须从它起步，否则离线时登录看不到「等网络」。
    pub fn spawn(
        driver: Arc<dyn LinkDriver>,
        access: Arc<AccessRuntime>,
        bus: EventBus,
        network_available: bool,
    ) -> Self {
        let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (signals_tx, signals_rx) = mpsc::channel(SIGNAL_CAPACITY);
        let epoch = ConnectionEpoch::new();
        let development_mode = access.development_mode();
        let changes = access.subscribe_changes();
        let status = Arc::new(ArcSwap::from_pointee(ConnectionStatus::idle(
            development_mode,
        )));
        let signals = LinkSignals {
            tx: signals_tx.clone(),
            epoch: epoch.clone(),
        };
        let supervisor = Supervisor {
            driver,
            access: access.clone(),
            bus,
            status: status.clone(),
            epoch,
            commands: commands_rx,
            signals: signals_rx,
            changes,
            signal_tx: signals_tx,
            phase: LinkPhase::Idle,
            facts: LinkFacts {
                development_mode,
                ..LinkFacts::default()
            },
            session: None,
            live: None,
            round: None,
            op: None,
            timer: None,
            retry_at: None,
            pending_prefer: Vec::new(),
            pending_only: None,
            degraded_until: None,
            degraded_by_rtt: false,
            resume_after_probe: false,
            next_maintenance: None,
            next_failback_check: 0,
            backoff: Backoff::default(),
            failed_rounds: 0,
            foreground: true,
            network_available,
            link_lost_at: None,
            last_switch_ms: 0,
            switch_from: None,
            last_rtt: None,
            published: None,
            drained: Vec::new(),
            manifest_move_at: None,
        };
        let worker = spawn_background_task(supervisor.run());
        Self {
            commands: commands_tx,
            signals,
            status,
            access,
            _worker: Arc::new(WorkerGuard(worker)),
        }
    }

    pub fn access(&self) -> &Arc<AccessRuntime> {
        &self.access
    }

    /// 监听器用的信号出口。
    pub fn signals(&self) -> LinkSignals {
        self.signals.clone()
    }

    async fn command(&self, command: LinkCommand) {
        if self.commands.send(command).await.is_err() {
            tracing::warn!("link supervisor is gone; command dropped");
        }
    }

    /// 开始（或继续）会话的链路。答复在两种时候到来：
    /// - `Ok`：已上线（收到 CONNECT_ACK），或首轮没连上但链路保留会话、自己按退避重试（没网、所有接入点
    ///   都连不上、服务端限流、接入清单或令牌暂时拿不到——即 [`crate::shared::error::link_keeps_session`]
    ///   那些）。在不在线只由 `connection://status` 表达。
    /// - `Err`：链路无法继续（鉴权失败且续期没换到、需要升级、配置错误、被踢、会话被停）。
    pub async fn start(&self, user_id: &str, tokens: Arc<dyn LinkTokenSource>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.command(LinkCommand::Start {
            user_id: user_id.to_owned(),
            tokens,
            reply,
        })
        .await;
        rx.await.unwrap_or_else(|_| {
            Err(FlareError::localized(
                ErrorCode::NotConnected,
                "link stopped before the connection was established",
            ))
        })
    }

    pub async fn stop(&self) {
        let (reply, rx) = oneshot::channel();
        self.command(LinkCommand::Stop { reply }).await;
        let _ = rx.await;
    }

    pub async fn reconnect_now(&self) {
        self.command(LinkCommand::ReconnectNow).await;
    }

    pub async fn network_changed(&self, available: bool, changed: bool) {
        self.command(LinkCommand::Network { available, changed })
            .await;
    }

    pub async fn app_state(&self, foreground: bool) {
        self.command(LinkCommand::AppState { foreground }).await;
    }

    pub async fn repin(&self) {
        self.command(LinkCommand::Repin).await;
    }

    pub fn status(&self) -> ConnectionStatus {
        (**self.status.load()).clone()
    }

    pub fn sdk_state(&self) -> SdkState {
        self.status.load().sdk_state()
    }
}

/// 进行中的异步操作的结果。
enum OpResult {
    Directory(std::result::Result<(), AccessUnavailable>),
    Token(ConnectToken),
    Renewed(Option<String>),
    Opened {
        epoch: u64,
        target: LinkTarget,
        // 建连报告比其它结果大一个数量级：装箱，免得每次唤醒都搬一个大枚举。
        result: Box<std::result::Result<ConnectReport, ConnectFailure>>,
    },
    Synced {
        epoch: u64,
    },
}

struct Session {
    user_id: String,
    tokens: Arc<dyn LinkTokenSource>,
    start_reply: Option<oneshot::Sender<Result<()>>>,
    online_once: bool,
    /// 本次恢复里是否已因鉴权被拒续期过：只续期一次，再被拒即终止。上线后清零。
    auth_renewed: bool,
    /// 本会话是否已发过 `access://changed`。
    announced_directory: bool,
}

struct LiveLink {
    epoch: u64,
    access_point_id: ApId,
    protocol: Protocol,
    /// 已收到 CONNECT_ACK 但补同步未完成。
    syncing: bool,
}

struct Round {
    token: Option<String>,
    tried: Vec<String>,
    prefer: Vec<String>,
    only: Option<String>,
    last_error: Option<FlareError>,
}

struct Supervisor {
    driver: Arc<dyn LinkDriver>,
    access: Arc<AccessRuntime>,
    bus: EventBus,
    status: Arc<ArcSwap<ConnectionStatus>>,
    epoch: ConnectionEpoch,
    commands: mpsc::Receiver<LinkCommand>,
    signals: mpsc::Receiver<LinkSignal>,
    /// 采纳了新清单（刷新任务或导入恢复链接）。
    changes: broadcast::Receiver<DirectoryChange>,
    signal_tx: mpsc::Sender<LinkSignal>,

    phase: LinkPhase,
    facts: LinkFacts,
    session: Option<Session>,
    live: Option<LiveLink>,
    round: Option<Round>,
    op: Option<LinkFuture<OpResult>>,
    timer: Option<(i64, LinkFuture<()>)>,
    retry_at: Option<i64>,
    pending_prefer: Vec<String>,
    pending_only: Option<String>,
    degraded_until: Option<i64>,
    degraded_by_rtt: bool,
    /// 验活通过后要恢复发件箱（挂起期间连接没断）。
    resume_after_probe: bool,
    next_maintenance: Option<i64>,
    next_failback_check: i64,
    backoff: Backoff,
    failed_rounds: u32,
    foreground: bool,
    network_available: bool,
    link_lost_at: Option<i64>,
    last_switch_ms: i64,
    switch_from: Option<(AccessEndpoint, SwitchReason)>,
    last_rtt: Option<Duration>,
    published: Option<ConnectionStatus>,
    /// 发来排空 / 停机 CLOSE 的接入点与放一边的截止时刻：期间不选它（还有别的候选时），见
    /// [`Supervisor::hold_drained`]。
    drained: Vec<(String, i64)>,
    /// 新清单让当前接入点不再 active：到这个时刻迁走。
    manifest_move_at: Option<i64>,
}

enum Wake {
    Command(Option<LinkCommand>),
    Signal(Option<LinkSignal>),
    Op(OpResult),
    Directory(DirectoryChange),
    Timer,
}

async fn poll_op(slot: &mut Option<LinkFuture<OpResult>>) -> OpResult {
    match slot.as_mut() {
        Some(future) => {
            let result = future.await;
            *slot = None;
            result
        }
        None => std::future::pending().await,
    }
}

async fn poll_timer(slot: &mut Option<(i64, LinkFuture<()>)>) {
    match slot.as_mut() {
        Some((_, future)) => {
            future.await;
            *slot = None;
        }
        None => std::future::pending().await,
    }
}

/// 下一次目录变更。落后了也没关系：目录是整体替换的，读到的总是最新的，下一条变更照常会来。
async fn next_change(changes: &mut broadcast::Receiver<DirectoryChange>) -> DirectoryChange {
    loop {
        match changes.recv().await {
            Ok(change) => return change,
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => std::future::pending::<()>().await,
        }
    }
}

fn unit() -> f64 {
    rand::thread_rng().r#gen::<f64>()
}

fn action_error(reason: ActionRequiredReason) -> FlareError {
    let (code, message) = match reason {
        ActionRequiredReason::Kicked => (
            ErrorCode::AuthenticationExpired,
            "signed in on another device; sign in again to continue",
        ),
        ActionRequiredReason::Expired => (
            ErrorCode::AuthenticationFailed,
            "the connect token was rejected and could not be renewed",
        ),
        ActionRequiredReason::UpgradeRequired => (
            ErrorCode::UpgradeRequired,
            "the server requires a newer app build",
        ),
        ActionRequiredReason::TenantUnavailable => (
            ErrorCode::PermissionDenied,
            crate::shared::error::TENANT_UNAVAILABLE,
        ),
    };
    FlareError::localized(code, message)
}

impl Supervisor {
    async fn run(mut self) {
        loop {
            self.arm_timer();
            let wake = tokio::select! {
                command = self.commands.recv() => Wake::Command(command),
                signal = self.signals.recv() => Wake::Signal(signal),
                result = poll_op(&mut self.op) => Wake::Op(result),
                change = next_change(&mut self.changes) => Wake::Directory(change),
                () = poll_timer(&mut self.timer) => Wake::Timer,
            };
            match wake {
                Wake::Command(None) => break,
                Wake::Command(Some(command)) => self.on_command(command).await,
                Wake::Signal(Some(signal)) => self.on_signal(signal).await,
                Wake::Signal(None) => {}
                Wake::Op(result) => self.on_op(result).await,
                Wake::Directory(change) => self.on_directory_changed(change).await,
                Wake::Timer => self.on_timer().await,
            }
        }
        // 所有句柄都已释放：会话没了，收尾关掉连接。
        self.epoch.advance();
        self.driver.close().await;
        tracing::debug!("link supervisor stopped");
    }

    // ---------------------------------------------------------------- 状态与事件

    fn set_phase(&mut self, phase: LinkPhase) {
        self.phase = phase;
        self.publish();
    }

    fn publish(&mut self) {
        let status = project(self.phase, &self.facts);
        if self.published.as_ref() == Some(&status) {
            return;
        }
        tracing::debug!(phase = status.phase.as_str(), access_point = ?status.access_point_id, attempt = status.attempt, "connection status");
        self.status.store(Arc::new(status.clone()));
        self.published = Some(status.clone());
        self.bus
            .publish(SdkEvent::Connection(ConnectionEvent::Status(status)));
    }

    fn answer_start(&mut self, result: Result<()>) {
        if let Some(reply) = self
            .session
            .as_mut()
            .and_then(|session| session.start_reply.take())
        {
            let _ = reply.send(result);
        }
    }

    fn arm_timer(&mut self) {
        let next = [
            self.retry_at,
            self.degraded_until,
            self.next_maintenance,
            self.manifest_move_at,
        ]
        .into_iter()
        .flatten()
        .min();
        match next {
            None => self.timer = None,
            Some(at) => {
                if self.timer.as_ref().map(|(armed, _)| *armed) != Some(at) {
                    let wait = (at - now_ms()).max(0) as u64;
                    self.timer = Some((at, boxed(delay(Duration::from_millis(wait)))));
                }
            }
        }
    }

    // ---------------------------------------------------------------- 命令

    async fn on_command(&mut self, command: LinkCommand) {
        match command {
            LinkCommand::Start {
                user_id,
                tokens,
                reply,
            } => self.on_start(user_id, tokens, reply).await,
            LinkCommand::Stop { reply } => {
                self.stop_session().await;
                let _ = reply.send(());
            }
            LinkCommand::ReconnectNow => {
                if self.session.is_none() {
                    return;
                }
                self.access.wake_refresh();
                match self.phase {
                    LinkPhase::Backoff | LinkPhase::Recovering | LinkPhase::Suspended => {
                        self.backoff.reset();
                        self.failed_rounds = 0;
                        self.begin_round(Vec::new(), None);
                    }
                    // 已在连接、已在线或已终止（只能由重新登录离开）：不打断。
                    _ => {}
                }
            }
            LinkCommand::Network { available, changed } => {
                self.on_network(available, changed).await;
            }
            LinkCommand::AppState { foreground } => self.on_app_state(foreground).await,
            LinkCommand::Repin => self.on_repin().await,
        }
    }

    async fn on_start(
        &mut self,
        user_id: String,
        tokens: Arc<dyn LinkTokenSource>,
        reply: oneshot::Sender<Result<()>>,
    ) {
        let same_session = self.phase != LinkPhase::Terminated
            && self
                .session
                .as_ref()
                .is_some_and(|session| session.user_id == user_id);
        if same_session {
            // 同一用户已在会话中：在线就直接答复，否则等这一轮的结果（停在退避里的立即重试）。
            let online = matches!(
                self.phase,
                LinkPhase::Syncing | LinkPhase::Ready | LinkPhase::Degraded
            );
            if let Some(session) = self.session.as_mut() {
                session.tokens = tokens;
                if online {
                    let _ = reply.send(Ok(()));
                } else if let Some(previous) = session.start_reply.replace(reply) {
                    let _ = previous.send(Err(FlareError::localized(
                        ErrorCode::ConnectionReconnecting,
                        "superseded by a newer connect request",
                    )));
                }
            }
            if !online
                && matches!(
                    self.phase,
                    LinkPhase::Backoff | LinkPhase::Recovering | LinkPhase::Idle
                )
            {
                self.backoff.reset();
                self.failed_rounds = 0;
                self.begin_round(Vec::new(), None);
            }
            return;
        }
        if self.session.is_some() {
            self.stop_session().await;
        }
        self.session = Some(Session {
            user_id,
            tokens,
            start_reply: Some(reply),
            online_once: false,
            auth_renewed: false,
            announced_directory: false,
        });
        self.facts = LinkFacts {
            development_mode: self.access.development_mode(),
            ..LinkFacts::default()
        };
        self.backoff.reset();
        self.failed_rounds = 0;
        self.switch_from = None;
        self.link_lost_at = None;
        if !self.network_available {
            // 没网：会话照常开始，等网络回来再连；登录成功，状态是 waitingForNetwork。
            tracing::info!("the device has no network; the session waits for it");
            self.answer_start(Ok(()));
            self.set_phase(LinkPhase::Suspended);
            return;
        }
        self.begin_round(Vec::new(), None);
    }

    async fn stop_session(&mut self) {
        self.epoch.advance();
        self.op = None;
        self.round = None;
        self.retry_at = None;
        self.degraded_until = None;
        self.next_maintenance = None;
        self.manifest_move_at = None;
        self.live = None;
        self.access.set_current(None);
        self.access.set_recovering(false);
        self.answer_start(Err(FlareError::localized(
            ErrorCode::NotConnected,
            "the session was disconnected",
        )));
        self.session = None;
        self.driver.close().await;
        self.driver.stop().await;
        self.access.flush_health(true).await;
        self.facts = LinkFacts {
            development_mode: self.access.development_mode(),
            ..LinkFacts::default()
        };
        self.set_phase(LinkPhase::Idle);
    }

    async fn on_network(&mut self, available: bool, changed: bool) {
        self.network_available = available;
        if self.session.is_none() || self.phase == LinkPhase::Terminated {
            return;
        }
        if !available {
            // 停止一切尝试；连接若还在就留着（网络可能只是抖了一下），恢复后先验活。
            self.round = None;
            self.retry_at = None;
            if self.live.is_none() {
                self.op = None;
            }
            self.access.suspend_refresh();
            self.driver.link_down().await;
            self.set_phase(LinkPhase::Suspended);
            return;
        }
        // 恢复期间换了网络：之前到不了的引导源可能到得了了。
        self.access.wake_refresh();
        if let Some(live) = self.live.as_ref() {
            let current_unhealthy = changed && self.access.is_unhealthy(&live.access_point_id);
            let was_suspended = self.phase == LinkPhase::Suspended;
            match decide(
                &FailoverEvent::NetworkAvailable {
                    link_alive: true,
                    current_unhealthy,
                },
                unit(),
            ) {
                FailoverAction::ProbeWake => {
                    // 旧路径上的连接常被中间设备静默丢弃：立即验活，窗口内没判死就认为还活着。
                    self.driver.probe();
                    let window = i64::from(self.access.policy().probe_wake_timeout_ms);
                    self.degraded_until = Some(now_ms() + window + PROBE_GRACE_MS);
                    self.degraded_by_rtt = false;
                    self.resume_after_probe |= was_suspended;
                    if !live.syncing {
                        self.set_phase(LinkPhase::Degraded);
                    }
                }
                _ => {
                    self.switch_away(Vec::new(), SwitchReason::Failure).await;
                }
            }
            return;
        }
        if matches!(
            self.phase,
            LinkPhase::Suspended | LinkPhase::Backoff | LinkPhase::Recovering
        ) {
            // 换了网络，之前的失败不再说明问题：重置退避，按新网络的健康记录立即选路。
            self.backoff.reset();
            self.failed_rounds = 0;
            let prefer = std::mem::take(&mut self.pending_prefer);
            self.begin_round(prefer, None);
        }
    }

    async fn on_app_state(&mut self, foreground: bool) {
        let was_foreground = self.foreground;
        self.foreground = foreground;
        if !foreground {
            self.access.flush_health(true).await;
            return;
        }
        if was_foreground || self.session.is_none() {
            return;
        }
        self.access.wake_refresh();
        match self.phase {
            // 回到前台：后台的长退避不该让用户等，立即一轮。
            LinkPhase::Backoff | LinkPhase::Recovering if self.network_available => {
                self.backoff.reset();
                self.begin_round(Vec::new(), None);
            }
            // 回前台是恢复切回的自然边界。
            LinkPhase::Ready => self.check_failback(true).await,
            _ => {}
        }
    }

    async fn on_repin(&mut self) {
        let (Some(live), Some(kernel)) = (self.live.as_ref(), self.access.kernel_now()) else {
            return;
        };
        let Some(pinned) = kernel.selector().pinned(now_ms()) else {
            return;
        };
        if pinned.ap_id != live.access_point_id {
            self.switch_away(vec![pinned.ap_id.to_string()], SwitchReason::Manual)
                .await;
        }
    }

    // ---------------------------------------------------------------- 一轮建连

    fn begin_round(&mut self, prefer: Vec<String>, only: Option<String>) {
        if self.session.is_none() {
            return;
        }
        self.retry_at = None;
        self.op = None;
        self.round = Some(Round {
            token: None,
            tried: Vec::new(),
            prefer,
            only,
            last_error: None,
        });
        self.facts.next_retry_at = None;
        self.set_phase(LinkPhase::Resolving);
        self.continue_round();
    }

    fn continue_round(&mut self) {
        let Some(session) = self.session.as_ref() else {
            return;
        };
        let Some(kernel) = self.access.kernel_now() else {
            let access = self.access.clone();
            self.op = Some(boxed(async move {
                OpResult::Directory(access.kernel().await.map(|_| ()))
            }));
            return;
        };
        if !kernel.directory().snapshot().has_manifest() {
            // 没有任何可信清单，也就没有接入点可连：直接进入恢复，等引导拉取或恢复链接。
            tracing::warn!("no usable access manifest; recovering");
            if let Some(round) = self.round.as_mut() {
                round.last_error = Some(FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "no usable access manifest",
                ));
            }
            self.failed_rounds = self
                .failed_rounds
                .max(super::failover::RECOVERING_AFTER_FAILED_ROUNDS - 1);
            self.round_exhausted(None);
            return;
        }
        if !session.announced_directory
            && let Some(changed) = self.access.changed_payload()
        {
            self.bus
                .publish(SdkEvent::Connection(ConnectionEvent::AccessChanged(
                    changed,
                )));
            if let Some(session) = self.session.as_mut() {
                session.announced_directory = true;
            }
        }
        let Some(session) = self.session.as_ref() else {
            return;
        };
        if self
            .round
            .as_ref()
            .is_some_and(|round| round.token.is_none())
        {
            let tokens = session.tokens.clone();
            self.op = Some(boxed(async move {
                OpResult::Token(tokens.token_for_connect().await)
            }));
            return;
        }
        self.next_attempt();
    }

    fn next_attempt(&mut self) {
        let Some(kernel) = self.access.kernel_now() else {
            return;
        };
        let Some(user_id) = self.session.as_ref().map(|session| session.user_id.clone()) else {
            return;
        };
        let net = self.access.net();
        let held = self.held_drained();
        let picked = {
            let Some(round) = self.round.as_mut() else {
                return;
            };
            let Some(token) = round.token.clone() else {
                return;
            };
            loop {
                let tried: Vec<&str> = round.tried.iter().map(String::as_str).collect();
                let mut avoid = tried.clone();
                avoid.extend(held.iter().map(String::as_str));
                let mut selection = self.access.select(&kernel, ServiceClass::Im, None, &avoid);
                if selection.candidates.is_empty() && !held.is_empty() {
                    // 只剩正在排空的接入点（例如唯一的接入点停机重启）：仍然连它。
                    selection = self.access.select(&kernel, ServiceClass::Im, None, &tried);
                }
                let candidate = round
                    .only
                    .as_deref()
                    .and_then(|only| {
                        selection
                            .candidates
                            .iter()
                            .find(|candidate| &*candidate.ap_id == only)
                    })
                    .or_else(|| {
                        round.prefer.iter().find_map(|preferred| {
                            selection
                                .candidates
                                .iter()
                                .find(|candidate| &*candidate.ap_id == preferred.as_str())
                        })
                    })
                    .or_else(|| selection.first())
                    .cloned();
                round.only = None;
                let Some(candidate) = candidate else {
                    break Err(selection.next_half_open_at_ms);
                };
                // half-open 的接入点只放一个试探：认领不到就跳过它。
                if candidate.half_open
                    && !kernel.health().claim_trial(&candidate.ap_id, net, now_ms())
                {
                    round.tried.push(candidate.ap_id.to_string());
                    continue;
                }
                break Ok((selection, candidate, token));
            }
        };
        let (selection, candidate, token) = match picked {
            Ok(picked) => picked,
            Err(next_half_open) => {
                self.round_exhausted(next_half_open);
                return;
            }
        };

        let entry = selection.access_point(&candidate);
        let plan = candidate.protocols.clone();
        let protocols: Vec<_> = plan
            .as_ref()
            .map(|plan| plan.order.clone())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|protocol| {
                let kind = match protocol {
                    Protocol::Quic => ServiceKind::ImQuic,
                    Protocol::Ws => ServiceKind::ImWs,
                };
                selection
                    .endpoint(&candidate, kind)
                    .map(|url| (to_transport(protocol), self.access.dial_url(url)))
            })
            .collect();
        let stagger_ms = plan.as_ref().map_or(300, |plan| plan.stagger_ms);
        let connect_timeout_ms = plan.as_ref().map_or(8_000, |plan| plan.connect_timeout_ms);
        let policy = selection.snapshot.policy();
        let probe_timeout_ms = policy.probe_wake_timeout_ms;
        let heartbeat = LinkHeartbeat::from_policy(policy);
        let target = LinkTarget {
            access_point_id: candidate.ap_id.clone(),
            protocols,
            stagger: Duration::from_millis(u64::from(stagger_ms)),
            connect_timeout: Duration::from_millis(u64::from(connect_timeout_ms)),
            probe_timeout: Duration::from_millis(u64::from(probe_timeout_ms)),
            heartbeat,
            spki_pins: entry.spki_pins.clone(),
            ip_hints: entry.ip_hints.clone(),
            access_serial: selection.snapshot.serial(),
        };
        self.facts.access_point_id = Some(candidate.ap_id.to_string());
        self.facts.cell_id = Some(entry.cell_id.to_string());
        self.facts.region_id = Some(entry.region_id.to_string());
        self.facts.protocol = plan
            .as_ref()
            .and_then(|plan| plan.first())
            .map(|protocol| protocol.as_str().to_owned());
        self.facts.attempt = self.failed_rounds + 1;
        let epoch = self.epoch.advance();
        let driver = self.driver.clone();
        let request = OpenRequest {
            epoch,
            user_id,
            token,
            target: target.clone(),
            foreground: self.foreground,
            #[cfg(not(target_arch = "wasm32"))]
            doh: self.access.doh(),
        };
        self.op = Some(boxed(async move {
            let result = Box::new(driver.open(request).await);
            OpResult::Opened {
                epoch,
                target,
                result,
            }
        }));
        self.set_phase(LinkPhase::Connecting);
    }

    fn round_exhausted(&mut self, next_half_open_at_ms: Option<i64>) {
        let last_error = self
            .round
            .take()
            .and_then(|round| round.last_error)
            .unwrap_or_else(|| {
                FlareError::localized(
                    ErrorCode::ConnectionFailed,
                    "no access point is reachable right now",
                )
            });
        // 链路保留会话、按退避重试：首轮的失败不报给登录（它会自己恢复），只记日志与状态。
        tracing::info!(error = %last_error, "connect round failed; the link keeps the session and retries");
        debug_assert!(
            crate::shared::error::link_keeps_session(&last_error),
            "a failure the link retries must be in the recoverable set: {last_error}"
        );
        self.answer_start(Ok(()));
        self.failed_rounds += 1;
        self.schedule_retry(None, next_half_open_at_ms);
    }

    /// 排空 / 停机 CLOSE：把发出它的接入点放一边。时长是一个熔断冷却期（`breaker.cooldownBaseSec`），
    /// 服务端给的重连窗口更长就到窗口结束：排空不计熔断（§3），但正在下线的接入点在这段时间里不会再
    /// 接新连接，连回去只会白白失败一次。还有别的候选时才绕开它；连上它（只剩它可选）即解除。
    fn hold_drained(&mut self, access_point: &str, window_max_ms: u64) {
        let cooldown_ms = u64::from(self.access.policy().breaker.cooldown_base_sec) * 1_000;
        let until = now_ms() + window_max_ms.max(cooldown_ms) as i64;
        self.drained.retain(|(held, _)| held != access_point);
        self.drained.push((access_point.to_owned(), until));
    }

    /// 还在放一边的接入点（顺手清掉到期的）。
    fn held_drained(&mut self) -> Vec<String> {
        let now = now_ms();
        self.drained.retain(|(_, until)| *until > now);
        self.drained.iter().map(|(held, _)| held.clone()).collect()
    }

    /// 等下一轮时（退避、过载原地等、排空窗口）状态里报下一次要尝试的接入点：同点等待报当前点，有服务端
    /// 建议的报建议的，否则按健康簿预选一个，并让下一轮先试它，状态与实际一致。
    fn plan_next_location(&mut self) {
        let Some(kernel) = self.access.kernel_now() else {
            return;
        };
        let held = self.held_drained();
        let avoid: Vec<&str> = held.iter().map(String::as_str).collect();
        let mut selection = self.access.select(&kernel, ServiceClass::Im, None, &avoid);
        if selection.candidates.is_empty() && !held.is_empty() {
            selection = self.access.select(&kernel, ServiceClass::Im, None, &[]);
        }
        let wanted = self
            .pending_only
            .clone()
            .or_else(|| self.pending_prefer.first().cloned());
        let candidate = match wanted.as_deref() {
            Some(wanted) => selection
                .candidates
                .iter()
                .find(|candidate| &*candidate.ap_id == wanted)
                .cloned(),
            None => selection.first().cloned(),
        };
        let (ap_id, protocol) = match (&candidate, wanted) {
            (Some(candidate), _) => (
                candidate.ap_id.to_string(),
                candidate
                    .protocols
                    .as_ref()
                    .and_then(|plan| plan.first())
                    .map(|protocol| protocol.as_str().to_owned()),
            ),
            (None, Some(wanted)) => (wanted, None),
            (None, None) => {
                self.facts.access_point_id = None;
                self.facts.cell_id = None;
                self.facts.region_id = None;
                return;
            }
        };
        let snapshot = &selection.snapshot;
        let Some((_, entry)) = snapshot.access_point(&ap_id) else {
            return;
        };
        self.facts.cell_id = Some(entry.cell_id.to_string());
        self.facts.region_id = Some(entry.region_id.to_string());
        self.facts.protocol = protocol.or_else(|| self.facts.protocol.clone());
        if self.pending_only.is_none() && self.pending_prefer.is_empty() {
            self.pending_prefer = vec![ap_id.clone()];
        }
        self.facts.access_point_id = Some(ap_id);
    }

    /// 按退避安排下一轮（去相关抖动，与服务端窗口取大，不早于最早的 half-open）。
    fn schedule_retry(
        &mut self,
        server_window_ms: Option<(u64, u64)>,
        next_half_open: Option<i64>,
    ) {
        let policy = self.access.policy();
        let plan = plan_retry(
            &mut self.backoff,
            &policy.backoff,
            RetryContext {
                foreground: self.foreground,
                failed_rounds: self.failed_rounds,
                server_window_ms,
                next_half_open_at_ms: next_half_open,
                now_ms: now_ms(),
            },
            unit(),
            unit(),
        );
        self.retry_at = Some(plan.at_ms);
        self.facts.attempt = self.failed_rounds;
        self.facts.next_retry_at = Some(plan.at_ms);
        self.plan_next_location();
        if plan.phase == LinkPhase::Recovering {
            // 接入点这边照常按退避试探；引导面同时开始找新地址（运维可能已发布）。
            self.access.set_recovering(true);
        }
        self.set_phase(plan.phase);
    }

    // ---------------------------------------------------------------- 异步操作完成

    async fn on_op(&mut self, result: OpResult) {
        match result {
            OpResult::Directory(Ok(())) => self.continue_round(),
            OpResult::Directory(Err(error)) => {
                tracing::error!(%error, "access is misconfigured; the SDK cannot connect");
                self.answer_start(Err(error.to_error()));
                self.round = None;
                self.session = None;
                self.set_phase(LinkPhase::Idle);
            }
            OpResult::Token(ConnectToken::Ready(token)) => {
                if let Some(round) = self.round.as_mut() {
                    round.token = Some(token);
                }
                self.next_attempt();
            }
            OpResult::Token(ConnectToken::Unreachable(error)) => {
                // 签发或换新令牌没走通（多半是网络）：与连不上任何接入点一样，退避后下一轮再取。
                tracing::warn!(%error, "no connect token yet; retrying with backoff");
                if let Some(round) = self.round.as_mut() {
                    round.last_error = Some(error);
                }
                self.round_exhausted(None);
            }
            OpResult::Token(ConnectToken::Unavailable) => {
                self.terminate(ActionRequiredReason::Expired).await
            }
            OpResult::Renewed(Some(token)) => {
                tracing::info!("connect token renewed after the gateway rejected it; reconnecting");
                if let Some(round) = self.round.as_mut() {
                    round.token = Some(token);
                    // 鉴权失败与入口无关：换到新令牌后所有候选重新可用。
                    round.tried.clear();
                }
                self.next_attempt();
            }
            OpResult::Renewed(None) => self.terminate(ActionRequiredReason::Expired).await,
            OpResult::Opened {
                epoch,
                target,
                result,
            } => {
                if !self.epoch.is_current(epoch) {
                    return;
                }
                match *result {
                    Ok(report) => self.on_connected(epoch, target, report).await,
                    Err(failure) => self.on_attempt_failed(target, failure).await,
                }
            }
            OpResult::Synced { epoch } => {
                if self.live.as_ref().is_some_and(|live| live.epoch == epoch) {
                    self.on_ready();
                }
            }
        }
    }

    async fn on_connected(&mut self, epoch: u64, target: LinkTarget, report: ConnectReport) {
        let ap = target.access_point_id.clone();
        // 连上了（只剩它可选时）：它已重新接新连接，不再放一边。
        self.drained.retain(|(held, _)| held.as_str() != &*ap);
        for record in report_records(&report, target.stagger) {
            match record {
                ReportRecord::Observe(observation) => self.access.record(&ap, observation),
                ReportRecord::QuicSilence => self.access.note_quic_silence(),
            }
        }
        if let Some(server_time_ms) = report.ack.server_time_ms {
            self.driver.observe_server_time(server_time_ms);
        }
        // 任何一路连上就退出恢复；服务端知道比本地更新的清单就去取。
        self.access.set_recovering(false);
        if let Some(serial) = report.ack.access_serial {
            self.access.observe_ack_serial(serial);
        }
        let protocol = from_transport(report.protocol).unwrap_or(Protocol::Ws);
        tracing::info!(
            access_point = %ap,
            protocol = protocol.as_str(),
            handshake_ms = duration_ms(report.handshake),
            gateway = ?report.ack.gateway_id,
            "link established"
        );
        self.live = Some(LiveLink {
            epoch,
            access_point_id: ap.clone(),
            protocol,
            syncing: true,
        });
        // CONNECT_ACK 的诊断字段：服务端报的接入点和这里选的不一致（DNS 被劫持、解析到别的入口、网关配错）
        // 时打告警、记进健康簿（同探活头不符，按「探活不合法」计），不断连。
        let reported_ap = report.ack.ap_id.clone().filter(|id| !id.trim().is_empty());
        let mismatch = reported_ap
            .as_deref()
            .is_some_and(|reported| reported != &*ap);
        if mismatch {
            tracing::warn!(
                expected = %ap,
                reported = reported_ap.as_deref().unwrap_or_default(),
                gateway = ?report.ack.gateway_id,
                "the gateway reports a different access point than the one dialled"
            );
            self.access.record(
                &ap,
                Observation::new(
                    AttemptKind::Handshake(protocol),
                    Outcome::Failure(flare_access::FailureClass::ProbeInvalid),
                ),
            );
        }
        self.access.set_current(Some(CurrentLink {
            access_point_id: ap.clone(),
            protocol,
            ack: ConnectAckSummary {
                gateway_id: report.ack.gateway_id.clone(),
                access_point_id: reported_ap,
                access_serial: report.ack.access_serial,
                heartbeat_interval_hint_sec: report.ack.heartbeat_interval_hint_sec,
                access_point_mismatch: mismatch,
            },
        }));
        self.round = None;
        self.retry_at = None;
        self.facts.protocol = Some(protocol.as_str().to_owned());
        self.facts.rtt_ms = Some(duration_ms(report.handshake));
        self.last_rtt = None;
        self.facts.next_retry_at = None;
        self.set_phase(LinkPhase::Syncing);
        self.answer_start(Ok(()));
        // 新连接已收到 CONNECT_ACK：换到了另一个接入点就算切换完成（补同步在新连接上继续）。
        if let Some((from, reason)) = self.switch_from.take()
            && from.ap != ap.as_ref()
        {
            self.bus
                .publish(SdkEvent::Connection(ConnectionEvent::AccessSwitched(
                    AccessSwitched {
                        from,
                        to: AccessEndpoint {
                            ap: ap.to_string(),
                            protocol: protocol.as_str().to_owned(),
                        },
                        reason,
                    },
                )));
        }

        let up = match self.session.as_ref() {
            Some(session) if session.online_once => LinkUp::Reconnected {
                lost_at_ms: self.link_lost_at.take(),
            },
            _ => LinkUp::First,
        };
        if self.network_available {
            self.driver.resume_outbox().await;
        } else {
            // 设备报告没有网络却连上了（挂起时点了重试）：等网络回来、验活通过后再恢复发件箱。
            self.resume_after_probe = true;
        }
        let driver = self.driver.clone();
        self.op = Some(boxed(async move {
            let ready = driver.link_up(up).await;
            ready.await;
            OpResult::Synced { epoch }
        }));
    }

    fn on_ready(&mut self) {
        let Some(live) = self.live.as_mut() else {
            return;
        };
        live.syncing = false;
        if let Some(session) = self.session.as_mut() {
            session.online_once = true;
            session.auth_renewed = false;
        }
        self.backoff.reset();
        self.failed_rounds = 0;
        self.facts.was_online = true;
        self.facts.attempt = 0;
        self.facts.switch_reason = None;
        self.next_maintenance = Some(now_ms() + MAINTENANCE_INTERVAL_MS);
        self.next_failback_check = now_ms() + FAILBACK_CHECK_INTERVAL_MS;
        self.set_phase(if !self.network_available {
            // 补同步在「网络不可用」之后才收尾：仍在等网络，发件箱保持暂停，恢复时先验活。
            LinkPhase::Suspended
        } else if self.degraded_until.is_some() {
            LinkPhase::Degraded
        } else {
            LinkPhase::Ready
        });
    }

    async fn on_attempt_failed(&mut self, target: LinkTarget, failure: ConnectFailure) {
        let ap = target.access_point_id.to_string();
        for attempt in &failure.attempts {
            if let Some(protocol) = from_transport(attempt.protocol)
                && let Some(observation) = attempt_observation(protocol, &attempt.failure)
            {
                self.access.record(&ap, observation);
            }
        }
        let error = connect_error(&failure);
        tracing::warn!(access_point = %ap, %error, "connect attempt failed");
        if let Some(round) = self.round.as_mut() {
            round.last_error = Some(error);
        }
        if let Some(rejection) = failure.rejection() {
            match classify_rejection(rejection) {
                RejectionKind::Overloaded { retry_after_ms } => {
                    // 同一接入点等服务端给的窗口，不换入口、不计熔断（防止过载时级联迁移）。
                    if let FailoverAction::RetrySameAfter {
                        access_point,
                        delay_ms,
                    } = decide(
                        &FailoverEvent::Overloaded {
                            access_point: ap.clone(),
                            retry_after_ms,
                        },
                        unit(),
                    ) {
                        // 过载：原地等服务端给的窗口，会话保留，登录照常成功。
                        if let Some(error) = self.round.take().and_then(|round| round.last_error) {
                            tracing::info!(%error, "overloaded; waiting on the same access point");
                        }
                        self.answer_start(Ok(()));
                        self.pending_only = Some(access_point);
                        let at = now_ms() + delay_ms as i64;
                        self.retry_at = Some(at);
                        self.facts.next_retry_at = Some(at);
                        self.facts.attempt = self.failed_rounds + 1;
                        // 状态里报正在等的这个接入点。
                        self.plan_next_location();
                        self.set_phase(LinkPhase::Backoff);
                    }
                    return;
                }
                RejectionKind::Auth => {
                    let rejected = self.round.as_ref().and_then(|round| round.token.clone());
                    let renewal = match (self.session.as_mut(), rejected) {
                        (Some(session), Some(rejected)) if !session.auth_renewed => {
                            session.auth_renewed = true;
                            Some((session.tokens.clone(), rejected))
                        }
                        _ => None,
                    };
                    match renewal {
                        Some((tokens, rejected)) => {
                            self.set_phase(LinkPhase::Authenticating);
                            self.op = Some(boxed(async move {
                                OpResult::Renewed(tokens.renew_after_rejection(&rejected).await)
                            }));
                        }
                        None => self.terminate(ActionRequiredReason::Expired).await,
                    }
                    return;
                }
                RejectionKind::Terminal(reason) => {
                    if reason == ActionRequiredReason::UpgradeRequired {
                        self.access.note_upgrade_required(rejection.min_build);
                    }
                    self.terminate(reason).await;
                    return;
                }
                RejectionKind::NextCandidate { .. } => {}
            }
        }
        // 当前接入点失败：立即在候选里换下一个，不等退避。
        if let Some(round) = self.round.as_mut() {
            round.tried.push(ap);
        }
        self.next_attempt();
    }

    async fn terminate(&mut self, reason: ActionRequiredReason) {
        tracing::warn!(
            reason = reason.as_str(),
            "link terminated; only a user action can recover"
        );
        self.epoch.advance();
        self.op = None;
        self.round = None;
        self.retry_at = None;
        self.degraded_until = None;
        self.next_maintenance = None;
        self.manifest_move_at = None;
        self.live = None;
        self.access.set_current(None);
        self.access.set_recovering(false);
        self.driver.close().await;
        self.driver.link_down().await;
        self.answer_start(Err(action_error(reason)));
        self.facts.action_reason = Some(reason);
        self.set_phase(LinkPhase::Terminated);
    }

    // ---------------------------------------------------------------- 连接信号

    async fn on_signal(&mut self, signal: LinkSignal) {
        match signal {
            LinkSignal::Lost { epoch, reason } => self.on_lost(epoch, reason).await,
            LinkSignal::RoundTrip { epoch, rtt } => self.on_round_trip(epoch, rtt),
        }
    }

    async fn on_lost(&mut self, epoch: u64, reason: DisconnectReason) {
        // 旧连接的迟到断开（切换后被网关替换、被我们关掉）一律丢弃。
        if !self.epoch.is_current(epoch)
            || self.live.as_ref().is_none_or(|live| live.epoch != epoch)
        {
            return;
        }
        let Some(live) = self.live.take() else {
            return;
        };
        tracing::info!(access_point = %live.access_point_id, %reason, "link lost");
        self.access.set_current(None);
        self.op = None;
        self.degraded_until = None;
        self.next_maintenance = None;
        self.manifest_move_at = None;
        self.resume_after_probe = false;
        let loss = classify_loss(&reason);
        match &loss {
            LinkLoss::Failed(class) => self.access.record(
                &live.access_point_id,
                Observation::new(AttemptKind::Heartbeat, Outcome::Failure(*class)),
            ),
            LinkLoss::Planned { .. } => self.access.record(
                &live.access_point_id,
                Observation::new(
                    AttemptKind::Heartbeat,
                    Outcome::Neutral(NeutralReason::Draining),
                ),
            ),
            _ => {}
        }
        self.link_lost_at = Some(now_ms());
        let from = AccessEndpoint {
            ap: live.access_point_id.to_string(),
            protocol: live.protocol.as_str().to_owned(),
        };
        self.driver.link_down().await;
        if !self.network_available {
            self.set_phase(LinkPhase::Suspended);
            return;
        }
        let drain_window_max_ms = match &loss {
            LinkLoss::Planned { window_ms, .. } => Some(window_ms.map_or(0, |(_, max)| max)),
            _ => None,
        };
        match decide(&FailoverEvent::LinkLost(loss), unit()) {
            FailoverAction::Stay => {}
            FailoverAction::ConnectNow { prefer } => {
                self.switch_from = Some((from, SwitchReason::Failure));
                self.facts.switch_reason = Some(SwitchReason::Failure);
                self.begin_round(prefer, None);
                // 刚判死的接入点本轮先排除：立即换下一个；只有它一个时本轮耗尽，按退避再试它。
                self.exclude_failed(&live.access_point_id);
            }
            FailoverAction::ConnectAfter { delay_ms, prefer } => {
                // 计划性关闭：窗口内随机时刻重连，优先服务端建议的接入点；发出 CLOSE 的接入点正在
                // 排空、不再接新连接，这一轮及之后一段时间都先绕开它。
                self.hold_drained(&live.access_point_id, drain_window_max_ms.unwrap_or(0));
                self.switch_from = Some((from, SwitchReason::Drain));
                self.facts.switch_reason = Some(SwitchReason::Drain);
                self.pending_prefer = prefer;
                self.retry_at = Some(now_ms() + delay_ms as i64);
                self.plan_next_location();
                self.set_phase(LinkPhase::Resolving);
            }
            FailoverAction::Backoff => {
                self.switch_from = Some((from, SwitchReason::Failure));
                self.schedule_retry(None, None);
            }
            FailoverAction::Terminate(reason) => self.terminate(reason).await,
            FailoverAction::RetrySameAfter { .. }
            | FailoverAction::ProbeWake
            | FailoverAction::Suspend => {}
        }
    }

    /// 刚判死的接入点在本轮里排除（已在取令牌或装配目录时，下一次 `next_attempt` 生效）。
    fn exclude_failed(&mut self, access_point: &str) {
        if let Some(round) = self.round.as_mut()
            && !round.tried.iter().any(|tried| tried == access_point)
        {
            round.tried.push(access_point.to_owned());
        }
    }

    fn on_round_trip(&mut self, epoch: u64, rtt: Duration) {
        let Some(live) = self.live.as_ref() else {
            return;
        };
        if live.epoch != epoch || self.last_rtt == Some(rtt) {
            return;
        }
        self.last_rtt = Some(rtt);
        let rtt_ms = duration_ms(rtt);
        self.access
            .record(&live.access_point_id, Observation::heartbeat_rtt(rtt_ms));
        self.facts.rtt_ms = Some(rtt_ms);
        match self.phase {
            LinkPhase::Ready if rtt_ms >= POOR_RTT_MS => {
                self.degraded_by_rtt = true;
                self.set_phase(LinkPhase::Degraded);
            }
            LinkPhase::Degraded
                if self.degraded_by_rtt
                    && rtt_ms < POOR_RTT_MS
                    && self.degraded_until.is_none() =>
            {
                self.degraded_by_rtt = false;
                self.set_phase(LinkPhase::Ready);
            }
            _ => self.publish(),
        }
    }

    // ---------------------------------------------------------------- 定时

    async fn on_timer(&mut self) {
        let now = now_ms();
        if self.degraded_until.is_some_and(|at| at <= now) {
            // 验活窗口过去仍没判死：连接健康。挂起期间暂停的发件箱此时恢复。
            self.degraded_until = None;
            // 验活窗口里又断网了：留着标记，等网络回来的下一次验活。
            if self.resume_after_probe && self.network_available {
                self.resume_after_probe = false;
                // 恢复只放开发件箱、补一次不抢占的同步，不等同步收尾。
                drop(self.driver.link_up(LinkUp::Resumed).await);
            }
            if self.phase == LinkPhase::Degraded && !self.degraded_by_rtt {
                self.set_phase(LinkPhase::Ready);
            }
        }
        if self.retry_at.is_some_and(|at| at <= now) {
            self.retry_at = None;
            if self.network_available && self.session.is_some() {
                let prefer = std::mem::take(&mut self.pending_prefer);
                let only = self.pending_only.take();
                self.begin_round(prefer, only);
            }
        }
        if self.next_maintenance.is_some_and(|at| at <= now) {
            self.next_maintenance = None;
            self.maintenance(now).await;
        }
        if self.manifest_move_at.is_some_and(|at| at <= now) {
            self.manifest_move_at = None;
            self.move_off_manifest_change().await;
        }
    }

    // ---------------------------------------------------------------- 目录变更

    /// 采纳了新清单（§3.6）：当前接入点仍 active 不动；被摘除立即切换；变成 draining / disabled 按排空
    /// 规则在窗口内迁走；新增的只更新候选。没有连接时立即按新拓扑选路，不等退避到期。
    async fn on_directory_changed(&mut self, change: DirectoryChange) {
        let Some(session) = self.session.as_mut() else {
            return;
        };
        session.announced_directory = true;
        if let Some(payload) = self.access.change_payload(&change) {
            self.bus
                .publish(SdkEvent::Connection(ConnectionEvent::AccessChanged(
                    payload,
                )));
        }
        if self.phase == LinkPhase::Terminated {
            return;
        }
        if let Some(live) = self.live.as_ref() {
            let current = &live.access_point_id;
            if change.removed.iter().any(|ap| ap == current) {
                self.move_off_manifest_change().await;
            } else if change
                .changed
                .iter()
                .any(|(ap, status)| ap == current && *status != ApStatus::Active)
            {
                let (min, max) = MANIFEST_DRAIN_WINDOW_MS;
                let delay = between(min, max, unit());
                self.manifest_move_at = Some(now_ms() + delay as i64);
            }
            return;
        }
        if self.network_available
            && matches!(self.phase, LinkPhase::Backoff | LinkPhase::Recovering)
        {
            // 之前的失败说的是旧拓扑。等服务端窗口的（过载、排空）也不再等：要去的接入点可能已经没了。
            self.pending_prefer.clear();
            self.pending_only = None;
            self.backoff.reset();
            self.begin_round(Vec::new(), None);
        }
    }

    /// 新清单不再让用当前接入点：还有别的候选才切，否则留着这条还能用的连接（下一份清单再说）。
    async fn move_off_manifest_change(&mut self) {
        let (Some(live), Some(kernel)) = (self.live.as_ref(), self.access.kernel_now()) else {
            return;
        };
        let current = live.access_point_id.clone();
        let snapshot = kernel.directory().snapshot();
        let still_usable = snapshot
            .access_point(&current)
            .is_some_and(|(_, entry)| entry.is_active());
        if still_usable {
            return;
        }
        let selection = self
            .access
            .select(&kernel, ServiceClass::Im, None, &[&*current]);
        if selection.candidates.is_empty() {
            tracing::warn!(access_point = %current, "the manifest retired the current access point but offers no other; staying");
            return;
        }
        tracing::info!(access_point = %current, "the manifest retired the current access point; switching");
        self.switch_away(Vec::new(), SwitchReason::Manifest).await;
    }

    async fn maintenance(&mut self, now: i64) {
        let Some(live) = self.live.as_ref() else {
            return;
        };
        let epoch = live.epoch;
        self.next_maintenance = Some(now + MAINTENANCE_INTERVAL_MS);
        // 采样心跳 RTT：取样要拿客户端锁（发送等回执时会被占着），放到后台，结果经信号回来。
        let driver = self.driver.clone();
        let tx = self.signal_tx.clone();
        spawn_background(async move {
            if let Some(rtt) = driver.heartbeat_round_trip().await {
                let _ = tx.try_send(LinkSignal::RoundTrip { epoch, rtt });
            }
        });
        let access = self.access.clone();
        spawn_background(async move { access.flush_health(false).await });
        if now >= self.next_failback_check {
            self.next_failback_check = now + FAILBACK_CHECK_INTERVAL_MS;
            self.check_failback(false).await;
        }
    }

    /// 恢复切回（§3.7）：四个条件全部满足才切；探活样本不够时探一次首选接入点积累样本。
    async fn check_failback(&mut self, foregrounded: bool) {
        let (Some(live), Some(kernel)) = (self.live.as_ref(), self.access.kernel_now()) else {
            return;
        };
        if self.phase != LinkPhase::Ready {
            return;
        }
        let verdict = self.access.evaluate_failback(
            &kernel,
            &live.access_point_id,
            foregrounded,
            self.driver.last_activity_ms(),
            self.last_switch_ms,
        );
        match (verdict.decision, verdict.preferred) {
            (FailbackDecision::Switch, Some(preferred)) => {
                tracing::info!(from = %live.access_point_id, to = %preferred, "failing back to the preferred access point");
                self.switch_away(vec![preferred.to_string()], SwitchReason::Failback)
                    .await;
            }
            (
                FailbackDecision::Stay(
                    FailbackHold::NotEnoughProbes | FailbackHold::ProbeSpanTooShort,
                ),
                Some(preferred),
            ) => {
                let access = self.access.clone();
                spawn_background(async move {
                    let _ = access.probe(&preferred, ProbeTrigger::Failback).await;
                });
            }
            _ => {}
        }
    }

    /// 计划性切换（恢复切回、手动固定、新网络下当前入口不佳）：作废当前连接，切到别的接入点。
    /// 不重登：不调用 prepare、会话世代不变，发件箱暂停后在新连接上原样恢复。
    async fn switch_away(&mut self, prefer: Vec<String>, reason: SwitchReason) {
        if let Some(live) = self.live.take() {
            self.switch_from = Some((
                AccessEndpoint {
                    ap: live.access_point_id.to_string(),
                    protocol: live.protocol.as_str().to_owned(),
                },
                reason,
            ));
            // 先作废代数：旧连接接下来的帧与断开都不再算数（建新连接前会关掉它）。
            self.epoch.advance();
            self.access.set_current(None);
            self.link_lost_at = Some(now_ms());
            self.driver.link_down().await;
        }
        self.last_switch_ms = now_ms();
        self.degraded_until = None;
        self.next_maintenance = None;
        self.manifest_move_at = None;
        self.resume_after_probe = false;
        self.facts.switch_reason = Some(reason);
        self.begin_round(prefer, None);
    }
}

/// 建连失败转成给宿主的错误：服务端明确拒绝的类别原样保留，其余是连不上。
fn connect_error(failure: &ConnectFailure) -> FlareError {
    let error = FlareError::from(failure.clone());
    let code = match error.code() {
        Some(
            code @ (ErrorCode::AuthenticationFailed
            | ErrorCode::AuthenticationExpired
            | ErrorCode::AuthenticationInvalid
            | ErrorCode::TokenInvalid
            | ErrorCode::TokenExpired
            | ErrorCode::PermissionDenied
            | ErrorCode::ServiceUnavailable
            | ErrorCode::Overloaded
            | ErrorCode::UpgradeRequired
            | ErrorCode::ProtocolVersionMismatch
            | ErrorCode::ConnectionTimeout),
        ) => code,
        _ => ErrorCode::ConnectionFailed,
    };
    let error = FlareError::localized(code, error.to_string());
    // 过载拒绝带着网关给的等待时间：宿主（OverloadedException 等）据此安排重试提示。
    match failure
        .rejection()
        .filter(|rejection| rejection.is_overloaded())
        .and_then(|rejection| rejection.retry_after_ms)
    {
        Some(retry_after_ms) => crate::shared::error::retrying_after(error, retry_after_ms),
        None => error,
    }
}

#[cfg(test)]
mod tests;
