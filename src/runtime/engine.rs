use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::time::Duration;

use flare_core::client::link::{ConnectFailure, ConnectReport};
use flare_core::common::HeartbeatAppState;
use tokio::sync::watch;

use crate::application::notification::{
    AlertLedger, NotificationHost, NotificationInboundPipeline, NotifyCenter, NotifyCenterParts,
};
use crate::application::services::EventDeduper;
use crate::application::services::MessageDeduper;
use crate::application::usecases::sync_request::SyncRequestUseCase;
use crate::extension::middleware::MiddlewareChain;
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::protocol::{Codec, DeliveryAcks, PacketSender};
use crate::infrastructure::transport::socket::socket_client::connect_device_platform;
use crate::infrastructure::transport::{SocketHandler, SocketTransport};
use crate::kernel::event::{
    EventBus, EventFilter, EventReceiver, ReadinessStage, SdkEvent, SdkEventType, SyncEventType,
    SyncNotify,
};
use crate::kernel::{
    ConversationSummarySync, CurrentUserIdStore, SdkState, ServerClock, SessionSyncRunner,
    StartupClass, SyncManager, SyncResponseHandler, SyncRunContext, TenantNoticePort,
};
use crate::runtime::link::{
    AccessRuntime, ConnectionStatus, DeviceLinkState, DeviceReach, LinkDriver, LinkFuture,
    LinkHandle, LinkSignals, LinkTokenSource, LinkUp, OpenRequest, ReachProfile, boxed,
    device_state_report,
};
use crate::runtime::{ReliableSendQueue, ReliableSendQueueConfig};
use crate::shared::util::{
    BackgroundTask, delay, now_millis, spawn_background, spawn_background_task, timeout,
};
use crate::spi::metrics::MetricsRecorder;

/// 补同步就绪的等待上限：到点还没就绪也按上线处理（同步在后台继续），不让「正在同步」一直挂着。
const SYNC_READY_CAP: Duration = Duration::from_secs(10);
/// 换了新连接后补发断链前多久之内发出的 ACK：覆盖心跳判死（前台 90 s）加验活窗口的时间。
const ACK_REPLAY_WINDOW_MS: u64 = 120_000;
/// 托管前冲刷的时限：攒着的送达确认发出去，在途的发送等到回执回来，到点就不再等。
/// 系统留给进后台的应用的时间只有几秒，不能都花在这里。
const PARK_FLUSH_CAP: Duration = Duration::from_secs(2);
/// 冲刷时每隔多久看一次在途的发送有没有收尾。
const PARK_OUTBOX_POLL: Duration = Duration::from_millis(100);
/// 等服务端应答 `PARKING` 的时限。
const PARK_ACK_CAP: Duration = Duration::from_secs(1);
/// 应答之后送出最后一批送达确认的时限。
const PARK_LAST_ACKS_CAP: Duration = Duration::from_millis(500);
/// 读角标（未读总数）的时限：读不出来就报 0，不为它拖住状态上报。
const BADGE_READ_CAP: Duration = Duration::from_millis(300);

pub struct SdkEngine {
    stores: StoreProvider,
    bus: EventBus,
    sender: Arc<PacketSender>,
    transport: SocketTransport,
    current_user_id: CurrentUserIdStore,
    sync_manager: Arc<SyncManager>,
    session_sync: Option<Arc<dyn SessionSyncRunner>>,
    conversation_summary_sync: Option<Arc<dyn ConversationSummarySync>>,
    chain: Arc<MiddlewareChain>,
    reliable_queue: Option<Arc<ReliableSendQueue>>,
    /// Receives the tenant's config notices of every connection.
    tenant_notices: Arc<OnceLock<Arc<dyn TenantNoticePort>>>,
    /// The server clock estimate, fed by the downlink frames of every connection.
    server_clock: ServerClock,
    /// 设备可达状态：前后台、后台展示方、托管进度，决定到达的消息由不由本进程确认。
    reach: Arc<DeviceReach>,
    /// 提示台账：链路确认了的、推送展示过的，按合并键记到哪个 seq。属于这一个登录会话。
    alerts: Arc<AlertLedger>,
    /// 链路在线时的提示决策点：应用内横幅、后台系统通知、修正与角标。
    notify: Arc<NotifyCenter>,
    link_driver: Arc<EngineLinkDriver>,
    /// 唯一的连接生命周期：建连、重连、切换、终止都由它决定。
    link: LinkHandle,
}

pub(crate) struct SdkEngineConfig {
    pub stores: StoreProvider,
    pub chain: Arc<MiddlewareChain>,
    pub transport: SocketTransport,
    pub current_user_id: CurrentUserIdStore,
    pub codec: Arc<dyn Codec>,
    pub bus: EventBus,
    pub sync_response_handler: Option<Arc<dyn SyncResponseHandler>>,
    pub session_sync: Option<Arc<dyn SessionSyncRunner>>,
    pub conversation_summary_sync: Option<Arc<dyn ConversationSummarySync>>,
    pub event_deduper: EventDeduper,
    pub message_deduper: MessageDeduper,
    pub notification_pipeline: NotificationInboundPipeline,
    pub ack_timeout_secs: Option<u64>,
    pub ack_max_retries: Option<u32>,
    pub ack_max_in_flight: Option<usize>,
    pub metrics: MetricsRecorder,
    /// 接入运行时（跨会话共享：健康簿、熔断属于设备与网络，不属于某次登录）。
    pub access: Arc<AccessRuntime>,
    /// 宿主此前报过的网络可用性（链路状态机的初始值；没报过为 `true`）。
    pub network_available: bool,
    /// 本设备的 ID：读写用户级通知设置的同步请求带它。
    pub device_id: String,
    /// 应用的界面语言（BCP 47）：引擎自己出的通知文案按它选中文或英文。
    pub locale: Option<String>,
}

impl SdkEngine {
    /// 创建引擎并启动链路状态机（空闲，等会话开始）。
    pub(crate) fn new(config: SdkEngineConfig) -> Self {
        let SdkEngineConfig {
            stores,
            chain,
            transport,
            current_user_id,
            codec,
            bus,
            sync_response_handler,
            session_sync,
            conversation_summary_sync,
            event_deduper,
            message_deduper,
            notification_pipeline,
            ack_timeout_secs,
            ack_max_retries,
            ack_max_in_flight,
            metrics,
            access,
            network_available,
            device_id,
            locale,
        } = config;
        let _ = message_deduper;
        let sender = transport.sender().clone();
        let reliable_queue = stores.pending_sends().map(|(reader, writer)| {
            // 发出后迟迟没回执时验活当前连接；句柄与传输共享客户端槽位，换了连接探的是新连接。
            Arc::new(ReliableSendQueue::with_link_probe(
                ReliableSendQueueConfig {
                    pending_reader: reader,
                    pending_writer: writer,
                    sender: sender.clone(),
                    message_store: stores.messages.clone(),
                    conversation_store: stores.conversations.clone(),
                    current_user_id: current_user_id.clone(),
                    bus: bus.clone(),
                    timeout_secs: ack_timeout_secs,
                    max_retries: ack_max_retries,
                    max_in_flight: ack_max_in_flight,
                    metrics: metrics.clone(),
                    // 链路上线之前不派发：没有连接时发出去只会白白耗掉重试次数。
                    start_paused: true,
                },
                Arc::new(transport.link_probe()),
            ))
        });
        let sync_manager = Arc::new(SyncManager::new());
        let tenant_notices = Arc::new(OnceLock::new());
        let server_clock = ServerClock::new();
        // 进后台后链路怎么办、后台能不能自己提示，都按 CONNECT 里报的那个平台定。
        let profile = ReachProfile::for_platform(connect_device_platform().as_str());
        let reach = Arc::new(DeviceReach::new(profile));
        let alerts = Arc::new(AlertLedger::new());
        let notify = NotifyCenter::start(NotifyCenterParts {
            bus: bus.clone(),
            stores: stores.clone(),
            reach: reach.clone(),
            alerts: alerts.clone(),
            sync: SyncRequestUseCase::new(sender.clone(), device_id),
            locale,
        });
        // 实时到达的消息经各条连接的分发器交给提示决策点；同步拉回的走另一条管道，不提示。
        let notification_pipeline = notification_pipeline.with_notify_center(notify.clone());
        let delivery_acks = Arc::new(DeliveryAcks::new(sender.clone()));
        let (state_reports, pending_reports) = watch::channel(None);
        spawn_background(run_state_reports(
            pending_reports,
            sender.clone(),
            stores.clone(),
            reach.clone(),
        ));
        let link_driver = Arc::new(EngineLinkDriver {
            transport: transport.clone(),
            stores: stores.clone(),
            bus: bus.clone(),
            sender: sender.clone(),
            codec,
            current_user_id: current_user_id.clone(),
            sync_manager: sync_manager.clone(),
            sync_response_handler,
            session_sync: session_sync.clone(),
            conversation_summary_sync: conversation_summary_sync.clone(),
            reliable_queue: reliable_queue.clone(),
            event_deduper,
            notification_pipeline,
            metrics,
            tenant_notices: tenant_notices.clone(),
            server_clock: server_clock.clone(),
            reach: reach.clone(),
            alerts: alerts.clone(),
            notify: notify.clone(),
            delivery_acks,
            state_reports,
            anti_entropy_probe: StdMutex::new(None),
            signals: OnceLock::new(),
        });
        let link = LinkHandle::spawn(
            link_driver.clone() as Arc<dyn LinkDriver>,
            access,
            bus.clone(),
            network_available,
            reach.clone(),
        );
        let _ = link_driver.signals.set(link.signals());
        Self {
            stores,
            bus,
            sender,
            transport,
            current_user_id,
            sync_manager,
            session_sync,
            conversation_summary_sync,
            chain,
            reliable_queue,
            tenant_notices,
            server_clock,
            reach,
            alerts,
            notify,
            link_driver,
            link,
        }
    }

    /// 这一个登录会话的提示台账（推送入口按它去重）。
    pub(crate) fn alert_ledger(&self) -> &Arc<AlertLedger> {
        &self.alerts
    }

    /// 链路在线时的提示决策点。
    pub(crate) fn notify_center(&self) -> &Arc<NotifyCenter> {
        &self.notify
    }

    /// 会话开始前带上客户端那份宿主事实：后台展示方接没接上、正在看哪个会话（宿主常在登录前就报过）。
    pub(crate) fn adopt_notification_host(&self, host: Arc<NotificationHost>) {
        self.reach
            .set_background_presenter(host.background_presenter());
        self.notify.adopt_host(host);
    }

    /// 后台展示方（平台推送包的本地通知）接上或卸下。接上之后，手机在后台报 `STANDBY`：从链路收到
    /// 的消息由引擎确认并交给它提示。没接上时报 `DEFERRED`：引擎不确认，提示由服务端直接走推送。
    /// 应用正在后台时，状态随即重新上报。
    pub(crate) async fn set_background_presenter(&self, attached: bool) {
        self.reach.set_background_presenter(attached);
        self.link.device_state_changed().await;
    }

    /// 会话开始前带上宿主此前报过的前后台（平台常在登录前就报过，之后对相同状态去重）。链路那边
    /// 由调用方在锁外另行告知，见 [`LinkHandle::app_state`]。
    pub(crate) fn adopt_app_state(&self, foreground: bool) {
        self.reach.set_foreground(foreground);
    }

    /// The estimate of the server clock, shared with whatever compares the
    /// device clock against a deadline the server issued.
    pub(crate) fn server_clock(&self) -> &ServerClock {
        &self.server_clock
    }

    /// Hands the tenant config notices of every later connection to
    /// `notices`; set once while the client is built.
    pub(crate) fn set_tenant_notices(&self, notices: Arc<dyn TenantNoticePort>) {
        if self.tenant_notices.set(notices).is_err() {
            tracing::warn!("tenant notices already set; keeping the first");
        }
    }

    /// prepare（本地半段登录）预写会话身份：本地优先读路径（各 API 的
    /// `ensure_session_active`）在 connect 之前即可用。登出经 [`Self::deactivate_local_session`] 清空。
    pub(crate) async fn adopt_local_session_identity(&self, user_id: &str) {
        *self.current_user_id.write().await = user_id.to_string();
    }

    /// 开始会话的链路：上线（收到 CONNECT_ACK）即返回。首轮候选全部失败时返回错误，但链路状态机
    /// 会继续按退避恢复（终止除外），状态经 `connection://status` 报给宿主。
    #[tracing::instrument(skip(self, tokens), fields(user_id = %user_id))]
    pub(crate) async fn start_link(
        &self,
        user_id: &str,
        tokens: Arc<dyn LinkTokenSource>,
    ) -> crate::shared::error::Result<()> {
        *self.current_user_id.write().await = user_id.to_string();
        self.link.start(user_id, tokens).await
    }

    pub(crate) fn link(&self) -> &LinkHandle {
        &self.link
    }

    /// 开始链路所需的两样东西：链路句柄与当前用户存储。会话层拿到后在锁外等待建连。
    pub(crate) fn link_start_parts(&self) -> (LinkHandle, CurrentUserIdStore) {
        (self.link.clone(), self.current_user_id.clone())
    }

    pub(crate) async fn deactivate_local_session(&self) {
        self.notify.end_session();
        self.link_driver.abort_anti_entropy_probe();
        self.sync_manager.stop_sync();
        if let Some(queue) = &self.reliable_queue {
            queue.shutdown();
        }
        *self.current_user_id.write().await = String::new();
    }

    pub async fn disconnect(&mut self) -> crate::shared::error::Result<()> {
        self.link.stop().await;
        self.deactivate_local_session().await;
        Ok(())
    }

    /// 当前连接状态（由链路状态机驱动）
    pub fn state(&self) -> SdkState {
        self.link.sdk_state()
    }

    /// `connection://status` 的当前值。
    pub fn connection_status(&self) -> ConnectionStatus {
        self.link.status()
    }

    /// 前后台变化：当前连接的心跳跟着变（回前台时传输层立即验活一次），链路状态机据此调整退避上限、
    /// 回前台时立即重试与恢复切回，并把设备状态报给服务端（iOS 进后台即托管）。
    pub async fn set_app_state(&self, state: HeartbeatAppState) -> crate::Result<()> {
        let foreground = matches!(state, HeartbeatAppState::Foreground);
        // 确认的闸门立刻跟着宿主报的状态走，不等链路状态机处理到这条命令：手机一进后台，之后到的
        // 消息就不能再由本进程确认。
        let returned = foreground && !self.reach.is_foreground();
        self.reach.set_foreground(foreground);
        if returned {
            // 回到前台：闸门换代，撤掉已读会话的系统通知，校准角标。不挡生命周期回调。
            let notify = self.notify.clone();
            spawn_background(async move { notify.on_foreground().await });
        }
        // 先告诉链路：进后台后系统留的时间只有几秒，托管不能排在改心跳后面（那要等连接的锁）。
        self.link.app_state(foreground).await;
        self.transport.set_heartbeat_app_state(state).await?;
        Ok(())
    }

    pub async fn set_heartbeat_nat_timeout(&self, timeout: Option<Duration>) -> crate::Result<()> {
        self.transport.set_heartbeat_nat_timeout(timeout).await
    }

    pub async fn heartbeat_effective_interval(&self) -> Option<Duration> {
        self.transport.heartbeat_effective_interval().await
    }

    pub fn bus(&self) -> &EventBus {
        &self.bus
    }

    pub fn sender(&self) -> &Arc<PacketSender> {
        &self.sender
    }

    pub fn stores(&self) -> &StoreProvider {
        &self.stores
    }

    pub fn middleware_chain(&self) -> Arc<MiddlewareChain> {
        self.chain.clone()
    }

    pub fn sync_manager(&self) -> Arc<SyncManager> {
        self.sync_manager.clone()
    }

    /// 单会话消息同步与已读上报（会话列表同步由同步引擎内部执行，不暴露）
    pub fn session_sync_runner(&self) -> Option<Arc<dyn SessionSyncRunner>> {
        self.session_sync.clone()
    }

    pub fn conversation_summary_sync(&self) -> Option<Arc<dyn ConversationSummarySync>> {
        self.conversation_summary_sync.clone()
    }

    pub(crate) fn reliable_queue(&self) -> Option<Arc<ReliableSendQueue>> {
        self.reliable_queue.clone()
    }

    /// 当前已连接用户 ID（未连接时为空）
    pub async fn current_user_id(&self) -> String {
        self.current_user_id.read().await.clone()
    }
}

/// 引擎给链路状态机的驱动：每条连接一个分发器与监听器，上线后恢复发件箱、补发 ACK、补同步。
pub(crate) struct EngineLinkDriver {
    transport: SocketTransport,
    stores: StoreProvider,
    bus: EventBus,
    sender: Arc<PacketSender>,
    codec: Arc<dyn Codec>,
    current_user_id: CurrentUserIdStore,
    sync_manager: Arc<SyncManager>,
    sync_response_handler: Option<Arc<dyn SyncResponseHandler>>,
    session_sync: Option<Arc<dyn SessionSyncRunner>>,
    conversation_summary_sync: Option<Arc<dyn ConversationSummarySync>>,
    reliable_queue: Option<Arc<ReliableSendQueue>>,
    event_deduper: EventDeduper,
    notification_pipeline: NotificationInboundPipeline,
    metrics: MetricsRecorder,
    tenant_notices: Arc<OnceLock<Arc<dyn TenantNoticePort>>>,
    server_clock: ServerClock,
    /// 设备可达状态：每条连接的监听器与分发器各带一份，托管交接的进度也记在这里。
    reach: Arc<DeviceReach>,
    /// 提示台账：各条连接的分发器把确认了的消息记进去。
    alerts: Arc<AlertLedger>,
    /// 提示决策点：每次上线读一次用户级通知设置。
    notify: Arc<NotifyCenter>,
    /// 送达确认的合并器：各条连接的分发器往里记，托管前从这里冲刷。
    delivery_acks: Arc<DeliveryAcks>,
    /// 待上报的设备状态：只留最新的一个，由 [`run_state_reports`] 送出。
    state_reports: watch::Sender<Option<DeviceLinkState>>,
    /// 防熵探测循环（低频对账兜底），会话上线时装载、注销/断连时中止。
    anti_entropy_probe: StdMutex<Option<BackgroundTask>>,
    /// 链路状态机的信号出口：状态机启动后立即写入，之后每条连接的监听器都带一份。
    signals: OnceLock<LinkSignals>,
}

impl EngineLinkDriver {
    /// 托管前的冲刷：攒着的送达确认立即发出，在途的发送等到回执回来。已读回执是即时发的，没有
    /// 攒着的。调用方给它限时。
    async fn flush_before_parking(&self) {
        self.delivery_acks.flush().await;
        let Some(queue) = &self.reliable_queue else {
            return;
        };
        while queue.in_flight().await > 0 {
            delay(PARK_OUTBOX_POLL).await;
        }
        // 等在途发送的这段时间里又处理完的消息。
        self.delivery_acks.flush().await;
    }

    /// 廉价判定本地是否已有当前用户可展示数据（会话非空即视为热启动）。
    /// 存在性探测（EXISTS），不拉整张带 JOIN 的列表。
    async fn has_local_user_data(&self) -> bool {
        match self.stores.conversations.has_any().await {
            Ok(has_any) => has_any,
            Err(error) => {
                tracing::warn!(error = %error, "本地数据探测失败，按冷启动处理");
                false
            }
        }
    }

    /// 启动补同步并返回「就绪」的等待。`nonblocking=true` 走不抢占当前编排的 catch-up。
    fn start_sync(&self, user_id: &str, run: SyncRunContext, nonblocking: bool) -> LinkFuture<()> {
        // 先订阅再启动：就绪事件不能在订阅之前发出。只订就绪事件（量小，不会被进度事件挤掉）。
        let receiver = self
            .bus
            .subscribe_filter(EventFilter::Type(SdkEventType::Sync(
                SyncEventType::Readiness,
            )));
        let run_id = run.run_id.clone();
        // T0 本地水合就绪：本地缓存此刻已可出图，UI 可立即收起骨架、不必等网络同步。
        // 始终发布（即使热启/重连的 sync 为 Silent），因 Readiness 恒对 UI 可见。
        self.bus.publish(SdkEvent::Sync(SyncNotify::Readiness {
            run: run.clone(),
            stage: ReadinessStage::LocalReady,
        }));
        if nonblocking {
            self.sync_manager.run_nonblocking_with_context(
                user_id,
                run,
                self.stores.clone(),
                self.bus.clone(),
            );
        } else {
            self.sync_manager
                .run_with_context(user_id, run, self.stores.clone(), self.bus.clone());
        }
        // 会话上线即（重）装载防熵探测（重复调用会替换并中止旧循环，幂等）。
        self.spawn_anti_entropy_probe();
        boxed(wait_until_synced(receiver, run_id))
    }

    /// 低频防熵探测：每隔 [`ANTI_ENTROPY_INTERVAL`] 做一次摘要级静默对账（O(变化)，
    /// 零增量时=1 个空响应 RPC）。兜"连接健康但下行事件被静默丢失、且无任何后续帧/触发"的
    /// 极端窗口——事件驱动触发（waterline/重连/前台）覆盖不到它。
    fn spawn_anti_entropy_probe(&self) {
        const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(300);
        let Some(summary_sync) = self.conversation_summary_sync.clone() else {
            return;
        };
        let current_user_id = self.current_user_id.clone();
        let task = spawn_background_task(async move {
            loop {
                delay(ANTI_ENTROPY_INTERVAL).await;
                let user_id = current_user_id.read().await.clone();
                if user_id.is_empty() {
                    break;
                }
                if let Err(error) = summary_sync
                    .sync_foreground_convergence(
                        &user_id,
                        SyncRunContext::silent_multidevice_private_data(),
                    )
                    .await
                {
                    tracing::debug!(error = %error, "anti-entropy probe failed (best-effort)");
                }
            }
        });
        let previous = {
            let mut guard = self
                .anti_entropy_probe
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.replace(task)
        };
        if let Some(previous) = previous {
            previous.abort();
        }
    }

    fn abort_anti_entropy_probe(&self) {
        let previous = {
            let mut guard = self
                .anti_entropy_probe
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.take()
        };
        if let Some(previous) = previous {
            previous.abort();
        }
    }
}

/// 等到这次补同步的前台新鲜度达成（或收敛），最多 [`SYNC_READY_CAP`]。
async fn wait_until_synced(mut receiver: EventReceiver, run_id: String) {
    let ready = async {
        loop {
            match receiver.recv().await {
                Ok(SdkEvent::Sync(SyncNotify::Readiness { run, stage }))
                    if run.run_id == run_id && !matches!(stage, ReadinessStage::LocalReady) =>
                {
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    };
    let _ = timeout(SYNC_READY_CAP, ready).await;
}

/// 设备状态上报的唯一出口：一次送一个，送的时候还没送出的只留最新的那个。
///
/// 发送要等连接的锁（同步请求等回包时一直占着），所以不放在链路状态机的执行线上。又必须有先后：
/// 用户刚进后台又回来时，`PARKING` 若排到 `ACTIVE` 后面送出，服务端就会在用户看着屏幕时把提示交给
/// 推送，链路上不再有新消息。发送端随引擎销毁，循环随之结束。
async fn run_state_reports(
    mut reports: watch::Receiver<Option<DeviceLinkState>>,
    sender: Arc<PacketSender>,
    stores: StoreProvider,
    reach: Arc<DeviceReach>,
) {
    while reports.changed().await.is_ok() {
        let latest = *reports.borrow_and_update();
        let Some(state) = latest else {
            continue;
        };
        // 已作废的托管（用户回来了、换了连接）不再送出；读角标期间作废的也一样。
        if state == DeviceLinkState::Parking && !reach.parking_pending() {
            continue;
        }
        let badge = unread_badge(&stores).await;
        if state == DeviceLinkState::Parking && !reach.parking_pending() {
            continue;
        }
        let report = device_state_report(state, badge, now_millis() as i64);
        if let Err(error) = sender.send_device_state(&report).await {
            // 没有连接时属正常：下一次建连会带上当时的状态。
            tracing::debug!(%error, ?state, "device state report was not sent");
        }
    }
}

/// 角标：未读总数，口径与首页一致。读不出来或太慢就报 0。
async fn unread_badge(stores: &StoreProvider) -> u32 {
    match timeout(BADGE_READ_CAP, stores.conversations.list()).await {
        Ok(Ok(conversations)) => {
            u32::try_from(crate::domain::total_unread(&conversations)).unwrap_or(u32::MAX)
        }
        Ok(Err(error)) => {
            tracing::debug!(%error, "reading the unread total for the badge failed");
            0
        }
        Err(_) => 0,
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl LinkDriver for EngineLinkDriver {
    async fn open(&self, request: OpenRequest) -> Result<ConnectReport, ConnectFailure> {
        let Some(signals) = self.signals.get().cloned() else {
            return Err(ConnectFailure {
                attempts: Vec::new(),
                skipped: Vec::new(),
                elapsed: Duration::ZERO,
            });
        };
        // 新连接没有托管过：上一条连接的托管进度到此为止。
        self.reach.end_parking();
        let dispatcher = Arc::new(
            Dispatcher::new(
                self.bus.clone(),
                self.reliable_queue.clone(),
                self.sync_response_handler.clone(),
                self.session_sync.clone(),
                Some(self.stores.clone()),
                self.current_user_id.clone(),
                self.event_deduper.clone(),
                self.notification_pipeline.clone(),
                self.metrics.clone(),
            )
            .with_tenant_notices(self.tenant_notices.get().cloned())
            .with_server_clock(self.server_clock.clone())
            .with_delivery_confirmation(self.reach.clone(), self.delivery_acks.clone())
            .with_alert_ledger(self.alerts.clone()),
        );
        // A2 win#2：建连后启动有界串行持久化 worker，使接收热路径的落盘脱离 socket 读循环。
        dispatcher.start_persist_worker();
        dispatcher.start_typing_sweep();
        let listener = Arc::new(SocketHandler::new(
            dispatcher,
            self.codec.clone(),
            request.epoch,
            signals,
            self.reach.clone(),
        ));
        self.transport.open(&request, listener).await
    }

    async fn close(&self) {
        if let Err(error) = self.transport.disconnect().await {
            tracing::warn!(%error, "closing the connection failed");
        }
    }

    async fn close_parked(&self) {
        self.transport.disconnect_parked().await;
    }

    fn report_device_state(&self, state: DeviceLinkState) {
        // 报了别的状态，进行中的托管即作废：之后才到的 PARKING 应答不算数，排着还没送出的
        // PARKING 也不再送。
        self.reach.end_parking();
        self.state_reports.send_replace(Some(state));
    }

    async fn hand_over(&self) -> bool {
        if timeout(PARK_FLUSH_CAP, self.flush_before_parking())
            .await
            .is_err()
        {
            tracing::debug!("flushing before parking ran out of time");
        }
        // 从这里起等 PARKING 的应答。它之前服务端写出的帧到达时照常处理、照常确认；它之后到的帧
        // 仍然落库，但不再确认，连接关闭时服务端把它们交给推送。
        self.reach.begin_parking();
        self.state_reports
            .send_replace(Some(DeviceLinkState::Parking));
        let acked = timeout(PARK_ACK_CAP, self.reach.parking_acked())
            .await
            .is_ok();
        // 应答之前到的帧的确认赶在关连接之前送出。还在处理中的赶不上，由推送去提示。
        let _ = timeout(PARK_LAST_ACKS_CAP, self.delivery_acks.flush()).await;
        acked
    }

    fn probe(&self) {
        use crate::infrastructure::transport::socket::LinkProbe;
        self.transport.link_probe().request_probe();
    }

    async fn retune_heartbeat(
        &self,
        heartbeat: crate::runtime::link::LinkHeartbeat,
        probe_timeout: Duration,
    ) {
        let foreground = self.reach.is_foreground();
        self.transport
            .retune_heartbeat(heartbeat, probe_timeout, foreground)
            .await;
        // 换节奏的同时回到了前台：宿主报的前台在改传输层之前就写进了可达状态，这里补一次，免得刚换上
        // 的后台配置盖掉前台节奏。
        if !foreground
            && self.reach.is_foreground()
            && let Err(error) = self
                .transport
                .set_heartbeat_app_state(HeartbeatAppState::Foreground)
                .await
        {
            tracing::debug!(%error, "restoring the foreground heartbeat failed");
        }
    }

    async fn heartbeat_round_trip(&self) -> Option<Duration> {
        self.transport.heartbeat_round_trip().await
    }

    async fn resume_outbox(&self) {
        if self.current_user_id.read().await.is_empty() {
            return;
        }
        if let Some(queue) = &self.reliable_queue
            && let Err(error) = queue.resume(true).await
        {
            tracing::warn!(%error, "resume the outbox on the new connection failed");
        }
    }

    async fn link_up(&self, up: LinkUp) -> LinkFuture<()> {
        let user_id = self.current_user_id.read().await.clone();
        if user_id.is_empty() {
            return boxed(async {});
        }
        self.notify.on_link_up(matches!(up, LinkUp::First));
        match up {
            LinkUp::First => {
                // 启动分类：本地已有该用户数据 → 热启动（静默补齐，不阻塞首屏）；本地空 → 冷启动（用户可见 gate）。
                let startup = StartupClass::classify(self.has_local_user_data().await);
                tracing::debug!(?startup, "startup classified");
                self.start_sync(&user_id, startup.startup_sync_run(), false)
            }
            LinkUp::Reconnected { lost_at_ms } => {
                // 发件箱已由 resume_outbox 恢复（在途与待发以原 client_msg_id 重发，服务端幂等返回原结果），
                // 这里补发断链前可能没送到的已读/送达回执（取最大值语义，重复无害）。
                let since = lost_at_ms
                    .map(|lost| (lost.max(0) as u64).saturating_sub(ACK_REPLAY_WINDOW_MS))
                    .unwrap_or(0);
                let replayed = self.sender.replay_acks_since(since).await;
                if replayed > 0 {
                    tracing::debug!(replayed, "replayed acks on the new connection");
                }
                self.start_sync(&user_id, SyncRunContext::reconnect(), false)
            }
            LinkUp::Resumed => {
                if let Some(queue) = &self.reliable_queue
                    && let Err(error) = queue.resume(false).await
                {
                    tracing::warn!(%error, "resume the outbox failed");
                }
                // 连接没断：只做不抢占的补齐，不等它（同步在调用时已启动，返回的只是「等就绪」）。
                drop(self.start_sync(&user_id, SyncRunContext::reconnect(), true));
                boxed(async {})
            }
        }
    }

    async fn link_down(&self) {
        if let Some(queue) = &self.reliable_queue
            && let Err(error) = queue.pause().await
        {
            tracing::warn!(%error, "pause the outbox failed");
        }
    }

    async fn stop(&self) {
        self.abort_anti_entropy_probe();
        self.sync_manager.stop_sync();
    }

    fn last_activity_ms(&self) -> i64 {
        self.sender.last_activity_ms() as i64
    }

    fn observe_server_time(&self, server_time_ms: u64) {
        self.server_clock.observe_server_ms(server_time_ms);
    }
}

use crate::runtime::Dispatcher;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio::time::{Duration, timeout};

    use super::{SdkEngine, SdkEngineConfig};
    use crate::application::notification::{
        NotificationHandlerRegistry, NotificationInboundPipeline,
    };
    use crate::application::services::{EventDeduper, MessageDeduper};
    use crate::client::config::SdkConfig;
    use crate::extension::middleware::MiddlewareChain;
    use crate::infrastructure::persistence::memory_im::in_memory_im_provider;
    use crate::infrastructure::protocol::{Codec, ProtobufCodec};
    use crate::infrastructure::transport::SocketTransport;
    use crate::kernel::CurrentUserIdStore;
    use crate::kernel::event::EventBus;
    use crate::model::IMMessage;
    use crate::runtime::link::{AccessRuntime, AccessStoreSource, ConnectionPhase};
    use crate::spi::metrics::MetricsRecorder;

    fn test_engine(current_user_id: CurrentUserIdStore) -> SdkEngine {
        let stores = in_memory_im_provider();
        let bus = EventBus::new();
        let message_deduper = MessageDeduper::new(Some(8));
        SdkEngine::new(SdkEngineConfig {
            stores,
            chain: Arc::new(MiddlewareChain::new()),
            transport: SocketTransport::new(SdkConfig::default()),
            current_user_id,
            codec: Arc::new(ProtobufCodec) as Arc<dyn Codec>,
            bus: bus.clone(),
            sync_response_handler: None,
            session_sync: None,
            conversation_summary_sync: None,
            event_deduper: EventDeduper::new(Some(8)),
            message_deduper: message_deduper.clone(),
            notification_pipeline: NotificationInboundPipeline::new(
                Arc::new(NotificationHandlerRegistry::new()),
                message_deduper,
                bus,
            ),
            ack_timeout_secs: Some(60),
            ack_max_retries: Some(3),
            ack_max_in_flight: Some(4),
            metrics: MetricsRecorder::disabled(),
            access: Arc::new(AccessRuntime::new(None, AccessStoreSource::memory())),
            network_available: true,
            device_id: "device-1".to_string(),
            locale: None,
        })
    }

    #[tokio::test]
    async fn a_new_engine_is_idle_until_a_session_starts_its_link() {
        let engine = test_engine(Arc::new(RwLock::new(String::new())));
        assert_eq!(engine.connection_status().phase, ConnectionPhase::Idle);
        assert_eq!(engine.state(), crate::kernel::SdkState::Disconnected);
    }

    #[tokio::test]
    async fn deactivate_local_session_clears_user_and_stops_reliable_queue() {
        let current_user_id = Arc::new(RwLock::new("u1".to_string()));
        let engine = test_engine(current_user_id.clone());
        let queue = engine
            .reliable_queue()
            .expect("in-memory provider exposes pending send store");

        engine.deactivate_local_session().await;

        assert_eq!(engine.current_user_id().await, "");
        assert_eq!(current_user_id.read().await.as_str(), "");

        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-after-logout".to_string();
        message.conversation_id = "conv-after-logout".to_string();
        message.sender_id = "u1".to_string();

        let result = timeout(Duration::from_millis(200), queue.enqueue(message)).await;
        assert!(
            matches!(result, Ok(Err(_))),
            "shutdown queue must reject stale enqueue attempts"
        );
    }
}
