use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::time::Duration;

use flare_core::client::link::{ConnectFailure, ConnectReport};
use flare_core::common::HeartbeatAppState;

use crate::application::notification::NotificationInboundPipeline;
use crate::application::services::EventDeduper;
use crate::application::services::MessageDeduper;
use crate::extension::middleware::MiddlewareChain;
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::protocol::{Codec, PacketSender};
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
    AccessRuntime, ConnectionStatus, LinkDriver, LinkFuture, LinkHandle, LinkSignals,
    LinkTokenSource, LinkUp, OpenRequest, boxed,
};
use crate::runtime::{ReliableSendQueue, ReliableSendQueueConfig};
use crate::shared::util::{BackgroundTask, delay, spawn_background_task, timeout};
use crate::spi::metrics::MetricsRecorder;

/// 补同步就绪的等待上限：到点还没就绪也按上线处理（同步在后台继续），不让「正在同步」一直挂着。
const SYNC_READY_CAP: Duration = Duration::from_secs(10);
/// 换了新连接后补发断链前多久之内发出的 ACK：覆盖心跳判死（前台 90 s）加验活窗口的时间。
const ACK_REPLAY_WINDOW_MS: u64 = 120_000;

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
            anti_entropy_probe: StdMutex::new(None),
            signals: OnceLock::new(),
        });
        let link = LinkHandle::spawn(
            link_driver.clone() as Arc<dyn LinkDriver>,
            access,
            bus.clone(),
            network_available,
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
            link_driver,
            link,
        }
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
    /// 回前台时立即重试与恢复切回。
    pub async fn set_app_state(&self, state: HeartbeatAppState) -> crate::Result<()> {
        self.transport.set_heartbeat_app_state(state).await?;
        self.link
            .app_state(matches!(state, HeartbeatAppState::Foreground))
            .await;
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
    /// 防熵探测循环（低频对账兜底），会话上线时装载、注销/断连时中止。
    anti_entropy_probe: StdMutex<Option<BackgroundTask>>,
    /// 链路状态机的信号出口：状态机启动后立即写入，之后每条连接的监听器都带一份。
    signals: OnceLock<LinkSignals>,
}

impl EngineLinkDriver {
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
            .with_server_clock(self.server_clock.clone()),
        );
        // A2 win#2：建连后启动有界串行持久化 worker，使接收热路径的落盘脱离 socket 读循环。
        dispatcher.start_persist_worker();
        dispatcher.start_typing_sweep();
        let listener = Arc::new(SocketHandler::new(
            dispatcher,
            self.codec.clone(),
            request.epoch,
            signals,
        ));
        self.transport.open(&request, listener).await
    }

    async fn close(&self) {
        if let Err(error) = self.transport.disconnect().await {
            tracing::warn!(%error, "closing the connection failed");
        }
    }

    fn probe(&self) {
        use crate::infrastructure::transport::socket::LinkProbe;
        self.transport.link_probe().request_probe();
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
