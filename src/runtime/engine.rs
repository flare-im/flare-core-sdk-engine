use std::sync::Arc;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use flare_core::common::{HeartbeatAppState, HeartbeatConfig};
use tokio::sync::RwLock;

use crate::application::notification::NotificationInboundPipeline;
use crate::application::services::EventDeduper;
use crate::application::services::MessageDeduper;
use crate::extension::middleware::MiddlewareChain;
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::protocol::{Codec, PacketSender};
use crate::infrastructure::transport::{SocketHandler, SocketTransport};
use crate::kernel::event::{
    ConnectionEvent as SdkConnectionEvent, EventBus, ReadinessStage, SdkEvent, SyncNotify,
};
use crate::kernel::{
    ConnectionEvent, ConnectionFsm, ConnectionState, ConversationSummarySync, CurrentUserIdStore,
    PluginCatalogNoticePort, SdkState, ServerClock, SessionSyncRunner, StartupClass, SyncManager,
    SyncResponseHandler, SyncRunContext,
};
use crate::runtime::{ReliableSendQueue, ReliableSendQueueConfig};
use crate::shared::error::FlareError;
use crate::shared::util::{BackgroundTask, delay, spawn_background_task};
use crate::spi::metrics::MetricsRecorder;

use std::sync::Mutex as StdMutex;

pub struct SdkEngine {
    stores: StoreProvider,
    bus: EventBus,
    sender: Arc<PacketSender>,
    transport: SocketTransport,
    current_user_id: CurrentUserIdStore,
    sync_manager: Arc<SyncManager>,
    sync_response_handler: Option<Arc<dyn SyncResponseHandler>>,
    session_sync: Option<Arc<dyn SessionSyncRunner>>,
    conversation_summary_sync: Option<Arc<dyn ConversationSummarySync>>,
    codec: Arc<dyn Codec>,
    chain: Arc<MiddlewareChain>,
    reliable_queue: Option<Arc<ReliableSendQueue>>,
    connection_state: StdArc<RwLock<ConnectionState>>,
    state_snapshot: AtomicU8,
    event_deduper: EventDeduper,
    message_deduper: MessageDeduper,
    notification_pipeline: NotificationInboundPipeline,
    /// 防熵探测循环（低频对账兜底），会话激活时装载、注销/断连时中止。
    anti_entropy_probe: StdMutex<Option<BackgroundTask>>,
    /// Receives the tenant's plugin catalog-changed notices of every connection.
    plugin_catalog_notices: std::sync::OnceLock<Arc<dyn PluginCatalogNoticePort>>,
    /// The server clock estimate, fed by the downlink frames of every connection.
    server_clock: ServerClock,
    metrics: MetricsRecorder,
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
}

#[derive(Clone, Debug)]
enum ReconnectPlan {
    CatchUpOnly,
    /// 重建传输，携带应先执行的 FSM 转移（决策集中在 plan_reconnect，执行侧不再二次 match state）。
    ReconnectTransport {
        transition: ConnectionEvent,
    },
    AlreadyReconnecting,
    RejectInFlight,
    RejectDifferentUser,
}

fn plan_reconnect(
    state: ConnectionState,
    current_user_id: &str,
    requested_user_id: &str,
    transport_connected: bool,
) -> ReconnectPlan {
    match state {
        ConnectionState::Ready if current_user_id != requested_user_id => {
            ReconnectPlan::RejectDifferentUser
        }
        ConnectionState::Ready if transport_connected => ReconnectPlan::CatchUpOnly,
        ConnectionState::Ready => ReconnectPlan::ReconnectTransport {
            transition: ConnectionEvent::ReconnectRequested,
        },
        ConnectionState::Disconnected => ReconnectPlan::ReconnectTransport {
            transition: ConnectionEvent::ConnectRequested,
        },
        ConnectionState::Reconnecting => ReconnectPlan::AlreadyReconnecting,
        ConnectionState::Connecting | ConnectionState::Connected => ReconnectPlan::RejectInFlight,
    }
}

impl SdkEngine {
    /// 创建引擎。连接就绪后由 [connect] 内 [bootstrap] 激活同步；同步状态仅通过 [EventBus] 的同步回调获取。
    /// `sync_response_handler` / `session_sync` 通常为同一 application SyncProtocolAdapter 的 Arc。
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
        } = config;
        let sender = transport.sender().clone();
        let reliable_queue = stores.pending_sends().map(|(reader, writer)| {
            // 发出后迟迟没回执时验活当前连接；句柄与传输共享客户端槽位，重连后探的是新连接。
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
                },
                Arc::new(transport.link_probe()),
            ))
        });
        Self {
            stores,
            bus,
            sender,
            transport,
            current_user_id,
            sync_manager: Arc::new(SyncManager::new()),
            sync_response_handler,
            session_sync,
            conversation_summary_sync,
            codec,
            chain,
            reliable_queue,
            connection_state: StdArc::new(RwLock::new(ConnectionState::Disconnected)),
            state_snapshot: AtomicU8::new(SdkState::Disconnected.as_u8()),
            event_deduper,
            message_deduper,
            notification_pipeline,
            anti_entropy_probe: StdMutex::new(None),
            plugin_catalog_notices: std::sync::OnceLock::new(),
            server_clock: ServerClock::new(),
            metrics,
        }
    }

    /// The estimate of the server clock, shared with whatever compares the
    /// device clock against a deadline the server issued.
    pub(crate) fn server_clock(&self) -> &ServerClock {
        &self.server_clock
    }

    /// Hands the tenant's plugin catalog-changed notices of every later
    /// connection to `notices`; set once while the client is built.
    pub(crate) fn set_plugin_catalog_notices(&self, notices: Arc<dyn PluginCatalogNoticePort>) {
        if self.plugin_catalog_notices.set(notices).is_err() {
            tracing::warn!("plugin catalog notices already set; keeping the first");
        }
    }

    fn publish_state(&self, state: ConnectionState) {
        self.bus
            .publish(SdkEvent::Connection(SdkConnectionEvent::StateChanged {
                state: state.into(),
            }));
    }

    fn store_state_snapshot(&self, state: ConnectionState) {
        self.state_snapshot
            .store(SdkState::from(state).as_u8(), Ordering::Release);
    }

    async fn transition(&self, event: ConnectionEvent) {
        let mut guard = self.connection_state.write().await;
        match ConnectionFsm::transition(*guard, &event) {
            Ok(next) => {
                *guard = next;
                self.store_state_snapshot(next);
                drop(guard);
                self.publish_state(next);
                // FSM `Connected` 与 EventBus `ConnectionEvent::Connected` 不同名域；
                // Tauri `im://connected` 依赖后者，仅发 StateChanged 会导致前端一直停在「连接中」。
                if next == ConnectionState::Connected {
                    self.bus
                        .publish(SdkEvent::Connection(SdkConnectionEvent::Connected));
                }
            }
            Err(e) => {
                tracing::warn!(%e, "connection FSM transition rejected");
            }
        }
    }

    async fn force_disconnected_after_connect_failure(&self, user_id: &str, error: &FlareError) {
        self.sync_manager.stop_sync();
        if let Err(disconnect_error) = self.transport.disconnect().await {
            tracing::warn!(
                user_id,
                error = %disconnect_error,
                "transport cleanup after connection failure failed"
            );
        }
        // 连接失败保留本地会话身份：库与身份都属于该用户（prepare 已建立），
        // 本地优先读路径（热启动离线出图）不因网络失败而失效；登出才清空。
        {
            let mut guard = self.connection_state.write().await;
            *guard = ConnectionState::Disconnected;
            self.store_state_snapshot(ConnectionState::Disconnected);
        }
        self.publish_state(ConnectionState::Disconnected);
        tracing::warn!(
            user_id,
            error = %error,
            "connection attempt failed; engine state reset to Disconnected"
        );
    }

    /// prepare（本地半段登录）预写会话身份：本地优先读路径（各 API 的
    /// `ensure_session_active`）在 connect 之前即可用。连接态由 `connection_state`
    /// 单独把关；登出经 [`Self::deactivate_local_session`] 清空。
    pub(crate) async fn adopt_local_session_identity(&self, user_id: &str) {
        *self.current_user_id.write().await = user_id.to_string();
    }

    #[tracing::instrument(skip(self, token, sync_run), fields(user_id = %user_id, trigger = ?sync_run.trigger))]
    async fn connect_after_state_transition(
        &mut self,
        user_id: &str,
        token: &str,
        sync_run: SyncRunContext,
    ) -> crate::shared::error::Result<()> {
        let ready = Arc::new(tokio::sync::Notify::new());
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
            .with_plugin_catalog_notices(self.plugin_catalog_notices.get().cloned())
            .with_server_clock(self.server_clock.clone()),
        );
        // A2 win#2：建连后启动有界串行持久化 worker，使接收热路径的落盘脱离 socket 读循环。
        dispatcher.start_persist_worker();
        dispatcher.start_typing_sweep();
        let listener = Arc::new(SocketHandler::new(
            dispatcher,
            self.codec.clone(),
            ready.clone(),
        ));
        if let Err(error) = self
            .transport
            .connect(user_id, token, listener, ready)
            .await
        {
            self.force_disconnected_after_connect_failure(user_id, &error)
                .await;
            return Err(error);
        }
        *self.current_user_id.write().await = user_id.to_string();
        if let Some(queue) = &self.reliable_queue
            && let Err(error) = queue.recover_pending_for_current_user().await
        {
            tracing::warn!(
                %user_id,
                %error,
                "recover pending sends after reconnect failed"
            );
        }
        self.transition(ConnectionEvent::Connected).await;
        if let Err(error) = self.bootstrap(sync_run).await {
            self.force_disconnected_after_connect_failure(user_id, &error)
                .await;
            return Err(error);
        }
        self.transition(ConnectionEvent::BootstrapDone).await;
        Ok(())
    }

    /// 连接服务器。同一用户已就绪时幂等返回；正在连接中或已连接为其他用户时返回错误，避免重复建连导致服务端踢线。
    #[tracing::instrument(skip(self, token), fields(user_id = %user_id))]
    pub async fn connect(
        &mut self,
        user_id: &str,
        token: &str,
    ) -> crate::shared::error::Result<()> {
        let (state, current_uid) = {
            let s = *self.connection_state.read().await;
            let uid = self.current_user_id.read().await.clone();
            (s, uid)
        };
        match state {
            ConnectionState::Ready if current_uid == user_id => {
                tracing::debug!(%user_id, "connect idempotent: already connected as same user");
                return Ok(());
            }
            ConnectionState::Ready => {
                return Err(FlareError::general_error(format!(
                    "already connected as {}, disconnect first before connecting as {}",
                    current_uid, user_id
                )));
            }
            ConnectionState::Connecting
            | ConnectionState::Connected
            | ConnectionState::Reconnecting => {
                return Err(FlareError::general_error(
                    "connect already in progress or reconnecting, wait for it to finish or fail",
                ));
            }
            ConnectionState::Disconnected => {}
        }

        self.transition(ConnectionEvent::ConnectRequested).await;
        // 启动分类：本地已有该用户数据 → 热启动（静默补齐，不阻塞首屏）；本地空 → 冷启动（用户可见 gate）。
        let startup = StartupClass::classify(self.has_local_user_data().await);
        tracing::debug!(?startup, "startup classified");
        self.connect_after_state_transition(user_id, token, startup.startup_sync_run())
            .await
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

    pub async fn mark_transport_disconnected(&self) {
        self.transition(ConnectionEvent::Disconnected).await;
    }

    #[tracing::instrument(skip(self, token), fields(user_id = %user_id))]
    pub async fn reconnect(
        &mut self,
        user_id: &str,
        token: &str,
    ) -> crate::shared::error::Result<()> {
        let (state, current_uid) = {
            let state = *self.connection_state.read().await;
            let current_uid = self.current_user_id.read().await.clone();
            (state, current_uid)
        };
        let transport_connected = self.transport.is_connected().await;
        match plan_reconnect(state, &current_uid, user_id, transport_connected) {
            ReconnectPlan::CatchUpOnly => {
                tracing::debug!(
                    %user_id,
                    "reconnect catch-up only: transport is still connected"
                );
                self.bootstrap_nonblocking(SyncRunContext::reconnect())
                    .await?;
                Ok(())
            }
            ReconnectPlan::ReconnectTransport { transition } => {
                self.transition(transition).await;
                self.sync_manager.stop_sync();
                self.transport.disconnect().await?;
                self.connect_after_state_transition(user_id, token, SyncRunContext::reconnect())
                    .await
            }
            ReconnectPlan::AlreadyReconnecting => Ok(()),
            ReconnectPlan::RejectInFlight => {
                return Err(FlareError::general_error(
                    "connect already in progress, skip reconnect",
                ));
            }
            ReconnectPlan::RejectDifferentUser => Err(FlareError::general_error(format!(
                "already connected as {}, disconnect first before reconnecting as {}",
                current_uid, user_id
            ))),
        }
    }

    pub(crate) async fn deactivate_local_session(&self) {
        self.abort_anti_entropy_probe();
        self.sync_manager.stop_sync();
        if let Some(queue) = &self.reliable_queue {
            queue.shutdown();
        }
        *self.current_user_id.write().await = String::new();
    }

    pub async fn disconnect(&mut self) -> crate::shared::error::Result<()> {
        self.transition(ConnectionEvent::DisconnectRequested).await;
        self.deactivate_local_session().await;
        self.transport.disconnect().await?;
        {
            let mut guard = self.connection_state.write().await;
            // The FSM normally performed this transition before transport
            // teardown. Keep the assignment as a defensive fallback, but do not
            // publish the same disconnected state twice.
            if *guard != ConnectionState::Disconnected {
                *guard = ConnectionState::Disconnected;
                self.store_state_snapshot(ConnectionState::Disconnected);
                drop(guard);
                self.publish_state(ConnectionState::Disconnected);
            }
        }
        Ok(())
    }

    pub async fn bootstrap(
        &mut self,
        sync_run: SyncRunContext,
    ) -> crate::shared::error::Result<()> {
        self.bootstrap_with(sync_run, false).await
    }

    async fn bootstrap_nonblocking(
        &mut self,
        sync_run: SyncRunContext,
    ) -> crate::shared::error::Result<()> {
        self.bootstrap_with(sync_run, true).await
    }

    /// 低频防熵探测：每隔 [`ANTI_ENTROPY_INTERVAL`] 做一次摘要级静默对账（O(变化)，
    /// 零增量时=1 个空响应 RPC）。兜"连接健康但下行事件被静默丢失、且无任何后续帧/触发"的
    /// 极端窗口——事件驱动触发（waterline/重连/前台）覆盖不到它。
    ///
    /// 与 C2 删除的 30s 扁平前台轮询不同：这是端到端**源头对账**（网关无源头水位，
    /// 心跳捎带水位方案被否——对账必须问数据源头，本质上就是这次轻量增量同步），
    /// 周期长一个数量级、成本 O(变化)。会话注销/断连时中止。
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

    /// `nonblocking=true` 走不抢占当前编排的 catch-up（热重连/网络切换）；否则抢占式全量编排。
    async fn bootstrap_with(
        &mut self,
        sync_run: SyncRunContext,
        nonblocking: bool,
    ) -> crate::shared::error::Result<()> {
        let user_id = self.current_user_id.read().await.clone();
        if user_id.is_empty() {
            return Ok(());
        }
        // T0 本地水合就绪：本地缓存此刻已可出图，UI 可立即收起骨架、不必等网络同步。
        // 始终发布（即使热启/重连的 sync 为 Silent），因 Readiness 恒对 UI 可见。
        self.bus.publish(SdkEvent::Sync(SyncNotify::Readiness {
            run: sync_run.clone(),
            stage: ReadinessStage::LocalReady,
        }));
        if nonblocking {
            self.sync_manager.run_nonblocking_with_context(
                &user_id,
                sync_run,
                self.stores.clone(),
                self.bus.clone(),
            );
        } else {
            self.sync_manager.run_with_context(
                &user_id,
                sync_run,
                self.stores.clone(),
                self.bus.clone(),
            );
        }
        // 会话激活即（重）装载防熵探测（重复调用会替换并中止旧循环，幂等）。
        self.spawn_anti_entropy_probe();
        Ok(())
    }

    /// 当前连接状态（由 FSM 驱动）
    pub fn state(&self) -> SdkState {
        SdkState::from_u8(self.state_snapshot.load(Ordering::Acquire))
    }

    pub async fn transport_connected(&self) -> bool {
        self.transport.is_connected().await
    }

    /// 连接「看起来」还在（Ready）时对它做一次即时验活：立即 ping，验活窗口（8 s）内没有 PONG
    /// 就按心跳超时断开，之后走断开 → 重连监视器。返回是否真的请求了验活。
    pub async fn probe_connection_if_ready(&self) -> bool {
        if self.state() != SdkState::Ready {
            return false;
        }
        self.transport.probe_link().await
    }

    pub async fn update_heartbeat_config(&self, config: HeartbeatConfig) -> crate::Result<()> {
        self.transport.update_heartbeat_config(config).await
    }

    pub async fn set_heartbeat_app_state(&self, state: HeartbeatAppState) -> crate::Result<()> {
        self.transport.set_heartbeat_app_state(state).await
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

use crate::runtime::Dispatcher;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio::time::{Duration, timeout};

    use super::{ReconnectPlan, SdkEngine, SdkEngineConfig, plan_reconnect};
    use crate::application::notification::{
        NotificationHandlerRegistry, NotificationInboundPipeline,
    };
    use crate::application::services::{EventDeduper, MessageDeduper};
    use crate::client::config::SdkConfig;
    use crate::extension::middleware::MiddlewareChain;
    use crate::infrastructure::persistence::memory_im::in_memory_im_provider;
    use crate::infrastructure::protocol::{Codec, ProtobufCodec};
    use crate::infrastructure::transport::SocketTransport;
    use crate::kernel::event::EventBus;
    use crate::kernel::{ConnectionState, CurrentUserIdStore};
    use crate::model::IMMessage;
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
        })
    }

    #[test]
    fn reconnect_plan_uses_catch_up_only_when_ready_transport_is_alive() {
        use crate::kernel::ConnectionEvent;
        assert!(matches!(
            plan_reconnect(ConnectionState::Ready, "u1", "u1", true),
            ReconnectPlan::CatchUpOnly
        ));
        assert!(matches!(
            plan_reconnect(ConnectionState::Ready, "u1", "u1", false),
            ReconnectPlan::ReconnectTransport {
                transition: ConnectionEvent::ReconnectRequested
            }
        ));
        assert!(matches!(
            plan_reconnect(ConnectionState::Disconnected, "", "u1", false),
            ReconnectPlan::ReconnectTransport {
                transition: ConnectionEvent::ConnectRequested
            }
        ));
        assert!(matches!(
            plan_reconnect(ConnectionState::Ready, "u1", "u2", true),
            ReconnectPlan::RejectDifferentUser
        ));
        assert!(matches!(
            plan_reconnect(ConnectionState::Connecting, "u1", "u1", false),
            ReconnectPlan::RejectInFlight
        ));
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
