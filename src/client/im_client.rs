//! [`IMClient`]：初始化、登录、登出与消息/会话 API（单一入口）。
//!
//! 内部为 [`tokio::sync::RwLock`]：同步读路径使用 `try_read`（禁止 `blocking_read`），避免在 `#[tokio::main]` 或异步任务中 panic。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use arc_swap::ArcSwapOption;
use tokio::sync::{Notify, RwLock};

use crate::FlareError;
use crate::application::notification::NotificationHandlerRegistry;
use crate::application::services::LocalIdentityRepairService;
use crate::client::api::{
    CapabilityApi, ConversationApi, MediaApi, MessageApi, MessageBuildApi, PresenceApi, ViewApi,
};
use crate::client::builder::{IMClientBuilder, IMClientExtensionComponents};
use crate::client::connected_apis::ConnectedApis;
use crate::client::lifecycle::{SdkConfigOverlay, resolve_sdk_data_root};
use crate::extension::ExtensionRuntime;
use crate::extension::capability::SdkCapabilityRegistry;
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::transport::http::HttpRequestContext;
use crate::kernel::SdkState;
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::model::message::MessageLocalState;
use crate::runtime::SdkEngine;
use crate::shared::error::{ErrorCode, Result};
use crate::spi::metrics::{MetricsRecorder, MetricsSnapshot};
use flare_core::common::HeartbeatAppState;
use flare_proto::common::MessageStatus;
use std::future::Future;

use crate::shared::util::spawn_background;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

const HEARTBEAT_APP_STATE_FOREGROUND: u8 = 0;
const HEARTBEAT_APP_STATE_BACKGROUND: u8 = 1;

mod async_accessors;
mod extension_lifecycle;
mod facades;
mod link_api;
mod push_api;
mod session_lifecycle;
mod session_watchers;
mod sync_api;
mod update_api;

pub use link_api::PinnedAccessPoint;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetworkChangeEvent {
    /// Whether the host currently has a usable network route.
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expensive: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metered: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// 平台给的粗粒度网络标识（原生：Wi-Fi 网关 IP 前缀 / 蜂窝 MCC-MNC；Web：`navigator.connection.type`）。
    /// 只在本地与 `interface` 一起哈希成网络指纹，健康记录按它分开；不上报，不要传 SSID。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_hint: Option<String>,
}

impl NetworkChangeEvent {
    /// 按流量计费，或系统认为这条路贵（蜂窝、个人热点）。
    pub(crate) fn is_metered(&self) -> bool {
        self.metered.unwrap_or(false) || self.expensive.unwrap_or(false)
    }
}

/// 服务端时钟的估计（[`IMClient::server_clock`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerClockEstimate {
    /// 服务端时钟减本机时钟（毫秒）；还没有估计时为 0，即本机时钟。
    pub offset_ms: i64,
    /// 是否已有估计：本次会话收到过服务端打了时间戳的载荷帧。
    pub estimated: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeHealthSnapshot {
    pub metrics_enabled: bool,
    pub state: SdkState,
    pub session_generation: u64,
    pub raw_subscriber_dropped_total: u64,
    pub metrics: MetricsSnapshot,
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_im_background<F>(future: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_background(async move {
        let _ = future.await;
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_im_background<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    spawn_background(future);
}

#[derive(Default)]
pub(crate) struct IMClientInner {
    pub environment: Option<String>,
    pub sdk_config: Option<SdkConfigOverlay>,
    /// 构建期传入的完整配置。登录重建会话时以它为底叠 overlay，而不是以默认值为底——
    /// 否则宿主在构建期配好的接入配置等会在登录那一刻被默认值顶掉。
    pub configured_config: Option<crate::client::config::SdkConfig>,
    /// 接入运行时（目录、健康簿、选路）。跨会话共用：属于设备与网络，不属于某次登录。
    pub access: Option<Arc<crate::runtime::link::AccessRuntime>>,
    /// 宿主注入的登录前全局存储（[`IMClient::set_access_store`]）；没有时 init 用平台默认。
    pub access_store: Option<InjectedAccessStore>,
    /// 宿主最后一次报的网络状态（`notify_network_change` 的完整输入）。平台常在登录之前首报、之后对
    /// 相同状态去重不再重报：之后新建的接入运行时与链路状态机都从它起步。没报过为 `None`（视为可用）。
    pub last_network: Option<NetworkChangeEvent>,
    /// 上一个准备过的会话用户：同一用户重新 prepare 时恢复发件箱，换了用户才清空。
    pub last_session_user: Option<String>,
    pub data_root: Option<PathBuf>,
    pub current_user_id: Option<String>,
    pub connect_token: Option<String>,
    /// 长效刷新令牌（SDK 托管形态由网关下发）。接入令牌过期后凭它换新，支撑 7x24。
    pub refresh_token: Option<String>,
    /// 宿主托管令牌时的续期回调（见 [`crate::client::token_provider::ConnectTokenRefresher`]）。
    pub connect_token_refresher:
        Option<Arc<dyn crate::client::token_provider::ConnectTokenRefresher>>,
    pub engine: Option<SdkEngine>,
    pub message_api: Option<MessageApi>,
    pub media_api: Option<Arc<MediaApi>>,
    pub capability_api: Option<Arc<CapabilityApi>>,
    pub presence_api: Option<Arc<PresenceApi>>,
    pub capability_registry: Option<Arc<SdkCapabilityRegistry>>,
    pub notification_registry: Option<Arc<NotificationHandlerRegistry>>,
    pub extension_runtime: Option<Arc<ExtensionRuntime>>,
    pub extension_components: IMClientExtensionComponents,
    pub message_build_api: Option<Arc<MessageBuildApi>>,
    pub conversation_api: Option<Arc<ConversationApi>>,
    pub view_api: Option<Arc<ViewApi>>,
    pub http_request_context: Option<Arc<HttpRequestContext>>,
    pub session_generation: u64,
    pub metrics: MetricsRecorder,
}

/// 宿主注入的登录前全局存储。
///
/// 浏览器宿主的存储握着 JS 对象，不是 `Send`；wasm32-unknown-unknown 只有页面线程，值不可能离开它，
/// 所以与 [`crate::runtime::link::AccessRuntime`] 同样只在 wasm 上声明 `Send + Sync`，
/// 让 [`IMClientInner`] 的线程约束在两端保持一致。
#[derive(Clone)]
pub(crate) struct InjectedAccessStore(pub(crate) Arc<dyn flare_access::AccessStore>);

#[cfg(target_arch = "wasm32")]
unsafe impl Send for InjectedAccessStore {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for InjectedAccessStore {}

/// 会话结束信号：会话世代每变一次（登出、断开、重新登录、终止会话）唤醒所有在等的会话监视器；
/// 最后一个 [`IMClient`] 句柄释放（客户端销毁）时也唤醒一次。
///
/// 监视器只持有里面的 [`Notify`]，不持有本结构，所以句柄全部释放时 `Drop` 一定会跑到。
struct SessionEndSignal(Arc<Notify>);

impl SessionEndSignal {
    fn new() -> Self {
        Self(Arc::new(Notify::new()))
    }

    fn notify(&self) {
        self.0.notify_waiters();
    }
}

impl Drop for SessionEndSignal {
    fn drop(&mut self) {
        self.notify();
    }
}

/// 会话监视器等待「本世代会话已结束」的句柄，见 [`IMClient::session_end_watch`]。
#[derive(Clone)]
struct SessionEndWatch {
    notify: Arc<Notify>,
    generation_snapshot: Weak<AtomicU64>,
    generation: u64,
}

impl SessionEndWatch {
    /// 会话世代已不是监视器所属的那一代，或客户端已销毁。
    fn is_ended(&self) -> bool {
        self.generation_snapshot
            .upgrade()
            .is_none_or(|snapshot| snapshot.load(Ordering::Acquire) != self.generation)
    }

    /// 本世代会话结束时返回；已结束则立即返回。
    async fn ended(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // 先登记再判断：判断与等待之间发生的结束不会被漏掉。
            notified.as_mut().enable();
            if self.is_ended() {
                return;
            }
            notified.await;
        }
    }
}

/// 唯一 SDK 句柄：[`Self::init`] → [`Self::login`]，或 [`Self::builder`] → [`Self::connect`]。
#[derive(Clone)]
pub struct IMClient {
    pub(crate) inner: Arc<RwLock<IMClientInner>>,
    session_snapshot: Arc<ArcSwapOption<ConnectedApis>>,
    state_snapshot: Arc<AtomicU8>,
    session_generation_snapshot: Arc<AtomicU64>,
    heartbeat_app_state_snapshot: Arc<AtomicU8>,
    startup_convergence_in_flight: Arc<AtomicBool>,
    session_end: Arc<SessionEndSignal>,
    push_lifecycle: Arc<crate::client::push_lifecycle::PushLifecycle>,
}

#[derive(Clone)]
pub(crate) struct WeakIMClient {
    inner: Weak<RwLock<IMClientInner>>,
    session_snapshot: Weak<ArcSwapOption<ConnectedApis>>,
    state_snapshot: Weak<AtomicU8>,
    session_generation_snapshot: Weak<AtomicU64>,
    heartbeat_app_state_snapshot: Weak<AtomicU8>,
    startup_convergence_in_flight: Weak<AtomicBool>,
    session_end: Weak<SessionEndSignal>,
    push_lifecycle: Weak<crate::client::push_lifecycle::PushLifecycle>,
}

impl WeakIMClient {
    pub(crate) fn upgrade(&self) -> Option<IMClient> {
        Some(IMClient {
            inner: self.inner.upgrade()?,
            session_snapshot: self.session_snapshot.upgrade()?,
            state_snapshot: self.state_snapshot.upgrade()?,
            session_generation_snapshot: self.session_generation_snapshot.upgrade()?,
            heartbeat_app_state_snapshot: self.heartbeat_app_state_snapshot.upgrade()?,
            startup_convergence_in_flight: self.startup_convergence_in_flight.upgrade()?,
            session_end: self.session_end.upgrade()?,
            push_lifecycle: self.push_lifecycle.upgrade()?,
        })
    }
}

impl IMClient {
    /// 创建一个空的 SDK 客户端句柄。
    ///
    /// 仅创建内存中的客户端壳，不会触发网络连接或磁盘初始化；
    /// 需后续调用 [`Self::init`] + [`Self::login`]（或 builder 路径）进入可用态。
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(IMClientInner::default())),
            session_snapshot: Arc::new(ArcSwapOption::empty()),
            state_snapshot: Arc::new(AtomicU8::new(SdkState::Disconnected.as_u8())),
            session_generation_snapshot: Arc::new(AtomicU64::new(0)),
            heartbeat_app_state_snapshot: Arc::new(AtomicU8::new(HEARTBEAT_APP_STATE_FOREGROUND)),
            startup_convergence_in_flight: Arc::new(AtomicBool::new(false)),
            session_end: Arc::new(SessionEndSignal::new()),
            push_lifecycle: Arc::new(crate::client::push_lifecycle::PushLifecycle::default()),
        }
    }

    /// 返回链式构建器，用于自定义存储、编解码器与中间件后再构建 [`IMClient`]。
    pub fn builder() -> IMClientBuilder {
        IMClientBuilder::new()
    }

    pub(crate) fn from_inner(inner: IMClientInner) -> Self {
        let generation = inner.session_generation;
        let state = inner
            .engine
            .as_ref()
            .map(|engine| engine.state())
            .unwrap_or(SdkState::Disconnected);
        Self {
            inner: Arc::new(RwLock::new(inner)),
            session_snapshot: Arc::new(ArcSwapOption::empty()),
            state_snapshot: Arc::new(AtomicU8::new(state.as_u8())),
            session_generation_snapshot: Arc::new(AtomicU64::new(generation)),
            heartbeat_app_state_snapshot: Arc::new(AtomicU8::new(HEARTBEAT_APP_STATE_FOREGROUND)),
            startup_convergence_in_flight: Arc::new(AtomicBool::new(false)),
            session_end: Arc::new(SessionEndSignal::new()),
            push_lifecycle: Arc::new(crate::client::push_lifecycle::PushLifecycle::default()),
        }
    }

    pub(crate) fn downgrade(&self) -> WeakIMClient {
        WeakIMClient {
            inner: Arc::downgrade(&self.inner),
            session_snapshot: Arc::downgrade(&self.session_snapshot),
            state_snapshot: Arc::downgrade(&self.state_snapshot),
            session_generation_snapshot: Arc::downgrade(&self.session_generation_snapshot),
            heartbeat_app_state_snapshot: Arc::downgrade(&self.heartbeat_app_state_snapshot),
            startup_convergence_in_flight: Arc::downgrade(&self.startup_convergence_in_flight),
            session_end: Arc::downgrade(&self.session_end),
            push_lifecycle: Arc::downgrade(&self.push_lifecycle),
        }
    }

    pub(crate) fn into_inner(self) -> Result<IMClientInner> {
        match Arc::try_unwrap(self.inner) {
            Ok(rw) => Ok(rw.into_inner()),
            Err(_) => Err(FlareError::localized(
                ErrorCode::InternalError,
                "IMClient must be uniquely owned",
            )),
        }
    }

    fn not_connected() -> FlareError {
        FlareError::localized(ErrorCode::NotConnected, "SDK not connected")
    }

    fn lock_busy() -> FlareError {
        FlareError::localized(ErrorCode::InternalError, "IMClient lock busy")
    }

    fn resolve_tenant_id(g: &IMClientInner) -> String {
        g.sdk_config
            .as_ref()
            .and_then(|c| c.tenant_id.clone())
            .filter(|t| !t.is_empty())
            .or_else(|| std::env::var("FLARE_IM_TENANT_ID").ok())
            .map(crate::shared::util::normalize_tenant_id)
            .unwrap_or_else(|| "0".to_string())
    }

    fn store_state_snapshot(&self, state: SdkState) {
        self.state_snapshot.store(state.as_u8(), Ordering::Release);
    }

    fn load_state_snapshot(&self) -> SdkState {
        SdkState::from_u8(self.state_snapshot.load(Ordering::Acquire))
    }

    fn store_session_generation_snapshot(&self, generation: u64) {
        self.session_generation_snapshot
            .store(generation, Ordering::Release);
        // 世代一变，上一代会话的监视器就该收手（登出、断开、重新登录、终止都走这里）。
        self.session_end.notify();
    }

    /// 给第 `generation` 代会话的监视器用：会话结束（世代变化或客户端销毁）时返回。
    fn session_end_watch(&self, generation: u64) -> SessionEndWatch {
        SessionEndWatch {
            notify: self.session_end.0.clone(),
            generation_snapshot: Arc::downgrade(&self.session_generation_snapshot),
            generation,
        }
    }

    fn load_session_generation_snapshot(&self) -> u64 {
        self.session_generation_snapshot.load(Ordering::Acquire)
    }

    fn store_heartbeat_app_state_snapshot(&self, state: HeartbeatAppState) {
        self.heartbeat_app_state_snapshot.store(
            match state {
                HeartbeatAppState::Foreground => HEARTBEAT_APP_STATE_FOREGROUND,
                HeartbeatAppState::Background => HEARTBEAT_APP_STATE_BACKGROUND,
            },
            Ordering::Release,
        );
    }

    #[cfg(test)]
    fn is_app_foreground_snapshot(&self) -> bool {
        self.heartbeat_app_state_snapshot.load(Ordering::Acquire) == HEARTBEAT_APP_STATE_FOREGROUND
    }

    fn clear_session_snapshot(&self) {
        self.push_lifecycle.detach();
        self.session_snapshot.store(None);
    }

    fn store_connected_apis_snapshot(&self, apis: ConnectedApis) {
        self.session_snapshot.store(Some(Arc::new(apis)));
    }

    fn active_connected_apis_snapshot(&self) -> Option<ConnectedApis> {
        self.session_snapshot
            .load_full()
            .filter(|apis| apis.session_generation() == self.load_session_generation_snapshot())
            .map(|apis| (*apis).clone())
    }

    fn connected_apis_from_inner(g: &IMClientInner) -> Result<ConnectedApis> {
        Ok(ConnectedApis {
            session_generation: g.session_generation,
            message_api: g.message_api.clone().ok_or_else(Self::not_connected)?,
            conversation_api: g
                .conversation_api
                .as_ref()
                .map(|a| a.as_ref().clone())
                .ok_or_else(Self::not_connected)?,
            media_api: g.media_api.clone().ok_or_else(Self::not_connected)?,
            capability_api: g.capability_api.clone().ok_or_else(Self::not_connected)?,
            presence_api: g.presence_api.clone().ok_or_else(Self::not_connected)?,
            message_build_api: g
                .message_build_api
                .clone()
                .ok_or_else(Self::not_connected)?,
            view_api: g.view_api.clone().ok_or_else(Self::not_connected)?,
            capability_registry: g
                .capability_registry
                .clone()
                .ok_or_else(Self::not_connected)?,
        })
    }

    fn connected_apis_sync(&self) -> Result<ConnectedApis> {
        if let Some(apis) = self.active_connected_apis_snapshot() {
            return Ok(apis);
        }

        let g = self.read_active_inner()?;
        let apis = Self::connected_apis_from_inner(&g)?;
        self.store_connected_apis_snapshot(apis.clone());
        drop(g);
        Ok(apis)
    }

    // Local APIs belong to the prepared user session, not to a socket state.
    // Remote operations enforce their own transport requirements.
    fn inner_session_active(&self, g: &IMClientInner) -> bool {
        g.current_user_id
            .as_ref()
            .is_some_and(|id| !id.trim().is_empty())
    }

    /// 同步 API 使用的读锁：在 Tokio worker 上 **禁止** `blocking_read`，必须用 `try_read`。
    pub(crate) fn read_inner(&self) -> Result<tokio::sync::RwLockReadGuard<'_, IMClientInner>> {
        self.inner.try_read().map_err(|_| Self::lock_busy())
    }

    fn read_active_inner(&self) -> Result<tokio::sync::RwLockReadGuard<'_, IMClientInner>> {
        let g = self.read_inner()?;
        if self.inner_session_active(&g) {
            Ok(g)
        } else {
            Err(Self::not_connected())
        }
    }

    /// 异步读锁：IPC 热路径应优先使用，避免 `try_read` 在写锁排队时返回 lock busy。
    pub(crate) async fn read_inner_async(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, IMClientInner>> {
        Ok(self.inner.read().await)
    }

    pub(crate) fn with_engine<R>(&self, f: impl FnOnce(&SdkEngine) -> R) -> Result<R> {
        let g = self.read_inner()?;
        let e = g.engine.as_ref().ok_or_else(Self::not_connected)?;
        Ok(f(e))
    }

    pub(crate) async fn with_engine_async<R>(&self, f: impl FnOnce(&SdkEngine) -> R) -> Result<R> {
        let g = self.read_inner_async().await?;
        let e = g.engine.as_ref().ok_or_else(Self::not_connected)?;
        Ok(f(e))
    }

    /// 登录成功后导出 Facade 快照，供 Tauri `SdkState` 缓存（避免每条 IPC 抢 `IMClient` 锁）。
    pub async fn connected_apis(&self) -> Result<ConnectedApis> {
        if let Some(apis) = self.active_connected_apis_snapshot() {
            return Ok(apis);
        }

        let g = self.read_inner_async().await?;
        if !self.inner_session_active(&g) {
            return Err(Self::not_connected());
        }
        let apis = Self::connected_apis_from_inner(&g)?;
        self.store_connected_apis_snapshot(apis.clone());
        drop(g);
        Ok(apis)
    }

    /// 装上登录前全局存储（接入清单 LKG、密钥状态、健康簿），下一次 [`Self::init`] 起生效。
    ///
    /// 浏览器宿主用它接 IndexedDB；`None` 还原平台默认（原生在数据根下开 SQLite，wasm 在内存里）。
    pub async fn set_access_store(&self, store: Option<Arc<dyn flare_access::AccessStore>>) {
        self.inner.write().await.access_store = store.map(InjectedAccessStore);
    }

    /// 初始化运行环境与 SDK 配置快照。
    ///
    /// - 更新本地配置，不建立 IM 连接；配置稳定设备和 Core HTTP 后可恢复旧推送撤销；
    /// - 若传入 `sdkConfig.dataUrl`，会解析为数据根；未传则使用 SDK 默认系统数据目录；
    /// - 会确保数据根存在，登录时按用户自动创建 SQLite 与媒体缓存目录；
    /// - 后续 [`Self::login`] 会基于该配置构建实际存储与连接参数。
    pub async fn init(
        &self,
        environment: Option<String>,
        sdk_config: Option<SdkConfigOverlay>,
    ) -> Result<()> {
        self.push_lifecycle.detach();
        let mut g = self.inner.write().await;
        g.environment = environment;
        let data_root =
            resolve_sdk_data_root(sdk_config.as_ref().and_then(|cfg| cfg.data_url.as_deref()))?;
        // Synchronous dir creation: this runs inside a host command future (e.g. the Tauri
        // tokio runtime), and core's `tokio::fs` (a possibly different tokio version) would
        // look up its own runtime handle and panic with "no reactor running". A one-time
        // mkdir needs no async anyway.
        #[cfg(not(target_arch = "wasm32"))]
        std::fs::create_dir_all(&data_root).map_err(|e| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                format!("sdk data root create_dir_all failed: {}", e),
            )
        })?;
        // 接入配置：init 的覆盖项优先，其次构建期配置。装配（读 LKG、验签、加载健康簿）在后台开始，
        // 不占调用线程；第一次建连时若还没完成再等它。
        let access_config = sdk_config
            .as_ref()
            .and_then(|cfg| cfg.access.clone())
            .or_else(|| {
                g.configured_config
                    .as_ref()
                    .and_then(|cfg| cfg.access.clone())
            });
        let merged = crate::client::lifecycle::merge_sdk_config_onto(
            g.configured_config.clone().unwrap_or_default(),
            sdk_config.as_ref(),
        );
        // SDK 托管令牌只走 Core API：签发路径写成绝对地址等在这里就报配置错误，不等到登录。
        if let Some(auth) = merged.auth.as_ref() {
            auth.token_path()?;
        }
        // 探活用部署的信任根（私有 CA）；接入点的钉扎在探活时按接入点叠加。
        let trust = merged.core_tls_config();
        let store = match g.access_store.clone() {
            Some(InjectedAccessStore(store)) => {
                crate::runtime::link::AccessStoreSource::Ready(store)
            }
            None => {
                crate::runtime::link::AccessStoreSource::platform_default(Some(data_root.clone()))
            }
        };
        let access = Arc::new(crate::runtime::link::AccessRuntime::for_platform(
            access_config,
            store,
            trust,
        ));
        if let Some(network) = g.last_network.as_ref() {
            access.set_network(
                network.interface.as_deref(),
                network.fingerprint_hint.as_deref(),
            );
        }
        access.warm_up();
        let previous_update = g.access.as_ref().and_then(|previous| previous.update());
        access.install_update(
            crate::runtime::update::UpdateOptions::new(&merged, data_root.join("update"), &access),
            previous_update,
        );
        if let (Some(update), Some(network)) = (access.update(), g.last_network.as_ref()) {
            update.network_changed(network.available, network.is_metered());
        }
        g.access = Some(access);
        g.data_root = Some(data_root);
        g.sdk_config = sdk_config;
        drop(g);
        self.configure_push_transport().await;
        self.reconcile_push_in_background();
        Ok(())
    }

    /// 绑定 / 读取本客户端 CONNECT 时上报的设备标识。
    ///
    /// **契约**：该值必须与登录时签进 `im_connect_token` 的 `device_id` 一致——网关在两侧
    /// 都非空且不相等时直接拒绝连接（设备绑定校验）。组合入口（如社交 SDK 的
    /// `FlareAppClient`）应在登录**之前**用本方法确定唯一值，再把同一个值同时用于社交登录
    /// 与本连接，避免两处各自取值而漂移。
    ///
    /// - 传 `Some` 非空值：写入配置并回显（须在 [`Self::login`] / 建连之前调用）；
    /// - 传 `None` 或空值：只读，返回当前已配置值；未配置则返回 `None`，届时连接会回退到
    ///   进程级临时标识（跨重启会变，token 绑定与多端互踢不可用，见 `SdkConfig::effective_device_id`）。
    pub async fn bind_device_id(&self, device_id: Option<String>) -> Option<String> {
        let requested = device_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned);
        let mut g = self.inner.write().await;
        if let Some(id) = requested {
            g.sdk_config.get_or_insert_with(Default::default).device_id = Some(id.clone());
            drop(g);
            self.configure_push_transport().await;
            self.reconcile_push_in_background();
            return Some(id);
        }
        g.sdk_config
            .as_ref()
            .and_then(|cfg| cfg.device_id.as_deref())
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
    }

    /// 返回当前配置的 SDK 数据根目录（未传 `dataUrl` 时为 SDK 默认系统数据目录）。
    pub async fn data_root(&self) -> Option<PathBuf> {
        self.inner.read().await.data_root.clone()
    }

    /// 基于数据根目录解析一个子路径并确保父目录存在。
    ///
    /// 常用于上层保存附件、缓存或导出文件。若尚未 `init`，会使用 SDK 默认系统数据目录。
    pub async fn resolve_data_subpath(
        &self,
        relative: impl AsRef<std::path::Path>,
    ) -> Result<PathBuf> {
        let root = self
            .data_root()
            .await
            .unwrap_or_else(crate::shared::util::default_sdk_data_root);
        let p = root.join(relative.as_ref());
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FlareError::localized(
                    ErrorCode::InvalidParameter,
                    format!("resolve_data_subpath create_dir_all failed: {}", e),
                )
            })?;
        }
        Ok(p)
    }

    /// 返回当前生效的 `(environment, sdkConfig)` 快照。
    pub async fn config_snapshot(&self) -> (Option<String>, Option<SdkConfigOverlay>) {
        let g = self.inner.read().await;
        (g.environment.clone(), g.sdk_config.clone())
    }

    /// Runtime health snapshot for diagnostics surfaces and platform SDKs.
    pub async fn runtime_health_snapshot(&self) -> RuntimeHealthSnapshot {
        let g = self.inner.read().await;
        RuntimeHealthSnapshot {
            metrics_enabled: g.metrics.is_enabled(),
            state: g
                .engine
                .as_ref()
                .map(|engine| engine.state())
                .unwrap_or_else(|| self.load_state_snapshot()),
            session_generation: g.session_generation,
            raw_subscriber_dropped_total: EventBus::raw_subscriber_dropped_total(),
            metrics: g.metrics.snapshot(),
        }
    }

    /// 服务端时钟的估计，从长连接下行帧的服务端时间戳得来（见 [`crate::kernel::ServerClock`]）。
    ///
    /// 服务端签发的时刻（截止时间、倒计时的终点、冷却结束的时刻）是服务端时钟上的值：宿主拿它们和
    /// 「本机时间 + `offset_ms`」比，而不是和本机时间比，设备时钟快了慢了都不会提前或推迟到点。
    /// 没有会话、或本次会话还没收到服务端的载荷帧时 `estimated` 为 false、`offset_ms` 为 0；宿主此时
    /// 沿用自己上一次拿到的估计，没有就用本机时钟。
    pub async fn server_clock(&self) -> ServerClockEstimate {
        let g = self.inner.read().await;
        let estimate = session_watchers::server_clock_of(&g).estimate_ms();
        ServerClockEstimate {
            offset_ms: estimate.unwrap_or(0),
            estimated: estimate.is_some(),
        }
    }

    /// prepare 时处理发件箱。
    ///
    /// - 同一用户（包括进程被杀后的冷启动）：原样恢复，链路上线后按原 `client_msg_id` 继续发送——
    ///   与「切换线路」语义一致；只有创建时间早于 [`PENDING_RESUME_MAX_AGE_MS`] 的标为失败、等用户手动重发，
    ///   避免几天前的消息突然被发出。
    /// - 换了用户：清空，全部标为失败并通知界面。
    async fn prepare_outbox(&self, same_user: bool) -> Result<()> {
        if !same_user {
            return self.reset_pending_queue_on_login().await;
        }
        let Some(queue) = self.with_engine_async(|e| e.reliable_queue()).await? else {
            return Ok(());
        };
        let expired = queue
            .expire_stale_pending(PENDING_RESUME_MAX_AGE_MS)
            .await?;
        if !expired.is_empty() {
            tracing::info!(
                expired = expired.len(),
                "pending sends older than the resume window were marked failed"
            );
        }
        Ok(())
    }

    /// 登录专属队列重置：清空历史 pending，并将对应本地消息收敛为 failed。
    ///
    /// 设计目标：
    /// - 登录视为新会话边界，不继承历史待发队列；
    /// - 重连路径不触发该逻辑，仍保留同账号历史待发消息继续发送。
    async fn reset_pending_queue_on_login(&self) -> Result<()> {
        let mut should_publish_failed = true;
        let dropped_client_ids =
            if let Some(queue) = self.with_engine_async(|e| e.reliable_queue()).await? {
                // 由队列 actor 原子处理 in_flight + pending，避免与后台 tick 竞态。
                should_publish_failed = false;
                queue.reset_pending_on_login().await?
            } else {
                // 兜底分支：无可靠队列实现时，沿用仓储清理逻辑。
                let current_user_id = self.current_user_id().await.unwrap_or_default();
                if current_user_id.trim().is_empty() {
                    return Ok(());
                }
                let stores = self.stores_async().await?;
                let Some((pending_reader, pending_writer)) = stores.pending_sends() else {
                    return Ok(());
                };
                let pending_entries = pending_reader.list().await?;
                if pending_entries.is_empty() {
                    return Ok(());
                }
                let pending_ids = pending_entries
                    .iter()
                    .map(|entry| entry.client_msg_id.clone())
                    .collect::<Vec<_>>();
                let mut local_by_id = stores
                    .messages
                    .get_by_client_msg_ids(&pending_ids)
                    .await?
                    .into_iter()
                    .map(|message| (message.client_msg_id.clone(), message))
                    .collect::<HashMap<_, _>>();
                let mut dropped_client_ids = Vec::with_capacity(pending_entries.len());
                let mut failed_messages = Vec::new();
                for entry in pending_entries {
                    let _ = pending_writer.pop(&entry.client_msg_id).await?;
                    if let Some(mut local) = local_by_id.remove(&entry.client_msg_id) {
                        local.server_id = local.client_msg_id.clone();
                        local.local_state = MessageLocalState {
                            sending: false,
                            failed: true,
                            is_local: true,
                            uploading: false,
                            upload_progress: 0,
                            sort_ts: local.local_state.sort_ts,
                        };
                        local.status = MessageStatus::Failed as i32;
                        failed_messages.push(local);
                    }
                    dropped_client_ids.push(entry.client_msg_id);
                }
                if !failed_messages.is_empty() {
                    stores.messages.save_batch(&failed_messages).await?;
                }
                dropped_client_ids
            };

        if dropped_client_ids.is_empty() {
            return Ok(());
        }

        if !should_publish_failed {
            return Ok(());
        }

        let bus = self.bus().await?;
        for client_msg_id in dropped_client_ids {
            bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                client_msg_id,
                reason: PENDING_DROPPED_ON_LOGIN_RESET.to_string(),
                error: crate::domain::SendFailure::local_reason(PENDING_DROPPED_ON_LOGIN_RESET),
            }));
        }
        Ok(())
    }

    async fn repair_local_conversation_identities_on_login(&self, user_id: &str) -> Result<()> {
        let stores = self.stores_async().await?;
        let report =
            LocalIdentityRepairService::new(stores.messages.clone(), stores.conversations.clone())
                .repair_single_chat_identities(user_id)
                .await?;
        if report.has_changes() {
            tracing::info!(
                target: "flare_sdk.identity",
                scanned_conversations = report.scanned_conversations,
                rewritten_conversations = report.rewritten_conversations,
                moved_messages = report.moved_messages,
                "repaired local conversation identities on login"
            );
        }
        Ok(())
    }

    /// 获取底层存储提供者（Message/Conversation/Cursor/PendingSend）。
    ///
    /// 通常用于诊断、调试或高级扩展场景。
    pub fn stores(&self) -> Result<StoreProvider> {
        self.with_engine(|e| e.stores().clone())
    }

    pub async fn stores_async(&self) -> Result<StoreProvider> {
        self.with_engine_async(|e| e.stores().clone()).await
    }

    /// 业务端**推送**用户资料(名称/头像)到本地身份缓存,并刷新受影响会话的展示。
    ///
    /// "读多写少"下身份的写入口:业务同步好友/用户(有变化时)调用本方法把变更写进缓存;
    /// 读路径(消息/会话)直接对缓存批量 join 渲染**当前身份**,无需逐条组装。缓存 miss 时
    /// 消息回退内嵌的发送时快照,再没有就是结构化的「无名」(`sender` / `title`),名字不会是裸 ID。
    /// `alias`(当前用户给这个人的私有标签,如好友备注)一并写入,人名解析链里排第一。
    ///
    /// IM core 不拥有用户身份;名字/头像的数据源由业务端提供(本方法 push,或 `ProfileProvider` pull)。
    pub async fn upsert_user_profiles(
        &self,
        profiles: Vec<crate::domain::UserProfile>,
    ) -> Result<()> {
        let profiles: Vec<crate::domain::UserProfile> = profiles
            .into_iter()
            .filter(|p| !p.user_id.trim().is_empty())
            .collect();
        if profiles.is_empty() {
            return Ok(());
        }
        let stores = self.stores_async().await?;
        let mut affected: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for profile in &profiles {
            for conversation_id in stores.apply_user_profile(profile).await? {
                affected.insert(conversation_id);
            }
        }
        // 通知受影响会话刷新展示;打开中的视图只重读这些人发出的行与以他们命名的会话。
        if let Ok(bus) = self.bus().await {
            for conversation_id in affected {
                bus.publish(SdkEvent::Conversation(
                    crate::kernel::event::ConversationEvent::Updated { conversation_id },
                ));
            }
            bus.publish_name_change(crate::kernel::event::NameChange::users(
                profiles.iter().map(|profile| profile.user_id.as_str()),
            ));
        }
        Ok(())
    }

    /// 业务端推送会话内成员昵称（社交业务里的群昵称）：时间线与会话列表投影人名时，群与频道
    /// 会话里它排在别名之后、资料名之前。空昵称表示清除。受影响会话收到 `ConversationUpdated`；
    /// 打开中的这些会话的时间线只重读这些人发出的行。
    pub async fn upsert_participant_display_snapshots(
        &self,
        snapshots: Vec<crate::domain::ParticipantDisplaySnapshot>,
    ) -> Result<()> {
        let stores = self.stores_async().await?;
        let affected = stores.apply_participant_display(&snapshots).await?;
        if let Ok(bus) = self.bus().await {
            bus.publish_name_change(crate::kernel::event::NameChange::written_member_nicknames(
                &snapshots, &affected,
            ));
            for conversation_id in affected {
                bus.publish(SdkEvent::Conversation(
                    crate::kernel::event::ConversationEvent::Updated { conversation_id },
                ));
            }
        }
        Ok(())
    }
}

const PENDING_DROPPED_ON_LOGIN_RESET: &str = "pending queue dropped during login session reset";

/// 同一用户重新 prepare 时，待发消息最多保留这么久；更早的标为失败、由用户决定是否重发。
pub(crate) const PENDING_RESUME_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;

impl Default for IMClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
