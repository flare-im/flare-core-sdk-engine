//! 接入运行时：`AccessKernel` 的懒装配，以及 IM 与 REST 共用的选路上下文（网络指纹、IM 当前接入点）。
//!
//! 生命周期跟 `IMClient` 走，不跟某一次登录走：健康簿、熔断、UDP 封锁记忆属于这台设备与这个网络，
//! 换账号不该清掉。装配（读 LKG、验签、加载健康簿）是异步 I/O，在 SDK 初始化时于后台开始，
//! 第一次真正需要时再等它完成，不占调用线程。

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use flare_access::kernel::{AccessPointStatus, ManifestUpdate};
use flare_access::probe::{ProbeReport, ProbeTransport, ProbeTrigger, Prober};
use flare_access::select::PinnedAccessPoint;
use flare_access::store::{AcceptError, MemoryAccessStore};
use flare_access::{
    AccessConfig, AccessKernel, AccessStore, AccessWarning, ApId, BootstrapReport, DirectoryChange,
    HttpFetch, KernelOptions, NetPrint, Observation, SelectRequest, Selection, ServiceClass,
    TrustMode,
};
#[cfg(not(target_arch = "wasm32"))]
use flare_core::client::dial::DohCache;
use serde::Serialize;
use tokio::sync::{broadcast, oneshot};

use super::refresher::{
    RecoveringStatus, RefreshCommand, RefreshReportDto, RefreshStatus, Refresher,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::infrastructure::transport::http::doh::DirectoryDoh;
use crate::kernel::event::link_status::{AccessChanged, AccessChanges};
use crate::runtime::update::{UpdateOptions, UpdateRuntime};
use crate::shared::util::now_millis;

/// 接入运行时当前不可用的原因。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessUnavailable {
    /// 初始化配置里没有 `access`：SDK 不知道该连哪里。
    #[error(
        "access is not configured: set sdkConfig.access (development mode: trustMode=development with developmentEndpoints)"
    )]
    NotConfigured,
    /// 配置本身有错（缺信任根、生产模式带了开发端点等），重试没有意义。
    #[error("access configuration rejected: {0}")]
    Config(String),
}

impl AccessUnavailable {
    pub fn to_error(&self) -> crate::FlareError {
        crate::FlareError::localized(
            crate::shared::error::ErrorCode::ConfigurationError,
            self.to_string(),
        )
    }
}

/// 存储从哪里来：原生默认在数据根下开 SQLite（异步打开，所以延后到装配时），其余用注入的实现。
pub enum AccessStoreSource {
    Ready(Arc<dyn AccessStore>),
    #[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
    SqliteAt(PathBuf),
}

impl AccessStoreSource {
    pub fn memory() -> Self {
        AccessStoreSource::Ready(Arc::new(MemoryAccessStore::new()))
    }

    /// 平台默认：原生（开了 SQLite 生命周期）在 `{data_root}/access/access.db`；
    /// 没有数据根或 wasm（宿主没经 `set_access_store` 注入存储）时在内存里。
    pub fn platform_default(data_root: Option<PathBuf>) -> Self {
        #[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
        if let Some(root) = data_root {
            return AccessStoreSource::SqliteAt(root);
        }
        let _ = data_root;
        Self::memory()
    }

    async fn open(self) -> Arc<dyn AccessStore> {
        match self {
            AccessStoreSource::Ready(store) => store,
            #[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
            AccessStoreSource::SqliteAt(root) => {
                match flare_access::store::SqliteAccessStore::open(&root).await {
                    Ok(store) => Arc::new(store),
                    Err(error) => {
                        // 健康簿与 LKG 只是加速与兜底：打不开就在内存里跑，本次进程照常可用。
                        tracing::warn!(%error, root = %root.display(), "open access store failed; using an in-memory store");
                        Arc::new(MemoryAccessStore::new())
                    }
                }
            }
        }
    }
}

/// IM 当前所在的接入点（REST 跟随它，诊断展示它）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentLink {
    pub access_point_id: ApId,
    pub protocol: flare_access::Protocol,
    /// 这条连接的 CONNECT_ACK 带回的诊断字段。
    pub ack: ConnectAckSummary,
}

/// CONNECT_ACK 带回、只用于诊断的字段（§5.7）。`server_time_ms` 另用于时钟校正，不在这里。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectAckSummary {
    /// 处理这条连接的网关实例 ID。
    pub gateway_id: Option<String>,
    /// 服务端报的接入点 ID（网关配了才有）；和客户端选的不一致时 `access_point_mismatch` 为真。
    pub access_point_id: Option<String>,
    /// 服务端已知的最新清单 serial；比本地新时触发一次刷新。
    pub access_serial: Option<u64>,
    /// 网关的心跳间隔（秒）。心跳节奏只由接入策略决定，这里只展示。
    pub heartbeat_interval_hint_sec: Option<u32>,
    pub access_point_mismatch: bool,
}

#[derive(Debug, Default)]
struct NetContext {
    print: NetPrint,
    tags: Vec<String>,
    interface: Option<String>,
}

/// 装配一次后留下的摘要（`access://changed` 与诊断用）。
#[derive(Debug, Clone)]
pub struct LoadedDirectory {
    pub report: BootstrapReport,
}

/// DoH 备用解析：查询与它的缓存。目录装配好才开始查。
#[cfg(not(target_arch = "wasm32"))]
struct RuntimeDoh {
    directory: Arc<DirectoryDoh>,
    cache: Arc<DohCache>,
}

/// 把清单展开出的 IM 入口换成实验台的回环地址（清单的主机名与 443 端口在本机连不到）。
#[cfg(not(target_arch = "wasm32"))]
pub type DialRewrite = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// 实验台注入：假镜像与回环入口。走的是生产模式的全部逻辑，只换掉网络的两端。
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
#[derive(Clone)]
pub struct AccessLab {
    pub http: Arc<dyn HttpFetch>,
    pub dial: DialRewrite,
}

/// 目录变更通道的容量：变更一天也没几次，订阅方（链路状态机）只是偶尔慢一拍。
const CHANGE_CHANNEL_CAPACITY: usize = 16;

pub struct AccessRuntime {
    config: Option<AccessConfig>,
    store_source: Mutex<Option<AccessStoreSource>>,
    options: KernelOptions,
    kernel: tokio::sync::OnceCell<Arc<AccessKernel>>,
    loaded: Mutex<Option<LoadedDirectory>>,
    net: Mutex<NetContext>,
    current: Arc<Mutex<Option<CurrentLink>>>,
    prober: Mutex<Option<Arc<Prober>>>,
    probe_transport: Option<Arc<dyn ProbeTransport>>,
    http_fetch: Option<Arc<dyn HttpFetch>>,
    /// 清单刷新任务（生产模式、有 HTTP 能力时，目录装配完成后启动）。
    refresher: Mutex<Option<Refresher>>,
    /// 采纳新清单时发出：链路状态机按新拓扑选路。
    changes: broadcast::Sender<DirectoryChange>,
    /// 更新中心。放在这里是因为它和接入运行时一样跨会话，链路、HTTP 路由、租户通知手里都有这份运行时。
    update: Mutex<Option<Arc<UpdateRuntime>>>,
    /// 最近一次 CONNECT 被 `UPGRADE_REQUIRED` 拒绝的时刻与服务端带来的最低构建号，只进诊断导出。
    upgrade_refusal: Mutex<Option<(i64, Option<u64>)>>,
    #[cfg(not(target_arch = "wasm32"))]
    dial_rewrite: Option<DialRewrite>,
    /// IM 拨号、REST 与引导拉取共用一份。
    #[cfg(not(target_arch = "wasm32"))]
    doh: Option<RuntimeDoh>,
}

// 浏览器里的 wasm32-unknown-unknown 只有一个线程（SDK 的任务都经 spawn_local 跑在页面线程上），
// 宿主存储与 fetch 句柄天然不是 Send。引擎的公共类型在两端都要求 Send + Sync（原生端真的跨线程），
// 所以只在 wasm 上为接入运行时声明它们：值不可能离开唯一的线程。开了 wasm 线程（atomics）就不再成立，
// 下面的编译期检查会拦住。
#[cfg(target_arch = "wasm32")]
unsafe impl Send for AccessRuntime {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for AccessRuntime {}
#[cfg(all(target_arch = "wasm32", target_feature = "atomics"))]
compile_error!(
    "AccessRuntime assumes single-threaded wasm; revisit its Send/Sync impls before enabling wasm threads"
);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn now_ms() -> i64 {
    now_millis() as i64
}

impl AccessRuntime {
    pub fn new(config: Option<AccessConfig>, store: AccessStoreSource) -> Self {
        Self {
            config,
            store_source: Mutex::new(Some(store)),
            // 只有原生端且编进了 QUIC 才把它列为可用：否则交错竞速每次都先试一个必失败的 QUIC。
            options: KernelOptions {
                quic_supported: cfg!(all(feature = "quic", not(target_arch = "wasm32"))),
            },
            kernel: tokio::sync::OnceCell::new(),
            loaded: Mutex::new(None),
            net: Mutex::new(NetContext::default()),
            current: Arc::new(Mutex::new(None)),
            prober: Mutex::new(None),
            probe_transport: None,
            http_fetch: None,
            refresher: Mutex::new(None),
            changes: broadcast::channel(CHANGE_CHANNEL_CAPACITY).0,
            update: Mutex::new(None),
            upgrade_refusal: Mutex::new(None),
            #[cfg(not(target_arch = "wasm32"))]
            dial_rewrite: None,
            #[cfg(not(target_arch = "wasm32"))]
            doh: None,
        }
    }

    /// 平台默认：带上本平台的探活传输与引导拉取（原生 reqwest、浏览器 fetch）。`trust` 是部署的信任根
    /// （系统根 + 配置的私有 CA），探活与拉取时再叠加接入点的 SPKI 钉扎；浏览器里不用它。
    pub fn for_platform(
        config: Option<AccessConfig>,
        store: AccessStoreSource,
        trust: flare_core::common::config_types::TlsConfig,
    ) -> Self {
        use crate::infrastructure::transport::http::{
            fetch::PlatformHttpFetch, probe::PlatformProbeTransport,
        };
        let runtime = Self::new(config, store);
        #[cfg(not(target_arch = "wasm32"))]
        let (runtime, probe, http) = {
            let lookup = PlatformHttpFetch::new(trust.clone());
            let runtime = runtime.with_doh(Arc::new(DirectoryDoh::new(Arc::new(lookup))));
            let doh = runtime.doh();
            let probe = PlatformProbeTransport::new(trust.clone()).with_doh(doh.clone());
            let http = PlatformHttpFetch::new(trust).with_doh(doh);
            (runtime, probe, http)
        };
        #[cfg(target_arch = "wasm32")]
        let (probe, http) = (
            PlatformProbeTransport::new(trust.clone()),
            PlatformHttpFetch::new(trust),
        );
        runtime
            .with_probe_transport(Arc::new(probe))
            .with_http_fetch(Arc::new(http))
    }

    /// 装上 DoH 备用解析。`directory` 用的 HTTP 能力不能带这份缓存。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_doh(mut self, directory: Arc<DirectoryDoh>) -> Self {
        let cache = DohCache::new(directory.clone());
        self.doh = Some(RuntimeDoh { directory, cache });
        self
    }

    /// 测试：换上假的 DoH 缓存，不用准备清单里的解析器。
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn with_doh_cache(mut self, cache: Arc<DohCache>) -> Self {
        let lookup = crate::infrastructure::transport::http::fetch::PlatformHttpFetch::default();
        self.doh = Some(RuntimeDoh {
            directory: Arc::new(DirectoryDoh::new(Arc::new(lookup))),
            cache,
        });
        self
    }

    /// 测试：这个主机现在查不查 DoH。
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn doh_covers(&self, host: &str) -> bool {
        use flare_core::client::dial::DohLookup;

        self.doh
            .as_ref()
            .is_some_and(|doh| doh.directory.covers(host))
    }

    /// IM 拨号、REST、引导拉取、探活与制品下载共用的 DoH 缓存。没有解析器时它什么也不查。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn doh(&self) -> Option<Arc<DohCache>> {
        self.doh.as_ref().map(|doh| doh.cache.clone())
    }

    /// 验过签的发布清单要下载这些地址：它们的主机也能查 DoH。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn cover_downloads<'a>(&self, urls: impl IntoIterator<Item = &'a String>) {
        if let Some(doh) = &self.doh {
            doh.directory.cover_downloads(urls);
        }
    }

    /// 装一个引导拉取用的 HTTP 能力。
    pub fn with_http_fetch(mut self, http: Arc<dyn HttpFetch>) -> Self {
        self.http_fetch = Some(http);
        self
    }

    /// 实验台：换掉网络的两端（假镜像、回环入口）。
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn with_lab(mut self, lab: AccessLab) -> Self {
        self.http_fetch = Some(lab.http);
        self.dial_rewrite = Some(lab.dial);
        self.doh = None;
        self
    }

    /// 实验台：这份运行时不经过 `init`，更新中心要单独装。
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn install_lab_update(
        self: &Arc<Self>,
        config: &crate::client::config::SdkConfig,
        download_dir: std::path::PathBuf,
    ) {
        self.install_update(UpdateOptions::new(config, download_dir, self), None);
    }

    /// 建连实际拨的地址。
    pub(crate) fn dial_url(&self, url: &str) -> String {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(rewrite) = &self.dial_rewrite {
            return rewrite(url);
        }
        url.to_owned()
    }

    /// 装一个探活传输（恢复切回前确认首选接入点用）。
    pub fn with_probe_transport(mut self, transport: Arc<dyn ProbeTransport>) -> Self {
        self.probe_transport = Some(transport);
        self
    }

    pub fn config(&self) -> Option<&AccessConfig> {
        self.config.as_ref()
    }

    /// 端点来自开发模式配置。装配前就能回答：Kit 的「开发模式」角标从第一帧起就该在。
    pub fn development_mode(&self) -> bool {
        self.config
            .as_ref()
            .is_some_and(|config| config.trust_mode == TrustMode::Development)
    }

    /// 已装配的内核；还没装配完成时为 `None`（不等待）。
    pub fn kernel_now(&self) -> Option<Arc<AccessKernel>> {
        self.kernel.get().cloned()
    }

    /// 装配（只成功一次；失败不缓存，下次调用重试——LKG 可能在此期间被写入）。
    // wasm 上内核与探活器握着浏览器句柄、不是 Send：单线程前提见上面 `AccessRuntime` 的 Send/Sync 说明。
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub async fn kernel(&self) -> Result<Arc<AccessKernel>, AccessUnavailable> {
        let config = self
            .config
            .clone()
            .ok_or(AccessUnavailable::NotConfigured)?;
        self.kernel
            .get_or_try_init(|| async {
                // 先取出再 await：锁守卫不能跨 await（原生端要求 future 可跨线程）。
                let source = lock(&self.store_source).take();
                let store = match source {
                    Some(source) => source.open().await,
                    None => Arc::new(MemoryAccessStore::new()) as Arc<dyn AccessStore>,
                };
                let result =
                    AccessKernel::bootstrap(&config, store.clone(), self.options, now_ms()).await;
                match result {
                    Ok((kernel, report)) => {
                        for warning in &report.warnings {
                            log_warning(warning);
                        }
                        tracing::info!(
                            serial = report.serial,
                            origin = ?report.origin,
                            access_points = kernel.directory().snapshot().access_points().len(),
                            "access directory loaded"
                        );
                        *lock(&self.loaded) = Some(LoadedDirectory { report });
                        if let Some(transport) = self.probe_transport.clone() {
                            *lock(&self.prober) =
                                Some(Arc::new(Prober::new(transport, kernel.health().clone())));
                        }
                        let kernel = Arc::new(kernel);
                        #[cfg(not(target_arch = "wasm32"))]
                        if !kernel.is_development()
                            && let Some(doh) = &self.doh
                        {
                            doh.directory.attach(kernel.clone());
                        }
                        if !kernel.is_development()
                            && let Some(http) = self.http_fetch.clone()
                        {
                            *lock(&self.refresher) = Some(Refresher::spawn(
                                kernel.clone(),
                                http,
                                self.current.clone(),
                                self.changes.clone(),
                            ));
                        }
                        Ok(kernel)
                    }
                    Err(error) => {
                        // 存储已经打开：放回去，下次重试时不用再开一次。
                        *lock(&self.store_source) = Some(AccessStoreSource::Ready(store));
                        Err(AccessUnavailable::Config(error.to_string()))
                    }
                }
            })
            .await
            .cloned()
    }

    /// 后台开始装配（SDK 初始化时调用），调用方不等它。
    pub fn warm_up(self: &Arc<Self>) {
        if self.config.is_none() || self.kernel.initialized() {
            return;
        }
        let runtime = self.clone();
        crate::shared::util::spawn_background(async move {
            if let Err(error) = runtime.kernel().await {
                tracing::warn!(%error, "access warm-up failed; will retry on connect");
            }
        });
    }

    pub fn loaded(&self) -> Option<LoadedDirectory> {
        lock(&self.loaded).clone()
    }

    /// 平台报告的网络（`interface` + `fingerprintHint`）。返回网络指纹是否变了。
    pub fn set_network(&self, interface: Option<&str>, hint: Option<&str>) -> bool {
        let print = NetPrint::from_parts(interface, hint);
        let mut net = lock(&self.net);
        let changed = net.print != print;
        net.print = print;
        net.interface = interface.map(str::to_owned);
        #[cfg(not(target_arch = "wasm32"))]
        if changed && let Some(doh) = &self.doh {
            doh.cache.clear();
        }
        changed
    }

    pub fn set_net_tags(&self, tags: Vec<String>) {
        lock(&self.net).tags = tags;
    }

    pub fn net(&self) -> NetPrint {
        lock(&self.net).print
    }

    fn net_tags(&self) -> Vec<String> {
        lock(&self.net).tags.clone()
    }

    pub fn set_current(&self, current: Option<CurrentLink>) {
        *lock(&self.current) = current;
    }

    pub fn current(&self) -> Option<CurrentLink> {
        lock(&self.current).clone()
    }

    /// 取候选。REST 传 `current = None` 时自动跟随 IM 当前接入点（同 AP 优先，落在同一 Cell）。
    pub fn select(
        &self,
        kernel: &AccessKernel,
        service: ServiceClass,
        current: Option<&str>,
        exclude: &[&str],
    ) -> Selection {
        let tags = self.net_tags();
        let following = self.current().map(|link| link.access_point_id);
        let current = current.or(following.as_deref());
        kernel.selector().select(&SelectRequest {
            service,
            net: self.net(),
            net_tags: &tags,
            current,
            exclude,
            now_ms: now_ms(),
        })
    }

    /// 回写一次结果（IM 与 REST 共用健康簿）。还没装配时丢弃：没有目录就没有接入点可言。
    pub fn record(&self, ap: &str, observation: Observation) {
        if let Some(kernel) = self.kernel_now() {
            let effect = kernel
                .health()
                .record(ap, self.net(), observation, now_ms());
            if let Some(transition) = effect.breaker {
                tracing::info!(
                    access_point = ap,
                    ?transition,
                    "access point breaker changed"
                );
            }
            if let Some(until) = effect.udp_blocked_until_ms {
                tracing::info!(
                    until,
                    "UDP looks blocked on this network; QUIC is skipped until then"
                );
            }
        }
    }

    /// 记一次 QUIC 静默（UDP 封锁证据，不计熔断）。
    pub fn note_quic_silence(&self) {
        if let Some(kernel) = self.kernel_now()
            && let Some(until) = kernel
                .health()
                .note_quic_silence(self.net(), now_ms())
                .udp_blocked_until_ms
        {
            tracing::info!(
                until,
                "UDP looks blocked on this network; QUIC is skipped until then"
            );
        }
    }

    /// 恢复切回判定（§3.7）：只在 IM 已上线时调用。
    pub fn evaluate_failback(
        &self,
        kernel: &AccessKernel,
        current: &str,
        foregrounded: bool,
        last_activity_ms: i64,
        last_switch_ms: i64,
    ) -> flare_access::select::FailbackVerdict {
        let tags = self.net_tags();
        kernel
            .selector()
            .evaluate_failback(&flare_access::select::FailbackRequest {
                net: self.net(),
                net_tags: &tags,
                current,
                foregrounded,
                last_activity_ms,
                last_switch_ms,
                now_ms: now_ms(),
            })
    }

    /// 当前网络下某接入点的健康记录是否不佳（熔断非闭合，或近期失败率过半）：网络切换后据此决定换不换入口。
    pub fn is_unhealthy(&self, ap: &str) -> bool {
        let Some(kernel) = self.kernel_now() else {
            return false;
        };
        let view = kernel.health().view(ap, self.net(), now_ms());
        view.breaker != flare_access::health::BreakerView::Closed || view.failure_ratio >= 0.5
    }

    /// 当前目录的客户端链路参数（没装配时用默认值）。
    pub fn policy(&self) -> flare_access::ClientPolicy {
        self.kernel_now()
            .map(|kernel| kernel.directory().snapshot().policy().clone())
            .unwrap_or_default()
    }

    /// 健康簿落盘（节流；`force` 用于进入后台与退出前）。
    pub async fn flush_health(&self, force: bool) {
        let Some(kernel) = self.kernel_now() else {
            return;
        };
        let now = now_ms();
        if !force && !kernel.health().flush_due(now) {
            return;
        }
        if let Err(error) = kernel.flush_health(now, force).await {
            tracing::debug!(%error, "flush access health failed; will retry");
        }
    }

    /// 探一个接入点（恢复切回前积累连续成功）。没有探活传输或该接入点没有 ping 入口时什么也不做。
    pub async fn probe(&self, ap_id: &str, trigger: ProbeTrigger) -> Option<ProbeReport> {
        let kernel = self.kernel_now()?;
        let prober = lock(&self.prober).clone()?;
        let snapshot = kernel.directory().snapshot();
        let (index, _) = snapshot.access_point(ap_id)?;
        Some(
            prober
                .probe(&snapshot, index, self.net(), trigger, now_ms())
                .await,
        )
    }

    pub fn pin(&self, ap_id: &str) -> Result<PinnedAccessPoint, crate::FlareError> {
        let kernel = self.kernel_now().ok_or_else(|| {
            crate::FlareError::localized(
                crate::shared::error::ErrorCode::ServiceUnavailable,
                "access directory is not loaded yet",
            )
        })?;
        kernel.selector().pin(ap_id, now_ms()).map_err(|error| {
            crate::FlareError::localized(
                crate::shared::error::ErrorCode::InvalidParameter,
                error.to_string(),
            )
        })
    }

    pub fn unpin(&self) {
        if let Some(kernel) = self.kernel_now() {
            kernel.selector().unpin();
        }
    }

    /// 订阅目录变更（采纳新清单、导入恢复链接）。
    pub(crate) fn subscribe_changes(&self) -> broadcast::Receiver<DirectoryChange> {
        self.changes.subscribe()
    }

    fn refresh_command(&self, command: RefreshCommand) {
        if let Some(refresher) = lock(&self.refresher).as_ref() {
            refresher.send(command);
        }
    }

    /// 全部接入点连续失败：开始（`true`）或结束（`false`）恢复循环。
    pub(crate) fn set_recovering(&self, recovering: bool) {
        self.refresh_command(RefreshCommand::Recovering(recovering));
    }

    /// 网络变化、回到前台、用户点重试：恢复期间立即来一轮。
    pub(crate) fn wake_refresh(&self) {
        self.refresh_command(RefreshCommand::Wake);
    }

    /// 设备没有网络：恢复循环暂停。
    pub(crate) fn suspend_refresh(&self) {
        self.refresh_command(RefreshCommand::Suspend);
    }

    /// 装上更新中心并开始计时第一次检查。发布清单与接入清单走同一个 HTTP 能力。
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub(crate) fn install_update(
        self: &Arc<Self>,
        options: UpdateOptions,
        previous: Option<Arc<UpdateRuntime>>,
    ) {
        let update = Arc::new(UpdateRuntime::new(
            Arc::downgrade(self),
            self.http_fetch.clone(),
            options,
        ));
        if let Some(previous) = previous {
            update.adopt_session(&previous);
        }
        *lock(&self.update) = Some(update.clone());
        update.start();
    }

    pub(crate) fn update(&self) -> Option<Arc<UpdateRuntime>> {
        lock(&self.update).clone()
    }

    /// 服务端拒绝了这个版本（网关 `UPGRADE_REQUIRED`、REST 426）：更新中心进 `required` 与只读。
    pub(crate) fn note_upgrade_required(&self, min_build: Option<u64>) {
        *lock(&self.upgrade_refusal) = Some((now_ms(), min_build));
        if let Some(update) = self.update() {
            update.rejected(min_build);
        }
    }

    /// 收到 `flare.release` 通知（已在 `spread_ms` 内随机延迟过）。
    pub(crate) fn notify_release(&self) {
        if let Some(update) = self.update() {
            update.release_notice();
        }
    }

    /// 收到 `flare.access` 通知（已在 `spread_ms` 内随机延迟过）。
    pub fn notify_access_serial(&self, serial: u64) {
        if let Some(refresher) = lock(&self.refresher).as_ref() {
            refresher.notify_serial(serial, 0);
        }
    }

    /// CONNECT_ACK 报的服务端 serial：比本地新就刷新。
    pub(crate) fn observe_ack_serial(&self, serial: u64) {
        let Some(kernel) = self.kernel_now() else {
            return;
        };
        if serial > kernel.directory().snapshot().serial()
            && let Some(refresher) = lock(&self.refresher).as_ref()
        {
            refresher.notify_ack_serial(serial);
        }
    }

    /// `access.refresh`：立即刷新一轮，返回结果。开发模式下端点由配置决定，结果是 `skipped`。
    pub async fn refresh(&self) -> Result<RefreshReportDto, crate::FlareError> {
        let kernel = self.kernel().await.map_err(|error| error.to_error())?;
        let (reply, answer) = oneshot::channel();
        {
            let refresher = lock(&self.refresher);
            let Some(refresher) = refresher.as_ref() else {
                return Ok(RefreshReportDto::skipped(
                    kernel.directory().snapshot().serial(),
                ));
            };
            refresher.send(RefreshCommand::Manual(reply));
        }
        let report = answer.await.map_err(|_| {
            crate::FlareError::localized(
                crate::shared::error::ErrorCode::ServiceUnavailable,
                "the access refresh task stopped",
            )
        })?;
        Ok(RefreshReportDto::new(
            &report,
            kernel.status(now_ms()).keystate_serial,
        ))
    }

    /// `access.import_recovery`：导入恢复链接或二维码内容。链接本身有问题是预期内的结果（用户拿到了
    /// 过期的、被改过的链接），放在返回值里，`code` 是跨端稳定的错误码。
    pub async fn import_recovery(
        &self,
        link: &str,
    ) -> Result<RecoveryImportDto, crate::FlareError> {
        let kernel = self.kernel().await.map_err(|error| error.to_error())?;
        let result = kernel.accept_recovery(link, now_ms()).await;
        let serial = kernel.directory().snapshot().serial();
        let (outcome, code) = match result {
            Ok(ManifestUpdate::Adopted { change, .. }) => {
                tracing::info!(serial = change.to_serial, "recovery link imported");
                let _ = self.changes.send(change);
                // 恢复清单只是临时目录：有网就换回完整清单。
                self.refresh_command(RefreshCommand::Soon);
                ("adopted", None)
            }
            Ok(ManifestUpdate::Unchanged { .. }) => ("unchanged", None),
            Err(AcceptError::Trust(error)) => {
                tracing::warn!(%error, "recovery link rejected");
                ("rejected", Some(error.code()))
            }
            Err(AcceptError::DevelopmentMode) => ("rejected", Some("developmentMode".to_owned())),
            Err(error) => ("rejected", Some(error.to_string())),
        };
        Ok(RecoveryImportDto {
            outcome: outcome.to_owned(),
            code,
            serial,
        })
    }

    fn refresh_status(&self) -> (Option<RefreshStatus>, Option<RecoveringStatus>) {
        match lock(&self.refresher).as_ref() {
            Some(refresher) => (
                Some(refresher.refresh_status()),
                refresher.recovering_status(),
            ),
            None => (None, None),
        }
    }

    /// `access.get_status`。
    pub fn status(&self) -> AccessStatusDto {
        let current = self.current();
        let Some(kernel) = self.kernel_now() else {
            return AccessStatusDto {
                loaded: false,
                serial: 0,
                source: None,
                stale: false,
                development_mode: self.development_mode(),
                issued_at_ms: None,
                expires_at_ms: None,
                fetched_at_ms: None,
                region_id: None,
                cell_id: None,
                keystate_serial: None,
                access_point_id: current
                    .as_ref()
                    .map(|link| link.access_point_id.to_string()),
                protocol: current
                    .as_ref()
                    .map(|link| link.protocol.as_str().to_owned()),
                pinned: None,
                connect_ack: current.map(|link| link.ack),
                refresh: None,
                recovering: None,
            };
        };
        let now = now_ms();
        let status = kernel.status(now);
        let (refresh, recovering) = self.refresh_status();
        // 空目录时这些字段没有意义，不要拿 0 和空串冒充。
        let has_manifest = status.has_manifest;
        AccessStatusDto {
            loaded: has_manifest,
            serial: status.serial,
            source: Some(status.source),
            stale: status.stale,
            development_mode: status.development,
            issued_at_ms: has_manifest.then_some(status.issued_at_ms),
            expires_at_ms: has_manifest.then_some(status.expires_at_ms),
            fetched_at_ms: status.fetched_at_ms,
            region_id: has_manifest.then_some(status.region_id),
            cell_id: has_manifest.then_some(status.cell_id),
            keystate_serial: status.keystate_serial,
            access_point_id: current
                .as_ref()
                .map(|link| link.access_point_id.to_string()),
            protocol: current
                .as_ref()
                .map(|link| link.protocol.as_str().to_owned()),
            connect_ack: current.map(|link| link.ack),
            pinned: kernel.selector().pinned(now).map(|pin| PinnedDto {
                access_point_id: pin.ap_id.to_string(),
                expires_at_ms: pin.expires_at_ms,
            }),
            refresh,
            recovering,
        }
    }

    /// `access.list_access_points`。
    pub fn list_access_points(&self) -> Vec<AccessPointDto> {
        let Some(kernel) = self.kernel_now() else {
            return Vec::new();
        };
        let current = self.current().map(|link| link.access_point_id);
        kernel
            .list_access_points(self.net(), now_ms())
            .into_iter()
            .map(|status| AccessPointDto::from_status(status, current.as_deref()))
            .collect()
    }

    /// `access.export_diagnostics`：只有接入与健康信息，不含令牌、用户与消息内容。
    pub fn export_diagnostics(&self) -> serde_json::Value {
        let loaded = self.loaded();
        let interface = lock(&self.net).interface.clone();
        // 指纹只是本地哈希，不可逆；导出便于对照同一网络的健康记录。
        let fingerprint = self.net().to_hex();
        serde_json::json!({
            "generatedAtMs": now_ms(),
            "sdkVersion": env!("CARGO_PKG_VERSION"),
            "network": {
                "interface": interface,
                "fingerprint": fingerprint,
            },
            "status": self.status(),
            "accessPoints": self.list_access_points(),
            "warnings": self.diagnostic_warnings(loaded.as_ref()),
            "udpBlockedUntilMs": self.kernel_now().and_then(|kernel| {
                kernel.health().udp_blocked_until(self.net(), now_ms())
            }),
            // 网关在 CONNECT 拒绝里带来的最低构建号：分辨「被网关拒」与「只从发布清单得知要升级」。
            "upgradeRefusal": lock(&self.upgrade_refusal).map(|(at_ms, min_build)| {
                serde_json::json!({ "atMs": at_ms, "minBuild": min_build })
            }),
            "bootstrap": self.kernel_now().map(|kernel| {
                let snapshot = kernel.directory().snapshot();
                serde_json::json!({
                    "bootstrapId": kernel.bootstrap_id(),
                    "sources": snapshot.manifest().bootstrap.sources.len(),
                    "seedSources": kernel.seed_sources().len(),
                })
            }),
        })
    }

    /// 装配时的告警，加上当前连接上服务端报的接入点与客户端选的不一致。
    fn diagnostic_warnings(&self, loaded: Option<&LoadedDirectory>) -> serde_json::Value {
        let mut warnings = loaded
            .map(|loaded| serde_json::to_value(&loaded.report.warnings).unwrap_or_default())
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        if let Some(link) = self.current()
            && link.ack.access_point_mismatch
        {
            warnings.push(serde_json::json!({
                "kind": "accessPointMismatch",
                "expected": link.access_point_id.to_string(),
                "reported": link.ack.access_point_id,
            }));
        }
        serde_json::Value::Array(warnings)
    }

    /// `access://changed` 的载荷：采纳了一份新清单。
    pub fn change_payload(&self, change: &DirectoryChange) -> Option<AccessChanged> {
        let kernel = self.kernel_now()?;
        let status = kernel.status(now_ms());
        Some(AccessChanged {
            serial: change.to_serial,
            source: status.source,
            stale: status.stale,
            development_mode: status.development,
            changes: AccessChanges {
                access_points_added: change.added.iter().map(ToString::to_string).collect(),
                access_points_removed: change.removed.iter().map(ToString::to_string).collect(),
                policy_changed: change.policy_changed,
            },
        })
    }

    /// `access://changed` 的载荷（会话开始时宣告当前目录）。
    pub fn changed_payload(&self) -> Option<AccessChanged> {
        let kernel = self.kernel_now()?;
        let snapshot = kernel.directory().snapshot();
        let status = kernel.status(now_ms());
        Some(AccessChanged {
            serial: status.serial,
            source: status.source,
            stale: status.stale,
            development_mode: status.development,
            changes: AccessChanges {
                access_points_added: snapshot
                    .access_points()
                    .iter()
                    .map(|entry| entry.id.to_string())
                    .collect(),
                access_points_removed: Vec::new(),
                policy_changed: true,
            },
        })
    }
}

fn log_warning(warning: &AccessWarning) {
    match warning {
        AccessWarning::DevelopmentMode => tracing::warn!(
            "access is in DEVELOPMENT mode: endpoints come from configuration, unsigned; never ship this build"
        ),
        other => tracing::warn!(warning = ?other, "access warning"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedDto {
    pub access_point_id: String,
    pub expires_at_ms: i64,
}

/// `access.get_status` 的返回值。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessStatusDto {
    /// 目录里是否有可用清单（`false` 时没有接入点可连，等引导拉取或恢复链接）。
    pub loaded: bool,
    pub serial: u64,
    /// `seed` / `https:<镜像基址>` / `wellKnown:<apId>` / `recovery` / `development` / `none`（目录为空）。
    pub source: Option<String>,
    pub stale: bool,
    pub development_mode: bool,
    /// 清单签发时刻与过期时刻（Unix 毫秒）；`stale` 即已过 `expiresAtMs`。
    pub issued_at_ms: Option<i64>,
    pub expires_at_ms: Option<i64>,
    /// 清单到达本机的时刻；种子与开发模式为 null。
    pub fetched_at_ms: Option<i64>,
    pub region_id: Option<String>,
    pub cell_id: Option<String>,
    pub keystate_serial: Option<u64>,
    /// IM 当前接入点与协议。
    pub access_point_id: Option<String>,
    pub protocol: Option<String>,
    pub pinned: Option<PinnedDto>,
    /// 当前连接的 CONNECT_ACK 诊断字段（网关 ID、服务端报的接入点、清单 serial、网关心跳间隔）；没有连接时为 null。
    pub connect_ack: Option<ConnectAckSummary>,
    /// 清单刷新的进度；开发模式为 null。
    pub refresh: Option<RefreshStatus>,
    /// 全部接入点失败、正在恢复时才有。
    pub recovering: Option<RecoveringStatus>,
}

/// `access.import_recovery` 的返回值。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryImportDto {
    /// `adopted` / `unchanged` / `rejected`。
    pub outcome: String,
    /// `rejected` 时的错误码（`badSignature`、`rollback`、`wrongBootstrapId`、`tooLarge`、
    /// `malformedEnvelope`、`invalidPayload`、`unknownKey`、`developmentMode` 等）。
    pub code: Option<String>,
    /// 导入之后的目录 serial。
    pub serial: u64,
}

/// `access.list_access_points` 的一行。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessPointDto {
    pub access_point_id: String,
    pub region_id: String,
    pub host: String,
    /// `active` / `draining` / `disabled` / `unknown`。
    pub status: String,
    pub rtt_ms: Option<f64>,
    /// `closed` / `open` / `halfOpen`。
    pub breaker: String,
    pub protocol_winner: Option<String>,
    pub pinned: bool,
    pub current: bool,
}

impl AccessPointDto {
    fn from_status(status: AccessPointStatus, current: Option<&str>) -> Self {
        Self {
            current: current.is_some_and(|current| current == &*status.ap_id),
            access_point_id: status.ap_id.to_string(),
            region_id: status.region_id,
            host: status.host,
            status: status.status.as_str().to_owned(),
            rtt_ms: status.rtt_ms,
            breaker: status.breaker.as_str().to_owned(),
            protocol_winner: status
                .protocol_winner
                .map(|protocol| protocol.as_str().to_owned()),
            pinned: status.pinned,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_access::{DevelopmentAccessPoint, DevelopmentEndpoints};

    fn development() -> AccessConfig {
        AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051/ws".into()),
            core_api_url: Some("http://127.0.0.1:60061".into()),
            access_points: vec![DevelopmentAccessPoint {
                id: "ap-b".into(),
                ws_url: Some("ws://127.0.0.1:61051/ws".into()),
                core_api_url: Some("http://127.0.0.1:61061".into()),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn development_directory_loads_lazily_and_rest_follows_the_im_access_point() {
        let runtime = AccessRuntime::new(Some(development()), AccessStoreSource::memory());
        assert!(runtime.development_mode());
        assert!(runtime.kernel_now().is_none(), "装配是懒的");
        let kernel = runtime.kernel().await.expect("development directory");
        assert_eq!(runtime.status().access_point_id, None);
        assert!(runtime.status().development_mode);

        runtime.set_current(Some(CurrentLink {
            access_point_id: "ap-b".into(),
            protocol: flare_access::Protocol::Ws,
            ack: ConnectAckSummary::default(),
        }));
        let selection = runtime.select(&kernel, ServiceClass::CoreApi, None, &[]);
        assert_eq!(
            &*selection.candidates[0].ap_id, "ap-b",
            "REST 跟随 IM 当前接入点"
        );
        assert_eq!(
            selection.http_base(&selection.candidates[0], ServiceClass::CoreApi),
            Some("http://127.0.0.1:61061")
        );

        let changed = runtime.changed_payload().unwrap();
        assert_eq!(
            changed.changes.access_points_added,
            vec!["development".to_owned(), "ap-b".to_owned()]
        );
        let listed = runtime.list_access_points();
        assert!(
            listed
                .iter()
                .any(|ap| ap.access_point_id == "ap-b" && ap.current)
        );
        let diagnostics = runtime.export_diagnostics().to_string();
        assert!(!diagnostics.contains("token"));
    }

    #[test]
    fn an_upgrade_refusal_is_exported_with_its_minimum_build() {
        let runtime = AccessRuntime::new(None, AccessStoreSource::memory());
        assert!(runtime.export_diagnostics()["upgradeRefusal"].is_null());
        runtime.note_upgrade_required(Some(20302));
        let refusal = &runtime.export_diagnostics()["upgradeRefusal"];
        assert_eq!(refusal["minBuild"], 20302);
        assert!(refusal["atMs"].as_i64().unwrap() > 0);
        runtime.note_upgrade_required(None);
        assert!(runtime.export_diagnostics()["upgradeRefusal"]["minBuild"].is_null());
    }

    #[tokio::test]
    async fn missing_configuration_is_not_retryable() {
        let runtime = AccessRuntime::new(None, AccessStoreSource::memory());
        let Err(error) = runtime.kernel().await else {
            panic!("no access configuration must not load a directory");
        };
        assert_eq!(error, AccessUnavailable::NotConfigured);
        assert!(!runtime.development_mode());
    }
}
