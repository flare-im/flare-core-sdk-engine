//! 更新运行时：把 `flare_access::release` 的纯状态机接到时钟、存储、网络和事件上。
//!
//! 生命周期跟接入运行时走，不跟某次登录走：登录页上就要能查更新、下载更新，IM 节点全部不可达时也一样，
//! 发布清单走接入清单那组镜像。事件总线是按会话的，所以这里先发到自己的广播通道，
//! 有会话时由监视器转成 `update://state`；没有会话时宿主用 `update.get_state` 取。
//!
//! 资源包跟着发布清单走：每次评估之后在后台同步，这一轮下到的和撤回的下次启动才生效。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use flare_access::release::{
    CheckResult, InstallOutcome, InstallStep, PersistedUpdate, ReleaseFetchOutcome, ReleaseFetcher,
    STARTUP_CHECK_DELAY_MS, UpdateApp, UpdateInstallPlan, UpdateMachine, UpdatePrompt,
    UpdateResourcePack, UpdateResources, UpdateStatus, codes, load_packs, sync_packs,
};
use flare_access::store::{install_id, keys};
use flare_access::{AccessKernel, AccessStore, HttpFetch};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::oneshot;
use tokio::sync::{OnceCell, broadcast};

use crate::client::config::SdkConfig;
use crate::runtime::link::AccessRuntime;
use crate::runtime::link::access::now_ms;
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::{delay, spawn_background};

/// 状态变化的广播容量：订阅方只关心最新值，落后了跳过即可。
const STATE_CHANNEL_CAPACITY: usize = 16;
/// 没有配置渠道时用的发布渠道。
const DEFAULT_CHANNEL: &str = "stable";

/// 这一轮检查为什么发生。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckTrigger {
    /// 启动、回前台、恢复联网、`update.check`：受频率限制。手动的在状态机里记着，不受限。
    Scheduled,
    /// 收到 `flare.release` 通知或被服务端拒绝：不受频率限制，但不打扰界面。
    Pushed,
}

/// 发布清单路径里的平台段。浏览器是 `web`；桌面是 Tauri 或 Electron 外壳（§8.10）。
fn release_platform(distribution: Option<&str>) -> &'static str {
    if cfg!(target_arch = "wasm32") {
        return "web";
    }
    match std::env::consts::OS {
        "android" => "android",
        "ios" => "ios",
        _ if distribution == Some("electron") => "electron",
        _ => "tauri",
    }
}

/// 更新中心需要的本机信息。
pub struct UpdateOptions {
    pub app: UpdateApp,
    /// 安装包放哪里（应用私有目录）。
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    pub download_dir: PathBuf,
    #[cfg(not(target_arch = "wasm32"))]
    pub artifacts: Arc<dyn flare_access::download::ArtifactTransport>,
}

impl UpdateOptions {
    /// 制品下载和 `access` 共用 DoH 缓存。
    pub fn new(
        config: &SdkConfig,
        download_dir: PathBuf,
        #[cfg_attr(target_arch = "wasm32", allow(unused_variables))] access: &AccessRuntime,
    ) -> Self {
        let distribution = config.effective_distribution();
        Self {
            app: UpdateApp {
                platform: release_platform(distribution.as_deref()).to_owned(),
                channel: config
                    .effective_update_channel()
                    .unwrap_or_else(|| DEFAULT_CHANNEL.to_owned()),
                version: config.effective_app_version(),
                build: config.app.build,
                build_id: config.effective_build_id(),
                distribution,
                target: config.effective_update_target(),
                locale: config.effective_locale(),
            },
            download_dir,
            #[cfg(not(target_arch = "wasm32"))]
            artifacts: Arc::new(
                crate::infrastructure::transport::http::artifact::PlatformArtifactTransport::new(
                    config.core_tls_config(),
                )
                .with_doh(access.doh()),
            ),
        }
    }
}

pub struct UpdateRuntime {
    access: Weak<AccessRuntime>,
    http: Option<Arc<dyn HttpFetch>>,
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    download_dir: PathBuf,
    #[cfg(not(target_arch = "wasm32"))]
    artifacts: Arc<dyn flare_access::download::ArtifactTransport>,
    machine: Mutex<UpdateMachine>,
    states: broadcast::Sender<UpdateStatus>,
    /// 本地记录读进状态机一次。
    loaded: tokio::sync::OnceCell<()>,
    /// 同一时刻只跑一轮检查。
    checking: tokio::sync::Mutex<()>,
    metered: AtomicBool,
    /// 本次运行的资源包，读定之后不变。重新 `init` 时接过上一个运行时的。
    resources: Mutex<Arc<OnceCell<Vec<UpdateResourcePack>>>>,
    /// 同一时刻只跑一轮资源包同步。
    syncing: tokio::sync::Mutex<()>,
}

// 与 `AccessRuntime` 同样的前提：浏览器里只有一个线程，宿主 fetch 句柄不是 Send。
#[cfg(target_arch = "wasm32")]
unsafe impl Send for UpdateRuntime {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for UpdateRuntime {}

impl UpdateRuntime {
    pub(crate) fn new(
        access: Weak<AccessRuntime>,
        http: Option<Arc<dyn HttpFetch>>,
        options: UpdateOptions,
    ) -> Self {
        Self {
            access,
            http,
            download_dir: options.download_dir,
            #[cfg(not(target_arch = "wasm32"))]
            artifacts: options.artifacts,
            machine: Mutex::new(UpdateMachine::new(options.app)),
            states: broadcast::channel(STATE_CHANNEL_CAPACITY).0,
            loaded: tokio::sync::OnceCell::new(),
            checking: tokio::sync::Mutex::new(()),
            metered: AtomicBool::new(false),
            resources: Mutex::new(Arc::new(OnceCell::new())),
            syncing: tokio::sync::Mutex::new(()),
        }
    }

    fn machine(&self) -> MutexGuard<'_, UpdateMachine> {
        self.machine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 订阅状态变化。
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<UpdateStatus> {
        self.states.subscribe()
    }

    /// 当前状态，不等加载，也不发网络请求。
    pub(crate) fn status_now(&self) -> UpdateStatus {
        self.machine().status(now_ms())
    }

    fn publish(&self) -> UpdateStatus {
        let status = self.status_now();
        let _ = self.states.send(status.clone());
        status
    }

    /// 内核与本地记录就绪。没有接入配置时为 `None`：状态机留在 `idle`。
    async fn ready(&self) -> Option<Arc<AccessKernel>> {
        let access = self.access.upgrade()?;
        let kernel = access.kernel().await.ok()?;
        self.loaded.get_or_init(|| self.load(&kernel)).await;
        Some(kernel)
    }

    async fn load(&self, kernel: &Arc<AccessKernel>) {
        let id = install_id(&**kernel.store()).await;
        let persisted: PersistedUpdate = match kernel
            .store()
            .get(&keys::update_state(kernel.bootstrap_id()))
            .await
        {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap_or_default(),
            _ => PersistedUpdate::default(),
        };
        let now = now_ms();
        let app = self.machine().app().clone();
        let manifest = match kernel.release_store(&app.platform, &app.channel) {
            Some(store) => kernel
                .load_release(&store, &app.platform, &app.channel, now)
                .await
                .map(|(verified, _)| verified),
            None => None,
        };
        self.machine().restore(id, persisted, manifest, now);
        self.remove_discarded().await;
    }

    async fn persist(&self, kernel: &AccessKernel) {
        let persisted = self.machine().persisted();
        let Ok(bytes) = serde_json::to_vec(&persisted) else {
            return;
        };
        let key = keys::update_state(kernel.bootstrap_id());
        if let Err(error) = kernel.store().put(&key, &bytes).await {
            tracing::debug!(%error, "persist update state failed; will retry on the next change");
        }
    }

    /// 删掉不再需要的安装包。
    async fn remove_discarded(&self) {
        let discarded = self.machine().take_discarded();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(record) = discarded {
            let _ = tokio::fs::remove_file(&record.path).await;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = discarded;
    }

    /// 接过上一个运行时本次运行的提示记录、宿主选的语言和资源包快照。
    pub(crate) fn adopt_session(&self, previous: &UpdateRuntime) {
        *self.resource_cell_slot() = previous.resource_cell();
        let previous = previous.machine();
        self.machine().adopt_session(&previous);
    }

    /// 启动时读定资源包，然后是第一次检查。没有首帧信号，就从 SDK 初始化完成开始计时。
    pub(crate) fn start(self: &Arc<Self>) {
        self.machine().set_startup_check_pending(true);
        let runtime = self.clone();
        spawn_background(async move {
            runtime.resources().await;
        });
        let runtime = self.clone();
        spawn_background(async move {
            delay(Duration::from_millis(STARTUP_CHECK_DELAY_MS)).await;
            runtime.run_background_check(CheckTrigger::Scheduled).await;
            runtime.machine().set_startup_check_pending(false);
            runtime.publish();
        });
    }

    fn resource_cell_slot(&self) -> MutexGuard<'_, Arc<OnceCell<Vec<UpdateResourcePack>>>> {
        self.resources
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn resource_cell(&self) -> Arc<OnceCell<Vec<UpdateResourcePack>>> {
        self.resource_cell_slot().clone()
    }

    /// 读本地的包。没有内核时报错，不留结果：内核晚一点就绪，下次还要读。
    async fn read_resources(&self) -> std::result::Result<Vec<UpdateResourcePack>, ()> {
        let kernel = self.ready().await.ok_or(())?;
        let build = self.machine().app().build;
        match load_packs(&**kernel.store(), kernel.bootstrap_id(), build).await {
            Ok(packs) => Ok(packs.into_iter().map(UpdateResourcePack::from).collect()),
            Err(error) => {
                tracing::warn!(%error, "reading resource packs failed; this run goes without them");
                Ok(Vec::new())
            }
        }
    }

    /// `update.get_resources`：本次运行的资源包，等启动读取完成才返回。
    pub(crate) async fn resources(&self) -> UpdateResources {
        let cell = self.resource_cell();
        let packs = cell.get_or_try_init(|| self.read_resources()).await;
        UpdateResources {
            packs: packs.cloned().unwrap_or_default(),
        }
    }

    /// 发布清单评估之后同步资源包。放到后台：`update.*` 的返回和状态都不等它。
    fn spawn_resource_sync(self: &Arc<Self>, kernel: Arc<AccessKernel>) {
        let Some(http) = self.http.clone() else {
            return;
        };
        let runtime = self.clone();
        spawn_background(async move {
            runtime.sync_resources(kernel, http).await;
        });
    }

    async fn sync_resources(&self, kernel: Arc<AccessKernel>, http: Arc<dyn HttpFetch>) {
        // 快照要先读定：这一轮写进去的东西下次启动才算数。
        self.resources().await;
        // 同一时刻只跑一轮，后来的接着跑，用的是最新的清单。
        let _round = self.syncing.lock().await;
        let Some((verified, input)) = self.machine().pack_source() else {
            return;
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(access) = self.access.upgrade() {
            let releases = verified.manifest.releases.iter();
            let packs = releases.flat_map(|release| &release.resource_packs);
            access.cover_downloads(packs.flat_map(|pack| &pack.url));
        }
        let metered = self.metered.load(Ordering::Relaxed);
        let report = sync_packs(
            &**kernel.store(),
            &*http,
            kernel.bootstrap_id(),
            &verified.manifest,
            &input,
            metered,
        )
        .await;
        match report {
            Ok(report) => {
                for (id, code) in &report.failed {
                    tracing::warn!(%id, %code, "resource pack not updated; the next check retries");
                }
                tracing::debug!(
                    fetched = report.fetched.len(),
                    removed = report.removed.len(),
                    postponed = report.postponed.len(),
                    "resource packs synced"
                );
            }
            Err(error) => tracing::warn!(%error, "resource pack sync failed"),
        }
    }

    fn spawn_check(self: &Arc<Self>, trigger: CheckTrigger) {
        let runtime = self.clone();
        spawn_background(async move {
            runtime.run_background_check(trigger).await;
        });
    }

    async fn run_background_check(self: &Arc<Self>, trigger: CheckTrigger) -> UpdateStatus {
        self.machine().begin_background_check();
        self.run_check(trigger).await
    }

    /// 跑一轮检查。这一轮已经在状态机里记过，所有出口都要把它收掉，否则状态停在 `checking`。
    async fn run_check(self: &Arc<Self>, trigger: CheckTrigger) -> UpdateStatus {
        let kernel = self.ready().await;
        let can_check = self.machine().app().can_check();
        let (Some(kernel), Some(http), true) = (kernel, self.http.clone(), can_check) else {
            self.machine().check_unavailable();
            return self.publish();
        };
        // 同一时刻只跑一轮，后来的等它结束：结果通常已经是想要的。
        let _round = self.checking.lock().await;
        if trigger == CheckTrigger::Scheduled && !self.machine().check_still_due(now_ms()) {
            return self.publish();
        }
        let app = self.machine().app().clone();
        let fetcher = ReleaseFetcher::new(kernel.clone(), http, app.platform, app.channel);
        let report = fetcher.fetch(now_ms()).await;
        let result = match (report.outcome, report.manifest) {
            (ReleaseFetchOutcome::Skipped, _) => CheckResult::Skipped,
            (ReleaseFetchOutcome::NotPublished, _) => CheckResult::NotPublished,
            (ReleaseFetchOutcome::Rejected { code }, _) => {
                tracing::warn!(%code, "release manifest rejected by every source");
                CheckResult::Failed {
                    code: codes::MANIFEST_REJECTED,
                }
            }
            (ReleaseFetchOutcome::Unavailable { code }, _) => {
                tracing::debug!(?code, "release manifest unavailable");
                CheckResult::Failed {
                    code: codes::MANIFEST_UNAVAILABLE,
                }
            }
            (_, Some(manifest)) => CheckResult::Manifest(manifest),
            (_, None) => CheckResult::NotPublished,
        };
        let evaluated = matches!(result, CheckResult::Manifest(_));
        self.machine().finish_check(result, now_ms());
        self.persist(&kernel).await;
        self.remove_discarded().await;
        if evaluated {
            self.spawn_resource_sync(kernel);
        }
        self.publish()
    }

    /// `update.get_state`。`foreground` 时先按回前台处理：控制器一回前台就读状态，
    /// `lifecycle.set_app_state` 可能还没到。
    pub(crate) async fn state(self: &Arc<Self>, foreground: bool) -> UpdateStatus {
        self.ready().await;
        if foreground {
            self.foregrounded();
        }
        self.status_now()
    }

    /// `update.set_locale`：更新说明和封禁原因马上按新语言选。
    pub(crate) async fn set_locale(&self, locale: String) -> UpdateStatus {
        self.ready().await;
        self.machine().set_locale(locale);
        self.publish()
    }

    /// `update.check`：不等这一轮。Web 的绑定层同一时刻只跑一个调用，这一轮最长约一分钟，等它就把
    /// 发消息全堵住。要查就先进 `checking` 再放到后台，宿主对 `checking` 轮询 `update.get_state`。
    /// 不用查时返回当前状态；已有一轮在跑时并进去，不重复起。
    pub(crate) fn request_check(self: &Arc<Self>, manual: bool) -> UpdateStatus {
        let start = self.machine().request_check(manual, now_ms());
        let status = self.publish();
        if start {
            let runtime = self.clone();
            spawn_background(async move {
                runtime.run_check(CheckTrigger::Scheduled).await;
            });
        }
        status
    }

    /// 等这一轮结束再返回。路由走 [`Self::request_check`]，测试要看结果。
    #[cfg(test)]
    pub(crate) async fn check(self: &Arc<Self>, manual: bool) -> UpdateStatus {
        if !self.machine().request_check(manual, now_ms()) {
            return self.status_now();
        }
        self.run_check(CheckTrigger::Scheduled).await
    }

    /// `update.download`：等下载结束才返回，进度经 `update://state` 与 `update.get_state` 取。
    /// 下载在独立任务里跑：调用方的等待被取消（退出登录）时下载照常进行。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn download(self: &Arc<Self>, allow_metered: bool) -> UpdateStatus {
        let Some(kernel) = self.ready().await else {
            return self.status_now();
        };
        let metered = self.metered.load(Ordering::Relaxed);
        let spec = self.machine().begin_download(metered, allow_metered);
        let Some(spec) = spec else {
            return self.publish();
        };
        self.publish();
        let (done, finished) = oneshot::channel();
        let runtime = self.clone();
        spawn_background(async move {
            let _ = done.send(runtime.run_download(kernel, spec).await);
        });
        finished.await.unwrap_or_else(|_| self.status_now())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn run_download(
        self: Arc<Self>,
        kernel: Arc<AccessKernel>,
        spec: flare_access::release::DownloadSpec,
    ) -> UpdateStatus {
        use flare_access::download::{DownloadError, download};

        if let Some(access) = self.access.upgrade() {
            access.cover_downloads(&spec.urls);
        }
        let mut observer = Observer {
            runtime: self.clone(),
        };
        let result = download(
            self.artifacts.as_ref(),
            &spec,
            &self.download_dir,
            &now_ms,
            &mut observer,
        )
        .await;
        match result {
            Ok(artifact) => self.machine().download_ready(artifact.record(), now_ms()),
            Err(error) => {
                tracing::warn!(%error, "update download failed");
                // 写盘失败、空间不足都可能恢复，由用户决定要不要再试。
                let retryable = !matches!(error, DownloadError::Io(_));
                self.machine().download_failed(error.code(), retryable);
            }
        }
        self.persist(&kernel).await;
        self.remove_discarded().await;
        self.publish()
    }

    /// 浏览器没有安装包可下：Web 走 Service Worker。
    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn download(self: &Arc<Self>, _allow_metered: bool) -> UpdateStatus {
        self.ready().await;
        self.machine().begin_download(false, false);
        self.publish()
    }

    /// `update.install`。计划给不出时抛错，错误文本是 `update.*` 错误码。
    pub(crate) async fn install(&self) -> Result<UpdateInstallPlan> {
        let kernel = self.ready().await;
        let step = self.machine().begin_install();
        let plan = match step {
            Some(InstallStep::Plan(plan)) => Some(plan),
            #[cfg(not(target_arch = "wasm32"))]
            Some(InstallStep::Package(record)) => {
                match flare_access::download::verify_existing(&record).await {
                    Ok(artifact) => {
                        self.machine().package_handed_over();
                        Some(artifact.install_plan())
                    }
                    Err(error) => {
                        tracing::warn!(%error, "downloaded package failed its re-check");
                        self.machine().package_rejected();
                        None
                    }
                }
            }
            #[cfg(target_arch = "wasm32")]
            Some(InstallStep::Package(_)) => None,
            None => None,
        };
        if let Some(kernel) = &kernel {
            self.persist(kernel).await;
        }
        self.remove_discarded().await;
        let status = self.publish();
        plan.ok_or_else(|| {
            let code = status
                .error
                .map_or_else(|| codes::NO_ARTIFACT.to_owned(), |error| error.code);
            FlareError::localized(ErrorCode::InvalidParameter, code)
        })
    }

    /// `update.report_install`。
    pub(crate) async fn report_install(
        &self,
        outcome: &str,
        code: Option<&str>,
    ) -> Result<UpdateStatus> {
        let outcome = InstallOutcome::parse(outcome).ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                format!("unknown install outcome {outcome:?}"),
            )
        })?;
        let kernel = self.ready().await;
        self.machine().report_install(outcome, code);
        if let Some(kernel) = &kernel {
            self.persist(kernel).await;
        }
        self.remove_discarded().await;
        Ok(self.publish())
    }

    /// `update.defer`。
    pub(crate) async fn defer(&self) -> UpdateStatus {
        let kernel = self.ready().await;
        self.machine().defer(now_ms());
        if let Some(kernel) = &kernel {
            self.persist(kernel).await;
        }
        self.publish()
    }

    /// `update.skip`。
    pub(crate) async fn skip(&self, build: u64) -> UpdateStatus {
        let kernel = self.ready().await;
        self.machine().skip(build);
        if let Some(kernel) = &kernel {
            self.persist(kernel).await;
        }
        self.publish()
    }

    /// `update.mark_prompted`。
    pub(crate) async fn mark_prompted(
        &self,
        build: Option<u64>,
        form: &str,
    ) -> Result<UpdateStatus> {
        let form = UpdatePrompt::parse(form).ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                format!("unknown prompt form {form:?}"),
            )
        })?;
        let kernel = self.ready().await;
        self.machine().mark_prompted(build, form, now_ms());
        if let Some(kernel) = &kernel {
            self.persist(kernel).await;
        }
        Ok(self.publish())
    }

    /// 回到前台：强制提示重新出现，到时间了就检查。
    pub(crate) fn foregrounded(self: &Arc<Self>) {
        self.machine().foregrounded(now_ms());
        self.publish();
        self.spawn_check(CheckTrigger::Scheduled);
    }

    /// 进了后台：下一次回前台一定是新的一次。
    pub(crate) fn backgrounded(&self) {
        self.machine().backgrounded();
    }

    /// 网络状态变化。`metered` 决定下载要不要先征得同意；恢复联网时到时间了就检查。
    pub(crate) fn network_changed(self: &Arc<Self>, available: bool, metered: bool) {
        self.metered.store(metered, Ordering::Relaxed);
        if available {
            self.spawn_check(CheckTrigger::Scheduled);
        }
    }

    /// 收到 `flare.release` 通知（已在 `spread_ms` 内随机延迟过）。
    pub(crate) fn release_notice(self: &Arc<Self>) {
        self.spawn_check(CheckTrigger::Pushed);
    }

    /// 服务端拒绝了这个版本：立刻进入 `required` 与只读，再查一次清单找更新目标。
    pub(crate) fn rejected(self: &Arc<Self>, min_build: Option<u64>) {
        let runtime = self.clone();
        spawn_background(async move {
            runtime.ready().await;
            let first = !runtime.machine().rejected_by_server();
            runtime.machine().rejected(min_build, now_ms());
            runtime.publish();
            if first {
                runtime.run_background_check(CheckTrigger::Pushed).await;
            }
        });
    }
}

/// 下载进度写进状态机，这样登录前也能从 `update.get_state` 读到。
#[cfg(not(target_arch = "wasm32"))]
struct Observer {
    runtime: Arc<UpdateRuntime>,
}

#[cfg(not(target_arch = "wasm32"))]
impl flare_access::download::DownloadObserver for Observer {
    fn progress(&mut self, received: u64, total: u64) {
        self.runtime.machine().download_progress(received, total);
        self.runtime.publish();
    }

    fn verifying(&mut self) {
        self.runtime.machine().begin_verifying();
        self.runtime.publish();
    }
}

#[cfg(test)]
mod tests;
