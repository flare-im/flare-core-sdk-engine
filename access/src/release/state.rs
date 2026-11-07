//! 更新状态机（§10.6）：纯状态，所有「现在」由调用方传入，I/O 与定时都在引擎。
//!
//! 状态分两层：评估结果（[`Offer`]）决定 `idle`、`upToDate`、`available`、`required`；进行中的动作
//! （`Phase`）盖在上面。这样下载失败、取消安装之后自然回到评估结果，不用记「上一个状态」。

use serde::{Deserialize, Serialize};

use super::evaluate::{
    Decision, EvaluationInput, ServerGate, Urgency, artifact_for, block_reason, evaluate,
};
use super::locale::select_notes;
use super::plan::{UpdateInstallPlan, store_url};
use super::prompt::{PromptHistory, PromptSession, UpdatePrompt, decide_prompt, held_form};
use super::resources::PackInput;
use crate::manifest::{ReleaseArtifact, ReleaseEntry, is_valid_path_segment};
use crate::time::HOUR_MS;
use crate::trust::VerifiedReleaseManifest;

/// 自动检查的最小间隔；手动检查不受限。
pub const CHECK_INTERVAL_MS: i64 = 6 * HOUR_MS;
/// 首帧之后多久才做第一次检查，不占冷启动预算。
pub const STARTUP_CHECK_DELAY_MS: u64 = 3_000;
/// 同一次回前台的两条路（控制器读状态、`lifecycle.set_app_state`）最多差多久。
pub const FOREGROUND_SETTLE_MS: i64 = 5_000;

/// 距上次检查是否已超过间隔。时钟被拨回（上次检查在未来）时也算到期，否则要等到时钟追上来。
pub fn check_due(last_checked_at_ms: Option<i64>, now_ms: i64) -> bool {
    last_checked_at_ms.is_none_or(|last| last > now_ms || now_ms - last > CHECK_INTERVAL_MS)
}

/// 跨端稳定的错误码。
pub mod codes {
    pub const MANIFEST_UNAVAILABLE: &str = "update.manifestUnavailable";
    pub const MANIFEST_REJECTED: &str = "update.manifestRejected";
    pub const NO_ARTIFACT: &str = "update.noArtifact";
    pub const METERED_CONFIRMATION_REQUIRED: &str = "update.meteredConfirmationRequired";
    pub const INSUFFICIENT_SPACE: &str = "update.insufficientSpace";
    pub const DOWNLOAD_FAILED: &str = "update.downloadFailed";
    pub const HASH_MISMATCH: &str = "update.hashMismatch";
    pub const INSTALL_FAILED: &str = "update.installFailed";
    pub const SIGNATURE_MISMATCH: &str = "update.signatureMismatch";
    pub const DOWNGRADE_REJECTED: &str = "update.downgradeRejected";
    pub const NOT_SKIPPABLE: &str = "update.notSkippable";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UpdateState {
    Idle,
    Checking,
    UpToDate,
    Available,
    Required,
    Downloading,
    Verifying,
    ReadyToInstall,
    Installing,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReleaseKind {
    Store,
    Package,
    WebBundle,
    Desktop,
}

impl ReleaseKind {
    fn of(artifact: &ReleaseArtifact) -> Option<Self> {
        match artifact.kind.as_str() {
            "store" => Some(ReleaseKind::Store),
            "apk" => Some(ReleaseKind::Package),
            "web_bundle" => Some(ReleaseKind::WebBundle),
            "tauri" | "electron" => Some(ReleaseKind::Desktop),
            _ => None,
        }
    }
}

/// 本机是谁。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateApp {
    /// 发布清单路径里的平台段。
    pub platform: String,
    pub channel: String,
    pub version: Option<String>,
    pub build: Option<u64>,
    /// Web 运行中的构建。
    pub build_id: Option<String>,
    pub distribution: Option<String>,
    /// 桌面目标，例如 `darwin-aarch64`。
    pub target: Option<String>,
    /// 配置里的语言。宿主用 `update.set_locale` 选过语言时以那个为准。
    pub locale: Option<String>,
}

impl UpdateApp {
    /// 配置够不够查更新：路径段合法，并且有东西可比（构建号或 Web 构建）。
    pub fn can_check(&self) -> bool {
        is_valid_path_segment(&self.platform)
            && is_valid_path_segment(&self.channel)
            && (self.build.is_some() || self.build_id.is_some())
    }
}

/// `update://state` 的 `current`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentApp {
    pub version: Option<String>,
    pub build: Option<u64>,
    pub channel: String,
    pub distribution: Option<String>,
}

/// `update://state` 的 `release`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseSummary {
    pub release_id: String,
    pub version: String,
    pub build: u64,
    /// 已按语言选好。
    pub notes: Vec<String>,
    /// 商店跳转时为 null。
    pub size_bytes: Option<u64>,
    pub mandatory: bool,
    pub recommended: bool,
    pub kind: ReleaseKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProgress {
    pub received: u64,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateError {
    pub code: String,
    pub retryable: bool,
    pub fallback_url: Option<String>,
}

/// `update.*` 的返回值，也是 `update://state` 的载荷。每个字段都会出现，没有值时为 null。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateStatus {
    pub state: UpdateState,
    pub current: CurrentApp,
    pub release: Option<ReleaseSummary>,
    pub progress: Option<UpdateProgress>,
    pub prompt: UpdatePrompt,
    pub block_ui: bool,
    pub read_only: bool,
    /// Unix 毫秒，从未检查过为 null。
    pub last_checked_at: Option<i64>,
    pub error: Option<UpdateError>,
    /// 当前构建命中 `blockedBuilds` 时的原因，已按语言选好。
    pub block_reason: Option<String>,
}

/// 已下载并校验过的制品。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactRecord {
    pub path: String,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_cert_sha256: Option<String>,
    pub build: u64,
    pub size_bytes: u64,
}

/// 状态机的持久部分（`update/state`）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PersistedUpdate {
    pub last_checked_at_ms: Option<i64>,
    pub prompts: PromptHistory,
    pub artifact: Option<ArtifactRecord>,
}

/// 下载模块要的全部信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadSpec {
    pub urls: Vec<String>,
    pub sha256: String,
    pub size_bytes: u64,
    pub signing_cert_sha256: Option<String>,
    pub build: u64,
    pub file_name: String,
}

/// 一轮检查的结果。
#[derive(Debug, Clone)]
pub enum CheckResult {
    /// 现在手里最新的已验签清单（可能是本地那份）。
    Manifest(VerifiedReleaseManifest),
    /// 镜像答复没有这个平台渠道的清单，本机也没有。
    NotPublished,
    /// 没有拿到可信结果。手里的清单和结论不动。
    Failed { code: &'static str },
    /// 开发模式：没有信任材料，查不了。
    Skipped,
}

/// `update.report_install` 的 `outcome`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallOutcome {
    Started,
    Completed,
    Cancelled,
    Failed,
}

impl InstallOutcome {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "started" => Some(InstallOutcome::Started),
            "completed" => Some(InstallOutcome::Completed),
            "cancelled" => Some(InstallOutcome::Cancelled),
            "failed" => Some(InstallOutcome::Failed),
            _ => None,
        }
    }
}

/// `update.install` 的下一步。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallStep {
    Plan(UpdateInstallPlan),
    /// 安装包：调用方重新校验文件后才能得到计划。
    Package(ArtifactRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OfferedRelease {
    entry: ReleaseEntry,
    artifact: ReleaseArtifact,
    distribution: String,
}

/// 评估结果。
#[derive(Debug, Clone, PartialEq, Eq)]
struct Offer {
    urgency: Urgency,
    release: Option<OfferedRelease>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Settled,
    Checking,
    Downloading,
    Verifying,
    Ready,
    Installing,
    Failed,
}

#[derive(Debug, Clone)]
pub struct UpdateMachine {
    app: UpdateApp,
    install_id: String,
    manifest: Option<VerifiedReleaseManifest>,
    server: Option<ServerGate>,
    offer: Option<Offer>,
    /// 做过一次评估：区分「没查过」与「已是最新」。
    evaluated: bool,
    phase: Phase,
    progress: Option<UpdateProgress>,
    last_checked_at_ms: Option<i64>,
    error: Option<UpdateError>,
    prompts: PromptHistory,
    session: PromptSession,
    /// 本会话最后一次弹出的提示：目标版本与形态。更新流程进行中靠它撑住提示。
    shown_prompt: Option<(Option<u64>, UpdatePrompt)>,
    /// 流程已结束（安装完成或取消、用户点了「稍后」），回到频率规则，直到下一个动作。
    prompt_released: bool,
    /// 启动检查还没结束。登录前没有事件，宿主靠 `checking` 继续轮询。
    startup_check_pending: bool,
    /// 在跑和排着的检查轮数。归零才离开 `checking`。
    check_rounds: u32,
    /// 宿主手动要查：这一轮不受频率限制，失败要写进 `error`。
    manual_check: bool,
    /// 宿主用 `update.set_locale` 选的语言。只在内存里，重新 `init` 时留住。
    chosen_locale: Option<String>,
    /// 最近一次算数的回前台。
    foregrounded_at_ms: Option<i64>,
    /// 提示正显示在屏幕上：`mark_prompted` 之后，到用户关掉或安装结束为止。
    prompt_open: bool,
    /// 用户在强制更新上点了「继续浏览」：本地只读，不再挡着。
    read_only_accepted: bool,
    /// 下载中用户点了「稍后」：提示收起，下载继续，下载结束再出现。
    prompt_hidden: bool,
    ready: Option<ArtifactRecord>,
    /// 不再需要的制品，等调用方删掉文件。
    discarded: Option<ArtifactRecord>,
}

fn sanitized(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        .collect()
}

impl UpdateMachine {
    pub fn new(app: UpdateApp) -> Self {
        Self {
            app,
            install_id: String::new(),
            manifest: None,
            server: None,
            offer: None,
            evaluated: false,
            phase: Phase::Settled,
            progress: None,
            last_checked_at_ms: None,
            error: None,
            prompts: PromptHistory::default(),
            session: PromptSession::default(),
            shown_prompt: None,
            prompt_released: false,
            startup_check_pending: false,
            check_rounds: 0,
            manual_check: false,
            chosen_locale: None,
            foregrounded_at_ms: None,
            prompt_open: false,
            read_only_accepted: false,
            prompt_hidden: false,
            ready: None,
            discarded: None,
        }
    }

    pub fn app(&self) -> &UpdateApp {
        &self.app
    }

    /// 装入本地记录。手里有清单就按它评估：没到检查时间时用缓存结论。
    pub fn restore(
        &mut self,
        install_id: String,
        persisted: PersistedUpdate,
        manifest: Option<VerifiedReleaseManifest>,
        now_ms: i64,
    ) {
        self.install_id = install_id;
        self.last_checked_at_ms = persisted.last_checked_at_ms;
        self.prompts = persisted.prompts;
        self.ready = persisted.artifact;
        self.manifest = manifest;
        self.reassess(now_ms);
        if self.ready.is_some() {
            self.phase = Phase::Ready;
            // 新的一次运行没有打开着的提示：已下好的包按频率规则提醒，不是每次启动都弹。
            self.prompt_released = true;
        }
    }

    pub fn persisted(&self) -> PersistedUpdate {
        PersistedUpdate {
            last_checked_at_ms: self.last_checked_at_ms,
            prompts: self.prompts.clone(),
            artifact: self.ready.clone(),
        }
    }

    pub fn last_checked_at_ms(&self) -> Option<i64> {
        self.last_checked_at_ms
    }

    /// 手里的清单和选资源包要的本机信息。本地记录还没读进来、也没查到清单时为空。
    pub fn pack_source(&self) -> Option<(VerifiedReleaseManifest, PackInput)> {
        let verified = self.manifest.clone()?;
        let input = PackInput {
            current_build: self.app.build,
            install_id: self.install_id.clone(),
        };
        Some((verified, input))
    }

    /// 已被服务端拒绝过。
    pub fn rejected_by_server(&self) -> bool {
        self.server.is_some()
    }

    fn input(&self, stale: bool) -> EvaluationInput<'_> {
        EvaluationInput {
            current_build: self.app.build,
            current_build_id: self.app.build_id.as_deref(),
            distribution: self.app.distribution.as_deref().unwrap_or_default(),
            target: self.app.target.as_deref(),
            install_id: &self.install_id,
            stale,
            server: self.server,
        }
    }

    fn offered(&self, entry: &ReleaseEntry) -> Option<OfferedRelease> {
        let distribution = self.app.distribution.clone().unwrap_or_default();
        let artifact = artifact_for(entry, &distribution, self.app.target.as_deref())?.clone();
        Some(OfferedRelease {
            entry: entry.clone(),
            artifact,
            distribution,
        })
    }

    fn assess(&self, now_ms: i64) -> Option<Offer> {
        let Some(verified) = &self.manifest else {
            return self.server.map(|_| Offer {
                urgency: Urgency::Required { block_ui: false },
                release: None,
            });
        };
        let input = self.input(verified.is_stale(now_ms));
        match evaluate(&verified.manifest, &input) {
            Decision::UpToDate => None,
            Decision::Available {
                release,
                recommended,
            } => Some(Offer {
                urgency: if recommended {
                    Urgency::Recommended
                } else {
                    Urgency::Optional
                },
                release: self.offered(release),
            }),
            Decision::Required { release, block_ui } => Some(Offer {
                urgency: Urgency::Required { block_ui },
                release: release.and_then(|release| self.offered(release)),
            }),
        }
    }

    /// 重新评估。下载好的包不再是提示的版本时撤销它（灰度暂停后已下载的客户端不再提示）。
    fn reassess(&mut self, now_ms: i64) {
        self.offer = self.assess(now_ms);
        self.evaluated = self.evaluated || self.manifest.is_some();
        let target = self.target_build();
        if self
            .ready
            .as_ref()
            .is_some_and(|ready| Some(ready.build) != target)
        {
            self.discarded = self.ready.take();
            if self.phase == Phase::Ready {
                self.phase = Phase::Settled;
            }
        }
    }

    fn target_build(&self) -> Option<u64> {
        self.release().map(|release| release.entry.build)
    }

    fn release(&self) -> Option<&OfferedRelease> {
        self.offer.as_ref().and_then(|offer| offer.release.as_ref())
    }

    fn urgency(&self) -> Option<Urgency> {
        self.offer.as_ref().map(|offer| offer.urgency)
    }

    fn busy(&self) -> bool {
        matches!(
            self.phase,
            Phase::Downloading | Phase::Verifying | Phase::Installing
        )
    }

    fn soft_error(&mut self, code: &str, retryable: bool) {
        self.prompt_released = false;
        self.error = Some(UpdateError {
            code: code.to_owned(),
            retryable,
            fallback_url: self.fallback_url(),
        });
    }

    /// 失败时给用户的另一条路：商店页或官网，直装和桌面没有就用下载地址本身。
    /// Web 没有：`urls` 是资产清单，不是给人打开的页面。
    fn fallback_url(&self) -> Option<String> {
        let release = self.release()?;
        let artifact = &release.artifact;
        match ReleaseKind::of(artifact)? {
            ReleaseKind::WebBundle => None,
            ReleaseKind::Store => store_url(&release.distribution, artifact),
            ReleaseKind::Package => artifact
                .store_url
                .clone()
                .or_else(|| artifact.urls.first().cloned()),
            ReleaseKind::Desktop => artifact.store_url.clone().or_else(|| {
                let target = self.app.target.as_deref()?;
                artifact.targets.get(target)?.urls.first().cloned()
            }),
        }
    }

    /// 取走待删的制品。
    pub fn take_discarded(&mut self) -> Option<ArtifactRecord> {
        self.discarded.take()
    }

    /// 记下一轮后台检查（启动、回前台、恢复联网、`flare.release`）。不改界面状态：提示不该因为
    /// 一次静默检查闪一下。
    pub fn begin_background_check(&mut self) {
        self.check_rounds += 1;
    }

    /// 宿主要查更新（`update.check`）：先进 `checking`，这一轮由调用方放到后台跑。返回要不要新起
    /// 一轮：已有一轮在跑时不重复起，这次请求并进去，那一轮结束时一起收回来。
    /// 非手动且没到时间时什么也不做，不进 `checking`。
    pub fn request_check(&mut self, manual: bool, now_ms: i64) -> bool {
        let running = self.check_rounds > 0;
        if !manual && !running && !check_due(self.last_checked_at_ms, now_ms) {
            return false;
        }
        self.error = None;
        self.manual_check = self.manual_check || manual;
        if matches!(self.phase, Phase::Settled | Phase::Failed) {
            self.phase = Phase::Checking;
        }
        if running {
            return false;
        }
        self.check_rounds = 1;
        true
    }

    /// 受频率限制的这一轮还要不要去问镜像，不用问时这一轮就此结束。`request_check` 的判断可能早于
    /// 本地记录读进来，这里才准。判定和收尾在一起：分开的话手动请求能并进一轮刚判定作废的检查。
    pub fn check_still_due(&mut self, now_ms: i64) -> bool {
        let due = self.manual_check || check_due(self.last_checked_at_ms, now_ms);
        if !due {
            self.end_check_round();
        }
        due
    }

    /// 一轮结束，返回它是不是手动的。
    fn end_check_round(&mut self) -> bool {
        self.check_rounds = self.check_rounds.saturating_sub(1);
        if self.check_rounds == 0 && self.phase == Phase::Checking {
            self.phase = Phase::Settled;
        }
        std::mem::take(&mut self.manual_check)
    }

    /// 配置不够查更新，这一轮就此结束。手动的才报错。
    pub fn check_unavailable(&mut self) {
        if self.end_check_round() {
            self.soft_error(codes::MANIFEST_UNAVAILABLE, false);
        }
    }

    pub fn finish_check(&mut self, result: CheckResult, now_ms: i64) {
        let manual = self.end_check_round();
        match result {
            CheckResult::Manifest(manifest) => {
                self.manifest = Some(manifest);
                self.last_checked_at_ms = Some(now_ms);
                self.reassess(now_ms);
            }
            CheckResult::NotPublished => {
                self.last_checked_at_ms = Some(now_ms);
                self.evaluated = true;
                self.reassess(now_ms);
            }
            CheckResult::Failed { code } => {
                if manual {
                    self.soft_error(code, true);
                }
            }
            CheckResult::Skipped => {}
        }
    }

    /// 同一次运行里重新 `init`：本次运行弹过什么要留着，否则同一次启动会弹两回。
    /// 宿主选的语言也留着：配置里的往往是系统语言，不是 App 内的语言。
    pub fn adopt_session(&mut self, previous: &UpdateMachine) {
        self.session = previous.session.clone();
        self.shown_prompt = previous.shown_prompt;
        self.prompt_open = previous.prompt_open;
        self.read_only_accepted = previous.read_only_accepted;
        self.chosen_locale = previous.chosen_locale.clone();
        self.foregrounded_at_ms = previous.foregrounded_at_ms;
    }

    /// `update.set_locale`：更新说明和封禁原因从下一次取状态起按新语言选。
    pub fn set_locale(&mut self, locale: String) {
        self.chosen_locale = Some(locale);
    }

    fn locale(&self) -> Option<&str> {
        self.chosen_locale.as_deref().or(self.app.locale.as_deref())
    }

    pub fn set_startup_check_pending(&mut self, pending: bool) {
        self.startup_check_pending = pending;
    }

    /// 回到前台：强制提示重新出现。同一次回前台会从两条路到（控制器的 `update.get_state`、
    /// `lifecycle.set_app_state`），先后不定，只算一次：后到的那条要是在 `mark_prompted` 之后再算，
    /// 就把刚记下的「弹过」清掉了，用户关掉提示马上又弹一回。
    pub fn foregrounded(&mut self, now_ms: i64) {
        let same_return = self
            .foregrounded_at_ms
            .is_some_and(|at| (0..FOREGROUND_SETTLE_MS).contains(&(now_ms - at)));
        if same_return {
            return;
        }
        self.foregrounded_at_ms = Some(now_ms);
        self.session.foregrounded();
    }

    /// 进了后台：下一次回前台一定是新的一次，不管隔了多久。
    pub fn backgrounded(&mut self) {
        self.foregrounded_at_ms = None;
    }

    /// 服务端拒绝了这个版本：立刻进入 `required` 与只读，不等清单。
    pub fn rejected(&mut self, min_build: Option<u64>, now_ms: i64) {
        let merged = self
            .server
            .and_then(|server| server.min_build)
            .max(min_build);
        self.server = Some(ServerGate { min_build: merged });
        self.reassess(now_ms);
    }

    /// 「稍后」。下载、校验中只收起提示，下载继续，不算推迟：下完还要告诉用户重启。
    pub fn defer(&mut self, now_ms: i64) {
        self.error = None;
        if matches!(self.phase, Phase::Downloading | Phase::Verifying) {
            self.prompt_hidden = true;
            return;
        }
        match self.urgency() {
            Some(Urgency::Required { block_ui: true }) => {
                self.soft_error(codes::NOT_SKIPPABLE, false);
            }
            // 强制但不阻断：这是「继续浏览」，只读，回前台或下次启动再提示。
            Some(urgency) if urgency.is_required() => {
                self.read_only_accepted = true;
            }
            Some(_) => {
                self.prompts.defer(now_ms);
                if self.phase == Phase::Failed {
                    self.phase = Phase::Settled;
                }
            }
            None => {}
        }
        self.prompt_open = false;
        self.prompt_released = true;
    }

    pub fn skip(&mut self, build: u64) {
        self.error = None;
        if self.urgency().is_some_and(Urgency::is_required) {
            self.soft_error(codes::NOT_SKIPPABLE, false);
            return;
        }
        self.prompts.skip(build);
        self.prompt_open = false;
    }

    pub fn mark_prompted(&mut self, build: Option<u64>, form: UpdatePrompt, now_ms: i64) {
        if let Some(urgency) = self.urgency() {
            self.prompts
                .record(&mut self.session, urgency, build, form, now_ms);
            if matches!(
                form,
                UpdatePrompt::Sheet | UpdatePrompt::Modal | UpdatePrompt::Fullscreen
            ) {
                self.shown_prompt = Some((build, form));
                self.prompt_open = true;
                self.prompt_released = false;
            }
        }
    }

    /// 开始下载。`None` 时不用下载：要么已在下载或下好了，要么 `error` 里写了原因。
    pub fn begin_download(&mut self, metered: bool, allow_metered: bool) -> Option<DownloadSpec> {
        if self.busy() || self.phase == Phase::Ready {
            return None;
        }
        self.error = None;
        self.prompt_released = false;
        let spec = self.release().and_then(|release| {
            let artifact = &release.artifact;
            if ReleaseKind::of(artifact) != Some(ReleaseKind::Package) {
                return None;
            }
            let version = sanitized(&release.entry.version);
            Some(DownloadSpec {
                urls: artifact.urls.clone(),
                sha256: artifact.sha256.clone()?,
                size_bytes: artifact.size_bytes?,
                signing_cert_sha256: artifact.signing_cert_sha256.clone(),
                build: release.entry.build,
                file_name: format!("{version}-{}.apk", release.entry.build),
            })
        });
        let Some(spec) = spec else {
            self.soft_error(codes::NO_ARTIFACT, false);
            return None;
        };
        if metered && !allow_metered {
            self.soft_error(codes::METERED_CONFIRMATION_REQUIRED, false);
            return None;
        }
        self.phase = Phase::Downloading;
        self.progress = Some(UpdateProgress {
            received: 0,
            total: spec.size_bytes,
        });
        Some(spec)
    }

    pub fn download_progress(&mut self, received: u64, total: u64) {
        if self.phase == Phase::Downloading {
            self.progress = Some(UpdateProgress { received, total });
        }
    }

    pub fn begin_verifying(&mut self) {
        if self.phase == Phase::Downloading {
            self.phase = Phase::Verifying;
        }
    }

    pub fn download_ready(&mut self, artifact: ArtifactRecord, now_ms: i64) {
        self.progress = None;
        self.error = None;
        self.prompt_hidden = false;
        self.ready = Some(artifact);
        self.phase = Phase::Ready;
        // 下载期间清单可能变了：这时候再判一次是否还要这个版本。
        self.reassess(now_ms);
    }

    pub fn download_failed(&mut self, code: &str, retryable: bool) {
        self.progress = None;
        self.prompt_hidden = false;
        self.phase = Phase::Failed;
        self.soft_error(code, retryable);
    }

    /// 开始安装。`None` 时 `error` 里写了原因。
    pub fn begin_install(&mut self) -> Option<InstallStep> {
        self.error = None;
        self.prompt_released = false;
        let Some(release) = self.release() else {
            self.soft_error(codes::NO_ARTIFACT, false);
            return None;
        };
        let required = self.urgency().is_some_and(Urgency::is_required);
        let block_ui = matches!(self.urgency(), Some(Urgency::Required { block_ui: true }));
        let artifact = &release.artifact;
        let step = match ReleaseKind::of(artifact) {
            Some(ReleaseKind::Store) => Some(InstallStep::Plan(UpdateInstallPlan::OpenStore {
                url: store_url(&release.distribution, artifact),
                immediate: required && release.distribution == "play",
                store: release.distribution.clone(),
                package_name: artifact.package_name.clone(),
                app_id: artifact.app_id.clone(),
            })),
            Some(ReleaseKind::Package) => self.ready.clone().map(InstallStep::Package),
            Some(ReleaseKind::WebBundle) => {
                artifact.build_id.clone().zip(artifact.sha256.clone()).map(
                    |(build_id, asset_manifest_sha256)| {
                        InstallStep::Plan(UpdateInstallPlan::ActivateWebBundle {
                            build_id,
                            asset_manifest_urls: artifact.urls.clone(),
                            asset_manifest_sha256,
                            mandatory: required,
                            block_ui,
                        })
                    },
                )
            }
            Some(ReleaseKind::Desktop) => self.app.target.clone().and_then(|target| {
                let sha256 = artifact.targets.get(&target)?.sha256.clone();
                Some(InstallStep::Plan(UpdateInstallPlan::DesktopUpdater {
                    version: release.entry.version.clone(),
                    target,
                    sha256,
                }))
            }),
            None => None,
        };
        match step {
            Some(InstallStep::Plan(plan)) => {
                self.phase = Phase::Installing;
                Some(InstallStep::Plan(plan))
            }
            Some(package) => Some(package),
            None => {
                self.soft_error(codes::NO_ARTIFACT, false);
                None
            }
        }
    }

    /// 安装包重新校验通过，计划已交给平台。
    pub fn package_handed_over(&mut self) {
        self.phase = Phase::Installing;
    }

    /// 平台回报结果。
    pub fn report_install(&mut self, outcome: InstallOutcome, code: Option<&str>) {
        match outcome {
            InstallOutcome::Started => {
                self.error = None;
                self.phase = Phase::Installing;
            }
            InstallOutcome::Completed => {
                self.error = None;
                self.prompt_open = false;
                self.prompt_released = true;
                // 商店只是打开了，用户不一定装；其余由安装器接管，进程会重启。
                let store = self
                    .release()
                    .and_then(|release| ReleaseKind::of(&release.artifact))
                    == Some(ReleaseKind::Store);
                if store {
                    self.phase = Phase::Settled;
                }
            }
            InstallOutcome::Cancelled => {
                self.error = None;
                self.prompt_open = false;
                self.prompt_released = true;
                self.phase = if self.ready.is_some() {
                    Phase::Ready
                } else {
                    Phase::Settled
                };
            }
            InstallOutcome::Failed => {
                let code = code.unwrap_or(codes::INSTALL_FAILED);
                // 这个包本身不可信或不可用，重试同一个文件没有意义。
                let bad_package = matches!(
                    code,
                    codes::SIGNATURE_MISMATCH | codes::DOWNGRADE_REJECTED | codes::HASH_MISMATCH
                );
                if bad_package {
                    self.discarded = self.ready.take();
                }
                self.phase = Phase::Failed;
                self.soft_error(code, !bad_package);
            }
        }
    }

    /// 文件在交给平台之前校验失败。
    pub fn package_rejected(&mut self) {
        self.discarded = self.ready.take();
        self.phase = Phase::Failed;
        self.soft_error(codes::HASH_MISMATCH, true);
    }

    fn state(&self) -> UpdateState {
        match self.phase {
            Phase::Checking => UpdateState::Checking,
            Phase::Downloading => UpdateState::Downloading,
            Phase::Verifying => UpdateState::Verifying,
            Phase::Ready => UpdateState::ReadyToInstall,
            Phase::Installing => UpdateState::Installing,
            Phase::Failed => UpdateState::Failed,
            Phase::Settled => match self.urgency() {
                Some(urgency) if urgency.is_required() => UpdateState::Required,
                Some(_) => UpdateState::Available,
                None if self.startup_check_pending => UpdateState::Checking,
                None if self.evaluated => UpdateState::UpToDate,
                None => UpdateState::Idle,
            },
        }
    }

    fn summary(&self) -> Option<ReleaseSummary> {
        let urgency = self.urgency()?;
        let release = self.release()?;
        let artifact = &release.artifact;
        let kind = ReleaseKind::of(artifact)?;
        let size_bytes = match kind {
            ReleaseKind::Store => None,
            ReleaseKind::Package | ReleaseKind::WebBundle => artifact.size_bytes,
            ReleaseKind::Desktop => self
                .app
                .target
                .as_deref()
                .and_then(|target| artifact.targets.get(target))
                .and_then(|target| target.size_bytes),
        };
        Some(ReleaseSummary {
            release_id: release.entry.release_id.clone(),
            version: release.entry.version.clone(),
            build: release.entry.build,
            notes: select_notes(&release.entry.notes, self.locale()),
            size_bytes,
            mandatory: urgency.is_required(),
            recommended: urgency == Urgency::Recommended,
            kind,
        })
    }

    /// 手里的清单里写的当前构建的封禁原因。过期清单不再据此强制，但原因本身没变。
    fn block_reason(&self) -> Option<String> {
        let policy = &self.manifest.as_ref()?.manifest.policy;
        block_reason(policy, self.app.build?, self.locale())
    }

    /// 更新流程进行中：提示要撑住，不能因为状态变了就消失。
    fn prompt_is_held(&self) -> bool {
        if self.prompt_released {
            return false;
        }
        match self.phase {
            Phase::Downloading
            | Phase::Verifying
            | Phase::Ready
            | Phase::Installing
            | Phase::Failed => true,
            // 手动检查失败由发起检查的页面自己显示，不弹提示。
            Phase::Settled => {
                self.prompt_is_open()
                    || self.error.as_ref().is_some_and(|error| {
                        error.code != codes::MANIFEST_UNAVAILABLE
                            && error.code != codes::MANIFEST_REJECTED
                    })
            }
            Phase::Checking => false,
        }
    }

    /// 屏幕上的提示还是当前目标版本的：弹出后不能因为频率规则记了账就消失。
    fn prompt_is_open(&self) -> bool {
        let build = self.target_build();
        self.prompt_open
            && self
                .shown_prompt
                .is_some_and(|(shown_build, _)| shown_build == build)
    }

    fn prompt(&self, now_ms: i64) -> UpdatePrompt {
        let Some(urgency) = self.urgency() else {
            return UpdatePrompt::None;
        };
        // 阻断界面的强制更新一直全屏，下载和安装都在全屏里进行。
        if urgency == (Urgency::Required { block_ui: true }) {
            return UpdatePrompt::Fullscreen;
        }
        if self.prompt_hidden || self.phase == Phase::Checking {
            return UpdatePrompt::None;
        }
        let build = self.target_build();
        if self.prompt_is_held() {
            let shown = self
                .shown_prompt
                .filter(|(shown_build, _)| *shown_build == build)
                .map(|(_, form)| form);
            return held_form(urgency, shown);
        }
        decide_prompt(urgency, build, now_ms, &self.prompts, &self.session)
    }

    /// 当前状态。
    pub fn status(&self, now_ms: i64) -> UpdateStatus {
        UpdateStatus {
            state: self.state(),
            current: CurrentApp {
                version: self.app.version.clone(),
                build: self.app.build,
                channel: self.app.channel.clone(),
                distribution: self.app.distribution.clone(),
            },
            release: self.summary(),
            progress: self.progress,
            prompt: self.prompt(now_ms),
            block_ui: self.urgency() == Some(Urgency::Required { block_ui: true }),
            read_only: self.server.is_some()
                || (self.read_only_accepted && self.urgency().is_some_and(Urgency::is_required)),
            last_checked_at: self.last_checked_at_ms,
            error: self.error.clone(),
            block_reason: self.block_reason(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::manifest::ReleaseManifest;

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    /// 2026-11-04T08:00:00Z
    const NOW: i64 = 1_793_779_200_000;

    fn app(distribution: &str) -> UpdateApp {
        UpdateApp {
            platform: "android".into(),
            channel: "stable".into(),
            version: Some("2.3.2".into()),
            build: Some(20302),
            distribution: Some(distribution.into()),
            locale: Some("zh-CN".into()),
            ..Default::default()
        }
    }

    fn verified(policy: serde_json::Value, halted: bool) -> VerifiedReleaseManifest {
        let payload = serde_json::json!({
            "schema": "flare.release.v1",
            "bootstrapId": "b7f3c9a1e2d4b6a8",
            "platform": "android",
            "channel": "stable",
            "serial": 88,
            "issuedAt": "2026-11-04T08:00:00Z",
            "expiresAt": "2026-11-18T08:00:00Z",
            "policy": policy,
            "releases": [{
                "releaseId": "rel_01JC8W",
                "version": "2.4.0",
                "build": 20400,
                "rollout": { "basisPoints": 10000, "salt": "rel_01JC8W", "halted": halted },
                "notes": { "zh-CN": ["新增消息定时发送"], "en": ["Scheduled messages"] },
                "artifacts": {
                    "play": { "kind": "store", "packageName": "com.acme.chat" },
                    "direct": {
                        "kind": "apk",
                        "urls": ["https://dl-a.example-cdn.com/acme/2.4.0-20400.apk"],
                        "sizeBytes": 52428800,
                        "sha256": DIGEST,
                        "signingCertSha256": DIGEST
                    }
                }
            }]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let (manifest, times) = ReleaseManifest::from_slice(&bytes).unwrap();
        VerifiedReleaseManifest {
            serial: manifest.serial,
            manifest: Arc::new(manifest),
            key_id: "test".into(),
            issued_at_ms: times.issued_at_ms,
            expires_at_ms: times.expires_at_ms,
            envelope: Arc::from(bytes.as_slice()),
            digest: String::new(),
        }
    }

    fn machine(distribution: &str, manifest: VerifiedReleaseManifest) -> UpdateMachine {
        let mut machine = UpdateMachine::new(app(distribution));
        machine.restore("install-1".into(), PersistedUpdate::default(), None, NOW);
        machine.finish_check(CheckResult::Manifest(manifest), NOW);
        machine
    }

    fn record() -> ArtifactRecord {
        ArtifactRecord {
            path: "/tmp/2.4.0-20400.apk".into(),
            sha256: DIGEST.into(),
            signing_cert_sha256: Some(DIGEST.into()),
            build: 20400,
            size_bytes: 52_428_800,
        }
    }

    #[test]
    fn a_fresh_machine_is_idle_and_a_check_settles_it() {
        let mut machine = UpdateMachine::new(app("play"));
        assert_eq!(machine.status(NOW).state, UpdateState::Idle);
        assert!(machine.request_check(true, NOW));
        assert_eq!(machine.status(NOW).state, UpdateState::Checking);
        machine.finish_check(CheckResult::NotPublished, NOW);
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::UpToDate);
        assert_eq!(status.last_checked_at, Some(NOW));
        assert_eq!(status.release, None);
    }

    #[test]
    fn an_available_release_is_summarized_in_the_chosen_language() {
        let machine = machine("direct", verified(serde_json::json!({}), false));
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::Available);
        assert_eq!(status.prompt, UpdatePrompt::Sheet);
        let release = status.release.unwrap();
        assert_eq!(release.notes, vec!["新增消息定时发送"]);
        assert_eq!(release.kind, ReleaseKind::Package);
        assert_eq!(release.size_bytes, Some(52_428_800));
        assert!(!release.mandatory);
    }

    #[test]
    fn a_failed_background_check_keeps_the_last_result_and_says_nothing() {
        let mut machine = machine("play", verified(serde_json::json!({}), false));
        machine.begin_background_check();
        assert_eq!(machine.status(NOW).state, UpdateState::Available);
        machine.finish_check(
            CheckResult::Failed {
                code: codes::MANIFEST_UNAVAILABLE,
            },
            NOW + 1,
        );
        let status = machine.status(NOW + 1);
        assert_eq!(status.state, UpdateState::Available);
        assert_eq!(status.error, None);
        assert_eq!(status.last_checked_at, Some(NOW));

        assert!(machine.request_check(true, NOW + 2));
        machine.finish_check(
            CheckResult::Failed {
                code: codes::MANIFEST_UNAVAILABLE,
            },
            NOW + 2,
        );
        let status = machine.status(NOW + 2);
        assert_eq!(status.state, UpdateState::Available);
        assert_eq!(status.error.unwrap().code, codes::MANIFEST_UNAVAILABLE);
    }

    #[test]
    fn a_server_rejection_is_required_and_read_only_without_any_manifest() {
        let mut machine = UpdateMachine::new(app("play"));
        machine.restore("install-1".into(), PersistedUpdate::default(), None, NOW);
        machine.rejected(Some(20400), NOW);
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::Required);
        assert!(status.read_only);
        assert_eq!(status.prompt, UpdatePrompt::Modal);
        assert_eq!(status.release, None);
        assert!(!status.block_ui);

        machine.finish_check(
            CheckResult::Manifest(verified(serde_json::json!({ "blockUi": true }), false)),
            NOW,
        );
        let status = machine.status(NOW);
        assert_eq!(status.prompt, UpdatePrompt::Fullscreen);
        assert!(status.block_ui);
        assert!(status.release.unwrap().mandatory);
    }

    #[test]
    fn later_on_a_required_release_continues_read_only_and_it_cannot_be_skipped() {
        let mut machine = machine(
            "play",
            verified(serde_json::json!({ "minBuild": 20400 }), false),
        );
        machine.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Modal);
        machine.defer(NOW);
        let status = machine.status(NOW);
        assert!(status.error.is_none());
        assert!(status.read_only);
        assert_eq!(status.prompt, UpdatePrompt::Badge);
        machine.foregrounded(NOW);
        let status = machine.status(NOW);
        assert_eq!(status.prompt, UpdatePrompt::Modal, "回前台再提示");
        assert!(status.read_only);
        machine.skip(20400);
        assert_eq!(
            machine.status(NOW).error.unwrap().code,
            codes::NOT_SKIPPABLE
        );
        assert!(machine.prompts.skipped_builds.is_empty());
    }

    #[test]
    fn a_store_release_plans_the_store_and_returns_after_it_opens() {
        let mut machine = machine(
            "play",
            verified(serde_json::json!({ "minBuild": 20400 }), false),
        );
        let step = machine.begin_install().unwrap();
        assert_eq!(
            step,
            InstallStep::Plan(UpdateInstallPlan::OpenStore {
                store: "play".into(),
                url: Some("https://play.google.com/store/apps/details?id=com.acme.chat".into()),
                package_name: Some("com.acme.chat".into()),
                app_id: None,
                immediate: true,
            })
        );
        assert_eq!(machine.status(NOW).state, UpdateState::Installing);
        machine.report_install(InstallOutcome::Completed, None);
        assert_eq!(machine.status(NOW).state, UpdateState::Required);
    }

    #[test]
    fn metered_networks_need_consent_and_a_store_release_has_nothing_to_download() {
        let mut direct = machine("direct", verified(serde_json::json!({}), false));
        assert_eq!(direct.begin_download(true, false), None);
        assert_eq!(
            direct.status(NOW).error.unwrap().code,
            codes::METERED_CONFIRMATION_REQUIRED
        );
        let spec = direct.begin_download(true, true).unwrap();
        assert_eq!(spec.file_name, "2.4.0-20400.apk");
        assert_eq!(spec.size_bytes, 52_428_800);
        assert_eq!(direct.status(NOW).state, UpdateState::Downloading);
        assert_eq!(direct.begin_download(true, true), None);

        let mut store = machine("play", verified(serde_json::json!({}), false));
        assert_eq!(store.begin_download(false, false), None);
        assert_eq!(store.status(NOW).error.unwrap().code, codes::NO_ARTIFACT);
    }

    #[test]
    fn a_download_moves_through_verifying_to_ready_and_can_fail_back() {
        let mut machine = machine("direct", verified(serde_json::json!({}), false));
        machine.begin_download(false, false).unwrap();
        machine.download_progress(1024, 52_428_800);
        assert_eq!(
            machine.status(NOW).progress,
            Some(UpdateProgress {
                received: 1024,
                total: 52_428_800
            })
        );
        machine.begin_verifying();
        assert_eq!(machine.status(NOW).state, UpdateState::Verifying);
        machine.download_ready(record(), NOW);
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::ReadyToInstall);
        assert_eq!(status.progress, None);
        assert_eq!(machine.persisted().artifact, Some(record()));
        assert_eq!(
            machine.begin_install(),
            Some(InstallStep::Package(record()))
        );
        machine.package_handed_over();
        assert_eq!(machine.status(NOW).state, UpdateState::Installing);
        machine.report_install(InstallOutcome::Cancelled, None);
        assert_eq!(machine.status(NOW).state, UpdateState::ReadyToInstall);

        machine.report_install(InstallOutcome::Failed, Some(codes::SIGNATURE_MISMATCH));
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::Failed);
        let error = status.error.unwrap();
        assert_eq!(error.code, codes::SIGNATURE_MISMATCH);
        assert!(!error.retryable);
        assert_eq!(
            error.fallback_url.as_deref(),
            Some("https://dl-a.example-cdn.com/acme/2.4.0-20400.apk")
        );
        assert_eq!(machine.take_discarded(), Some(record()));
        assert_eq!(machine.persisted().artifact, None);
    }

    #[test]
    fn halting_a_rollout_revokes_a_package_that_is_not_installed_yet() {
        let mut machine = machine("direct", verified(serde_json::json!({}), false));
        machine.begin_download(false, false).unwrap();
        machine.download_ready(record(), NOW);
        machine.finish_check(
            CheckResult::Manifest(verified(serde_json::json!({}), true)),
            NOW,
        );
        assert_eq!(machine.status(NOW).state, UpdateState::UpToDate);
        assert_eq!(machine.take_discarded(), Some(record()));
    }

    #[test]
    fn a_downloaded_package_survives_a_restart() {
        let mut first = machine("direct", verified(serde_json::json!({}), false));
        first.begin_download(false, false).unwrap();
        first.download_ready(record(), NOW);
        let persisted = first.persisted();

        let mut restarted = UpdateMachine::new(app("direct"));
        restarted.restore(
            "install-1".into(),
            persisted,
            Some(verified(serde_json::json!({}), false)),
            NOW,
        );
        assert_eq!(restarted.status(NOW).state, UpdateState::ReadyToInstall);
    }

    #[test]
    fn prompts_are_remembered_and_foregrounding_brings_a_required_one_back() {
        let mut machine = machine(
            "play",
            verified(serde_json::json!({ "minBuild": 20400 }), false),
        );
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Modal);
        machine.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        machine.defer(NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Badge);
        machine.foregrounded(NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Modal);
    }

    #[test]
    fn the_prompt_stays_up_through_download_install_and_failure() {
        let mut machine = machine("direct", verified(serde_json::json!({}), false));
        machine.mark_prompted(Some(20400), UpdatePrompt::Sheet, NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet);

        assert_eq!(machine.begin_download(true, false), None);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet, "带错误");
        machine.begin_download(true, true).unwrap();
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet);
        machine.download_failed(codes::DOWNLOAD_FAILED, true);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet);
        machine.begin_download(false, false).unwrap();
        machine.begin_verifying();
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet);
        machine.download_ready(record(), NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet);
        machine.begin_install().unwrap();
        machine.package_handed_over();
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Sheet);
        machine.report_install(InstallOutcome::Cancelled, None);
        assert_eq!(machine.status(NOW).state, UpdateState::ReadyToInstall);
        assert_eq!(
            machine.status(NOW).prompt,
            UpdatePrompt::Badge,
            "回到频率规则"
        );
    }

    #[test]
    fn a_flow_started_without_a_prompt_takes_the_form_of_its_class() {
        let mut optional = machine("direct", verified(serde_json::json!({}), false));
        optional.begin_download(false, false).unwrap();
        assert_eq!(optional.status(NOW).prompt, UpdatePrompt::Sheet);

        let mut recommended = machine(
            "direct",
            verified(serde_json::json!({ "recommendBelowBuild": 20400 }), false),
        );
        recommended.begin_download(false, false).unwrap();
        assert_eq!(recommended.status(NOW).prompt, UpdatePrompt::Modal);
    }

    #[test]
    fn later_during_a_download_hides_the_prompt_until_the_package_is_ready() {
        let mut machine = machine(
            "direct",
            verified(serde_json::json!({ "recommendBelowBuild": 20400 }), false),
        );
        machine.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        machine.begin_download(false, false).unwrap();
        machine.defer(NOW);
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::Downloading);
        assert_eq!(status.prompt, UpdatePrompt::None);
        assert_eq!(machine.prompts.deferred_until_ms, None, "不算推迟");
        machine.begin_verifying();
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::None);
        machine.download_ready(record(), NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Modal);
    }

    #[test]
    fn later_after_a_failure_ends_the_flow_and_defers() {
        let mut machine = machine(
            "direct",
            verified(serde_json::json!({ "recommendBelowBuild": 20400 }), false),
        );
        machine.begin_download(false, false).unwrap();
        machine.download_failed(codes::DOWNLOAD_FAILED, true);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Modal);
        machine.defer(NOW);
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::Available);
        assert_eq!(status.prompt, UpdatePrompt::Badge);
        assert_eq!(status.error, None);
    }

    #[test]
    fn a_blocking_update_stays_fullscreen_even_after_later() {
        let mut machine = machine(
            "direct",
            verified(
                serde_json::json!({ "minBuild": 20400, "blockUi": true }),
                false,
            ),
        );
        machine.begin_download(false, false).unwrap();
        machine.defer(NOW);
        assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Fullscreen);
    }

    #[test]
    fn a_package_found_after_a_restart_follows_the_frequency_rule() {
        let mut first = machine("direct", verified(serde_json::json!({}), false));
        first.mark_prompted(Some(20400), UpdatePrompt::Sheet, NOW);
        first.begin_download(false, false).unwrap();
        first.download_ready(record(), NOW);

        let mut restarted = UpdateMachine::new(app("direct"));
        restarted.restore(
            "install-1".into(),
            first.persisted(),
            Some(verified(serde_json::json!({}), false)),
            NOW,
        );
        assert_eq!(restarted.status(NOW).prompt, UpdatePrompt::Badge);
    }

    #[test]
    fn the_fallback_is_a_page_a_person_can_open() {
        let failed = |distribution: &str, app: UpdateApp, manifest| {
            let mut machine = UpdateMachine::new(UpdateApp {
                distribution: Some(distribution.into()),
                ..app
            });
            machine.restore("install-1".into(), PersistedUpdate::default(), None, NOW);
            machine.finish_check(CheckResult::Manifest(manifest), NOW);
            machine.report_install(InstallOutcome::Failed, None);
            machine.status(NOW).error.unwrap().fallback_url
        };
        let release = |artifacts: serde_json::Value| {
            let mut manifest = (*verified(serde_json::json!({}), false).manifest).clone();
            manifest.releases[0].artifacts = serde_json::from_value(artifacts).unwrap();
            let mut verified = verified(serde_json::json!({}), false);
            verified.manifest = Arc::new(manifest);
            verified
        };
        assert_eq!(
            failed(
                "direct",
                app("direct"),
                release(serde_json::json!({ "direct": {
                "kind": "apk",
                "urls": ["https://dl.example.com/a.apk"],
                "storeUrl": "https://www.example.com/download",
                "sizeBytes": 1,
                "sha256": DIGEST,
                "signingCertSha256": DIGEST
            } }))
            )
            .as_deref(),
            Some("https://www.example.com/download")
        );
        let web = UpdateApp {
            build: None,
            build_id: Some("bundle-1".into()),
            ..app("web")
        };
        assert_eq!(
            failed(
                "web",
                web,
                release(serde_json::json!({ "web": {
                "kind": "web_bundle",
                "buildId": "bundle-2",
                "urls": ["https://app.example.com/assets.json"],
                "storeUrl": "https://www.example.com/download",
                "sha256": DIGEST
            } }))
            ),
            None
        );
        let desktop = UpdateApp {
            target: Some("darwin-aarch64".into()),
            ..app("tauri")
        };
        assert_eq!(
            failed(
                "tauri",
                desktop,
                release(serde_json::json!({ "tauri": {
                "kind": "tauri",
                "targets": { "darwin-aarch64": {
                    "urls": ["https://dl.example.com/a.tar.gz"],
                    "sizeBytes": 1,
                    "sha256": DIGEST
                } }
            } }))
            )
            .as_deref(),
            Some("https://dl.example.com/a.tar.gz")
        );
    }

    #[test]
    fn checks_are_due_after_six_hours_or_a_clock_step_back() {
        assert!(check_due(None, NOW));
        assert!(!check_due(Some(NOW - CHECK_INTERVAL_MS), NOW));
        assert!(check_due(Some(NOW - CHECK_INTERVAL_MS - 1), NOW));
        assert!(check_due(Some(NOW + 1), NOW));
    }

    #[test]
    fn a_failed_manual_check_is_for_the_page_that_asked_and_opens_no_prompt() {
        for code in [codes::MANIFEST_UNAVAILABLE, codes::MANIFEST_REJECTED] {
            let mut machine = machine("direct", verified(serde_json::json!({}), false));
            machine.defer(NOW);
            assert_eq!(machine.status(NOW).prompt, UpdatePrompt::Badge);
            assert!(machine.request_check(true, NOW));
            assert_eq!(machine.status(NOW).prompt, UpdatePrompt::None);
            machine.finish_check(CheckResult::Failed { code }, NOW);
            let status = machine.status(NOW);
            assert_eq!(status.state, UpdateState::Available);
            assert_eq!(status.error.unwrap().code, code);
            assert_eq!(status.prompt, UpdatePrompt::Badge);
        }
    }

    #[test]
    fn a_check_that_is_not_due_leaves_the_state_alone() {
        let mut machine = machine("play", verified(serde_json::json!({}), false));
        assert!(!machine.request_check(false, NOW + 1));
        assert_eq!(machine.status(NOW + 1).state, UpdateState::Available);
        let due = NOW + CHECK_INTERVAL_MS + 1;
        assert!(machine.request_check(false, due));
        assert_eq!(machine.status(due).state, UpdateState::Checking);
    }

    #[test]
    fn a_request_during_a_round_joins_it() {
        let mut asked = UpdateMachine::new(app("play"));
        assert!(asked.request_check(true, NOW));
        assert!(!asked.request_check(true, NOW), "不重复起");
        assert_eq!(asked.status(NOW).state, UpdateState::Checking);
        asked.finish_check(CheckResult::NotPublished, NOW);
        assert_eq!(asked.status(NOW).state, UpdateState::UpToDate);

        let mut quiet = machine("play", verified(serde_json::json!({}), false));
        quiet.begin_background_check();
        assert_eq!(quiet.status(NOW + 1).state, UpdateState::Available);
        assert!(
            !quiet.request_check(false, NOW + 1),
            "没到时间，但这一轮已经在跑"
        );
        assert_eq!(quiet.status(NOW + 1).state, UpdateState::Checking);
        quiet.finish_check(
            CheckResult::Failed {
                code: codes::MANIFEST_UNAVAILABLE,
            },
            NOW + 1,
        );
        let status = quiet.status(NOW + 1);
        assert_eq!(status.state, UpdateState::Available);
        assert_eq!(status.error, None, "非手动不报错");
    }

    #[test]
    fn a_manual_request_makes_the_running_round_report_its_failure() {
        let mut machine = machine("play", verified(serde_json::json!({}), false));
        machine.begin_background_check();
        assert!(!machine.request_check(true, NOW + 1));
        assert!(machine.check_still_due(NOW + 1), "手动不受频率限制");
        machine.finish_check(
            CheckResult::Failed {
                code: codes::MANIFEST_UNAVAILABLE,
            },
            NOW + 1,
        );
        assert_eq!(
            machine.status(NOW + 1).error.unwrap().code,
            codes::MANIFEST_UNAVAILABLE
        );

        machine.begin_background_check();
        assert!(!machine.check_still_due(NOW + 1), "手动只算那一轮");
    }

    #[test]
    fn checking_lasts_until_the_last_round_ends() {
        let mut machine = machine("play", verified(serde_json::json!({}), false));
        machine.begin_background_check();
        machine.begin_background_check();
        assert!(!machine.request_check(true, NOW));
        machine.finish_check(CheckResult::Skipped, NOW);
        assert_eq!(machine.status(NOW).state, UpdateState::Checking);
        machine.finish_check(CheckResult::Skipped, NOW);
        assert_eq!(machine.status(NOW).state, UpdateState::Available);
    }

    #[test]
    fn a_round_that_asks_nobody_still_leaves_checking() {
        let build_less = || UpdateApp {
            build: None,
            ..app("play")
        };
        let mut manual = UpdateMachine::new(build_less());
        assert!(manual.request_check(true, NOW));
        assert_eq!(manual.status(NOW).state, UpdateState::Checking);
        manual.check_unavailable();
        let status = manual.status(NOW);
        assert_eq!(status.state, UpdateState::Idle);
        assert_eq!(status.error.unwrap().code, codes::MANIFEST_UNAVAILABLE);

        let mut silent = UpdateMachine::new(build_less());
        assert!(silent.request_check(false, NOW));
        silent.check_unavailable();
        let status = silent.status(NOW);
        assert_eq!(status.state, UpdateState::Idle);
        assert_eq!(status.error, None);

        // 判断时本地记录还没读进来：按没查过算，读进来才知道不用查。
        let mut early = UpdateMachine::new(app("play"));
        assert!(early.request_check(false, NOW));
        early.restore(
            "install-1".into(),
            PersistedUpdate {
                last_checked_at_ms: Some(NOW - 1),
                ..PersistedUpdate::default()
            },
            None,
            NOW,
        );
        assert!(!early.check_still_due(NOW));
        assert_eq!(early.status(NOW).state, UpdateState::Idle);
    }

    #[test]
    fn the_startup_check_reads_as_checking_until_it_ends() {
        let mut fresh = UpdateMachine::new(app("play"));
        fresh.set_startup_check_pending(true);
        assert_eq!(fresh.status(NOW).state, UpdateState::Checking, "还没查过");
        fresh.finish_check(CheckResult::NotPublished, NOW);
        assert_eq!(
            fresh.status(NOW).state,
            UpdateState::Checking,
            "已是最新也等它结束"
        );
        fresh.set_startup_check_pending(false);
        assert_eq!(fresh.status(NOW).state, UpdateState::UpToDate);

        let mut offered = UpdateMachine::new(app("play"));
        offered.set_startup_check_pending(true);
        offered.restore(
            "install-1".into(),
            PersistedUpdate::default(),
            Some(verified(serde_json::json!({}), false)),
            NOW,
        );
        assert_eq!(
            offered.status(NOW).state,
            UpdateState::Available,
            "手里有结论不用等"
        );
    }

    #[test]
    fn a_marked_prompt_keeps_its_form_until_it_is_answered() {
        let mut deferred = machine(
            "direct",
            verified(serde_json::json!({ "recommendBelowBuild": 20400 }), false),
        );
        assert_eq!(deferred.status(NOW).prompt, UpdatePrompt::Modal);
        deferred.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        assert_eq!(
            deferred.status(NOW).prompt,
            UpdatePrompt::Modal,
            "频率规则记了账也不消失"
        );
        deferred.defer(NOW);
        assert_eq!(deferred.status(NOW).prompt, UpdatePrompt::Badge);

        let mut skipped = machine("direct", verified(serde_json::json!({}), false));
        skipped.mark_prompted(Some(20400), UpdatePrompt::Sheet, NOW);
        assert_eq!(skipped.status(NOW).prompt, UpdatePrompt::Sheet);
        skipped.skip(20400);
        assert_eq!(skipped.status(NOW).prompt, UpdatePrompt::None);

        let mut superseded = machine("direct", verified(serde_json::json!({}), false));
        superseded.mark_prompted(Some(20400), UpdatePrompt::Sheet, NOW);
        superseded.finish_check(
            CheckResult::Manifest(verified(serde_json::json!({}), true)),
            NOW,
        );
        assert_eq!(
            superseded.status(NOW).prompt,
            UpdatePrompt::None,
            "目标版本没了"
        );
    }

    #[test]
    fn later_on_a_blocking_update_is_refused_and_nothing_goes_read_only() {
        let mut machine = machine(
            "play",
            verified(
                serde_json::json!({ "minBuild": 20400, "blockUi": true }),
                false,
            ),
        );
        machine.defer(NOW);
        let status = machine.status(NOW);
        assert_eq!(status.error.unwrap().code, codes::NOT_SKIPPABLE);
        assert!(!status.read_only);
        assert_eq!(status.prompt, UpdatePrompt::Fullscreen);
    }

    #[test]
    fn a_second_init_keeps_what_this_run_showed_and_the_chosen_language() {
        let required = || verified(serde_json::json!({ "minBuild": 20400 }), false);
        let reopened = |previous: &UpdateMachine| {
            let mut next = UpdateMachine::new(app("play"));
            next.adopt_session(previous);
            next.restore(
                "install-1".into(),
                PersistedUpdate::default(),
                Some(required()),
                NOW,
            );
            next
        };

        let mut answered = machine("play", required());
        answered.set_locale("en".into());
        answered.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        answered.defer(NOW);
        let status = reopened(&answered).status(NOW);
        assert_eq!(status.prompt, UpdatePrompt::Badge, "这次运行弹过了");
        assert!(status.read_only, "用户选了继续浏览");
        assert_eq!(status.release.unwrap().notes, vec!["Scheduled messages"]);

        let mut open = machine("play", required());
        open.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        let status = reopened(&open).status(NOW);
        assert_eq!(status.prompt, UpdatePrompt::Modal, "开着的提示不会掉");
        assert_eq!(
            status.release.unwrap().notes,
            vec!["新增消息定时发送"],
            "没选过语言时用配置里的"
        );
    }

    #[test]
    fn one_return_to_the_foreground_counts_once() {
        let mut machine = machine(
            "play",
            verified(serde_json::json!({ "minBuild": 20400 }), false),
        );
        machine.foregrounded(NOW);
        machine.mark_prompted(Some(20400), UpdatePrompt::Modal, NOW);
        machine.foregrounded(NOW + 300);
        machine.defer(NOW + 400);
        assert_eq!(
            machine.status(NOW + 400).prompt,
            UpdatePrompt::Badge,
            "晚到的那条路不算第二次"
        );

        let later = NOW + FOREGROUND_SETTLE_MS;
        machine.foregrounded(later);
        assert_eq!(
            machine.status(later).prompt,
            UpdatePrompt::Modal,
            "隔得够久是新的一次"
        );

        machine.mark_prompted(Some(20400), UpdatePrompt::Modal, later);
        machine.defer(later);
        machine.backgrounded();
        machine.foregrounded(later + 1);
        assert_eq!(
            machine.status(later + 1).prompt,
            UpdatePrompt::Modal,
            "进过后台就是新的一次"
        );
    }

    #[test]
    fn a_blocked_build_carries_its_reason_in_the_chosen_language() {
        let policy = serde_json::json!({
            "blockedBuilds": [{
                "from": 20300,
                "to": 20310,
                "reason": { "en": "Data loss", "zh-CN": "数据丢失" }
            }]
        });
        let mut machine = machine("play", verified(policy, false));
        let status = machine.status(NOW);
        assert_eq!(status.state, UpdateState::Required);
        assert_eq!(status.block_reason.as_deref(), Some("数据丢失"));
        assert_eq!(status.release.unwrap().notes, vec!["新增消息定时发送"]);

        machine.set_locale("en".into());
        let status = machine.status(NOW);
        assert_eq!(status.block_reason.as_deref(), Some("Data loss"));
        assert_eq!(status.release.unwrap().notes, vec!["Scheduled messages"]);
    }

    #[test]
    fn a_build_outside_the_blocked_ranges_has_no_reason() {
        let policy = serde_json::json!({
            "minBuild": 20400,
            "blockedBuilds": [{ "from": 20000, "to": 20100, "reason": { "en": "Data loss" } }]
        });
        let status = machine("play", verified(policy, false)).status(NOW);
        assert_eq!(status.state, UpdateState::Required);
        assert_eq!(status.block_reason, None);
    }

    #[test]
    fn the_status_serializes_with_the_wire_names() {
        let status = machine("direct", verified(serde_json::json!({}), false)).status(NOW);
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "available");
        assert_eq!(value["prompt"], "sheet");
        assert_eq!(value["blockUi"], false);
        assert_eq!(value["readOnly"], false);
        assert_eq!(value["lastCheckedAt"], NOW);
        assert_eq!(value["progress"], serde_json::Value::Null);
        assert_eq!(value["error"], serde_json::Value::Null);
        assert_eq!(value["blockReason"], serde_json::Value::Null);
        assert_eq!(value["current"]["channel"], "stable");
        assert_eq!(value["release"]["releaseId"], "rel_01JC8W");
        assert_eq!(value["release"]["sizeBytes"], 52_428_800);
        assert_eq!(value["release"]["kind"], "package");
    }

    #[test]
    fn configuration_decides_whether_checks_can_run() {
        assert!(app("play").can_check());
        let web = UpdateApp {
            platform: "web".into(),
            channel: "stable".into(),
            build_id: Some("20261105-abc".into()),
            ..Default::default()
        };
        assert!(web.can_check());
        assert!(!UpdateApp::default().can_check());
        let bad = UpdateApp {
            channel: "../x".into(),
            ..app("play")
        };
        assert!(!bad.can_check());
    }
}
