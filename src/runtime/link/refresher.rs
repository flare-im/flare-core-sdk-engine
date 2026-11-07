//! 清单刷新的调度：周期刷新、新版本通知、全部接入点失败时的恢复循环。
//!
//! 拉取本身（分层、验签、防回滚）在 `flare_access::BootstrapFetcher`，一次只跑一轮；这里决定下一轮何时
//! 开始。生命周期跟接入运行时走，不跟某次登录走：登录页上也要能换到新地址。链路状态机只告诉它
//! 「进入 / 离开恢复」「网络回来了」，采纳的结果经 [`AccessRuntime`](super::AccessRuntime) 的变更通道回去。

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flare_access::bootstrap::SourceAttempt;
use flare_access::{
    AccessKernel, BootstrapClock, BootstrapFetcher, DirectoryChange, RefreshMode, RefreshOutcome,
    RefreshReport,
};
use rand::Rng;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};

use super::access::{CurrentLink, now_ms};
use super::driver::{LinkFuture, boxed};
use crate::shared::util::{BackgroundTask, delay, spawn_background_task};

/// 清单过期后的刷新间隔（§4.3）。
pub const STALE_REFRESH_INTERVAL_MS: i64 = 600_000;
/// 恢复轮之间的退避：30 秒起，翻倍，上限 5 分钟（§4.4）。
pub const RECOVERING_BASE_MS: u64 = 30_000;
pub const RECOVERING_CAP_MS: u64 = 300_000;
/// 通知模式最多再来几轮：CDN 缓存 60 秒、过期后还可能先回一次旧值，四轮（两分钟）足够覆盖；
/// 之后交给周期刷新，不让一条错误的通知变成无限重试。
pub const NOTIFIED_MAX_RETRIES: u32 = 4;
/// 冷启动的第一次刷新在这个窗口里随机错开。
const COLD_START_SPREAD_MS: f64 = 5_000.0;
/// CONNECT_ACK 报的 serial 比本地新：大量客户端可能同时重连，稍微错开。
const CONNECT_ACK_SPREAD_MS: u64 = 5_000;

fn unit() -> f64 {
    rand::thread_rng().r#gen::<f64>()
}

/// 周期刷新的间隔：`refreshIntervalSec`，清单过期（或它的签名密钥被吊销）后 10 分钟，±10% 抖动。
pub(crate) fn refresh_interval_ms(interval_sec: u64, urgent: bool, unit: f64) -> i64 {
    let base = if urgent {
        STALE_REFRESH_INTERVAL_MS as f64
    } else {
        interval_sec as f64 * 1_000.0
    };
    (base * (0.9 + 0.2 * unit)) as i64
}

/// 下一次恢复退避。`previous` 为 0 表示这是第一次。±20% 抖动，全网一起恢复时不会对齐，但不超过上限。
pub(crate) fn next_recovering_delay(previous: u64, unit: f64) -> (u64, u64) {
    let delay = if previous == 0 {
        RECOVERING_BASE_MS
    } else {
        previous.saturating_mul(2).min(RECOVERING_CAP_MS)
    };
    let jittered = (delay as f64 * (0.8 + 0.4 * unit)) as u64;
    (delay, jittered.min(RECOVERING_CAP_MS))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NotifiedWait {
    serial: u64,
    at_ms: i64,
    retries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveringLoop {
    since_ms: i64,
    rounds: u32,
    /// 上一次退避的基准值（不含抖动）。
    delay_ms: u64,
    next_at_ms: i64,
    /// 设备没有网络：不再发起，等网络回来。
    suspended: bool,
}

/// 下一轮的时间表（纯数据，所有时刻由调用方给）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RefreshSchedule {
    background_at_ms: Option<i64>,
    notified: Option<NotifiedWait>,
    recovering: Option<RecoveringLoop>,
}

/// 一轮结束后调度需要知道的事实。
#[derive(Debug, Clone, Copy)]
pub(crate) struct RoundFacts {
    pub now_ms: i64,
    pub interval_sec: u64,
    /// 清单已过期，或它的签名密钥已被吊销。
    pub urgent: bool,
    pub unit: f64,
}

impl RefreshSchedule {
    /// 第一次周期刷新：上次刷新距今不足一个间隔就等够，否则在小窗口里随机错开后立即开始。
    pub fn start(&mut self, last_refresh_ms: Option<i64>, facts: RoundFacts) {
        let soon = facts.now_ms + (COLD_START_SPREAD_MS * facts.unit) as i64;
        let due = last_refresh_ms
            .map(|last| last + refresh_interval_ms(facts.interval_sec, facts.urgent, facts.unit));
        self.background_at_ms = Some(due.map_or(soon, |due| due.max(soon)));
    }

    /// 下一轮的时刻（恢复循环进行中时，其它两种都由它覆盖）。
    pub fn next_at(&self) -> Option<i64> {
        if let Some(recovering) = self.recovering {
            return (!recovering.suspended).then_some(recovering.next_at_ms);
        }
        [
            self.background_at_ms,
            self.notified.map(|notified| notified.at_ms),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// 现在该跑哪一种。
    pub fn due(&self, now_ms: i64) -> Option<RefreshMode> {
        if let Some(recovering) = self.recovering {
            return (!recovering.suspended && recovering.next_at_ms <= now_ms)
                .then_some(RefreshMode::Recovering);
        }
        if let Some(notified) = self.notified
            && notified.at_ms <= now_ms
        {
            return Some(RefreshMode::Notified(notified.serial));
        }
        self.background_at_ms
            .is_some_and(|at| at <= now_ms)
            .then_some(RefreshMode::Background)
    }

    /// 手动刷新用的模式：恢复期间就是恢复。
    pub fn manual_mode(&self) -> RefreshMode {
        if self.recovering.is_some() {
            RefreshMode::Recovering
        } else {
            RefreshMode::Background
        }
    }

    /// 全部接入点失败：立即开始第一轮。已在恢复中不打断它的退避。
    pub fn enter_recovering(&mut self, now_ms: i64) {
        self.recovering.get_or_insert(RecoveringLoop {
            since_ms: now_ms,
            rounds: 0,
            delay_ms: 0,
            next_at_ms: now_ms,
            suspended: false,
        });
    }

    /// 任何一路连上：退出。
    pub fn leave_recovering(&mut self) {
        self.recovering = None;
    }

    /// 网络变化、回到前台、用户点重试：立即一轮，退避重新开始。
    pub fn wake(&mut self, now_ms: i64) {
        if let Some(recovering) = self.recovering.as_mut() {
            recovering.suspended = false;
            recovering.delay_ms = 0;
            recovering.next_at_ms = now_ms;
        }
    }

    pub fn suspend(&mut self) {
        if let Some(recovering) = self.recovering.as_mut() {
            recovering.suspended = true;
        }
    }

    /// 尽快来一轮周期刷新（恢复链接导入后换回完整清单）。
    pub fn soon(&mut self, now_ms: i64) {
        self.background_at_ms = Some(now_ms);
    }

    /// 通知或 CONNECT_ACK 报了更新的 serial。合并到同一次等待里：取最大的 serial、最早的时刻。
    pub fn notify(&mut self, serial: u64, at_ms: i64) {
        self.notified = Some(match self.notified {
            Some(pending) => NotifiedWait {
                serial: pending.serial.max(serial),
                at_ms: pending.at_ms.min(at_ms),
                retries: if serial > pending.serial {
                    0
                } else {
                    pending.retries
                },
            },
            None => NotifiedWait {
                serial,
                at_ms,
                retries: 0,
            },
        });
    }

    /// 一轮结束：安排同类的下一轮。
    pub fn finished(&mut self, report: &RefreshReport, facts: RoundFacts) {
        match report.mode {
            RefreshMode::Recovering => {
                if let Some(recovering) = self.recovering.as_mut() {
                    recovering.rounds += 1;
                    // 拿到了新拓扑：之前的退避不再适用，这份新地址失败再从头数。
                    if report.outcome == RefreshOutcome::Adopted {
                        recovering.delay_ms = 0;
                    }
                    let (delay, wait) = next_recovering_delay(recovering.delay_ms, facts.unit);
                    recovering.delay_ms = delay;
                    recovering.next_at_ms = facts.now_ms + wait as i64;
                }
            }
            RefreshMode::Notified(_) => {
                if let (Some(pending), Some(retry_after_ms)) =
                    (self.notified.as_mut(), report.retry_after_ms)
                    && pending.retries < NOTIFIED_MAX_RETRIES
                {
                    pending.retries += 1;
                    pending.at_ms = facts.now_ms + retry_after_ms as i64;
                } else {
                    self.notified = None;
                }
            }
            RefreshMode::Background => {}
        }
        // 无论哪一种，拿到了通知要的版本就不用再等，周期也从这里重新算。
        if self
            .notified
            .is_some_and(|pending| report.serial >= pending.serial)
        {
            self.notified = None;
        }
        if report.outcome != RefreshOutcome::Failed || report.mode == RefreshMode::Background {
            self.background_at_ms = Some(
                facts.now_ms + refresh_interval_ms(facts.interval_sec, facts.urgent, facts.unit),
            );
        }
    }

    fn recovering_status(&self) -> Option<RecoveringStatus> {
        self.recovering.map(|recovering| RecoveringStatus {
            since_ms: recovering.since_ms,
            rounds: recovering.rounds,
            next_round_at_ms: (!recovering.suspended).then_some(recovering.next_at_ms),
        })
    }
}

/// `access.get_status.recovering`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveringStatus {
    pub since_ms: i64,
    /// 已跑过几轮引导拉取。
    pub rounds: u32,
    /// 下一轮时刻；设备没有网络时为 null。
    pub next_round_at_ms: Option<i64>,
}

/// `access.get_status.refresh` 与诊断导出。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshStatus {
    pub last_attempt_at_ms: Option<i64>,
    /// 最近一次有源给出可信结论的时刻。
    pub last_success_at_ms: Option<i64>,
    /// `adopted` / `upToDate` / `oldValues` / `failed`。
    pub last_outcome: Option<String>,
    pub next_at_ms: Option<i64>,
}

/// `access.refresh` 的一行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshAttemptDto {
    /// `https:<url>` / `wellKnown:<apId>` / `doh:<resolver>`。
    pub source: String,
    /// `mirror` / `wellKnown` / `pointer` / `resolver`。
    pub tier: String,
    /// `access` / `keys` / `pointer`。
    pub document: String,
    /// `adopted` / `notModified` / `sameSerial` / `olderSerial` / `rejected` / `failed` / `resolved`。
    pub result: String,
    pub code: Option<String>,
    pub serial: Option<u64>,
}

impl From<&SourceAttempt> for RefreshAttemptDto {
    fn from(attempt: &SourceAttempt) -> Self {
        Self {
            source: attempt.source.clone(),
            tier: attempt.tier.as_str().to_owned(),
            document: attempt.document.to_owned(),
            result: attempt.result.as_str().to_owned(),
            code: attempt.code.clone(),
            serial: attempt.serial,
        }
    }
}

/// `access.refresh` 的返回值。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshReportDto {
    /// `adopted` / `upToDate` / `oldValues` / `failed` / `skipped`。
    pub outcome: String,
    /// `background` / `notified` / `recovering`。
    pub mode: String,
    /// 这一轮结束时的 serial。
    pub serial: u64,
    /// 采纳的清单来自哪个源。
    pub source: Option<String>,
    pub keystate_serial: Option<u64>,
    pub attempts: Vec<RefreshAttemptDto>,
}

impl RefreshReportDto {
    pub(crate) fn new(report: &RefreshReport, keystate_serial: Option<u64>) -> Self {
        Self {
            outcome: report.outcome.as_str().to_owned(),
            mode: report.mode.as_str().to_owned(),
            serial: report.serial,
            source: report
                .adoptions
                .last()
                .map(|adoption| adoption.source.label()),
            keystate_serial,
            attempts: report.attempts.iter().map(Into::into).collect(),
        }
    }

    /// 开发模式：端点由配置决定，没有什么可刷新。
    pub(crate) fn skipped(serial: u64) -> Self {
        Self {
            outcome: RefreshOutcome::Skipped.as_str().to_owned(),
            mode: RefreshMode::Background.as_str().to_owned(),
            serial,
            source: None,
            keystate_serial: None,
            attempts: Vec::new(),
        }
    }
}

pub(crate) enum RefreshCommand {
    /// `access.refresh`：立即一轮（已有一轮在跑就等它的结果）。
    Manual(oneshot::Sender<RefreshReport>),
    Notified {
        serial: u64,
        delay_ms: u64,
    },
    Recovering(bool),
    Suspend,
    Wake,
    Soon,
}

struct LinkClock;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BootstrapClock for LinkClock {
    fn now_ms(&self) -> i64 {
        now_ms()
    }

    async fn sleep(&self, ms: u64) {
        delay(Duration::from_millis(ms)).await;
    }
}

/// 命令队列上限。刷新任务一直在取，这个数只防它卡住时无限堆积。
const COMMAND_CAPACITY: usize = 64;

/// 刷新任务的句柄：释放时任务结束。
pub(crate) struct Refresher {
    commands: mpsc::Sender<RefreshCommand>,
    shared: Arc<Mutex<SharedStatus>>,
    task: BackgroundTask,
}

impl Drop for Refresher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug, Clone, Default)]
struct SharedStatus {
    refresh: RefreshStatus,
    recovering: Option<RecoveringStatus>,
}

fn lock(shared: &Mutex<SharedStatus>) -> std::sync::MutexGuard<'_, SharedStatus> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Refresher {
    pub fn spawn(
        kernel: Arc<AccessKernel>,
        http: Arc<dyn flare_access::HttpFetch>,
        current: Arc<Mutex<Option<CurrentLink>>>,
        changes: broadcast::Sender<DirectoryChange>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(COMMAND_CAPACITY);
        let shared = Arc::new(Mutex::new(SharedStatus::default()));
        let fetcher = Arc::new(BootstrapFetcher::new(
            kernel.clone(),
            http,
            Arc::new(LinkClock),
        ));
        let worker = Worker {
            kernel,
            fetcher,
            current,
            changes,
            commands: rx,
            shared: shared.clone(),
            schedule: RefreshSchedule::default(),
            running: None,
            waiters: Vec::new(),
            manual_pending: false,
        };
        Self {
            commands: tx,
            shared,
            task: spawn_background_task(worker.run()),
        }
    }

    /// 队列满时丢弃这条：定时轮次会补上，手动刷新的调用方收到任务已停止的错误。
    pub fn send(&self, command: RefreshCommand) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.commands.try_send(command) {
            tracing::warn!("the access refresh queue is full, dropping a command");
        }
    }

    pub fn notify_serial(&self, serial: u64, delay_ms: u64) {
        self.send(RefreshCommand::Notified { serial, delay_ms });
    }

    /// CONNECT_ACK 报的 serial 比本地新。
    pub fn notify_ack_serial(&self, serial: u64) {
        let delay_ms = (CONNECT_ACK_SPREAD_MS as f64 * unit()) as u64;
        self.send(RefreshCommand::Notified { serial, delay_ms });
    }

    pub fn refresh_status(&self) -> RefreshStatus {
        lock(&self.shared).refresh.clone()
    }

    pub fn recovering_status(&self) -> Option<RecoveringStatus> {
        lock(&self.shared).recovering
    }
}

struct Worker {
    kernel: Arc<AccessKernel>,
    fetcher: Arc<BootstrapFetcher>,
    current: Arc<Mutex<Option<CurrentLink>>>,
    changes: broadcast::Sender<DirectoryChange>,
    commands: mpsc::Receiver<RefreshCommand>,
    shared: Arc<Mutex<SharedStatus>>,
    schedule: RefreshSchedule,
    running: Option<LinkFuture<RefreshReport>>,
    waiters: Vec<oneshot::Sender<RefreshReport>>,
    manual_pending: bool,
}

async fn poll_running(slot: &mut Option<LinkFuture<RefreshReport>>) -> RefreshReport {
    match slot.as_mut() {
        Some(future) => {
            let report = future.await;
            *slot = None;
            report
        }
        None => std::future::pending().await,
    }
}

impl Worker {
    fn facts(&self) -> RoundFacts {
        let now = now_ms();
        let snapshot = self.kernel.directory().snapshot();
        RoundFacts {
            now_ms: now,
            interval_sec: snapshot.manifest().refresh_interval_sec(),
            urgent: snapshot.is_stale(now) || self.kernel.current_manifest_revoked(),
            unit: unit(),
        }
    }

    async fn run(mut self) {
        let last_refresh_ms = self.fetcher.last_refresh_ms().await;
        let facts = self.facts();
        self.schedule.start(last_refresh_ms, facts);
        self.publish();
        loop {
            if self.running.is_none() {
                self.start_due();
            }
            let wait = if self.running.is_some() {
                None
            } else {
                self.schedule.next_at()
            };
            let timer: LinkFuture<()> = match wait {
                Some(at) => boxed(delay(Duration::from_millis((at - now_ms()).max(0) as u64))),
                None => boxed(std::future::pending()),
            };
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) => self.on_command(command),
                    None => break,
                },
                report = poll_running(&mut self.running) => self.on_report(report),
                () = timer => {}
            }
        }
    }

    fn start_due(&mut self) {
        let mode = if self.manual_pending {
            self.manual_pending = false;
            Some(self.schedule.manual_mode())
        } else {
            self.schedule.due(now_ms())
        };
        let Some(mode) = mode else {
            return;
        };
        let fetcher = self.fetcher.clone();
        let current = self
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|link| link.access_point_id.to_string());
        self.running = Some(boxed(async move {
            fetcher.refresh(mode, current.as_deref()).await
        }));
    }

    fn on_command(&mut self, command: RefreshCommand) {
        let now = now_ms();
        match command {
            RefreshCommand::Manual(reply) => {
                self.waiters.push(reply);
                if self.running.is_none() {
                    self.manual_pending = true;
                }
            }
            RefreshCommand::Notified { serial, delay_ms } => {
                if serial > self.kernel.directory().snapshot().serial() {
                    self.schedule.notify(serial, now + delay_ms as i64);
                }
            }
            RefreshCommand::Recovering(true) => self.schedule.enter_recovering(now),
            RefreshCommand::Recovering(false) => self.schedule.leave_recovering(),
            RefreshCommand::Suspend => self.schedule.suspend(),
            RefreshCommand::Wake => self.schedule.wake(now),
            RefreshCommand::Soon => self.schedule.soon(now),
        }
        self.publish();
    }

    fn on_report(&mut self, report: RefreshReport) {
        tracing::info!(
            mode = report.mode.as_str(),
            outcome = report.outcome.as_str(),
            serial = report.serial,
            attempts = report.attempts.len(),
            "access refresh finished"
        );
        for attempt in report
            .attempts
            .iter()
            .filter(|attempt| attempt.code.is_some())
        {
            tracing::debug!(
                source = %attempt.source,
                document = attempt.document,
                result = attempt.result.as_str(),
                code = ?attempt.code,
                "access source did not answer"
            );
        }
        for adoption in &report.adoptions {
            tracing::info!(
                from = adoption.change.from_serial,
                to = adoption.change.to_serial,
                source = %adoption.source.label(),
                persisted = adoption.persisted,
                "access manifest adopted"
            );
            let _ = self.changes.send(adoption.change.clone());
        }
        let facts = self.facts();
        self.schedule.finished(&report, facts);
        {
            let mut shared = lock(&self.shared);
            shared.refresh.last_attempt_at_ms = Some(facts.now_ms);
            shared.refresh.last_outcome = Some(report.outcome.as_str().to_owned());
            if report.outcome != RefreshOutcome::Failed {
                shared.refresh.last_success_at_ms = Some(facts.now_ms);
            }
        }
        for waiter in self.waiters.drain(..) {
            let _ = waiter.send(report.clone());
        }
        self.publish();
    }

    fn publish(&self) {
        let mut shared = lock(&self.shared);
        shared.refresh.next_at_ms = self.schedule.next_at();
        shared.recovering = self.schedule.recovering_status();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_000_000_000;

    fn facts(now_ms: i64) -> RoundFacts {
        RoundFacts {
            now_ms,
            interval_sec: 21_600,
            urgent: false,
            unit: 0.5,
        }
    }

    fn report(mode: RefreshMode, outcome: RefreshOutcome, serial: u64) -> RefreshReport {
        RefreshReport {
            mode,
            outcome,
            serial,
            adoptions: Vec::new(),
            keystate: None,
            attempts: Vec::new(),
            retry_after_ms: None,
        }
    }

    #[test]
    fn periodic_refresh_follows_the_manifest_interval_with_jitter() {
        assert_eq!(refresh_interval_ms(21_600, false, 0.5), 21_600_000);
        assert_eq!(refresh_interval_ms(21_600, false, 0.0), 19_440_000);
        assert_eq!(refresh_interval_ms(21_600, false, 1.0), 23_760_000);
        assert_eq!(refresh_interval_ms(21_600, true, 0.5), 600_000);

        let mut schedule = RefreshSchedule::default();
        // 刚刷新过：等够一个间隔。
        schedule.start(Some(NOW - 1_000), facts(NOW));
        assert_eq!(schedule.next_at(), Some(NOW - 1_000 + 21_600_000));
        assert_eq!(schedule.due(NOW), None);
        // 从没刷新过、或很久没刷新：几秒内就来一轮。
        schedule.start(None, facts(NOW));
        assert_eq!(schedule.next_at(), Some(NOW + 2_500));
        schedule.start(Some(NOW - 90_000_000), facts(NOW));
        assert_eq!(schedule.next_at(), Some(NOW + 2_500));
        assert_eq!(schedule.due(NOW + 2_500), Some(RefreshMode::Background));

        schedule.finished(
            &report(RefreshMode::Background, RefreshOutcome::UpToDate, 5),
            facts(NOW + 3_000),
        );
        assert_eq!(schedule.next_at(), Some(NOW + 3_000 + 21_600_000));

        // 清单过期：提到 10 分钟。
        schedule.finished(
            &report(RefreshMode::Background, RefreshOutcome::Failed, 5),
            RoundFacts {
                urgent: true,
                ..facts(NOW + 4_000)
            },
        );
        assert_eq!(schedule.next_at(), Some(NOW + 4_000 + 600_000));
    }

    /// 恢复：30 秒起、翻倍、上限 5 分钟，抖动不越过上限。
    #[test]
    fn recovering_backs_off_from_thirty_seconds_up_to_five_minutes() {
        let mut schedule = RefreshSchedule::default();
        schedule.start(None, facts(NOW));
        schedule.enter_recovering(NOW);
        assert_eq!(schedule.due(NOW), Some(RefreshMode::Recovering));

        let failed = report(RefreshMode::Recovering, RefreshOutcome::Failed, 5);
        let mut now = NOW;
        let mut waits = Vec::new();
        for _ in 0..7 {
            schedule.finished(&failed, facts(now));
            let next = schedule.next_at().unwrap();
            waits.push(next - now);
            assert_eq!(schedule.due(now), None);
            assert_eq!(schedule.due(next), Some(RefreshMode::Recovering));
            now = next;
        }
        assert_eq!(
            waits,
            [30_000, 60_000, 120_000, 240_000, 300_000, 300_000, 300_000]
        );
        assert_eq!(schedule.recovering_status().unwrap().rounds, 7);
        assert_eq!(schedule.recovering_status().unwrap().since_ms, NOW);

        for (unit, expected) in [(0.0, 24_000), (0.999_999, 35_999)] {
            assert_eq!(next_recovering_delay(0, unit).1, expected);
        }
        assert_eq!(next_recovering_delay(300_000, 0.999).1, RECOVERING_CAP_MS);

        // 再次进入不打断退避。
        let before = schedule.next_at();
        schedule.enter_recovering(now + 1);
        assert_eq!(schedule.next_at(), before);

        // 网络变化、回前台：立即一轮，退避重新开始。
        schedule.wake(now + 5);
        assert_eq!(schedule.due(now + 5), Some(RefreshMode::Recovering));
        schedule.finished(&failed, facts(now + 5));
        assert_eq!(schedule.next_at(), Some(now + 5 + 30_000));

        // 没有网络：暂停。网络回来才继续。
        schedule.suspend();
        assert_eq!(schedule.next_at(), None);
        assert_eq!(schedule.due(now + 999_999), None);
        assert_eq!(schedule.recovering_status().unwrap().next_round_at_ms, None);
        schedule.wake(now + 1_000_000);
        assert_eq!(schedule.due(now + 1_000_000), Some(RefreshMode::Recovering));

        // 连上了：退出，回到周期刷新。
        schedule.leave_recovering();
        assert!(schedule.recovering_status().is_none());
        assert_eq!(schedule.manual_mode(), RefreshMode::Background);
        assert!(
            schedule
                .due(now + 1_000_000)
                .is_some_and(|mode| mode == RefreshMode::Background)
        );
    }

    #[test]
    fn a_new_topology_restarts_the_recovering_backoff() {
        let mut schedule = RefreshSchedule::default();
        schedule.enter_recovering(NOW);
        let failed = report(RefreshMode::Recovering, RefreshOutcome::Failed, 5);
        schedule.finished(&failed, facts(NOW));
        schedule.finished(&failed, facts(NOW));
        schedule.finished(
            &report(RefreshMode::Recovering, RefreshOutcome::Adopted, 6),
            facts(NOW),
        );
        assert_eq!(schedule.next_at(), Some(NOW + 30_000));
    }

    #[test]
    fn notified_retries_every_thirty_seconds_then_gives_up_to_the_period() {
        let mut schedule = RefreshSchedule::default();
        schedule.start(Some(NOW), facts(NOW));
        schedule.notify(12, NOW + 700);
        schedule.notify(11, NOW + 900);
        assert_eq!(schedule.next_at(), Some(NOW + 700));
        assert_eq!(schedule.due(NOW + 700), Some(RefreshMode::Notified(12)));

        let mut old = report(RefreshMode::Notified(12), RefreshOutcome::OldValues, 10);
        old.retry_after_ms = Some(30_000);
        let mut now = NOW + 700;
        for _ in 0..NOTIFIED_MAX_RETRIES {
            schedule.finished(&old, facts(now));
            assert_eq!(schedule.next_at(), Some(now + 30_000));
            now += 30_000;
        }
        schedule.finished(&old, facts(now));
        assert_eq!(schedule.notified, None);

        // 传播到了：不再等。
        schedule.notify(13, now);
        let adopted = report(RefreshMode::Notified(13), RefreshOutcome::Adopted, 13);
        schedule.finished(&adopted, facts(now));
        assert_eq!(schedule.notified, None);

        // 恢复循环拿到了通知要的版本，同样不用再等。
        schedule.notify(14, now);
        schedule.enter_recovering(now);
        assert_eq!(schedule.due(now), Some(RefreshMode::Recovering));
        schedule.finished(
            &report(RefreshMode::Recovering, RefreshOutcome::Adopted, 14),
            facts(now),
        );
        assert_eq!(schedule.notified, None);
    }

    #[test]
    fn a_newer_notice_restarts_the_retries() {
        let mut schedule = RefreshSchedule::default();
        schedule.notify(12, NOW);
        let mut old = report(RefreshMode::Notified(12), RefreshOutcome::OldValues, 10);
        old.retry_after_ms = Some(30_000);
        schedule.finished(&old, facts(NOW));
        schedule.notify(13, NOW + 10);
        assert_eq!(schedule.notified.unwrap().retries, 0);
        assert_eq!(schedule.notified.unwrap().serial, 13);
        assert_eq!(schedule.notified.unwrap().at_ms, NOW + 10);
    }
}
