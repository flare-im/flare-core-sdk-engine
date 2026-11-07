//! 分层引导拉取（§4.4）：T2 镜像 → T3 接入点 well-known → T4 DoH TXT 指针。
//!
//! 一次 [`BootstrapFetcher::refresh`] 只跑一轮：何时再来（周期、通知传播、恢复退避）由引擎决定。
//! 文档来自不可信的网络，所以这里每一步都按「源可能在撒谎」处理：验签与防回滚在
//! [`AccessKernel`] 里完成，失败只丢弃文档、记这个源一笔，绝不影响已有 LKG。

use std::collections::{HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;

use futures_util::future::{Either, join, select};
use futures_util::lock::Mutex;
use futures_util::stream::{FuturesUnordered, StreamExt};

use super::doh;
use super::sources::SourceBook;
use super::{FetchError, FetchRequest, FetchResponse, HttpFetch};
use crate::directory::{DirectoryChange, DirectorySnapshot};
use crate::kernel::{AccessKernel, KeystateUpdate, ManifestUpdate};
use crate::manifest::{BootstrapSource, MAX_DOCUMENT_BYTES};
use crate::platform::{BoxedFuture, MaybeSendSync};
use crate::store::{AcceptError, FetchMeta, ManifestSource, keys};
use crate::trust::{TrustError, TrustFailureKind};
use crate::types::ApStatus;

pub use super::sources::DEMOTION_MS;

/// 每个镜像的超时。
pub const MIRROR_TIMEOUT_MS: u32 = 5_000;
/// 每个接入点 well-known 的超时。
pub const WELL_KNOWN_TIMEOUT_MS: u32 = 4_000;
/// 每个 DoH 解析器的超时。
pub const DOH_TIMEOUT_MS: u32 = 3_000;
/// 交错并发：每隔这么久启动下一个源。
pub const STAGGER_MS: u64 = 400;
/// 同时在途的源数上限。
pub const MAX_IN_FLIGHT: usize = 3;
/// 通知说有新版本、但所有源都还是旧值（CDN 传播延迟）时，多久后再来一轮。
pub const NOTIFIED_RETRY_DELAY_MS: u64 = 30_000;
/// 一轮最多查几个 DoH 解析器。
const MAX_DOH_LOOKUPS: usize = 16;

const ACCESS_FILE: &str = "access.dsse.json";
const KEYS_FILE: &str = "keys.dsse.json";

/// 时钟与定时器：本 crate 不读系统时钟，也不依赖任何异步运行时。
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait BootstrapClock: MaybeSendSync {
    fn now_ms(&self) -> i64;

    async fn sleep(&self, ms: u64);
}

/// 拉取模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    /// 周期刷新：任一源确认「无更新」即结束。
    Background,
    /// 收到新版本通知：拿到不小于这个 serial 的文档才算完。
    Notified(u64),
    /// 全部接入点失败：试遍所有层，直到拿到更新的文档。
    Recovering,
}

impl RefreshMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RefreshMode::Background => "background",
            RefreshMode::Notified(_) => "notified",
            RefreshMode::Recovering => "recovering",
        }
    }
}

/// 源所在的层。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceTier {
    /// 清单列出的 https 镜像（T2）。
    Mirror,
    /// 接入点上的 `/.well-known/flare/v1/…`（T3）。
    WellKnown,
    /// DoH TXT 指针解析出的镜像（T4）。
    Pointer,
    /// DoH 解析器本身（T4）。
    Resolver,
}

impl SourceTier {
    pub fn as_str(self) -> &'static str {
        match self {
            SourceTier::Mirror => "mirror",
            SourceTier::WellKnown => "wellKnown",
            SourceTier::Pointer => "pointer",
            SourceTier::Resolver => "resolver",
        }
    }
}

/// 一个源在这一轮里的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptResult {
    /// 文档已采纳。
    Adopted,
    /// 304：与上次相同。
    NotModified,
    /// 200，但 serial 与本机相同。
    SameSerial,
    /// serial 比本机已接受的小：丢弃，该源降权。
    OlderSerial,
    /// 验签或结构校验失败：丢弃。
    Rejected,
    /// 没拿到文档（连不上、超时、非 200）。
    Failed,
    /// DoH 解析器答复了指针。
    Resolved,
}

impl AttemptResult {
    pub fn as_str(self) -> &'static str {
        match self {
            AttemptResult::Adopted => "adopted",
            AttemptResult::NotModified => "notModified",
            AttemptResult::SameSerial => "sameSerial",
            AttemptResult::OlderSerial => "olderSerial",
            AttemptResult::Rejected => "rejected",
            AttemptResult::Failed => "failed",
            AttemptResult::Resolved => "resolved",
        }
    }
}

/// 诊断用的一条记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceAttempt {
    /// `https:<url>` / `wellKnown:<apId>` / `doh:<resolver>`。
    pub source: String,
    pub tier: SourceTier,
    /// `access` / `keys` / `pointer`。
    pub document: &'static str,
    pub result: AttemptResult,
    /// 跨端稳定的错误码（`TrustError::code`、`FetchError::code`、`http.<status>`）。
    pub code: Option<String>,
    pub serial: Option<u64>,
}

/// 采纳了一份新清单。
#[derive(Debug, Clone, PartialEq)]
pub struct Adoption {
    pub change: DirectoryChange,
    pub source: ManifestSource,
    pub stale: bool,
    pub persisted: bool,
}

/// 一轮的结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// 采纳了新清单。
    Adopted,
    /// 有源确认没有更新。
    UpToDate,
    /// 通知模式：能拿到的都比通知的 serial 旧（传播延迟），`retry_after_ms` 后再来。
    OldValues,
    /// 没有任何源给出可信的结果。
    Failed,
    /// 不适用（开发模式的端点由配置决定）。
    Skipped,
}

impl RefreshOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RefreshOutcome::Adopted => "adopted",
            RefreshOutcome::UpToDate => "upToDate",
            RefreshOutcome::OldValues => "oldValues",
            RefreshOutcome::Failed => "failed",
            RefreshOutcome::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RefreshReport {
    pub mode: RefreshMode,
    pub outcome: RefreshOutcome,
    /// 这一轮结束时的目录 serial。
    pub serial: u64,
    /// 按采纳顺序（通知模式可能先后采纳两份）。
    pub adoptions: Vec<Adoption>,
    pub keystate: Option<KeystateUpdate>,
    pub attempts: Vec<SourceAttempt>,
    /// 引擎应在这么久后再来一轮（通知模式下还没等到目标 serial）。
    pub retry_after_ms: Option<u64>,
}

impl RefreshReport {
    fn finished(mode: RefreshMode, outcome: RefreshOutcome, serial: u64) -> Self {
        Self {
            mode,
            outcome,
            serial,
            adoptions: Vec::new(),
            keystate: None,
            attempts: Vec::new(),
            retry_after_ms: None,
        }
    }
}

/// 一个要拉取的源。
#[derive(Debug, Clone)]
struct SourceCandidate {
    key: String,
    tier: SourceTier,
    /// 不带结尾 `/` 的基址。
    base_url: String,
    source: ManifestSource,
    ip_hints: Vec<IpAddr>,
    spki_pins: Vec<String>,
    timeout_ms: u32,
}

impl SourceCandidate {
    fn mirror(url: &str, tier: SourceTier, ip_hints: Vec<IpAddr>) -> Self {
        let source = ManifestSource::Https {
            url: url.to_owned(),
        };
        Self {
            key: source.label(),
            tier,
            base_url: url.trim_end_matches('/').to_owned(),
            source,
            ip_hints,
            spki_pins: Vec::new(),
            timeout_ms: MIRROR_TIMEOUT_MS,
        }
    }

    fn request(&self, file: &str, if_none_match: Option<String>) -> FetchRequest {
        FetchRequest {
            url: format!("{}/{file}", self.base_url),
            if_none_match,
            accept: None,
            timeout_ms: self.timeout_ms,
            max_body_bytes: MAX_DOCUMENT_BYTES,
            ip_hints: self.ip_hints.clone(),
            spki_pins: self.spki_pins.clone(),
        }
    }
}

/// 一个源的两份文档。
struct Fetched {
    candidate: SourceCandidate,
    keys: Result<FetchResponse, FetchError>,
    access: Result<FetchResponse, FetchError>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Done,
}

/// 一轮的进度。
struct Round {
    mode: RefreshMode,
    tried: HashSet<String>,
    /// 给出过可信结论（文档或 304）的源数。
    answered: usize,
    adoptions: Vec<Adoption>,
    keystate: Option<KeystateUpdate>,
    attempts: Vec<SourceAttempt>,
}

impl Round {
    fn record(
        &mut self,
        candidate: &SourceCandidate,
        document: &'static str,
        result: AttemptResult,
        code: Option<String>,
        serial: Option<u64>,
    ) {
        self.attempts.push(SourceAttempt {
            source: candidate.key.clone(),
            tier: candidate.tier,
            document,
            result,
            code,
            serial,
        });
    }
}

/// 这类失败说明源在提供不可信内容（被劫持、配错），降权；过期、吊销更可能是 CDN 还留着旧对象，
/// 新版本 schema 是客户端太旧，都不是源的错。
fn is_suspicious(error: &TrustError) -> bool {
    !matches!(
        error.kind(),
        TrustFailureKind::Expired
            | TrustFailureKind::Revoked
            | TrustFailureKind::UnsupportedSchema
            | TrustFailureKind::Config
    )
}

pub struct BootstrapFetcher {
    kernel: Arc<AccessKernel>,
    http: Arc<dyn HttpFetch>,
    clock: Arc<dyn BootstrapClock>,
    /// 第一轮时才从存储读入。锁跨整轮：同一时刻只有一轮在跑。
    book: Mutex<Option<SourceBook>>,
}

impl BootstrapFetcher {
    pub fn new(
        kernel: Arc<AccessKernel>,
        http: Arc<dyn HttpFetch>,
        clock: Arc<dyn BootstrapClock>,
    ) -> Self {
        Self {
            kernel,
            http,
            clock,
            book: Mutex::new(None),
        }
    }

    async fn load_book(&self) -> SourceBook {
        let key = keys::bootstrap_sources(self.kernel.bootstrap_id());
        match self.kernel.store().get(&key).await {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap_or_default(),
            _ => SourceBook::default(),
        }
    }

    /// 上一轮的时刻（跨重启）：引擎据此决定冷启动后要不要马上刷新。
    pub async fn last_refresh_ms(&self) -> Option<i64> {
        let mut book = self.book.lock().await;
        if book.is_none() {
            *book = Some(self.load_book().await);
        }
        book.as_ref().and_then(SourceBook::last_refresh_ms)
    }

    /// 跑一轮。`current_ap` 是 IM 当前接入点（T3 优先问它）。
    pub async fn refresh(&self, mode: RefreshMode, current_ap: Option<&str>) -> RefreshReport {
        let snapshot = self.kernel.directory().snapshot();
        if self.kernel.is_development() {
            return RefreshReport::finished(mode, RefreshOutcome::Skipped, snapshot.serial());
        }
        if self.reached_target(mode) {
            return RefreshReport::finished(mode, RefreshOutcome::UpToDate, snapshot.serial());
        }
        let mut book_slot = self.book.lock().await;
        let mut book = match book_slot.take() {
            Some(book) => book,
            None => self.load_book().await,
        };
        let mut round = Round {
            mode,
            tried: HashSet::new(),
            answered: 0,
            adoptions: Vec::new(),
            keystate: None,
            attempts: Vec::new(),
        };
        let candidates = self.candidates(&snapshot, mode, current_ap, &book);
        let flow = self.run_sources(candidates, &mut round, &mut book).await;
        // T4 是最后的手段：T2/T3 全部失败才去问 DNS。
        if flow == Flow::Continue && mode == RefreshMode::Recovering && round.answered == 0 {
            let pointers = self.resolve_pointers(&snapshot, &mut round).await;
            self.run_sources(pointers, &mut round, &mut book).await;
        }
        book.touch(self.clock.now_ms());
        self.persist(&book).await;
        *book_slot = Some(book);
        drop(book_slot);
        self.conclude(round)
    }

    /// 通知模式：本机已不低于通知的 serial，无需再拉。
    fn reached_target(&self, mode: RefreshMode) -> bool {
        match mode {
            RefreshMode::Notified(serial) => self.kernel.directory().snapshot().serial() >= serial,
            _ => false,
        }
    }

    fn conclude(&self, round: Round) -> RefreshReport {
        let serial = self.kernel.directory().snapshot().serial();
        let reached = self.reached_target(round.mode);
        let (outcome, retry_after_ms) = match round.mode {
            RefreshMode::Notified(_) if !reached => {
                let outcome = if !round.adoptions.is_empty() {
                    RefreshOutcome::Adopted
                } else if round.answered > 0 {
                    RefreshOutcome::OldValues
                } else {
                    RefreshOutcome::Failed
                };
                (outcome, Some(NOTIFIED_RETRY_DELAY_MS))
            }
            _ if !round.adoptions.is_empty() => (RefreshOutcome::Adopted, None),
            _ if round.answered > 0 => (RefreshOutcome::UpToDate, None),
            _ => (RefreshOutcome::Failed, None),
        };
        RefreshReport {
            mode: round.mode,
            outcome,
            serial,
            adoptions: round.adoptions,
            keystate: round.keystate,
            attempts: round.attempts,
            retry_after_ms,
        }
    }

    async fn persist(&self, book: &SourceBook) {
        // 写失败只影响下次的排序与条件请求，不影响这一轮的结果。
        if let Ok(bytes) = serde_json::to_vec(book) {
            let key = keys::bootstrap_sources(self.kernel.bootstrap_id());
            let _ = self.kernel.store().put(&key, &bytes).await;
        }
    }

    /// T2（上次成功的排前）++ T3（当前接入点优先）；被降权的放到最后。恢复时追加种子里的引导源。
    fn candidates(
        &self,
        snapshot: &DirectorySnapshot,
        mode: RefreshMode,
        current_ap: Option<&str>,
        book: &SourceBook,
    ) -> Vec<SourceCandidate> {
        let now = self.clock.now_ms();
        let hints: &[BootstrapSource] = if mode == RefreshMode::Recovering {
            self.kernel.seed_sources()
        } else {
            &[]
        };
        let listed = snapshot.manifest().bootstrap.sources.iter().chain(hints);
        let mut mirrors: Vec<SourceCandidate> = Vec::new();
        let mut well_known = false;
        for source in listed {
            match (source.kind.as_str(), source.url.as_deref()) {
                ("https", Some(url)) => {
                    let ip_hints = source
                        .ip_hints
                        .iter()
                        .filter_map(|hint| hint.parse().ok())
                        .collect();
                    let candidate = SourceCandidate::mirror(url, SourceTier::Mirror, ip_hints);
                    if !mirrors.iter().any(|known| known.key == candidate.key) {
                        mirrors.push(candidate);
                    }
                }
                ("wellKnown", _) => well_known = true,
                _ => {}
            }
        }
        mirrors.sort_by_key(|mirror| {
            std::cmp::Reverse(book.last_success_ms(&mirror.key).unwrap_or(i64::MIN))
        });

        let mut access_points: Vec<SourceCandidate> = Vec::new();
        if well_known {
            let bootstrap_id = self.kernel.bootstrap_id();
            let mut entries: Vec<_> = snapshot
                .access_points()
                .iter()
                .filter(|entry| entry.status != ApStatus::Disabled)
                .collect();
            entries.sort_by_key(|entry| current_ap != Some(&*entry.id));
            for entry in entries {
                let source = ManifestSource::WellKnown {
                    ap_id: entry.id.to_string(),
                };
                access_points.push(SourceCandidate {
                    key: source.label(),
                    tier: SourceTier::WellKnown,
                    base_url: format!("https://{}/.well-known/flare/v1/{bootstrap_id}", entry.host),
                    source,
                    ip_hints: entry.ip_hints.clone(),
                    spki_pins: entry.spki_pins.clone(),
                    timeout_ms: WELL_KNOWN_TIMEOUT_MS,
                });
            }
        }
        let (trusted, demoted): (Vec<_>, Vec<_>) = mirrors
            .into_iter()
            .chain(access_points)
            .partition(|candidate| !book.is_demoted(&candidate.key, now));
        trusted.into_iter().chain(demoted).collect()
    }

    async fn fetch_source(&self, candidate: SourceCandidate, book_etags: EtagPair) -> Fetched {
        let access = candidate.request(ACCESS_FILE, book_etags.access);
        let keys = candidate.request(KEYS_FILE, book_etags.keys);
        let (access, keys) = join(self.http.fetch(&access), self.http.fetch(&keys)).await;
        Fetched {
            candidate,
            keys,
            access,
        }
    }

    /// 交错并发：第一个立即开始，之后每 400 ms 启动下一个，最多 3 个在途。
    /// 结果按到达顺序处理；一旦可以结束，丢弃其余在途请求即取消。
    async fn run_sources(
        &self,
        candidates: Vec<SourceCandidate>,
        round: &mut Round,
        book: &mut SourceBook,
    ) -> Flow {
        let mut pending: VecDeque<_> = candidates
            .into_iter()
            .filter(|candidate| round.tried.insert(candidate.key.clone()))
            .collect();
        let current_serial = self.kernel.directory().snapshot().serial();
        let keystate_serial = self
            .kernel
            .trust()
            .and_then(|trust| trust.keystate_serial());
        let now = self.clock.now_ms();

        let mut in_flight: FuturesUnordered<BoxedFuture<'_, Fetched>> = FuturesUnordered::new();
        let mut stagger: Option<BoxedFuture<'_, ()>> = None;
        loop {
            if stagger.is_none()
                && in_flight.len() < MAX_IN_FLIGHT
                && let Some(candidate) = pending.pop_front()
            {
                let etags = EtagPair {
                    access: book.access_etag(&candidate.key, current_serial, now),
                    keys: book.keys_etag(&candidate.key, keystate_serial, now),
                };
                in_flight.push(Box::pin(self.fetch_source(candidate, etags)));
                if !pending.is_empty() {
                    stagger = Some(Box::pin(self.pause(STAGGER_MS)));
                }
            }
            let fetched = match stagger.take() {
                Some(timer) if in_flight.is_empty() => {
                    timer.await;
                    continue;
                }
                Some(timer) => match select(in_flight.next(), timer).await {
                    Either::Left((fetched, timer)) => {
                        stagger = Some(timer);
                        fetched
                    }
                    Either::Right(((), _)) => continue,
                },
                None => in_flight.next().await,
            };
            let Some(fetched) = fetched else {
                if pending.is_empty() {
                    return Flow::Continue;
                }
                continue;
            };
            if self.ingest(fetched, round, book).await == Flow::Done {
                return Flow::Done;
            }
        }
    }

    async fn pause(&self, ms: u64) {
        self.clock.sleep(ms).await;
    }

    async fn ingest(&self, fetched: Fetched, round: &mut Round, book: &mut SourceBook) -> Flow {
        let now = self.clock.now_ms();
        // 密钥状态先于清单：新吊销立刻对同一批文档生效，轮换后的新证书也可能只在这里。
        self.ingest_keys(&fetched.candidate, fetched.keys, round, book, now)
            .await;
        self.ingest_access(&fetched.candidate, fetched.access, round, book, now)
            .await
    }

    /// 拉不到密钥状态不算源失败：还没发过吊销的部署没有这个文件，清单照样可验。
    async fn ingest_keys(
        &self,
        candidate: &SourceCandidate,
        response: Result<FetchResponse, FetchError>,
        round: &mut Round,
        book: &mut SourceBook,
        now: i64,
    ) {
        let Ok(response) = response else {
            return;
        };
        if response.status != 200 {
            return;
        }
        let meta = FetchMeta {
            source: candidate.source.clone(),
            etag: response.etag.clone(),
            fetched_at_ms: now,
        };
        match self.kernel.accept_keystate(&response.body, meta, now).await {
            Ok(update) => {
                let serial = match update {
                    KeystateUpdate::Adopted { serial, .. } => {
                        round.keystate = Some(update);
                        serial
                    }
                    KeystateUpdate::Unchanged { serial } => {
                        round.keystate.get_or_insert(update);
                        serial
                    }
                };
                book.served_keys(&candidate.key, response.etag, serial);
            }
            Err(AcceptError::Trust(error)) => {
                if is_suspicious(&error) {
                    book.demote(&candidate.key, now);
                }
                let (result, serial) = trust_failure(&error);
                round.record(candidate, "keys", result, Some(error.code()), serial);
            }
            Err(_) => {}
        }
    }

    async fn ingest_access(
        &self,
        candidate: &SourceCandidate,
        response: Result<FetchResponse, FetchError>,
        round: &mut Round,
        book: &mut SourceBook,
        now: i64,
    ) -> Flow {
        let response = match response {
            Ok(response) => response,
            Err(FetchError::TooLarge) => {
                book.demote(&candidate.key, now);
                round.record(
                    candidate,
                    "access",
                    AttemptResult::Rejected,
                    Some(FetchError::TooLarge.code().to_owned()),
                    None,
                );
                return Flow::Continue;
            }
            Err(error) => {
                round.record(
                    candidate,
                    "access",
                    AttemptResult::Failed,
                    Some(error.code().to_owned()),
                    None,
                );
                return Flow::Continue;
            }
        };
        match response.status {
            304 => {
                book.confirmed(&candidate.key, now);
                round.answered += 1;
                round.record(candidate, "access", AttemptResult::NotModified, None, None);
                self.after_no_update(round.mode)
            }
            200 => {
                self.ingest_document(candidate, response, round, book, now)
                    .await
            }
            status => {
                round.record(
                    candidate,
                    "access",
                    AttemptResult::Failed,
                    Some(format!("http.{status}")),
                    None,
                );
                Flow::Continue
            }
        }
    }

    async fn ingest_document(
        &self,
        candidate: &SourceCandidate,
        response: FetchResponse,
        round: &mut Round,
        book: &mut SourceBook,
        now: i64,
    ) -> Flow {
        let meta = FetchMeta {
            source: candidate.source.clone(),
            etag: response.etag.clone(),
            fetched_at_ms: now,
        };
        match self.kernel.accept_manifest(&response.body, meta, now).await {
            Ok(ManifestUpdate::Adopted {
                change,
                stale,
                persisted,
            }) => {
                let serial = change.to_serial;
                book.served_access(&candidate.key, response.etag, serial, now);
                round.answered += 1;
                round.record(
                    candidate,
                    "access",
                    AttemptResult::Adopted,
                    None,
                    Some(serial),
                );
                round.adoptions.push(Adoption {
                    change,
                    source: candidate.source.clone(),
                    stale,
                    persisted,
                });
                match round.mode {
                    RefreshMode::Notified(_) if !self.reached_target(round.mode) => Flow::Continue,
                    _ => Flow::Done,
                }
            }
            Ok(ManifestUpdate::Unchanged { serial }) => {
                book.served_access(&candidate.key, response.etag, serial, now);
                round.answered += 1;
                round.record(
                    candidate,
                    "access",
                    AttemptResult::SameSerial,
                    None,
                    Some(serial),
                );
                self.after_no_update(round.mode)
            }
            Err(AcceptError::Trust(error)) => {
                if is_suspicious(&error) {
                    book.demote(&candidate.key, now);
                }
                let (result, serial) = trust_failure(&error);
                round.record(candidate, "access", result, Some(error.code()), serial);
                Flow::Continue
            }
            Err(error) => {
                round.record(
                    candidate,
                    "access",
                    AttemptResult::Failed,
                    Some(error.to_string()),
                    None,
                );
                Flow::Continue
            }
        }
    }

    /// 一个源确认「没有更新」之后：周期刷新到此为止；通知模式这只说明它还是旧值；恢复要试遍所有源。
    fn after_no_update(&self, mode: RefreshMode) -> Flow {
        match mode {
            RefreshMode::Background => Flow::Done,
            RefreshMode::Notified(_) | RefreshMode::Recovering => Flow::Continue,
        }
    }

    /// T4：并行查询全部 DoH 解析器，取指针的并集。
    async fn resolve_pointers(
        &self,
        snapshot: &DirectorySnapshot,
        round: &mut Round,
    ) -> Vec<SourceCandidate> {
        let lookups: Vec<(String, String)> = snapshot
            .manifest()
            .bootstrap
            .sources
            .iter()
            .chain(self.kernel.seed_sources())
            .filter(|source| source.kind == "dohTxt")
            .filter_map(|source| Some((source.name.clone()?, source.resolvers.clone())))
            .flat_map(|(name, resolvers)| {
                resolvers
                    .into_iter()
                    .map(move |resolver| (resolver, name.clone()))
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .take(MAX_DOH_LOOKUPS)
            .collect();
        let mut in_flight: FuturesUnordered<BoxedFuture<'_, (String, Option<Vec<String>>)>> =
            lookups
                .into_iter()
                .map(|(resolver, name)| Box::pin(self.lookup(resolver, name)) as BoxedFuture<'_, _>)
                .collect();
        let mut pointers = Vec::new();
        while let Some((resolver, urls)) = in_flight.next().await {
            let result = match &urls {
                Some(_) => AttemptResult::Resolved,
                None => AttemptResult::Failed,
            };
            round.attempts.push(SourceAttempt {
                source: format!("doh:{resolver}"),
                tier: SourceTier::Resolver,
                document: "pointer",
                result,
                code: None,
                serial: None,
            });
            for url in urls.into_iter().flatten() {
                let candidate = SourceCandidate::mirror(&url, SourceTier::Pointer, Vec::new());
                if !pointers
                    .iter()
                    .any(|known: &SourceCandidate| known.key == candidate.key)
                {
                    pointers.push(candidate);
                }
            }
        }
        pointers
    }

    /// 问一个解析器。`None`：没得到可解析的答复。
    async fn lookup(&self, resolver: String, name: String) -> (String, Option<Vec<String>>) {
        let Some(url) = doh::query_url(&resolver, &name) else {
            return (resolver, None);
        };
        let request = FetchRequest {
            url,
            accept: Some(doh::DNS_MESSAGE),
            timeout_ms: DOH_TIMEOUT_MS,
            max_body_bytes: doh::MAX_RESPONSE_BYTES,
            ..FetchRequest::default()
        };
        let urls = match self.http.fetch(&request).await {
            Ok(response) if response.status == 200 => {
                doh::parse_txt_response(&response.body).map(|records| {
                    records
                        .iter()
                        .flat_map(|record| doh::parse_pointer(record))
                        .collect()
                })
            }
            _ => None,
        };
        (resolver, urls)
    }
}

/// 发给一个源的条件请求所用的 ETag。
struct EtagPair {
    access: Option<String>,
    keys: Option<String>,
}

fn trust_failure(error: &TrustError) -> (AttemptResult, Option<u64>) {
    match error {
        TrustError::Rollback { serial, .. } => (AttemptResult::OlderSerial, Some(*serial)),
        _ => (AttemptResult::Rejected, None),
    }
}

#[cfg(test)]
#[path = "fetcher_tests.rs"]
mod tests;
