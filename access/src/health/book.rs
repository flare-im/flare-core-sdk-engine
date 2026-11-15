//! 健康簿实现。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use super::breaker::{Breaker, BreakerPhase, BreakerTransition};
use super::netprint::NetPrint;
use super::{AttemptKind, Observation, Outcome};
use crate::manifest::schema::MAX_ACCESS_POINTS;
use crate::manifest::{BreakerPolicy, ClientPolicy};
use crate::platform::{DefaultRandom, RandomSource};
use crate::store::{AccessStore, StoreError, StoreOp, keys};
use crate::time::{DAY_MS, HOUR_MS, MINUTE_MS, SECOND_MS};
use crate::types::{ApId, Protocol};

/// 失败率窗口长度（近 20 次尝试）。
pub const WINDOW_SIZE: u8 = 20;
/// 失败率规则的最少样本数：样本太少时 1 次失败就是 100%，只看连续失败更可靠。
pub const MIN_RATIO_SAMPLES: u8 = 10;
/// RTT EWMA 平滑系数。
pub const EWMA_ALPHA: f64 = 0.3;
/// RTT 样本超过 30 分钟未更新即视为未知（网络环境可能已经变了）。
pub const RTT_STALE_AFTER_MS: i64 = 30 * MINUTE_MS;
/// 失败率窗口超过 24 小时没有新结果即清空：昨天的失败不应长期压低一个入口的评分，
/// 否则它很少再被选中，也就没有机会产生新样本洗掉旧记录。
pub const WINDOW_STALE_AFTER_MS: i64 = DAY_MS;
/// QUIC 连续失败多少次（且同期 WS 成功）判定该网络封锁 UDP。
pub const UDP_BLOCK_QUIC_FAILURES: u32 = 3;
/// 落盘节流间隔。
pub const FLUSH_INTERVAL_MS: i64 = 30 * SECOND_MS;
/// 持久化记录保留期：30 天未更新即清理。
pub const RETENTION_MS: i64 = 30 * DAY_MS;
/// 最多记住的网络数；超出时淘汰最久未用的网络。
pub const MAX_NETWORKS: usize = 32;

/// 最近 20 次尝试的成败（位图，1 = 失败）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeWindow {
    bits: u32,
    len: u8,
}

impl OutcomeWindow {
    const MASK: u32 = (1 << WINDOW_SIZE) - 1;

    fn push(&mut self, failure: bool) {
        self.bits = ((self.bits << 1) | u32::from(failure)) & Self::MASK;
        self.len = (self.len + 1).min(WINDOW_SIZE);
    }

    fn failures(&self) -> u32 {
        let mask = if self.len >= 32 {
            u32::MAX
        } else {
            (1u32 << self.len) - 1
        };
        (self.bits & mask).count_ones()
    }

    fn ratio(&self) -> f64 {
        if self.len == 0 {
            0.0
        } else {
            f64::from(self.failures()) / f64::from(self.len)
        }
    }
}

/// 连续探活成功的计数与起点，供恢复切回判定（需要跨度 ≥ 5 分钟的连续成功）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeStreak {
    pub successes: u32,
    pub since_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApHealth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rtt_ewma_ms: Option<f64>,
    #[serde(default)]
    rtt_updated_ms: i64,
    #[serde(default)]
    window: OutcomeWindow,
    #[serde(default)]
    consecutive_failures: u32,
    #[serde(default)]
    breaker: Breaker,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    protocol_winner: Option<Protocol>,
    #[serde(default)]
    probe_streak: ProbeStreak,
    /// 最近一次计入窗口的结果时间；`None` 表示还没有（不能用 0 当哨兵：0 是合法时间）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_result_ms: Option<i64>,
    #[serde(default)]
    touched_ms: i64,
    /// 有未落盘的变化。放在条目里而不是单独的脏集合：记录时不需要为键再分配一次字符串。
    #[serde(skip)]
    dirty: bool,
}

impl ApHealth {
    fn rtt(&self, now_ms: i64) -> Option<f64> {
        self.rtt_ewma_ms
            .filter(|_| now_ms.saturating_sub(self.rtt_updated_ms) <= RTT_STALE_AFTER_MS)
    }

    fn window_is_stale(&self, now_ms: i64) -> bool {
        self.last_result_ms
            .is_some_and(|last| now_ms.saturating_sub(last) > WINDOW_STALE_AFTER_MS)
    }

    fn failure_ratio(&self, now_ms: i64) -> f64 {
        if self.window_is_stale(now_ms) {
            0.0
        } else {
            self.window.ratio()
        }
    }

    fn sample_rtt(&mut self, sample_ms: u32, now_ms: i64) {
        let sample = f64::from(sample_ms);
        self.rtt_ewma_ms = Some(match self.rtt(now_ms) {
            Some(previous) => EWMA_ALPHA * sample + (1.0 - EWMA_ALPHA) * previous,
            None => sample,
        });
        self.rtt_updated_ms = now_ms;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UdpState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blocked_until_ms: Option<i64>,
    #[serde(default)]
    quic_fail_streak: u32,
    #[serde(default)]
    ws_ok_during_streak: bool,
    #[serde(default)]
    touched_ms: i64,
}

#[derive(Default)]
struct NetHealth {
    aps: HashMap<ApId, ApHealth>,
    udp: UdpState,
    udp_dirty: bool,
    touched_ms: i64,
}

impl NetHealth {
    fn ap_mut(&mut self, ap: &str, now_ms: i64) -> &mut ApHealth {
        // 已有条目走借用查找，不分配；只有首次出现才创建 `ApId`。
        if self.aps.contains_key(ap) {
            let entry = self.aps.get_mut(ap).expect("checked above");
            entry.touched_ms = now_ms;
            return entry;
        }
        if self.aps.len() >= MAX_ACCESS_POINTS
            && let Some(oldest) = self
                .aps
                .iter()
                .min_by_key(|(_, health)| health.touched_ms)
                .map(|(id, _)| id.clone())
        {
            self.aps.remove(&oldest);
        }
        let entry = self.aps.entry(ApId::from(ap)).or_default();
        entry.touched_ms = now_ms;
        entry
    }
}

/// 选路需要的只读视图。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HealthView {
    /// 新鲜的 RTT EWMA；无样本或已过期为 `None`。
    pub rtt_ms: Option<f64>,
    /// 近 20 次失败率。
    pub failure_ratio: f64,
    pub breaker: BreakerView,
}

impl HealthView {
    pub const UNKNOWN: HealthView = HealthView {
        rtt_ms: None,
        failure_ratio: 0.0,
        breaker: BreakerView::Closed,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerView {
    Closed,
    Open {
        until_ms: i64,
    },
    /// `trial_available = false` 表示已有试探在途，本次不应再选它。
    HalfOpen {
        trial_available: bool,
    },
}

/// 一次记录产生的状态变化，引擎据此打日志或发诊断事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecordEffect {
    pub breaker: Option<BreakerTransition>,
    /// 本次记录让该网络进入 UDP 封锁（到期时间）。
    pub udp_blocked_until_ms: Option<i64>,
}

/// 诊断页使用的完整记录。
#[derive(Debug, Clone, PartialEq)]
pub struct ApHealthReport {
    pub rtt_ms: Option<f64>,
    pub failure_ratio: f64,
    pub samples: u8,
    pub consecutive_failures: u32,
    pub breaker: BreakerPhase,
    pub open_until_ms: Option<i64>,
    pub cooldown_ms: u64,
    pub protocol_winner: Option<Protocol>,
    pub probe_streak: ProbeStreak,
}

/// 启动加载结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoadReport {
    pub loaded: usize,
    pub expired: usize,
    pub corrupt: usize,
}

struct BookState {
    breaker_policy: BreakerPolicy,
    udp_blocked_ttl_ms: i64,
    nets: HashMap<NetPrint, NetHealth>,
    rng: Box<dyn RandomSource>,
    /// 任一条目有未落盘变化。
    dirty: bool,
    last_flush_ms: Option<i64>,
}

impl BookState {
    fn net_mut(&mut self, net: NetPrint, now_ms: i64) -> &mut NetHealth {
        if !self.nets.contains_key(&net)
            && self.nets.len() >= MAX_NETWORKS
            && let Some(oldest) = self
                .nets
                .iter()
                .min_by_key(|(_, health)| health.touched_ms)
                .map(|(net, _)| *net)
        {
            self.nets.remove(&oldest);
        }
        let entry = self.nets.entry(net).or_default();
        entry.touched_ms = now_ms;
        entry
    }

    fn ap(&self, ap: &str, net: NetPrint) -> Option<&ApHealth> {
        self.nets.get(&net)?.aps.get(ap)
    }

    fn view(&self, ap: &str, net: NetPrint, now_ms: i64) -> HealthView {
        let Some(health) = self.ap(ap, net) else {
            return HealthView::UNKNOWN;
        };
        let breaker = match health.breaker.phase(now_ms) {
            BreakerPhase::Closed => BreakerView::Closed,
            BreakerPhase::Open => BreakerView::Open {
                until_ms: health.breaker.open_until_ms().unwrap_or(now_ms),
            },
            BreakerPhase::HalfOpen => BreakerView::HalfOpen {
                trial_available: health.breaker.trial_available(now_ms),
            },
        };
        HealthView {
            rtt_ms: health.rtt(now_ms),
            failure_ratio: health.failure_ratio(now_ms),
            breaker,
        }
    }
}

/// 健康簿。所有方法只在内存中短暂加锁，不做 I/O；持久化见 [`HealthBook::flush`]。
pub struct HealthBook {
    state: Mutex<BookState>,
    /// 串行化落盘，保证后一次快照不会被前一次较旧的写入覆盖。
    flush_gate: futures_util::lock::Mutex<()>,
}

impl HealthBook {
    pub fn new(policy: &ClientPolicy) -> Self {
        Self::with_random(policy, Box::new(DefaultRandom::new()))
    }

    /// 注入随机源（冷却抖动），测试与向量回放使用。
    pub fn with_random(policy: &ClientPolicy, rng: Box<dyn RandomSource>) -> Self {
        Self {
            state: Mutex::new(BookState {
                breaker_policy: policy.breaker.clone(),
                udp_blocked_ttl_ms: i64::from(policy.udp_blocked_ttl_hours) * HOUR_MS,
                nets: HashMap::new(),
                rng,
                dirty: false,
                last_flush_ms: None,
            }),
            flush_gate: futures_util::lock::Mutex::new(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BookState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 清单更新后应用新的熔断参数与 UDP 封锁时长；已有状态保留。
    pub fn apply_policy(&self, policy: &ClientPolicy) {
        let mut state = self.lock();
        state.breaker_policy = policy.breaker.clone();
        state.udp_blocked_ttl_ms = i64::from(policy.udp_blocked_ttl_hours) * HOUR_MS;
    }

    /// 回写一次结果。热路径：一次加锁、若干哈希查找，首次见到（接入点, 网络）时分配一个条目。
    pub fn record(
        &self,
        ap: &str,
        net: NetPrint,
        observation: Observation,
        now_ms: i64,
    ) -> RecordEffect {
        let mut guard = self.lock();
        let state = &mut *guard;
        let policy = state.breaker_policy.clone();
        let udp_ttl_ms = state.udp_blocked_ttl_ms;

        // 先决定是否需要抽样，保证随机源只在熔断真正打开/重开时被消耗（向量据此可复现）。
        let needs_draw = matches!(observation.outcome, Outcome::Failure(_))
            && match state.ap(ap, net) {
                None => trips(1, &single_failure_window(), &policy),
                Some(health) => match health.breaker.phase(now_ms) {
                    BreakerPhase::HalfOpen => true,
                    BreakerPhase::Open => false,
                    BreakerPhase::Closed => {
                        let stale = health.window_is_stale(now_ms);
                        let mut window = if stale {
                            OutcomeWindow::default()
                        } else {
                            health.window
                        };
                        let consecutive = if stale {
                            0
                        } else {
                            health.consecutive_failures
                        };
                        window.push(true);
                        trips(consecutive + 1, &window, &policy)
                    }
                },
            };
        let unit = if needs_draw {
            state.rng.next_unit()
        } else {
            0.5
        };

        let mut effect = RecordEffect::default();
        let net_state = state.net_mut(net, now_ms);
        let health = net_state.ap_mut(ap, now_ms);
        if health.window_is_stale(now_ms) {
            health.window = OutcomeWindow::default();
            health.consecutive_failures = 0;
        }

        match observation.outcome {
            Outcome::Success { rtt_ms } => {
                if let Some(rtt_ms) = rtt_ms {
                    health.sample_rtt(rtt_ms, now_ms);
                }
                if observation.kind != AttemptKind::Heartbeat {
                    let transition = health.breaker.on_success(now_ms);
                    if transition == Some(BreakerTransition::Closed) {
                        // 试探成功说明已经恢复：清空旧窗口，避免残留失败率让它刚闭合就再次熔断。
                        health.window = OutcomeWindow::default();
                    }
                    effect.breaker = transition;
                    health.window.push(false);
                    health.consecutive_failures = 0;
                    health.last_result_ms = Some(now_ms);
                }
                match observation.kind {
                    AttemptKind::Handshake(protocol) => health.protocol_winner = Some(protocol),
                    AttemptKind::Probe => {
                        if health.probe_streak.successes == 0 {
                            health.probe_streak.since_ms = now_ms;
                        }
                        health.probe_streak.successes += 1;
                    }
                    AttemptKind::Heartbeat | AttemptKind::Http => {}
                }
            }
            Outcome::Failure(_) => {
                health.window.push(true);
                health.consecutive_failures += 1;
                health.last_result_ms = Some(now_ms);
                health.probe_streak = ProbeStreak::default();
                let trip = trips(health.consecutive_failures, &health.window, &policy);
                effect.breaker = health.breaker.on_failure(now_ms, trip, &policy, unit);
            }
            Outcome::Neutral(_) => health.breaker.on_neutral(),
        }
        health.dirty = true;

        // UDP 封锁记忆按网络维度：QUIC 连续失败 3 次，且这段时间里 WS 成功过。
        let udp = &mut net_state.udp;
        match (observation.kind, observation.outcome) {
            (AttemptKind::Handshake(Protocol::Quic), Outcome::Failure(_)) => {
                udp.quic_fail_streak += 1;
            }
            (AttemptKind::Handshake(Protocol::Quic), Outcome::Success { .. }) => {
                udp.quic_fail_streak = 0;
                udp.ws_ok_during_streak = false;
            }
            (AttemptKind::Handshake(Protocol::Ws), Outcome::Success { .. })
                if udp.quic_fail_streak > 0 =>
            {
                udp.ws_ok_during_streak = true;
            }
            _ => {}
        }
        if udp.quic_fail_streak >= UDP_BLOCK_QUIC_FAILURES && udp.ws_ok_during_streak {
            let until = now_ms + udp_ttl_ms;
            udp.blocked_until_ms = Some(until);
            udp.quic_fail_streak = 0;
            udp.ws_ok_during_streak = false;
            effect.udp_blocked_until_ms = Some(until);
        }
        udp.touched_ms = now_ms;
        net_state.udp_dirty = true;
        state.dirty = true;
        effect
    }

    /// 交错竞速里 QUIC 一直没建起传输、被先赢的 WebSocket 取消（UDP 被静默丢弃的典型表现）：
    /// 只作为 UDP 封锁的证据累计，不进该接入点的失败率与熔断——被取消不是入口的失败。
    /// 同一网络累计 [`UDP_BLOCK_QUIC_FAILURES`] 次且期间 WS 成功过即进入封锁记忆，与真实的 QUIC 握手失败同等计数。
    pub fn note_quic_silence(&self, net: NetPrint, now_ms: i64) -> RecordEffect {
        let mut guard = self.lock();
        let state = &mut *guard;
        let udp_ttl_ms = state.udp_blocked_ttl_ms;
        let net_state = state.net_mut(net, now_ms);
        let udp = &mut net_state.udp;
        udp.quic_fail_streak += 1;
        udp.touched_ms = now_ms;
        let mut effect = RecordEffect::default();
        if udp.quic_fail_streak >= UDP_BLOCK_QUIC_FAILURES && udp.ws_ok_during_streak {
            let until = now_ms + udp_ttl_ms;
            udp.blocked_until_ms = Some(until);
            udp.quic_fail_streak = 0;
            udp.ws_ok_during_streak = false;
            effect.udp_blocked_until_ms = Some(until);
        }
        net_state.udp_dirty = true;
        state.dirty = true;
        effect
    }

    /// 连接 half-open 接入点之前认领试探；返回 `false` 表示已有试探在途或仍在冷却，应换下一个候选。
    /// Closed 的接入点直接返回 `true`。
    pub fn claim_trial(&self, ap: &str, net: NetPrint, now_ms: i64) -> bool {
        let mut state = self.lock();
        match state.nets.get_mut(&net).and_then(|net| net.aps.get_mut(ap)) {
            Some(health) => health.breaker.claim_trial(now_ms),
            None => true,
        }
    }

    /// 有外部证据表明网络已经可用（例如收到了推送）时调用：该网络下仍在冷却的接入点都提前转为
    /// half-open，各放一次试探；试探失败照常以翻倍的冷却重新打开。返回提前了几个。
    pub fn expedite_trials(&self, net: NetPrint, now_ms: i64) -> usize {
        let mut state = self.lock();
        let Some(net_state) = state.nets.get_mut(&net) else {
            return 0;
        };
        let expedited = net_state
            .aps
            .values_mut()
            .map(|health| health.breaker.expedite(now_ms))
            .filter(|expedited| *expedited)
            .count();
        if expedited > 0 {
            state.dirty = true;
        }
        expedited
    }

    pub fn view(&self, ap: &str, net: NetPrint, now_ms: i64) -> HealthView {
        self.lock().view(ap, net, now_ms)
    }

    /// 一次加锁取多个接入点的视图（选路用）。
    pub fn views<'a>(
        &self,
        aps: impl IntoIterator<Item = &'a str>,
        net: NetPrint,
        now_ms: i64,
    ) -> Vec<HealthView> {
        let state = self.lock();
        aps.into_iter()
            .map(|ap| state.view(ap, net, now_ms))
            .collect()
    }

    /// 该网络的 UDP 封锁到期时间（未封锁或已过期返回 `None`）。
    pub fn udp_blocked_until(&self, net: NetPrint, now_ms: i64) -> Option<i64> {
        self.lock()
            .nets
            .get(&net)?
            .udp
            .blocked_until_ms
            .filter(|until| *until > now_ms)
    }

    /// 该接入点在该网络下上次握手胜出的协议。
    pub fn protocol_winner(&self, ap: &str, net: NetPrint) -> Option<Protocol> {
        self.lock().ap(ap, net)?.protocol_winner
    }

    pub fn probe_streak(&self, ap: &str, net: NetPrint) -> ProbeStreak {
        self.lock()
            .ap(ap, net)
            .map(|health| health.probe_streak)
            .unwrap_or_default()
    }

    pub fn report(&self, ap: &str, net: NetPrint, now_ms: i64) -> Option<ApHealthReport> {
        let state = self.lock();
        let health = state.ap(ap, net)?;
        Some(ApHealthReport {
            rtt_ms: health.rtt(now_ms),
            failure_ratio: health.failure_ratio(now_ms),
            samples: if health.window_is_stale(now_ms) {
                0
            } else {
                health.window.len
            },
            consecutive_failures: if health.window_is_stale(now_ms) {
                0
            } else {
                health.consecutive_failures
            },
            breaker: health.breaker.phase(now_ms),
            open_until_ms: health.breaker.open_until_ms(),
            cooldown_ms: health.breaker.cooldown_ms(),
            protocol_winner: health.protocol_winner,
            probe_streak: health.probe_streak,
        })
    }

    /// 是否到了落盘时机：有未落盘的变化，且距上次落盘不少于 30 秒。
    pub fn flush_due(&self, now_ms: i64) -> bool {
        let state = self.lock();
        state.dirty
            && state
                .last_flush_ms
                .is_none_or(|last| now_ms.saturating_sub(last) >= FLUSH_INTERVAL_MS)
    }

    /// 批量落盘。`force = false` 时按节流规则可能什么都不做；进入后台或退出前用 `force = true`。
    /// 写失败时变化重新标脏，下次再试。返回写入的条目数。
    pub async fn flush(
        &self,
        store: &dyn AccessStore,
        now_ms: i64,
        force: bool,
    ) -> Result<usize, StoreError> {
        let _gate = self.flush_gate.lock().await;
        let (ops, ap_keys, net_keys) = {
            let mut guard = self.lock();
            let state = &mut *guard;
            let due = state
                .last_flush_ms
                .is_none_or(|last| now_ms.saturating_sub(last) >= FLUSH_INTERVAL_MS);
            if !state.dirty || (!force && !due) {
                return Ok(0);
            }
            let mut ops = Vec::new();
            let mut ap_keys: Vec<(NetPrint, ApId)> = Vec::new();
            let mut net_keys: Vec<NetPrint> = Vec::new();
            for (net, net_state) in state.nets.iter_mut() {
                let net_hex = net.to_hex();
                for (ap, health) in net_state.aps.iter_mut().filter(|(_, health)| health.dirty) {
                    health.dirty = false;
                    ops.push(StoreOp::Put {
                        key: keys::health_ap(ap, &net_hex),
                        value: serde_json::to_vec(health).unwrap_or_default(),
                    });
                    ap_keys.push((*net, ap.clone()));
                }
                if net_state.udp_dirty {
                    net_state.udp_dirty = false;
                    ops.push(StoreOp::Put {
                        key: keys::health_net(&net_hex),
                        value: serde_json::to_vec(&net_state.udp).unwrap_or_default(),
                    });
                    net_keys.push(*net);
                }
            }
            state.dirty = false;
            state.last_flush_ms = Some(now_ms);
            (ops, ap_keys, net_keys)
        };
        if ops.is_empty() {
            return Ok(0);
        }
        match store.apply(&ops).await {
            Ok(()) => Ok(ops.len()),
            Err(error) => {
                // 写失败：把这批条目重新标脏（期间被淘汰的条目忽略），下次再试。
                let mut state = self.lock();
                for (net, ap) in ap_keys {
                    if let Some(health) = state
                        .nets
                        .get_mut(&net)
                        .and_then(|net| net.aps.get_mut(&ap))
                    {
                        health.dirty = true;
                    }
                }
                for net in net_keys {
                    if let Some(net_state) = state.nets.get_mut(&net) {
                        net_state.udp_dirty = true;
                    }
                }
                state.dirty = true;
                Err(error)
            }
        }
    }

    /// 启动时从存储加载；30 天未更新的记录与无法解析的记录一并删除。
    pub async fn load(
        &self,
        store: &dyn AccessStore,
        now_ms: i64,
    ) -> Result<LoadReport, StoreError> {
        let entries = store.scan_prefix(keys::HEALTH_PREFIX).await?;
        let mut report = LoadReport::default();
        let mut deletes = Vec::new();
        {
            let mut state = self.lock();
            for (key, value) in entries {
                if let Some(rest) = key.strip_prefix(keys::HEALTH_AP_PREFIX) {
                    let parsed = rest.rsplit_once('/').and_then(|(ap, net)| {
                        let net = NetPrint::from_hex(net)?;
                        let health: ApHealth = serde_json::from_slice(&value).ok()?;
                        Some((ap.to_owned(), net, health))
                    });
                    match parsed {
                        Some((_, _, health))
                            if now_ms.saturating_sub(health.touched_ms) > RETENTION_MS =>
                        {
                            report.expired += 1;
                            deletes.push(StoreOp::Delete { key });
                        }
                        Some((ap, net, health)) => {
                            let touched = health.touched_ms;
                            let net_state = state.net_mut(net, touched);
                            let slot = net_state.ap_mut(&ap, touched);
                            *slot = health;
                            report.loaded += 1;
                        }
                        None => {
                            report.corrupt += 1;
                            deletes.push(StoreOp::Delete { key });
                        }
                    }
                } else if let Some(rest) = key.strip_prefix(keys::HEALTH_NET_PREFIX) {
                    let parsed = NetPrint::from_hex(rest).and_then(|net| {
                        serde_json::from_slice::<UdpState>(&value)
                            .ok()
                            .map(|udp| (net, udp))
                    });
                    match parsed {
                        Some((_, udp)) if now_ms.saturating_sub(udp.touched_ms) > RETENTION_MS => {
                            report.expired += 1;
                            deletes.push(StoreOp::Delete { key });
                        }
                        Some((net, udp)) => {
                            let touched = udp.touched_ms;
                            state.net_mut(net, touched).udp = udp;
                            report.loaded += 1;
                        }
                        None => {
                            report.corrupt += 1;
                            deletes.push(StoreOp::Delete { key });
                        }
                    }
                }
            }
            // 加载后的状态与磁盘一致，无需立刻回写。
            state.last_flush_ms = Some(now_ms);
        }
        if !deletes.is_empty() {
            store.apply(&deletes).await?;
        }
        Ok(report)
    }
}

/// 熔断判定：连续失败达到阈值，或样本足够时失败率达到阈值。
fn trips(consecutive_failures: u32, window: &OutcomeWindow, policy: &BreakerPolicy) -> bool {
    consecutive_failures >= policy.failure_threshold
        || (window.len >= MIN_RATIO_SAMPLES && window.ratio() >= policy.failure_ratio)
}

fn single_failure_window() -> OutcomeWindow {
    let mut window = OutcomeWindow::default();
    window.push(true);
    window
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{FailureClass, NeutralReason};
    use crate::platform::SequenceRandom;
    use crate::store::MemoryAccessStore;

    fn book() -> HealthBook {
        HealthBook::with_random(
            &ClientPolicy::default(),
            Box::new(SequenceRandom::new([0.5; 16])),
        )
    }

    const NET: NetPrint = NetPrint::UNKNOWN;

    #[test]
    fn ewma_and_staleness() {
        let book = book();
        book.record("a", NET, Observation::handshake_ok(Protocol::Ws, 100), 0);
        book.record("a", NET, Observation::heartbeat_rtt(200), 1_000);
        // 0.3 × 200 + 0.7 × 100 = 130
        assert!((book.view("a", NET, 1_000).rtt_ms.unwrap() - 130.0).abs() < 1e-9);
        assert_eq!(
            book.view("a", NET, 1_000 + RTT_STALE_AFTER_MS + 1).rtt_ms,
            None
        );
        // 过期后的新样本直接作为起点。
        book.record(
            "a",
            NET,
            Observation::heartbeat_rtt(50),
            1_000 + RTT_STALE_AFTER_MS + 1,
        );
        assert_eq!(
            book.view("a", NET, 1_000 + RTT_STALE_AFTER_MS + 1).rtt_ms,
            Some(50.0)
        );
        // 心跳成功不进窗口。
        assert_eq!(book.report("a", NET, 1_000).unwrap().samples, 1);
    }

    #[test]
    fn three_consecutive_failures_open_and_neutral_does_not_count() {
        let book = book();
        let fail = Observation::handshake_failed(Protocol::Ws, FailureClass::Unreachable);
        book.record("a", NET, fail, 0);
        book.record(
            "a",
            NET,
            Observation::http(Outcome::Neutral(NeutralReason::Overloaded)),
            1,
        );
        book.record(
            "a",
            NET,
            Observation::http(Outcome::Neutral(NeutralReason::AuthRejected)),
            2,
        );
        book.record("a", NET, fail, 3);
        assert_eq!(book.view("a", NET, 4).breaker, BreakerView::Closed);
        let effect = book.record("a", NET, fail, 4);
        assert_eq!(
            effect.breaker,
            Some(BreakerTransition::Opened {
                until_ms: 30_004,
                cooldown_ms: 30_000
            })
        );
        assert_eq!(
            book.view("a", NET, 5).breaker,
            BreakerView::Open { until_ms: 30_004 }
        );
        assert!(!book.claim_trial("a", NET, 5));
        assert!(book.claim_trial("a", NET, 30_004));
        assert_eq!(
            book.view("a", NET, 30_005).breaker,
            BreakerView::HalfOpen {
                trial_available: false
            }
        );
        let effect = book.record(
            "a",
            NET,
            Observation::handshake_ok(Protocol::Ws, 40),
            30_010,
        );
        assert_eq!(effect.breaker, Some(BreakerTransition::Closed));
        assert_eq!(book.view("a", NET, 30_011).failure_ratio, 0.0);
    }

    #[test]
    fn failure_ratio_rule_needs_ten_samples() {
        let book = book();
        let ok = Observation::http(Outcome::Success { rtt_ms: Some(30) });
        let fail = Observation::http(Outcome::Failure(FailureClass::HttpConnectFailed));
        // ok, fail 交替：连续失败从不超过 1；第 10 个样本时失败率 50%。
        for step in 0..9 {
            let observation = if step % 2 == 0 { ok } else { fail };
            book.record("a", NET, observation, step);
            assert_eq!(
                book.view("a", NET, step).breaker,
                BreakerView::Closed,
                "step {step}"
            );
        }
        let effect = book.record("a", NET, fail, 9);
        assert!(matches!(
            effect.breaker,
            Some(BreakerTransition::Opened { .. })
        ));
    }

    #[test]
    fn udp_blocked_after_three_quic_failures_with_ws_success() {
        let book = book();
        let net = NetPrint::from_parts(Some("wifi"), Some("10.0.0"));
        let quic_fail =
            Observation::handshake_failed(Protocol::Quic, FailureClass::HandshakeTimeout);
        book.record("a", net, quic_fail, 0);
        book.record("b", net, quic_fail, 1);
        book.record("a", net, quic_fail, 2);
        assert_eq!(book.udp_blocked_until(net, 3), None);
        let effect = book.record("a", net, Observation::handshake_ok(Protocol::Ws, 80), 3);
        assert_eq!(effect.udp_blocked_until_ms, Some(3 + 24 * HOUR_MS));
        assert_eq!(book.udp_blocked_until(net, 4), Some(3 + 24 * HOUR_MS));
        assert_eq!(book.udp_blocked_until(net, 3 + 24 * HOUR_MS), None);
        // 其他网络不受影响。
        assert_eq!(book.udp_blocked_until(NET, 4), None);
        assert_eq!(book.protocol_winner("a", net), Some(Protocol::Ws));
    }

    #[test]
    fn stale_window_is_cleared() {
        let book = book();
        let fail = Observation::http(Outcome::Failure(FailureClass::HttpConnectFailed));
        book.record("a", NET, fail, 0);
        book.record("a", NET, fail, 1);
        let later = 1 + WINDOW_STALE_AFTER_MS + 1;
        assert_eq!(book.view("a", NET, later).failure_ratio, 0.0);
        // 旧的两次连续失败不再累计：再失败一次不会熔断。
        book.record("a", NET, fail, later);
        assert_eq!(book.view("a", NET, later).breaker, BreakerView::Closed);
    }

    #[tokio::test]
    async fn flush_is_throttled_and_round_trips() {
        let store = MemoryAccessStore::new();
        let book = book();
        let net = NetPrint::from_parts(Some("wifi"), Some("10.0.0"));
        let fail = Observation::handshake_failed(Protocol::Quic, FailureClass::Unreachable);
        for at in 0..3 {
            book.record("a", net, fail, at);
        }
        book.record("a", net, Observation::handshake_ok(Protocol::Ws, 90), 3);
        assert!(book.flush_due(3));
        assert_eq!(book.flush(&store, 3, false).await.unwrap(), 2);
        book.record(
            "b",
            net,
            Observation::probe(Outcome::Success { rtt_ms: Some(10) }),
            4,
        );
        // 30 秒内不落盘，除非强制。
        assert!(!book.flush_due(5));
        assert_eq!(book.flush(&store, 5, false).await.unwrap(), 0);
        assert_eq!(book.flush(&store, 5, true).await.unwrap(), 2);

        let restored = HealthBook::new(&ClientPolicy::default());
        let report = restored.load(&store, 10).await.unwrap();
        assert_eq!(
            report,
            LoadReport {
                loaded: 3,
                expired: 0,
                corrupt: 0
            }
        );
        assert_eq!(restored.udp_blocked_until(net, 10), Some(3 + 24 * HOUR_MS));
        assert_eq!(restored.protocol_winner("a", net), Some(Protocol::Ws));
        assert_eq!(restored.view("b", net, 10).rtt_ms, Some(10.0));
        assert_eq!(
            restored.probe_streak("b", net),
            ProbeStreak {
                successes: 1,
                since_ms: 4
            }
        );

        // 30 天后加载即清理。
        let expired = HealthBook::new(&ClientPolicy::default());
        let report = expired.load(&store, 5 + RETENTION_MS + 1).await.unwrap();
        assert_eq!(report.expired, 3);
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn failed_flush_keeps_changes_dirty() {
        let store = MemoryAccessStore::new();
        let book = book();
        book.record("a", NET, Observation::heartbeat_rtt(10), 0);
        store.set_fail_writes(true);
        assert!(book.flush(&store, 0, true).await.is_err());
        store.set_fail_writes(false);
        assert_eq!(book.flush(&store, 1, true).await.unwrap(), 2);
    }
}

#[cfg(test)]
mod quic_silence_tests {
    use super::*;
    use crate::manifest::ClientPolicy;

    /// 被取消的静默 QUIC 只累计 UDP 证据：三次（期间 WS 成功）进入封锁，入口熔断不受影响。
    #[test]
    fn silent_quic_counts_towards_udp_block_but_not_the_breaker() {
        let book = HealthBook::new(&ClientPolicy::default());
        let net = NetPrint::from_parts(Some("wifi"), Some("gw"));
        let mut effects = Vec::new();
        for round in 0..3_i64 {
            let now = 1_000 + round * 10;
            effects.push(book.note_quic_silence(net, now).udp_blocked_until_ms);
            book.record(
                "ap-a",
                net,
                Observation::handshake_ok(Protocol::Ws, 30),
                now,
            );
        }
        assert_eq!(effects[0], None);
        assert_eq!(effects[1], None);
        assert!(effects[2].is_some(), "第三次静默（期间 WS 成功过）进入封锁");
        assert!(book.udp_blocked_until(net, 1_100).is_some());
        let view = book.view("ap-a", net, 1_100);
        assert_eq!(view.breaker, BreakerView::Closed);
        assert_eq!(view.failure_ratio, 0.0);
    }
}
