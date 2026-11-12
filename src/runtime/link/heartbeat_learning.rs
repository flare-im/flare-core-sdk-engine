//! 后台心跳学习：按网络指纹记住这张网上的后台心跳能拉到多长。
//!
//! 后台心跳越稀，手机的蜂窝模块醒得越少；但拉得太长，运营商或路由器的 NAT 映射会先过期，连接悄悄
//! 死掉，要到下一次心跳才发现。各张网的 NAT 空闲超时差得很远，只能在本机按网络学：
//!
//! - 只在后台、只在保留链路的手机上学（iOS 进后台就托管，没有后台链路）；前台心跳不受影响；
//! - 第一次见到的网络从接入策略的后台间隔起步；在当前间隔上连续 3 次后台心跳得到应答，就把间隔
//!   拉长 30 秒，上限是策略允许的后台间隔上限（`POLICY_BOUNDS`）与 540 秒中较小的那个；
//! - 应用在后台空闲时连接在间隔 I 上死了：记下这张网的上限 I − 30 秒（不低于策略的后台间隔）和
//!   学到它的时刻，退到上限；之后在这张网上不再超过它。上限学到 7 天后作废：一次偶然的断连不能把一张网
//!   永远压在低处，作废后这张网退回策略的后台间隔，从那里重新探；
//! - 表按网络指纹存在数据根下，最多 64 张网，最久没用的先淘汰。还没报过网络（指纹未知）时不学，
//!   未知是所有网络共用的桶。
//!
//! 判死超时与前台间隔都不动：接入策略保证超时 ≥ 2 × 前台间隔、后台间隔 ≥ 前台间隔，学到的后台间隔
//! 不低于策略的后台间隔，这两条照样成立。

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use flare_access::NetPrint;
use flare_access::manifest::POLICY_BOUNDS;
use serde::{Deserialize, Serialize};

use super::driver::LinkHeartbeat;

/// 每次拉长多少秒；死了之后也按它退。
const STEP_SEC: u32 = 30;
/// 在当前间隔上连续多少次后台心跳得到应答才拉长。
const STREAK: u32 = 3;
/// 后台间隔最长到多少秒（策略上限更小时取策略的）。
const HARD_CAP_SEC: u32 = 540;
/// 最多记多少张网。
const MAX_NETWORKS: usize = 64;
/// 死过之后的上限管多久（毫秒）：7 天后作废，从策略的后台间隔重新探。
const CEILING_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// 学习表的文件（在数据根下）。
#[cfg(not(target_arch = "wasm32"))]
const LEARNING_FILE: &str = "link/heartbeat-learning.json";
/// 读学习表时的大小上限：64 张网用不了这么多。
#[cfg(not(target_arch = "wasm32"))]
const LEARNING_FILE_MAX_BYTES: u64 = 64 * 1024;
const LEARNING_VERSION: u32 = 1;

/// 后台间隔能学到的最长值（秒）：策略允许的后台间隔上限与 540 秒中较小的那个。
pub(crate) fn upper_bound_sec() -> u32 {
    POLICY_BOUNDS
        .iter()
        .find(|(name, _)| *name == "heartbeat.backgroundIntervalSec")
        .map_or(HARD_CAP_SEC, |(_, bound)| bound.max as u32)
        .min(HARD_CAP_SEC)
}

/// 在一张网上死过之后的上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Ceiling {
    /// 不再超过的后台间隔（秒）。
    #[serde(rename = "s")]
    sec: u32,
    /// 学到它的时刻（Unix 毫秒）。
    #[serde(rename = "t")]
    learned_ms: i64,
}

impl Ceiling {
    /// 学到超过 7 天（时钟往回拨了 7 天以上也算）：作废。
    fn expired(&self, now_ms: i64) -> bool {
        now_ms.abs_diff(self.learned_ms) >= CEILING_TTL_MS
    }
}

/// 一张网学到的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Learned {
    /// 学到的后台间隔（秒）。
    #[serde(rename = "i")]
    interval_sec: u32,
    /// 在这张网上死过之后的上限：7 天内不再超过它。
    #[serde(rename = "c", default, skip_serializing_if = "Option::is_none")]
    ceiling: Option<Ceiling>,
    /// 最近一次用到的时刻（Unix 毫秒）：表满时淘汰最久没用的。
    #[serde(rename = "u")]
    used_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LearningFile {
    v: u32,
    #[serde(default)]
    nets: BTreeMap<String, Learned>,
}

/// 这张网此刻该用的后台间隔：学到的值夹在策略的后台间隔与上限之间；没学过就是策略的后台间隔。
fn effective_sec(learned: Option<&Learned>, policy_sec: u32) -> u32 {
    let Some(learned) = learned else {
        return policy_sec;
    };
    let limit = learned
        .ceiling
        .map_or(u32::MAX, |ceiling| ceiling.sec)
        .min(upper_bound_sec())
        .max(policy_sec);
    learned.interval_sec.clamp(policy_sec, limit)
}

/// 连续成功之后的下一个间隔；已经到上限（策略上限、540 秒、死过的上限）时为 `None`。
fn extended_sec(current_sec: u32, policy_sec: u32, ceiling_sec: Option<u32>) -> Option<u32> {
    let limit = ceiling_sec
        .unwrap_or(u32::MAX)
        .min(upper_bound_sec())
        .max(policy_sec);
    let next = current_sec.max(policy_sec).saturating_add(STEP_SEC);
    (next <= limit).then_some(next)
}

/// 在间隔 `interval_sec` 上死了之后的上限：退一步，不低于策略的后台间隔，也不高于以前死过的上限。
fn ceiling_after_death(interval_sec: u32, policy_sec: u32, previous: Option<u32>) -> u32 {
    let ceiling = interval_sec.saturating_sub(STEP_SEC).max(policy_sec);
    previous.map_or(ceiling, |previous| ceiling.min(previous.max(policy_sec)))
}

/// 表里这张网学到的结果。上限已经作废的先清掉：间隔退回策略的后台间隔，从那里重新探。第二个值是这次
/// 有没有清掉一个作废的上限（要写回）。
fn current(
    table: &mut BTreeMap<NetPrint, Learned>,
    net: NetPrint,
    policy_sec: u32,
    now_ms: i64,
) -> (Option<&mut Learned>, bool) {
    let Some(learned) = table.get_mut(&net) else {
        return (None, false);
    };
    let expired = learned
        .ceiling
        .is_some_and(|ceiling| ceiling.expired(now_ms));
    if expired {
        learned.ceiling = None;
        learned.interval_sec = policy_sec;
    }
    (Some(learned), expired)
}

/// 按网络指纹记的后台心跳学习表，跨登录共用（属于设备与网络，与健康簿一样）。
#[derive(Default)]
pub(crate) struct HeartbeatLearning {
    table: Mutex<BTreeMap<NetPrint, Learned>>,
    /// 原生端落盘的位置；没有数据根（测试、浏览器）时只在内存里。
    #[cfg(not(target_arch = "wasm32"))]
    file: Mutex<Option<std::path::PathBuf>>,
}

impl HeartbeatLearning {
    fn table(&self) -> std::sync::MutexGuard<'_, BTreeMap<NetPrint, Learned>> {
        self.table
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 从数据根下读出学习表，之后的变化都写回去。读不出来（没有、坏了）就从空表开始。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn persist_under(&self, data_root: &std::path::Path) {
        use std::io::Read;

        let path = data_root.join(LEARNING_FILE);
        let loaded = std::fs::File::open(&path).ok().and_then(|file| {
            let mut bytes = Vec::new();
            file.take(LEARNING_FILE_MAX_BYTES)
                .read_to_end(&mut bytes)
                .ok()?;
            serde_json::from_slice::<LearningFile>(&bytes).ok()
        });
        if let Some(loaded) = loaded.filter(|loaded| loaded.v == LEARNING_VERSION) {
            let mut table = self.table();
            for (key, learned) in loaded.nets {
                if let Some(net) = NetPrint::from_hex(&key).filter(|net| *net != NetPrint::UNKNOWN)
                {
                    table.insert(net, learned);
                }
            }
            evict(&mut table, MAX_NETWORKS);
        }
        *self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(path);
    }

    /// 网络 `net` 上该用的后台间隔（秒），顺手记下这张网刚被用过。指纹未知时用策略的后台间隔；上限
    /// 学到超过 7 天的作废，用策略的后台间隔重新探。
    pub(crate) fn interval_for(&self, net: NetPrint, policy_sec: u32, now_ms: i64) -> u32 {
        if net == NetPrint::UNKNOWN {
            return policy_sec;
        }
        let (interval, expired) = {
            let mut table = self.table();
            let (learned, expired) = current(&mut table, net, policy_sec, now_ms);
            let interval = effective_sec(learned.as_deref(), policy_sec);
            if let Some(learned) = learned {
                learned.used_ms = now_ms;
            }
            (interval, expired)
        };
        if expired {
            self.save();
        }
        interval
    }

    /// 在 `current_sec` 上连续成功了：能拉长就拉长并记下，返回新的间隔；已到上限时为 `None`。这张网的
    /// 上限学到超过 7 天作废了时，这条连接也退回策略的后台间隔重新探（返回它；本来就在那里时为 `None`）。
    pub(crate) fn extend(
        &self,
        net: NetPrint,
        policy_sec: u32,
        current_sec: u32,
        now_ms: i64,
    ) -> Option<u32> {
        if net == NetPrint::UNKNOWN {
            return None;
        }
        let (next, changed) = {
            let mut table = self.table();
            let (learned, expired) = current(&mut table, net, policy_sec, now_ms);
            if expired {
                if let Some(learned) = learned {
                    learned.used_ms = now_ms;
                }
                ((current_sec != policy_sec).then_some(policy_sec), true)
            } else {
                let ceiling = learned.and_then(|learned| learned.ceiling);
                match extended_sec(current_sec, policy_sec, ceiling.map(|ceiling| ceiling.sec)) {
                    Some(next) => {
                        upsert(
                            &mut table,
                            net,
                            Learned {
                                interval_sec: next,
                                ceiling,
                                used_ms: now_ms,
                            },
                        );
                        (Some(next), true)
                    }
                    None => (None, false),
                }
            }
        };
        if changed {
            self.save();
        }
        next
    }

    /// 后台空闲时连接在 `interval_sec` 上死了：记下上限与学到它的时刻并退到它，返回退到的间隔。
    pub(crate) fn died_at(
        &self,
        net: NetPrint,
        policy_sec: u32,
        interval_sec: u32,
        now_ms: i64,
    ) -> u32 {
        if net == NetPrint::UNKNOWN {
            return policy_sec;
        }
        let ceiling = {
            let mut table = self.table();
            // 作废了的上限不算以前的上限
            let (learned, _) = current(&mut table, net, policy_sec, now_ms);
            let previous = learned
                .and_then(|learned| learned.ceiling)
                .map(|ceiling| ceiling.sec);
            let ceiling = ceiling_after_death(interval_sec, policy_sec, previous);
            upsert(
                &mut table,
                net,
                Learned {
                    interval_sec: ceiling,
                    ceiling: Some(Ceiling {
                        sec: ceiling,
                        learned_ms: now_ms,
                    }),
                    used_ms: now_ms,
                },
            );
            ceiling
        };
        self.save();
        ceiling
    }

    /// 整张表写回数据根。表很小（几 KiB），只在拉长或死掉时写，就地写完，免得两次写乱序。
    fn save(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(path) = self
                .file
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            else {
                return;
            };
            let file = LearningFile {
                v: LEARNING_VERSION,
                nets: self
                    .table()
                    .iter()
                    .map(|(net, learned)| (net.to_hex(), *learned))
                    .collect(),
            };
            let Ok(bytes) = serde_json::to_vec(&file) else {
                return;
            };
            if let Err(error) = crate::shared::util::atomic_file::write_atomic(&path, &bytes) {
                tracing::debug!(%error, "saving the heartbeat learning table failed");
            }
        }
    }
}

/// 写入一张网的结果；表满时先淘汰最久没用的那张。
fn upsert(table: &mut BTreeMap<NetPrint, Learned>, net: NetPrint, learned: Learned) {
    if !table.contains_key(&net) {
        evict(table, MAX_NETWORKS - 1);
    }
    table.insert(net, learned);
}

/// 淘汰最久没用的，直到不超过 `keep` 张。
fn evict(table: &mut BTreeMap<NetPrint, Learned>, keep: usize) {
    while table.len() > keep {
        let Some(oldest) = table
            .iter()
            .min_by_key(|(_, learned)| learned.used_ms)
            .map(|(net, _)| *net)
        else {
            return;
        };
        table.remove(&oldest);
    }
}

/// 一条连接上的后台心跳学习进度（状态机持有）。
#[derive(Debug, Default)]
pub(crate) struct HeartbeatTrial {
    /// 这条连接的代数；连接换了，进度作废。
    epoch: Option<u64>,
    /// 这条连接所在的网络。
    net: NetPrint,
    /// 这条连接正在用的心跳节奏（后台间隔就是正在试的间隔）。
    heartbeat: Option<LinkHeartbeat>,
    /// 这条连接的验活窗口：换节奏时原样带上。
    probe_timeout: Duration,
    /// 接入策略的后台间隔（秒）：学到的值不低于它。
    policy_sec: u32,
    /// 在当前间隔上连续得到应答的后台心跳次数。
    streak: u32,
    /// 最近一次知道连接还活着的时刻（后台心跳得到应答、转入后台、换上新间隔），Unix 毫秒。
    alive_ms: i64,
}

/// 一次后台心跳之后要做的事。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BeatOutcome {
    /// 照常计数。
    Counted,
    /// 拉长了：把新的节奏交给当前连接。
    Retune(LinkHeartbeat),
}

impl HeartbeatTrial {
    /// 新连接上线：从它建连时用的节奏开始计。
    pub(crate) fn begin(
        &mut self,
        epoch: u64,
        net: NetPrint,
        heartbeat: LinkHeartbeat,
        probe_timeout: Duration,
        policy_sec: u32,
        now_ms: i64,
    ) {
        *self = Self {
            epoch: Some(epoch),
            net,
            heartbeat: Some(heartbeat),
            probe_timeout,
            policy_sec,
            streak: 0,
            alive_ms: now_ms,
        };
    }

    /// 连接没了：进度作废。
    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }

    /// 转入后台（或回到前台）：连续计数从头开始。
    pub(crate) fn restart(&mut self, now_ms: i64) {
        self.streak = 0;
        self.alive_ms = now_ms;
    }

    /// 这条连接正在试的后台间隔（秒）。
    fn interval_sec(&self) -> Option<u32> {
        self.heartbeat
            .map(|heartbeat| heartbeat.background_interval.as_secs() as u32)
    }

    /// 换上新的后台间隔，返回完整的节奏。
    fn with_interval(&mut self, interval_sec: u32, now_ms: i64) -> Option<LinkHeartbeat> {
        let heartbeat = self.heartbeat.as_mut()?;
        heartbeat.background_interval =
            Duration::from_secs(u64::from(interval_sec)).max(heartbeat.interval);
        self.streak = 0;
        self.alive_ms = now_ms;
        Some(*heartbeat)
    }

    /// 后台里第 `epoch` 条连接的一次心跳得到了应答。
    pub(crate) fn on_beat(
        &mut self,
        learning: &HeartbeatLearning,
        epoch: u64,
        now_ms: i64,
    ) -> Option<BeatOutcome> {
        if self.epoch != Some(epoch) {
            return None;
        }
        self.alive_ms = now_ms;
        self.streak = self.streak.saturating_add(1);
        if self.streak < STREAK {
            return Some(BeatOutcome::Counted);
        }
        self.streak = 0;
        let current = self.interval_sec()?;
        match learning.extend(self.net, self.policy_sec, current, now_ms) {
            Some(next) => self.with_interval(next, now_ms).map(BeatOutcome::Retune),
            None => Some(BeatOutcome::Counted),
        }
    }

    /// 第 `epoch` 条连接在后台丢了。`idle_ms` 是丢之前多久没有收发业务数据。是空闲时 NAT 过期的
    /// 样子就记下上限，返回退到的间隔（秒）。
    ///
    /// 只认这一种死法：连接从上一次知道还活着到丢掉不超过两个间隔加判死超时（再长说明进程被冻结过，
    /// 这次断开说明不了间隔），丢之前至少一个间隔没有业务数据。
    pub(crate) fn on_idle_death(
        &mut self,
        learning: &HeartbeatLearning,
        epoch: u64,
        idle_ms: i64,
        now_ms: i64,
    ) -> Option<u32> {
        if self.epoch != Some(epoch) {
            return None;
        }
        let heartbeat = self.heartbeat?;
        let interval_sec = self.interval_sec()?;
        let interval_ms = i64::from(interval_sec) * 1_000;
        let window_ms = 2 * interval_ms + heartbeat.timeout.as_millis() as i64 + 30_000;
        let silent_ms = now_ms.saturating_sub(self.alive_ms);
        if silent_ms > window_ms || idle_ms < interval_ms {
            return None;
        }
        Some(learning.died_at(self.net, self.policy_sec, interval_sec, now_ms))
    }

    /// 网络变了而连接还在：换上新网络学到的间隔。变了就返回新的节奏。
    pub(crate) fn on_network(
        &mut self,
        learning: &HeartbeatLearning,
        net: NetPrint,
        now_ms: i64,
    ) -> Option<LinkHeartbeat> {
        self.epoch?;
        if self.net == net {
            return None;
        }
        self.net = net;
        let interval = learning.interval_for(net, self.policy_sec, now_ms);
        if Some(interval) == self.interval_sec() {
            self.restart(now_ms);
            return None;
        }
        self.with_interval(interval, now_ms)
    }

    /// 换节奏时要带上的验活窗口。
    pub(crate) fn probe_timeout(&self) -> Duration {
        self.probe_timeout
    }
}

/// 建连时的心跳节奏：在学的平台上，后台间隔换成这张网学到的值。
pub(crate) fn learned_heartbeat(
    learning: &HeartbeatLearning,
    heartbeat: LinkHeartbeat,
    net: NetPrint,
    now_ms: i64,
) -> LinkHeartbeat {
    let policy_sec = heartbeat.background_interval.as_secs() as u32;
    let learned = learning.interval_for(net, policy_sec, now_ms);
    LinkHeartbeat {
        background_interval: Duration::from_secs(u64::from(learned)).max(heartbeat.interval),
        ..heartbeat
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: u32 = 120;

    fn net(hint: &str) -> NetPrint {
        NetPrint::from_parts(Some("cellular"), Some(hint))
    }

    fn policy_heartbeat() -> LinkHeartbeat {
        LinkHeartbeat::from_policy(&flare_access::ClientPolicy::default())
    }

    /// 上限取策略允许的后台间隔上限与 540 秒中较小的那个。
    #[test]
    fn the_upper_bound_is_the_policy_bound_capped_at_540() {
        assert_eq!(upper_bound_sec(), 540);
    }

    /// 连续 3 次成功拉长 30 秒，一直到上限；没学过的网从策略的后台间隔起步。
    #[test]
    fn three_answered_beats_extend_the_interval_up_to_the_bound() {
        let learning = HeartbeatLearning::default();
        let mut trial = HeartbeatTrial::default();
        let heartbeat = learned_heartbeat(&learning, policy_heartbeat(), net("460-01"), 0);
        assert_eq!(heartbeat.background_interval, Duration::from_secs(120));
        trial.begin(
            1,
            net("460-01"),
            heartbeat,
            Duration::from_secs(5),
            POLICY,
            0,
        );
        assert_eq!(trial.on_beat(&learning, 1, 1), Some(BeatOutcome::Counted));
        assert_eq!(trial.on_beat(&learning, 1, 2), Some(BeatOutcome::Counted));
        let Some(BeatOutcome::Retune(retuned)) = trial.on_beat(&learning, 1, 3) else {
            panic!("the third beat extends the interval");
        };
        assert_eq!(retuned.background_interval, Duration::from_secs(150));
        assert_eq!(retuned.interval, heartbeat.interval, "前台间隔不动");
        assert_eq!(retuned.timeout, heartbeat.timeout, "判死超时不动");
        assert!(retuned.timeout >= retuned.interval * 2);
        assert!(retuned.background_interval >= retuned.interval);
        for beat in 0..200 {
            trial.on_beat(&learning, 1, 10 + beat);
        }
        assert_eq!(trial.interval_sec(), Some(540));
        assert_eq!(learning.interval_for(net("460-01"), POLICY, 1_000), 540);
        assert_eq!(
            trial.on_beat(&learning, 2, 2_000),
            None,
            "别的连接的心跳不算"
        );
    }

    /// 空闲时在 I 上死了：上限 I − 30 秒，退到它，之后不再超过；不低于策略的后台间隔。
    #[test]
    fn an_idle_death_sets_a_ceiling_one_step_below() {
        let learning = HeartbeatLearning::default();
        let wifi = net("192.168.1");
        let mut trial = HeartbeatTrial::default();
        let mut heartbeat = policy_heartbeat();
        heartbeat.background_interval = Duration::from_secs(240);
        trial.begin(7, wifi, heartbeat, Duration::from_secs(5), POLICY, 0);
        let interval_ms = 240_000;
        assert_eq!(
            trial.on_idle_death(&learning, 7, interval_ms, interval_ms * 2),
            Some(210)
        );
        assert_eq!(learning.interval_for(wifi, POLICY, 1), 210);
        assert_eq!(
            extended_sec(210, POLICY, Some(210)),
            None,
            "不再超过死过的上限"
        );
        assert_eq!(
            ceiling_after_death(120, POLICY, None),
            POLICY,
            "不低于策略的后台间隔"
        );
        assert_eq!(
            ceiling_after_death(300, POLICY, Some(180)),
            180,
            "不高于以前的上限"
        );
    }

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

    /// 死过的上限只管 7 天：到期作废，这张网退回策略的后台间隔，从那里重新探，能越过以前的上限。
    #[test]
    fn a_ceiling_expires_after_seven_days() {
        let learning = HeartbeatLearning::default();
        let wifi = net("wifi-1");
        let learned_at = 1_000;
        assert_eq!(learning.died_at(wifi, POLICY, 240, learned_at), 210);
        assert_eq!(
            learning.interval_for(wifi, POLICY, learned_at + 7 * DAY_MS - 1),
            210
        );
        assert_eq!(
            learning.extend(wifi, POLICY, 210, learned_at + 7 * DAY_MS - 1),
            None,
            "7 天之内照样不越过上限"
        );
        assert_eq!(
            learning.interval_for(wifi, POLICY, learned_at + 7 * DAY_MS),
            POLICY,
            "到期作废，从策略的后台间隔重新探"
        );
        let mut current = POLICY;
        let mut now = learned_at + 7 * DAY_MS;
        while let Some(next) = learning.extend(wifi, POLICY, current, now) {
            current = next;
            now += 1;
        }
        assert_eq!(current, upper_bound_sec(), "作废之后能越过以前的上限");
    }

    /// 时钟往回拨了 7 天以上，上限同样作废，不会被卡住。
    #[test]
    fn a_ceiling_from_the_far_future_expires_too() {
        let learning = HeartbeatLearning::default();
        let wifi = net("wifi-2");
        learning.died_at(wifi, POLICY, 300, 30 * DAY_MS);
        assert_eq!(
            learning.interval_for(wifi, POLICY, 30 * DAY_MS - DAY_MS),
            270
        );
        assert_eq!(
            learning.interval_for(wifi, POLICY, 30 * DAY_MS - 8 * DAY_MS),
            POLICY
        );
    }

    /// 连接还在而上限到期：下一次拉长时这条连接也退回策略的后台间隔，从头再探。
    #[test]
    fn a_live_link_restarts_from_the_policy_interval_when_its_ceiling_expires() {
        let learning = HeartbeatLearning::default();
        let wifi = net("wifi-3");
        learning.died_at(wifi, POLICY, 240, 0);
        let mut trial = HeartbeatTrial::default();
        let heartbeat = learned_heartbeat(&learning, policy_heartbeat(), wifi, 1);
        assert_eq!(heartbeat.background_interval, Duration::from_secs(210));
        trial.begin(1, wifi, heartbeat, Duration::from_secs(5), POLICY, 1);
        let later = 7 * DAY_MS;
        trial.on_beat(&learning, 1, later);
        trial.on_beat(&learning, 1, later + 1);
        let Some(BeatOutcome::Retune(retuned)) = trial.on_beat(&learning, 1, later + 2) else {
            panic!("the expired ceiling sends the link back to the policy interval");
        };
        assert_eq!(
            retuned.background_interval,
            Duration::from_secs(u64::from(POLICY))
        );
        assert_eq!(learning.interval_for(wifi, POLICY, later + 3), POLICY);
    }

    /// 新的一次死亡重新记时刻：上限从那次起再管 7 天。
    #[test]
    fn a_new_death_restarts_the_seven_days() {
        let learning = HeartbeatLearning::default();
        let wifi = net("wifi-4");
        learning.died_at(wifi, POLICY, 300, 0);
        assert_eq!(learning.died_at(wifi, POLICY, 270, 6 * DAY_MS), 240);
        assert_eq!(learning.interval_for(wifi, POLICY, 12 * DAY_MS), 240);
        assert_eq!(learning.interval_for(wifi, POLICY, 13 * DAY_MS), POLICY);
    }

    /// 冻结过（沉默远超两个间隔加超时）或丢之前还有业务数据：说明不了间隔，不记。
    #[test]
    fn deaths_that_say_nothing_about_the_interval_are_ignored() {
        let learning = HeartbeatLearning::default();
        let mut trial = HeartbeatTrial::default();
        trial.begin(
            1,
            net("a"),
            policy_heartbeat(),
            Duration::from_secs(5),
            POLICY,
            0,
        );
        assert_eq!(
            trial.on_idle_death(&learning, 1, 3_600_000, 3_600_000),
            None
        );
        assert_eq!(trial.on_idle_death(&learning, 1, 1_000, 200_000), None);
        assert_eq!(trial.on_idle_death(&learning, 2, 200_000, 200_000), None);
        assert_eq!(learning.interval_for(net("a"), POLICY, 1), POLICY);
    }

    /// 指纹未知时不学：所有网络共用这个桶。
    #[test]
    fn an_unknown_network_is_not_learned() {
        let learning = HeartbeatLearning::default();
        assert_eq!(learning.extend(NetPrint::UNKNOWN, POLICY, POLICY, 0), None);
        assert_eq!(learning.died_at(NetPrint::UNKNOWN, POLICY, 300, 0), POLICY);
        assert_eq!(learning.interval_for(NetPrint::UNKNOWN, POLICY, 0), POLICY);
    }

    /// 表最多 64 张网，满了淘汰最久没用的。
    #[test]
    fn the_table_keeps_the_64_most_recently_used_networks() {
        let learning = HeartbeatLearning::default();
        for index in 0..70 {
            learning.extend(net(&format!("net-{index}")), POLICY, POLICY, index);
        }
        assert_eq!(learning.table().len(), MAX_NETWORKS);
        assert_eq!(
            learning.interval_for(net("net-0"), POLICY, 100),
            POLICY,
            "最早的被淘汰"
        );
        assert_eq!(learning.interval_for(net("net-69"), POLICY, 100), 150);
    }

    /// 网络变了而连接还在：换上新网络学到的间隔，计数从头来。
    #[test]
    fn a_network_change_retunes_the_live_link() {
        let learning = HeartbeatLearning::default();
        learning.extend(net("b"), POLICY, 270, 0);
        let mut trial = HeartbeatTrial::default();
        trial.begin(
            1,
            net("a"),
            policy_heartbeat(),
            Duration::from_secs(5),
            POLICY,
            0,
        );
        let retuned = trial.on_network(&learning, net("b"), 10).expect("retuned");
        assert_eq!(retuned.background_interval, Duration::from_secs(300));
        assert_eq!(trial.on_network(&learning, net("b"), 20), None);
    }

    /// 落盘再读回来：学到的间隔、上限与学到上限的时刻都在。
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn the_table_survives_a_restart() {
        let root = std::env::temp_dir().join(format!("flare-hb-{}", uuid::Uuid::new_v4()));
        let learning = HeartbeatLearning::default();
        learning.persist_under(&root);
        learning.extend(net("c"), POLICY, 180, 1);
        learning.died_at(net("d"), POLICY, 300, 2);
        let reloaded = HeartbeatLearning::default();
        reloaded.persist_under(&root);
        assert_eq!(reloaded.interval_for(net("c"), POLICY, 3), 210);
        assert_eq!(reloaded.interval_for(net("d"), POLICY, 3), 270);
        assert_eq!(
            reloaded
                .table()
                .get(&net("d"))
                .and_then(|learned| learned.ceiling),
            Some(Ceiling {
                sec: 270,
                learned_ms: 2
            })
        );
        assert_eq!(
            reloaded.interval_for(net("d"), POLICY, 2 + 7 * DAY_MS),
            POLICY,
            "读回来的上限照样按学到的时刻作废"
        );
        let again = HeartbeatLearning::default();
        again.persist_under(&root);
        assert_eq!(
            again
                .table()
                .get(&net("d"))
                .and_then(|learned| learned.ceiling),
            None,
            "作废写回了"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
