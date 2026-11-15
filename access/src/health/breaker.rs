//! 接入点熔断器（每个接入点 × 网络指纹一份），§3.6 状态机：
//!
//! ```text
//! Closed   --(连续失败 ≥ 阈值 或 近 20 次失败率 ≥ 阈值)-->  Open（冷却 = 起始值）
//! Open     --(冷却到期)-->                                  HalfOpen
//! HalfOpen --(试探成功)-->                                  Closed（冷却复位）
//! HalfOpen --(试探失败)-->                                  Open（冷却 ×2，封顶）
//! ```
//!
//! 冷却时长每次实际生效时乘以 `[0.8, 1.2)` 的随机系数：大量客户端同时熔断同一个接入点时，
//! 抖动让它们的 half-open 试探在时间上散开，而不是在同一秒一起打回去。

use serde::{Deserialize, Serialize};

use crate::manifest::BreakerPolicy;
use crate::time::SECOND_MS;

/// half-open 试探的租约：认领后若一直没有回写结果（例如调用方崩溃或任务被取消），
/// 到期后允许下一个试探，避免永远卡在「有人在试」。
pub const TRIAL_LEASE_MS: i64 = 30 * SECOND_MS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BreakerPhase {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            BreakerPhase::Closed => "closed",
            BreakerPhase::Open => "open",
            BreakerPhase::HalfOpen => "halfOpen",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerTransition {
    /// Closed → Open。
    Opened { until_ms: i64, cooldown_ms: u64 },
    /// HalfOpen 试探失败 → Open，冷却翻倍。
    Reopened { until_ms: i64, cooldown_ms: u64 },
    /// HalfOpen 试探成功 → Closed。
    Closed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Breaker {
    /// `Some` 表示已熔断；到期后即为 half-open。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    open_until_ms: Option<i64>,
    /// 最近一次生效的冷却基值（未抖动），用于翻倍。
    #[serde(default)]
    cooldown_ms: u64,
    /// 进程内的试探认领，不持久化：重启后没有在途试探。
    #[serde(skip)]
    trial_claimed_at_ms: Option<i64>,
}

impl Breaker {
    pub fn phase(&self, now_ms: i64) -> BreakerPhase {
        match self.open_until_ms {
            None => BreakerPhase::Closed,
            Some(until) if now_ms < until => BreakerPhase::Open,
            Some(_) => BreakerPhase::HalfOpen,
        }
    }

    pub fn open_until_ms(&self) -> Option<i64> {
        self.open_until_ms
    }

    pub fn cooldown_ms(&self) -> u64 {
        self.cooldown_ms
    }

    /// half-open 且没有未过期的试探认领。
    pub fn trial_available(&self, now_ms: i64) -> bool {
        self.phase(now_ms) == BreakerPhase::HalfOpen
            && self
                .trial_claimed_at_ms
                .is_none_or(|claimed| now_ms.saturating_sub(claimed) >= TRIAL_LEASE_MS)
    }

    /// 认领 half-open 试探。Closed 不需要认领，直接放行；Open 一律拒绝。
    pub fn claim_trial(&mut self, now_ms: i64) -> bool {
        match self.phase(now_ms) {
            BreakerPhase::Closed => true,
            BreakerPhase::Open => false,
            BreakerPhase::HalfOpen => {
                if self.trial_available(now_ms) {
                    self.trial_claimed_at_ms = Some(now_ms);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// 提前结束冷却：仍在 Open 的熔断器此刻起即为 half-open，放一次试探。Closed 与已经 half-open 的
    /// 不变；冷却基值不动，试探再失败照常翻倍重新打开。返回是否真的提前了。
    pub fn expedite(&mut self, now_ms: i64) -> bool {
        if self.phase(now_ms) != BreakerPhase::Open {
            return false;
        }
        self.open_until_ms = Some(now_ms);
        self.trial_claimed_at_ms = None;
        true
    }

    /// 记一次计入熔断的失败。`trip` 由调用方按连续失败与窗口失败率算出（只在 Closed 时使用）；
    /// `unit` 是 `[0, 1)` 的随机数，用于冷却抖动。
    ///
    /// Open 期间的失败（例如熔断前已经发出的请求陆续超时）不改变状态，避免把冷却越推越远。
    pub fn on_failure(
        &mut self,
        now_ms: i64,
        trip: bool,
        policy: &BreakerPolicy,
        unit: f64,
    ) -> Option<BreakerTransition> {
        let base_ms = u64::from(policy.cooldown_base_sec) * 1_000;
        let max_ms = u64::from(policy.cooldown_max_sec) * 1_000;
        match self.phase(now_ms) {
            BreakerPhase::Closed if trip => {
                let until_ms = now_ms + jittered(base_ms, unit);
                self.open_until_ms = Some(until_ms);
                self.cooldown_ms = base_ms;
                self.trial_claimed_at_ms = None;
                Some(BreakerTransition::Opened {
                    until_ms,
                    cooldown_ms: base_ms,
                })
            }
            BreakerPhase::Closed | BreakerPhase::Open => None,
            BreakerPhase::HalfOpen => {
                let cooldown_ms = self.cooldown_ms.max(base_ms).saturating_mul(2).min(max_ms);
                let until_ms = now_ms + jittered(cooldown_ms, unit);
                self.open_until_ms = Some(until_ms);
                self.cooldown_ms = cooldown_ms;
                self.trial_claimed_at_ms = None;
                Some(BreakerTransition::Reopened {
                    until_ms,
                    cooldown_ms,
                })
            }
        }
    }

    /// 记一次成功。只有 half-open 会因此闭合；Open 期间的零星成功（迟到的在途请求）不提前闭合，
    /// 恢复必须经过一次真正的试探。
    pub fn on_success(&mut self, now_ms: i64) -> Option<BreakerTransition> {
        if self.phase(now_ms) == BreakerPhase::HalfOpen {
            *self = Breaker::default();
            Some(BreakerTransition::Closed)
        } else {
            None
        }
    }

    /// 不计入熔断的结果（过载、鉴权失败、排空、无网络……）：释放试探认领，状态不变。
    pub fn on_neutral(&mut self) {
        self.trial_claimed_at_ms = None;
    }
}

fn jittered(cooldown_ms: u64, unit: f64) -> i64 {
    let factor = 0.8 + 0.4 * crate::platform::clamp_unit(unit);
    (cooldown_ms as f64 * factor).round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ClientPolicy;

    /// 提前结束冷却：Open 立即转 half-open 放一次试探，试探失败照常翻倍；Closed 与 half-open 不动。
    #[test]
    fn expedite_turns_an_open_breaker_half_open_once() {
        let policy = ClientPolicy::default().breaker;
        let mut breaker = Breaker::default();
        assert!(!breaker.expedite(0), "closed stays closed");
        breaker.on_failure(0, true, &policy, 0.0);
        assert_eq!(breaker.phase(1_000), BreakerPhase::Open);
        assert!(breaker.expedite(1_000));
        assert_eq!(breaker.phase(1_000), BreakerPhase::HalfOpen);
        assert!(!breaker.expedite(1_000), "already half-open");
        assert!(breaker.claim_trial(1_000));
        assert!(!breaker.claim_trial(1_001), "one trial at a time");
        assert_eq!(
            breaker.on_failure(1_000, false, &policy, 0.5),
            Some(BreakerTransition::Reopened {
                until_ms: 61_000,
                cooldown_ms: 60_000
            })
        );
    }

    #[test]
    fn cooldown_doubles_to_cap_with_jitter_bounds() {
        let policy = ClientPolicy::default().breaker;
        let mut breaker = Breaker::default();
        // 起始 30s，unit=0 → ×0.8。
        assert_eq!(
            breaker.on_failure(0, true, &policy, 0.0),
            Some(BreakerTransition::Opened {
                until_ms: 24_000,
                cooldown_ms: 30_000
            })
        );
        assert_eq!(breaker.phase(23_999), BreakerPhase::Open);
        assert_eq!(breaker.phase(24_000), BreakerPhase::HalfOpen);
        // 试探失败：60s，unit=0.5 → ×1.0。
        assert!(breaker.claim_trial(24_000));
        assert!(!breaker.claim_trial(24_001));
        assert_eq!(
            breaker.on_failure(24_000, false, &policy, 0.5),
            Some(BreakerTransition::Reopened {
                until_ms: 84_000,
                cooldown_ms: 60_000
            })
        );
        // 继续翻倍直到上限 600s。
        let mut now = 84_000;
        for expected in [120_000u64, 240_000, 480_000, 600_000, 600_000] {
            let transition = breaker.on_failure(now, false, &policy, 0.5).unwrap();
            let BreakerTransition::Reopened {
                until_ms,
                cooldown_ms,
            } = transition
            else {
                panic!()
            };
            assert_eq!(cooldown_ms, expected);
            now = until_ms;
        }
        assert_eq!(breaker.on_success(now), Some(BreakerTransition::Closed));
        assert_eq!(breaker.phase(now), BreakerPhase::Closed);
        assert_eq!(breaker.cooldown_ms(), 0);
    }

    #[test]
    fn trial_lease_expires_and_neutral_releases() {
        let policy = ClientPolicy::default().breaker;
        let mut breaker = Breaker::default();
        breaker.on_failure(0, true, &policy, 0.5);
        let half_open = 30_000;
        assert!(breaker.claim_trial(half_open));
        assert!(!breaker.trial_available(half_open + TRIAL_LEASE_MS - 1));
        assert!(breaker.trial_available(half_open + TRIAL_LEASE_MS));
        breaker.on_neutral();
        assert!(breaker.trial_available(half_open + 1));
        // Open 期间成功不提前闭合。
        let mut fresh = Breaker::default();
        fresh.on_failure(0, true, &policy, 0.5);
        assert_eq!(fresh.on_success(1_000), None);
        assert_eq!(fresh.phase(1_000), BreakerPhase::Open);
    }
}
