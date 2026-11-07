//! 恢复切回（§3.7）：故障转移后，何时回到首选接入点。纯函数。
//!
//! 四个条件全部满足才切回：首选接入点连续 3 次探活成功且跨度 ≥ 5 分钟；评分比当前好 20% 以上；
//! 处于自然边界（刚回前台，或 60 秒内无发送与同步）；距上次切换 ≥ 10 分钟。
//! 条件严格是有意的：切回本身会断一次连接，宁可多留在次优入口，也不要来回抖动。

use crate::manifest::FailbackPolicy;
use crate::time::{MINUTE_MS, SECOND_MS};

/// 首选接入点需要的连续探活成功次数。
pub const FAILBACK_MIN_PROBES: u32 = 3;
/// 「60 秒内无发送与同步活动」视为自然边界。
pub const FAILBACK_IDLE_MS: i64 = MINUTE_MS;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FailbackInput {
    /// 首选接入点当前的连续探活成功次数。
    pub probe_successes: u32,
    /// 连续成功的第一次到现在的跨度。
    pub probe_span_ms: i64,
    /// 首选接入点评分（不含粘性加成）。
    pub preferred_score: f64,
    /// 当前接入点评分（不含粘性加成）。
    pub current_score: f64,
    /// 刚从后台回到前台。
    pub foregrounded: bool,
    /// 距最近一次发送或同步活动的时间。
    pub idle_ms: i64,
    /// 距上次切换的时间。
    pub since_last_switch_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailbackDecision {
    Switch,
    Stay(FailbackHold),
}

/// 不切回的原因（按检查顺序取第一个不满足的条件）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailbackHold {
    /// 已经在首选接入点上。
    AlreadyPreferred,
    /// 没有可用的首选接入点（全部熔断或不可用）。
    NoPreferred,
    NotEnoughProbes,
    ProbeSpanTooShort,
    NotBetterEnough,
    SwitchedRecently,
    NotAtNaturalBoundary,
}

impl FailbackHold {
    pub fn as_str(self) -> &'static str {
        match self {
            FailbackHold::AlreadyPreferred => "alreadyPreferred",
            FailbackHold::NoPreferred => "noPreferred",
            FailbackHold::NotEnoughProbes => "notEnoughProbes",
            FailbackHold::ProbeSpanTooShort => "probeSpanTooShort",
            FailbackHold::NotBetterEnough => "notBetterEnough",
            FailbackHold::SwitchedRecently => "switchedRecently",
            FailbackHold::NotAtNaturalBoundary => "notAtNaturalBoundary",
        }
    }
}

pub fn evaluate_failback(input: &FailbackInput, policy: &FailbackPolicy) -> FailbackDecision {
    use FailbackDecision::Stay;
    if input.probe_successes < FAILBACK_MIN_PROBES {
        return Stay(FailbackHold::NotEnoughProbes);
    }
    if input.probe_span_ms < i64::from(policy.min_healthy_span_sec) * SECOND_MS {
        return Stay(FailbackHold::ProbeSpanTooShort);
    }
    // 分数越小越好：「好 20% 以上」即 preferred ≤ current × (1 − 0.2)。
    if input.preferred_score > input.current_score * (1.0 - policy.min_improvement_ratio) {
        return Stay(FailbackHold::NotBetterEnough);
    }
    if input.since_last_switch_ms < i64::from(policy.min_interval_sec) * SECOND_MS {
        return Stay(FailbackHold::SwitchedRecently);
    }
    if !input.foregrounded && input.idle_ms < FAILBACK_IDLE_MS {
        return Stay(FailbackHold::NotAtNaturalBoundary);
    }
    FailbackDecision::Switch
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ClientPolicy;

    #[test]
    fn requires_every_condition() {
        let policy = ClientPolicy::default().failback;
        let ok = FailbackInput {
            probe_successes: 3,
            probe_span_ms: 300_000,
            preferred_score: 80.0,
            current_score: 100.0,
            foregrounded: false,
            idle_ms: 60_000,
            since_last_switch_ms: 600_000,
        };
        assert_eq!(evaluate_failback(&ok, &policy), FailbackDecision::Switch);
        let cases = [
            (
                FailbackInput {
                    probe_successes: 2,
                    ..ok
                },
                FailbackHold::NotEnoughProbes,
            ),
            (
                FailbackInput {
                    probe_span_ms: 299_999,
                    ..ok
                },
                FailbackHold::ProbeSpanTooShort,
            ),
            (
                FailbackInput {
                    preferred_score: 80.1,
                    ..ok
                },
                FailbackHold::NotBetterEnough,
            ),
            (
                FailbackInput {
                    since_last_switch_ms: 599_999,
                    ..ok
                },
                FailbackHold::SwitchedRecently,
            ),
            (
                FailbackInput {
                    idle_ms: 59_999,
                    ..ok
                },
                FailbackHold::NotAtNaturalBoundary,
            ),
        ];
        for (input, hold) in cases {
            assert_eq!(
                evaluate_failback(&input, &policy),
                FailbackDecision::Stay(hold)
            );
        }
        let foreground = FailbackInput {
            idle_ms: 0,
            foregrounded: true,
            ..ok
        };
        assert_eq!(
            evaluate_failback(&foreground, &policy),
            FailbackDecision::Switch
        );
    }
}
