//! 主动探活的全局预算：每设备每小时至多 N 次（默认 30，清单可调，硬上限 120）。
//! 超出后只在用户主动操作（手动重连、打开线路诊断）时探测。

use std::collections::VecDeque;

use crate::time::HOUR_MS;

/// 预算记录的硬上限，与 `probe.budgetPerHour` 的上限一致：窗口长度有界，内存可预期。
pub const MAX_BUDGET_PER_HOUR: usize = 120;

/// 探活触发原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbeTrigger {
    /// 冷启动无可用样本：≤3 个并行。
    ColdStart,
    /// 故障切换前确认候选：≤3 个，间隔 250ms 交错。
    Failover,
    /// 后台复评（Wi-Fi 每 30 分钟一轮，蜂窝仅在评分劣化时）。
    Background,
    /// 恢复切回前对首选接入点的确认。
    Failback,
    /// 用户主动操作：不受预算限制（仍计入窗口）。
    UserInitiated,
}

impl ProbeTrigger {
    pub fn bypasses_budget(self) -> bool {
        matches!(self, ProbeTrigger::UserInitiated)
    }
}

/// 滑动一小时窗口。
#[derive(Debug, Default, Clone)]
pub struct ProbeBudget {
    stamps: VecDeque<i64>,
}

impl ProbeBudget {
    pub fn new() -> Self {
        Self::default()
    }

    fn evict(&mut self, now_ms: i64) {
        while self
            .stamps
            .front()
            .is_some_and(|stamp| now_ms.saturating_sub(*stamp) >= HOUR_MS)
        {
            self.stamps.pop_front();
        }
    }

    /// 本小时还能发起几次（非用户触发）。
    pub fn remaining(&mut self, now_ms: i64, per_hour: u32) -> u32 {
        self.evict(now_ms);
        let limit = (per_hour as usize).min(MAX_BUDGET_PER_HOUR);
        limit.saturating_sub(self.stamps.len()) as u32
    }

    /// 尝试消耗一次预算；成功返回 `true` 并记账。
    pub fn try_consume(&mut self, now_ms: i64, per_hour: u32, trigger: ProbeTrigger) -> bool {
        self.evict(now_ms);
        let limit = (per_hour as usize).min(MAX_BUDGET_PER_HOUR);
        if !trigger.bypasses_budget() && self.stamps.len() >= limit {
            return false;
        }
        if self.stamps.len() >= MAX_BUDGET_PER_HOUR {
            // 只有用户触发能走到这里：丢最旧的一条，窗口长度保持有界。
            self.stamps.pop_front();
        }
        self.stamps.push_back(now_ms);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hourly_window_and_user_bypass() {
        let mut budget = ProbeBudget::new();
        for at in 0..30 {
            assert!(budget.try_consume(at, 30, ProbeTrigger::Background));
        }
        assert!(!budget.try_consume(30, 30, ProbeTrigger::Failover));
        assert_eq!(budget.remaining(30, 30), 0);
        assert!(budget.try_consume(31, 30, ProbeTrigger::UserInitiated));
        // 用户触发也计入窗口：一小时整时只有 0 滑出，加上用户那次仍满。
        assert!(!budget.try_consume(HOUR_MS, 30, ProbeTrigger::Background));
        // 再过 1ms，1 也滑出。
        assert!(budget.try_consume(HOUR_MS + 1, 30, ProbeTrigger::Background));
        // 清单把预算调到 0：除用户触发外全部停止。
        let mut zero = ProbeBudget::new();
        assert!(!zero.try_consume(0, 0, ProbeTrigger::ColdStart));
        // 超过硬上限按上限计。
        let mut capped = ProbeBudget::new();
        for at in 0..120 {
            assert!(capped.try_consume(at, 10_000, ProbeTrigger::Background));
        }
        assert!(!capped.try_consume(121, 10_000, ProbeTrigger::Background));
    }
}
