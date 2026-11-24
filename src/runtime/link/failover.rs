//! 故障转移决策（§3.6）：纯函数，不做 I/O、不读时钟，随机数由调用方传入。
//!
//! 状态机（`LinkSupervisor`）只负责执行；「失败算不算入口的错」「下一步是立即换候选、原地等、
//! 退避还是终止」全部在这里决定，所以每一行决策表都能用确定的输入单测。

use std::time::Duration;

use flare_core::client::link::{DisconnectReason, ServerRejection};
use flare_core::common::error::ErrorCode;

use super::status::{ActionRequiredReason, LinkPhase};

/// 连续这么多轮候选全部失败，进入 Recovering（对外 serviceUnavailable）。
pub const RECOVERING_AFTER_FAILED_ROUNDS: u32 = 2;
/// Recovering 期间的退避上限（§3.11：引导失败退避，上限 5 分钟）。
pub const RECOVERING_CAP_MS: u64 = 300_000;
/// 手机上应用在后台、链路断开之后，引擎最多自己再连这么多轮；用完就停下，等回到前台、网络恢复或
/// 显式的重连请求。后台没有界面在等，按退避一直空转只是耗电，而提示此时已由推送通道负责。
pub const BACKGROUND_RECONNECT_ROUNDS: u32 = 3;

/// 没有人在等的时候（应用在后台）还能不能自己再安排一轮。`background_rounds` 是后台里已经发起的
/// 轮数。前台不受限；后台也只有推送能接手提示的平台（`push_takes_over`，即手机）才受限：桌面与 Web
/// 没有推送，停止重连就等于收不到消息，所以照常按退避重连。
pub fn unattended_round_allowed(
    foreground: bool,
    push_takes_over: bool,
    background_rounds: u32,
) -> bool {
    foreground || !push_takes_over || background_rounds < BACKGROUND_RECONNECT_ROUNDS
}

/// 服务端拒绝 CONNECT 的处理方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionKind {
    /// 过载：同一接入点等 `retry_after` 再试，不换入口、不计熔断（防止级联迁移）。
    Overloaded { retry_after_ms: Option<u64> },
    /// 令牌问题：续期一次后重连，不计熔断。
    Auth,
    /// 只能由用户动作恢复。
    Terminal(ActionRequiredReason),
    /// 换下一个候选。`counted` 为是否计入该入口的失败（握手中收到的计划性关闭不计）。
    Retry,
}

pub fn classify_rejection(rejection: &ServerRejection) -> RejectionKind {
    if rejection.is_overloaded() {
        return RejectionKind::Overloaded {
            retry_after_ms: rejection.retry_after_ms,
        };
    }
    match rejection.code {
        ErrorCode::UpgradeRequired | ErrorCode::ProtocolVersionMismatch => {
            RejectionKind::Terminal(ActionRequiredReason::UpgradeRequired)
        }
        ErrorCode::PermissionDenied => {
            RejectionKind::Terminal(ActionRequiredReason::TenantUnavailable)
        }
        ErrorCode::AuthenticationFailed
        | ErrorCode::AuthenticationExpired
        | ErrorCode::AuthenticationInvalid
        | ErrorCode::AuthenticationRequired
        | ErrorCode::TokenInvalid
        | ErrorCode::TokenExpired => RejectionKind::Auth,
        _ => RejectionKind::Retry,
    }
}

/// 一条已上线的连接为什么结束。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkLoss {
    /// 我方断开（主动断开、切换时关掉旧连接）：状态机自己发起的，不触发恢复。
    ClientRequested,
    /// 计入熔断的失败：心跳判死、对端关闭、读写出错。立即换候选。
    Failed,
    /// 服务端计划性关闭（排空、停机、再均衡）：窗口内随机时刻重连，优先 `prefer`；不计熔断。
    Planned { window_ms: Option<(u64, u64)> },
    /// 结束会话的踢线：终止，只能由用户动作离开。
    SessionEnded(ActionRequiredReason),
    /// 不结束会话的踢线（例如同设备替换、运维踢线）：按退避重连，不计熔断。
    Kicked,
}

pub fn classify_loss(reason: &DisconnectReason) -> LinkLoss {
    match reason {
        DisconnectReason::ClientRequested => LinkLoss::ClientRequested,
        DisconnectReason::ServerClose(close) => LinkLoss::Planned {
            window_ms: close
                .retry_window()
                .map(|(min, max)| (min.as_millis() as u64, max.as_millis() as u64)),
        },
        // 在线连接被终止类拒绝（升级门槛抬高）：与建连时被拒同样终止；其余拒绝类别不会以此上报。
        DisconnectReason::Rejected(rejection) => match classify_rejection(rejection) {
            RejectionKind::Terminal(reason) => LinkLoss::SessionEnded(reason),
            _ => LinkLoss::Failed,
        },
        DisconnectReason::Kicked {
            reason,
            ends_session: true,
        } => LinkLoss::SessionEnded(
            if reason.trim() == crate::shared::error::TENANT_UNAVAILABLE {
                ActionRequiredReason::TenantUnavailable
            } else {
                ActionRequiredReason::Kicked
            },
        ),
        DisconnectReason::Kicked { .. } => LinkLoss::Kicked,
        DisconnectReason::HeartbeatTimeout => LinkLoss::Failed,
        DisconnectReason::PeerClosed { .. } | DisconnectReason::TransportError { .. } => {
            LinkLoss::Failed
        }
    }
}

/// 去相关抖动退避（decorrelated jitter）：`sleep = min(cap, random_between(base, prev × 3))`。
///
/// 比「指数 + 80–120% 抖动」在大规模重连时分布更均匀，也不会让大量客户端在同一个上限值处对齐。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Backoff {
    prev_ms: u64,
}

impl Backoff {
    pub fn reset(&mut self) {
        self.prev_ms = 0;
    }

    /// 下一次等待（毫秒）。`unit` 为 `[0, 1)` 的随机数。
    pub fn next(&mut self, base_ms: u64, cap_ms: u64, unit: f64) -> u64 {
        let base = base_ms.max(1);
        let cap = cap_ms.max(base);
        let prev = self.prev_ms.max(base);
        let high = prev.saturating_mul(3).max(base);
        let sleep = between(base, high, unit).min(cap);
        self.prev_ms = sleep;
        sleep
    }
}

/// `[low, high]` 中按 `unit ∈ [0,1)` 取值。
pub fn between(low: u64, high: u64, unit: f64) -> u64 {
    if high <= low {
        return low;
    }
    let unit = if unit.is_nan() {
        0.0
    } else {
        unit.clamp(0.0, 1.0)
    };
    low + ((high - low) as f64 * unit).round() as u64
}

#[derive(Debug, Clone, Copy)]
pub struct BackoffPolicy {
    pub base_ms: u32,
    pub cap_foreground_ms: u32,
    pub cap_background_ms: u32,
}

pub const CONNECTION_BACKOFF: BackoffPolicy = BackoffPolicy {
    base_ms: 1_000,
    cap_foreground_ms: 60_000,
    cap_background_ms: 300_000,
};

/// 退避的上下文。
#[derive(Debug, Clone, Copy)]
pub struct RetryContext {
    pub foreground: bool,
    /// 连续全部失败的轮数（含这一轮）。
    pub failed_rounds: u32,
    /// 服务端给出的重连窗口（计划性关闭、过载）。
    pub server_window_ms: Option<(u64, u64)>,
    pub now_ms: i64,
}

/// 下一轮的安排。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPlan {
    pub at_ms: i64,
    /// Backoff 或 Recovering。
    pub phase: LinkPhase,
}

/// 一轮连接失败后何时再试：去相关抖动，与服务端窗口取大。
pub fn plan_retry(
    backoff: &mut Backoff,
    policy: &BackoffPolicy,
    context: RetryContext,
    jitter_unit: f64,
    window_unit: f64,
) -> RetryPlan {
    let recovering = context.failed_rounds >= RECOVERING_AFTER_FAILED_ROUNDS;
    let cap = if recovering {
        RECOVERING_CAP_MS.max(u64::from(policy.cap_background_ms).min(RECOVERING_CAP_MS))
    } else if context.foreground {
        u64::from(policy.cap_foreground_ms)
    } else {
        u64::from(policy.cap_background_ms)
    };
    let mut sleep = backoff.next(u64::from(policy.base_ms), cap, jitter_unit);
    if let Some((min, max)) = context.server_window_ms {
        sleep = sleep.max(between(min, max.max(min), window_unit));
    }
    let at = context.now_ms.saturating_add(sleep as i64);
    RetryPlan {
        at_ms: at,
        phase: if recovering {
            LinkPhase::Recovering
        } else {
            LinkPhase::Backoff
        },
    }
}

pub fn duration_ms(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

pub const DEFAULT_OVERLOAD_WAIT_MS: u64 = 2_000;

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::client::link::ServerClose;

    fn rejection(code: ErrorCode, reason: &str) -> ServerRejection {
        ServerRejection {
            code,
            reason: reason.into(),
            retry_after_ms: None,
            min_build: None,
            release_hint: None,
        }
    }

    fn policy() -> BackoffPolicy {
        BackoffPolicy {
            base_ms: 1_000,
            cap_foreground_ms: 60_000,
            cap_background_ms: 300_000,
        }
    }

    /// 去相关抖动：首次落在 [base, 3·base]，之后不超过上一次的 3 倍，且永不超过上限。
    #[test]
    fn decorrelated_jitter_stays_within_bounds() {
        let mut backoff = Backoff::default();
        assert_eq!(backoff.next(1_000, 60_000, 0.0), 1_000);
        let mut backoff = Backoff::default();
        assert_eq!(backoff.next(1_000, 60_000, 1.0), 3_000);
        assert_eq!(backoff.next(1_000, 60_000, 1.0), 9_000);
        for _ in 0..10 {
            backoff.next(1_000, 60_000, 1.0);
        }
        assert_eq!(backoff.next(1_000, 60_000, 1.0), 60_000, "上限封顶");
        backoff.reset();
        assert_eq!(backoff.next(1_000, 60_000, 0.5), 2_000);
    }

    #[test]
    fn retries_respect_server_windows_and_recovering() {
        let now = 1_000_000;
        let mut backoff = Backoff::default();
        // 服务端窗口比退避长：取窗口。
        let plan = plan_retry(
            &mut backoff,
            &policy(),
            RetryContext {
                foreground: true,
                failed_rounds: 1,
                server_window_ms: Some((10_000, 20_000)),
                now_ms: now,
            },
            0.0,
            0.5,
        );
        assert_eq!(plan.at_ms, now + 15_000);
        assert_eq!(plan.phase, LinkPhase::Backoff);

        // 连续两轮全部失败：Recovering，退避上限放宽到 5 分钟。
        let mut backoff = Backoff { prev_ms: 200_000 };
        let plan = plan_retry(
            &mut backoff,
            &policy(),
            RetryContext {
                foreground: true,
                failed_rounds: 2,
                server_window_ms: None,
                now_ms: now,
            },
            0.99,
            0.0,
        );
        assert_eq!(plan.phase, LinkPhase::Recovering);
        assert!(plan.at_ms - now > 60_000 && plan.at_ms - now <= 300_000);

        // 后台上限 300s，前台 60s。
        let mut backoff = Backoff { prev_ms: 200_000 };
        let plan = plan_retry(
            &mut backoff,
            &policy(),
            RetryContext {
                foreground: true,
                failed_rounds: 1,
                server_window_ms: None,
                now_ms: now,
            },
            0.99,
            0.0,
        );
        assert_eq!(plan.at_ms - now, 60_000);
    }

    /// 手机在后台最多自己连三轮；前台不限，没有推送可接手的平台（桌面、Web）后台也不限。
    #[test]
    fn a_backgrounded_phone_gets_three_unattended_rounds() {
        for rounds in 0..BACKGROUND_RECONNECT_ROUNDS {
            assert!(
                unattended_round_allowed(false, true, rounds),
                "round {rounds}"
            );
        }
        assert!(!unattended_round_allowed(
            false,
            true,
            BACKGROUND_RECONNECT_ROUNDS
        ));
        assert!(!unattended_round_allowed(false, true, u32::MAX));
        assert!(unattended_round_allowed(true, true, u32::MAX));
        assert!(unattended_round_allowed(false, false, u32::MAX));
    }
}
