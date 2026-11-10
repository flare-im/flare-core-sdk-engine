//! 故障转移决策（§3.6）：纯函数，不做 I/O、不读时钟，随机数由调用方传入。
//!
//! 状态机（`LinkSupervisor`）只负责执行；「失败算不算入口的错」「下一步是立即换候选、原地等、
//! 退避还是终止」全部在这里决定，所以每一行决策表都能用确定的输入单测。

use std::time::Duration;

use flare_access::manifest::BackoffPolicy;
use flare_access::{FailureClass, NeutralReason, Observation, Outcome, Protocol};
use flare_core::client::link::{
    AttemptFailure, CloseKind, ConnectReport, DisconnectReason, ServerRejection,
};
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::error::ErrorCode;

use super::status::{ActionRequiredReason, LinkPhase};

/// 连续这么多轮候选全部失败，进入 Recovering（对外 serviceUnavailable）。
pub const RECOVERING_AFTER_FAILED_ROUNDS: u32 = 2;
/// Recovering 期间的退避上限（§3.11：引导失败退避，上限 5 分钟）。
pub const RECOVERING_CAP_MS: u64 = 300_000;

pub fn to_transport(protocol: Protocol) -> TransportProtocol {
    match protocol {
        Protocol::Quic => TransportProtocol::QUIC,
        Protocol::Ws => TransportProtocol::WebSocket,
    }
}

pub fn from_transport(protocol: TransportProtocol) -> Option<Protocol> {
    match protocol {
        TransportProtocol::QUIC => Some(Protocol::Quic),
        TransportProtocol::WebSocket => Some(Protocol::Ws),
        _ => None,
    }
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
    NextCandidate { counted: bool },
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
        _ => RejectionKind::NextCandidate {
            counted: rejection.counts_against_access_point(),
        },
    }
}

/// 一次握手失败 → 健康簿观察。`None`：不记（本端不支持该协议，与入口健康无关）。
pub fn attempt_observation(protocol: Protocol, failure: &AttemptFailure) -> Option<Observation> {
    let outcome = match failure {
        AttemptFailure::Unreachable { .. } | AttemptFailure::Closed { .. } => {
            Outcome::Failure(FailureClass::Unreachable)
        }
        AttemptFailure::Tls { .. } => Outcome::Failure(FailureClass::TlsFailed),
        AttemptFailure::Timeout => Outcome::Failure(FailureClass::HandshakeTimeout),
        AttemptFailure::ConnectAckTimeout => Outcome::Failure(FailureClass::ConnectAckTimeout),
        AttemptFailure::Rejected(rejection) => match classify_rejection(rejection) {
            RejectionKind::Overloaded { .. } => Outcome::Neutral(NeutralReason::Overloaded),
            RejectionKind::Auth => Outcome::Neutral(NeutralReason::AuthRejected),
            RejectionKind::Terminal(_) => Outcome::Neutral(NeutralReason::ServerResponse),
            RejectionKind::NextCandidate { counted: true } => {
                Outcome::Failure(FailureClass::Unreachable)
            }
            RejectionKind::NextCandidate { counted: false } => {
                if CloseKind::parse(&rejection.reason).is_some() {
                    Outcome::Neutral(NeutralReason::Draining)
                } else {
                    Outcome::Neutral(NeutralReason::ServerResponse)
                }
            }
        },
        AttemptFailure::Unsupported { .. } => return None,
    };
    Some(Observation::new(
        flare_access::AttemptKind::Handshake(protocol),
        outcome,
    ))
}

/// 竞速成功后要回写健康簿的一条记录。顺序有意义：失败与 UDP 证据在前、胜者在后，
/// 健康簿才能把「QUIC 失败期间 WS 成功」识别为 UDP 封锁。
#[derive(Debug, Clone, PartialEq)]
pub enum ReportRecord {
    Observe(Observation),
    /// QUIC 没建起传输就被先赢的 WebSocket 取消：UDP 证据，不计熔断。
    QuicSilence,
}

pub fn report_records(report: &ConnectReport, stagger: Duration) -> Vec<ReportRecord> {
    let mut records = Vec::with_capacity(report.failed.len() + report.cancelled.len() + 1);
    for attempt in &report.failed {
        if let Some(protocol) = from_transport(attempt.protocol)
            && let Some(observation) = attempt_observation(protocol, &attempt.failure)
        {
            records.push(ReportRecord::Observe(observation));
        }
    }
    for cancelled in &report.cancelled {
        let Some(protocol) = from_transport(cancelled.protocol) else {
            continue;
        };
        if protocol == Protocol::Quic
            && report.protocol == TransportProtocol::WebSocket
            && !cancelled.transport_ready
            && cancelled.ran_for >= stagger
        {
            records.push(ReportRecord::QuicSilence);
        } else {
            records.push(ReportRecord::Observe(Observation::new(
                flare_access::AttemptKind::Handshake(protocol),
                Outcome::Neutral(NeutralReason::Cancelled),
            )));
        }
    }
    if let Some(protocol) = from_transport(report.protocol) {
        records.push(ReportRecord::Observe(Observation::handshake_ok(
            protocol,
            duration_ms(report.handshake),
        )));
    }
    records
}

pub fn duration_ms(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

/// 一条已上线的连接为什么结束。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkLoss {
    /// 我方断开（主动断开、切换时关掉旧连接）：状态机自己发起的，不触发恢复。
    ClientRequested,
    /// 计入熔断的失败：心跳判死、对端关闭、读写出错。立即换候选。
    Failed(FailureClass),
    /// 服务端计划性关闭（排空、停机、再均衡）：窗口内随机时刻重连，优先 `prefer`；不计熔断。
    Planned {
        window_ms: Option<(u64, u64)>,
        prefer: Vec<String>,
    },
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
            prefer: close.prefer_ap.clone(),
        },
        // 在线连接被终止类拒绝（升级门槛抬高）：与建连时被拒同样终止；其余拒绝类别不会以此上报。
        DisconnectReason::Rejected(rejection) => match classify_rejection(rejection) {
            RejectionKind::Terminal(reason) => LinkLoss::SessionEnded(reason),
            _ => LinkLoss::Failed(FailureClass::Unreachable),
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
        DisconnectReason::HeartbeatTimeout => LinkLoss::Failed(FailureClass::HeartbeatDead),
        DisconnectReason::PeerClosed { .. } | DisconnectReason::TransportError { .. } => {
            LinkLoss::Failed(FailureClass::Unreachable)
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

/// 退避的上下文。
#[derive(Debug, Clone, Copy)]
pub struct RetryContext {
    pub foreground: bool,
    /// 连续全部失败的轮数（含这一轮）。
    pub failed_rounds: u32,
    /// 服务端给出的重连窗口（计划性关闭、过载）。
    pub server_window_ms: Option<(u64, u64)>,
    /// 候选为空且因熔断时，最早进入 half-open 的时刻。
    pub next_half_open_at_ms: Option<i64>,
    pub now_ms: i64,
}

/// 下一轮的安排。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPlan {
    pub at_ms: i64,
    /// Backoff 或 Recovering。
    pub phase: LinkPhase,
}

/// 一轮候选全部失败后何时再试：去相关抖动，与服务端窗口取大，再不早于最早的 half-open。
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
    let mut at = context.now_ms.saturating_add(sleep as i64);
    if let Some(half_open) = context.next_half_open_at_ms
        && half_open > at
    {
        at = half_open;
    }
    RetryPlan {
        at_ms: at,
        phase: if recovering {
            LinkPhase::Recovering
        } else {
            LinkPhase::Backoff
        },
    }
}

/// 一次事件之后的动作（§3.6 决策表）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverAction {
    /// 立即重新选路建连（`prefer` 非空时优先其中的接入点）。
    ConnectNow { prefer: Vec<String> },
    /// 窗口内某个时刻重连：`delay_ms` 已按窗口随机取好。
    ConnectAfter { delay_ms: u64, prefer: Vec<String> },
    /// 同一接入点等待后重试，不换入口。
    RetrySameAfter { access_point: String, delay_ms: u64 },
    /// 按退避等下一轮（具体时刻由 [`plan_retry`] 算）。
    Backoff,
    /// 对当前连接立即验活。
    ProbeWake,
    /// 停止一切尝试，等网络恢复。
    Suspend,
    /// 终止。
    Terminate(ActionRequiredReason),
    /// 什么也不做。
    Stay,
}

/// 决策表的输入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverEvent {
    /// 当前连接结束。
    LinkLost(LinkLoss),
    /// 本轮候选全部失败。
    RoundExhausted,
    /// 服务端过载拒绝。
    Overloaded {
        access_point: String,
        retry_after_ms: Option<u64>,
    },
    /// 网络恢复/切换。`link_alive`：还有在用的连接；`current_unhealthy`：新网络下当前接入点的健康记录不佳。
    NetworkAvailable {
        link_alive: bool,
        current_unhealthy: bool,
    },
    NetworkUnavailable,
}

/// 过载时没给 `retry_after` 的兜底等待。
pub const DEFAULT_OVERLOAD_WAIT_MS: u64 = 2_000;

/// 决策表（纯函数）。`window_unit` 用于在服务端窗口里取随机时刻。
pub fn decide(event: &FailoverEvent, window_unit: f64) -> FailoverAction {
    match event {
        FailoverEvent::LinkLost(loss) => match loss {
            LinkLoss::ClientRequested => FailoverAction::Stay,
            // 当前接入点失败：立即在候选中换下一个，不等退避（健康簿会把它排到后面或熔断）。
            LinkLoss::Failed(_) => FailoverAction::ConnectNow { prefer: Vec::new() },
            LinkLoss::Planned { window_ms, prefer } => match window_ms {
                Some((min, max)) => FailoverAction::ConnectAfter {
                    delay_ms: between(*min, *max, window_unit),
                    prefer: prefer.clone(),
                },
                None => FailoverAction::ConnectNow {
                    prefer: prefer.clone(),
                },
            },
            LinkLoss::SessionEnded(reason) => FailoverAction::Terminate(*reason),
            LinkLoss::Kicked => FailoverAction::Backoff,
        },
        FailoverEvent::RoundExhausted => FailoverAction::Backoff,
        FailoverEvent::Overloaded {
            access_point,
            retry_after_ms,
        } => FailoverAction::RetrySameAfter {
            access_point: access_point.clone(),
            delay_ms: retry_after_ms.unwrap_or(DEFAULT_OVERLOAD_WAIT_MS),
        },
        FailoverEvent::NetworkAvailable {
            link_alive,
            current_unhealthy,
        } => {
            if *link_alive && !*current_unhealthy {
                FailoverAction::ProbeWake
            } else {
                FailoverAction::ConnectNow { prefer: Vec::new() }
            }
        }
        FailoverEvent::NetworkUnavailable => FailoverAction::Suspend,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::client::link::{AttemptReport, CancelledAttempt, ConnectAckInfo, ServerClose};

    fn rejection(code: ErrorCode, reason: &str) -> ServerRejection {
        ServerRejection {
            code,
            reason: reason.into(),
            retry_after_ms: None,
            min_build: None,
            release_hint: None,
        }
    }

    #[test]
    fn rejections_are_classified_for_the_decision_table() {
        let mut overloaded = rejection(ErrorCode::Overloaded, "OVERLOADED");
        overloaded.retry_after_ms = Some(2_500);
        assert_eq!(
            classify_rejection(&overloaded),
            RejectionKind::Overloaded {
                retry_after_ms: Some(2_500)
            }
        );
        assert_eq!(
            classify_rejection(&rejection(ErrorCode::UpgradeRequired, "UPGRADE_REQUIRED")),
            RejectionKind::Terminal(ActionRequiredReason::UpgradeRequired)
        );
        assert_eq!(
            classify_rejection(&rejection(
                ErrorCode::ProtocolVersionMismatch,
                "PROTOCOL_VERSION_MISMATCH"
            )),
            RejectionKind::Terminal(ActionRequiredReason::UpgradeRequired)
        );
        assert_eq!(
            classify_rejection(&rejection(
                ErrorCode::PermissionDenied,
                "TENANT_UNAVAILABLE"
            )),
            RejectionKind::Terminal(ActionRequiredReason::TenantUnavailable)
        );
        assert_eq!(
            classify_rejection(&rejection(ErrorCode::TokenExpired, "TOKEN_EXPIRED")),
            RejectionKind::Auth
        );
        assert_eq!(
            classify_rejection(&rejection(ErrorCode::ServiceUnavailable, "DRAIN")),
            RejectionKind::NextCandidate { counted: false }
        );
        assert_eq!(
            classify_rejection(&rejection(
                ErrorCode::ServiceUnavailable,
                "SERVICE_UNAVAILABLE"
            )),
            RejectionKind::NextCandidate { counted: true }
        );
    }

    /// §3.6：计为失败的类型进熔断；过载、鉴权、排空、取消、本端不支持不计。
    #[test]
    fn handshake_failures_map_onto_health_observations() {
        let failed = |failure: AttemptFailure| {
            attempt_observation(Protocol::Quic, &failure).map(|observation| observation.outcome)
        };
        assert_eq!(
            failed(AttemptFailure::Unreachable {
                detail: "refused".into()
            }),
            Some(Outcome::Failure(FailureClass::Unreachable))
        );
        assert_eq!(
            failed(AttemptFailure::Tls {
                detail: "pin".into()
            }),
            Some(Outcome::Failure(FailureClass::TlsFailed))
        );
        assert_eq!(
            failed(AttemptFailure::Timeout),
            Some(Outcome::Failure(FailureClass::HandshakeTimeout))
        );
        assert_eq!(
            failed(AttemptFailure::ConnectAckTimeout),
            Some(Outcome::Failure(FailureClass::ConnectAckTimeout))
        );
        assert_eq!(
            failed(AttemptFailure::Rejected(rejection(
                ErrorCode::Overloaded,
                "OVERLOADED"
            ))),
            Some(Outcome::Neutral(NeutralReason::Overloaded))
        );
        assert_eq!(
            failed(AttemptFailure::Rejected(rejection(
                ErrorCode::AuthenticationFailed,
                "AUTHENTICATION_FAILED"
            ))),
            Some(Outcome::Neutral(NeutralReason::AuthRejected))
        );
        assert_eq!(
            failed(AttemptFailure::Rejected(rejection(
                ErrorCode::ServiceUnavailable,
                "DRAIN"
            ))),
            Some(Outcome::Neutral(NeutralReason::Draining))
        );
        assert_eq!(
            failed(AttemptFailure::Unsupported {
                detail: "browser".into()
            }),
            None
        );
    }

    fn report(
        winner: TransportProtocol,
        failed: Vec<AttemptReport>,
        cancelled: Vec<CancelledAttempt>,
    ) -> ConnectReport {
        ConnectReport {
            protocol: winner,
            handshake: Duration::from_millis(120),
            elapsed: Duration::from_millis(420),
            ack: ConnectAckInfo::default(),
            failed,
            cancelled,
        }
    }

    /// QUIC 被静默丢弃（没建起传输、跑满交错间隔后被 WS 取消）记为 UDP 证据；建好了在排队的
    /// QUIC 只是输了，记中性；失败在前、胜者在后。
    #[test]
    fn race_reports_record_failures_first_and_udp_evidence_separately() {
        let stagger = Duration::from_millis(300);
        let silent = report(
            TransportProtocol::WebSocket,
            vec![],
            vec![CancelledAttempt {
                protocol: TransportProtocol::QUIC,
                ran_for: Duration::from_millis(420),
                transport_ready: false,
            }],
        );
        let records = report_records(&silent, stagger);
        assert_eq!(records[0], ReportRecord::QuicSilence);
        assert_eq!(
            records[1],
            ReportRecord::Observe(Observation::handshake_ok(Protocol::Ws, 120))
        );

        let queued = report(
            TransportProtocol::WebSocket,
            vec![],
            vec![CancelledAttempt {
                protocol: TransportProtocol::QUIC,
                ran_for: Duration::from_millis(420),
                transport_ready: true,
            }],
        );
        assert_eq!(
            report_records(&queued, stagger)[0],
            ReportRecord::Observe(Observation::new(
                flare_access::AttemptKind::Handshake(Protocol::Quic),
                Outcome::Neutral(NeutralReason::Cancelled)
            ))
        );

        let timed_out = report(
            TransportProtocol::WebSocket,
            vec![AttemptReport {
                protocol: TransportProtocol::QUIC,
                elapsed: Duration::from_secs(8),
                failure: AttemptFailure::Timeout,
            }],
            vec![],
        );
        let records = report_records(&timed_out, stagger);
        assert_eq!(
            records[0],
            ReportRecord::Observe(Observation::handshake_failed(
                Protocol::Quic,
                FailureClass::HandshakeTimeout
            ))
        );
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn disconnect_reasons_are_classified() {
        assert_eq!(
            classify_loss(&DisconnectReason::HeartbeatTimeout),
            LinkLoss::Failed(FailureClass::HeartbeatDead)
        );
        assert_eq!(
            classify_loss(&DisconnectReason::PeerClosed {
                detail: "eof".into()
            }),
            LinkLoss::Failed(FailureClass::Unreachable)
        );
        assert_eq!(
            classify_loss(&DisconnectReason::Rejected(rejection(
                ErrorCode::UpgradeRequired,
                "UPGRADE_REQUIRED"
            ))),
            LinkLoss::SessionEnded(ActionRequiredReason::UpgradeRequired)
        );
        assert_eq!(
            classify_loss(&DisconnectReason::ClientRequested),
            LinkLoss::ClientRequested
        );
        let drain = ServerClose::new(CloseKind::Drain)
            .with_retry_window(Duration::from_secs(1), Duration::from_secs(5))
            .with_prefer_ap(["ap-b"]);
        assert_eq!(
            classify_loss(&DisconnectReason::ServerClose(drain)),
            LinkLoss::Planned {
                window_ms: Some((1_000, 5_000)),
                prefer: vec!["ap-b".to_owned()]
            }
        );
        assert_eq!(
            classify_loss(&DisconnectReason::Kicked {
                reason: "DEVICE_CONFLICT".into(),
                ends_session: true
            }),
            LinkLoss::SessionEnded(ActionRequiredReason::Kicked)
        );
        assert_eq!(
            classify_loss(&DisconnectReason::Kicked {
                reason: "TENANT_UNAVAILABLE".into(),
                ends_session: true
            }),
            LinkLoss::SessionEnded(ActionRequiredReason::TenantUnavailable)
        );
        assert_eq!(
            classify_loss(&DisconnectReason::Kicked {
                reason: "REPLACED".into(),
                ends_session: false
            }),
            LinkLoss::Kicked
        );
    }

    /// 决策表（§3.6）逐行。
    #[test]
    fn decision_table() {
        assert_eq!(
            decide(
                &FailoverEvent::LinkLost(LinkLoss::Failed(FailureClass::HeartbeatDead)),
                0.5
            ),
            FailoverAction::ConnectNow { prefer: vec![] }
        );
        assert_eq!(
            decide(
                &FailoverEvent::LinkLost(LinkLoss::Planned {
                    window_ms: Some((1_000, 5_000)),
                    prefer: vec!["ap-b".into()]
                }),
                0.5
            ),
            FailoverAction::ConnectAfter {
                delay_ms: 3_000,
                prefer: vec!["ap-b".into()]
            }
        );
        assert_eq!(
            decide(
                &FailoverEvent::Overloaded {
                    access_point: "ap-a".into(),
                    retry_after_ms: Some(2_000)
                },
                0.5
            ),
            FailoverAction::RetrySameAfter {
                access_point: "ap-a".into(),
                delay_ms: 2_000
            }
        );
        assert_eq!(
            decide(&FailoverEvent::RoundExhausted, 0.5),
            FailoverAction::Backoff
        );
        assert_eq!(
            decide(
                &FailoverEvent::NetworkAvailable {
                    link_alive: true,
                    current_unhealthy: false
                },
                0.5
            ),
            FailoverAction::ProbeWake
        );
        assert_eq!(
            decide(
                &FailoverEvent::NetworkAvailable {
                    link_alive: true,
                    current_unhealthy: true
                },
                0.5
            ),
            FailoverAction::ConnectNow { prefer: vec![] }
        );
        assert_eq!(
            decide(
                &FailoverEvent::NetworkAvailable {
                    link_alive: false,
                    current_unhealthy: false
                },
                0.5
            ),
            FailoverAction::ConnectNow { prefer: vec![] }
        );
        assert_eq!(
            decide(&FailoverEvent::NetworkUnavailable, 0.5),
            FailoverAction::Suspend
        );
        assert_eq!(
            decide(
                &FailoverEvent::LinkLost(LinkLoss::SessionEnded(ActionRequiredReason::Kicked)),
                0.5
            ),
            FailoverAction::Terminate(ActionRequiredReason::Kicked)
        );
        assert_eq!(
            decide(&FailoverEvent::LinkLost(LinkLoss::ClientRequested), 0.5),
            FailoverAction::Stay
        );
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
    fn retries_respect_server_windows_half_open_and_recovering() {
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
                next_half_open_at_ms: None,
                now_ms: now,
            },
            0.0,
            0.5,
        );
        assert_eq!(plan.at_ms, now + 15_000);
        assert_eq!(plan.phase, LinkPhase::Backoff);

        // 熔断要更久才进入 half-open：等到那一刻，下一轮才有候选。
        let mut backoff = Backoff::default();
        let plan = plan_retry(
            &mut backoff,
            &policy(),
            RetryContext {
                foreground: true,
                failed_rounds: 1,
                server_window_ms: None,
                next_half_open_at_ms: Some(now + 30_000),
                now_ms: now,
            },
            0.0,
            0.0,
        );
        assert_eq!(plan.at_ms, now + 30_000);

        // 连续两轮全部失败：Recovering，退避上限放宽到 5 分钟。
        let mut backoff = Backoff { prev_ms: 200_000 };
        let plan = plan_retry(
            &mut backoff,
            &policy(),
            RetryContext {
                foreground: true,
                failed_rounds: 2,
                server_window_ms: None,
                next_half_open_at_ms: None,
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
                next_half_open_at_ms: None,
                now_ms: now,
            },
            0.99,
            0.0,
        );
        assert_eq!(plan.at_ms - now, 60_000);
    }
}
