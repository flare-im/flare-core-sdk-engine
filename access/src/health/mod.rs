//! 健康簿：每个（接入点, 网络指纹）的 RTT、近期失败率、熔断、协议胜者，以及每个网络的 UDP 封锁记忆。
//!
//! IM 与 REST 共用同一份健康簿：一条链路的失败对另一条立即可见。
//! 记录接口只更新内存（一次短暂加锁），落盘由引擎定时调用 [`HealthBook::flush`] 批量完成。

mod book;
mod breaker;
mod netprint;

pub use book::{
    ApHealthReport, BreakerView, FLUSH_INTERVAL_MS, HealthBook, HealthView, LoadReport,
    MIN_RATIO_SAMPLES, ProbeStreak, RTT_STALE_AFTER_MS, RecordEffect, UDP_BLOCK_QUIC_FAILURES,
    WINDOW_SIZE, WINDOW_STALE_AFTER_MS,
};
pub use breaker::{Breaker, BreakerPhase, BreakerTransition, TRIAL_LEASE_MS};
pub use netprint::{NetInterface, NetPrint};

use crate::types::Protocol;

/// 这次结果来自哪种尝试。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttemptKind {
    /// 建连握手（含 TLS 与 CONNECT_ACK）。成功时记录协议胜者，RTT 取握手耗时。
    Handshake(Protocol),
    /// 心跳 PING/PONG。成功只贡献 RTT 样本，不进入失败率窗口：
    /// 心跳频率远高于建连，计入会把真实的建连失败稀释掉。判死（`HeartbeatDead`）照常计失败。
    Heartbeat,
    /// 经该接入点的 HTTP 请求。
    Http,
    /// 主动探活（`/.well-known/flare/ping`）。成功累计「连续探活成功」，供恢复切回判定。
    Probe,
}

/// 计入熔断的失败（§3.6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureClass {
    /// TCP/UDP 不可达、DNS 失败。
    Unreachable,
    /// TLS 握手或证书（含 SPKI 钉扎）校验失败。
    TlsFailed,
    HandshakeTimeout,
    ConnectAckTimeout,
    HeartbeatDead,
    /// 探活响应不合法（非 204、`x-flare-ap`/`x-flare-cell` 不符：强制门户、劫持页、DNS 污染）。
    ProbeInvalid,
    /// HTTP 连接阶段失败（请求未发出）。
    HttpConnectFailed,
}

/// 不计入熔断的结果：它们与「这个入口能不能用」无关，计入会让健康的入口被误熔断。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NeutralReason {
    /// 服务端明确过载（`OVERLOADED`/429）：应等待 `retry_after`，而不是迁移到别的入口制造级联。
    Overloaded,
    /// 鉴权失败：令牌问题，与入口无关。
    AuthRejected,
    /// 服务端排空（`CLOSE{DRAIN}`）。
    Draining,
    /// 设备无网络。
    NoNetwork,
    /// 调用方主动取消（例如交错竞速中输掉的一路）。
    Cancelled,
    /// 入口可达且给出了应用层错误响应（例如业务 4xx/5xx）。
    ServerResponse,
}

/// 调用方的结果分类。哪些算失败由调用方决定，健康簿只按分类更新。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Outcome {
    /// `rtt_ms`：握手耗时、心跳往返、HTTP 耗时或探活耗时。
    Success {
        rtt_ms: Option<u32>,
    },
    Failure(FailureClass),
    Neutral(NeutralReason),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    pub kind: AttemptKind,
    pub outcome: Outcome,
}

impl Observation {
    pub fn new(kind: AttemptKind, outcome: Outcome) -> Self {
        Self { kind, outcome }
    }

    pub fn handshake_ok(protocol: Protocol, rtt_ms: u32) -> Self {
        Self::new(
            AttemptKind::Handshake(protocol),
            Outcome::Success {
                rtt_ms: Some(rtt_ms),
            },
        )
    }

    pub fn handshake_failed(protocol: Protocol, class: FailureClass) -> Self {
        Self::new(AttemptKind::Handshake(protocol), Outcome::Failure(class))
    }

    pub fn heartbeat_rtt(rtt_ms: u32) -> Self {
        Self::new(
            AttemptKind::Heartbeat,
            Outcome::Success {
                rtt_ms: Some(rtt_ms),
            },
        )
    }

    pub fn heartbeat_dead() -> Self {
        Self::new(
            AttemptKind::Heartbeat,
            Outcome::Failure(FailureClass::HeartbeatDead),
        )
    }

    pub fn http(outcome: Outcome) -> Self {
        Self::new(AttemptKind::Http, outcome)
    }

    pub fn probe(outcome: Outcome) -> Self {
        Self::new(AttemptKind::Probe, outcome)
    }
}
