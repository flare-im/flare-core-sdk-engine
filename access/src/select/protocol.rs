//! 协议优先序：同一个接入点上先试哪个协议、隔多久再试第二个（交错竞速的输入）。

use crate::directory::AccessPointEntry;
use crate::manifest::ClientPolicy;
use crate::types::{Protocol, ServiceKind};

/// 交给传输层的建连计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPlan {
    /// 依次尝试的协议（最多两个），第一个立即发起，第二个在 `stagger_ms` 后仍未握手成功时发起。
    pub order: Vec<Protocol>,
    pub stagger_ms: u32,
    pub connect_timeout_ms: u32,
    /// 首选协议来自该网络下的历史胜者（否则来自清单 `protocolOrder`）。
    pub from_winner: bool,
    /// 该网络处于 UDP 封锁记忆期，QUIC 已被剔除。
    pub udp_blocked: bool,
}

impl ProtocolPlan {
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn first(&self) -> Option<Protocol> {
        self.order.first().copied()
    }
}

/// 纯函数：
/// 1. 从清单 `protocolOrder` 出发，剔除接入点没有入口的协议、平台不支持的 QUIC、UDP 封锁期内的 QUIC；
/// 2. 若该网络下有历史胜者且仍可用，把它提到最前。
pub fn plan_protocols(
    ap: &AccessPointEntry,
    policy: &ClientPolicy,
    winner: Option<Protocol>,
    udp_blocked: bool,
    quic_supported: bool,
) -> ProtocolPlan {
    let available = |protocol: Protocol| match protocol {
        Protocol::Quic => quic_supported && !udp_blocked && ap.endpoints.has(ServiceKind::ImQuic),
        Protocol::Ws => ap.endpoints.has(ServiceKind::ImWs),
    };
    let mut order: Vec<Protocol> = policy
        .protocol_order
        .iter()
        .copied()
        .filter(|protocol| available(*protocol))
        .collect();
    let mut from_winner = false;
    if let Some(winner) = winner
        && let Some(position) = order.iter().position(|protocol| *protocol == winner)
    {
        order[..=position].rotate_right(1);
        from_winner = true;
    }
    ProtocolPlan {
        order,
        stagger_ms: policy.race_stagger_ms,
        connect_timeout_ms: policy.connect_timeout_ms,
        from_winner,
        udp_blocked,
    }
}
