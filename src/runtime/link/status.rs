//! 连接状态：链路状态机的内部阶段，与对外唯一的连接事件 `connection://status`。
//!
//! 内部阶段比对外阶段细（Resolving / Connecting / Authenticating 对用户都是「正在连接」），
//! 对外只给 Kit 需要区分的九种；显示与否、文案、延迟阈值全部由 Kit 按 `phase` 决定，宿主不再判断。

pub use crate::kernel::event::link_status::{
    AccessEndpoint, AccessSwitched, ActionRequiredReason, ConnectionPhase, ConnectionStatus,
    LinkQuality, SwitchReason,
};

/// 链路状态机的内部阶段（§3.11）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LinkPhase {
    /// 没有会话，或会话已主动断开。
    Idle,
    /// 正在向选路器要候选。
    Resolving,
    /// 正在建传输（交错竞速）。
    Connecting,
    /// 传输已建、等待 CONNECT_ACK。交错竞速把这一段合并在一次握手里，这里只在续期后的重连中短暂出现。
    Authenticating,
    /// 已收到 CONNECT_ACK，补同步进行中。发件箱已恢复。
    Syncing,
    Ready,
    /// 连接还在，但有理由怀疑它（网络变化、心跳 RTT 劣化）：已发起验活，等结果。
    Degraded,
    /// 本轮候选全部失败，按退避等待下一轮。
    Backoff,
    /// 连续两轮全部失败：服务暂时不可用，按更长的退避继续从现有目录恢复。
    Recovering,
    /// 设备没有网络：停止一切尝试，发件箱计时冻结。
    Suspended,
    /// 已托管：应用在后台，提示交给了推送通道，链路由引擎自己关掉。没有连接，不重连，也不设任何
    /// 重试定时；只由回到前台或显式的重连请求离开，网络变化不动它。
    Parked,
    /// 只能由用户动作离开（重新登录、升级后重启）。
    Terminated,
}

/// 链路此刻的事实，由状态机维护；[`project`] 把它投影成对外状态。
#[derive(Debug, Clone, Default)]
pub struct LinkFacts {
    /// 本会话里是否已经上线过：首次建连显示「正在连接」，之后的建连显示「正在切换线路」。
    pub was_online: bool,
    pub access_point_id: Option<String>,
    pub cell_id: Option<String>,
    pub region_id: Option<String>,
    pub protocol: Option<String>,
    pub rtt_ms: Option<u32>,
    pub attempt: u32,
    pub next_retry_at: Option<i64>,
    pub switch_reason: Option<SwitchReason>,
    pub action_reason: Option<ActionRequiredReason>,
    pub development_mode: bool,
}

/// 内部阶段 + 事实 → 对外状态（§3.11 表）。纯函数，便于逐行单测。
pub fn project(phase: LinkPhase, facts: &LinkFacts) -> ConnectionStatus {
    let mut status = ConnectionStatus::idle(facts.development_mode);
    let located = |status: &mut ConnectionStatus| {
        status.access_point_id = facts.access_point_id.clone();
        status.cell_id = facts.cell_id.clone();
        status.region_id = facts.region_id.clone();
        status.protocol = facts.protocol.clone();
    };
    match phase {
        LinkPhase::Idle => {}
        LinkPhase::Resolving | LinkPhase::Connecting | LinkPhase::Authenticating => {
            located(&mut status);
            status.attempt = facts.attempt;
            if facts.was_online {
                status.phase = ConnectionPhase::Switching;
                status.reason = facts.switch_reason.map(|reason| reason.as_str().to_owned());
            } else {
                status.phase = ConnectionPhase::Connecting;
            }
        }
        LinkPhase::Syncing => {
            located(&mut status);
            status.phase = ConnectionPhase::Syncing;
            status.rtt_ms = facts.rtt_ms;
        }
        // 托管对外仍是 online，与 Ready 的投影逐字段相同，所以在线时进后台不会多发一个状态事件。
        // 托管是引擎自己做的交接，不是故障。进后台那一刻的界面会被系统留作快照，回来时用户第一眼
        // 看到的就是它：这里若报成 reconnecting 或 waitingForNetwork，快照里就留着一条错误横幅。
        // 回到前台立即建连，状态照常走 switching → syncing → online，快的时候不到 Kit 的显示阈值，
        // 界面上什么都不出现；连不上再按真实结果显示横幅。
        LinkPhase::Ready | LinkPhase::Degraded | LinkPhase::Parked => {
            located(&mut status);
            status.phase = ConnectionPhase::Online;
            status.rtt_ms = facts.rtt_ms;
            status.quality = Some(if phase == LinkPhase::Degraded {
                LinkQuality::Poor
            } else {
                LinkQuality::Good
            });
        }
        LinkPhase::Backoff => {
            // 报下一次要尝试的接入点（同点等待时就是当前点）。
            located(&mut status);
            status.phase = ConnectionPhase::Reconnecting;
            status.attempt = facts.attempt;
            status.next_retry_at = facts.next_retry_at;
        }
        LinkPhase::Recovering => {
            status.phase = ConnectionPhase::ServiceUnavailable;
            status.attempt = facts.attempt;
            status.next_retry_at = facts.next_retry_at;
        }
        LinkPhase::Suspended => {
            status.phase = ConnectionPhase::WaitingForNetwork;
        }
        LinkPhase::Terminated => {
            status.phase = ConnectionPhase::ActionRequired;
            status.reason = Some(
                facts
                    .action_reason
                    .unwrap_or(ActionRequiredReason::Expired)
                    .as_str()
                    .to_owned(),
            );
        }
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts() -> LinkFacts {
        LinkFacts {
            access_point_id: Some("ap-a".into()),
            cell_id: Some("cell-1".into()),
            region_id: Some("cn-east".into()),
            protocol: Some("quic".into()),
            rtt_ms: Some(42),
            attempt: 2,
            next_retry_at: Some(1_700_000_000_000),
            ..Default::default()
        }
    }

    /// §3.11 表逐行：内部阶段 → phase 与附加字段。
    #[test]
    fn internal_phases_project_onto_the_kit_phases() {
        let first = facts();
        let mut switching = facts();
        switching.was_online = true;
        switching.switch_reason = Some(SwitchReason::Drain);

        assert_eq!(
            project(LinkPhase::Idle, &first).phase,
            ConnectionPhase::Idle
        );
        for phase in [
            LinkPhase::Resolving,
            LinkPhase::Connecting,
            LinkPhase::Authenticating,
        ] {
            let status = project(phase, &first);
            assert_eq!(status.phase, ConnectionPhase::Connecting);
            assert_eq!(status.access_point_id.as_deref(), Some("ap-a"));
            assert_eq!(status.protocol.as_deref(), Some("quic"));

            let status = project(phase, &switching);
            assert_eq!(status.phase, ConnectionPhase::Switching);
            assert_eq!(status.reason.as_deref(), Some("drain"));
        }
        let syncing = project(LinkPhase::Syncing, &first);
        assert_eq!(syncing.phase, ConnectionPhase::Syncing);
        assert_eq!(syncing.next_retry_at, None);

        let online = project(LinkPhase::Ready, &first);
        assert_eq!(online.phase, ConnectionPhase::Online);
        assert_eq!(online.quality, Some(LinkQuality::Good));
        assert_eq!(online.rtt_ms, Some(42));
        assert_eq!(
            project(LinkPhase::Degraded, &first).quality,
            Some(LinkQuality::Poor)
        );

        let backoff = project(LinkPhase::Backoff, &first);
        assert_eq!(backoff.phase, ConnectionPhase::Reconnecting);
        assert_eq!(backoff.next_retry_at, Some(1_700_000_000_000));
        assert_eq!(backoff.attempt, 2);
        assert_eq!(
            backoff.access_point_id.as_deref(),
            Some("ap-a"),
            "reconnecting names the access point it will try next"
        );
        assert_eq!(backoff.cell_id.as_deref(), Some("cell-1"));
        assert_eq!(backoff.region_id.as_deref(), Some("cn-east"));

        assert_eq!(
            project(LinkPhase::Suspended, &first).phase,
            ConnectionPhase::WaitingForNetwork
        );
        assert_eq!(
            project(LinkPhase::Recovering, &first).phase,
            ConnectionPhase::ServiceUnavailable
        );

        let mut kicked = facts();
        kicked.action_reason = Some(ActionRequiredReason::Kicked);
        let terminated = project(LinkPhase::Terminated, &kicked);
        assert_eq!(terminated.phase, ConnectionPhase::ActionRequired);
        assert_eq!(terminated.reason.as_deref(), Some("kicked"));
        // 没有记下原因时按「登录已过期」显示（与 Kit 的缺省一致）。
        assert_eq!(
            project(LinkPhase::Terminated, &first).reason.as_deref(),
            Some("expired")
        );
    }

    /// 托管对外与在线逐字段相同：在线时进后台不产生新的状态事件，界面快照里也不留横幅。
    #[test]
    fn parked_projects_exactly_like_ready() {
        let facts = facts();
        let parked = project(LinkPhase::Parked, &facts);
        assert_eq!(parked.phase, ConnectionPhase::Online);
        assert_eq!(parked.quality, Some(LinkQuality::Good));
        assert_eq!(parked.next_retry_at, None, "托管时没有重试定时");
        assert_eq!(parked.attempt, 0);
        assert_eq!(parked, project(LinkPhase::Ready, &facts));
    }
}
