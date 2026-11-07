//! 链路相关事件的载荷：`connection://status`、`access://switched`、`access://changed`。
//!
//! 只有数据定义；状态机（`runtime::link`）负责何时、以什么值发出。字段名与取值是跨端契约
//! （Kit `ConnectionNotice`、平台包事件类型），改名要同步契约与 Kit 向量。

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::kernel::SdkState;

/// 对外阶段（`connection://status` 的 `phase`）。取值与 Kit `ConnectionNotice` 契约一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionPhase {
    Idle,
    Connecting,
    Syncing,
    Online,
    Switching,
    Reconnecting,
    WaitingForNetwork,
    ServiceUnavailable,
    ActionRequired,
}

impl ConnectionPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectionPhase::Idle => "idle",
            ConnectionPhase::Connecting => "connecting",
            ConnectionPhase::Syncing => "syncing",
            ConnectionPhase::Online => "online",
            ConnectionPhase::Switching => "switching",
            ConnectionPhase::Reconnecting => "reconnecting",
            ConnectionPhase::WaitingForNetwork => "waitingForNetwork",
            ConnectionPhase::ServiceUnavailable => "serviceUnavailable",
            ConnectionPhase::ActionRequired => "actionRequired",
        }
    }
}

/// 在线时的链路质量。两种取值都不显示横幅（「网络较差」只在诊断页出现）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum LinkQuality {
    Good,
    Poor,
}

/// `actionRequired` 的原因：链路停下，只能由用户动作恢复。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ActionRequiredReason {
    /// 被结束会话的踢线（同平台新设备登录）。
    Kicked,
    /// 令牌被拒且续期一次后仍被拒，或拿不到新令牌。
    Expired,
    /// 服务端要求升级（`UPGRADE_REQUIRED` / 协议版本不被支持）。
    UpgradeRequired,
    /// 所属租户已停用或正在删除。
    TenantUnavailable,
}

impl ActionRequiredReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ActionRequiredReason::Kicked => "kicked",
            ActionRequiredReason::Expired => "expired",
            ActionRequiredReason::UpgradeRequired => "upgradeRequired",
            ActionRequiredReason::TenantUnavailable => "tenantUnavailable",
        }
    }
}

/// 一次切换线路的原因（`access://switched` 的 `reason`，切换中的 `connection://status.reason`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SwitchReason {
    /// 当前接入点失败（判死、建连失败）。
    Failure,
    /// 服务端计划性关闭（排空、停机、再均衡）。
    Drain,
    /// 恢复切回首选接入点。
    Failback,
    /// 用户手动固定了别的接入点。
    Manual,
    /// 新清单把当前接入点标为不可用。
    Manifest,
}

impl SwitchReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SwitchReason::Failure => "failure",
            SwitchReason::Drain => "drain",
            SwitchReason::Failback => "failback",
            SwitchReason::Manual => "manual",
            SwitchReason::Manifest => "manifest",
        }
    }
}

/// 切换线路一端的接入点与协议。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessEndpoint {
    pub ap: String,
    /// `quic` / `ws`。
    pub protocol: String,
}

/// `access://switched` 的载荷：IM 换到了另一个接入点（切换完成、补同步就绪时发出一次）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessSwitched {
    pub from: AccessEndpoint,
    pub to: AccessEndpoint,
    pub reason: SwitchReason,
}

/// `connection://status` 的载荷，也是 `connection.get_status` 的返回值。
///
/// 每个字段都会出现（没有值时为 `null`）：宿主每收到一个事件就整体替换状态，不做字段合并。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionStatus {
    pub phase: ConnectionPhase,
    /// 只在 `online` 有值。
    pub quality: Option<LinkQuality>,
    /// 当前（或正在连接的）接入点。
    pub access_point_id: Option<String>,
    pub cell_id: Option<String>,
    pub region_id: Option<String>,
    /// `quic` / `ws`。
    pub protocol: Option<String>,
    /// 最近的心跳往返（没有样本时为握手耗时）。
    pub rtt_ms: Option<u32>,
    /// 本次恢复中的第几轮尝试（上线后归零）。
    pub attempt: u32,
    /// 下一轮尝试的时刻：设备时钟的 Unix 毫秒（不是服务端时钟、不是相对延迟）。
    pub next_retry_at: Option<i64>,
    /// `actionRequired`：`kicked` / `expired` / `upgradeRequired` / `tenantUnavailable`；
    /// `switching`：切换原因（`failure` / `drain` / `failback` / `manual` / `manifest`）。
    pub reason: Option<String>,
    /// 端点来自开发模式配置（未签名清单）：Kit 在任何阶段都显示「开发模式」角标。
    pub development_mode: bool,
}

impl ConnectionStatus {
    pub fn idle(development_mode: bool) -> Self {
        Self {
            phase: ConnectionPhase::Idle,
            quality: None,
            access_point_id: None,
            cell_id: None,
            region_id: None,
            protocol: None,
            rtt_ms: None,
            attempt: 0,
            next_retry_at: None,
            reason: None,
            development_mode,
        }
    }

    /// 粗粒度的会话连接态（仅供内部门禁与旧的健康快照使用）。
    pub fn sdk_state(&self) -> SdkState {
        match self.phase {
            ConnectionPhase::Idle | ConnectionPhase::ActionRequired => SdkState::Disconnected,
            ConnectionPhase::Connecting => SdkState::Connecting,
            ConnectionPhase::Syncing => SdkState::Connected,
            ConnectionPhase::Online => SdkState::Ready,
            ConnectionPhase::Switching
            | ConnectionPhase::Reconnecting
            | ConnectionPhase::WaitingForNetwork
            | ConnectionPhase::ServiceUnavailable => SdkState::Reconnecting,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessChanges {
    pub access_points_added: Vec<String>,
    pub access_points_removed: Vec<String>,
    pub policy_changed: bool,
}

/// `access://changed` 的载荷。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessChanged {
    pub serial: u64,
    pub source: String,
    pub stale: bool,
    pub development_mode: bool,
    pub changes: AccessChanges,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 载荷字段名与 Kit 契约一致，且没有值的字段以 null 出现（宿主整体替换，不做合并）。
    #[test]
    fn status_serializes_every_field_in_camel_case() {
        let value = serde_json::to_value(ConnectionStatus {
            phase: ConnectionPhase::Reconnecting,
            attempt: 2,
            next_retry_at: Some(1_700_000_000_000),
            ..ConnectionStatus::idle(true)
        })
        .unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "phase": "reconnecting",
                "quality": null,
                "accessPointId": null,
                "cellId": null,
                "regionId": null,
                "protocol": null,
                "rttMs": null,
                "attempt": 2,
                "nextRetryAt": 1_700_000_000_000_i64,
                "reason": null,
                "developmentMode": true,
            })
        );
        assert_eq!(
            serde_json::to_value(ConnectionPhase::WaitingForNetwork).unwrap(),
            "waitingForNetwork"
        );
    }

    #[test]
    fn sdk_state_follows_the_phase() {
        let status = |phase| ConnectionStatus {
            phase,
            ..ConnectionStatus::idle(false)
        };
        assert_eq!(status(ConnectionPhase::Online).sdk_state(), SdkState::Ready);
        assert_eq!(
            status(ConnectionPhase::Syncing).sdk_state(),
            SdkState::Connected
        );
        assert_eq!(
            status(ConnectionPhase::Switching).sdk_state(),
            SdkState::Reconnecting
        );
        assert_eq!(
            status(ConnectionPhase::ActionRequired).sdk_state(),
            SdkState::Disconnected
        );
    }
}
