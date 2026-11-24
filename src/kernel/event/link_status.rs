//! `connection://status` 事件载荷。
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

/// `connection://status.reason` 中的计划重连原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SwitchReason {
    /// 服务端计划性关闭（排空、停机、再均衡）。
    Drain,
    /// 宿主刷新地址或主动请求重连。
    Manual,
}

impl SwitchReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SwitchReason::Drain => "drain",
            SwitchReason::Manual => "manual",
        }
    }
}

/// `connection://status` 的载荷，也是 `connection.get_status` 的返回值。
///
/// 除直连配置的版本号外，没有值的字段为 `null`。宿主每收到一个事件就整体替换状态。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionStatus {
    pub phase: ConnectionPhase,
    /// 只在 `online` 有值。
    pub quality: Option<LinkQuality>,
    /// `quic` / `ws`。
    pub protocol: Option<String>,
    /// Generation of the host-selected address group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_revision: Option<u32>,
    /// 最近的心跳往返（没有样本时为握手耗时）。
    pub rtt_ms: Option<u32>,
    /// 本次恢复中的第几轮尝试（上线后归零）。
    pub attempt: u32,
    /// 下一轮尝试的时刻：设备时钟的 Unix 毫秒（不是服务端时钟、不是相对延迟）。
    pub next_retry_at: Option<i64>,
    /// `actionRequired`：`kicked` / `expired` / `upgradeRequired` / `tenantUnavailable`；
    /// `switching`：计划重连的原因（`drain` / `manual`）。
    /// `reconnecting` / `serviceUnavailable`：`transportUnavailable` 或 `overloaded`。
    pub reason: Option<String>,
    /// 端点来自开发模式配置：Kit 在任何阶段都显示「开发模式」角标。
    pub development_mode: bool,
}

impl ConnectionStatus {
    pub fn idle(development_mode: bool) -> Self {
        Self {
            phase: ConnectionPhase::Idle,
            quality: None,
            protocol: None,
            endpoint_revision: None,
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
