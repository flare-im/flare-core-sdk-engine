//! 跨模块共用的小类型。

use std::fmt;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// 接入点 ID。用 `Arc<str>` 是因为它会同时出现在目录快照、健康簿键与选路结果里，
/// 克隆只增加引用计数，热路径上不复制字符串。
pub type ApId = Arc<str>;

/// IM 长连接协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Quic,
    Ws,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Quic => "quic",
            Protocol::Ws => "ws",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "quic" => Some(Protocol::Quic),
            "ws" => Some(Protocol::Ws),
            _ => None,
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 接入点状态。未知值按「不可用」处理：新版控制面可以加状态，旧客户端不会误把它当成 active。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ApStatus {
    /// 正常接新连接。
    Active,
    /// 不接新连接，已有连接按排空窗口迁走。
    Draining,
    /// 从候选中移除。
    Disabled,
    /// 本版本不认识的状态。
    #[serde(other)]
    Unknown,
}

impl ApStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ApStatus::Active => "active",
            ApStatus::Draining => "draining",
            ApStatus::Disabled => "disabled",
            ApStatus::Unknown => "unknown",
        }
    }
}

/// 接入点上的单个服务入口。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ServiceKind {
    ImWs,
    ImQuic,
    CoreApi,
    SocialApi,
    Media,
    Ping,
}

impl ServiceKind {
    pub const ALL: [ServiceKind; 6] = [
        ServiceKind::ImWs,
        ServiceKind::ImQuic,
        ServiceKind::CoreApi,
        ServiceKind::SocialApi,
        ServiceKind::Media,
        ServiceKind::Ping,
    ];
    pub const COUNT: usize = 6;

    pub fn index(self) -> usize {
        match self {
            ServiceKind::ImWs => 0,
            ServiceKind::ImQuic => 1,
            ServiceKind::CoreApi => 2,
            ServiceKind::SocialApi => 3,
            ServiceKind::Media => 4,
            ServiceKind::Ping => 5,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ServiceKind::ImWs => "imWs",
            ServiceKind::ImQuic => "imQuic",
            ServiceKind::CoreApi => "coreApi",
            ServiceKind::SocialApi => "socialApi",
            ServiceKind::Media => "media",
            ServiceKind::Ping => "ping",
        }
    }
}

/// 选路的目标。IM 把 `imWs`/`imQuic` 视为同一个服务的两种协议：
/// 选路以接入点为单位，协议降级只在同一个接入点内发生。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ServiceClass {
    Im,
    CoreApi,
    SocialApi,
    Media,
}

impl ServiceClass {
    /// REST 类服务对应的单一入口；IM 返回 `None`（由协议计划决定用哪个入口）。
    pub fn http_service(self) -> Option<ServiceKind> {
        match self {
            ServiceClass::Im => None,
            ServiceClass::CoreApi => Some(ServiceKind::CoreApi),
            ServiceClass::SocialApi => Some(ServiceKind::SocialApi),
            ServiceClass::Media => Some(ServiceKind::Media),
        }
    }
}
