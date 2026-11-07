//! 客户端链路：连接状态机（[`LinkHandle`]）、故障转移决策、连接代数与对外的连接状态。
//!
//! 依赖方向：`client → runtime::link → flare-access / flare-transport`。选路、健康簿、熔断在
//! `flare-access`；这里决定生命周期（何时连、何时等、何时切、何时停），并把结果回写健康簿。

pub mod access;
pub(crate) mod driver;
pub mod epoch;
pub mod failover;
#[cfg(test)]
mod golden_vectors;
pub mod refresher;
pub mod status;
pub(crate) mod supervisor;
pub(crate) mod token;

pub(crate) use flare_access::MaybeSendSync;

#[cfg(not(target_arch = "wasm32"))]
pub use access::{AccessLab, DialRewrite};
pub use access::{
    AccessPointDto, AccessRuntime, AccessStatusDto, AccessStoreSource, AccessUnavailable,
    ConnectAckSummary, CurrentLink, RecoveryImportDto,
};
pub(crate) use driver::{
    LinkDriver, LinkFuture, LinkHeartbeat, LinkTarget, LinkUp, OpenRequest, boxed,
};
pub use epoch::ConnectionEpoch;
pub use refresher::{RecoveringStatus, RefreshAttemptDto, RefreshReportDto, RefreshStatus};
pub use status::{
    AccessEndpoint, AccessSwitched, ActionRequiredReason, ConnectionPhase, ConnectionStatus,
    LinkPhase, LinkQuality, SwitchReason,
};
pub(crate) use supervisor::{LinkHandle, LinkSignal, LinkSignals};
pub(crate) use token::{ConnectToken, LinkTokenSource};
