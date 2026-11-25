//! 客户端链路：连接状态机（[`LinkHandle`]）、故障转移决策、连接代数与对外的连接状态。
//!
//! 依赖方向：`client → runtime::link → flare-access / flare-im-transport`。选路、健康簿、熔断在
//! `flare-access`；这里决定生命周期（何时连、何时等、何时切、何时停），并把结果回写健康簿。

pub(crate) mod connection;
pub(crate) mod driver;
pub mod epoch;
pub mod failover;
pub(crate) mod heartbeat_learning;
pub(crate) mod reach;
pub mod status;
pub(crate) mod supervisor;
pub(crate) mod token;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Send + Sync> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub(crate) trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSendSync for T {}

pub(crate) use connection::{ConnectionRuntime, ProtocolMode};
pub(crate) use driver::{
    DeviceLinkState, LinkDriver, LinkFuture, LinkHeartbeat, LinkTarget, LinkUp, OpenRequest, boxed,
};
pub use epoch::ConnectionEpoch;
pub(crate) use reach::{DeviceReach, ReachProfile, device_state_report};
pub use status::{
    ActionRequiredReason, ConnectionPhase, ConnectionStatus, LinkPhase, LinkQuality, SwitchReason,
};
pub(crate) use supervisor::{LinkHandle, LinkSignal, LinkSignals, WakeSource};
pub(crate) use token::{ConnectToken, LinkTokenSource};
