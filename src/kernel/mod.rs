//! Kernel-level contracts shared by runtime, application, extensions, and SDK facades.
//!
//! The kernel owns business-neutral event contracts, finite state machines, and
//! sync abstractions. It must not depend on application services or runtime
//! orchestration.

pub mod event;
pub mod fsm;
mod reliable_queue;
pub mod server_clock;
pub mod sync;
mod tenant_notice;

use std::sync::Arc;
use tokio::sync::RwLock;

pub use fsm::{
    MessageState, MessageStateEvent, MessageStateFsm, SyncFsm, SyncState, SyncTransition,
};
pub use reliable_queue::ReliableSendQueuePort;
pub use server_clock::ServerClock;
pub use sync::{
    ApplyOutcome, AttentionRegistry, AttentionState, ConvergeTarget, ConvergenceDriver,
    ConvergencePriority, ConvergenceScheduler, ConvergenceTarget, ConversationSummarySync,
    DeviceGlobalSyncPoint, DeviceGlobalSyncPointStore, DomainCursor, DomainDelta, DomainId,
    DomainItem, DomainPhase, DrainReport, LaneSpec, RouteReport, SessionSyncRunner, StartupClass,
    SyncContext, SyncDomain, SyncDomainRegistry, SyncFailurePolicy, SyncManager, SyncMode,
    SyncPhase, SyncProgress, SyncReason, SyncResponseHandler, SyncResult, SyncRunContext,
    SyncScope, SyncTask, SyncTaskResult, SyncTrigger, SyncVisibility, watermark_provably_clean,
};
pub use tenant_notice::TenantNoticePort;

/// 粗粒度的会话连接态，由链路状态机的对外阶段派生（`connection://status` 才是对外的连接状态）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SdkState {
    Disconnected,
    Connecting,
    Connected,
    Ready,
    Reconnecting,
}

impl SdkState {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            SdkState::Disconnected => 0,
            SdkState::Connecting => 1,
            SdkState::Connected => 2,
            SdkState::Ready => 3,
            SdkState::Reconnecting => 4,
        }
    }

    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            1 => SdkState::Connecting,
            2 => SdkState::Connected,
            3 => SdkState::Ready,
            4 => SdkState::Reconnecting,
            _ => SdkState::Disconnected,
        }
    }
}

/// 当前用户 ID 存储（连接后由 runtime 写入）
pub type CurrentUserIdStore = Arc<RwLock<String>>;
