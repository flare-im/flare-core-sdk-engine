//! Legacy crate-internal core facade.
//!
//! Real implementations live in `kernel/` and `runtime/`. This module keeps
//! the old crate-internal paths stable while T-09 removes the historical
//! `core -> application` ownership cycle.
//!
//! New code should import from `crate::kernel` or `crate::runtime` according to
//! ownership.

pub mod event {
    pub use crate::kernel::event::*;
}

pub use crate::kernel::{
    ConversationSummarySync, CurrentUserIdStore, MessageState, MessageStateEvent, MessageStateFsm,
    SessionSyncRunner, SyncContext, SyncFailurePolicy, SyncFsm, SyncManager, SyncMode, SyncPhase,
    SyncProgress, SyncReason, SyncResponseHandler, SyncResult, SyncRunContext, SyncScope,
    SyncState, SyncTask, SyncTaskResult, SyncTransition, SyncTrigger, SyncVisibility,
};
pub use crate::runtime::{Dispatcher, SdkEngine, SdkState};
pub(crate) use crate::runtime::{ReliableSendQueue, ReliableSendQueueConfig, SdkEngineConfig};
