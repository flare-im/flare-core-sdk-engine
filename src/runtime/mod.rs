//! Runtime orchestration for the SDK.
//!
//! Runtime wires transport packets, sync orchestration, and reliable delivery
//! actors to application use cases. Unlike the kernel, runtime may depend on
//! application services.

mod dispatcher;
mod engine;
pub(crate) mod link;
mod reliable_queue;
pub(crate) mod update;

pub use crate::kernel::SdkState;
pub use dispatcher::Dispatcher;
pub use engine::SdkEngine;
pub(crate) use engine::SdkEngineConfig;
pub(crate) use reliable_queue::{ReliableSendQueue, ReliableSendQueueConfig};
