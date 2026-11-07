//! 有限状态机（FSM）
//!
//! 消息、同步两类状态；转移显式定义、由事件触发，保证流程不乱。连接的生命周期由
//! `runtime::link` 的链路状态机负责。

mod message_state;
mod sync_state;

pub use message_state::{MessageState, MessageStateEvent, MessageStateFsm};
pub use sync_state::{SyncFsm, SyncState, SyncTransition};
