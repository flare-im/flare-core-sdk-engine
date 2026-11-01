//! 事件系统：内部事件总线 + 类型化回调 API
//!
//! 流程：内部发布 [SdkEvent] → broadcast 通道 → 异步分发 → 调用已注册的 `on_*` 回调。
//! 不暴露大 trait，仅暴露类型化注册（如 [EventBus::on_conversation_updated]），便于跨语言绑定。
//!
//! # SDK 用户注册回调示例
//!
//! ```ignore
//! // 连接
//! client.on_connected(|| { println!("connected"); });
//! client.on_disconnected(|reason| { println!("disconnected: {}", reason); });
//! client.on_state_changed(|state| { ... });
//! client.on_sync_state_changed(|state| { ... });
//! client.on_server_error(|code, msg| { ... });
//!
//! // 消息
//! client.on_message(|msg| { ... });
//! client.on_send_ack(|ack| { ... });
//! client.on_send_failed(|client_msg_id, reason| { ... });
//! client.on_recalled(|conversation_id, event| { ... });
//! client.on_typing(|conversation_id, event| { ... });
//!
//! // 会话
//! client.on_conversation_synced(|ids| { ... });
//! client.on_conversation_updated(|conversation_id| { ... });
//! client.on_conversation_deleted(|conversation_id| { ... });
//!
//! // 扩展
//! client.on_extension(|source, event_type, payload| { ... });
//!
//! // 同步
//! client.on_sync_started(|| { ... });
//! client.on_sync_finished(|phase| { ... });
//! client.on_sync_failed(|task, message| { ... });
//! client.on_sync_progress(|task, progress, detail| { ... });
//! client.on_sync_task_completed(|task| { ... });
//!
//! // 任意事件（完整 SdkEvent）
//! client.on_any(|ev| { ... });
//! ```

mod event_bus;
mod name_change;
mod selector;
mod types;

pub use event_bus::{
    EventBus, EventReceiver, FilteredEventReceiver, PublishOutcome, RawSdkEvent,
    SharedEventReceiver, Subscription,
};
pub use selector::{
    ConnectionEventType, ConversationEventType, CustomEventDefinition, CustomEventSelector,
    EventFilter, ExtensionEventType, MessageEventType, NotificationEventType, PluginEventType,
    SdkEventKind, SdkEventType, SyncEventType, ViewEventType,
};
pub use types::{
    ConnectionEvent, ConversationEvent, ExtensionEvent, MessageEvent, NotificationEvent,
    PluginEvent, PluginRealtime, ReadinessStage, SdkEvent, SyncNotify, SyncPhase,
};

pub(crate) use name_change::NameChange;

use std::sync::Arc;

/// 共享事件（Arc 包装单条事件）
pub type SharedEvent = Arc<SdkEvent>;
