//! 内部事件总线 + 类型化回调 API
//!
//! 流程：内部发布 SdkEvent → bounded fan-out → 按事件类型调用已注册的回调和 raw 订阅者。
//! 不暴露大 trait，仅暴露 `on_*` 类型化注册，便于跨语言绑定（Swift / Kotlin / TypeScript）。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use flare_proto::common::{
    MessageRecallEvent, ReadReceiptEvent, SendAck, TypingAggregatePacket, TypingStatePacket,
};

use crate::model::IMMessage;
use tokio::sync::mpsc;

use super::selector::EventFilter;
use super::types::{
    ConnectionEvent, ConversationEvent, MessageEvent, NotificationEvent, SdkEvent, SyncNotify,
    SyncPhase,
};
use crate::extension::middleware::MiddlewareChain;
use crate::kernel::{SdkState, SyncState};
use crate::spi::metrics::MetricsRecorder;
use tracing::warn;

/// 每个 raw 订阅者的默认队列容量。慢消费者超过容量时会被丢事件保护 SDK 内存。
const BUS_CAPACITY: usize = 2048;
static RAW_SUBSCRIBER_DROPPED: AtomicU64 = AtomicU64::new(0);

mod dispatch;
mod listeners;
mod receivers;
use dispatch::{
    RecoverableRwLock, dispatch_any_callbacks, dispatch_callbacks, dispatch_callbacks_with,
    dispatch_route_callbacks, take_unreported_callback_drops,
};
pub use receivers::{
    EventReceiveError, EventReceiver, FilteredEventReceiver, RawSdkEvent, SharedEventReceiver,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    Published { receiver_count: usize },
    DroppedSilentSync,
    DroppedByMiddleware,
    NoReceivers,
}

/// 释放 `reporting_callback_drops` 门的 RAII 卫兵：即便补发路径 panic 也不会把门
/// 永久留在占用态（那会让此后所有回调丢弃都不再被通报）。
struct ReportingGuard<'a>(&'a AtomicBool);

impl Drop for ReportingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn same_arc<T: ?Sized>(left: &Arc<T>, right: &Arc<T>) -> bool {
    Arc::ptr_eq(left, right)
}

fn same_event_route(left: &EventRoute, right: &EventRoute) -> bool {
    left.filter == right.filter && Arc::ptr_eq(&left.callback, &right.callback)
}

// 回调存储：Arc 便于 clone 后传入 spawn_blocking，不阻塞分发循环
type FnConnected = Arc<dyn Fn() + Send + Sync>;
type FnDisconnected = Arc<dyn Fn(String) + Send + Sync>;
type FnStateChanged = Arc<dyn Fn(SdkState) + Send + Sync>;
type FnSyncStateChanged = Arc<dyn Fn(SyncState) + Send + Sync>;
type FnServerError = Arc<dyn Fn(i32, String) + Send + Sync>;
type FnKickedOff = Arc<dyn Fn(String) + Send + Sync>;
type FnTokenExpired = Arc<dyn Fn(String) + Send + Sync>;
type FnMessage = Arc<dyn Fn(IMMessage) + Send + Sync>;
type FnMessageBatch = Arc<dyn Fn(Vec<IMMessage>) + Send + Sync>;
type FnSendAck = Arc<dyn Fn(SendAck) + Send + Sync>;
type FnSendFailed = Arc<dyn Fn(String, String) + Send + Sync>;
type FnRecalled = Arc<dyn Fn(String, MessageRecallEvent) + Send + Sync>;
type FnTyping = Arc<dyn Fn(String, TypingStatePacket) + Send + Sync>;
type FnTypingAggregate = Arc<dyn Fn(String, TypingAggregatePacket) + Send + Sync>;
type FnReadReceipt = Arc<dyn Fn(String, ReadReceiptEvent) + Send + Sync>;
type FnConversationIds = Arc<dyn Fn(Vec<String>) + Send + Sync>;
type FnConversationId = Arc<dyn Fn(String) + Send + Sync>;
type FnConversationUnreadCountChanged = Arc<dyn Fn(String, u32) + Send + Sync>;
type FnExtension = Arc<dyn Fn(String, String, Vec<u8>) + Send + Sync>;
type FnNotification = Arc<dyn Fn(IMMessage) + Send + Sync>;
type FnSyncPhase = Arc<dyn Fn(SyncPhase) + Send + Sync>;
type FnSyncProgress = Arc<dyn Fn(String, f32, String) + Send + Sync>;
type FnAny = Arc<dyn Fn(Arc<SdkEvent>) + Send + Sync>;

#[derive(Clone)]
struct EventRoute {
    filter: EventFilter,
    callback: FnAny,
}

#[derive(Clone)]
struct RawSubscriber {
    filter: EventFilter,
    tx: mpsc::Sender<SdkEvent>,
    lagged: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
}

#[derive(Clone)]
struct SharedRawSubscriber {
    filter: EventFilter,
    tx: mpsc::Sender<Arc<RawSdkEvent>>,
    lagged: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct EventBus {
    middleware: Arc<MiddlewareChain>,
    raw_subscriber_capacity: usize,
    subscribers: Arc<RwLock<Vec<RawSubscriber>>>,
    shared_subscribers: Arc<RwLock<Vec<SharedRawSubscriber>>>,
    typed_callback_count: Arc<AtomicUsize>,
    route_callback_count: Arc<AtomicUsize>,
    any_callback_count: Arc<AtomicUsize>,
    /// 正在补发回调丢弃通知（重入保护，见 `report_pending_callback_drops`）。
    reporting_callback_drops: Arc<std::sync::atomic::AtomicBool>,
    last_connection_state: Arc<RwLock<Option<SdkState>>>,
    last_sync_state: Arc<RwLock<Option<SyncState>>>,
    last_sync_finished: Arc<RwLock<Option<SyncPhase>>>,
    // Connection（std::sync::RwLock 支持同步注册，回调在专用分发线程中执行）
    on_connected: Arc<RwLock<Vec<FnConnected>>>,
    on_disconnected: Arc<RwLock<Vec<FnDisconnected>>>,
    on_state_changed: Arc<RwLock<Vec<FnStateChanged>>>,
    on_sync_state_changed: Arc<RwLock<Vec<FnSyncStateChanged>>>,
    on_server_error: Arc<RwLock<Vec<FnServerError>>>,
    on_kicked_off: Arc<RwLock<Vec<FnKickedOff>>>,
    on_token_expired: Arc<RwLock<Vec<FnTokenExpired>>>,
    // Message
    on_message: Arc<RwLock<Vec<FnMessage>>>,
    on_message_batch: Arc<RwLock<Vec<FnMessageBatch>>>,
    on_send_ack: Arc<RwLock<Vec<FnSendAck>>>,
    on_send_failed: Arc<RwLock<Vec<FnSendFailed>>>,
    on_recalled: Arc<RwLock<Vec<FnRecalled>>>,
    on_typing: Arc<RwLock<Vec<FnTyping>>>,
    on_typing_aggregate: Arc<RwLock<Vec<FnTypingAggregate>>>,
    on_read_receipt: Arc<RwLock<Vec<FnReadReceipt>>>,
    // Conversation
    on_conversation_synced: Arc<RwLock<Vec<FnConversationIds>>>,
    on_conversation_created: Arc<RwLock<Vec<FnConversationId>>>,
    on_conversation_updated: Arc<RwLock<Vec<FnConversationId>>>,
    on_conversation_unread_count_changed: Arc<RwLock<Vec<FnConversationUnreadCountChanged>>>,
    on_conversation_deleted: Arc<RwLock<Vec<FnConversationId>>>,
    // Extension
    on_extension: Arc<RwLock<Vec<FnExtension>>>,
    on_notification: Arc<RwLock<Vec<FnNotification>>>,
    // Sync
    on_sync_started: Arc<RwLock<Vec<FnConnected>>>,
    on_sync_finished: Arc<RwLock<Vec<FnSyncPhase>>>,
    on_sync_failed: Arc<RwLock<Vec<FnSendFailed>>>,
    on_sync_progress: Arc<RwLock<Vec<FnSyncProgress>>>,
    on_sync_task_completed: Arc<RwLock<Vec<FnConversationId>>>,
    // Filtered event routes
    routes: Arc<RwLock<Vec<EventRoute>>>,
    // Any
    on_any: Arc<RwLock<Vec<FnAny>>>,
    metrics: MetricsRecorder,
}

impl EventBus {
    pub fn new() -> Self {
        Self::with_capacity(BUS_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_middleware_and_capacity(Arc::new(MiddlewareChain::new()), capacity)
    }

    pub fn with_middleware(middleware: Arc<MiddlewareChain>) -> Self {
        Self::with_middleware_and_capacity(middleware, BUS_CAPACITY)
    }

    pub fn with_middleware_and_capacity(middleware: Arc<MiddlewareChain>, capacity: usize) -> Self {
        Self::with_middleware_capacity_and_metrics(
            middleware,
            capacity,
            MetricsRecorder::disabled(),
        )
    }

    pub(crate) fn with_middleware_capacity_and_metrics(
        middleware: Arc<MiddlewareChain>,
        capacity: usize,
        metrics: MetricsRecorder,
    ) -> Self {
        let raw_subscriber_capacity = capacity.max(1);
        let subscribers = Arc::new(RwLock::new(Vec::new()));
        let shared_subscribers = Arc::new(RwLock::new(Vec::new()));
        let typed_callback_count = Arc::new(AtomicUsize::new(0));
        let route_callback_count = Arc::new(AtomicUsize::new(0));
        let any_callback_count = Arc::new(AtomicUsize::new(0));
        let reporting_callback_drops = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let last_connection_state = Arc::new(RwLock::new(None));
        let last_sync_state = Arc::new(RwLock::new(None));
        let last_sync_finished = Arc::new(RwLock::new(None));

        let on_connected = Arc::new(RwLock::new(Vec::<FnConnected>::new()));
        let on_disconnected = Arc::new(RwLock::new(Vec::new()));
        let on_state_changed = Arc::new(RwLock::new(Vec::new()));
        let on_sync_state_changed = Arc::new(RwLock::new(Vec::new()));
        let on_server_error = Arc::new(RwLock::new(Vec::new()));
        let on_kicked_off = Arc::new(RwLock::new(Vec::new()));
        let on_token_expired = Arc::new(RwLock::new(Vec::new()));
        let on_message = Arc::new(RwLock::new(Vec::new()));
        let on_message_batch = Arc::new(RwLock::new(Vec::new()));
        let on_send_ack = Arc::new(RwLock::new(Vec::new()));
        let on_send_failed = Arc::new(RwLock::new(Vec::new()));
        let on_recalled = Arc::new(RwLock::new(Vec::new()));
        let on_typing = Arc::new(RwLock::new(Vec::new()));
        let on_typing_aggregate = Arc::new(RwLock::new(Vec::new()));
        let on_read_receipt = Arc::new(RwLock::new(Vec::new()));
        let on_conversation_synced = Arc::new(RwLock::new(Vec::new()));
        let on_conversation_created = Arc::new(RwLock::new(Vec::new()));
        let on_conversation_updated = Arc::new(RwLock::new(Vec::new()));
        let on_conversation_unread_count_changed = Arc::new(RwLock::new(Vec::new()));
        let on_conversation_deleted = Arc::new(RwLock::new(Vec::new()));
        let on_extension = Arc::new(RwLock::new(Vec::new()));
        let on_notification = Arc::new(RwLock::new(Vec::new()));
        let on_sync_started = Arc::new(RwLock::new(Vec::new()));
        let on_sync_finished = Arc::new(RwLock::new(Vec::new()));
        let on_sync_failed = Arc::new(RwLock::new(Vec::new()));
        let on_sync_progress = Arc::new(RwLock::new(Vec::new()));
        let on_sync_task_completed = Arc::new(RwLock::new(Vec::new()));
        let routes = Arc::new(RwLock::new(Vec::new()));
        let on_any = Arc::new(RwLock::new(Vec::new()));

        Self {
            middleware,
            raw_subscriber_capacity,
            subscribers,
            shared_subscribers,
            typed_callback_count,
            route_callback_count,
            any_callback_count,
            reporting_callback_drops,
            last_connection_state,
            last_sync_state,
            last_sync_finished,
            on_connected,
            on_disconnected,
            on_state_changed,
            on_sync_state_changed,
            on_server_error,
            on_kicked_off,
            on_token_expired,
            on_message,
            on_message_batch,
            on_send_ack,
            on_send_failed,
            on_recalled,
            on_typing,
            on_typing_aggregate,
            on_read_receipt,
            on_conversation_synced,
            on_conversation_created,
            on_conversation_updated,
            on_conversation_unread_count_changed,
            on_conversation_deleted,
            on_extension,
            on_notification,
            on_sync_started,
            on_sync_finished,
            on_sync_failed,
            on_sync_progress,
            on_sync_task_completed,
            routes,
            on_any,
            metrics,
        }
    }

    fn publish_to_subscribers(&self, event: &SdkEvent) -> usize {
        let mut delivered = 0;
        let mut has_closed = false;
        let mut shared_has_closed = false;
        let mut dropped_full = 0usize;

        // try_send 全程同步不 await → 持读锁遍历即可，免去每次 publish 克隆整张订阅者表
        //（含 EventFilter 内 String 的逐个克隆）——这是接收热路径，每条消息都会走到。
        {
            let subscribers = self.subscribers.safe_read("event_bus");
            for subscriber in subscribers.iter() {
                if subscriber.tx.is_closed() {
                    has_closed = true;
                    continue;
                }
                if !subscriber.filter.matches(event) {
                    continue;
                }
                match subscriber.tx.try_send(event.clone()) {
                    Ok(()) => delivered += 1,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        has_closed = true;
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        subscriber.lagged.store(true, Ordering::Release);
                        subscriber.dropped_events.fetch_add(1, Ordering::Relaxed);
                        dropped_full += 1;
                    }
                }
            }
        }

        {
            let shared_subscribers = self.shared_subscribers.safe_read("event_bus");
            let mut raw_event: Option<Arc<RawSdkEvent>> = None;
            for subscriber in shared_subscribers.iter() {
                if subscriber.tx.is_closed() {
                    shared_has_closed = true;
                    continue;
                }
                if !subscriber.filter.matches(event) {
                    continue;
                }
                let event = raw_event
                    .get_or_insert_with(|| Arc::new(RawSdkEvent::from_event(event.clone())));
                match subscriber.tx.try_send(Arc::clone(event)) {
                    Ok(()) => delivered += 1,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        shared_has_closed = true;
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        subscriber.lagged.store(true, Ordering::Release);
                        subscriber.dropped_events.fetch_add(1, Ordering::Relaxed);
                        dropped_full += 1;
                    }
                }
            }
        }

        if dropped_full > 0 {
            let previous = RAW_SUBSCRIBER_DROPPED.fetch_add(dropped_full as u64, Ordering::Relaxed);
            let current = previous + dropped_full as u64;
            self.metrics
                .counter("event.raw_subscriber_dropped_total", dropped_full as u64);
            if previous == 0 || previous / 1024 != current / 1024 {
                warn!(
                    dropped = dropped_full,
                    total_dropped = current,
                    "EventBus raw subscriber queue full; dropping events for slow subscribers"
                );
            }
        }

        if has_closed {
            self.subscribers
                .safe_write("event_bus")
                .retain(|subscriber| !subscriber.tx.is_closed());
        }
        if shared_has_closed {
            self.shared_subscribers
                .safe_write("event_bus")
                .retain(|subscriber| !subscriber.tx.is_closed());
        }

        delivered
    }

    pub fn raw_subscriber_dropped_total() -> u64 {
        RAW_SUBSCRIBER_DROPPED.load(Ordering::Relaxed)
    }

    fn dispatch_typed_callbacks(&self, ev: &SdkEvent) -> usize {
        let mut dispatched = 0;
        match ev {
            SdkEvent::Connection(ce) => match ce {
                ConnectionEvent::Connected => {
                    dispatched += dispatch_callbacks(&self.on_connected, |f| f());
                }
                ConnectionEvent::Disconnected { reason } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_disconnected,
                        || reason.clone(),
                        |f, reason| f(reason.clone()),
                    );
                }
                ConnectionEvent::StateChanged { state } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_state_changed,
                        || *state,
                        |f, state| f(*state),
                    );
                }
                ConnectionEvent::SyncStateChanged { state } => {
                    let s = *state;
                    dispatched += dispatch_callbacks(&self.on_sync_state_changed, move |f| {
                        f(s);
                    });
                }
                ConnectionEvent::ServerError { code, message } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_server_error,
                        || (*code, message.clone()),
                        |f, (code, message)| f(*code, message.clone()),
                    );
                }
                ConnectionEvent::KickedOff { reason } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_kicked_off,
                        || reason.clone(),
                        |f, reason| f(reason.clone()),
                    );
                }
                ConnectionEvent::TokenExpired { message } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_token_expired,
                        || message.clone(),
                        |f, message| f(message.clone()),
                    );
                }
                ConnectionEvent::Reconnecting { .. } => {}
            },
            SdkEvent::Message(me) => match me {
                MessageEvent::Received { message } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_message,
                        || message.as_ref().clone(),
                        |f, message| f(message.clone()),
                    );
                }
                MessageEvent::ReceivedBatch { messages } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_message_batch,
                        || messages.clone(),
                        |f, messages| f(messages.clone()),
                    );
                }
                MessageEvent::SendAck { ack } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_send_ack,
                        || ack.as_ref().clone(),
                        |f, ack| f(ack.clone()),
                    );
                }
                MessageEvent::SendFailed {
                    client_msg_id,
                    reason,
                    ..
                } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_send_failed,
                        || (client_msg_id.clone(), reason.clone()),
                        |f, (client_msg_id, reason)| {
                            f(client_msg_id.clone(), reason.clone());
                        },
                    );
                }
                MessageEvent::Recalled {
                    conversation_id,
                    event,
                } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_recalled,
                        || (conversation_id.clone(), event.clone()),
                        |f, (conversation_id, event)| {
                            f(conversation_id.clone(), event.clone());
                        },
                    );
                }
                MessageEvent::Typing {
                    conversation_id,
                    event,
                } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_typing,
                        || (conversation_id.clone(), event.clone()),
                        |f, (conversation_id, event)| {
                            f(conversation_id.clone(), event.clone());
                        },
                    );
                }
                MessageEvent::TypingAggregate {
                    conversation_id,
                    event,
                } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_typing_aggregate,
                        || (conversation_id.clone(), event.clone()),
                        |f, (conversation_id, event)| {
                            f(conversation_id.clone(), event.clone());
                        },
                    );
                }
                MessageEvent::ReadReceipt {
                    conversation_id,
                    event,
                } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_read_receipt,
                        || (conversation_id.clone(), event.clone()),
                        |f, (conversation_id, event)| {
                            f(conversation_id.clone(), event.clone());
                        },
                    );
                }
                MessageEvent::Edited { .. }
                | MessageEvent::ReactionChanged { .. }
                | MessageEvent::Deleted { .. }
                | MessageEvent::RetentionScheduled { .. }
                | MessageEvent::RetentionExpired { .. }
                | MessageEvent::RetentionPurged { .. }
                | MessageEvent::Pinned { .. }
                | MessageEvent::Unpinned { .. }
                | MessageEvent::Marked { .. }
                | MessageEvent::Unmarked { .. }
                | MessageEvent::PresenceChanged { .. }
                | MessageEvent::Custom { .. }
                | MessageEvent::PluginStateChanged { .. }
                | MessageEvent::ReadSummariesChanged { .. } => {}
            },
            SdkEvent::Notification(NotificationEvent::Received { message }) => {
                dispatched += dispatch_callbacks_with(
                    &self.on_notification,
                    || message.as_ref().clone(),
                    |f, message| f(message.clone()),
                );
            }
            SdkEvent::Conversation(ce) => match ce {
                ConversationEvent::Synced { conversation_ids } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_conversation_synced,
                        || conversation_ids.clone(),
                        |f, conversation_ids| f(conversation_ids.clone()),
                    );
                }
                ConversationEvent::Created { conversation_id } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_conversation_created,
                        || conversation_id.clone(),
                        |f, conversation_id| f(conversation_id.clone()),
                    );
                }
                ConversationEvent::Updated { conversation_id } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_conversation_updated,
                        || conversation_id.clone(),
                        |f, conversation_id| f(conversation_id.clone()),
                    );
                }
                ConversationEvent::UnreadCountChanged {
                    conversation_id,
                    unread_count,
                } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_conversation_unread_count_changed,
                        || (conversation_id.clone(), *unread_count),
                        |f, (conversation_id, unread_count)| {
                            f(conversation_id.clone(), *unread_count);
                        },
                    );
                }
                ConversationEvent::Deleted { conversation_id } => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_conversation_deleted,
                        || conversation_id.clone(),
                        |f, conversation_id| f(conversation_id.clone()),
                    );
                }
            },
            SdkEvent::Sync(se) => match se {
                SyncNotify::Started { run } if run.visibility.emits_status_events() => {
                    dispatched += dispatch_callbacks(&self.on_sync_started, |f| f());
                }
                SyncNotify::Finished { run, phase } if run.visibility.emits_status_events() => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_sync_finished,
                        || phase.clone(),
                        |f, phase| f(phase.clone()),
                    );
                }
                SyncNotify::Failed { run, task, message }
                    if run.visibility.emits_status_events() =>
                {
                    dispatched += dispatch_callbacks_with(
                        &self.on_sync_failed,
                        || (task.clone(), message.clone()),
                        |f, (task, message)| f(task.clone(), message.clone()),
                    );
                }
                SyncNotify::Progress {
                    run,
                    task,
                    progress,
                    detail,
                } if run.visibility.emits_status_events() => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_sync_progress,
                        || (task.clone(), *progress, detail.clone()),
                        |f, (task, progress, detail)| {
                            f(task.clone(), *progress, detail.clone());
                        },
                    );
                }
                SyncNotify::TaskCompleted { run, task } if run.visibility.emits_status_events() => {
                    dispatched += dispatch_callbacks_with(
                        &self.on_sync_task_completed,
                        || task.clone(),
                        |f, task| f(task.clone()),
                    );
                }
                SyncNotify::StateChanged { run, state } if run.visibility.emits_status_events() => {
                    let s = *state;
                    dispatched += dispatch_callbacks(&self.on_sync_state_changed, move |f| {
                        f(s);
                    });
                }
                _ => {}
            },
            SdkEvent::Extension(ext) => {
                dispatched += dispatch_callbacks_with(
                    &self.on_extension,
                    || {
                        (
                            ext.source.clone(),
                            ext.event_type.clone(),
                            ext.payload.clone(),
                        )
                    },
                    |f, (source, event_type, payload)| {
                        f(source.clone(), event_type.clone(), payload.clone());
                    },
                );
            }
            SdkEvent::View(_) | SdkEvent::Plugin(_) => {}
        }
        dispatched
    }

    fn has_typed_callbacks(&self) -> bool {
        self.typed_callback_count.load(Ordering::Acquire) > 0
    }

    fn has_route_callbacks(&self) -> bool {
        self.route_callback_count.load(Ordering::Acquire) > 0
    }

    fn has_any_callbacks(&self) -> bool {
        self.any_callback_count.load(Ordering::Acquire) > 0
    }

    /// 回调队列曾丢弃投递 → 补发一条 `ResyncNeeded`，让应用层从 SDK 存储重建读模型。
    ///
    /// 丢弃本身是有意的削峰，但 `Fn` 回调消费者（`on_message` 等）没有 raw subscriber 的
    /// lag 兜底，丢了就是消息不上屏且无任何提示。合并上报：一个丢弃窗口只发一条。
    fn report_pending_callback_drops(&self) {
        // 先拿门再取数：反过来会在「取走计数后发现门被占」时把那批丢弃数吞掉——
        // 补发自身的嵌套 publish 与并发 publish 都会走到这条路径。门被占时一个数都不取，
        // 计数留给持门者或下一次发布。
        if self.reporting_callback_drops.swap(true, Ordering::AcqRel) {
            return;
        }
        let _guard = ReportingGuard(&self.reporting_callback_drops);
        let dropped_events = take_unreported_callback_drops();
        if dropped_events == 0 {
            return;
        }
        self.publish(SdkEvent::Sync(SyncNotify::ResyncNeeded {
            scope: "global".to_string(),
            reason: "callback_dispatch_dropped".to_string(),
            dropped_events,
        }));
    }

    pub fn publish(&self, mut event: SdkEvent) -> PublishOutcome {
        if matches!(&event, SdkEvent::Sync(sync) if !sync.should_publish()) {
            return PublishOutcome::DroppedSilentSync;
        }
        self.report_pending_callback_drops();
        if self.middleware.before_publish(&mut event).is_drop() {
            return PublishOutcome::DroppedByMiddleware;
        }
        match &event {
            SdkEvent::Connection(ConnectionEvent::StateChanged { state }) => {
                *self.last_connection_state.safe_write("event_bus") = Some(*state);
            }
            SdkEvent::Connection(ConnectionEvent::Connected) => {
                *self.last_connection_state.safe_write("event_bus") = Some(SdkState::Connected);
            }
            SdkEvent::Connection(ConnectionEvent::Disconnected { .. }) => {
                *self.last_connection_state.safe_write("event_bus") = Some(SdkState::Disconnected);
            }
            SdkEvent::Sync(SyncNotify::StateChanged { state, .. }) => {
                *self.last_sync_state.safe_write("event_bus") = Some(*state);
            }
            SdkEvent::Sync(SyncNotify::Finished { phase, .. }) => {
                *self.last_sync_finished.safe_write("event_bus") = Some(phase.clone());
            }
            SdkEvent::Sync(SyncNotify::Started { .. }) => {
                *self.last_sync_finished.safe_write("event_bus") = None;
            }
            _ => {}
        }
        self.middleware.on_publish(&event);
        let raw_count = self.publish_to_subscribers(&event);
        let typed_count = if self.has_typed_callbacks() {
            self.dispatch_typed_callbacks(&event)
        } else {
            0
        };
        let route_count = if self.has_route_callbacks() {
            dispatch_route_callbacks(&self.routes, &event)
        } else {
            0
        };
        let any_count = if self.has_any_callbacks() {
            dispatch_any_callbacks(&self.on_any, event)
        } else {
            0
        };
        let receiver_count = raw_count + typed_count + route_count + any_count;
        if receiver_count == 0 {
            PublishOutcome::NoReceivers
        } else {
            PublishOutcome::Published { receiver_count }
        }
    }

    pub fn publish_extension(
        &self,
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> PublishOutcome {
        self.publish(SdkEvent::custom_extension(source, event_type, payload))
    }

    pub fn subscribe(&self) -> EventReceiver {
        self.subscribe_filter(EventFilter::Any)
    }

    pub fn subscribe_filter(&self, filter: impl Into<EventFilter>) -> FilteredEventReceiver {
        let filter = filter.into();
        let (tx, rx) = mpsc::channel(self.raw_subscriber_capacity);
        let lagged = Arc::new(AtomicBool::new(false));
        let dropped_events = Arc::new(AtomicU64::new(0));
        self.subscribers
            .safe_write("event_bus")
            .push(RawSubscriber {
                filter: filter.clone(),
                tx,
                lagged: lagged.clone(),
                dropped_events: dropped_events.clone(),
            });
        EventReceiver::new(rx, filter, lagged, dropped_events)
    }

    pub fn subscribe_shared_raw(&self) -> SharedEventReceiver {
        self.subscribe_shared_filter(EventFilter::Any)
    }

    pub fn subscribe_shared_filter(&self, filter: impl Into<EventFilter>) -> SharedEventReceiver {
        let filter = filter.into();
        let (tx, rx) = mpsc::channel(self.raw_subscriber_capacity);
        let lagged = Arc::new(AtomicBool::new(false));
        let dropped_events = Arc::new(AtomicU64::new(0));
        self.shared_subscribers
            .safe_write("event_bus")
            .push(SharedRawSubscriber {
                filter: filter.clone(),
                tx,
                lagged: lagged.clone(),
                dropped_events: dropped_events.clone(),
            });
        SharedEventReceiver::new(rx, filter, lagged, dropped_events)
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Callback subscription handle.
///
/// Dropping the handle removes the callback from the EventBus. This keeps
/// hot-reload, screen remount, and long-running app sessions from accumulating
/// stale host callbacks.
pub struct Subscription {
    cleanup: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

#[cfg(test)]
mod tests;
