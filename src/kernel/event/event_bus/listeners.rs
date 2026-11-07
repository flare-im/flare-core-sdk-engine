use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use flare_proto::common::{
    CustomEvent, MessageRecallEvent, ReadReceiptEvent, SendAck, TypingAggregatePacket,
    TypingStatePacket,
};

use crate::kernel::SyncState;

use super::super::link_status::ConnectionStatus;
use crate::model::IMMessage;

use super::super::selector::{
    CustomEventSelector, EventFilter, ExtensionEventType, MessageEventType, NotificationEventType,
    PluginEventType, SdkEventKind, SdkEventType,
};
use super::super::types::{
    MessageEvent, NotificationEvent, PluginEvent, PluginRealtime, SdkEvent, SyncPhase,
};
use super::dispatch::{RecoverableRwLock, replay_after_dispatch_window};
use super::{
    EventBus, EventReceiver, EventRoute, FilteredEventReceiver, FnAny, FnConnected,
    FnConnectionStatus, FnConversationId, FnConversationIds, FnConversationUnreadCountChanged,
    FnExtension, FnMessage, FnMessageBatch, FnNotification, FnReadReceipt, FnRecalled, FnSendAck,
    FnSendFailed, FnSyncPhase, FnSyncProgress, FnSyncStateChanged, FnTyping, FnTypingAggregate,
    Subscription, same_arc, same_event_route,
};

impl EventBus {
    pub fn subscribe_kind(&self, kind: SdkEventKind) -> FilteredEventReceiver {
        self.subscribe_filter(kind)
    }

    pub fn subscribe_event_type(&self, event_type: SdkEventType) -> FilteredEventReceiver {
        self.subscribe_filter(event_type)
    }

    fn callback_subscription<T, S>(
        &self,
        callbacks: &Arc<RwLock<Vec<T>>>,
        active_callbacks: &Arc<AtomicUsize>,
        callback: T,
        same: S,
    ) -> Subscription
    where
        T: Clone + Send + Sync + 'static,
        S: Fn(&T, &T) -> bool + Send + Sync + 'static,
    {
        active_callbacks.fetch_add(1, Ordering::AcqRel);
        callbacks.safe_write("event_bus").push(callback.clone());
        let callbacks = callbacks.clone();
        let active_callbacks = active_callbacks.clone();
        Subscription {
            cleanup: Some(Box::new(move || {
                callbacks
                    .safe_write("event_bus")
                    .retain(|stored| !same(stored, &callback));
                active_callbacks.fetch_sub(1, Ordering::AcqRel);
            })),
        }
    }

    // ---------- Connection ----------
    /// 注册「连接状态」回调（`connection://status`）。注册时已有状态的，立即补收一次最近的状态。
    pub fn on_connection_status<F>(&self, f: F) -> Subscription
    where
        F: Fn(ConnectionStatus) + Send + Sync + 'static,
    {
        let f = Arc::new(f) as FnConnectionStatus;
        let invoked = Arc::new(AtomicBool::new(false));
        let callback = {
            let f = f.clone();
            let invoked = invoked.clone();
            Arc::new(move |status| {
                invoked.store(true, Ordering::Release);
                f(status);
            }) as FnConnectionStatus
        };
        let last = self.last_connection_status.safe_read("event_bus").clone();
        let subscription = self.callback_subscription(
            &self.on_connection_status,
            &self.typed_callback_count,
            callback,
            same_arc,
        );
        if let Some(status) = last {
            replay_after_dispatch_window(move || {
                if !invoked.load(Ordering::Acquire) {
                    f(status);
                }
            });
        }
        subscription
    }

    /// 注册「同步状态变更」回调
    pub fn on_sync_state_changed<F>(&self, f: F) -> Subscription
    where
        F: Fn(SyncState) + Send + Sync + 'static,
    {
        let f = Arc::new(f) as FnSyncStateChanged;
        let invoked = Arc::new(AtomicBool::new(false));
        let callback = {
            let f = f.clone();
            let invoked = invoked.clone();
            Arc::new(move |state| {
                invoked.store(true, Ordering::Release);
                f(state);
            }) as FnSyncStateChanged
        };
        let last = *self.last_sync_state.safe_read("event_bus");
        let subscription = self.callback_subscription(
            &self.on_sync_state_changed,
            &self.typed_callback_count,
            callback,
            same_arc,
        );
        if let Some(state) = last {
            replay_after_dispatch_window(move || {
                if !invoked.load(Ordering::Acquire) {
                    f(state);
                }
            });
        }
        subscription
    }

    // ---------- Message ----------
    /// 注册「收到一条新消息」回调（参数为 SDK 统一类型 IMMessage）
    pub fn on_message<F>(&self, f: F) -> Subscription
    where
        F: Fn(&IMMessage) + Send + Sync + 'static,
    {
        let f: FnMessage = Arc::new(move |m| f(&m));
        self.callback_subscription(&self.on_message, &self.typed_callback_count, f, same_arc)
    }

    /// 注册「新消息批量」回调（同步或批量推送时一次多条，参数为 IMMessage 切片）
    pub fn on_message_batch<F>(&self, f: F) -> Subscription
    where
        F: Fn(&[IMMessage]) + Send + Sync + 'static,
    {
        let f: FnMessageBatch = Arc::new(move |msgs| f(msgs.as_slice()));
        self.callback_subscription(
            &self.on_message_batch,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「发送回执」回调
    pub fn on_send_ack<F>(&self, f: F) -> Subscription
    where
        F: Fn(&SendAck) + Send + Sync + 'static,
    {
        let f: FnSendAck = Arc::new(move |a| f(&a));
        self.callback_subscription(&self.on_send_ack, &self.typed_callback_count, f, same_arc)
    }

    /// 注册「发送失败」回调
    pub fn on_send_failed<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, &str) + Send + Sync + 'static,
    {
        let f: FnSendFailed = Arc::new(move |id, r| f(id.as_str(), r.as_str()));
        self.callback_subscription(
            &self.on_send_failed,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「消息撤回」回调
    pub fn on_recalled<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, &MessageRecallEvent) + Send + Sync + 'static,
    {
        let f: FnRecalled = Arc::new(move |cid, e| f(cid.as_str(), &e));
        self.callback_subscription(&self.on_recalled, &self.typed_callback_count, f, same_arc)
    }

    /// 注册「正在输入」回调（DATA realtime_control.typing）
    pub fn on_typing<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, &TypingStatePacket) + Send + Sync + 'static,
    {
        let f: FnTyping = Arc::new(move |cid, e| f(cid.as_str(), &e));
        self.callback_subscription(&self.on_typing, &self.typed_callback_count, f, same_arc)
    }

    /// 注册「N 人正在输入」聚合回调（DATA realtime_control.typing_aggregate，超大群网关聚合下发）
    pub fn on_typing_aggregate<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, &TypingAggregatePacket) + Send + Sync + 'static,
    {
        let f: FnTypingAggregate = Arc::new(move |cid, e| f(cid.as_str(), &e));
        self.callback_subscription(
            &self.on_typing_aggregate,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「已读回执」回调（已读光标：会话 id + ReadReceiptEvent，对端已读到 read_seq）
    pub fn on_read_receipt<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, &ReadReceiptEvent) + Send + Sync + 'static,
    {
        let f: FnReadReceipt = Arc::new(move |cid, e| f(cid.as_str(), &e));
        self.callback_subscription(
            &self.on_read_receipt,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// Plugin realtime packets (`im.realtime.deliver`); expired packets and
    /// non-object payloads never arrive.
    pub fn on_plugin_realtime<F>(&self, f: F) -> Subscription
    where
        F: Fn(&PluginRealtime) + Send + Sync + 'static,
    {
        self.on_event_type(
            SdkEventType::Plugin(PluginEventType::Realtime),
            move |event| {
                if let SdkEvent::Plugin(PluginEvent::Realtime(realtime)) = event.as_ref() {
                    f(realtime);
                }
            },
        )
    }

    /// The tenant's plugin catalog changed; the argument is the announced
    /// version. The SDK has already invalidated its catalog.
    pub fn on_plugin_catalog_changed<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.on_event_type(
            SdkEventType::Plugin(PluginEventType::CatalogChanged),
            move |event| {
                if let SdkEvent::Plugin(PluginEvent::CatalogChanged { version }) = event.as_ref() {
                    f(version);
                }
            },
        )
    }

    // ---------- Conversation ----------
    /// 注册「会话列表同步完成」回调
    pub fn on_conversation_synced<F>(&self, f: F) -> Subscription
    where
        F: Fn(&[String]) + Send + Sync + 'static,
    {
        let f: FnConversationIds = Arc::new(move |ids| f(ids.as_slice()));
        self.callback_subscription(
            &self.on_conversation_synced,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「新会话」回调
    pub fn on_conversation_created<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let f: FnConversationId = Arc::new(move |cid| f(cid.as_str()));
        self.callback_subscription(
            &self.on_conversation_created,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「会话更新」回调
    pub fn on_conversation_updated<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let f: FnConversationId = Arc::new(move |cid| f(cid.as_str()));
        self.callback_subscription(
            &self.on_conversation_updated,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「会话未读数变化」回调
    pub fn on_conversation_unread_count_changed<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, u32) + Send + Sync + 'static,
    {
        let f: FnConversationUnreadCountChanged =
            Arc::new(move |cid: String, cnt: u32| f(cid.as_str(), cnt));
        self.callback_subscription(
            &self.on_conversation_unread_count_changed,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「会话删除」回调
    pub fn on_conversation_deleted<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let f: FnConversationId = Arc::new(move |cid| f(cid.as_str()));
        self.callback_subscription(
            &self.on_conversation_deleted,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    // ---------- Extension ----------
    /// 注册「扩展事件」回调
    pub fn on_extension<F>(&self, f: F) -> Subscription
    where
        F: Fn(&str, &str, &[u8]) + Send + Sync + 'static,
    {
        let f: FnExtension = Arc::new(move |s, t, p| f(s.as_str(), t.as_str(), &p));
        self.callback_subscription(&self.on_extension, &self.typed_callback_count, f, same_arc)
    }

    /// 注册某个扩展源 + 扩展事件类型的回调。
    pub fn on_extension_event<F>(
        &self,
        source: impl Into<String>,
        event_type: impl Into<String>,
        f: F,
    ) -> Subscription
    where
        F: Fn(&str, &str, &[u8]) + Send + Sync + 'static,
    {
        let event_type = SdkEventType::Extension(ExtensionEventType::named(source, event_type));
        self.on_event_type(event_type, move |event| {
            if let SdkEvent::Extension(extension) = event.as_ref() {
                f(
                    extension.source.as_str(),
                    extension.event_type.as_str(),
                    extension.payload.as_slice(),
                );
            }
        })
    }

    /// 注册 IM 下行 Notification 回调（与聊天 `on_message` 分离）。
    pub fn on_notification<F>(&self, f: F) -> Subscription
    where
        F: Fn(&IMMessage) + Send + Sync + 'static,
    {
        let f: FnNotification = Arc::new(move |m| f(&m));
        self.callback_subscription(
            &self.on_notification,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册某个 `NotificationContent.notification_type` 的回调。
    pub fn on_notification_type<F>(
        &self,
        notification_type: impl Into<String>,
        f: F,
    ) -> Subscription
    where
        F: Fn(&IMMessage) + Send + Sync + 'static,
    {
        let event_type =
            SdkEventType::Notification(NotificationEventType::notification_type(notification_type));
        self.on_event_type(event_type, move |event| {
            if let SdkEvent::Notification(NotificationEvent::Received { message }) = event.as_ref()
            {
                f(message.as_ref());
            }
        })
    }

    /// 注册某个 durable `CustomEvent` 的回调。
    pub fn on_custom_event<F>(&self, selector: impl Into<CustomEventSelector>, f: F) -> Subscription
    where
        F: Fn(&str, &CustomEvent) + Send + Sync + 'static,
    {
        let event_type = SdkEventType::Message(MessageEventType::CustomNamed(selector.into()));
        self.on_event_type(event_type, move |event| {
            if let SdkEvent::Message(MessageEvent::Custom {
                conversation_id,
                event,
            }) = event.as_ref()
            {
                f(conversation_id.as_str(), event);
            }
        })
    }

    // ---------- Sync ----------
    /// 注册「同步开始」回调
    pub fn on_sync_started<F>(&self, f: F) -> Subscription
    where
        F: Fn() + Send + Sync + 'static,
    {
        let f: FnConnected = Arc::new(f);
        self.callback_subscription(
            &self.on_sync_started,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「同步阶段结束」回调
    pub fn on_sync_finished<F>(&self, f: F) -> Subscription
    where
        F: Fn(SyncPhase) + Send + Sync + 'static,
    {
        let f = Arc::new(f) as FnSyncPhase;
        let invoked = Arc::new(AtomicBool::new(false));
        let callback = {
            let f = f.clone();
            let invoked = invoked.clone();
            Arc::new(move |phase| {
                invoked.store(true, Ordering::Release);
                f(phase);
            }) as FnSyncPhase
        };
        let last = self.last_sync_finished.safe_read("event_bus").clone();
        let subscription = self.callback_subscription(
            &self.on_sync_finished,
            &self.typed_callback_count,
            callback,
            same_arc,
        );
        if let Some(phase) = last {
            replay_after_dispatch_window(move || {
                if !invoked.load(Ordering::Acquire) {
                    f(phase);
                }
            });
        }
        subscription
    }

    /// 注册「同步失败」回调
    pub fn on_sync_failed<F>(&self, f: F) -> Subscription
    where
        F: Fn(String, String) + Send + Sync + 'static,
    {
        let f: FnSendFailed = Arc::new(f);
        self.callback_subscription(
            &self.on_sync_failed,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「同步进度」回调
    pub fn on_sync_progress<F>(&self, f: F) -> Subscription
    where
        F: Fn(String, f32, String) + Send + Sync + 'static,
    {
        let f: FnSyncProgress = Arc::new(f);
        self.callback_subscription(
            &self.on_sync_progress,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    /// 注册「同步单任务完成」回调
    pub fn on_sync_task_completed<F>(&self, f: F) -> Subscription
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let f: FnConversationId = Arc::new(f);
        self.callback_subscription(
            &self.on_sync_task_completed,
            &self.typed_callback_count,
            f,
            same_arc,
        )
    }

    // ---------- Raw / Any ----------
    /// 订阅原始事件流（每个订阅者独立 lossless queue）。
    pub fn subscribe_raw(&self) -> EventReceiver {
        self.subscribe()
    }

    /// 注册过滤后的事件回调。多个相同过滤器的回调会全部执行。
    pub fn on_event_filter<F>(&self, filter: impl Into<EventFilter>, f: F) -> Subscription
    where
        F: Fn(Arc<SdkEvent>) + Send + Sync + 'static,
    {
        let route = EventRoute {
            filter: filter.into(),
            callback: Arc::new(f),
        };
        self.callback_subscription(
            &self.routes,
            &self.route_callback_count,
            route,
            same_event_route,
        )
    }

    /// 注册某个顶层事件域的回调。
    pub fn on_event_kind<F>(&self, kind: SdkEventKind, f: F) -> Subscription
    where
        F: Fn(Arc<SdkEvent>) + Send + Sync + 'static,
    {
        self.on_event_filter(kind, f)
    }

    /// 注册某个精确事件类型的回调。
    pub fn on_event_type<F>(&self, event_type: SdkEventType, f: F) -> Subscription
    where
        F: Fn(Arc<SdkEvent>) + Send + Sync + 'static,
    {
        self.on_event_filter(event_type, f)
    }

    /// 注册「任意事件」回调（拿到完整 SdkEvent，用于日志、审计或未分类扩展）
    pub fn on_any<F>(&self, f: F) -> Subscription
    where
        F: Fn(Arc<SdkEvent>) + Send + Sync + 'static,
    {
        let f: FnAny = Arc::new(f);
        self.callback_subscription(&self.on_any, &self.any_callback_count, f, same_arc)
    }
}
