//! Push / Sync 统一的 Notification 入站管道。

use std::sync::Arc;

use crate::application::services::MessageDeduper;
use crate::content::message_elem::Elem;
use crate::kernel::event::{EventBus, MessageEvent, NotificationEvent, SdkEvent};
use crate::model::IMMessage;

use super::center::NotifyCenter;
use super::registry::NotificationHandlerRegistry;
use super::types::{
    InboundDelivery, InboundNotificationView, should_publish_notification_as_message,
};

/// `NotificationContent.persistent=false`：触达但不落本地库、不参与会话摘要投影。
pub fn partition_notification_durability(
    messages: Vec<IMMessage>,
) -> (Vec<IMMessage>, Vec<IMMessage>) {
    let mut durable = Vec::new();
    let mut ephemeral = Vec::new();
    for message in messages {
        if matches!(
            message.content.as_ref(),
            Some(Elem::Notification(n)) if !n.persistent
        ) {
            ephemeral.push(message);
        } else {
            durable.push(message);
        }
    }
    (durable, ephemeral)
}

/// 去重、业务分发之后的一条下行。
enum Prepared {
    /// 进时间线（通知类内容也可能同时进）。
    Timeline(IMMessage),
    /// 只作为通知送达，不进时间线。
    NoticeOnly(IMMessage),
}

#[derive(Clone)]
pub struct NotificationInboundPipeline {
    registry: Arc<NotificationHandlerRegistry>,
    message_deduper: MessageDeduper,
    bus: EventBus,
    /// 链路在线时的提示决策点。引擎为各条连接的分发器装上；同步与单测的管道没有它，不提示。
    notify: Option<Arc<NotifyCenter>>,
}

impl NotificationInboundPipeline {
    pub(crate) fn new(
        registry: Arc<NotificationHandlerRegistry>,
        message_deduper: MessageDeduper,
        bus: EventBus,
    ) -> Self {
        Self {
            registry,
            message_deduper,
            bus,
            notify: None,
        }
    }

    pub(crate) fn with_notify_center(mut self, notify: Arc<NotifyCenter>) -> Self {
        self.notify = Some(notify);
        self
    }

    /// 服务端经 `sync:<用户>` 收件箱发来「用户设置变了」：交给提示决策点去重新读。
    pub(crate) fn user_settings_changed(&self, payload: &[u8]) {
        if let Some(notify) = &self.notify {
            notify.on_user_settings_changed(payload);
        }
    }

    pub fn registry(&self) -> &Arc<NotificationHandlerRegistry> {
        &self.registry
    }

    pub async fn finish_one(&self, message: IMMessage) {
        let Some(Prepared::Timeline(message)) = self
            .prepare_received_message(message, InboundDelivery::Live)
            .await
        else {
            return;
        };
        self.publish_received_message(message);
    }

    async fn prepare_received_message(
        &self,
        message: IMMessage,
        delivery: InboundDelivery,
    ) -> Option<Prepared> {
        if !self.message_deduper.record_if_new(&message).await {
            return None;
        }

        if let Some(view) = InboundNotificationView::from_message(&message) {
            if delivery != InboundDelivery::HistoryReplay && !self.registry.is_empty().await {
                self.registry.dispatch(&view).await;
            }
            if view.is_system() {
                // One System carrier drives business handling and normal timeline display.
                // Publishing a NotificationEvent too would produce a second visual reminder.
                return Some(Prepared::Timeline(message));
            }
            let should_publish_as_message = should_publish_notification_as_message(&view);
            if delivery == InboundDelivery::Live {
                self.bus
                    .publish(SdkEvent::Notification(NotificationEvent::Received {
                        message: Box::new(message.clone()),
                    }));
            }
            return Some(if should_publish_as_message {
                Prepared::Timeline(message)
            } else {
                Prepared::NoticeOnly(message)
            });
        }

        Some(Prepared::Timeline(message))
    }

    fn publish_received_message(&self, message: IMMessage) {
        self.bus.publish(SdkEvent::Message(MessageEvent::Received {
            message: Box::new(message),
        }));
    }

    pub async fn finish_batch(&self, messages: Vec<IMMessage>, current_user_id: &str) {
        self.finish_batch_inner(messages, InboundDelivery::Live, Some(current_user_id))
            .await;
    }

    pub async fn finish_batch_with_delivery(
        &self,
        messages: Vec<IMMessage>,
        delivery: InboundDelivery,
    ) {
        self.finish_batch_inner(messages, delivery, None).await;
    }

    async fn finish_batch_inner(
        &self,
        messages: Vec<IMMessage>,
        delivery: InboundDelivery,
        current_user_id: Option<&str>,
    ) {
        let mut received_messages = Vec::with_capacity(messages.len());
        let mut notices = Vec::new();
        for message in messages {
            match self.prepare_received_message(message, delivery).await {
                Some(Prepared::Timeline(message)) => received_messages.push(message),
                Some(Prepared::NoticeOnly(message)) => notices.push(message),
                None => {}
            }
        }

        // 提示候选先挑出来（只拷贝要提示的那几条），时间线不必等提示决策读库。
        let notify = current_user_id.zip(self.notify.as_ref());
        let mut candidates = Vec::new();
        if let Some((user_id, notify)) = notify {
            candidates.extend(notify.live_candidates(&received_messages, user_id));
            candidates.extend(notify.live_candidates(&notices, user_id));
        }
        if !received_messages.is_empty() {
            self.bus
                .publish(SdkEvent::Message(MessageEvent::ReceivedBatch {
                    messages: received_messages,
                }));
        }
        if let Some((user_id, notify)) = notify {
            notify.present_live(candidates, user_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::{NotificationHandleResult, NotificationHandler};
    use super::*;
    use crate::content::message_elem::{Elem, NotificationElem};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    fn plain_message(server_id: &str, seq: u64) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = server_id.into();
        message.client_msg_id = format!("cli-{seq}");
        message.conversation_id = "c1".into();
        message.sender_id = "u1".into();
        message.conversation_seq = seq;
        message
    }

    fn test_pipeline(bus: EventBus) -> NotificationInboundPipeline {
        NotificationInboundPipeline::new(
            Arc::new(NotificationHandlerRegistry::new()),
            MessageDeduper::new(Some(64)),
            bus,
        )
    }

    fn notification_message(persistent: bool) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "srv-1".into();
        message.client_msg_id = "cli-1".into();
        message.conversation_id = "c1".into();
        message.sender_id = "u1".into();
        message.conversation_seq = 1;
        message.content = Some(Elem::Notification(NotificationElem {
            title: String::new(),
            body: String::new(),
            notification_type: "social.relation.friend_request_created".into(),
            data: Default::default(),
            target_user_ids: Vec::new(),
            target_role_id: String::new(),
            notify_all: false,
            persistent,
            show_in_list: false,
            show_badge: true,
            play_sound: false,
        }));
        message
    }

    #[test]
    fn partition_splits_ephemeral_notifications() {
        let durable = notification_message(true);
        let ephemeral = notification_message(false);
        let mut plain = notification_message(true);
        plain.server_id = "plain-1".into();
        plain.content = None;

        let (durable_out, ephemeral_out) = partition_notification_durability(vec![
            durable.clone(),
            ephemeral.clone(),
            plain.clone(),
        ]);

        assert_eq!(durable_out.len(), 2);
        assert_eq!(ephemeral_out.len(), 1);
        assert_eq!(ephemeral_out[0].server_id, ephemeral.server_id);
        assert!(matches!(
            durable_out[0].content.as_ref(),
            Some(Elem::Notification(n)) if n.persistent
        ));
        assert_eq!(durable_out[1].server_id, plain.server_id);
        assert!(durable_out[1].content.is_none());
    }

    #[tokio::test]
    async fn finish_one_publishes_single_message_callback() {
        let bus = EventBus::new();
        let pipeline = test_pipeline(bus.clone());
        let (single_tx, mut single_rx) = mpsc::unbounded_channel();

        let _single_sub = bus.on_message(move |message| {
            let _ = single_tx.send(message.server_id.clone());
        });

        pipeline.finish_one(plain_message("srv-1", 1)).await;

        let single_id = timeout(Duration::from_millis(200), single_rx.recv())
            .await
            .expect("expected single callback")
            .expect("single callback channel closed");
        assert_eq!(single_id, "srv-1");
    }

    #[tokio::test]
    async fn finish_batch_publishes_batch_without_single_message_replay() {
        let bus = EventBus::new();
        let pipeline = test_pipeline(bus.clone());
        let (batch_tx, mut batch_rx) = mpsc::unbounded_channel();
        let (single_tx, mut single_rx) = mpsc::unbounded_channel();

        let _batch_sub = bus.on_message_batch(move |messages| {
            let _ = batch_tx.send(messages.len());
        });

        let _single_sub = bus.on_message(move |message| {
            let _ = single_tx.send(message.server_id.clone());
        });

        pipeline
            .finish_batch(
                vec![plain_message("srv-1", 1), plain_message("srv-2", 2)],
                "me",
            )
            .await;

        let batch_size = timeout(Duration::from_millis(200), batch_rx.recv())
            .await
            .expect("expected batch callback")
            .expect("batch callback channel closed");
        assert_eq!(batch_size, 2);

        let single = timeout(Duration::from_millis(80), single_rx.recv()).await;
        assert!(
            single.is_err(),
            "batch inbound is canonical and should not replay per-message callbacks"
        );
    }

    struct BusinessHandler {
        handled: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl NotificationHandler for BusinessHandler {
        fn matches(&self, notification_type: &str) -> bool {
            notification_type == "business.change"
        }
        fn matches_view(&self, view: &InboundNotificationView<'_>) -> bool {
            self.matches(view.notification_type)
                || view
                    .data
                    .get("business.kind")
                    .is_some_and(|kind| self.matches(kind))
        }
        async fn handle(&self, _: &InboundNotificationView<'_>) -> NotificationHandleResult {
            self.handled.fetch_add(1, Ordering::SeqCst);
            NotificationHandleResult::Handled
        }
    }

    fn business_pipeline(bus: EventBus, handled: Arc<AtomicUsize>) -> NotificationInboundPipeline {
        NotificationInboundPipeline::new(
            Arc::new(NotificationHandlerRegistry::with_handlers(vec![Arc::new(
                BusinessHandler { handled },
            )])),
            MessageDeduper::new(Some(64)),
            bus,
        )
    }

    fn system_message(id: &str) -> IMMessage {
        let mut message = plain_message(id, 1);
        message.content = Some(Elem::System(crate::content::message_elem::SystemElem {
            event_kind: "product.short_kind".into(),
            body: "visible timeline".into(),
            data: std::collections::HashMap::from([(
                "business.kind".into(),
                "business.change".into(),
            )]),
            payload: Vec::new(),
        }));
        message
    }

    #[tokio::test]
    async fn system_dispatches_business_before_single_timeline_display_without_notification_popup()
    {
        let bus = EventBus::new();
        let handled = Arc::new(AtomicUsize::new(0));
        let pipeline = business_pipeline(bus.clone(), handled.clone());
        let mut events = bus.subscribe();
        let order = handled.clone();
        let _display = bus.on_message(move |_| assert_eq!(order.load(Ordering::SeqCst), 1));
        pipeline.finish_one(system_message("live-system")).await;
        assert_eq!(handled.load(Ordering::SeqCst), 1);
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::Received { .. })
        ));
        assert!(
            events.try_recv().is_err(),
            "System must not also produce a Notification display event"
        );
    }

    #[tokio::test]
    async fn history_only_displays_while_catch_up_keeps_business_and_suppresses_transient_popup() {
        let bus = EventBus::new();
        let handled = Arc::new(AtomicUsize::new(0));
        let pipeline = business_pipeline(bus.clone(), handled.clone());
        let mut events = bus.subscribe();
        pipeline
            .finish_batch_with_delivery(
                vec![system_message("history")],
                InboundDelivery::HistoryReplay,
            )
            .await;
        assert_eq!(handled.load(Ordering::SeqCst), 0);
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        let mut notification = notification_message(false);
        notification.server_id = "catch-up".into();
        if let Some(Elem::Notification(elem)) = notification.content.as_mut() {
            elem.notification_type = "business.change".into();
        }
        pipeline
            .finish_batch_with_delivery(vec![notification], InboundDelivery::CatchUp)
            .await;
        assert_eq!(
            handled.load(Ordering::SeqCst),
            1,
            "catch-up is not history and must keep missing business facts"
        );
        assert!(
            events.try_recv().is_err(),
            "catch-up must not raise an old instant notification"
        );
        pipeline
            .finish_batch_with_delivery(
                vec![system_message("catch-up-system")],
                InboundDelivery::CatchUp,
            )
            .await;
        assert_eq!(handled.load(Ordering::SeqCst), 2);
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn combined_transient_notification_is_one_carrier_one_business_callback_one_notification_display()
     {
        let bus = EventBus::new();
        let handled = Arc::new(AtomicUsize::new(0));
        let pipeline = business_pipeline(bus.clone(), handled.clone());
        let mut events = bus.subscribe();
        let mut notification = notification_message(false);
        if let Some(Elem::Notification(elem)) = notification.content.as_mut() {
            elem.notification_type = "business.change".into();
            elem.title = "Visible reminder".into();
            elem.body = "Same carrier".into();
        }
        pipeline.finish_one(notification.clone()).await;
        pipeline.finish_one(notification).await;
        assert_eq!(handled.load(Ordering::SeqCst), 1);
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Notification(_)
        ));
        assert!(
            events.try_recv().is_err(),
            "ephemeral combined carrier must not also become a timeline row"
        );
    }
}
