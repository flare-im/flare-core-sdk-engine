//! Push / Sync 统一的 Notification 入站管道。

use std::sync::Arc;

use crate::application::services::MessageDeduper;
use crate::content::message_elem::Elem;
use crate::domain::ConversationStore;
use crate::kernel::event::{EventBus, MessageEvent, NotificationEvent, SdkEvent};
use crate::model::IMMessage;

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

#[derive(Clone)]
pub struct NotificationInboundPipeline {
    registry: Arc<NotificationHandlerRegistry>,
    message_deduper: MessageDeduper,
    bus: EventBus,
    conversations: Option<Arc<dyn ConversationStore>>,
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
            conversations: None,
        }
    }

    pub(crate) fn with_conversations(mut self, conversations: Arc<dyn ConversationStore>) -> Self {
        self.conversations = Some(conversations);
        self
    }

    pub fn registry(&self) -> &Arc<NotificationHandlerRegistry> {
        &self.registry
    }

    pub async fn finish_one(&self, message: IMMessage) {
        let Some(message) = self
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
    ) -> Option<IMMessage> {
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
                return Some(message);
            }
            let should_publish_as_message = should_publish_notification_as_message(&view);
            if delivery == InboundDelivery::Live {
                self.bus
                    .publish(SdkEvent::Notification(NotificationEvent::Received {
                        message: Box::new(message.clone()),
                    }));
            }
            return should_publish_as_message.then_some(message);
        }

        Some(message)
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
        for message in messages {
            if let Some(message) = self.prepare_received_message(message, delivery).await {
                received_messages.push(message);
            }
        }

        if received_messages.is_empty() {
            return;
        }

        let sound = current_user_id
            .and_then(|user_id| self.conversations.as_ref().map(|store| (user_id, store)));
        // Let the timeline consume the normal event without waiting on sound-policy reads.
        let candidates = sound.map(|(user_id, _)| {
            received_messages
                .iter()
                .filter(|message| sound_candidate(message, user_id))
                .map(|message| {
                    let mentioned =
                        message.mention_all || message.mention_users.iter().any(|id| id == user_id);
                    (
                        message.conversation_id.clone(),
                        message.server_id.clone(),
                        mentioned,
                    )
                })
                .collect::<Vec<_>>()
        });
        self.bus
            .publish(SdkEvent::Message(MessageEvent::ReceivedBatch {
                messages: received_messages,
            }));
        if let (Some((_, store)), Some(candidates)) = (sound, candidates) {
            let mut preferences = std::collections::HashMap::new();
            for (conversation_id, message_id, mentioned) in candidates {
                let preference = match preferences.entry(conversation_id.clone()) {
                    std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let preference = match store.get(&conversation_id).await {
                            Ok(row) => Some(row.map_or((false, false), |row| {
                                (row.is_muted, row.is_mention_only.unwrap_or(false))
                            })),
                            Err(error) => {
                                tracing::warn!(%error, %conversation_id, "read message sound preference failed");
                                None
                            }
                        };
                        *entry.insert(preference)
                    }
                };
                if preference
                    .is_some_and(|(muted, mention_only)| !muted && (!mention_only || mentioned))
                {
                    self.bus
                        .publish(SdkEvent::Notification(NotificationEvent::MessageSound {
                            conversation_id,
                            message_id,
                        }));
                    break;
                }
            }
        }
    }
}

fn sound_candidate(message: &IMMessage, user_id: &str) -> bool {
    !user_id.is_empty()
        && !message.sender_id.is_empty()
        && message.sender_id != user_id
        && !message.conversation_id.is_empty()
        && !message.server_id.is_empty()
        && !message.is_recalled
        && message.status != flare_proto::common::MessageStatus::Deleted as i32
        && !matches!(
            message.content.as_ref(),
            None | Some(Elem::System(_) | Elem::Notification(_))
        )
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
    async fn live_message_sound_respects_preferences_without_hiding_messages() {
        use crate::content::message_elem::TextElem;
        use crate::infrastructure::persistence::memory_im::in_memory_im_provider;
        use crate::model::Conversation;

        let bus = EventBus::new();
        let mut events = bus.subscribe();
        let stores = in_memory_im_provider();
        let pipeline = test_pipeline(bus).with_conversations(stores.conversations.clone());
        let message = |seq: u64, mentioned: bool| {
            let mut message = plain_message(&format!("sound-{seq}"), seq);
            message.content = Some(Elem::Text(TextElem {
                text: "hello".into(),
                mentions: vec![],
            }));
            if mentioned {
                message.mention_users.push("me".into());
            }
            message
        };
        let settings = |muted, mention_only| Conversation {
            conversation_id: "c1".into(),
            is_muted: muted,
            is_mention_only: Some(mention_only),
            ..Default::default()
        };

        stores
            .conversations
            .save_one(&settings(true, false))
            .await
            .unwrap();
        pipeline.finish_batch(vec![message(1, true)], "me").await;
        assert!(
            matches!(events.try_recv().unwrap(), SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) if messages.len() == 1)
        );
        assert!(events.try_recv().is_err());

        stores
            .conversations
            .save_one(&settings(false, true))
            .await
            .unwrap();
        pipeline.finish_batch(vec![message(2, false)], "me").await;
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        assert!(events.try_recv().is_err());
        pipeline
            .finish_batch(
                vec![message(3, false), message(4, true), message(5, true)],
                "me",
            )
            .await;
        assert!(
            matches!(events.try_recv().unwrap(), SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) if messages.len() == 3)
        );
        assert!(
            matches!(events.try_recv().unwrap(), SdkEvent::Notification(NotificationEvent::MessageSound { message_id, .. }) if message_id == "sound-4")
        );
        assert!(events.try_recv().is_err());

        stores
            .conversations
            .save_one(&settings(false, false))
            .await
            .unwrap();
        pipeline
            .finish_batch_with_delivery(vec![message(6, false)], InboundDelivery::CatchUp)
            .await;
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        assert!(events.try_recv().is_err());
        pipeline.finish_batch(vec![message(6, false)], "me").await;
        assert!(events.try_recv().is_err());
        pipeline.finish_batch(vec![message(7, false)], "u1").await;
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        assert!(events.try_recv().is_err());
        pipeline.finish_batch(vec![message(8, false)], "me").await;
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        assert!(matches!(
            events.try_recv().unwrap(),
            SdkEvent::Notification(NotificationEvent::MessageSound { .. })
        ));
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
