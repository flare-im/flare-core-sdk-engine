//! Notification handler registry with fan-out dispatch.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::warn;

use super::types::{
    InboundNotificationView, NotificationDispatchReport, NotificationHandleResult,
    NotificationHandler,
};

pub struct NotificationHandlerRegistry {
    handlers: RwLock<Vec<Arc<dyn NotificationHandler>>>,
}

impl NotificationHandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(Vec::new()),
        }
    }

    pub fn with_handlers(handlers: Vec<Arc<dyn NotificationHandler>>) -> Self {
        Self {
            handlers: RwLock::new(handlers),
        }
    }

    pub async fn register(&self, handler: Arc<dyn NotificationHandler>) {
        self.handlers.write().await.push(handler);
    }

    pub async fn register_for_type(
        &self,
        notification_type: impl Into<String>,
        handler: Arc<dyn NotificationHandler>,
    ) {
        self.register(Arc::new(ExactNotificationHandler {
            notification_type: notification_type.into().trim().to_string(),
            inner: handler,
        }))
        .await;
    }

    pub async fn is_empty(&self) -> bool {
        self.handlers.read().await.is_empty()
    }

    pub async fn dispatch(&self, notification: &InboundNotificationView<'_>) {
        let _ = self.dispatch_all(notification).await;
    }

    pub async fn dispatch_all(
        &self,
        notification: &InboundNotificationView<'_>,
    ) -> NotificationDispatchReport {
        let handlers = self.handlers.read().await.clone();
        let mut report = NotificationDispatchReport::default();

        for handler in handlers.iter() {
            if !handler.matches_view(notification) {
                continue;
            }

            report.matched += 1;
            match handler.handle(notification).await {
                NotificationHandleResult::Handled => report.handled += 1,
                NotificationHandleResult::Ignored => report.ignored += 1,
            }
        }

        if report.matched == 0 && !notification.is_system() {
            warn!(
                notification_type = %notification.notification_type,
                server_id = %notification.message.server_id,
                "notification: no handler registered"
            );
        }

        report
    }
}

struct ExactNotificationHandler {
    notification_type: String,
    inner: Arc<dyn NotificationHandler>,
}

#[async_trait]
impl NotificationHandler for ExactNotificationHandler {
    fn matches(&self, notification_type: &str) -> bool {
        self.notification_type == notification_type.trim()
    }

    async fn handle(&self, notification: &InboundNotificationView<'_>) -> NotificationHandleResult {
        self.inner.handle(notification).await
    }
}

impl Default for NotificationHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::content::message_elem::{Elem, NotificationElem};
    use crate::model::IMMessage;

    struct CountingHandler {
        expected_type: &'static str,
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl NotificationHandler for CountingHandler {
        fn matches(&self, notification_type: &str) -> bool {
            notification_type == self.expected_type
        }

        async fn handle(
            &self,
            _notification: &InboundNotificationView<'_>,
        ) -> NotificationHandleResult {
            self.count.fetch_add(1, Ordering::SeqCst);
            NotificationHandleResult::Handled
        }
    }

    struct AlwaysHandler {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl NotificationHandler for AlwaysHandler {
        fn matches(&self, _notification_type: &str) -> bool {
            true
        }

        async fn handle(
            &self,
            _notification: &InboundNotificationView<'_>,
        ) -> NotificationHandleResult {
            self.count.fetch_add(1, Ordering::SeqCst);
            NotificationHandleResult::Handled
        }
    }

    fn notification_message(notification_type: &str) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "srv-1".into();
        message.content = Some(Elem::Notification(NotificationElem {
            title: String::new(),
            body: String::new(),
            notification_type: notification_type.into(),
            data: Default::default(),
            target_user_ids: Vec::new(),
            target_role_id: String::new(),
            notify_all: false,
            persistent: false,
            show_in_list: false,
            show_badge: false,
            play_sound: false,
        }));
        message
    }

    #[tokio::test]
    async fn dispatch_invokes_all_matching_handlers() {
        let registry = NotificationHandlerRegistry::new();
        let count = Arc::new(AtomicUsize::new(0));
        registry
            .register(Arc::new(CountingHandler {
                expected_type: "core.custom",
                count: count.clone(),
            }))
            .await;
        registry
            .register(Arc::new(CountingHandler {
                expected_type: "core.custom",
                count: count.clone(),
            }))
            .await;

        let message = notification_message("core.custom");
        let view = InboundNotificationView::from_message(&message).expect("notification view");
        let report = registry.dispatch_all(&view).await;

        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(report.matched, 2);
        assert_eq!(report.handled, 2);
    }

    #[tokio::test]
    async fn register_for_type_routes_exact_notification_type() {
        let registry = NotificationHandlerRegistry::new();
        let count = Arc::new(AtomicUsize::new(0));
        registry
            .register_for_type(
                "core.target",
                Arc::new(AlwaysHandler {
                    count: count.clone(),
                }),
            )
            .await;

        let ignored_message = notification_message("core.other");
        let ignored =
            InboundNotificationView::from_message(&ignored_message).expect("notification view");
        let ignored_report = registry.dispatch_all(&ignored).await;
        assert_eq!(ignored_report.matched, 0);

        let matched_message = notification_message("core.target");
        let matched =
            InboundNotificationView::from_message(&matched_message).expect("notification view");
        let matched_report = registry.dispatch_all(&matched).await;

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(matched_report.matched, 1);
        assert_eq!(matched_report.handled, 1);
    }
}
