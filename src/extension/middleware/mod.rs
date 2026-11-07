//! Core middleware pipeline.
//!
//! Middleware is a platform-neutral extension seam for business SDKs and
//! plugins. Platform differences still belong to `platform::ports` and
//! adapters; middleware only sees stable core contracts such as `IMMessage`,
//! `SdkEvent`, and `FlareError`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;
use tracing::warn;

use crate::kernel::event::SdkEvent;
use crate::model::message::{IMMessage, SendAck};
use crate::shared::error::{FlareError, Result};

/// Outbound message operation currently passing through middleware.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageOperation {
    /// Direct network send path, used when reliable queue storage is not
    /// configured.
    DirectSend,
    /// Reliable queue enqueue path. Network delivery and ack are completed by
    /// the queue worker later.
    ReliableQueueEnqueue,
}

/// Context passed to message interceptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageMiddlewareContext {
    pub operation: MessageOperation,
}

impl MessageMiddlewareContext {
    pub const fn new(operation: MessageOperation) -> Self {
        Self { operation }
    }
}

/// Result of event pre-publish middleware.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventMiddlewareAction {
    /// Continue publishing to the SDK event bus.
    Continue,
    /// Drop the event before it reaches public subscribers.
    Drop,
}

impl EventMiddlewareAction {
    pub const fn is_drop(self) -> bool {
        matches!(self, Self::Drop)
    }
}

/// Message interceptor for outbound send pipeline.
///
/// `before_send` is allowed to mutate or reject the message. It is the right
/// place for core-neutral work such as encryption, compliance tagging,
/// feature-flag enrichment, or business extension validation.
///
/// `after_send` and `on_send_error` are observers. Their failures must not
/// rewrite delivery semantics, so they do not return `Result`.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MessageInterceptor: Send + Sync {
    async fn before_send(
        &self,
        _message: &mut IMMessage,
        _ctx: &MessageMiddlewareContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn after_send(
        &self,
        _message: &IMMessage,
        _ack: Option<&SendAck>,
        _ctx: &MessageMiddlewareContext,
    ) {
    }

    async fn on_send_error(
        &self,
        _message: &IMMessage,
        _error: &FlareError,
        _ctx: &MessageMiddlewareContext,
    ) {
    }
}

/// Event interceptor for public SDK events.
///
/// Event middleware is synchronous because `EventBus::publish` is intentionally
/// lightweight and callable from sync paths. Expensive work should be moved to
/// callbacks or extension tasks.
pub trait EventInterceptor: Send + Sync {
    fn before_publish(&self, _event: &mut SdkEvent) -> Result<EventMiddlewareAction> {
        Ok(EventMiddlewareAction::Continue)
    }

    fn on_publish(&self, _event: &SdkEvent) {}

    fn on_publish_error(&self, _event: &SdkEvent, _error: &FlareError) {}
}

/// 拦截器链（可扩展）
pub struct MiddlewareChain {
    message: Vec<Arc<dyn MessageInterceptor>>,
    event: Vec<Arc<dyn EventInterceptor>>,
}

impl MiddlewareChain {
    pub fn new() -> Self {
        Self {
            message: Vec::new(),
            event: Vec::new(),
        }
    }

    pub fn add_message_interceptor(&mut self, interceptor: Arc<dyn MessageInterceptor>) {
        self.message.push(interceptor);
    }

    pub fn add_event_interceptor(&mut self, interceptor: Arc<dyn EventInterceptor>) {
        self.event.push(interceptor);
    }

    pub fn is_empty(&self) -> bool {
        self.message.is_empty() && self.event.is_empty()
    }

    pub fn has_message_interceptors(&self) -> bool {
        !self.message.is_empty()
    }

    pub fn message_interceptor_count(&self) -> usize {
        self.message.len()
    }

    pub fn event_interceptor_count(&self) -> usize {
        self.event.len()
    }

    pub async fn before_send(
        &self,
        message: &mut IMMessage,
        ctx: &MessageMiddlewareContext,
    ) -> Result<()> {
        for interceptor in &self.message {
            match AssertUnwindSafe(interceptor.before_send(message, ctx))
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(FlareError::general_error(
                        "message middleware panicked before send",
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn after_send(
        &self,
        message: &IMMessage,
        ack: Option<&SendAck>,
        ctx: &MessageMiddlewareContext,
    ) {
        for interceptor in &self.message {
            if AssertUnwindSafe(interceptor.after_send(message, ack, ctx))
                .catch_unwind()
                .await
                .is_err()
            {
                warn!("message middleware panicked after send; continuing");
            }
        }
    }

    pub async fn notify_send_error(
        &self,
        message: &IMMessage,
        error: &FlareError,
        ctx: &MessageMiddlewareContext,
    ) {
        for interceptor in &self.message {
            if AssertUnwindSafe(interceptor.on_send_error(message, error, ctx))
                .catch_unwind()
                .await
                .is_err()
            {
                warn!("message middleware panicked while handling send error");
            }
        }
    }

    pub fn before_publish(&self, event: &mut SdkEvent) -> EventMiddlewareAction {
        for interceptor in &self.event {
            let action = catch_unwind(AssertUnwindSafe(|| interceptor.before_publish(event)));
            match action {
                Ok(Ok(EventMiddlewareAction::Drop)) => return EventMiddlewareAction::Drop,
                Ok(Ok(EventMiddlewareAction::Continue)) => {}
                Ok(Err(error)) => {
                    warn!(
                        error = %error,
                        "event middleware failed before publish; continuing"
                    );
                    self.notify_publish_error(event, &error);
                }
                Err(_) => {
                    warn!("event middleware panicked before publish; continuing");
                }
            }
        }
        EventMiddlewareAction::Continue
    }

    pub fn on_publish(&self, event: &SdkEvent) {
        for interceptor in &self.event {
            if catch_unwind(AssertUnwindSafe(|| interceptor.on_publish(event))).is_err() {
                warn!("event middleware panicked while observing publish; continuing");
            }
        }
    }

    pub fn notify_publish_error(&self, event: &SdkEvent, error: &FlareError) {
        for interceptor in &self.event {
            if catch_unwind(AssertUnwindSafe(|| {
                interceptor.on_publish_error(event, error)
            }))
            .is_err()
            {
                warn!("event middleware panicked while handling publish error");
            }
        }
    }
}

impl Default for MiddlewareChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use flare_proto::common::Message as ProtoMessage;

    use crate::kernel::event::{ConnectionEvent, EventBus};

    struct AppendTag(&'static str);

    #[async_trait]
    impl MessageInterceptor for AppendTag {
        async fn before_send(
            &self,
            message: &mut IMMessage,
            _ctx: &MessageMiddlewareContext,
        ) -> Result<()> {
            message
                .attributes
                .entry("middlewareTags".to_string())
                .or_default()
                .push_str(self.0);
            Ok(())
        }
    }

    struct DropConnectionEvents;

    impl EventInterceptor for DropConnectionEvents {
        fn before_publish(&self, event: &mut SdkEvent) -> Result<EventMiddlewareAction> {
            if matches!(event, SdkEvent::Connection(_)) {
                return Ok(EventMiddlewareAction::Drop);
            }
            Ok(EventMiddlewareAction::Continue)
        }
    }

    fn test_message() -> IMMessage {
        let proto = ProtoMessage {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            sender_id: "user-1".to_string(),
            ..Default::default()
        };
        IMMessage::new(proto)
    }

    #[tokio::test]
    async fn message_interceptors_run_in_registration_order() {
        let mut chain = MiddlewareChain::new();
        chain.add_message_interceptor(Arc::new(AppendTag("a")));
        chain.add_message_interceptor(Arc::new(AppendTag("b")));

        let ctx = MessageMiddlewareContext::new(MessageOperation::DirectSend);
        let mut message = test_message();

        chain.before_send(&mut message, &ctx).await.unwrap();

        assert_eq!(
            message.attributes.get("middlewareTags").map(String::as_str),
            Some("ab")
        );
    }

    #[tokio::test]
    async fn event_interceptor_can_drop_event_before_broadcast() {
        let mut chain = MiddlewareChain::new();
        chain.add_event_interceptor(Arc::new(DropConnectionEvents));
        let bus = EventBus::with_middleware(Arc::new(chain));
        let mut rx = bus.subscribe();

        bus.publish(SdkEvent::Connection(ConnectionEvent::Status(
            crate::kernel::event::ConnectionStatus::idle(false),
        )));

        let received = tokio::time::timeout(std::time::Duration::from_millis(30), rx.recv()).await;
        assert!(received.is_err());
    }
}
