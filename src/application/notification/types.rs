//! IM 下行 Notification 视图与 Handler 契约。

use std::collections::HashMap;

use async_trait::async_trait;

use crate::content::message_elem::Elem;
use crate::model::IMMessage;

/// Delivery intent is explicit: batching alone cannot distinguish live catch-up from history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundDelivery {
    Live,
    CatchUp,
    HistoryReplay,
}

/// 从 [`IMMessage`] 投影的业务处理视图（无业务语义）。
#[derive(Debug, Clone)]
pub struct InboundNotificationView<'a> {
    pub message: &'a IMMessage,
    pub notification_type: &'a str,
    pub data: &'a HashMap<String, String>,
    pub persistent: bool,
    pub show_in_list: bool,
    pub show_badge: bool,
    pub play_sound: bool,
}

impl<'a> InboundNotificationView<'a> {
    pub fn from_message(message: &'a IMMessage) -> Option<Self> {
        match message.content.as_ref()? {
            Elem::Notification(n) => Some(Self {
                message,
                notification_type: n.notification_type.trim(),
                data: &n.data,
                persistent: n.persistent,
                show_in_list: n.show_in_list,
                show_badge: n.show_badge,
                play_sound: n.play_sound,
            }),
            Elem::System(system) => Some(Self {
                message,
                notification_type: system.event_kind.trim(),
                data: &system.data,
                persistent: true,
                show_in_list: true,
                show_badge: false,
                play_sound: false,
            }),
            _ => None,
        }
    }

    pub fn is_system(&self) -> bool {
        matches!(self.message.content.as_ref(), Some(Elem::System(_)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationHandleResult {
    Handled,
    Ignored,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotificationDispatchReport {
    pub matched: usize,
    pub handled: usize,
    pub ignored: usize,
}

/// 业务 SDK 实现的 IM 下行 Notification 处理器。
#[async_trait]
pub trait NotificationHandler: Send + Sync {
    fn matches(&self, notification_type: &str) -> bool;

    /// Business extensions may project their canonical type from typed carrier metadata.
    /// The Core SDK does not interpret any product-specific metadata key.
    fn matches_view(&self, notification: &InboundNotificationView<'_>) -> bool {
        self.matches(notification.notification_type)
    }

    async fn handle(&self, notification: &InboundNotificationView<'_>) -> NotificationHandleResult;
}

pub(crate) fn should_publish_notification_as_message(view: &InboundNotificationView<'_>) -> bool {
    view.persistent || view.show_in_list
}
