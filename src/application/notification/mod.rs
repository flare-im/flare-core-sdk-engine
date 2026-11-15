//! IM 下行 Notification 基础设施：Handler 注册 + Push/Sync 统一投递，以及链路在线时的提示决策
//! （NotifyCenter）。

mod alert_ledger;
mod center;
mod inbound;
mod local_time;
mod registry;
mod render;
mod types;

#[cfg(test)]
pub(crate) use alert_ledger::conversation_collapse_id;
pub(crate) use alert_ledger::{AlertLedger, AlertSkip};
pub(crate) use center::{
    AlertReach, AlertSurface, NotificationHost, NotifyCenter, NotifyCenterParts,
    USER_SETTINGS_CHANGED_TYPE,
};
pub use inbound::{NotificationInboundPipeline, partition_notification_durability};
pub use registry::NotificationHandlerRegistry;
pub use types::{
    InboundDelivery, InboundNotificationView, NotificationDispatchReport, NotificationHandleResult,
    NotificationHandler,
};
