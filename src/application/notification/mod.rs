//! IM 下行 Notification 基础设施：Handler 注册 + Push/Sync 统一投递。

mod inbound;
mod registry;
mod types;

pub use inbound::{NotificationInboundPipeline, partition_notification_durability};
pub use registry::NotificationHandlerRegistry;
pub use types::{
    InboundDelivery, InboundNotificationView, NotificationDispatchReport, NotificationHandleResult,
    NotificationHandler,
};
