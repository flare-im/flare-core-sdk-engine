//! 消息与会话 **Facade**：对应用层引擎与构建器的稳定调用面。
//!
//! 由 [`crate::client::IMClient`] 在连接后持有；事件订阅为 `IMClient::on_*`（见 [`crate::client::events`]，委托 [`crate::kernel::event::EventBus`]）。

mod capability;
mod conversation;
mod media;
mod message;
mod message_build;
mod page_command;
mod plugin_command;
// Presence goes through the Core API gateway on every target: the Online
// service is internal and only accepts gateway-signed context.
#[path = "presence/http.rs"]
mod presence;
#[path = "presence/wire.rs"]
mod presence_wire;
pub(crate) mod session_guard;
mod view;

pub use crate::extension::capability::ui::PluginComposerAction;
pub(crate) use capability::TenantNotices;
pub use capability::{
    CapabilityApi, CapabilityDescriptorDto, CapabilityDispatchResult, PluginFileUploadPage,
    PluginFileUploadRequest,
};
pub use conversation::ConversationApi;
pub use media::{
    FileDownloadProgress, FileDownloadProgressCallback, MediaApi, MessageMediaReference,
    UploadPhase, UploadProgress, UploadProgressCallback,
};
pub use message::{EditRichDocRequest, MessageApi};
pub use message_build::{
    CreateLocationRequest, CreateRichDocRequest, CreateStickerRequest, MessageBuildApi,
};
pub use presence::{ConnectionQualityDto, DevicePresenceDto, PresenceApi, UserPresenceDto};
pub use view::ViewApi;
