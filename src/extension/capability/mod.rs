//! SDK 侧 **扩展能力** 框架。
//!
//! ## 调用方式
//!
//! 1. **直连 gRPC**（不经过注册表）：[`crate::client::api::CapabilityApi`] — `dispatch`。
//! 2. **经插件注册表**（与 `SdkCapabilityPlugin` 对齐）：[`SdkCapabilityRegistry::invoke`] 或 [`crate::client::IMClient::invoke_capability`]。
//!
//! 插件下行实时包与目录变更通知以类型化事件 [`crate::kernel::event::PluginEvent`] 交给宿主。

/// L3 web bundles: cache, bridge broker, storage and events.
pub mod bundle;
mod manifest;
mod plugin;
/// Localized keys of plugin call failures.
pub mod plugin_errors;
mod registry;
/// Shared safe data-only UI evaluation; every binding uses the same contract.
pub mod ui;
/// Plugin pages: request parsing, source paging and page actions.
pub(crate) mod ui_page;
pub use flare_plugin_protocol::ui as ui_contract;

pub use manifest::{
    SdkPluginEventManifest, SdkPluginManifest, SdkPluginOperationManifest,
    SdkPluginPermissionManifest, SdkPluginUiKitManifest,
};
pub use plugin::SdkCapabilityPlugin;
pub use registry::SdkCapabilityRegistry;
