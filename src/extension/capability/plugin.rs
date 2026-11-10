//! [`SdkCapabilityPlugin`]：扩展能力在 SDK 内的统一调用面（由独立插件 crate 约定 ID / payload）。

use async_trait::async_trait;
use serde_json::Value;

use crate::client::api::CapabilityDispatchResult;
use crate::shared::error::Result;

use super::manifest::SdkPluginManifest;

#[async_trait]
pub trait SdkCapabilityPlugin: Send + Sync {
    fn plugin_id(&self) -> &'static str;
    fn capability_namespaces(&self) -> &'static [&'static str];

    /// An explicit operation declaration is required; an empty manifest cannot
    /// implicitly grant every operation under a namespace.
    fn manifest(&self) -> SdkPluginManifest;

    async fn invoke(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult>;
}
