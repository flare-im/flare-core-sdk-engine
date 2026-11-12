//! One-use native authentication flow dispatch. No payload or response is journaled.
use super::CapabilityApi;
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_plugin_protocol::{
    manifest::{ClientPlatform, UiContributionKind},
    ui::{NativeCapability, UiActionTarget},
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Session {
    tenant_id: String,
    user_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    plugin_id: String,
    page: Option<String>,
    contribution_id: Option<String>,
    action_id: String,
    platform: ClientPlatform,
    expected_user_id: String,
    #[serde(rename = "_session")]
    session: Session,
    operation: String,
    payload: Value,
    conversation_id: Option<String>,
}
fn denied() -> FlareError {
    FlareError::localized(
        ErrorCode::PermissionDenied,
        "sdk.plugin.native_action_unavailable",
    )
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}
fn has_command_id(value: &Value) -> bool {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::Object(object) => {
                if object
                    .keys()
                    .any(|key| matches!(key.as_str(), "commandId" | "command_id"))
                {
                    return true;
                }
                pending.extend(object.values());
            }
            Value::Array(values) => pending.extend(values),
            _ => {}
        }
    }
    false
}
impl CapabilityApi {
    /// Dispatch a same-provider operation explicitly admitted as transient by the
    /// current server catalog and a declared reauthenticate native action. Never
    /// calls the page command machinery or stores raw credentials/proofs.
    pub async fn plugin_native_invoke(&self, value: Value) -> Result<Value> {
        if serde_json::to_vec(&value).map_err(|_| denied())?.len() > 65536 {
            return Err(denied());
        }
        let request: Request = serde_json::from_value(value).map_err(|_| denied())?;
        if !valid_id(&request.plugin_id)
            || !valid_id(&request.action_id)
            || !valid_id(&request.operation)
            || request.page.is_some() == request.contribution_id.is_some()
            || request
                .page
                .as_deref()
                .or(request.contribution_id.as_deref())
                .is_none_or(|id| !valid_id(id))
            || !request.payload.is_object()
            || has_command_id(&request.payload)
            || request
                .conversation_id
                .as_deref()
                .is_some_and(|id| !flare_plugin_protocol::resource::valid_resource_id(id))
        {
            return Err(denied());
        }
        self.session_guard
            .run_with_user(|viewer| async move {
                let viewer = viewer.ok_or_else(denied)?;
                if request.expected_user_id != viewer
                    || request.session.user_id != viewer
                    || request.session.tenant_id != self.default_tenant_id
                {
                    return Err(denied());
                }
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let source = request
                    .page
                    .as_deref()
                    .or(request.contribution_id.as_deref())
                    .ok_or_else(denied)?;
                let target = catalog
                    .action(
                        &self.default_tenant_id,
                        &request.plugin_id,
                        source,
                        &request.action_id,
                        request.platform,
                        &[NativeCapability::Reauthenticate].into(),
                        true,
                        crate::extension::capability::ui_page::now_ms(),
                    )
                    .map_err(|_| denied())?;
                if target
                    != (UiActionTarget::Native {
                        capability: NativeCapability::Reauthenticate,
                    })
                {
                    return Err(denied());
                }
                let plugin = catalog
                    .plugins
                    .iter()
                    .find(|plugin| plugin.plugin_id == request.plugin_id)
                    .ok_or_else(denied)?;
                if request.page.is_some()
                    && !plugin
                        .extensions
                        .contributions
                        .iter()
                        .any(|entry| entry.id == source && entry.kind == UiContributionKind::Page)
                {
                    return Err(denied());
                }
                if !plugin.operations.contains(&request.operation)
                    || !plugin.transient_operations.contains(&request.operation)
                    || plugin.query_operations.contains(&request.operation)
                {
                    return Err(denied());
                }
                let mut ephemeral = self.clone();
                ephemeral.http = self.http.as_ref().map(|http| http.without_retries());
                let response = ephemeral
                    .dispatch_with(
                        &request.operation,
                        request.payload,
                        request.conversation_id.as_deref(),
                        Some(&self.default_tenant_id),
                        Some(&viewer),
                        None,
                    )
                    .await?;
                if response.plugin_id != request.plugin_id {
                    return Err(denied());
                }
                serde_json::to_value(response).map_err(|_| denied())
            })
            .await
    }
}
