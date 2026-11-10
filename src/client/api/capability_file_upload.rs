//! Private plugin form uploads. Grants stay in memory and never enter the
//! durable plugin command journal; business ownership remains with the plugin.
use super::CapabilityApi;
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_plugin_protocol::manifest::{ClientPlatform, UiContributionKind};
use flare_plugin_protocol::ui::{UiFileReference, UiInput, UiInputKind, UiRenderContext};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginFileUploadPage {
    pub plugin_id: String,
    pub page: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginFileUploadRequest {
    pub page: PluginFileUploadPage,
    pub input_id: String,
    pub platform: ClientPlatform,
    #[serde(default)]
    pub locale: String,
    pub conversation_id: Option<String>,
    pub arguments: Option<Value>,
    pub data: Option<Value>,
    pub form_values: Option<Value>,
    pub file_name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
    pub command_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedFile {
    file_id: String,
    #[serde(default)]
    upload_url: Option<String>,
    #[serde(default)]
    expires_at_ms: Option<i64>,
    content_type: String,
    size_bytes: u64,
    sha256: String,
    ready: bool,
}

fn invalid() -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, "invalid plugin file upload")
}
fn unavailable() -> FlareError {
    FlareError::localized(
        ErrorCode::PermissionDenied,
        "plugin file upload unavailable",
    )
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}
fn object(value: &Option<Value>) -> bool {
    value.as_ref().is_none_or(|v| v.is_null() || v.is_object())
}

/// Accept only a relative private object-capability path. In particular, a
/// protocol-relative URL, credentials, a fragment or encoded path traversal is
/// never delegated to the HTTP transport. The grant is never diagnosed.
fn valid_private_path(value: &str) -> bool {
    if value.len() > 8192
        || value.chars().any(|c| c.is_control() || c.is_whitespace())
        || value.contains('#')
        || value.contains('\\')
    {
        return false;
    }
    let (path, query) = value.split_once('?').unwrap_or((value, ""));
    path.starts_with("/private/")
        && !path.contains('%')
        && !path.contains("//")
        && path.split('/').all(|part| !matches!(part, "." | ".."))
        && !query.is_empty()
}

/// Origin comparison shared by native and browser targets. ParsedUrl rejects
/// userinfo during parsing and normalizes host names and default ports. The
/// relative private-path policy is validated separately before URL resolution.
fn same_private_upload_origin(base: &str, target: &str) -> bool {
    let (Ok(base), Ok(target)) = (
        flare_access::ParsedUrl::parse(base),
        flare_access::ParsedUrl::parse(target),
    ) else {
        return false;
    };
    matches!(base.scheme(), "http" | "https")
        && base.scheme() == target.scheme()
        && base.host_str() == target.host_str()
        && base.port() == target.port()
        && base.fragment().is_none()
        && target.fragment().is_none()
}

impl CapabilityApi {
    /// Upload one file of a real, visible page form. Prepare and complete are
    /// direct declared plugin commands, with the caller's stable commandId.
    /// No upload grant, bytes or intermediate reply is stored in a journal.
    pub async fn plugin_file_upload(&self, value: Value) -> Result<Value> {
        let request: PluginFileUploadRequest =
            serde_json::from_value(value).map_err(|_| invalid())?;
        if !valid_id(&request.page.plugin_id)
            || !valid_id(&request.page.page)
            || !valid_id(&request.input_id)
            || !valid_id(&request.command_id)
            || request.locale.len() > 64
            || request.file_name.trim().is_empty()
            || request.file_name.chars().count() > 255
            || request
                .file_name
                .chars()
                .any(|c| c.is_control() || matches!(c, '/' | '\\'))
            || request.bytes.is_empty()
            || request.bytes.len() > flare_plugin_protocol::ui::UI_FILE_MAX_BYTES as usize
            || !object(&request.arguments)
            || !object(&request.data)
            || !object(&request.form_values)
            || request
                .conversation_id
                .as_deref()
                .is_some_and(|v| !valid_id(v))
        {
            return Err(invalid());
        }
        self.session_guard
            .run_with_user(|viewer| async move {
                let viewer = viewer.ok_or_else(unavailable)?;
                let session = json!({"tenantId": self.default_tenant_id, "userId": viewer});
                if request
                    .data
                    .as_ref()
                    .filter(|data| !data.is_null())
                    .is_some_and(|data| data.get("_session") != Some(&session))
                {
                    return Err(unavailable());
                }
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let context = UiRenderContext {
                    tenant: &self.default_tenant_id,
                    platform: request.platform,
                    kind: UiContributionKind::Page,
                    locale: &request.locale,
                    viewer: &viewer,
                    // fetch_plugin_ui_catalog already localizes the server expiry.
                    now_ms: crate::extension::capability::ui_page::now_ms(),
                };
                let (plugin, contribution) = catalog
                    .page(&request.page.plugin_id, &request.page.page, &context)
                    .map_err(|_| unavailable())?;
                let declaration = contribution
                    .schema
                    .inputs
                    .iter()
                    .find(|input| input.id == request.input_id && input.kind == UiInputKind::File)
                    .ok_or_else(unavailable)?;
                let file = declaration
                    .file
                    .as_ref()
                    .filter(|file| file.is_valid())
                    .ok_or_else(unavailable)?;
                for operation in [
                    &file.upload.prepare_operation,
                    &file.upload.complete_operation,
                ] {
                    if !operation.starts_with(&format!("{}.", plugin.plugin_id))
                        || !plugin.operations.contains(operation)
                        || plugin.query_operations.contains(operation)
                    {
                        return Err(unavailable());
                    }
                }
                if !file.accept.contains(&request.content_type)
                    || request.bytes.len() as u64 > file.max_bytes
                {
                    return Err(invalid());
                }
                // Re-read the authenticated page; caller data cannot replace its
                // choices/defaults, disablement, grants or current visible actions.
                let mut page_request = json!({
                    "pluginId": request.page.plugin_id, "page": request.page.page,
                    "platform": request.platform, "locale": request.locale,
                });
                if let Some(conversation) = &request.conversation_id {
                    page_request["conversationId"] = json!(conversation);
                }
                if let Some(arguments) = &request.arguments {
                    page_request["arguments"] = arguments.clone();
                }
                let view = self.plugin_page(page_request).await?;
                if view.get("available") != Some(&Value::Bool(true))
                    || view.pointer("/data/_session") != Some(&session)
                {
                    return Err(unavailable());
                }
                let resolved: Vec<UiInput> =
                    serde_json::from_value(view["inputs"].clone()).map_err(|_| unavailable())?;
                let form_values = request.form_values.as_ref().unwrap_or(&Value::Null);
                if form_values.as_object().is_some_and(|values| {
                    values
                        .keys()
                        .any(|id| !resolved.iter().any(|input| &input.id == id))
                }) {
                    return Err(invalid());
                }
                let visible = contribution.schema.actions.iter().any(|action| {
                    if !action.form.contains(&request.input_id) {
                        return false;
                    }
                    let offered = ["actions", "menuActions"].iter().any(|key| {
                        view[*key].as_array().is_some_and(|actions| {
                            actions.iter().any(|v| {
                                v["id"] == action.id && v.get("form") == Some(&json!(action.form))
                            })
                        })
                    });
                    let scope: Vec<&UiInput> = action
                        .form
                        .iter()
                        .filter_map(|id| resolved.iter().find(|input| &input.id == id))
                        .collect();
                    offered
                        && scope.len() == action.form.len()
                        && flare_plugin_protocol::ui::validate_input_conditions(&scope).is_ok()
                        && scope
                            .iter()
                            .find(|input| input.id == request.input_id)
                            .is_some_and(|input| {
                                input.file.as_ref() == Some(file)
                                    && flare_plugin_protocol::ui::form_input_is_visible(
                                        input,
                                        &scope,
                                        form_values,
                                    )
                            })
                });
                if !visible {
                    return Err(unavailable());
                }
                let sha256 = hex::encode(Sha256::digest(&request.bytes));
                let prepared = self
                    .dispatch(
                        &file.upload.prepare_operation,
                        json!({
                            "commandId": request.command_id, "purpose": file.upload.purpose,
                            "fileName": request.file_name, "contentType": request.content_type,
                            "sizeBytes": request.bytes.len(), "sha256": sha256,
                        }),
                        request.conversation_id.as_deref(),
                        Some(&self.default_tenant_id),
                        Some(&viewer),
                    )
                    .await?;
                if !prepared.success {
                    return Err(unavailable());
                }
                let prepared: PreparedFile =
                    serde_json::from_value(prepared.data).map_err(|_| invalid())?;
                let reference = UiFileReference {
                    file_id: prepared.file_id,
                };
                if !reference.is_valid()
                    || prepared.content_type != request.content_type
                    || prepared.size_bytes != request.bytes.len() as u64
                    || prepared.sha256 != sha256
                {
                    return Err(invalid());
                }
                if prepared.ready {
                    return serde_json::to_value(reference).map_err(|_| invalid());
                }
                if prepared
                    .expires_at_ms
                    .is_some_and(|expiry| expiry <= self.server_clock.now_ms())
                {
                    return Err(unavailable());
                }
                let relative = prepared
                    .upload_url
                    .as_deref()
                    .filter(|url| valid_private_path(url))
                    .ok_or_else(invalid)?;
                let http = self
                    .http
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .for_service(flare_access::ServiceClass::SocialApi)
                    .without_request_context();
                let base = http.url_for("/").await?;
                let target = http.url_for(relative).await?;
                if !same_private_upload_origin(&base, &target) {
                    return Err(invalid());
                }
                http.put_private_bytes_full_url(
                    &target,
                    &request.bytes,
                    &request.content_type,
                    file.max_bytes as usize,
                )
                .await?;
                let complete = self
                    .dispatch(
                        &file.upload.complete_operation,
                        json!({
                            "commandId": request.command_id, "fileId": reference.file_id,
                        }),
                        request.conversation_id.as_deref(),
                        Some(&self.default_tenant_id),
                        Some(&viewer),
                    )
                    .await?;
                if !complete.success {
                    return Err(unavailable());
                }
                let completed: UiFileReference =
                    serde_json::from_value(complete.data).map_err(|_| invalid())?;
                if completed != reference || !completed.is_valid() {
                    return Err(invalid());
                }
                serde_json::to_value(completed).map_err(|_| invalid())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_upload_origin_policy_is_shared_and_cannot_forward_credentials_or_fragments() {
        for (base, target) in [
            (
                "https://EXAMPLE.test/",
                "https://example.test:443/private/file?grant=opaque",
            ),
            (
                "http://127.0.0.1:25200/",
                "http://127.0.0.1:25200/private/file?grant=opaque",
            ),
            (
                "http://example.test:80/",
                "http://example.test/private/file?grant=opaque",
            ),
            (
                "http://[::1]:25200/",
                "http://[0:0:0:0:0:0:0:1]:25200/private/file?grant=opaque",
            ),
            (
                "https://example.test/social/",
                "https://example.test/social/private/file?grant=opaque",
            ),
        ] {
            assert!(same_private_upload_origin(base, target));
        }
        for target in [
            "https://other.test/private/file?grant=opaque",
            "https://example.test.evil.test/private/file?grant=opaque",
            "http://example.test/private/file?grant=opaque",
            "https://example.test:444/private/file?grant=opaque",
            "https://user:password@example.test/private/file?grant=opaque",
            "https://@example.test/private/file?grant=opaque",
            "https://example.test/private/file?grant=opaque#fragment",
            "//example.test/private/file?grant=opaque",
            "https://example.test/private/../file?grant=opaque",
            "https://example.test/private/%2e%2e/file?grant=opaque",
            "https://example.test/private/file?grant=opaque\n",
        ] {
            assert!(!same_private_upload_origin("https://example.test/", target));
        }
        for base in [
            "https://user:password@example.test/",
            "https://@example.test/",
            "https://example.test/#fragment",
            "https:example.test",
            "file:///private/",
        ] {
            assert!(!same_private_upload_origin(
                base,
                "https://example.test/private/file?grant=opaque"
            ));
        }
        assert!(!same_private_upload_origin(
            "ws://example.test/",
            "ws://example.test/private/file?grant=opaque"
        ));
    }

    #[test]
    fn private_grants_are_relative_bounded_and_cannot_change_origin_or_path_scope() {
        assert!(valid_private_path(
            "/private/example/materials/file-1?grant=opaque-token"
        ));
        for bad in [
            "https://host/private/file?grant=token",
            "//host/private/file?grant=token",
            "/public/file?grant=token",
            "/private/file",
            "/private/file?",
            "/private/../file?grant=x",
            "/private/%2e%2e/file?grant=x",
            "/private/file?grant=x#fragment",
            "/private/\\file?grant=x",
            "/private/file?grant=x\n",
            "/private//file?grant=x",
        ] {
            assert!(!valid_private_path(bad));
        }
        assert!(!valid_private_path(&format!(
            "/private/file?grant={}",
            "a".repeat(8192)
        )));
    }

    #[test]
    fn upload_request_and_intermediate_response_are_strict_typed_envelopes() {
        let good = json!({"page":{"pluginId":"example.plugin","page":"example.plugin.form"},
            "inputId":"attachment","platform":"web","fileName":"receipt.png","contentType":"image/png",
            "bytes":[0,128,255],"commandId":"upload-1"});
        assert!(serde_json::from_value::<PluginFileUploadRequest>(good.clone()).is_ok());
        for bytes in [json!([-1]), json!([256]), json!([1.5]), json!("base64")] {
            let mut bad = good.clone();
            bad["bytes"] = bytes;
            assert!(serde_json::from_value::<PluginFileUploadRequest>(bad).is_err());
        }
        let mut bad = good;
        bad["uploadUrl"] = json!("/private/attacker?grant=x");
        assert!(serde_json::from_value::<PluginFileUploadRequest>(bad).is_err());
        assert!(
            serde_json::from_value::<PreparedFile>(json!({"fileId":"file-1","ready":true,
            "contentType":"image/png","sizeBytes":3,"sha256":"x"}))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<PreparedFile>(json!({"fileId":"file-1","ready":"true",
            "contentType":"image/png","sizeBytes":3,"sha256":"x"}))
            .is_err()
        );
    }
}
