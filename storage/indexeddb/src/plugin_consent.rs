//! Plugin capability consents in the browser's IndexedDB, per signed-in user.
use async_trait::async_trait;
use flare_im_core_sdk::spi::{PluginConsent, PluginConsentScope, PluginConsentStore};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};
use serde_json::{Value, json};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "/src/plugin_consent.mjs")]
extern "C" {
    #[wasm_bindgen(js_name = pluginConsentStore)]
    fn consent_call(encoded: &str) -> js_sys::Promise;
}

fn invalid(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("sdk.plugin_consent.{reason}"),
    )
}

pub(crate) struct IndexedDbPluginConsentStore {
    pub user_id: String,
}

impl IndexedDbPluginConsentStore {
    async fn call(
        &self,
        scope: &PluginConsentScope,
        operation: &str,
        extras: Value,
    ) -> Result<Value> {
        scope.validate()?;
        if scope.user_id != self.user_id {
            return Err(invalid("invalid_scope"));
        }
        let mut payload =
            json!({"operation": operation, "tenantId": scope.tenant_id, "userId": scope.user_id});
        if let (Some(payload), Some(extras)) = (payload.as_object_mut(), extras.as_object()) {
            payload.extend(extras.clone());
        }
        let encoded = payload.to_string();
        // JS futures are !Send: run them on the local executor and await only
        // the Send result.
        let (send, recv) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = wasm_bindgen_futures::JsFuture::from(consent_call(&encoded))
                .await
                .map_err(|_| FlareError::system("plugin consent storage failure"))
                .and_then(|value| value.as_string().ok_or_else(|| invalid("corrupt_consent")))
                .and_then(|text| {
                    serde_json::from_slice::<Value>(text.as_bytes())
                        .map_err(|_| invalid("corrupt_consent"))
                });
            let _ = send.send(result);
        });
        let value = recv
            .await
            .map_err(|_| FlareError::system("plugin consent storage task cancelled"))??;
        if let Some(reason) = value.get("error").and_then(Value::as_str) {
            return Err(invalid(reason));
        }
        Ok(value)
    }
}

fn decode(value: Value) -> Result<PluginConsent> {
    let consent: PluginConsent =
        flare_im_core_sdk::json::from_value(value).map_err(|_| invalid("corrupt_consent"))?;
    consent.validate().map_err(|_| invalid("corrupt_consent"))?;
    Ok(consent)
}

#[async_trait]
impl PluginConsentStore for IndexedDbPluginConsentStore {
    async fn get(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: &str,
    ) -> Result<Option<PluginConsent>> {
        let value = self
            .call(
                scope,
                "get",
                json!({"pluginId": plugin_id, "capability": capability}),
            )
            .await?;
        if value.is_null() {
            Ok(None)
        } else {
            decode(value).map(Some)
        }
    }

    async fn grant(&self, scope: &PluginConsentScope, consent: &PluginConsent) -> Result<()> {
        consent.validate()?;
        self.call(scope, "grant", json!({"consent": consent}))
            .await?;
        Ok(())
    }

    async fn revoke(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: Option<&str>,
    ) -> Result<u64> {
        let value = self
            .call(
                scope,
                "revoke",
                json!({"pluginId": plugin_id, "capability": capability}),
            )
            .await?;
        Ok(value.get("revoked").and_then(Value::as_u64).unwrap_or(0))
    }

    async fn list(
        &self,
        scope: &PluginConsentScope,
        plugin_id: Option<&str>,
    ) -> Result<Vec<PluginConsent>> {
        let value = self
            .call(scope, "list", json!({"pluginId": plugin_id}))
            .await?;
        match value {
            Value::Array(rows) => rows.into_iter().map(decode).collect(),
            _ => Err(invalid("corrupt_consent")),
        }
    }
}
