//! What plugin web bundles keep through the bridge's `storage.*` methods, in
//! the browser's IndexedDB: one namespace per (tenant, signed-in user,
//! plugin), the quota checked and the value written in one transaction, and
//! the user's namespaces removed on sign-out.
use async_trait::async_trait;
use flare_im_core_sdk::storage::{
    MAX_PLUGIN_BUNDLE_STORAGE_BYTES, PluginBundleStorage, PluginBundleStorageScope,
    bundle_storage_cost, valid_bundle_storage_key,
};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};
use serde_json::{Value, json};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "/src/plugin_bundle_storage.mjs")]
extern "C" {
    #[wasm_bindgen(js_name = pluginBundleStorage)]
    fn storage_call(encoded: &str) -> js_sys::Promise;
}

fn invalid(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("sdk.plugin_bundle_storage.{reason}"),
    )
}

/// The engine maps this code to the bridge's `quota_exceeded`.
fn quota_exceeded() -> FlareError {
    FlareError::localized(
        ErrorCode::ResourceExhausted,
        "sdk.plugin_bundle_storage.quota_exceeded",
    )
}

pub(crate) struct IndexedDbPluginBundleStorage {
    pub user_id: String,
}

impl IndexedDbPluginBundleStorage {
    async fn call(&self, payload: Value) -> Result<Value> {
        let encoded = payload.to_string();
        // JS futures are !Send: run them on the local executor and await only
        // the Send result.
        let (send, recv) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = wasm_bindgen_futures::JsFuture::from(storage_call(&encoded))
                .await
                .map_err(|_| FlareError::system("plugin bundle storage failure"))
                .and_then(|value| value.as_string().ok_or_else(|| invalid("corrupt_value")))
                .and_then(|text| {
                    serde_json::from_str::<Value>(&text).map_err(|_| invalid("corrupt_value"))
                });
            let _ = send.send(result);
        });
        let value = recv
            .await
            .map_err(|_| FlareError::system("plugin bundle storage task cancelled"))??;
        match value.get("error").and_then(Value::as_str) {
            Some("quota_exceeded") => Err(quota_exceeded()),
            Some(reason) => Err(invalid(reason)),
            None => Ok(value),
        }
    }

    /// The namespace's identity, refused unless it is the signed-in user's.
    fn owner(&self, scope: &PluginBundleStorageScope) -> Result<Value> {
        scope.validate()?;
        if scope.user_id != self.user_id {
            return Err(invalid("invalid_scope"));
        }
        Ok(json!({
            "tenantId": scope.tenant_id,
            "userId": scope.user_id,
            "pluginId": scope.plugin_id,
        }))
    }

    fn with(owner: Value, extras: Value) -> Value {
        let mut payload = owner;
        if let (Some(payload), Some(extras)) = (payload.as_object_mut(), extras.as_object()) {
            payload.extend(extras.clone());
        }
        payload
    }
}

#[async_trait]
impl PluginBundleStorage for IndexedDbPluginBundleStorage {
    async fn get(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<Option<Value>> {
        if !valid_bundle_storage_key(key) {
            return Err(invalid("invalid_key"));
        }
        let owner = self.owner(scope)?;
        let answer = self
            .call(Self::with(owner, json!({"operation": "get", "key": key})))
            .await?;
        match answer.get("value").and_then(Value::as_str) {
            Some(text) => serde_json::from_str(text)
                .map(Some)
                .map_err(|_| invalid("corrupt_value")),
            None => Ok(None),
        }
    }

    async fn set(&self, scope: &PluginBundleStorageScope, key: &str, value: &Value) -> Result<()> {
        if !valid_bundle_storage_key(key) {
            return Err(invalid("invalid_key"));
        }
        let owner = self.owner(scope)?;
        let cost = bundle_storage_cost(key, value)?;
        let text = serde_json::to_string(value).map_err(|_| invalid("invalid_value"))?;
        self.call(Self::with(
            owner,
            json!({"operation": "set", "key": key, "value": text, "cost": cost,
                "max": MAX_PLUGIN_BUNDLE_STORAGE_BYTES}),
        ))
        .await?;
        Ok(())
    }

    async fn remove(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<bool> {
        if !valid_bundle_storage_key(key) {
            return Err(invalid("invalid_key"));
        }
        let owner = self.owner(scope)?;
        let answer = self
            .call(Self::with(
                owner,
                json!({"operation": "remove", "key": key}),
            ))
            .await?;
        Ok(answer
            .get("removed")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    async fn keys(
        &self,
        scope: &PluginBundleStorageScope,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        let owner = self.owner(scope)?;
        let answer = self
            .call(Self::with(
                owner,
                json!({"operation": "keys", "prefix": prefix}),
            ))
            .await?;
        let keys = answer
            .get("keys")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("corrupt_value"))?;
        Ok(keys
            .iter()
            .filter_map(Value::as_str)
            .filter(|key| valid_bundle_storage_key(key))
            .map(str::to_owned)
            .collect())
    }

    async fn clear_user(&self, tenant_id: &str, user_id: &str) -> Result<u64> {
        if user_id != self.user_id {
            return Ok(0);
        }
        let answer = self
            .call(json!({"operation": "clear", "tenantId": tenant_id, "userId": user_id}))
            .await?;
        Ok(answer.get("cleared").and_then(Value::as_u64).unwrap_or(0))
    }
}
