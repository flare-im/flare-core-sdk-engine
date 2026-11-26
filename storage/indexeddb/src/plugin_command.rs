//! Browser-owned, atomic durable commands. This is independent of optional
//! message-cache host callbacks: a business-SDK WASM host must be safe as well.
use async_trait::async_trait;
use flare_im_core_sdk::spi::{
    PluginCommand, PluginCommandRecord, PluginCommandScope, PluginCommandStore,
};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};
use serde_json::{Value, json};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "/src/plugin_command.mjs")]
extern "C" {
    #[wasm_bindgen(js_name = pluginCommandJournal)]
    fn journal_call(encoded: &str) -> js_sys::Promise;
}
fn invalid(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("sdk.plugin_command.{reason}"),
    )
}
pub(crate) struct IndexedDbPluginCommandStore {
    pub user_id: String,
}
impl IndexedDbPluginCommandStore {
    async fn call(
        &self,
        scope: &PluginCommandScope,
        operation: &str,
        extras: Value,
    ) -> Result<Value> {
        scope.validate()?;
        if scope.user_id != self.user_id {
            return Err(invalid("invalid_scope"));
        }
        let mut payload = json!({"operation":operation,"tenantId":scope.tenant_id,"userId":scope.user_id,"slot":scope.slot});
        payload
            .as_object_mut()
            .unwrap()
            .extend(extras.as_object().unwrap().clone());
        let encoded = payload.to_string();
        // JS futures are !Send. Own them on the local executor and await only
        // the Send result; no fire-and-forget persistence before submission.
        let (send, recv) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = wasm_bindgen_futures::JsFuture::from(journal_call(&encoded))
                .await
                .map_err(|_| FlareError::system("plugin command journal storage failure"))
                .and_then(|value| value.as_string().ok_or_else(|| invalid("corrupt_journal")))
                .and_then(|text| {
                    serde_json::from_slice::<Value>(text.as_bytes())
                        .map_err(|_| invalid("corrupt_journal"))
                });
            let _ = send.send(result);
        });
        let value = recv
            .await
            .map_err(|_| FlareError::system("plugin command journal task cancelled"))??;
        if let Some(reason) = value.get("error").and_then(Value::as_str) {
            return Err(invalid(reason));
        }
        Ok(value)
    }
}
fn decode(value: Value) -> Result<PluginCommandRecord> {
    let record: PluginCommandRecord =
        flare_im_core_sdk::json::from_value(value).map_err(|_| invalid("corrupt_journal"))?;
    record.command.validate()?;
    if record
        .response
        .as_ref()
        .is_some_and(|r| !record.command.accepts_response(r) || r.to_string().len() > 65536)
    {
        return Err(invalid("corrupt_journal"));
    }
    Ok(record)
}
#[async_trait]
impl PluginCommandStore for IndexedDbPluginCommandStore {
    async fn prepare(
        &self,
        s: &PluginCommandScope,
        c: &PluginCommand,
    ) -> Result<PluginCommandRecord> {
        c.validate()?;
        decode(self.call(s,"prepare",json!({"commandJson":serde_json::to_string(c).map_err(|_| invalid("invalid_command"))?})).await?)
    }
    async fn pending(&self, s: &PluginCommandScope) -> Result<Option<PluginCommandRecord>> {
        let value = self.call(s, "pending", json!({})).await?;
        if value.is_null() {
            Ok(None)
        } else {
            decode(value).map(Some)
        }
    }
    async fn complete(
        &self,
        s: &PluginCommandScope,
        c: &PluginCommand,
        response: Value,
    ) -> Result<PluginCommandRecord> {
        c.validate()?;
        if !c.accepts_response(&response) {
            return Err(invalid("outcome_unconfirmed"));
        }
        let response = response.to_string();
        if response.len() > 65536 {
            return Err(invalid("response_limit"));
        }
        decode(self.call(s,"complete",json!({"commandJson":serde_json::to_string(c).map_err(|_| invalid("invalid_command"))?,"responseJson":response})).await?)
    }
    async fn acknowledge(&self, s: &PluginCommandScope, id: &str) -> Result<()> {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
        {
            return Err(invalid("invalid_command"));
        }
        self.call(s, "acknowledge", json!({"commandId":id})).await?;
        Ok(())
    }
}
