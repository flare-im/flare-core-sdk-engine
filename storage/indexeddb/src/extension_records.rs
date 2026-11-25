use async_trait::async_trait;
use flare_im_core_sdk::spi::{
    CheckpointUpdate, ExtensionRecord, PartitionState, RecordBackend, RecordScope,
};
use flare_im_core_sdk::{FlareError, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "/src/extension_records.mjs")]
extern "C" {
    #[wasm_bindgen(js_name = extensionRecords)]
    fn invoke(encoded: &str) -> js_sys::Promise;
}

pub struct IndexedDbExtensionRecords;

async fn call<T: serde::de::DeserializeOwned>(
    scope: &RecordScope,
    mut request: Value,
) -> Result<T> {
    request["scope"] = json!(scope);
    let encoded = request.to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let reply = wasm_bindgen_futures::JsFuture::from(invoke(&encoded))
            .await
            .map_err(|e| FlareError::system(format!("extension storage: {e:?}")))
            .and_then(|v| {
                v.as_string()
                    .ok_or_else(|| FlareError::system("invalid extension storage response"))
            });
        let _ = tx.send(reply);
    });
    let encoded = rx
        .await
        .map_err(|_| FlareError::system("extension storage closed"))??;
    serde_json::from_str(&encoded).map_err(|e| FlareError::system(e.to_string()))
}

#[async_trait]
impl RecordBackend for IndexedDbExtensionRecords {
    async fn get(&self, scope: &RecordScope, id: &str) -> Result<Option<ExtensionRecord>> {
        call(scope, json!({"op":"get", "id":id})).await
    }
    async fn checkpoint(&self, scope: &RecordScope) -> Result<Option<String>> {
        call(scope, json!({"op":"checkpoint"})).await
    }
    async fn commit(
        &self,
        scope: &RecordScope,
        expected: u32,
        record: &ExtensionRecord,
        indexes: &BTreeMap<String, String>,
        checkpoint: Option<&CheckpointUpdate>,
    ) -> Result<bool> {
        call(scope, json!({"op":"commit","id":record.id,"recordJson":serde_json::to_string(record).map_err(|e| FlareError::system(e.to_string()))?,"indexes":indexes,"expected":expected,"schemaVersion":record.schema_version,"checkpoint":checkpoint})).await
    }
    async fn installed(&self, scope: &RecordScope) -> Result<Option<PartitionState>> {
        call(scope, json!({"op":"installed"})).await
    }
    async fn install(
        &self,
        scope: &RecordScope,
        expected: Option<&PartitionState>,
        next: &PartitionState,
    ) -> Result<bool> {
        call(
            scope,
            json!({"op":"install","expected":expected,"next":next}),
        )
        .await
    }
    async fn purge_namespace(&self, scope: &RecordScope) -> Result<()> {
        call::<Option<()>>(scope, json!({"op":"purge"}))
            .await
            .map(|_| ())
    }
    async fn scan(&self, scope: &RecordScope, after: &str, limit: u32) -> Result<Vec<String>> {
        call(scope, json!({"op":"scan","after":after,"limit":limit})).await
    }
    async fn find(
        &self,
        scope: &RecordScope,
        index: &str,
        value: &str,
        after: &str,
        limit: u32,
    ) -> Result<Vec<String>> {
        call(
            scope,
            json!({"op":"find", "index":index,"value":value,"after":after,"limit":limit}),
        )
        .await
    }
}
