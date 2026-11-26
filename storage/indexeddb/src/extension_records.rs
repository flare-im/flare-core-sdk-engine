use async_trait::async_trait;
use flare_im_core_sdk::spi::{
    CheckpointWrite, ExtensionRecord, PartitionState, RecordBackend, RecordScope, RecordWrite,
};
use flare_im_core_sdk::{FlareError, Result};
use serde_json::{Value, json};
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
    async fn commit(&self, rows: &[RecordWrite], checkpoints: &[CheckpointWrite]) -> Result<bool> {
        let Some(scope) = rows
            .first()
            .map(|w| &w.scope)
            .or(checkpoints.first().map(|c| &c.scope))
        else {
            return Ok(true);
        };
        let mut encoded = Vec::with_capacity(rows.len());
        for w in rows {
            let record =
                serde_json::to_string(&w.record).map_err(|e| FlareError::system(e.to_string()))?;
            encoded.push(json!({"scope":w.scope,"id":w.record.id,"recordJson":record,"indexes":w.indexes,"expected":w.expected,"schemaVersion":w.record.schema_version}));
        }
        let checkpoints: Vec<_> = checkpoints
            .iter()
            .map(|c| json!({"scope":c.scope,"expected":c.update.expected,"next":c.update.next}))
            .collect();
        call(
            scope,
            json!({"op":"commit","rows":encoded,"checkpoints":checkpoints}),
        )
        .await
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
