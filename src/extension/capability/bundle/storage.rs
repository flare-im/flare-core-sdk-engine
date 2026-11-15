//! The bridge's `storage.*` methods over the user store's bundle storage
//! ([`crate::domain::PluginBundleStorage`]): one namespace per plugin of the
//! signed-in user, at most 1 MiB, a value at most 64 KiB.
use flare_plugin_protocol::bridge::{
    BridgeErrorCode, BridgeRequest, StorageGetResult, StorageKeysResult,
};
use serde_json::{Value, json};

use crate::domain::{PluginBundleStorage, PluginBundleStorageScope};

/// The result of a storage request, or the bridge code it fails with.
pub async fn run_storage(
    store: &dyn PluginBundleStorage,
    scope: &PluginBundleStorageScope,
    request: &BridgeRequest,
) -> Result<Value, BridgeErrorCode> {
    let failed = |error: crate::FlareError| super::broker::bridge_error_of(&error).code;
    match request {
        BridgeRequest::StorageGet(params) => {
            let value = store.get(scope, &params.key).await.map_err(failed)?;
            serde_json::to_value(StorageGetResult {
                value: value.unwrap_or(Value::Null),
            })
            .map_err(|_| BridgeErrorCode::Internal)
        }
        BridgeRequest::StorageSet(params) => {
            if params.value.is_null() {
                return Err(BridgeErrorCode::InvalidParams);
            }
            store
                .set(scope, &params.key, &params.value)
                .await
                .map_err(failed)?;
            Ok(json!({}))
        }
        BridgeRequest::StorageRemove(params) => {
            let removed = store.remove(scope, &params.key).await.map_err(failed)?;
            Ok(json!({ "removed": removed }))
        }
        BridgeRequest::StorageKeys(params) => {
            let keys = store
                .keys(scope, params.prefix.as_deref())
                .await
                .map_err(failed)?;
            serde_json::to_value(StorageKeysResult { keys }).map_err(|_| BridgeErrorCode::Internal)
        }
        _ => Err(BridgeErrorCode::MethodNotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::persistence::MemoryPluginBundleStorage;
    use flare_plugin_protocol::bridge::{StorageKeyParams, StorageKeysParams, StorageSetParams};

    fn scope(plugin: &str) -> PluginBundleStorageScope {
        PluginBundleStorageScope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            plugin_id: plugin.into(),
        }
    }

    #[tokio::test]
    async fn storage_requests_round_trip_within_the_plugin_namespace() {
        let store = MemoryPluginBundleStorage::new();
        let set = BridgeRequest::StorageSet(StorageSetParams {
            key: "draft/1".into(),
            value: json!({"title": "x"}),
        });
        assert_eq!(run_storage(&store, &scope("a"), &set).await, Ok(json!({})));
        let get = BridgeRequest::StorageGet(StorageKeyParams {
            key: "draft/1".into(),
        });
        assert_eq!(
            run_storage(&store, &scope("a"), &get).await,
            Ok(json!({"value": {"title": "x"}}))
        );
        assert_eq!(
            run_storage(&store, &scope("b"), &get).await,
            Ok(json!({"value": null})),
            "another plugin's namespace is empty"
        );
        let keys = BridgeRequest::StorageKeys(StorageKeysParams {
            prefix: Some("draft/".into()),
        });
        assert_eq!(
            run_storage(&store, &scope("a"), &keys).await,
            Ok(json!({"keys": ["draft/1"]}))
        );
    }

    #[tokio::test]
    async fn the_quota_answers_quota_exceeded() {
        let store = MemoryPluginBundleStorage::new();
        let chunk = json!("x".repeat(60 * 1024));
        let mut last = Ok(json!({}));
        for i in 0..20 {
            last = run_storage(
                &store,
                &scope("a"),
                &BridgeRequest::StorageSet(StorageSetParams {
                    key: format!("k{i}"),
                    value: chunk.clone(),
                }),
            )
            .await;
            if last.is_err() {
                break;
            }
        }
        assert_eq!(last, Err(BridgeErrorCode::QuotaExceeded));
    }
}
