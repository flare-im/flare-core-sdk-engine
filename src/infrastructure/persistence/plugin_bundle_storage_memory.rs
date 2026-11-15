//! Bundle storage kept for the life of the process: the store of a client
//! without a user database (the browser), and of tests.
use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::Result;
use crate::domain::{
    PluginBundleStorage, PluginBundleStorageScope, bundle_storage_cost, bundle_storage_error,
    bundle_storage_fits, bundle_storage_quota_exceeded, valid_bundle_storage_key,
};

type Namespace = (String, String, String);

#[derive(Default)]
pub struct MemoryPluginBundleStorage {
    values: Mutex<BTreeMap<Namespace, BTreeMap<String, Value>>>,
}

impl MemoryPluginBundleStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn namespace(scope: &PluginBundleStorageScope) -> Namespace {
        (
            scope.tenant_id.clone(),
            scope.user_id.clone(),
            scope.plugin_id.clone(),
        )
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, BTreeMap<Namespace, BTreeMap<String, Value>>> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn checked(scope: &PluginBundleStorageScope, key: &str) -> Result<()> {
    scope.validate()?;
    if !valid_bundle_storage_key(key) {
        return Err(bundle_storage_error("invalid_key"));
    }
    Ok(())
}

#[async_trait]
impl PluginBundleStorage for MemoryPluginBundleStorage {
    async fn get(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<Option<Value>> {
        checked(scope, key)?;
        Ok(self
            .locked()
            .get(&Self::namespace(scope))
            .and_then(|values| values.get(key).cloned()))
    }

    async fn set(&self, scope: &PluginBundleStorageScope, key: &str, value: &Value) -> Result<()> {
        checked(scope, key)?;
        let cost = bundle_storage_cost(key, value)?;
        let mut all = self.locked();
        let values = all.entry(Self::namespace(scope)).or_default();
        let mut used = 0;
        let mut replaced = 0;
        for (k, v) in values.iter() {
            let c = bundle_storage_cost(k, v)?;
            used += c;
            if k == key {
                replaced = c;
            }
        }
        if !bundle_storage_fits(used, replaced, cost) {
            return Err(bundle_storage_quota_exceeded());
        }
        values.insert(key.to_owned(), value.clone());
        Ok(())
    }

    async fn remove(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<bool> {
        checked(scope, key)?;
        Ok(self
            .locked()
            .get_mut(&Self::namespace(scope))
            .is_some_and(|values| values.remove(key).is_some()))
    }

    async fn keys(
        &self,
        scope: &PluginBundleStorageScope,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        scope.validate()?;
        Ok(self
            .locked()
            .get(&Self::namespace(scope))
            .map(|values| {
                values
                    .keys()
                    .filter(|k| prefix.is_none_or(|p| k.starts_with(p)))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn clear_user(&self, tenant_id: &str, user_id: &str) -> Result<u64> {
        let mut all = self.locked();
        let before = all.len();
        all.retain(|(tenant, user, _), _| tenant != tenant_id || user != user_id);
        Ok((before - all.len()) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_plugin_protocol::bridge::{MAX_BRIDGE_STORAGE_BYTES, MAX_BRIDGE_STORAGE_VALUE_BYTES};
    use serde_json::json;

    fn scope(plugin: &str) -> PluginBundleStorageScope {
        PluginBundleStorageScope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            plugin_id: plugin.into(),
        }
    }

    #[tokio::test]
    async fn namespaces_are_per_plugin_and_signing_out_clears_them() {
        let store = MemoryPluginBundleStorage::new();
        store.set(&scope("a"), "k", &json!(1)).await.unwrap();
        assert_eq!(store.get(&scope("b"), "k").await.unwrap(), None);
        assert_eq!(store.get(&scope("a"), "k").await.unwrap(), Some(json!(1)));
        store.set(&scope("a"), "list/2", &json!([2])).await.unwrap();
        assert_eq!(
            store.keys(&scope("a"), Some("list/")).await.unwrap(),
            vec!["list/2".to_owned()]
        );
        assert!(store.remove(&scope("a"), "k").await.unwrap());
        assert!(!store.remove(&scope("a"), "k").await.unwrap());
        assert_eq!(store.clear_user("t1", "u1").await.unwrap(), 1);
        assert!(store.keys(&scope("a"), None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn values_keys_and_the_quota_are_bounded() {
        let store = MemoryPluginBundleStorage::new();
        assert!(store.set(&scope("a"), "k", &Value::Null).await.is_err());
        assert!(store.set(&scope("a"), "bad key", &json!(1)).await.is_err());
        let big = json!("x".repeat(MAX_BRIDGE_STORAGE_VALUE_BYTES));
        assert!(store.set(&scope("a"), "k", &big).await.is_err());
        let chunk = json!("x".repeat(MAX_BRIDGE_STORAGE_VALUE_BYTES - 16));
        let fits = MAX_BRIDGE_STORAGE_BYTES / (MAX_BRIDGE_STORAGE_VALUE_BYTES - 8);
        for i in 0..fits {
            store
                .set(&scope("a"), &format!("k{i}"), &chunk)
                .await
                .unwrap();
        }
        let error = store
            .set(&scope("a"), "one-more", &chunk)
            .await
            .unwrap_err();
        assert!(crate::domain::is_bundle_storage_quota_error(&error));
        // Replacing a key counts its old value out.
        store.set(&scope("a"), "k0", &chunk).await.unwrap();
        // Another plugin has its own quota.
        store.set(&scope("b"), "one-more", &chunk).await.unwrap();
    }
}
