//! wasm 宿主桥的适配点。
//!
//! 浏览器端由平台包经已有的 `setStorageHost` 宿主桥新增 `global` 命名空间（IndexedDB）实现
//! [`GlobalStorageHost`]；本 crate 只定义宿主需要提供的最小能力，并把它适配成 [`AccessStore`]。
//! trait 本身与平台无关，原生端也可以用它接入宿主提供的存储（例如 Tauri 前端）。

use super::{AccessStore, StoreError, StoreOp};
use crate::platform::MaybeSendSync;

/// 宿主需要实现的全局命名空间键值能力。键已带命名空间前缀，宿主原样存取即可。
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait GlobalStorageHost: MaybeSendSync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;

    /// 在一个事务里提交全部操作（IndexedDB 单个 readwrite 事务即可满足）。
    async fn commit(&self, ops: &[StoreOp]) -> Result<(), StoreError>;

    /// 按前缀列出（IndexedDB 用 `IDBKeyRange.bound(prefix, prefix + '￿')`）。
    async fn scan(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError>;
}

/// 把宿主适配成 [`AccessStore`]，所有键加上 `namespace` 前缀，避免与宿主的其它数据混在一起。
pub struct HostAccessStore<H> {
    host: H,
    namespace: String,
}

impl<H> HostAccessStore<H> {
    /// 默认命名空间 `flare-access/`。
    pub fn new(host: H) -> Self {
        Self::with_namespace(host, "flare-access/")
    }

    pub fn with_namespace(host: H, namespace: impl Into<String>) -> Self {
        Self {
            host,
            namespace: namespace.into(),
        }
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{key}", self.namespace)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<H: GlobalStorageHost> AccessStore for HostAccessStore<H> {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.host.get(&self.full_key(key)).await
    }

    async fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        self.host
            .commit(&[StoreOp::Put {
                key: self.full_key(key),
                value: value.to_vec(),
            }])
            .await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.host
            .commit(&[StoreOp::Delete {
                key: self.full_key(key),
            }])
            .await
    }

    async fn apply(&self, ops: &[StoreOp]) -> Result<(), StoreError> {
        let prefixed: Vec<StoreOp> = ops
            .iter()
            .map(|op| match op {
                StoreOp::Put { key, value } => StoreOp::Put {
                    key: self.full_key(key),
                    value: value.clone(),
                },
                StoreOp::Delete { key } => StoreOp::Delete {
                    key: self.full_key(key),
                },
            })
            .collect();
        self.host.commit(&prefixed).await
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let entries = self.host.scan(&self.full_key(prefix)).await?;
        Ok(entries
            .into_iter()
            .filter_map(|(key, value)| {
                key.strip_prefix(&self.namespace)
                    .map(|stripped| (stripped.to_owned(), value))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryAccessStore;

    /// 用内存实现扮演宿主，验证前缀的加与去。
    struct FakeHost(MemoryAccessStore);

    #[async_trait::async_trait]
    impl GlobalStorageHost for FakeHost {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
            self.0.get(key).await
        }
        async fn commit(&self, ops: &[StoreOp]) -> Result<(), StoreError> {
            self.0.apply(ops).await
        }
        async fn scan(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
            self.0.scan_prefix(prefix).await
        }
    }

    #[tokio::test]
    async fn namespaces_keys() {
        let store = HostAccessStore::new(FakeHost(MemoryAccessStore::new()));
        store.put("health/ap/a/1", b"x").await.unwrap();
        store
            .apply(&[
                StoreOp::Put {
                    key: "health/net/1".into(),
                    value: b"y".to_vec(),
                },
                StoreOp::Delete {
                    key: "missing".into(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            store.host.0.keys(),
            vec!["flare-access/health/ap/a/1", "flare-access/health/net/1"]
        );
        let scanned = store.scan_prefix("health/").await.unwrap();
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].0, "health/ap/a/1");
        assert_eq!(
            store.get("health/net/1").await.unwrap(),
            Some(b"y".to_vec())
        );
    }
}
