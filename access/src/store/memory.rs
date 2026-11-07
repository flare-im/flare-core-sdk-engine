//! 进程内存实现：测试、无持久化能力的宿主、以及开发期快速验证使用。

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{AccessStore, StoreError, StoreOp};

#[derive(Debug, Default)]
pub struct MemoryAccessStore {
    entries: Mutex<BTreeMap<String, Vec<u8>>>,
    fail_writes: AtomicBool,
}

impl MemoryAccessStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 让后续写入失败，用来验证「持久化失败不影响内存中的已验证状态」这类路径。
    pub fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    pub fn keys(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn check_writable(&self) -> Result<(), StoreError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            Err(StoreError::Backend("writes disabled".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl AccessStore for MemoryAccessStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.lock().get(key).cloned())
    }

    async fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        self.check_writable()?;
        self.lock().insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.check_writable()?;
        self.lock().remove(key);
        Ok(())
    }

    async fn apply(&self, ops: &[StoreOp]) -> Result<(), StoreError> {
        self.check_writable()?;
        let mut entries = self.lock();
        for op in ops {
            match op {
                StoreOp::Put { key, value } => {
                    entries.insert(key.clone(), value.clone());
                }
                StoreOp::Delete { key } => {
                    entries.remove(key);
                }
            }
        }
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        Ok(self
            .lock()
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}
