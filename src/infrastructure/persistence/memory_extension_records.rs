//! Extension records for the in-memory account store: one lock per commit, so a batch
//! lands whole or not at all, and lives as long as the account's `StoreProvider`.
use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::extension::records::{
    CheckpointWrite, ExtensionRecord, PartitionState, RecordBackend, RecordScope, RecordWrite,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

type Key = (String, String, String, String);

#[derive(Default)]
struct Partition {
    /// String order is UTF-8 byte order, the same as SQLite BINARY.
    rows: BTreeMap<String, (ExtensionRecord, BTreeMap<String, String>)>,
    checkpoint: Option<String>,
    state: Option<PartitionState>,
}

#[derive(Default)]
pub struct MemoryExtensionRecords(Mutex<BTreeMap<Key, Partition>>);

fn key(scope: &RecordScope) -> Key {
    (
        scope.tenant.clone(),
        scope.user.clone(),
        scope.namespace.clone(),
        scope.partition.clone(),
    )
}

impl MemoryExtensionRecords {
    fn read<T>(&self, scope: &RecordScope, f: impl FnOnce(&Partition) -> T) -> T {
        let all = self.0.lock().unwrap_or_else(|p| p.into_inner());
        f(all.get(&key(scope)).unwrap_or(&Partition::default()))
    }
}

#[async_trait]
impl RecordBackend for MemoryExtensionRecords {
    async fn get(&self, scope: &RecordScope, id: &str) -> Result<Option<ExtensionRecord>> {
        Ok(self.read(scope, |p| p.rows.get(id).map(|r| r.0.clone())))
    }
    async fn checkpoint(&self, scope: &RecordScope) -> Result<Option<String>> {
        Ok(self.read(scope, |p| p.checkpoint.clone()))
    }
    async fn commit(&self, rows: &[RecordWrite], checkpoints: &[CheckpointWrite]) -> Result<bool> {
        let mut all = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let empty = Partition::default();
        for write in rows {
            let partition = all.get(&key(&write.scope)).unwrap_or(&empty);
            if partition
                .state
                .as_ref()
                .is_some_and(|s| s.schema_version != write.record.schema_version)
            {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "extension handle schema is not the installed schema",
                ));
            }
            let current = partition
                .rows
                .get(&write.record.id)
                .map_or(0, |r| r.0.revision);
            if current != write.expected {
                return Ok(false);
            }
        }
        if checkpoints
            .iter()
            .any(|c| all.get(&key(&c.scope)).unwrap_or(&empty).checkpoint != c.update.expected)
        {
            return Ok(false);
        }
        for write in rows {
            all.entry(key(&write.scope)).or_default().rows.insert(
                write.record.id.clone(),
                (write.record.clone(), write.indexes.clone()),
            );
        }
        for c in checkpoints {
            all.entry(key(&c.scope)).or_default().checkpoint = Some(c.update.next.clone());
        }
        Ok(true)
    }
    async fn installed(&self, scope: &RecordScope) -> Result<Option<PartitionState>> {
        Ok(self.read(scope, |p| p.state.clone()))
    }
    async fn install(
        &self,
        scope: &RecordScope,
        expected: Option<&PartitionState>,
        next: &PartitionState,
    ) -> Result<bool> {
        let mut all = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let partition = all.entry(key(scope)).or_default();
        if partition.state.as_ref() != expected {
            return Ok(false);
        }
        partition.state = Some(next.clone());
        Ok(true)
    }
    async fn purge_namespace(&self, scope: &RecordScope) -> Result<()> {
        let mut all = self.0.lock().unwrap_or_else(|p| p.into_inner());
        all.retain(|(tenant, user, namespace, _), _| {
            (tenant, user, namespace) != (&scope.tenant, &scope.user, &scope.namespace)
        });
        Ok(())
    }
    async fn scan(&self, scope: &RecordScope, after: &str, limit: u32) -> Result<Vec<String>> {
        Ok(self.read(scope, |p| {
            p.rows
                .range::<str, _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .take(limit as usize)
                .map(|(id, _)| id.clone())
                .collect()
        }))
    }
    async fn find(
        &self,
        scope: &RecordScope,
        index: &str,
        value: &str,
        after: &str,
        limit: u32,
    ) -> Result<Vec<String>> {
        Ok(self.read(scope, |p| {
            p.rows
                .range::<str, _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .filter(|(_, (_, indexes))| indexes.get(index).is_some_and(|v| v == value))
                .take(limit as usize)
                .map(|(id, _)| id.clone())
                .collect()
        }))
    }
}
