//! Account-owned, namespaced records. The host chooses a backend and hands an
//! extension only a partition handle. Field and index declarations stay in its package.
use crate::client::im_client::SessionEndWatch;
use crate::{ErrorCode, FlareError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize)]
pub struct RecordScope {
    pub tenant: String,
    pub user: String,
    pub namespace: String,
    pub partition: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    Text,
    Integer,
    Boolean,
}
impl FieldType {
    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Text => value.is_string(),
            Self::Integer => value.is_i64() || value.is_u64(),
            Self::Boolean => value.is_boolean(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordSchema {
    pub version: u32,
    pub fields: BTreeMap<String, FieldType>,
    pub indexed: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtensionRecord {
    pub id: String,
    pub revision: u32,
    pub schema_version: u32,
    /// A tombstone retains its revision so old updates cannot recreate it.
    pub fields: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CheckpointUpdate {
    pub expected: Option<String>,
    pub next: String,
}

/// A partition an extension declares at install time. Core opens it on the account's own
/// record backend, under the declaring extension's namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionDecl {
    pub name: String,
    pub schema: RecordSchema,
    pub access: RecordAccess,
}

/// One prepared row write: compare the previous revision (zero for an absent row), then
/// replace the record and its index entries.
#[derive(Clone, Debug, Serialize)]
pub struct RecordWrite {
    pub scope: RecordScope,
    pub expected: u32,
    pub record: ExtensionRecord,
    pub indexes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CheckpointWrite {
    pub scope: RecordScope,
    pub update: CheckpointUpdate,
}

/// The schema a partition is installed at, separate from the extension's own sync
/// checkpoint. `migrating_after` is Some while existing rows are being rewritten to
/// that schema: the last ID already rewritten, empty when the rewrite has not begun.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    pub schema_version: u32,
    pub migrating_after: Option<String>,
}

#[async_trait]
pub trait RecordBackend: Send + Sync {
    async fn get(&self, scope: &RecordScope, id: &str) -> Result<Option<ExtensionRecord>>;
    async fn checkpoint(&self, scope: &RecordScope) -> Result<Option<String>>;
    /// Applies every row and checkpoint write in one transaction, or none: false when a
    /// row revision or checkpoint is not its expected value. In the same transaction it
    /// must reject, with an error, a row whose schema version differs from its
    /// partition's installed version. Writes may span partitions of one namespace.
    async fn commit(&self, rows: &[RecordWrite], checkpoints: &[CheckpointWrite]) -> Result<bool>;
    async fn installed(&self, scope: &RecordScope) -> Result<Option<PartitionState>>;
    /// Compare-and-set of the installed state; `expected` None means not installed.
    async fn install(
        &self,
        scope: &RecordScope,
        expected: Option<&PartitionState>,
        next: &PartitionState,
    ) -> Result<bool>;
    /// Uninstall: drop every partition's records, indexes, checkpoint and state for
    /// this account's namespace. `scope.partition` is ignored.
    async fn purge_namespace(&self, scope: &RecordScope) -> Result<()>;
    /// Stable ID order, including tombstones, for restartable schema migrations.
    async fn scan(&self, scope: &RecordScope, after: &str, limit: u32) -> Result<Vec<String>>;
    async fn find(
        &self,
        scope: &RecordScope,
        index: &str,
        value: &str,
        after: &str,
        limit: u32,
    ) -> Result<Vec<String>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordAccess {
    Read,
    Write,
}

#[derive(Clone)]
pub struct ExtensionPartition {
    backend: Arc<dyn RecordBackend>,
    scope: RecordScope,
    schema: Arc<RecordSchema>,
    session: SessionEndWatch,
    access: RecordAccess,
    grant: super::ProjectionGrant,
}
fn invalid(message: &str) -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, message.to_string())
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256 && id.trim() == id && !id.chars().any(char::is_control)
}
pub(crate) fn valid_partition(name: &str, schema: &RecordSchema) -> bool {
    valid_id(name)
        && schema.version != 0
        && schema.fields.keys().all(|k| valid_id(k))
        && schema.indexed.iter().all(|k| schema.fields.contains_key(k))
}
impl ExtensionPartition {
    /// Opening at a newer schema installs it and starts a rewrite of existing rows,
    /// which revokes writes from handles still on the older schema. A writer may not
    /// open below the installed schema; a reader may.
    pub(crate) async fn open(
        backend: Arc<dyn RecordBackend>,
        scope: RecordScope,
        schema: RecordSchema,
        session: SessionEndWatch,
        access: RecordAccess,
        grant: super::ProjectionGrant,
    ) -> Result<Self> {
        let partition = Self::new(backend, scope, schema, session, access, grant)?;
        let version = partition.schema.version;
        loop {
            partition.active()?;
            let current = partition.backend.installed(&partition.scope).await?;
            let next = match &current {
                Some(state) if state.schema_version == version => break,
                Some(state) if state.schema_version > version => {
                    if access == RecordAccess::Write {
                        return Err(invalid("a newer extension schema is installed"));
                    }
                    break;
                }
                _ if access == RecordAccess::Read => break,
                None => PartitionState {
                    schema_version: version,
                    migrating_after: None,
                },
                Some(_) => PartitionState {
                    schema_version: version,
                    migrating_after: Some(String::new()),
                },
            };
            if partition
                .backend
                .install(&partition.scope, current.as_ref(), &next)
                .await?
            {
                break;
            }
        }
        Ok(partition)
    }

    /// Rewrites up to `limit` rows still on an older schema so their indexes match
    /// this one, then advances the rewrite bookmark. Returns true once every row is
    /// rewritten. Safe to repeat after a crash: rewritten rows are skipped.
    pub async fn migrate_step(&self, limit: u32) -> Result<bool> {
        let Some(state) = self.backend.installed(&self.scope).await? else {
            return Ok(true);
        };
        if state.schema_version != self.schema.version {
            return Err(invalid("extension partition schema changed"));
        }
        let Some(after) = state.migrating_after.clone() else {
            return Ok(true);
        };
        let ids = self.scan(&after, limit).await?;
        for id in &ids {
            let Some(row) = self.get(id).await? else {
                continue;
            };
            if row.schema_version < self.schema.version {
                let changes = row.fields.as_ref().map(|_| BTreeMap::new());
                // A concurrent writer already stored this row at the current schema.
                self.patch(id, row.revision, changes, None).await?;
            }
        }
        let next = PartitionState {
            schema_version: state.schema_version,
            migrating_after: (ids.len() as u32 == limit)
                .then(|| ids.last().cloned())
                .flatten(),
        };
        self.backend
            .install(&self.scope, Some(&state), &next)
            .await?;
        Ok(next.migrating_after.is_none())
    }

    fn new(
        backend: Arc<dyn RecordBackend>,
        scope: RecordScope,
        schema: RecordSchema,
        session: SessionEndWatch,
        access: RecordAccess,
        grant: super::ProjectionGrant,
    ) -> Result<Self> {
        if !valid_partition(&scope.partition, &schema) {
            return Err(invalid("invalid extension partition or schema"));
        }
        Ok(Self {
            backend,
            scope,
            schema: Arc::new(schema),
            session,
            access,
            grant,
        })
    }
    fn active(&self) -> Result<()> {
        if self.session.is_ended() || *self.grant.revoked.borrow() {
            return Err(FlareError::localized(
                ErrorCode::NotConnected,
                "extension grant revoked",
            ));
        }
        Ok(())
    }
    pub async fn get(&self, id: &str) -> Result<Option<ExtensionRecord>> {
        self.active()?;
        if !valid_id(id) {
            return Err(invalid("invalid record id"));
        }
        let value = self.backend.get(&self.scope, id).await?;
        self.active()?;
        Ok(value)
    }
    pub async fn checkpoint(&self) -> Result<Option<String>> {
        self.active()?;
        let value = self.backend.checkpoint(&self.scope).await?;
        self.active()?;
        Ok(value)
    }
    pub async fn scan(&self, after: &str, limit: u32) -> Result<Vec<String>> {
        self.active()?;
        if !(1..=512).contains(&limit) {
            return Err(invalid("invalid scan limit"));
        }
        let rows = self.backend.scan(&self.scope, after, limit).await?;
        self.active()?;
        Ok(rows)
    }
    /// Equality queries use the same JSON scalar representation as commit.
    pub async fn find(
        &self,
        field: &str,
        value: &Value,
        after: &str,
        limit: u32,
    ) -> Result<Vec<String>> {
        self.active()?;
        if !self.schema.indexed.contains(field)
            || !self.schema.fields[field].accepts(value)
            || !(1..=512).contains(&limit)
        {
            return Err(invalid("invalid record index query"));
        }
        let ids = self
            .backend
            .find(&self.scope, field, &value.to_string(), after, limit)
            .await?;
        self.active()?;
        Ok(ids)
    }
    /// None deletes an entity. Within a patch None removes just that field.
    pub async fn patch(
        &self,
        id: &str,
        expected: u32,
        changes: Option<BTreeMap<String, Option<Value>>>,
        checkpoint: Option<CheckpointUpdate>,
    ) -> Result<bool> {
        let Some(row) = self.prepare(id, expected, changes).await? else {
            return Ok(false);
        };
        let checkpoints = checkpoint
            .map(|update| CheckpointWrite {
                scope: self.scope.clone(),
                update,
            })
            .into_iter()
            .collect();
        self.submit(vec![row], checkpoints).await
    }

    /// The write for one row, or None when its revision is already not `expected`.
    async fn prepare(
        &self,
        id: &str,
        expected: u32,
        changes: Option<BTreeMap<String, Option<Value>>>,
    ) -> Result<Option<RecordWrite>> {
        self.writable()?;
        let old = self.get(id).await?;
        if old.as_ref().map_or(0, |r| r.revision) != expected {
            return Ok(None);
        }
        if old
            .as_ref()
            .is_some_and(|r| r.schema_version > self.schema.version)
        {
            return Err(invalid("newer extension record schema"));
        }
        let fields = if let Some(changes) = changes {
            let mut fields = old.and_then(|r| r.fields).unwrap_or_default();
            for (name, value) in changes {
                match value {
                    None => {
                        fields.remove(&name);
                    }
                    Some(value) => {
                        fields.insert(name, value);
                    }
                }
            }
            if fields
                .iter()
                .any(|(k, v)| !self.schema.fields.get(k).is_some_and(|ty| ty.accepts(v)))
            {
                return Err(invalid("record field does not match schema"));
            }
            Some(fields)
        } else {
            None
        };
        let mut indexes = BTreeMap::new();
        if let Some(row) = &fields {
            for name in &self.schema.indexed {
                if let Some(value) = row.get(name) {
                    indexes.insert(name.clone(), value.to_string());
                }
            }
        }
        Ok(Some(RecordWrite {
            scope: self.scope.clone(),
            expected,
            record: ExtensionRecord {
                id: id.into(),
                revision: expected
                    .checked_add(1)
                    .ok_or_else(|| invalid("record revision exhausted"))?,
                schema_version: self.schema.version,
                fields,
            },
            indexes,
        }))
    }

    fn writable(&self) -> Result<()> {
        self.active()?;
        if self.access != RecordAccess::Write {
            return Err(invalid("read-only extension partition"));
        }
        Ok(())
    }

    async fn submit(
        &self,
        rows: Vec<RecordWrite>,
        checkpoints: Vec<CheckpointWrite>,
    ) -> Result<bool> {
        self.active()?;
        let permit = self.grant.writes.clone().read_owned().await;
        self.active()?;
        let backend = self.backend.clone();
        let (done, result) = tokio::sync::oneshot::channel();
        // A dropped caller cannot cancel a transaction already submitted to IndexedDB.
        // Keep ownership until the adapter has actually committed or aborted it.
        crate::shared::util::background::spawn_background(async move {
            let result = backend.commit(&rows, &checkpoints).await;
            drop(permit);
            let _ = done.send(result);
        });
        result
            .await
            .map_err(|_| FlareError::system("extension commit interrupted"))?
    }
}

type Changes = BTreeMap<String, Option<Value>>;

/// Row and checkpoint writes across partitions of one namespace, committed in one
/// transaction: every write lands, or none does. Each row keeps its own revision CAS.
#[derive(Default)]
pub struct PartitionBatch {
    rows: Vec<(ExtensionPartition, String, u32, Option<Changes>)>,
    checkpoints: Vec<(ExtensionPartition, CheckpointUpdate)>,
}

impl PartitionBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Same meaning as [`ExtensionPartition::patch`]; one row may appear only once.
    pub fn patch(
        &mut self,
        partition: &ExtensionPartition,
        id: impl Into<String>,
        expected: u32,
        changes: Option<BTreeMap<String, Option<Value>>>,
    ) -> &mut Self {
        self.rows
            .push((partition.clone(), id.into(), expected, changes));
        self
    }

    /// Compare-and-set of a partition's checkpoint; one per partition.
    pub fn checkpoint(
        &mut self,
        partition: &ExtensionPartition,
        update: CheckpointUpdate,
    ) -> &mut Self {
        self.checkpoints.push((partition.clone(), update));
        self
    }

    /// False, with nothing written, when any row revision or checkpoint has moved.
    pub async fn commit(self) -> Result<bool> {
        let Some(first) = self
            .rows
            .first()
            .map(|r| &r.0)
            .or(self.checkpoints.first().map(|c| &c.0))
            .cloned()
        else {
            return Ok(true);
        };
        let same_namespace = |p: &ExtensionPartition| {
            Arc::ptr_eq(&p.backend, &first.backend)
                && p.scope.tenant == first.scope.tenant
                && p.scope.user == first.scope.user
                && p.scope.namespace == first.scope.namespace
        };
        let mut rows_seen = BTreeSet::new();
        let mut checkpoints_seen = BTreeSet::new();
        if !self.rows.iter().all(|(p, id, ..)| {
            same_namespace(p) && rows_seen.insert((p.scope.partition.clone(), id.clone()))
        }) || !self
            .checkpoints
            .iter()
            .all(|(p, _)| same_namespace(p) && checkpoints_seen.insert(p.scope.partition.clone()))
        {
            return Err(invalid(
                "a batch writes each row and checkpoint once, within one namespace",
            ));
        }
        let mut rows = Vec::with_capacity(self.rows.len());
        for (partition, id, expected, changes) in self.rows {
            match partition.prepare(&id, expected, changes).await? {
                Some(row) => rows.push(row),
                None => return Ok(false),
            }
        }
        let mut checkpoints = Vec::with_capacity(self.checkpoints.len());
        for (partition, update) in self.checkpoints {
            partition.writable()?;
            checkpoints.push(CheckpointWrite {
                scope: partition.scope.clone(),
                update,
            });
        }
        first.submit(rows, checkpoints).await
    }
}
