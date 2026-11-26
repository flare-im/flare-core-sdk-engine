//! Declared partitions through public APIs only: an extension declares them at install,
//! opens them from its granted contexts on the account's own record backend, commits
//! batches atomically, and Core finishes schema rewrites at login. The same contract runs
//! on the SQLite account store and the in-memory one.
use async_trait::async_trait;
use flare_im_core_sdk::{
    Result,
    client::{IMClient, LoginDbKind, SdkConfigOverlay},
    spi::{
        CheckpointUpdate, CheckpointWrite, ExtensionLifecycle, ExtensionLifecycleContext,
        ExtensionRecord, ExtensionRegistry, FieldType, PartitionBatch, PartitionDecl, RecordAccess,
        RecordBackend, RecordSchema, RecordScope, RecordWrite, SdkExtension,
    },
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const NS: &str = "example.declared";
const TENANT: &str = "tenant-declared";
const USER: &str = "viewer";

fn schema(version: u32) -> RecordSchema {
    let mut fields = BTreeMap::from([("label".into(), FieldType::Text)]);
    let mut indexed = BTreeSet::from(["label".to_owned()]);
    if version > 1 {
        fields.insert("rank".into(), FieldType::Integer);
        indexed.insert("rank".into());
    }
    RecordSchema {
        version,
        fields,
        indexed,
    }
}

struct Declared(u32);
impl SdkExtension for Declared {
    fn namespace(&self) -> &str {
        NS
    }
    fn install(&self, registry: &mut ExtensionRegistry) -> Result<()> {
        for name in ["items", "cursors"] {
            registry.add_partition(PartitionDecl {
                name: name.into(),
                schema: schema(self.0),
                access: RecordAccess::Write,
            });
        }
        registry.add_lifecycle(Arc::new(MarkLogin));
        Ok(())
    }
}

/// Lifecycle callbacks reach declared partitions without a host-supplied backend.
struct MarkLogin;
#[async_trait]
impl ExtensionLifecycle for MarkLogin {
    fn namespace(&self) -> &str {
        NS
    }
    async fn on_login(&self, context: &ExtensionLifecycleContext) -> Result<()> {
        let cursors = context.partition("cursors").await?;
        let revision = cursors.get("login").await?.map_or(0, |r| r.revision);
        let fields = BTreeMap::from([("label".into(), Some(json!("seen")))]);
        cursors.patch("login", revision, Some(fields), None).await?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Store {
    Sqlite,
    Memory,
}

/// One account's storage that survives logout, for either backend.
struct Account {
    store: Store,
    root: tempfile::TempDir,
    memory: flare_im_core_sdk::storage::StoreProvider,
}

impl Account {
    fn new(store: Store) -> Self {
        Self {
            store,
            root: tempfile::tempdir().unwrap(),
            memory: flare_im_core_sdk::storage::in_memory_im_provider(),
        }
    }
    async fn login(&self, version: u32) -> IMClient {
        let client = IMClient::builder()
            .stores(flare_im_core_sdk::storage::in_memory_im_provider())
            .add_extension(Declared(version))
            .build()
            .unwrap();
        let overlay = SdkConfigOverlay {
            data_url: Some(format!("file://{}", self.root.path().display())),
            tenant_id: Some(TENANT.into()),
            ..Default::default()
        };
        client.init(None, Some(overlay)).await.unwrap();
        let db = match self.store {
            Store::Sqlite => flare_im_core_sdk_sqlite_im::sqlite(),
            Store::Memory => LoginDbKind::provided(self.memory.clone()),
        };
        client.prepare(USER, db).await.unwrap();
        client
    }
}

async fn backend(client: &IMClient) -> Arc<dyn RecordBackend> {
    client
        .stores_async()
        .await
        .unwrap()
        .extension_records
        .expect("the account store selects a record backend")
}

fn scope(partition: &str) -> RecordScope {
    RecordScope {
        tenant: TENANT.into(),
        user: USER.into(),
        namespace: NS.into(),
        partition: partition.into(),
    }
}

fn label(value: &str) -> Option<BTreeMap<String, Option<serde_json::Value>>> {
    Some(BTreeMap::from([("label".into(), Some(json!(value)))]))
}

async fn batch_commit_is_atomic(store: Store) {
    let account = Account::new(store);
    let client = account.login(1).await;
    let context = client.extension_context_for(NS).await.unwrap();
    assert_eq!(
        context
            .partition("cursors")
            .await
            .unwrap()
            .get("login")
            .await
            .unwrap()
            .map(|r| r.revision),
        Some(1),
        "the login hook wrote through its own context"
    );
    assert!(context.partition("undeclared").await.is_err());
    let items = context.partition("items").await.unwrap();
    let cursors = context.partition("cursors").await.unwrap();

    let mut batch = PartitionBatch::new();
    batch
        .patch(&items, "a", 0, label("one"))
        .patch(&cursors, "agg", 0, label("v1"))
        .checkpoint(
            &items,
            CheckpointUpdate {
                expected: None,
                next: "e1".into(),
            },
        );
    assert!(batch.commit().await.unwrap());

    // A checkpoint that moved rejects the whole batch: neither row nor checkpoint lands.
    let mut stale = PartitionBatch::new();
    stale
        .patch(&items, "b", 0, label("two"))
        .patch(&cursors, "agg", 1, label("v2"))
        .checkpoint(
            &items,
            CheckpointUpdate {
                expected: None,
                next: "e2".into(),
            },
        );
    assert!(!stale.commit().await.unwrap());
    assert!(items.get("b").await.unwrap().is_none());
    assert_eq!(cursors.get("agg").await.unwrap().unwrap().revision, 1);
    assert_eq!(items.checkpoint().await.unwrap().as_deref(), Some("e1"));

    // A row CAS failing inside the backend transaction, after an earlier row was staged,
    // rolls back that row and the checkpoint.
    let record = |id: &str, revision| ExtensionRecord {
        id: id.into(),
        revision,
        schema_version: 1,
        fields: Some(BTreeMap::from([("label".into(), json!(id))])),
    };
    let row = |partition: &str, id: &str, expected| RecordWrite {
        scope: scope(partition),
        expected,
        record: record(id, expected + 1),
        indexes: BTreeMap::new(),
    };
    let raw = backend(&client).await;
    let committed = raw
        .commit(
            &[row("items", "c", 0), row("cursors", "agg", 0)],
            &[CheckpointWrite {
                scope: scope("items"),
                update: CheckpointUpdate {
                    expected: Some("e1".into()),
                    next: "e3".into(),
                },
            }],
        )
        .await
        .unwrap();
    assert!(!committed);
    assert!(items.get("c").await.unwrap().is_none());
    assert_eq!(items.checkpoint().await.unwrap().as_deref(), Some("e1"));

    // The same row twice is refused outright.
    let mut twice = PartitionBatch::new();
    twice
        .patch(&items, "d", 0, label("x"))
        .patch(&items, "d", 0, label("y"));
    assert!(twice.commit().await.is_err());
    assert_eq!(
        items.find("label", &json!("one"), "", 10).await.unwrap(),
        ["a"]
    );
    client.logout().await.unwrap();
}

async fn revoke_rejects_writes_and_purge_clears(store: Store) {
    let account = Account::new(store);
    let client = account.login(1).await;
    let context = client.extension_context_for(NS).await.unwrap();
    let items = context.partition("items").await.unwrap();
    assert!(items.patch("a", 0, label("one"), None).await.unwrap());
    let runtime = client.extension_runtime().unwrap();
    runtime.set_projection_enabled(NS, false).await.unwrap();
    assert!(items.patch("a", 1, label("two"), None).await.is_err());
    let mut batch = PartitionBatch::new();
    batch.patch(&items, "b", 0, label("x"));
    assert!(batch.commit().await.is_err());
    assert!(context.partition("items").await.is_err());
    assert!(client.extension_context_for(NS).await.is_err());
    runtime.set_projection_enabled(NS, true).await.unwrap();
    // Handles from before the revocation stay revoked; a fresh context writes again.
    assert!(items.get("a").await.is_err());
    let items = client
        .extension_context_for(NS)
        .await
        .unwrap()
        .partition("items")
        .await
        .unwrap();
    assert_eq!(items.get("a").await.unwrap().unwrap().revision, 1);

    client
        .purge_extension_records(NS, backend(&client).await)
        .await
        .unwrap();
    runtime.set_projection_enabled(NS, true).await.unwrap();
    let items = client
        .extension_context_for(NS)
        .await
        .unwrap()
        .partition("items")
        .await
        .unwrap();
    assert!(items.get("a").await.unwrap().is_none());
    client.logout().await.unwrap();
}

async fn rewrite_resumes_after_interruption(store: Store) {
    let account = Account::new(store);
    let client = account.login(1).await;
    let items = client
        .extension_context_for(NS)
        .await
        .unwrap()
        .partition("items")
        .await
        .unwrap();
    for id in ["a", "b", "c"] {
        assert!(items.patch(id, 0, label(id), None).await.unwrap());
    }
    // A v2 rewrite starts and is interrupted after one row.
    let raw = backend(&client).await;
    let upgrade = client
        .open_extension_partition(
            NS,
            "items".into(),
            schema(2),
            RecordAccess::Write,
            raw.clone(),
        )
        .await
        .unwrap();
    assert!(!upgrade.migrate_step(1).await.unwrap());
    assert_eq!(
        raw.installed(&scope("items"))
            .await
            .unwrap()
            .unwrap()
            .migrating_after,
        Some("a".into())
    );
    client.logout().await.unwrap();

    // The v2 extension's next login finishes the rewrite from the bookmark.
    let client = account.login(2).await;
    let raw = backend(&client).await;
    for partition in ["items", "cursors"] {
        let state = raw.installed(&scope(partition)).await.unwrap().unwrap();
        assert_eq!((state.schema_version, state.migrating_after), (2, None));
    }
    let items = client
        .extension_context_for(NS)
        .await
        .unwrap()
        .partition("items")
        .await
        .unwrap();
    for id in ["a", "b", "c"] {
        let row = items.get(id).await.unwrap().unwrap();
        assert_eq!((row.schema_version, row.revision), (2, 2), "{id}");
    }
    assert!(
        items
            .patch(
                "b",
                2,
                Some(BTreeMap::from([("rank".into(), Some(json!(5)))])),
                None
            )
            .await
            .unwrap()
    );
    assert_eq!(items.find("rank", &json!(5), "", 10).await.unwrap(), ["b"]);
    client.logout().await.unwrap();
}

#[tokio::test]
async fn declared_batch_commit_is_atomic_on_every_account_store() {
    batch_commit_is_atomic(Store::Sqlite).await;
    batch_commit_is_atomic(Store::Memory).await;
}

#[tokio::test]
async fn declared_partition_revocation_and_purge_on_every_account_store() {
    revoke_rejects_writes_and_purge_clears(Store::Sqlite).await;
    revoke_rejects_writes_and_purge_clears(Store::Memory).await;
}

#[tokio::test]
async fn declared_partition_rewrite_resumes_on_every_account_store() {
    rewrite_resumes_after_interruption(Store::Sqlite).await;
    rewrite_resumes_after_interruption(Store::Memory).await;
}

#[test]
fn invalid_or_duplicate_declarations_fail_the_build() {
    struct Twice;
    impl SdkExtension for Twice {
        fn namespace(&self) -> &str {
            NS
        }
        fn install(&self, registry: &mut ExtensionRegistry) -> Result<()> {
            for _ in 0..2 {
                registry.add_partition(PartitionDecl {
                    name: "items".into(),
                    schema: schema(1),
                    access: RecordAccess::Write,
                });
            }
            Ok(())
        }
    }
    let built = IMClient::builder()
        .stores(flare_im_core_sdk::storage::in_memory_im_provider())
        .add_extension(Twice)
        .build();
    assert!(built.is_err());
}
