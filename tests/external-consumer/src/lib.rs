//! Consumer contract using only public SDK and adapter APIs.
#[cfg(test)]
mod storage_contract;

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use flare_im_core_sdk::{
        Result,
        client::{IMClient, LoginDbKind, SdkConfigOverlay},
        model::IMMessage,
        spi::{
            CheckpointUpdate, ExtensionRegistry, FieldType, RecordAccess, RecordSchema,
            SdkExtension,
        },
        storage::{AccountStoreOpener, OutgoingSendStore, PendingSendVo, StoreProvider},
    };
    use flare_im_core_sdk_sqlite_im::{
        extension_records::SqliteExtensionRecords, provider::open_sqlite_store_for_user,
    };
    use serde_json::{Value, json};
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    /// A host can own the storage namespace and decorate the atomic send path.
    struct CustomerStore(Arc<AtomicUsize>);
    struct CountCommits(Arc<dyn OutgoingSendStore>, Arc<AtomicUsize>);
    #[async_trait]
    impl OutgoingSendStore for CountCommits {
        fn survives_restart(&self) -> bool {
            self.0.survives_restart()
        }
        async fn enqueue_send(&self, msg: &IMMessage, pending: &PendingSendVo) -> Result<()> {
            self.0.enqueue_send(msg, pending).await?;
            self.1.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        async fn complete_send(&self, msg: &IMMessage) -> Result<bool> {
            self.0.complete_send(msg).await
        }
    }
    #[async_trait]
    impl AccountStoreOpener for CustomerStore {
        async fn open(&self, root: &Path, tenant: &str, user: &str) -> Result<StoreProvider> {
            let mut stores =
                open_sqlite_store_for_user(root, "example.external", tenant, user).await?;
            let writer = stores.outgoing.take().unwrap();
            stores.outgoing = Some(Arc::new(CountCommits(writer, self.0.clone())));
            Ok(stores)
        }
    }
    struct Example;
    impl SdkExtension for Example {
        fn namespace(&self) -> &str {
            "example.external"
        }
        fn install(&self, _: &mut ExtensionRegistry) -> Result<()> {
            Ok(())
        }
    }
    fn sample() -> Value {
        serde_json::from_str(include_str!("../../storage_parity/send.json")).unwrap()
    }
    async fn open(root: &Path, backend: LoginDbKind, user: &str) -> IMClient {
        let client = IMClient::builder()
            .stores(flare_im_core_sdk::storage::in_memory_im_provider())
            .add_extension(Example)
            .build()
            .unwrap();
        let overlay = SdkConfigOverlay {
            data_url: Some(format!("file://{}", root.display())),
            tenant_id: Some(sample()["tenantId"].as_str().unwrap().into()),
            ..Default::default()
        };
        client.init(None, Some(overlay)).await.unwrap();
        client.prepare(user, backend).await.unwrap();
        client
    }
    #[tokio::test]
    async fn builtin_and_external_openers_recover_the_same_send_sample() {
        let fixture = sample();
        let user = fixture["senderId"].as_str().unwrap();
        let cid = fixture["conversationId"].as_str().unwrap();
        let samples = fixture["messages"].as_array().unwrap();
        let commits = Arc::new(AtomicUsize::new(0));
        for custom in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let backend = || {
                if custom {
                    LoginDbKind::Open(Arc::new(CustomerStore(commits.clone())))
                } else {
                    flare_im_core_sdk_sqlite_im::sqlite()
                }
            };
            let client = open(root.path(), backend(), user).await;
            for item in samples {
                let mut draft = client
                    .message_build()
                    .unwrap()
                    .create_text(cid, item["text"].as_str().unwrap(), false, &[])
                    .await
                    .unwrap();
                draft.client_msg_id = item["clientMsgId"].as_str().unwrap().into();
                draft.created_at = item["at"].as_u64().unwrap();
                draft.client_created_at = draft.created_at;
                draft.local_state.sort_ts = draft.created_at;
                client.message().unwrap().send(draft).await.unwrap();
            }
            client.logout().await.unwrap();
            drop(client);
            let reopened = open(root.path(), backend(), user).await;
            let stores = reopened.stores_async().await.unwrap();
            let pending = stores.pending_sends().unwrap().0.list().await.unwrap();
            assert_eq!(pending.len(), samples.len());
            assert!(stores.conversations.get(cid).await.unwrap().is_some());
            for (p, sample) in pending.iter().zip(samples) {
                assert_eq!(p.client_msg_id, sample["clientMsgId"].as_str().unwrap());
                assert!(
                    reopened
                        .message()
                        .unwrap()
                        .get_by_key(p.message.message_key().unwrap())
                        .await
                        .unwrap()
                        .is_some()
                );
            }
            reopened.logout().await.unwrap();
        }
        assert_eq!(commits.load(Ordering::Relaxed), samples.len());
    }
    #[tokio::test]
    async fn opaque_extension_payload_survives_reopening_without_its_plugin() {
        use flare_im_core_sdk::content::{ContentBuilder, message_elem::Elem};
        let fixture = sample();
        let root = tempfile::tempdir().unwrap();
        let user = fixture["senderId"].as_str().unwrap();
        let cid = fixture["conversationId"].as_str().unwrap();
        let client = open(root.path(), flare_im_core_sdk_sqlite_im::sqlite(), user).await;
        // Only the external consumer knows the format or the meaning of the gift ID.
        let payload = br#"{"giftId":"test-gift"}"#;
        let content = ContentBuilder::custom("example.external.gift")
            .payload(payload.to_vec())
            .metadata("schemaVersion", "1")
            .build();
        let draft = client
            .message_build()
            .unwrap()
            .create_with_content(cid, content)
            .await
            .unwrap();
        let key = draft.message_key().unwrap();
        client.message().unwrap().send(draft).await.unwrap();
        client.logout().await.unwrap();
        let bare = IMClient::builder()
            .stores(flare_im_core_sdk::storage::in_memory_im_provider())
            .build()
            .unwrap();
        bare.init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(format!("file://{}", root.path().display())),
                tenant_id: Some(fixture["tenantId"].as_str().unwrap().into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        bare.prepare(user, flare_im_core_sdk_sqlite_im::sqlite())
            .await
            .unwrap();
        let stored = bare
            .message()
            .unwrap()
            .get_by_key(key)
            .await
            .unwrap()
            .unwrap();
        let Some(Elem::Custom(envelope)) = stored.content else {
            panic!("lost custom envelope")
        };
        assert_eq!(envelope.r#type, "example.external.gift");
        assert_eq!(envelope.payload.as_slice(), payload);
        assert_eq!(envelope.metadata["schemaVersion"], "1");
        bare.logout().await.unwrap();
    }

    #[tokio::test]
    async fn external_schema_cas_and_revocation() {
        let root = tempfile::tempdir().unwrap();
        let client = open(root.path(), flare_im_core_sdk_sqlite_im::sqlite(), "viewer").await;
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let backend = Arc::new(SqliteExtensionRecords::open(pool).await.unwrap());
        let schema = RecordSchema {
            version: 1,
            fields: BTreeMap::from([("label".into(), FieldType::Text)]),
            indexed: BTreeSet::from(["label".into()]),
        };
        let partition = client
            .open_extension_partition(
                "example.external",
                "room-a".into(),
                schema.clone(),
                RecordAccess::Write,
                backend.clone(),
            )
            .await
            .unwrap();
        let fields = BTreeMap::from([("label".into(), Some(json!("one")))]);
        assert!(
            partition
                .patch(
                    "item",
                    0,
                    Some(fields.clone()),
                    Some(CheckpointUpdate {
                        expected: None,
                        next: "page-1".into()
                    })
                )
                .await
                .unwrap()
        );
        assert!(
            !partition
                .patch("item", 0, Some(fields), None)
                .await
                .unwrap()
        );
        assert!(
            !partition
                .patch(
                    "item",
                    1,
                    None,
                    Some(CheckpointUpdate {
                        expected: None,
                        next: "bad".into()
                    })
                )
                .await
                .unwrap()
        );
        assert_eq!(
            partition
                .find("label", &json!("one"), "", 10)
                .await
                .unwrap(),
            ["item"]
        );
        let mut upgraded = schema.clone();
        upgraded.version = 2;
        upgraded.fields.insert("rank".into(), FieldType::Integer);
        upgraded.indexed.insert("rank".into());
        let next = client
            .open_extension_partition(
                "example.external",
                "room-a".into(),
                upgraded,
                RecordAccess::Write,
                backend.clone(),
            )
            .await
            .unwrap();
        // Opening v2 revokes v1 writes and starts a rewrite that leaves the sync
        // checkpoint alone; the rewrite indexes the old row under the new schema.
        assert!(
            partition
                .patch("item", 1, Some(BTreeMap::new()), None)
                .await
                .is_err()
        );
        assert!(next.migrate_step(1).await.unwrap() || next.migrate_step(1).await.unwrap());
        assert_eq!(next.checkpoint().await.unwrap().as_deref(), Some("page-1"));
        assert_eq!(next.get("item").await.unwrap().unwrap().schema_version, 2);
        let patch = BTreeMap::from([("rank".into(), Some(json!(7)))]);
        assert!(next.patch("item", 2, Some(patch), None).await.unwrap());
        assert_eq!(
            next.find("rank", &json!(7), "", 10).await.unwrap(),
            ["item"]
        );
        assert!(partition.patch("item", 3, None, None).await.is_err());
        assert!(next.patch("item", 3, None, None).await.unwrap());
        assert!(
            next.find("rank", &json!(7), "", 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(next.get("item").await.unwrap().unwrap().revision, 4);
        let other = client
            .open_extension_partition(
                "example.external",
                "room-b".into(),
                schema.clone(),
                RecordAccess::Read,
                backend.clone(),
            )
            .await
            .unwrap();
        assert!(other.get("item").await.unwrap().is_none());
        let runtime = client.extension_runtime().unwrap();
        runtime
            .set_projection_enabled("example.external", false)
            .await
            .unwrap();
        runtime
            .set_projection_enabled("example.external", true)
            .await
            .unwrap();
        assert!(partition.get("item").await.is_err());
        client
            .purge_extension_records("example.external", backend.clone())
            .await
            .unwrap();
        runtime
            .set_projection_enabled("example.external", true)
            .await
            .unwrap();
        let reinstalled = client
            .open_extension_partition(
                "example.external",
                "room-a".into(),
                schema,
                RecordAccess::Write,
                backend,
            )
            .await
            .unwrap();
        assert!(reinstalled.get("item").await.unwrap().is_none());
        assert!(reinstalled.checkpoint().await.unwrap().is_none());
        client.logout().await.unwrap();
    }

    /// The same storage contract samples on the in-memory backend, the SQLite adapter
    /// and a host-owned opener assembled outside the Engine (`CustomerStore`).
    #[tokio::test]
    async fn storage_contract_samples_match_across_backends() {
        let root = tempfile::tempdir().unwrap();
        let sqlite = open_sqlite_store_for_user(root.path(), "contract", "tenant", "viewer")
            .await
            .unwrap();
        let custom = CustomerStore(Arc::new(AtomicUsize::new(0)))
            .open(&root.path().join("custom"), "tenant", "viewer")
            .await
            .unwrap();
        let memory = flare_im_core_sdk::storage::in_memory_im_provider();
        let expected = crate::storage_contract::run(&memory).await;
        assert_eq!(crate::storage_contract::run(&sqlite).await, expected);
        assert_eq!(crate::storage_contract::run(&custom).await, expected);
        assert_eq!(expected.latest_page, ["contract-c5", "contract-c4"]);
        assert_eq!(expected.older_page, ["contract-c3", "contract-c2"]);
        assert_eq!(
            expected.newer_page,
            ["contract-c3", "contract-c4", "contract-c5"]
        );
        assert_eq!(expected.by_key, Some(("contract-s3".into(), 3)));
        assert_eq!(expected.unread_and_read, (5, 0));
        assert_eq!(expected.unread_after_read, (2, 3));
    }
}
