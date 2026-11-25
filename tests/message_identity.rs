use flare_im_core_sdk as sdk;
use sdk::model::IMMessage;
use sdk::prelude::*;
use sdk::storage::StoreProvider;

fn message(server: &str, client: &str, conversation: &str) -> IMMessage {
    let mut row = IMMessage::new(Default::default());
    row.server_id = server.into();
    row.client_msg_id = client.into();
    row.conversation_id = conversation.into();
    row.sender_id = "me".into();
    row.conversation_seq = 1;
    row
}

async fn backends(case: &str) -> Vec<(&'static str, StoreProvider, Option<std::path::PathBuf>)> {
    let mut backends: Vec<(&'static str, StoreProvider, Option<std::path::PathBuf>)> =
        vec![("memory", sdk::storage::in_memory_im_provider(), None)];
    if cfg!(feature = "lifecycle-sqlite") {
        let path = std::env::temp_dir().join(format!(
            "message-identity-{case}-sqlite-{}",
            std::process::id()
        ));
        let runtime = flare_im_core_sdk_sqlite_im::SqliteRuntimeAssembler::default()
            .assemble(RuntimeConfig {
                platform: PlatformKind::Native,
                sdk: SdkConfig::default(),
                storage: StorageConfig {
                    kind: StorageKind::Sqlite,
                    namespace: "identity-test".into(),
                    tenant_id: "tenant".into(),
                    user_id: "me".into(),
                    path: Some(path.to_string_lossy().into()),
                    encryption: StorageEncryptionConfig::disabled(),
                },
                media: MediaRuntimeConfig {
                    kind: MediaRuntimeKind::Native,
                    cache_namespace: None,
                },
            })
            .await
            .unwrap();
        backends.push(("sqlite", runtime.stores, Some(path)));
    }
    backends
}

#[tokio::test]
async fn public_message_reads_prefer_canonical_ids_and_reject_ambiguous_aliases() {
    for (kind, store, path) in backends("reads").await {
        let client = IMClient::new();
        let data =
            std::env::temp_dir().join(format!("message-identity-{kind}-{}", std::process::id()));
        client
            .init(
                Some("identity".into()),
                Some(
                    sdk::json::from_value(sdk::serde_json::json!({
                        "dataUrl": data.to_string_lossy()
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
        client
            .prepare("me", LoginDbKind::Provided(store))
            .await
            .unwrap();
        let stores = client.stores_async().await.unwrap();
        stores
            .messages
            .save_batch(&[
                message("alias-server", "target-server", "other"),
                message("target-server", "shared", "wanted"),
                message("third-server", "shared", "other"),
                message("unique-server", "unique-client", "wanted"),
            ])
            .await
            .unwrap();
        let api = client.connected_apis().await.unwrap().message_api;
        assert_eq!(
            api.get("target-server").await.unwrap().unwrap().server_id,
            "target-server",
            "{kind}"
        );
        assert_eq!(
            api.get_raw("target-server")
                .await
                .unwrap()
                .unwrap()
                .conversation_id,
            "wanted",
            "{kind}"
        );
        assert!(api.get_raw("shared").await.unwrap().is_none(), "{kind}");
        assert!(
            stores
                .messages
                .get_by_client_msg_ids(&["shared".into()])
                .await
                .unwrap()
                .is_empty(),
            "{kind}"
        );
        assert_eq!(
            api.get_raw("unique-client")
                .await
                .unwrap()
                .unwrap()
                .server_id,
            "unique-server",
            "{kind}"
        );
        stores
            .messages
            .apply_reaction_event("wanted", "target-server", "me", "👍", 1, Some(10))
            .await
            .unwrap();
        assert!(
            api.get_raw("alias-server")
                .await
                .unwrap()
                .unwrap()
                .reactions
                .is_empty(),
            "{kind}"
        );
        client.uninit().await.unwrap();
        drop(stores);
        if let Some(path) = path {
            tokio::fs::remove_dir_all(path).await.unwrap();
        }
        if data.exists() {
            tokio::fs::remove_dir_all(data).await.unwrap();
        }
    }
}

#[tokio::test]
async fn trusted_reaction_snapshots_recover_missed_events_without_regressing_newer_pairs() {
    use sdk::storage::OperationApplyResult;
    for (kind, store, path) in backends("reactions").await {
        let original = message("reaction-target", "reaction-client", "wanted");
        store.messages.save_one(&original).await.unwrap();
        store
            .messages
            .apply_reaction_event("wanted", "reaction-target", "me", "👍", 1, Some(10))
            .await
            .unwrap();
        let mut snapshot = original.to_proto();
        snapshot
            .attributes
            .insert("reactionsJson".into(), "[]".into());
        snapshot.reaction_states = vec![flare_proto::common::MessageReactionState {
            user_id: "me".into(),
            emoji: "👍".into(),
            event_seq: 20,
            active: false,
        }];
        let snapshot = IMMessage::new(snapshot);
        assert_eq!(
            snapshot
                .attributes
                .get("lastReactionEventSeq:me:👍")
                .map(String::as_str),
            Some("20")
        );
        assert!(snapshot.reactions.is_empty());
        assert_eq!(snapshot.attributes["reactionsJson"], "[]");
        store.messages.save_one(&snapshot).await.unwrap();
        assert!(
            store
                .messages
                .get("reaction-target")
                .await
                .unwrap()
                .unwrap()
                .reactions
                .is_empty(),
            "{kind}"
        );
        assert_eq!(
            store
                .messages
                .apply_reaction_event("wanted", "reaction-target", "me", "👍", 1, Some(10))
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale,
            "{kind}"
        );
        store
            .messages
            .apply_reaction_event("wanted", "reaction-target", "me", "👍", 1, Some(30))
            .await
            .unwrap();
        // Another user's earlier sequence is independent of mine.
        store
            .messages
            .apply_reaction_event("wanted", "reaction-target", "other", "👍", 1, Some(15))
            .await
            .unwrap();
        store.messages.save_one(&snapshot).await.unwrap();
        let current = store
            .messages
            .get("reaction-target")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.reactions[0].count, 2, "{kind}");
        assert_eq!(
            current.attributes["lastReactionEventSeq:me:👍"], "30",
            "{kind}"
        );
        assert!(
            current.to_proto().reaction_states.is_empty(),
            "senders cannot forward server state"
        );
        drop(store);
        if let Some(path) = path {
            let (_, reopened, _) = backends("reactions")
                .await
                .into_iter()
                .find(|(backend, _, _)| *backend == "sqlite")
                .unwrap();
            let persisted = reopened
                .messages
                .get("reaction-target")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(persisted.attributes["lastReactionEventSeq:me:👍"], "30");
            assert_eq!(persisted.reactions[0].count, 2);
            drop(reopened);
            tokio::fs::remove_dir_all(path).await.unwrap();
        }
    }
}
