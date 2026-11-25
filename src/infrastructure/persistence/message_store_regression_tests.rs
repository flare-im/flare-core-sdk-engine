use crate::domain::{MessageStore, OperationApplyResult};
use crate::model::IMMessage;
use flare_proto::common::{
    ContentVisibility, MessageRetentionLifecycle, MessageRetentionPolicy, MessageRetentionState,
};

fn message() -> IMMessage {
    let mut message = IMMessage::new(Default::default());
    message.server_id = "m".into();
    message.client_msg_id = "local".into();
    message.conversation_id = "c".into();
    message.sender_id = "u".into();
    message.conversation_seq = 1;
    message.quote_preview = Some("private quote".into());
    message
        .attributes
        .insert("contentText".into(), "private searchable text".into());
    message.encoded_content = crate::content::ContentBuilder::text("private searchable text")
        .build()
        .encode();
    message
}

async fn retention_lifecycle(store: &dyn MessageStore) {
    let mut local = message();
    local.local_state.is_local = true;
    store.save_one(&local).await.unwrap();
    let mut state = MessageRetentionState {
        lifecycle: MessageRetentionLifecycle::Scheduled as i32,
        content_visibility: ContentVisibility::Available as i32,
        ..Default::default()
    };
    assert_eq!(
        store
            .apply_retention_scheduled_event(
                "local",
                &MessageRetentionPolicy::default(),
                &state,
                100,
                Some(10)
            )
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    state.lifecycle = MessageRetentionLifecycle::Expired as i32;
    state.content_visibility = ContentVisibility::Hidden as i32;
    assert_eq!(
        store
            .apply_retention_expired_event("m", &state, 200, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    store
        .apply_edit_event("m", message().encoded_content, Some(15))
        .await
        .unwrap();
    let expired = store.get("m").await.unwrap().unwrap();
    assert!(expired.encoded_content.is_empty());
    assert!(expired.text_preview.is_empty());
    assert!(expired.content.is_none());
    assert!(expired.quote_preview.is_none());
    assert!(!expired.attributes.contains_key("contentText"));
    assert_eq!(expired.retention_state, Some(state.clone()));
    assert!(store.search("private", 10).await.unwrap().is_empty());
    state.lifecycle = MessageRetentionLifecycle::Purged as i32;
    state.content_visibility = ContentVisibility::Purged as i32;
    assert_eq!(
        store
            .apply_retention_purged_event("m", &state, 300, Some(30))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    assert_eq!(
        store
            .apply_retention_scheduled_event(
                "m",
                &MessageRetentionPolicy::default(),
                &MessageRetentionState::default(),
                100,
                Some(10)
            )
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert_eq!(
        store
            .apply_retention_expired_event("missing", &state, 300, Some(30))
            .await
            .unwrap(),
        OperationApplyResult::NotFound
    );
    store.save_one(&message()).await.unwrap();
    store.update_after_ack("local", &message()).await.unwrap();
    assert!(
        store
            .get("m")
            .await
            .unwrap()
            .unwrap()
            .encoded_content
            .is_empty()
    );
    assert_eq!(
        store.get("m").await.unwrap().unwrap().retention_state,
        Some(state)
    );
}

#[tokio::test]
async fn memory_retention_lifecycle_hides_content_and_rejects_old_phases() {
    retention_lifecycle(&super::memory_im::MemoryMessageStore::new()).await;
}

#[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
#[tokio::test]
async fn sqlite_retention_and_application_partitions_survive_reopening() {
    use crate::domain::PendingSendVo;
    use crate::shared::util::sqlite_store::open_sqlite_store_for_user;
    let root = std::env::temp_dir().join(format!("flare-storage-scope-{}", uuid::Uuid::new_v4()));
    let a = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    retention_lifecycle(a.messages.as_ref()).await;
    ordered_operations(a.messages.as_ref()).await;
    a.pending_send_writer
        .as_ref()
        .unwrap()
        .push(PendingSendVo {
            client_msg_id: "local".into(),
            conversation_id: "c".into(),
            message: message(),
            enqueued_at_ms: 1,
        })
        .await
        .unwrap();
    let b = open_sqlite_store_for_user(&root, "core", "b", "u")
        .await
        .unwrap();
    assert!(b.messages.get("m").await.unwrap().is_none());
    assert!(
        b.pending_send_reader
            .as_ref()
            .unwrap()
            .list()
            .await
            .unwrap()
            .is_empty()
    );
    let reopened = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    assert!(
        reopened
            .messages
            .get("m")
            .await
            .unwrap()
            .unwrap()
            .encoded_content
            .is_empty()
    );
    assert_eq!(
        reopened
            .pending_send_reader
            .as_ref()
            .unwrap()
            .list()
            .await
            .unwrap()
            .len(),
        1
    );
    drop((a, b, reopened));
    tokio::fs::remove_dir_all(root).await.unwrap();
}

async fn ordered_operations(store: &dyn MessageStore) {
    use crate::content::ContentBuilder;
    use crate::domain::EditApplyResult;
    use flare_proto::common::ReactionAction;
    let mut m = message();
    m.server_id = "ops".into();
    m.client_msg_id = "ops-local".into();
    m.local_state.is_local = true;
    store.save_one(&m).await.unwrap();
    let new = ContentBuilder::text("new").build().encode();
    let old = ContentBuilder::text("old").build().encode();
    assert_eq!(
        store
            .apply_edit_event("ops-local", new.clone(), Some(20))
            .await
            .unwrap(),
        EditApplyResult::Applied
    );
    assert_eq!(
        store.apply_edit_event("ops", old, Some(10)).await.unwrap(),
        EditApplyResult::IgnoredStale
    );
    assert_eq!(
        store
            .apply_edit_event("ops", new.clone(), Some(20))
            .await
            .unwrap(),
        EditApplyResult::IgnoredStale
    );
    store.save_one(&m).await.unwrap();
    assert_eq!(
        store.get("ops").await.unwrap().unwrap().encoded_content,
        new
    );
    assert_eq!(
        store
            .apply_mark_event("ops", 1, Some(" red "), true, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    assert_eq!(
        store
            .get("ops")
            .await
            .unwrap()
            .unwrap()
            .attributes
            .get("markColor")
            .map(String::as_str),
        Some("red")
    );
    store
        .apply_mark_event("ops", 1, None, false, Some(30))
        .await
        .unwrap();
    assert_eq!(
        store
            .apply_mark_event("ops", 1, None, true, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert!(
        !store
            .get("ops")
            .await
            .unwrap()
            .unwrap()
            .attributes
            .contains_key("markType")
    );
    store.set_message_mark("ops", 2, None).await.unwrap();
    store.clear_message_mark("ops", 2).await.unwrap();
    assert!(
        !store
            .get("ops")
            .await
            .unwrap()
            .unwrap()
            .attributes
            .contains_key("markType")
    );
    store
        .apply_reaction_event(
            "c",
            "ops",
            "u",
            "👍",
            ReactionAction::Remove as i32,
            Some(20),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .apply_reaction_event("c", "ops", "u", "👍", ReactionAction::Add as i32, Some(10))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert!(
        store
            .get("ops")
            .await
            .unwrap()
            .unwrap()
            .reactions
            .is_empty()
    );
    assert_eq!(
        store
            .apply_reaction_event("c", "ops", "u", "👍", ReactionAction::Add as i32, Some(30))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    assert_eq!(store.get("ops").await.unwrap().unwrap().reactions.len(), 1);
}

#[tokio::test]
async fn memory_edit_mark_and_reaction_events_are_ordered() {
    ordered_operations(&super::memory_im::MemoryMessageStore::new()).await;
}

async fn terminal_snapshots_and_content_copies(stores: &super::StoreProvider) {
    let store = stores.messages.as_ref();
    let mut original = message();
    original.server_id = "snapshot".into();
    original.client_msg_id = "snapshot-local".into();
    original
        .extensions
        .insert("preview".into(), b"private".to_vec());
    original.offline_push_info = Some(Default::default());
    store.save_one(&original).await.unwrap();
    let scheduled = MessageRetentionState {
        lifecycle: MessageRetentionLifecycle::Scheduled as i32,
        content_visibility: ContentVisibility::Available as i32,
        ..Default::default()
    };
    store
        .apply_retention_scheduled_event(
            "snapshot",
            &MessageRetentionPolicy::default(),
            &scheduled,
            10,
            Some(10),
        )
        .await
        .unwrap();
    store
        .apply_edit_event(
            "snapshot",
            crate::content::ContentBuilder::text("new private edit")
                .build()
                .encode(),
            Some(20),
        )
        .await
        .unwrap();
    let mut redacted = original.clone();
    redacted.encoded_content.clear();
    redacted.retention_state = Some(MessageRetentionState {
        lifecycle: MessageRetentionLifecycle::Expired as i32,
        content_visibility: ContentVisibility::Redacted as i32,
        ..Default::default()
    });
    store.save_one(&redacted).await.unwrap();
    let actual = store.get("snapshot").await.unwrap().unwrap();
    assert_eq!(actual.retention_state, redacted.retention_state);
    assert!(actual.encoded_content.is_empty());
    assert!(actual.extensions.is_empty());
    assert!(actual.offline_push_info.is_none());
    store
        .update_after_ack("snapshot-local", &original)
        .await
        .unwrap();
    assert!(
        store
            .get("snapshot")
            .await
            .unwrap()
            .unwrap()
            .encoded_content
            .is_empty()
    );

    original.server_id = "server-only".into();
    original.client_msg_id = "server-only-local".into();
    let purged = MessageRetentionState {
        lifecycle: MessageRetentionLifecycle::Purged as i32,
        content_visibility: ContentVisibility::Purged as i32,
        ..Default::default()
    };
    let mut snapshot = original.clone();
    snapshot.retention_state = Some(purged.clone());
    snapshot.encoded_content.clear();
    store.save_one(&snapshot).await.unwrap();
    store.save_one(&original).await.unwrap();
    assert_eq!(
        store
            .apply_retention_scheduled_event(
                "server-only",
                &MessageRetentionPolicy::default(),
                &scheduled,
                40,
                Some(40)
            )
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    let actual = store.get("server-only").await.unwrap().unwrap();
    assert_eq!(actual.retention_state, Some(purged.clone()));
    assert!(actual.encoded_content.is_empty());

    original.server_id = "copies".into();
    original.client_msg_id = "copies-local".into();
    original.conversation_id = "copies-conversation".into();
    store.save_one(&original).await.unwrap();
    stores
        .conversations
        .save_one(&crate::model::Conversation {
            conversation_id: original.conversation_id.clone(),
            last_message_id: Some(original.server_id.clone()),
            last_message_preview: Some("private preview".into()),
            last_message: Some(crate::model::MessagePreviewElem {
                message_id: original.server_id.clone(),
                sender_id: "u".into(),
                r#type: 0,
                time: 1,
                text: "private preview".into(),
            }),
            max_seq: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .apply_retention_purged_event("copies", &purged, 30, Some(30))
        .await
        .unwrap();
    assert_retained_copies_cleared(stores).await;
}

async fn assert_retained_copies_cleared(stores: &super::StoreProvider) {
    let actual = stores.messages.get("copies").await.unwrap().unwrap();
    assert!(actual.encoded_content.is_empty());
    assert!(actual.extensions.is_empty());
    assert!(actual.offline_push_info.is_none());
    assert_eq!(
        serde_json::to_value(&actual).unwrap()["extensions"],
        serde_json::json!({})
    );
    let conversation = stores
        .conversations
        .get("copies-conversation")
        .await
        .unwrap()
        .unwrap();
    assert!(
        conversation
            .last_message_preview
            .as_deref()
            .unwrap_or_default()
            .is_empty()
    );
    assert!(
        conversation
            .last_message
            .as_ref()
            .is_none_or(|preview| preview.text.is_empty())
    );
    let mut stale = conversation.clone();
    stale.last_message_preview = Some("private preview".into());
    if let Some(preview) = &mut stale.last_message {
        preview.text = "private preview".into();
    }
    stores.conversations.save_one(&stale).await.unwrap();
    stores
        .conversations
        .update_last_message(
            "copies-conversation",
            "copies",
            "u",
            0,
            Some("private preview"),
            1,
        )
        .await
        .unwrap();
    let actual = stores
        .conversations
        .get("copies-conversation")
        .await
        .unwrap()
        .unwrap();
    assert!(
        actual
            .last_message_preview
            .as_deref()
            .unwrap_or_default()
            .is_empty(),
        "stale conversation snapshots cannot restore purged content"
    );
}

#[tokio::test]
async fn memory_terminal_snapshots_and_content_copies() {
    terminal_snapshots_and_content_copies(&super::memory_im::in_memory_im_provider()).await;
}

#[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
#[tokio::test]
async fn sqlite_terminal_snapshots_and_content_copies_survive_reopening() {
    use crate::shared::util::sqlite_store::open_sqlite_store_for_user;
    let root =
        std::env::temp_dir().join(format!("flare-terminal-snapshots-{}", uuid::Uuid::new_v4()));
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    terminal_snapshots_and_content_copies(&stores).await;
    drop(stores);
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    assert_retained_copies_cleared(&stores).await;
    assert!(
        stores
            .messages
            .get("server-only")
            .await
            .unwrap()
            .unwrap()
            .encoded_content
            .is_empty()
    );
    let path = crate::sqlite_test_adapter::provider::resolve_user_db_path(&root, "core", "a", "u");
    let pool = sqlx::SqlitePool::connect(
        &crate::shared::util::sqlite_store::sqlite_database_url_from_path(&path),
    )
    .await
    .unwrap();
    let preview: Option<String> = sqlx::query_scalar("SELECT last_message_preview FROM conversations WHERE conversation_id = 'copies-conversation'").fetch_one(&pool).await.unwrap();
    assert!(
        preview.is_none(),
        "purged plaintext must not return to the persisted conversation row"
    );
    pool.close().await;
    drop(stores);
    tokio::fs::remove_dir_all(root).await.unwrap();
}

async fn snapshots_preserve_ordered_operations(store: &dyn MessageStore) {
    use flare_proto::common::ReactionAction;
    let mut original = message();
    original.server_id = "reaction-snapshot".into();
    original.client_msg_id = "reaction-snapshot-local".into();
    store.save_one(&original).await.unwrap();
    store
        .apply_reaction_event(
            "c",
            &original.server_id,
            "u",
            "👍",
            ReactionAction::Add as i32,
            Some(10),
        )
        .await
        .unwrap();
    let mut old = store.get(&original.server_id).await.unwrap().unwrap();
    old.attributes.remove("lastReactionEventSeq:u:👍");
    old.apply_reaction_change("other", "👍", ReactionAction::Add as i32);
    store
        .apply_reaction_event(
            "c",
            &original.server_id,
            "u",
            "👍",
            ReactionAction::Remove as i32,
            Some(20),
        )
        .await
        .unwrap();
    store.save_one(&old).await.unwrap();
    let reactions = store
        .list_reactions(std::slice::from_ref(&original.server_id))
        .await
        .unwrap();
    let reactions = &reactions[&original.server_id];
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0].user_ids, vec!["other"]);
    assert_eq!(
        store
            .apply_reaction_event(
                "c",
                &original.server_id,
                "u",
                "👍",
                ReactionAction::Remove as i32,
                Some(20)
            )
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    store
        .apply_reaction_event(
            "c",
            &original.server_id,
            "u",
            "👍",
            ReactionAction::Add as i32,
            Some(30),
        )
        .await
        .unwrap();
    let mut newer = store.get(&original.server_id).await.unwrap().unwrap();
    newer.apply_reaction_change("u", "👍", ReactionAction::Remove as i32);
    newer
        .attributes
        .insert("lastReactionEventSeq:u:👍".into(), "40".into());
    store.save_one(&newer).await.unwrap();
    assert_eq!(
        store
            .list_reactions(std::slice::from_ref(&original.server_id))
            .await
            .unwrap()[&original.server_id][0]
            .user_ids,
        vec!["other"]
    );

    let mut marked = message();
    marked.server_id = "mark-snapshot".into();
    marked.client_msg_id = "mark-snapshot-local".into();
    marked
        .attributes
        .insert("lastMarkEventSeq:2".into(), "10".into());
    marked.attributes.insert("markType".into(), "2".into());
    store.save_one(&marked).await.unwrap();
    store
        .apply_mark_event(&marked.server_id, 3, Some("green"), true, Some(30))
        .await
        .unwrap();
    assert_eq!(
        store
            .apply_mark_event(&marked.server_id, 2, None, false, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    store.save_one(&marked).await.unwrap();
    let actual = store.get(&marked.server_id).await.unwrap().unwrap();
    assert_eq!(
        actual.attributes.get("markType").map(String::as_str),
        Some("3")
    );
    assert_eq!(
        actual.attributes.get("markColor").map(String::as_str),
        Some("green")
    );
    assert!(
        !actual
            .attributes
            .keys()
            .any(|key| key.starts_with("lastMarkEventSeq:"))
    );
    store
        .apply_mark_event(&marked.server_id, 2, None, false, Some(40))
        .await
        .unwrap();
    store
        .clear_message_mark(&marked.server_id, 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&marked.server_id)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .get("markType")
            .map(String::as_str),
        Some("3")
    );
    store
        .clear_message_mark(&marked.server_id, 3)
        .await
        .unwrap();
    store.save_one(&marked).await.unwrap();
    assert!(
        !store
            .get(&marked.server_id)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .contains_key("markType")
    );
}

async fn canonical_ids_do_not_mutate_a_client_id_alias(store: &dyn MessageStore) {
    let mut first = message();
    first.server_id = "11111111-1111-4111-8111-111111111111".into();
    first.client_msg_id = "first-client".into();
    first.conversation_id = "first-conversation".into();
    let mut other = message();
    other.server_id = "22222222-2222-4222-8222-222222222222".into();
    other.client_msg_id = first.server_id.clone();
    other.conversation_id = "other-conversation".into();
    store
        .save_batch(&[first.clone(), other.clone()])
        .await
        .unwrap();
    store
        .apply_reaction_event(
            &first.conversation_id,
            &first.server_id,
            "first-user",
            "👍",
            1,
            Some(5),
        )
        .await
        .unwrap();
    store
        .apply_reaction_event(
            &other.conversation_id,
            &other.server_id,
            "other-user",
            "👍",
            1,
            Some(5),
        )
        .await
        .unwrap();
    let reactions = store
        .list_reactions(&[first.server_id.clone(), other.server_id.clone()])
        .await
        .unwrap();
    assert_eq!(reactions[&first.server_id][0].user_ids, vec!["first-user"]);
    assert_eq!(reactions[&other.server_id][0].user_ids, vec!["other-user"]);
    let replacement = crate::content::ContentBuilder::text("edited first")
        .build()
        .encode();
    store
        .apply_edit_event(&first.server_id, replacement.clone(), Some(20))
        .await
        .unwrap();
    store
        .apply_mark_event(&first.server_id, 2, None, true, Some(10))
        .await
        .unwrap();
    store
        .apply_pin_event(&first.server_id, true, Some(10))
        .await
        .unwrap();
    store
        .update_status(
            &first.server_id,
            flare_proto::common::MessageStatus::Sent as i32,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&first.server_id)
            .await
            .unwrap()
            .unwrap()
            .encoded_content,
        replacement
    );
    let purged = MessageRetentionState {
        lifecycle: 5,
        content_visibility: 4,
        ..Default::default()
    };
    store
        .apply_retention_purged_event(&first.server_id, &purged, 30, Some(30))
        .await
        .unwrap();
    let actual = store.get(&other.server_id).await.unwrap().unwrap();
    assert_eq!(actual.encoded_content, other.encoded_content);
    assert_eq!(actual.status, other.status);
    assert!(actual.retention_state.is_none());
    assert!(!actual.attributes.contains_key("markType"));
    assert!(!actual.attributes.contains_key("pinned"));
    store
        .update_after_ack(&other.client_msg_id, &other)
        .await
        .unwrap();
    assert!(store.get(&first.server_id).await.unwrap().is_some());
    assert!(
        store
            .get(&other.server_id)
            .await
            .unwrap()
            .unwrap()
            .retention_state
            .is_none()
    );
    store.delete(&first.server_id).await.unwrap();
    assert!(store.get(&first.server_id).await.unwrap().is_none());
    store.delete(&first.server_id).await.unwrap();
    assert_eq!(
        store
            .apply_edit_event(&first.server_id, replacement, Some(40))
            .await
            .unwrap(),
        crate::domain::EditApplyResult::NotFound
    );
    assert!(
        !store
            .list_reactions(std::slice::from_ref(&first.server_id))
            .await
            .unwrap()
            .contains_key(&first.server_id)
    );
    assert_eq!(
        store
            .get(&other.server_id)
            .await
            .unwrap()
            .unwrap()
            .encoded_content,
        other.encoded_content
    );
}

#[tokio::test]
async fn memory_snapshot_operations_and_id_namespaces() {
    let stores = super::memory_im::in_memory_im_provider();
    snapshots_preserve_ordered_operations(stores.messages.as_ref()).await;
    canonical_ids_do_not_mutate_a_client_id_alias(stores.messages.as_ref()).await;
}

#[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
#[tokio::test]
async fn sqlite_snapshot_operations_and_id_namespaces_survive_reopening() {
    use crate::shared::util::sqlite_store::open_sqlite_store_for_user;
    let root = std::env::temp_dir().join(format!(
        "flare-operation-snapshots-{}",
        uuid::Uuid::new_v4()
    ));
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    snapshots_preserve_ordered_operations(stores.messages.as_ref()).await;
    canonical_ids_do_not_mutate_a_client_id_alias(stores.messages.as_ref()).await;
    drop(stores);
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    assert_eq!(
        stores
            .messages
            .apply_mark_event("mark-snapshot", 2, None, true, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert_eq!(
        stores
            .messages
            .apply_reaction_event(
                "c",
                "reaction-snapshot",
                "u",
                "👍",
                flare_proto::common::ReactionAction::Add as i32,
                Some(30)
            )
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert!(
        !stores
            .messages
            .get("mark-snapshot")
            .await
            .unwrap()
            .unwrap()
            .attributes
            .contains_key("markType")
    );
    assert_eq!(
        stores
            .messages
            .list_reactions(&["reaction-snapshot".into()])
            .await
            .unwrap()["reaction-snapshot"][0]
            .user_ids,
        vec!["other"]
    );
    drop(stores);
    tokio::fs::remove_dir_all(root).await.unwrap();
}

async fn batch_terminal_and_pending_identity(store: &dyn MessageStore) {
    for existing in [false, true] {
        for reverse in [false, true] {
            let mut original = message();
            original.server_id = format!("batch-{existing}-{reverse}");
            if existing {
                store.save_one(&original).await.unwrap();
            }
            let mut purged = original.clone();
            purged.retention_state = Some(MessageRetentionState {
                lifecycle: MessageRetentionLifecycle::Purged as i32,
                content_visibility: ContentVisibility::Purged as i32,
                ..Default::default()
            });
            let mut rows = (0..21)
                .map(|n| {
                    let mut m = message();
                    m.server_id = format!("padding-{existing}-{reverse}-{n}");
                    m
                })
                .collect::<Vec<_>>();
            rows.push(if reverse {
                original.clone()
            } else {
                purged.clone()
            });
            rows.push(if reverse { purged } else { original.clone() });
            store.save_batch(&rows).await.unwrap();
            let saved = store.get(&original.server_id).await.unwrap().unwrap();
            assert!(saved.encoded_content.is_empty());
            assert_eq!(
                saved.retention_state.unwrap().lifecycle,
                MessageRetentionLifecycle::Purged as i32
            );
        }
    }
    for (n, conversation, sender) in [(0, "other-conversation", "u"), (1, "c", "other-user")] {
        let mut pending = message();
        pending.server_id = format!("pending-{n}");
        pending.client_msg_id = format!("client-{n}");
        pending.local_state.is_local = true;
        pending.local_state.sending = true;
        pending.conversation_seq = 0;
        store.save_one(&pending).await.unwrap();
        let mut remote = pending.clone();
        remote.server_id = format!("remote-{n}");
        remote.conversation_id = conversation.into();
        remote.sender_id = sender.into();
        remote.local_state = Default::default();
        store.save_one(&remote).await.unwrap();
        assert!(store.get(&pending.server_id).await.unwrap().is_some());
        let mut accepted = pending.clone();
        accepted.server_id = format!("accepted-{n}");
        accepted.local_state = Default::default();
        store.save_one(&accepted).await.unwrap();
        assert!(store.get(&pending.server_id).await.unwrap().is_none());
        assert_eq!(
            store
                .get(&remote.server_id)
                .await
                .unwrap()
                .unwrap()
                .sender_id,
            sender
        );
    }
}

#[tokio::test]
async fn memory_batch_terminal_and_pending_identity() {
    batch_terminal_and_pending_identity(&super::memory_im::MemoryMessageStore::new()).await;
}

#[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
#[tokio::test]
async fn sqlite_batch_terminal_and_pending_identity_survive_restart() {
    use crate::shared::util::sqlite_store::open_sqlite_store_for_user;
    let root = std::env::temp_dir().join(format!("flare-batch-terminal-{}", uuid::Uuid::new_v4()));
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    batch_terminal_and_pending_identity(stores.messages.as_ref()).await;
    drop(stores);
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    assert!(
        stores
            .messages
            .get("batch-false-false")
            .await
            .unwrap()
            .unwrap()
            .encoded_content
            .is_empty()
    );
    assert!(stores.messages.get("remote-0").await.unwrap().is_some());
    drop(stores);
    std::fs::remove_dir_all(root).unwrap();
}

async fn authoritative_edit_snapshots(store: &dyn MessageStore) {
    let original = message();
    store.save_one(&original).await.unwrap();
    store
        .apply_edit_event(
            "m",
            crate::content::ContentBuilder::text("first edit")
                .build()
                .encode(),
            Some(10),
        )
        .await
        .unwrap();
    let latest = IMMessage::new(flare_proto::common::Message {
        server_id: "m".into(),
        client_msg_id: "local".into(),
        conversation_id: "c".into(),
        sender_id: "u".into(),
        conversation_seq: 1,
        last_edit_seq: 20,
        content: Some({
            use prost::Message;
            flare_proto::common::MessageContent::decode(
                crate::content::ContentBuilder::text("latest edit")
                    .build()
                    .encode()
                    .as_slice(),
            )
            .unwrap()
        }),
        ..Default::default()
    });
    store.save_one(&latest).await.unwrap();
    store.save_one(&original).await.unwrap();
    let got = store.get("m").await.unwrap().unwrap();
    assert_eq!(got.encoded_content, latest.encoded_content);
    assert_eq!(got.attributes["lastEditEventSeq"], "20");
    assert_eq!(
        store
            .apply_edit_event("m", original.encoded_content, Some(15))
            .await
            .unwrap(),
        crate::domain::EditApplyResult::IgnoredStale
    );
}

#[tokio::test]
async fn memory_authoritative_edit_snapshots() {
    authoritative_edit_snapshots(&super::memory_im::MemoryMessageStore::new()).await;
}

#[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
#[tokio::test]
async fn sqlite_authoritative_edit_snapshots_survive_restart() {
    use crate::shared::util::sqlite_store::open_sqlite_store_for_user;
    let root = std::env::temp_dir().join(format!("flare-edit-snapshot-{}", uuid::Uuid::new_v4()));
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    authoritative_edit_snapshots(stores.messages.as_ref()).await;
    drop(stores);
    let stores = open_sqlite_store_for_user(&root, "core", "a", "u")
        .await
        .unwrap();
    assert_eq!(
        stores.messages.get("m").await.unwrap().unwrap().attributes["lastEditEventSeq"],
        "20"
    );
    drop(stores);
    std::fs::remove_dir_all(root).unwrap();
}

async fn reaction_requires_the_message_conversation(store: &dyn MessageStore) {
    store.save_one(&message()).await.unwrap();
    assert_eq!(
        store
            .apply_reaction_event("other", "m", "u", "👍", 1, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::NotFound
    );
    assert!(store.get("m").await.unwrap().unwrap().reactions.is_empty());
    assert_eq!(
        store
            .apply_reaction_event("c", "m", "u", "👍", 1, Some(20))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
}

#[tokio::test]
async fn memory_reaction_requires_the_message_conversation() {
    reaction_requires_the_message_conversation(&super::memory_im::MemoryMessageStore::new()).await;
}

#[cfg(all(feature = "lifecycle-sqlite", not(target_arch = "wasm32")))]
#[tokio::test]
async fn sqlite_reaction_requires_the_message_conversation() {
    let root = std::env::temp_dir().join(format!("flare-reaction-scope-{}", uuid::Uuid::new_v4()));
    let stores =
        crate::shared::util::sqlite_store::open_sqlite_store_for_user(&root, "core", "a", "u")
            .await
            .unwrap();
    reaction_requires_the_message_conversation(stores.messages.as_ref()).await;
    drop(stores);
    std::fs::remove_dir_all(root).unwrap();
}
