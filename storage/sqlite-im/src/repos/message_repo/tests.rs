use super::super::SqliteConversationRepo;
use super::super::init_schema as sqlite_init_schema;
use super::SqliteMessageRepo;
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::model::message::{MessageStatus, ReactionAction};
use flare_im_core_sdk::model::{
    Conversation, IMMessage, MessageSearchKind, MessageSearchQuery, MessageType,
};
use flare_im_core_sdk::storage::{
    ConversationReader, ConversationWriter, EditApplyResult, MessageReader, MessageStore,
    MessageWriter, OperationApplyResult,
};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tokio::time::{Duration, sleep};

async fn make_repo() -> SqliteMessageRepo {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    SqliteMessageRepo::new(pool)
}

fn text_message(
    server_id: &str,
    conversation_id: &str,
    sender_id: &str,
    conversation_seq: u64,
    created_at: u64,
    text: &str,
) -> IMMessage {
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = server_id.to_string();
    message.client_msg_id = format!("client-{server_id}");
    message.conversation_id = conversation_id.to_string();
    message.sender_id = sender_id.to_string();
    message.conversation_seq = conversation_seq;
    message.created_at = created_at;
    message.client_created_at = created_at;
    message.content = Some(flare_im_core_sdk::model::Elem::Text(
        flare_im_core_sdk::content::message_elem::TextElem {
            text: text.to_string(),
            mentions: Vec::new(),
        },
    ));
    message.materialize_encoded_content_from_elem();
    message
}

#[tokio::test]
async fn public_text_send_does_not_publish_an_upload_before_enqueue() {
    use flare_im_core_sdk::client::IMClient;
    use flare_im_core_sdk::prelude::SdkConfigOverlay;
    let root = std::env::temp_dir().join(format!("flare-first-send-{}", uuid::Uuid::new_v4()));
    let client = IMClient::new();
    client
        .init(
            Some("development".into()),
            Some(SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                tenant_id: Some("t".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    client
        .prepare("me", super::super::super::sqlite())
        .await
        .unwrap();
    let path =
        super::super::super::provider::resolve_user_db_path(&root, "flare-im-core-sdk", "t", "me");
    let pool = SqlitePool::connect_with(SqliteConnectOptions::new().filename(path))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_enqueue BEFORE INSERT ON pending_sends BEGIN SELECT RAISE(ABORT, 'fail'); END").execute(&pool).await.unwrap();
    let message = client
        .message_build()
        .unwrap()
        .create_text("group-new", "first", false, &[])
        .await
        .unwrap();
    let api = client.message_async().await.unwrap();
    assert!(api.send(message.clone()).await.is_err());
    let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM conversations), (SELECT COUNT(*) FROM pending_sends)").fetch_one(&pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    sqlx::query("DROP TRIGGER reject_enqueue")
        .execute(&pool)
        .await
        .unwrap();
    api.send(message).await.unwrap();
    let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM conversations), (SELECT COUNT(*) FROM pending_sends)").fetch_one(&pool).await.unwrap();
    assert_eq!(counts, (1, 1, 1));
    drop(api);
    drop(client);
    pool.close().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// Fail each physical write and read the old state through a fresh connection.
#[tokio::test]
async fn outgoing_commit_is_atomic_after_reopen_and_own_replay() {
    use flare_im_core_sdk::storage::{OutgoingSendStore, PendingSendVo};
    for completing in [false, true] {
        for table in ["messages", "conversations", "pending_sends"] {
            let path = std::env::temp_dir().join(format!("flare-send-{}.db", uuid::Uuid::new_v4()));
            let options = SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(options.clone()).await.unwrap();
            sqlite_init_schema(&pool).await.unwrap();
            let repo = SqliteMessageRepo::new(pool.clone());
            let mut local = text_message("local", "conv", "me", 0, 100, "hello");
            local.client_msg_id = local.server_id.clone();
            local.local_state.is_local = true;
            local.local_state.sending = true;
            let entry = PendingSendVo {
                client_msg_id: local.client_msg_id.clone(),
                conversation_id: local.conversation_id.clone(),
                message: local.clone(),
                enqueued_at_ms: 100,
            };
            let mut confirmed = local.clone();
            confirmed.server_id = "canonical".into();
            confirmed.conversation_seq = 7;
            confirmed.local_state = Default::default();
            if completing {
                repo.enqueue_send(&local, &entry).await.unwrap();
            }
            let operation = if completing && table == "pending_sends" {
                "DELETE"
            } else {
                "INSERT"
            };
            let trigger = format!(
                "CREATE TRIGGER inject_fault BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'fault'); END"
            );
            sqlx::query(&trigger).execute(&pool).await.unwrap();
            let failed = if completing {
                repo.complete_send(&confirmed).await.map(|_| ())
            } else {
                repo.enqueue_send(&local, &entry).await
            };
            assert!(failed.is_err(), "{operation} on {table} did not abort");
            pool.close().await;

            let pool = SqlitePool::connect_with(options.clone()).await.unwrap();
            let repo = SqliteMessageRepo::new(pool.clone());
            let summary = SqliteConversationRepo::new(pool.clone())
                .get("conv")
                .await
                .unwrap();
            let recovery: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_sends")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(recovery, i64::from(completing));
            assert_eq!(repo.get("local").await.unwrap().is_some(), completing);
            assert!(repo.get("canonical").await.unwrap().is_none());
            assert_eq!(
                summary.and_then(|c| c.last_message_id),
                completing.then(|| "local".to_owned())
            );
            sqlx::query("DROP TRIGGER inject_fault")
                .execute(&pool)
                .await
                .unwrap();
            if !completing {
                repo.enqueue_send(&local, &entry).await.unwrap();
            }
            // The server summary may already advertise a newer waterline while
            // its visible preview still points to this optimistic client row.
            sqlx::query("UPDATE conversations SET max_seq = 99")
                .execute(&pool)
                .await
                .unwrap();
            repo.save_batch(std::slice::from_ref(&confirmed))
                .await
                .unwrap();
            repo.complete_send(&confirmed).await.unwrap();
            repo.complete_send(&confirmed).await.unwrap();
            assert!(repo.enqueue_send(&local, &entry).await.is_err());
            let failed = flare_im_core_sdk::storage::MessageDeliveryService::mark_failed(&local);
            assert!(!repo.complete_send(&failed).await.unwrap());
            pool.close().await;
            let pool = SqlitePool::connect_with(options).await.unwrap();
            let state: (i64, String, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM messages), last_message_id, (SELECT COUNT(*) FROM pending_sends) FROM conversations").fetch_one(&pool).await.unwrap();
            assert_eq!(state, (1, "canonical".into(), 0));
            pool.close().await;
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[tokio::test]
async fn save_batch_preserves_typed_thread_id() {
    let repo = make_repo().await;
    let mut message = text_message("thread-reply-1", "conv-thread", "u1", 1, 1_000, "reply");
    message.thread_id = Some("thread-root-1".to_string());

    repo.save_batch(&[message]).await.unwrap();

    let stored = repo
        .get_by_message_ids(&["thread-reply-1".to_string()])
        .await
        .unwrap()
        .pop()
        .expect("message should be stored");
    assert_eq!(stored.thread_id.as_deref(), Some("thread-root-1"));
}

#[tokio::test]
async fn update_status_reports_missing_target_and_applies_when_present() {
    let repo = make_repo().await;

    // 本地尚无该消息：必须反馈 0 行，供调用方保留事件重放（Recall 先于消息到达）。
    let affected = repo
        .update_status("missing-server-id", MessageStatus::Recalled as i32)
        .await
        .unwrap();
    assert_eq!(affected, 0);

    let message = text_message("server-recall-1", "conv-recall", "u1", 1, 1_000, "hello");
    repo.save_batch(&[message]).await.unwrap();

    let affected = repo
        .update_status("server-recall-1", MessageStatus::Recalled as i32)
        .await
        .unwrap();
    assert!(affected > 0);
    let stored = repo
        .get("server-recall-1")
        .await
        .unwrap()
        .expect("message should be stored");
    assert!(stored.is_recalled);
    assert_eq!(stored.status, MessageStatus::Recalled as i32);
}

#[tokio::test]
async fn reopened_history_preserves_recall_card_origin_and_unknown_payload() {
    use flare_proto::common::{
        AppCardContent, CustomContent, Message, MessageContent, message_content::Content,
    };

    let path = std::env::temp_dir().join(format!("flare-history-{}.db", uuid::Uuid::new_v4()));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options.clone()).await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let repo = SqliteMessageRepo::new(pool.clone());
    let message = |id: &str, content| {
        IMMessage::new(Message {
            server_id: id.into(),
            client_msg_id: format!("client-{id}"),
            conversation_id: "conv".into(),
            sender_id: "peer".into(),
            content: Some(MessageContent {
                content: Some(content),
            }),
            ..Default::default()
        })
    };
    let card = message("card", Content::AppCard(AppCardContent {
        app_id: "example.poll".into(),
        card_type: "flare.plugin.message.v1".into(),
        payload: br#"{"pluginId":"example.poll","contributionId":"example.poll.card","schemaVersion":1,"summary":"Poll","payload":{"pollId":"p"}}"#.to_vec(),
        ..Default::default()
    }));
    let unknown = message(
        "unknown",
        Content::Custom(CustomContent {
            r#type: "example.future.v19".into(),
            payload: vec![0, 255, 128, 1],
            ..Default::default()
        }),
    );
    let unknown_bytes = unknown.encoded_content.clone();
    assert!(!unknown_bytes.is_empty());
    let recalled = text_message("recalled", "conv", "peer", 3, 1000, "text");
    repo.save_batch(&[card, unknown, recalled]).await.unwrap();
    assert_eq!(
        repo.update_status("recalled", MessageStatus::Recalled as i32)
            .await
            .unwrap(),
        1
    );
    drop(repo);
    pool.close().await;

    let pool = SqlitePool::connect_with(options).await.unwrap();
    let repo = SqliteMessageRepo::new(pool.clone());
    let recalled = repo.get("recalled").await.unwrap().unwrap();
    assert!(recalled.is_recalled);
    assert_eq!(recalled.status, MessageStatus::Recalled as i32);
    let card = repo.get("card").await.unwrap().unwrap();
    assert_eq!(
        card.plugin_message().unwrap().contribution_id,
        "example.poll.card"
    );
    let unknown = repo.get("unknown").await.unwrap().unwrap();
    assert!(unknown.plugin_message().is_none());
    assert_eq!(unknown.encoded_content, unknown_bytes);
    assert!(matches!(unknown.to_proto().content.and_then(|c| c.content),
        Some(Content::Custom(custom)) if custom.r#type == "example.future.v19" && custom.payload == [0, 255, 128, 1]));
    drop(repo);
    pool.close().await;
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn get_by_conversation_repairs_single_chat_channel_alias_messages() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let message_repo = SqliteMessageRepo::new(pool.clone());
    let conversation_repo = SqliteConversationRepo::new(pool.clone());

    let mut conversation = Conversation::from_conversation_id("cid-canonical".to_string());
    conversation.conversation_type = ConversationType::Single;
    conversation.channel_id = "peer-12".to_string();
    conversation.display_name = "peer-12".to_string();
    conversation.unread_count = 17;
    conversation.max_seq = 17;
    conversation_repo.save_one(&conversation).await.unwrap();

    let mut message = text_message(
        "server-alias-window",
        "peer-12",
        "peer-12",
        17,
        17_000,
        "hello-from-peer",
    );
    message.conversation_type = ConversationType::Single.to_proto_int();
    message.channel_id = "peer-12".to_string();
    message_repo.save_one(&message).await.unwrap();

    let messages = message_repo
        .get_by_conversation("cid-canonical", 0, 20)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].conversation_id, "cid-canonical");
    assert_eq!(messages[0].server_id, "server-alias-window");

    assert!(
        message_repo
            .get_by_conversation("peer-12", 0, 20)
            .await
            .unwrap()
            .is_empty()
    );
}

// 测试辅助构造器，参数与被测消息字段一一对应；包成结构体反而更难读。
#[allow(clippy::too_many_arguments)]
fn file_message(
    server_id: &str,
    conversation_id: &str,
    sender_id: &str,
    conversation_seq: u64,
    created_at: u64,
    file_name: &str,
    mime_type: &str,
    description: &str,
) -> IMMessage {
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = server_id.to_string();
    message.client_msg_id = format!("client-{server_id}");
    message.conversation_id = conversation_id.to_string();
    message.sender_id = sender_id.to_string();
    message.conversation_seq = conversation_seq;
    message.created_at = created_at;
    message.client_created_at = created_at;
    message.message_type = MessageType::File as i32;
    message.content = Some(flare_im_core_sdk::model::Elem::File(
        flare_im_core_sdk::content::message_elem::FileElem {
            file_id: format!("file-{server_id}"),
            file_name: file_name.to_string(),
            mime_type: mime_type.to_string(),
            file_size: 1024,
            url: String::new(),
            description: description.to_string(),
        },
    ));
    message.materialize_encoded_content_from_elem();
    message
}

#[tokio::test]
async fn conversation_projection_uses_seq_over_timestamp_for_server_messages() {
    let repo = make_repo().await;
    let older_seq_future_time = text_message(
        "server-10",
        "conv-order",
        "u2",
        10,
        2_000,
        "older conversation_seq",
    );
    let newer_seq_past_time = text_message(
        "server-11",
        "conv-order",
        "u2",
        11,
        1_000,
        "newer conversation_seq",
    );

    repo.save_batch(&[older_seq_future_time]).await.unwrap();
    repo.save_batch(&[newer_seq_past_time]).await.unwrap();

    let conversations = SqliteConversationRepo::new(repo.pool.clone());
    let conversation = conversations
        .get("conv-order")
        .await
        .unwrap()
        .expect("conversation snapshot");

    assert_eq!(conversation.max_seq, 11);
    assert_eq!(conversation.last_message_id.as_deref(), Some("server-11"));
    assert_eq!(conversation.last_sender_id.as_deref(), Some("u2"));
}

#[tokio::test]
async fn deleting_last_message_rebuilds_conversation_preview_from_previous_visible_message() {
    let repo = make_repo().await;
    let first = text_message("server-del-1", "conv-delete", "u2", 1, 1_000, "first");
    let second = text_message("server-del-2", "conv-delete", "u2", 2, 2_000, "second");
    repo.save_batch(&[first, second]).await.unwrap();

    repo.delete("server-del-2").await.unwrap();

    let conversations = SqliteConversationRepo::new(repo.pool.clone());
    let conversation = conversations
        .get("conv-delete")
        .await
        .unwrap()
        .expect("conversation snapshot");

    assert_eq!(conversation.max_seq, 2);
    assert_eq!(
        conversation.last_message_id.as_deref(),
        Some("server-del-1")
    );
    assert_eq!(conversation.last_message_at, Some(1_000));
}

#[tokio::test]
async fn pending_local_message_updates_conversation_preview_with_client_time() {
    let repo = make_repo().await;
    let server = text_message(
        "server-pending-base",
        "conv-pending",
        "u2",
        7,
        1_000,
        "server",
    );
    let mut pending = text_message("", "conv-pending", "u1", 0, 0, "pending");
    pending.client_msg_id = "client-pending-new".to_string();
    pending.client_created_at = 5_000;
    pending.local_state.sending = true;
    pending.local_state.is_local = true;

    repo.save_batch(&[server]).await.unwrap();
    repo.save_batch(&[pending]).await.unwrap();

    let conversations = SqliteConversationRepo::new(repo.pool.clone());
    let conversation = conversations
        .get("conv-pending")
        .await
        .unwrap()
        .expect("conversation snapshot");

    assert_eq!(
        conversation.last_message_id.as_deref(),
        Some("client-pending-new")
    );
    assert!(conversation.last_message_at.unwrap_or_default() >= 5_000);
    assert!(
        conversation
            .last_message_preview
            .as_deref()
            .unwrap_or_default()
            .contains("pending")
    );
}

#[tokio::test]
async fn advanced_search_filters_conversation_sender_time_and_media_kind() {
    let repo = make_repo().await;
    let text = text_message("server-search-text", "conv-search", "u2", 1, 1_000, "alpha");
    let mut image = text_message(
        "server-search-image",
        "conv-search",
        "u3",
        2,
        2_000,
        "alpha image",
    );
    image.message_type = MessageType::Image as i32;
    let mut other_sender = text_message(
        "server-search-other",
        "conv-search",
        "u4",
        3,
        3_000,
        "alpha other",
    );
    other_sender.message_type = MessageType::Image as i32;
    let other_conversation = text_message(
        "server-search-foreign",
        "conv-foreign",
        "u3",
        4,
        4_000,
        "alpha image",
    );
    repo.save_batch(&[text, image, other_sender, other_conversation])
        .await
        .unwrap();

    let results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("alpha".to_string()),
            conversation_id: Some("conv-search".to_string()),
            sender_id: Some("u3".to_string()),
            from_time: Some(1_500),
            to_time: Some(2_500),
            kinds: vec![MessageSearchKind::Media],
            limit: 20,
            include_recalled: false,
        })
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].server_id, "server-search-image");

    let wildcard_results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("%".to_string()),
            limit: 20,
            ..MessageSearchQuery::default()
        })
        .await
        .unwrap();
    assert!(wildcard_results.is_empty());
}

#[tokio::test]
async fn file_search_matches_typed_file_fields() {
    let repo = make_repo().await;
    let file = file_message(
        "server-file-search",
        "conv-file-search",
        "u2",
        1,
        1_000,
        "合同终稿.pdf",
        "application/pdf",
        "Q2 procurement contract",
    );
    let text = text_message(
        "server-file-search-text",
        "conv-file-search",
        "u2",
        2,
        2_000,
        "合同终稿",
    );
    repo.save_batch(&[file, text]).await.unwrap();

    let file_name_results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("合同".to_string()),
            kinds: vec![MessageSearchKind::File],
            limit: 10,
            ..MessageSearchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(file_name_results.len(), 1);
    assert_eq!(file_name_results[0].server_id, "server-file-search");

    let mime_results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("application/pdf".to_string()),
            kinds: vec![MessageSearchKind::File],
            limit: 10,
            ..MessageSearchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(mime_results.len(), 1);
    assert_eq!(mime_results[0].server_id, "server-file-search");

    let media_results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("procurement".to_string()),
            kinds: vec![MessageSearchKind::Media],
            limit: 10,
            ..MessageSearchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(media_results.len(), 1);
    assert_eq!(media_results[0].server_id, "server-file-search");
}

#[tokio::test]
async fn file_search_index_tracks_content_update() {
    let repo = make_repo().await;
    let original = file_message(
        "server-file-update",
        "conv-file-update",
        "u2",
        1,
        1_000,
        "old-plan.pdf",
        "application/pdf",
        "legacy attachment",
    );
    repo.save_batch(&[original]).await.unwrap();

    let updated = file_message(
        "server-file-update",
        "conv-file-update",
        "u2",
        1,
        2_000,
        "new-roadmap.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "launch budget sheet",
    );
    assert!(
        repo.update_content("server-file-update", updated.encoded_content)
            .await
            .unwrap()
    );

    let old_results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("old-plan".to_string()),
            kinds: vec![MessageSearchKind::File],
            limit: 10,
            ..MessageSearchQuery::default()
        })
        .await
        .unwrap();
    assert!(old_results.is_empty());

    let new_results = repo
        .search_by_query(&MessageSearchQuery {
            keyword: Some("roadmap".to_string()),
            kinds: vec![MessageSearchKind::File],
            limit: 10,
            ..MessageSearchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(new_results.len(), 1);
    assert_eq!(new_results[0].server_id, "server-file-update");
}

#[tokio::test]
async fn short_keyword_search_uses_content_fallback_without_metadata_matches() {
    let repo = make_repo().await;
    let content = text_message(
        "server-short-content",
        "conv-short",
        "u2",
        1,
        1_000,
        "生产事故复盘",
    );
    let mut metadata_only = text_message(
        "server-short-metadata",
        "conv-short",
        "u3",
        2,
        2_000,
        "普通通知",
    );
    metadata_only
        .attributes
        .insert("debugNote".to_string(), "事故".to_string());
    repo.save_batch(&[content, metadata_only]).await.unwrap();

    let results = repo.search("事故", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].server_id, "server-short-content");

    sqlx::query(
        r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, attributes, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', NULL, ?, ?, ?)"#,
    )
    .bind("server-short-content-text")
    .bind("conv-short")
    .bind("client-short-content-text")
    .bind("u4")
    .bind(3_i64)
    .bind(3_000_i64)
    .bind(3_000_i64)
    .bind(1_i32)
    .bind(MessageType::Text as i32)
    .bind("u4")
    .bind(r#"{"contentText":"短词内容","debugNote":"ignored"}"#)
    .bind(3_000_i64)
    .bind(3_000_i64)
    .execute(&repo.pool)
    .await
    .unwrap();

    let content_text_results = repo.search("短词", 10).await.unwrap();
    assert_eq!(content_text_results.len(), 1);
    assert_eq!(
        content_text_results[0].server_id,
        "server-short-content-text"
    );
}

/// 消息落库顺带建出的会话不带名字：消息里只有对端、群、会话的 id。
#[tokio::test]
async fn a_conversation_created_by_saving_a_message_stores_no_name() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let message_repo = SqliteMessageRepo::new(pool.clone());
    let conversation_repo = SqliteConversationRepo::new(pool.clone());
    let mut message = text_message("server-new-1", "1A_new", "peer-7", 1, 1_000, "hi");
    message.conversation_type = ConversationType::Single.to_proto_int();
    message.channel_id = "peer-7".to_string();
    message.sender_name = "Peer at send time".to_string();
    message_repo.save_one(&message).await.unwrap();
    let created = conversation_repo
        .get("1A_new")
        .await
        .unwrap()
        .expect("conversation created");
    assert_eq!(created.channel_id, "peer-7");
    assert_eq!(created.display_name, "");
}

#[tokio::test]
async fn init_schema_backfills_fts_for_existing_messages() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    sqlx::query(
        r#"INSERT INTO messages (
                   server_id, conversation_id, client_msg_id, sender_id, conversation_seq,
                   created_at, client_created_at, conversation_type, message_type,
                   channel_id, encoded_content, text, sort_ts, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, X'', ?, ?, ?)"#,
    )
    .bind("server-fts-legacy")
    .bind("conv-fts")
    .bind("client-fts-legacy")
    .bind("u2")
    .bind(1_i64)
    .bind(1_000_i64)
    .bind(1_000_i64)
    .bind(1_i32)
    .bind(MessageType::Text as i32)
    .bind("u2")
    .bind("legacy searchable payload")
    .bind(1_000_i64)
    .bind(1_000_i64)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM messages_fts")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("PRAGMA user_version = 0")
        .execute(&pool)
        .await
        .unwrap();

    sqlite_init_schema(&pool).await.unwrap();
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(version, 7);

    let repo = SqliteMessageRepo::new(pool);
    let results = repo.search("searchable", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].server_id, "server-fts-legacy");
}

#[tokio::test]
async fn fts_search_tracks_save_update_and_delete() {
    let repo = make_repo().await;
    let message = text_message(
        "server-fts-live",
        "conv-fts-live",
        "u2",
        1,
        1_000,
        "生产事故复盘",
    );
    repo.save_batch(&[message]).await.unwrap();

    let saved = repo.search("生产事故", 10).await.unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].server_id, "server-fts-live");

    let updated = text_message(
        "server-fts-live",
        "conv-fts-live",
        "u2",
        1,
        2_000,
        "索引更新完成",
    );
    assert!(
        repo.update_content("server-fts-live", updated.encoded_content)
            .await
            .unwrap()
    );

    assert!(repo.search("生产事故", 10).await.unwrap().is_empty());
    let after_update = repo.search("索引更新", 10).await.unwrap();
    assert_eq!(after_update.len(), 1);
    assert_eq!(after_update[0].server_id, "server-fts-live");

    repo.delete("server-fts-live").await.unwrap();
    assert!(repo.search("索引更新", 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn large_save_batch_backfills_fts_after_hot_path_commit() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let repo = SqliteMessageRepo::new(pool);
    let messages = (0..33)
        .map(|index| {
            let text = if index == 32 {
                "large batch searchable needle"
            } else {
                "large batch filler"
            };
            text_message(
                &format!("server-fts-large-{index}"),
                "conv-fts-large",
                "u2",
                index as u64 + 1,
                1_000 + index as u64,
                text,
            )
        })
        .collect::<Vec<_>>();

    repo.save_batch(&messages).await.unwrap();

    let mut results = Vec::new();
    for _ in 0..20 {
        results = repo.search("needle", 10).await.unwrap();
        if !results.is_empty() {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].server_id, "server-fts-large-32");
}

#[tokio::test]
async fn deleting_client_only_pending_message_rebuilds_preview() {
    let repo = make_repo().await;
    let server = text_message(
        "server-client-delete",
        "conv-client-delete",
        "u2",
        3,
        1_000,
        "server",
    );
    let mut pending = text_message("", "conv-client-delete", "u1", 0, 2_000, "pending");
    pending.client_msg_id = "client-only-delete".to_string();
    pending.local_state.sending = true;
    pending.local_state.is_local = true;

    repo.save_batch(&[server, pending]).await.unwrap();
    repo.delete("client-only-delete").await.unwrap();

    assert!(repo.get("client-only-delete").await.unwrap().is_none());

    let conversations = SqliteConversationRepo::new(repo.pool.clone());
    let conversation = conversations
        .get("conv-client-delete")
        .await
        .unwrap()
        .expect("conversation snapshot");

    assert_eq!(
        conversation.last_message_id.as_deref(),
        Some("server-client-delete")
    );
    assert!(
        conversation
            .last_message_preview
            .as_deref()
            .unwrap_or_default()
            .contains("server")
    );
}

#[tokio::test]
async fn update_after_ack_clamps_self_echo_to_sent() {
    let repo = make_repo().await;
    let mut pending = IMMessage::new(flare_proto::common::Message::default());
    pending.server_id = "client-ack-1".to_string();
    pending.client_msg_id = "client-ack-1".to_string();
    pending.conversation_id = "conv-ack".to_string();
    pending.sender_id = "u1".to_string();
    pending.status = MessageStatus::Created as i32;
    pending.local_state.sending = true;
    pending.local_state.is_local = true;
    repo.save_batch(&[pending.clone()]).await.unwrap();

    let mut echoed = pending;
    echoed.server_id = "server-ack-1".to_string();
    echoed.conversation_seq = 11;
    echoed.status = MessageStatus::Persisted as i32;
    echoed.is_read = true;

    repo.update_after_ack("client-ack-1", &echoed)
        .await
        .unwrap();

    let stored = repo.get("server-ack-1").await.unwrap().unwrap();
    assert_eq!(stored.status, MessageStatus::Sent as i32);
    assert!(!stored.is_read);
    assert!(!stored.local_state.sending);
    assert!(!stored.local_state.is_local);
    assert!(repo.get("client-ack-1").await.unwrap().is_none());
}

#[tokio::test]
async fn save_batch_collapses_self_echo_with_same_client_msg_id() {
    let repo = make_repo().await;
    let mut pending = IMMessage::new(flare_proto::common::Message::default());
    pending.server_id = "client-dupe-1".to_string();
    pending.client_msg_id = "client-dupe-1".to_string();
    pending.conversation_id = "conv-dupe".to_string();
    pending.sender_id = "u1".to_string();
    pending.status = MessageStatus::Created as i32;
    pending.local_state.sending = true;
    pending.local_state.is_local = true;
    repo.save_batch(&[pending.clone()]).await.unwrap();

    let mut echoed = pending;
    echoed.server_id = "server-dupe-1".to_string();
    echoed.conversation_seq = 12;
    echoed.created_at = 2_000;
    echoed.status = MessageStatus::Persisted as i32;
    echoed.local_state.sending = false;
    echoed.local_state.is_local = false;
    repo.save_batch(&[echoed]).await.unwrap();

    assert!(repo.get("client-dupe-1").await.unwrap().is_none());

    let by_client = repo
        .get_by_client_msg_id("client-dupe-1")
        .await
        .unwrap()
        .expect("canonical message");
    assert_eq!(by_client.server_id, "server-dupe-1");

    let batch = repo
        .get_by_client_msg_ids(&["client-dupe-1".to_string()])
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].server_id, "server-dupe-1");

    let by_server_batch = repo
        .get_by_message_ids(&["server-dupe-1".to_string(), "missing".to_string()])
        .await
        .unwrap();
    assert_eq!(by_server_batch.len(), 1);
    assert_eq!(by_server_batch[0].server_id, "server-dupe-1");

    let timeline = repo.get_by_conversation("conv-dupe", 0, 10).await.unwrap();
    assert_eq!(timeline.len(), 1);
    assert_eq!(timeline[0].server_id, "server-dupe-1");
}

#[tokio::test]
async fn update_after_ack_collapses_noncanonical_same_client_msg_id_row() {
    let repo = make_repo().await;
    let mut stale = IMMessage::new(flare_proto::common::Message::default());
    stale.server_id = "stale-local-ack-1".to_string();
    stale.client_msg_id = "client-ack-dupe-1".to_string();
    stale.conversation_id = "conv-ack-dupe".to_string();
    stale.sender_id = "u1".to_string();
    stale.local_state.sending = true;
    stale.local_state.is_local = true;
    repo.save_batch(&[stale.clone()]).await.unwrap();

    let mut acked = stale;
    acked.server_id = "server-ack-dupe-1".to_string();
    acked.conversation_seq = 21;
    acked.status = MessageStatus::Persisted as i32;

    repo.update_after_ack("client-ack-dupe-1", &acked)
        .await
        .unwrap();

    assert!(repo.get("stale-local-ack-1").await.unwrap().is_none());
    let stored = repo.get("server-ack-dupe-1").await.unwrap().unwrap();
    assert_eq!(stored.client_msg_id, "client-ack-dupe-1");
    assert_eq!(stored.status, MessageStatus::Sent as i32);

    let timeline = repo
        .get_by_conversation("conv-ack-dupe", 0, 10)
        .await
        .unwrap();
    assert_eq!(timeline.len(), 1);
    assert_eq!(timeline[0].server_id, "server-ack-dupe-1");
}

#[tokio::test]
async fn latest_window_uses_seq_over_skewed_sort_ts_after_ack() {
    let repo = make_repo().await;
    let old = text_message("server-clock-old", "conv-clock", "u1", 10, 1_000, "old");
    let newest = text_message("server-clock-new", "conv-clock", "u2", 11, 2_000, "new");
    let mut pending = text_message("", "conv-clock", "u1", 0, 3_000, "pending");
    pending.client_msg_id = "client-clock-pending".to_string();
    pending.local_state.sending = true;
    pending.local_state.is_local = true;
    pending.local_state.sort_ts = 3_000;

    repo.save_batch(&[old, newest, pending]).await.unwrap();
    sqlx::query("UPDATE messages SET sort_ts = ? WHERE server_id = ?")
        .bind(99_999_i64)
        .bind("server-clock-old")
        .execute(&repo.pool)
        .await
        .unwrap();

    let latest = repo.get_by_conversation("conv-clock", 0, 3).await.unwrap();
    let ids = latest
        .iter()
        .map(IMMessage::timeline_key)
        .collect::<Vec<_>>();

    assert_eq!(
        ids,
        [
            "client:client-clock-pending",
            "server:server-clock-new",
            "server:server-clock-old",
        ]
    );
}

#[tokio::test]
async fn reconcile_outgoing_read_by_peer_seq_never_downgrades_read() {
    // 读态单调:偏低/陈旧的 peer_read_seq(冷启摘要可能为 0 或落后)不得把已读消息退回未读,
    // 否则「双勾过一下变单勾」。
    let repo = make_repo().await;
    let mut first = IMMessage::new(flare_proto::common::Message::default());
    first.server_id = "server-read-1".to_string();
    first.client_msg_id = "client-read-1".to_string();
    first.conversation_id = "conv-read".to_string();
    first.sender_id = "u1".to_string();
    first.conversation_seq = 1;
    first.status = MessageStatus::Sent as i32;
    first.is_read = true;

    let mut tail = first.clone();
    tail.server_id = "server-read-2".to_string();
    tail.client_msg_id = "client-read-2".to_string();
    tail.conversation_seq = 2;

    let mut other_sender = first.clone();
    other_sender.server_id = "server-read-other".to_string();
    other_sender.client_msg_id = "client-read-other".to_string();
    other_sender.sender_id = "u2".to_string();
    other_sender.conversation_seq = 3;

    repo.save_batch(&[first, tail, other_sender]).await.unwrap();

    // 用偏低的 peer_read_seq=1 reconcile:seq=2 的 tail 曾被读,不得因此回退。
    repo.reconcile_outgoing_read_by_peer_seq("conv-read", "u1", 1)
        .await
        .unwrap();

    let first = repo.get("server-read-1").await.unwrap().unwrap();
    let tail = repo.get("server-read-2").await.unwrap().unwrap();
    let other = repo.get("server-read-other").await.unwrap().unwrap();
    assert_eq!(first.status, MessageStatus::Sent as i32);
    assert!(first.is_read);
    assert_eq!(tail.status, MessageStatus::Sent as i32);
    assert!(tail.is_read);
    assert_eq!(other.status, MessageStatus::Sent as i32);
    assert!(other.is_read);
}

#[tokio::test]
async fn apply_edit_event_ignores_stale_seq_and_accepts_newer_seq() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-1".to_string();
    message.client_msg_id = "client-1".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    message.encoded_content = b"old".to_vec();
    repo.save_batch(&[message.clone()]).await.unwrap();

    // 首次编辑（seq=100）应用。
    let first = repo
        .apply_edit_event("server-1", b"first".to_vec(), Some(100))
        .await
        .unwrap();
    assert_eq!(first, EditApplyResult::Applied);
    assert_eq!(
        repo.get("server-1").await.unwrap().unwrap().encoded_content,
        b"first".to_vec()
    );

    // 陈旧/重复（seq<=100）忽略，内容不变。
    let stale = repo
        .apply_edit_event("server-1", b"stale".to_vec(), Some(100))
        .await
        .unwrap();
    assert_eq!(stale, EditApplyResult::IgnoredStale);
    assert_eq!(
        repo.get("server-1").await.unwrap().unwrap().encoded_content,
        b"first".to_vec()
    );

    // 关键回归：更高 seq 的第二次编辑必须应用（修复前因 edit_version 恒为 1 被误判陈旧丢弃）。
    let newer = repo
        .apply_edit_event("server-1", b"second".to_vec(), Some(150))
        .await
        .unwrap();
    assert_eq!(newer, EditApplyResult::Applied);
    let after_new = repo.get("server-1").await.unwrap().unwrap();
    assert_eq!(after_new.encoded_content, b"second".to_vec());
    assert_eq!(
        after_new
            .attributes
            .get("messageFsmState")
            .map(String::as_str),
        Some("EDITED")
    );
}

#[tokio::test]
async fn apply_pin_event_ignores_stale_seq_and_accepts_newer_seq() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-2".to_string();
    message.client_msg_id = "client-2".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message]).await.unwrap();

    let applied = repo
        .apply_pin_event("server-2", true, Some(10))
        .await
        .unwrap();
    assert_eq!(applied, OperationApplyResult::Applied);

    let stale = repo
        .apply_pin_event("server-2", false, Some(9))
        .await
        .unwrap();
    assert_eq!(stale, OperationApplyResult::IgnoredStale);

    let after_stale = repo.get("server-2").await.unwrap().unwrap();
    assert_eq!(
        after_stale.attributes.get("pinned").map(String::as_str),
        Some("true")
    );

    let newer = repo
        .apply_pin_event("server-2", false, Some(11))
        .await
        .unwrap();
    assert_eq!(newer, OperationApplyResult::Applied);
    let after_new = repo.get("server-2").await.unwrap().unwrap();
    assert_eq!(
        after_new.attributes.get("pinned").map(String::as_str),
        Some("false")
    );
    assert_eq!(
        after_new
            .attributes
            .get("lastPinEventSeq")
            .map(String::as_str),
        Some("11")
    );
}

#[tokio::test]
async fn save_batch_preserves_pin_event_attributes_when_snapshot_omits_them() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-2-persist".to_string();
    message.client_msg_id = "client-2-persist".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message.clone()]).await.unwrap();

    let applied = repo
        .apply_pin_event("server-2-persist", true, Some(10))
        .await
        .unwrap();
    assert_eq!(applied, OperationApplyResult::Applied);

    let mut snapshot = message.clone();
    snapshot.attributes.clear();
    snapshot.updated_at = 20;
    repo.save_batch(&[snapshot]).await.unwrap();

    let after_snapshot = repo.get("server-2-persist").await.unwrap().unwrap();
    assert_eq!(
        after_snapshot.attributes.get("pinned").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        after_snapshot
            .attributes
            .get("lastPinEventSeq")
            .map(String::as_str),
        Some("10")
    );

    let unapplied = repo
        .apply_pin_event("server-2-persist", false, Some(11))
        .await
        .unwrap();
    assert_eq!(unapplied, OperationApplyResult::Applied);

    let mut second_snapshot = message;
    second_snapshot.attributes.clear();
    second_snapshot.updated_at = 30;
    repo.save_batch(&[second_snapshot]).await.unwrap();

    let after_unpin_snapshot = repo.get("server-2-persist").await.unwrap().unwrap();
    assert_eq!(
        after_unpin_snapshot
            .attributes
            .get("pinned")
            .map(String::as_str),
        Some("false")
    );
    assert_eq!(
        after_unpin_snapshot
            .attributes
            .get("lastPinEventSeq")
            .map(String::as_str),
        Some("11")
    );
}

#[tokio::test]
async fn apply_mark_event_ignores_stale_seq_and_accepts_newer_seq() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-3".to_string();
    message.client_msg_id = "client-3".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message]).await.unwrap();

    let applied = repo
        .apply_mark_event("server-3", 7, Some("#ff0000"), true, Some(20))
        .await
        .unwrap();
    assert_eq!(applied, OperationApplyResult::Applied);

    let stale = repo
        .apply_mark_event("server-3", 7, None, false, Some(19))
        .await
        .unwrap();
    assert_eq!(stale, OperationApplyResult::IgnoredStale);

    let after_stale = repo.get("server-3").await.unwrap().unwrap();
    assert_eq!(
        after_stale.attributes.get("markType").map(String::as_str),
        Some("7")
    );
    assert_eq!(
        after_stale.attributes.get("markColor").map(String::as_str),
        Some("#ff0000")
    );

    let newer = repo
        .apply_mark_event("server-3", 7, None, false, Some(21))
        .await
        .unwrap();
    assert_eq!(newer, OperationApplyResult::Applied);

    let after_new = repo.get("server-3").await.unwrap().unwrap();
    assert!(!after_new.attributes.contains_key("markType"));
    assert!(!after_new.attributes.contains_key("markColor"));
    assert_eq!(
        after_new
            .attributes
            .get("lastMarkEventSeq")
            .map(String::as_str),
        Some("21")
    );
}

#[tokio::test]
async fn apply_reaction_event_ignores_stale_seq_and_accepts_newer_seq() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-4".to_string();
    message.client_msg_id = "client-4".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message]).await.unwrap();

    let applied = repo
        .apply_reaction_event(
            "conv-1",
            "server-4",
            "u2",
            "👍",
            ReactionAction::Add as i32,
            Some(30),
        )
        .await
        .unwrap();
    assert_eq!(applied, OperationApplyResult::Applied);

    let stale = repo
        .apply_reaction_event(
            "conv-1",
            "server-4",
            "u2",
            "👍",
            ReactionAction::Remove as i32,
            Some(29),
        )
        .await
        .unwrap();
    assert_eq!(stale, OperationApplyResult::IgnoredStale);

    let reactions = repo
        .list_reactions(&["server-4".to_string()])
        .await
        .unwrap();
    assert_eq!(
        reactions
            .get("server-4")
            .and_then(|entries| entries.iter().find(|entry| entry.emoji == "👍"))
            .map(|entry| entry.count),
        Some(1)
    );

    let newer = repo
        .apply_reaction_event(
            "conv-1",
            "server-4",
            "u2",
            "👍",
            ReactionAction::Remove as i32,
            Some(31),
        )
        .await
        .unwrap();
    assert_eq!(newer, OperationApplyResult::Applied);

    let reactions_after_remove = repo
        .list_reactions(&["server-4".to_string()])
        .await
        .unwrap();
    assert!(!reactions_after_remove.contains_key("server-4"));
}

#[tokio::test]
async fn rejected_reaction_write_does_not_advance_its_clock() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(Default::default());
    message.server_id = "atomic-reaction".into();
    message.conversation_id = "c".into();
    repo.save_one(&message).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_reaction BEFORE INSERT ON message_reactions BEGIN SELECT RAISE(ABORT, 'injected write failure'); END").execute(&repo.pool).await.unwrap();
    assert!(
        repo.apply_reaction_event(
            "c",
            "atomic-reaction",
            "u",
            "👍",
            ReactionAction::Add as i32,
            Some(10)
        )
        .await
        .is_err()
    );
    sqlx::query("DROP TRIGGER reject_reaction")
        .execute(&repo.pool)
        .await
        .unwrap();
    assert_eq!(
        repo.apply_reaction_event(
            "c",
            "atomic-reaction",
            "u",
            "👍",
            ReactionAction::Add as i32,
            Some(10)
        )
        .await
        .unwrap(),
        OperationApplyResult::Applied
    );
    assert_eq!(
        repo.list_reactions(&["atomic-reaction".into()])
            .await
            .unwrap()["atomic-reaction"][0]
            .user_ids,
        vec!["u"]
    );
}

#[tokio::test]
async fn apply_reaction_remove_clears_message_snapshot_immediately() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-local-reaction".to_string();
    message.client_msg_id = "client-local-reaction".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message]).await.unwrap();

    repo.apply_reaction(
        "conv-1",
        "server-local-reaction",
        "u1",
        "👍",
        ReactionAction::Add as i32,
    )
    .await
    .unwrap();
    let added = repo.get("server-local-reaction").await.unwrap().unwrap();
    assert_eq!(added.reactions.len(), 1);

    repo.apply_reaction(
        "conv-1",
        "server-local-reaction",
        "u1",
        "👍",
        ReactionAction::Remove as i32,
    )
    .await
    .unwrap();
    let removed = repo.get("server-local-reaction").await.unwrap().unwrap();
    assert!(removed.reactions.is_empty());
    assert_eq!(removed.attributes["reactionsJson"], "[]");
}

#[tokio::test]
async fn save_batch_without_reaction_snapshot_keeps_existing_reactions() {
    let repo = make_repo().await;
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-5".to_string();
    message.client_msg_id = "client-5".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message]).await.unwrap();

    repo.apply_reaction_event(
        "conv-1",
        "server-5",
        "u2",
        "👍",
        ReactionAction::Add as i32,
        Some(1),
    )
    .await
    .unwrap();
    let before = repo
        .list_reactions(&["server-5".to_string()])
        .await
        .unwrap();
    assert_eq!(
        before
            .get("server-5")
            .and_then(|entries| entries.iter().find(|entry| entry.emoji == "👍"))
            .map(|entry| entry.count),
        Some(1)
    );

    // 模拟同步下行：消息不带 reactions 快照（attributes 无 reactionsJson、reactions 为空）。
    let mut sync_message = IMMessage::new(flare_proto::common::Message::default());
    sync_message.server_id = "server-5".to_string();
    sync_message.client_msg_id = "client-5".to_string();
    sync_message.conversation_id = "conv-1".to_string();
    sync_message.sender_id = "u1".to_string();
    repo.save_batch(&[sync_message]).await.unwrap();

    let after = repo
        .list_reactions(&["server-5".to_string()])
        .await
        .unwrap();
    assert_eq!(
        after
            .get("server-5")
            .and_then(|entries| entries.iter().find(|entry| entry.emoji == "👍"))
            .map(|entry| entry.count),
        Some(1)
    );
}

#[tokio::test]
async fn apply_reaction_event_before_message_arrival_is_not_lost() {
    let repo = make_repo().await;

    // 先收到 reaction 事件（消息主体尚未落库）
    let applied = repo
        .apply_reaction_event(
            "conv-9",
            "server-9",
            "u9",
            "👍",
            ReactionAction::Add as i32,
            Some(9),
        )
        .await
        .unwrap();
    assert_eq!(applied, OperationApplyResult::Applied);

    let before = repo
        .list_reactions(&["server-9".to_string()])
        .await
        .unwrap();
    assert_eq!(
        before
            .get("server-9")
            .and_then(|entries| entries.iter().find(|entry| entry.emoji == "👍"))
            .map(|entry| entry.count),
        Some(1)
    );

    // 后续消息同步到本地
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-9".to_string();
    message.client_msg_id = "client-9".to_string();
    message.conversation_id = "conv-9".to_string();
    message.sender_id = "u1".to_string();
    repo.save_batch(&[message]).await.unwrap();

    // 反应仍可通过消息 ID 聚合读取，确保 UI 可展示
    let after = repo
        .list_reactions(&["server-9".to_string()])
        .await
        .unwrap();
    assert_eq!(
        after
            .get("server-9")
            .and_then(|entries| entries.iter().find(|entry| entry.emoji == "👍"))
            .map(|entry| entry.count),
        Some(1)
    );
}

/// 身份缓存读路径:命中缓存→当前身份;miss→消息内嵌发送时快照;都无→结构化的「无名」,不回退 sender_id。
/// 印证"写时/同步时把身份喂进缓存,读时直接批量 join,无需逐条组装"的设计。
#[tokio::test]
async fn timeline_read_resolves_current_identity_then_falls_back() {
    use flare_im_core_sdk::prelude::{IMClient, SdkConfigOverlay};
    use flare_im_core_sdk::spi::{DisplayName, NameEntity, NameSource};
    use flare_im_core_sdk::storage::UserProfile;

    let root = std::env::temp_dir().join(format!("flare-sender-profile-{}", uuid::Uuid::new_v4()));
    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    client
        .prepare("viewer", super::super::super::sqlite())
        .await
        .unwrap();
    let stores = client.stores_async().await.unwrap();
    let m1 = text_message("s1", "conv", "u1", 1, 1_000, "from u1"); // 将命中缓存
    let mut m2 = text_message("s2", "conv", "u2", 2, 2_000, "from u2");
    m2.sender_name = "Bob".to_string(); // 无缓存,但消息内嵌快照
    let m3 = text_message("s3", "conv", "u3", 3, 3_000, "from u3"); // 无缓存且无快照
    stores.messages.save_batch(&[m1, m2, m3]).await.unwrap();

    // 业务端 upsert 等价:把当前身份写进缓存
    stores
        .save_user_profiles(&[UserProfile {
            user_id: "u1".into(),
            nickname: "Alice".into(),
            avatar_url: String::new(),
            alias: String::new(),
        }])
        .await
        .unwrap();

    let list = client
        .connected_apis()
        .await
        .unwrap()
        .message_api
        .list("conv", 0, 50)
        .await
        .unwrap();
    let name_of = |id: &str| {
        list.iter()
            .find(|m| m.server_id == id)
            .unwrap()
            .sender
            .clone()
    };
    assert_eq!(
        name_of("s1"),
        DisplayName::named("Alice", NameSource::Profile),
        "缓存命中→当前身份"
    );
    assert_eq!(
        name_of("s2"),
        DisplayName::named("Bob", NameSource::Snapshot),
        "缓存 miss→内嵌发送时快照"
    );
    assert_eq!(
        name_of("s3"),
        DisplayName::unnamed(NameEntity::Person, "u3"),
        "都无→无名,不把 sender_id 当名字"
    );
    client.uninit().await.unwrap();
    drop(stores);
    std::fs::remove_dir_all(root).unwrap();
}

/// The sender's timeline draws its upload bar from `local_state.uploading` / `upload_progress`. SQLite used to keep
/// only sending/failed/is_local and read the other two back as false/0, so native clients never showed any progress.
#[tokio::test]
async fn upload_progress_survives_a_round_trip_through_both_writers() {
    let repo = make_repo().await;

    let mut single = text_message(
        "client-upload-single",
        "conv-upload",
        "u1",
        0,
        1_000,
        "file",
    );
    single.client_msg_id = "client-upload-single".to_string();
    single.local_state.sending = true;
    single.local_state.is_local = true;
    single.local_state.uploading = true;
    single.local_state.upload_progress = 37;
    repo.save_one(&single).await.unwrap();

    let mut batch = text_message(
        "client-upload-batch",
        "conv-upload",
        "u1",
        0,
        2_000,
        "album",
    );
    batch.client_msg_id = "client-upload-batch".to_string();
    batch.local_state.sending = true;
    batch.local_state.is_local = true;
    batch.local_state.uploading = true;
    batch.local_state.upload_progress = 250;
    repo.save_batch(&[batch]).await.unwrap();

    let single = repo
        .get_by_client_msg_id("client-upload-single")
        .await
        .unwrap()
        .expect("single");
    assert!(single.local_state.uploading);
    assert_eq!(single.local_state.upload_progress, 37);
    let batch = repo
        .get_by_client_msg_id("client-upload-batch")
        .await
        .unwrap()
        .expect("batch");
    assert!(batch.local_state.uploading);
    assert_eq!(
        batch.local_state.upload_progress, 100,
        "stored progress is capped at 100"
    );
}

/// A database created before the upload columns existed gains them on open, and its rows read as not uploading.
#[tokio::test]
async fn an_older_database_gains_the_upload_columns() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let repo = SqliteMessageRepo::new(pool.clone());
    let mut old = text_message("s-old", "conv-old", "u1", 1, 1_000, "before");
    old.client_msg_id = "client-old".to_string();
    repo.save_one(&old).await.unwrap();
    sqlx::query("ALTER TABLE messages DROP COLUMN uploading")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE messages DROP COLUMN upload_progress")
        .execute(&pool)
        .await
        .unwrap();

    sqlite_init_schema(&pool).await.unwrap();

    let stored = repo
        .get_by_client_msg_id("client-old")
        .await
        .unwrap()
        .expect("old row");
    assert!(!stored.local_state.uploading);
    assert_eq!(stored.local_state.upload_progress, 0);
}

fn plugin_state(version: u64, status: &str) -> flare_plugin_protocol::ui::PluginMessageState {
    serde_json::from_value(serde_json::json!({"version": version, "values": {"status": status}}))
        .unwrap()
}

fn plugin_status(message: &IMMessage) -> Option<(u64, String)> {
    let state = message.plugin_message_state()?;
    Some((state.version, state.values["status"].as_str()?.to_string()))
}

#[tokio::test]
async fn plugin_message_state_is_monotonic_and_survives_ack_sync_and_edit() {
    let repo = make_repo().await;
    let mut pending = text_message("client-plugin-1", "conv-plugin", "u1", 0, 1_000, "card");
    pending.client_msg_id = "client-plugin-1".to_string();
    pending.status = MessageStatus::Created as i32;
    pending.local_state.sending = true;
    pending.local_state.is_local = true;
    repo.save_batch(&[pending.clone()]).await.unwrap();

    assert_eq!(
        repo.apply_plugin_message_state("missing", &plugin_state(1, "open"))
            .await
            .unwrap(),
        OperationApplyResult::NotFound
    );
    assert_eq!(
        repo.apply_plugin_message_state("client-plugin-1", &plugin_state(2, "open"))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );

    // The ACK replaces the optimistic row and does not carry the keys itself.
    let mut acked = pending;
    acked.server_id = "server-plugin-1".to_string();
    acked.conversation_seq = 5;
    acked.status = MessageStatus::Persisted as i32;
    acked.attributes.clear();
    repo.update_after_ack("client-plugin-1", &acked)
        .await
        .unwrap();
    let stored = repo.get("server-plugin-1").await.unwrap().unwrap();
    assert_eq!(plugin_status(&stored), Some((2, "open".to_string())));

    assert_eq!(
        repo.apply_plugin_message_state("server-plugin-1", &plugin_state(1, "stale"))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert_eq!(
        repo.apply_plugin_message_state("server-plugin-1", &plugin_state(2, "claimed"))
            .await
            .unwrap(),
        OperationApplyResult::Applied,
        "the same version is replaced by the later result"
    );
    let older_copy = repo.get("server-plugin-1").await.unwrap().unwrap();
    assert_eq!(
        repo.apply_plugin_message_state("client-plugin-1", &plugin_state(3, "completed"))
            .await
            .unwrap(),
        OperationApplyResult::Applied,
        "the client id still addresses the acknowledged row"
    );

    // A sync snapshot without the keys and a re-save of an older local copy keep the newer state.
    let mut synced = acked.clone();
    synced.updated_at = 50;
    repo.save_batch(&[synced, older_copy]).await.unwrap();
    repo.update_content("server-plugin-1", acked.encoded_content.clone())
        .await
        .unwrap();
    let after = repo.get("server-plugin-1").await.unwrap().unwrap();
    assert_eq!(plugin_status(&after), Some((3, "completed".to_string())));

    let invalid: flare_plugin_protocol::ui::PluginMessageState =
        serde_json::from_value(serde_json::json!({"version": 9, "values": {"nested": {"a": 1}}}))
            .unwrap();
    assert!(
        repo.apply_plugin_message_state("server-plugin-1", &invalid)
            .await
            .is_err()
    );
}

fn shared_state(version: u64, voters: u64) -> flare_plugin_protocol::state::SharedState {
    serde_json::from_value(serde_json::json!({
        "entity": "poll-1", "version": version, "values": {"voterCount": voters}
    }))
    .unwrap()
}

fn shared_voters(message: &IMMessage) -> Option<(u64, u64)> {
    let state = message.plugin_shared_state(None)?;
    Some((state.version, state.values["voterCount"].as_u64()?))
}

#[tokio::test]
async fn plugin_shared_state_moves_forward_only_and_keeps_the_newer_of_wire_and_local() {
    let repo = make_repo().await;
    let mut pending = text_message("client-shared-1", "conv-plugin", "u1", 0, 1_000, "card");
    pending.client_msg_id = "client-shared-1".to_string();
    pending.status = MessageStatus::Created as i32;
    pending.local_state.sending = true;
    pending.local_state.is_local = true;
    repo.save_batch(&[pending.clone()]).await.unwrap();

    assert_eq!(
        repo.apply_plugin_shared_state("missing", &shared_state(1, 1))
            .await
            .unwrap(),
        OperationApplyResult::NotFound
    );
    assert_eq!(
        repo.apply_plugin_shared_state("client-shared-1", &shared_state(2, 3))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    for stale in [shared_state(2, 9), shared_state(1, 9)] {
        assert_eq!(
            repo.apply_plugin_shared_state("client-shared-1", &stale)
                .await
                .unwrap(),
            OperationApplyResult::IgnoredStale
        );
    }

    // The ACK replaces the optimistic row; the shared layer goes with it.
    let mut acked = pending;
    acked.server_id = "server-shared-1".to_string();
    acked.conversation_seq = 5;
    acked.status = MessageStatus::Persisted as i32;
    acked.attributes.clear();
    repo.update_after_ack("client-shared-1", &acked)
        .await
        .unwrap();
    let stored = repo.get("server-shared-1").await.unwrap().unwrap();
    assert_eq!(shared_voters(&stored), Some((2, 3)));

    // An older snapshot from history loses; a newer one wins and keeps the stored entity.
    let mut synced = acked.clone();
    synced
        .attributes
        .insert("pluginSharedStateJson".into(), r#"{"voterCount":1}"#.into());
    synced
        .attributes
        .insert("pluginSharedStateVersion".into(), "1".into());
    repo.save_batch(&[synced.clone()]).await.unwrap();
    let after = repo.get("server-shared-1").await.unwrap().unwrap();
    assert_eq!(shared_voters(&after), Some((2, 3)));
    synced
        .attributes
        .insert("pluginSharedStateJson".into(), r#"{"voterCount":7}"#.into());
    synced
        .attributes
        .insert("pluginSharedStateVersion".into(), "6".into());
    repo.save_batch(&[synced]).await.unwrap();
    let newer = repo.get("server-shared-1").await.unwrap().unwrap();
    assert_eq!(shared_voters(&newer), Some((6, 7)));
    assert_eq!(newer.attributes["pluginSharedEntity"], "poll-1");
    assert_eq!(
        repo.apply_plugin_shared_state("server-shared-1", &shared_state(5, 0))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale,
        "a version older than the one history delivered is dropped"
    );
}

#[tokio::test]
async fn orphan_reaction_clocks_upgrade_and_survive_reopening_before_message_hydration() {
    let root =
        std::env::temp_dir().join(format!("flare-orphan-reactions-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let options = SqliteConnectOptions::new()
        .filename(root.join("upgrade.db"))
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options.clone()).await.unwrap();
    sqlx::query("CREATE TABLE message_reactions(message_server_id TEXT NOT NULL, conversation_id TEXT NOT NULL DEFAULT '', emoji TEXT NOT NULL, user_id TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(message_server_id, emoji, user_id))").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO message_reactions(message_server_id, conversation_id, emoji, user_id) VALUES ('legacy', 'c', '👍', 'legacy-user')").execute(&pool).await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let repo = SqliteMessageRepo::new(pool.clone());
    assert_eq!(
        repo.list_reactions(&["legacy".into()]).await.unwrap()["legacy"][0].user_ids,
        vec!["legacy-user"]
    );
    assert!(
        repo.apply_reaction_event(
            "c",
            "cold",
            "u",
            "👍",
            ReactionAction::Add as i32,
            Some(u64::MAX)
        )
        .await
        .is_err()
    );
    assert_eq!(
        repo.apply_reaction_event(
            "c",
            "cold",
            "u",
            "👍",
            ReactionAction::Remove as i32,
            Some(20)
        )
        .await
        .unwrap(),
        OperationApplyResult::Applied
    );
    assert_eq!(
        repo.apply_reaction_event("c", "cold", "u", "👍", ReactionAction::Add as i32, Some(10))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert!(
        !repo
            .list_reactions(&["cold".into()])
            .await
            .unwrap()
            .contains_key("cold")
    );
    drop(repo);
    pool.close().await;

    let pool = SqlitePool::connect_with(options).await.unwrap();
    sqlite_init_schema(&pool).await.unwrap();
    let repo = SqliteMessageRepo::new(pool.clone());
    assert_eq!(
        repo.apply_reaction_event("c", "cold", "u", "👍", ReactionAction::Add as i32, Some(10))
            .await
            .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    let mut old = IMMessage::new(Default::default());
    old.server_id = "cold".into();
    old.client_msg_id = "local-cold".into();
    old.conversation_id = "c".into();
    old.apply_reaction_change("u", "👍", ReactionAction::Add as i32);
    repo.save_one(&old).await.unwrap();
    assert!(
        repo.get("cold")
            .await
            .unwrap()
            .unwrap()
            .reactions
            .is_empty()
    );
    assert!(
        !repo
            .list_reactions(&["cold".into()])
            .await
            .unwrap()
            .contains_key("cold")
    );
    assert_eq!(
        repo.apply_reaction_event(
            "c",
            "cold",
            "u",
            "👍",
            ReactionAction::Remove as i32,
            Some(20)
        )
        .await
        .unwrap(),
        OperationApplyResult::IgnoredStale
    );
    assert_eq!(
        repo.apply_reaction_event("c", "cold", "u", "👍", ReactionAction::Add as i32, Some(30))
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    assert_eq!(
        repo.list_reactions(&["cold".into()]).await.unwrap()["cold"][0].user_ids,
        vec!["u"]
    );
    drop(repo);
    pool.close().await;
    tokio::fs::remove_dir_all(root).await.unwrap();
}
