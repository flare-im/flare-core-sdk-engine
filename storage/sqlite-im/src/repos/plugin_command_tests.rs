use super::*;
use flare_im_core_sdk::storage::MAX_TERMINAL_ERROR_CODES;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

fn scope(user: &str, slot: &str) -> PluginCommandScope {
    PluginCommandScope {
        tenant_id: "201".into(),
        user_id: user.into(),
        slot: slot.into(),
    }
}
fn command(id: &str) -> PluginCommand {
    PluginCommand {
        continuation: None,
        command_id: id.into(),
        capability_id: "example.create".into(),
        conversation_id: Some("c1".into()),
        message_id: None,
        payload: json!({"commandId":id,"amount":"500"}),
        terminal_error_codes: Vec::new(),
        reconciliation_capability_id: None,
    }
}
async fn memory() -> SqlitePluginCommandRepo {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    init_schema(&pool).await.unwrap();
    SqlitePluginCommandRepo::new(pool)
}
#[tokio::test]
async fn plugin_command_unknown_outcome_cannot_be_cleared_or_replaced() {
    let repo = memory().await;
    let s = scope("alice", "create");
    let c = command("command-1");
    let record = repo.prepare(&s, &c).await.unwrap();
    assert_eq!(record.command, c);
    assert!(record.response.is_none());
    assert!(repo.acknowledge(&s, &c.command_id).await.is_err());
    assert!(repo.prepare(&s, &command("new-command")).await.is_err());
    let mut changed = c.clone();
    changed.payload["amount"] = json!("501");
    assert!(repo.prepare(&s, &changed).await.is_err());
    assert!(
        repo.complete(&s, &changed, json!({"success":true}))
            .await
            .is_err()
    );
    assert_eq!(repo.pending(&s).await.unwrap().unwrap(), record);
    assert_eq!(repo.prepare(&s, &c).await.unwrap(), record);
}
#[tokio::test]
async fn plugin_command_scope_replay_and_first_response_are_isolated() {
    let repo = memory().await;
    let s = scope("alice", "create");
    let c = command("command-1");
    repo.prepare(&s, &c).await.unwrap();
    assert!(
        repo.pending(&scope("bob", "create"))
            .await
            .unwrap()
            .is_none()
    );
    let mut other = s.clone();
    other.tenant_id = "202".into();
    assert!(repo.pending(&other).await.unwrap().is_none());
    assert!(
        repo.pending(&scope("alice", "other"))
            .await
            .unwrap()
            .is_none()
    );
    let response = json!({"success":true,"data":{"id":"packet-1"}});
    let completed = repo.complete(&s, &c, response.clone()).await.unwrap();
    assert_eq!(completed.response, Some(response));
    assert_eq!(
        repo.complete(
            &s,
            &c,
            json!({"success":true,"data":{"id":"later-response"}})
        )
        .await
        .unwrap(),
        completed
    );
    assert!(repo.acknowledge(&s, "wrong-command").await.is_err());
    assert_eq!(repo.prepare(&s, &c).await.unwrap(), completed);
    repo.acknowledge(&s, &c.command_id).await.unwrap();
    repo.acknowledge(&s, &c.command_id).await.unwrap();
    assert!(repo.pending(&s).await.unwrap().is_none());
}
#[tokio::test]
async fn plugin_command_bounds_fail_before_persistence() {
    let repo = memory().await;
    let s = scope("alice", "create");
    let mut c = command("command-1");
    c.payload = json!({"value":"x".repeat(32768)});
    assert!(repo.prepare(&s, &c).await.is_err());
    let mut c = command("command-1");
    c.terminal_error_codes = (0..=MAX_TERMINAL_ERROR_CODES)
        .map(|i| format!("CODE_{i}"))
        .collect();
    assert!(
        repo.prepare(&s, &c).await.is_err(),
        "too many terminal codes"
    );
    for slot in 0..32 {
        repo.prepare(&scope("alice", &format!("slot-{slot}")), &command("c"))
            .await
            .unwrap();
    }
    assert!(repo.prepare(&s, &command("new")).await.is_err());
    assert!(
        repo.prepare(&scope("../alice", "create"), &command("c"))
            .await
            .is_err()
    );
    repo.prepare(&scope("bob", "create"), &command("c"))
        .await
        .unwrap();
    assert!(
        repo.complete(
            &scope("bob", "create"),
            &command("c"),
            json!({"value":"x".repeat(65536)})
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn plugin_command_reopens_and_serializes_independent_connections() {
    let path =
        std::env::temp_dir().join(format!("flare-plugin-journal-{}.db", uuid::Uuid::new_v4()));
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .busy_timeout(std::time::Duration::from_secs(2));
    let p1 = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    let p2 = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    init_schema(&p1).await.unwrap();
    let a = SqlitePluginCommandRepo::new(p1.clone());
    let b = SqlitePluginCommandRepo::new(p2.clone());
    let s = scope("alice", "create");
    let c1 = command("one");
    let c2 = command("two");
    let (r1, r2) = tokio::join!(a.prepare(&s, &c1), b.prepare(&s, &c2));
    assert_ne!(r1.is_ok(), r2.is_ok());
    let winner = a.pending(&s).await.unwrap().unwrap();
    drop(a);
    drop(b);
    p1.close().await;
    p2.close().await;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let repo = SqlitePluginCommandRepo::new(pool.clone());
    assert_eq!(repo.pending(&s).await.unwrap(), Some(winner));
    drop(repo);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn plugin_command_only_declared_terminal_rejections_can_be_acknowledged() {
    let repo = memory().await;
    let s = scope("alice", "create");
    let mut c = command("one");
    c.terminal_error_codes = vec!["INSUFFICIENT_FUNDS".into()];
    repo.prepare(&s, &c).await.unwrap();
    for code in ["UNAVAILABLE", "ACCESS_DENIED", "INTERNAL"] {
        assert!(
            repo.complete(&s, &c, json!({"success":false,"data":{"errorCode":code}}))
                .await
                .is_err()
        );
        assert!(repo.pending(&s).await.unwrap().unwrap().response.is_none());
        assert!(repo.acknowledge(&s, &c.command_id).await.is_err());
    }
    repo.complete(
        &s,
        &c,
        json!({"success":false,"data":{"errorCode":"INSUFFICIENT_FUNDS"}}),
    )
    .await
    .unwrap();
    repo.acknowledge(&s, &c.command_id).await.unwrap();
    assert!(repo.pending(&s).await.unwrap().is_none());
}
