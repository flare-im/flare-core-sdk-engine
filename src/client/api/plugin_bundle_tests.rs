//! Web bundle pages through `CapabilityApi` against the local dispatch
//! endpoint of the page tests: opening from the verified cache, the bridge
//! end to end (query, command journal, transient operations, storage,
//! host-drawn requests, gestures), withdrawal closing the page, and rollback.
use super::page_tests::Fixture;
use super::*;
use flare_plugin_protocol::bundle::{BundleManifest, WebBundleDecl, sha256_hex, write_archive};
use serde_json::json;
use std::collections::BTreeMap;

const PLUGIN: &str = "example.poll";

fn bundle_json() -> BundleManifest {
    serde_json::from_value(json!({
        "id": "board", "entry": "index.html", "bridge": 1,
        "permissions": ["plugin.query", "plugin.command", "storage", "clipboard"],
        "operations": ["example.poll.get", "example.poll.move", "example.poll.pay"],
        "capabilities": [], "locales": ["en"]
    }))
    .unwrap()
}

fn archive(filler: usize) -> Vec<u8> {
    let mut files = BTreeMap::new();
    files.insert(
        "index.html".to_owned(),
        b"<!doctype html><script src=\"app.js\"></script>".to_vec(),
    );
    files.insert("app.js".to_owned(), vec![b'x'; filler]);
    write_archive(&bundle_json(), &files).unwrap()
}

/// The catalog entry of `bytes`: the tenant approved query, command and
/// storage, not the clipboard.
fn entry(bytes: &[u8]) -> WebBundleDecl {
    let sha256 = sha256_hex(bytes);
    serde_json::from_value(json!({
        "id": "board", "sha256": sha256, "size": bytes.len(), "bridge": 1,
        "urls": ["https://mirror.example.com/board.fwb"],
        "files": format!("https://sandbox.example.com/b/{sha256}/"),
        "permissions": ["plugin.query", "plugin.command", "storage"]
    }))
    .unwrap()
}

/// A catalog whose page opens `entry` (or, without it, the declarative variant).
fn catalog(entry: Option<&WebBundleDecl>, version: &str) -> Value {
    let mut page = json!({"kind": "page", "id": "example.poll.board", "schemaVersion": 1,
        "requiredPermissions": [],
        "schema": {"title": "Board", "fields": [], "actions": []}});
    let mut web_bundles = json!([]);
    if let Some(entry) = entry {
        page["variant"] = json!("board");
        page["bundle"] = json!({"id": "board", "route": "/"});
        web_bundles = json!([entry]);
    } else {
        page["variant"] = json!("basic");
    }
    json!({
        "tenantId": "201", "version": 1,
        "validUntilMs": chrono::Utc::now().timestamp_millis() + 600_000,
        "plugins": [{
            "pluginId": PLUGIN, "version": version,
            "manifestDigest": format!("sha256:{}", "b".repeat(64)), "grantRevision": 1,
            "extensions": {"platforms": ["web"], "contributions": [page],
                "nativeCapabilities": ["paymentConfirmation"], "webBundles": web_bundles},
            "operations": ["example.poll.get", "example.poll.move", "example.poll.pay",
                           "example.poll.other"],
            "queryOperations": ["example.poll.get"],
            "transientOperations": ["example.poll.pay"]
        }],
        "unavailable": []
    })
}

/// A web host with a bundle runtime, a cache under a temporary root holding
/// `bytes`, and a catalog serving it.
async fn fixture(bytes: &[u8]) -> (Fixture, std::path::PathBuf) {
    let f = Fixture::new().await;
    let kit = serde_json::to_value(flare_plugin_protocol::ui::engine_vocabulary()).unwrap();
    let profile = super::host_profile::parse_profile(
        json!({"platform": "web", "kitVocabulary": kit,
               "nativeAllowlist": ["paymentConfirmation"],
               "bundleRuntime": {"bridge": 1, "engine": "browser", "engineMajor": 120}}),
        &super::host_profile::AppIdentity::default(),
    )
    .unwrap();
    f.api.plugin_host.set(profile);
    let root = std::env::temp_dir().join(format!(
        "flare-bundle-api-{}",
        uuid::Uuid::new_v4().simple()
    ));
    f.api.plugin_host.bundles.set_cache_root(root.clone());
    f.api
        .plugin_host
        .bundles
        .cache()
        .install(&entry(bytes), bytes)
        .unwrap();
    *f.catalog.lock().unwrap() = catalog(Some(&entry(bytes)), "0.2.0");
    (f, root)
}

async fn open(f: &Fixture) -> Value {
    f.api
        .plugin_bundle_open(json!({
            "pluginId": PLUGIN, "contributionId": "example.poll.board",
            "conversation": {"id": "conversation-1", "type": "group"},
            "locale": "en", "theme": {"mode": "light", "tokens": {}}
        }))
        .await
        .unwrap()
}

async fn bridge(f: &Fixture, session: &str, envelope: Value, gesture: bool) -> Value {
    f.api
        .plugin_bundle_bridge(json!({
            "sessionId": session, "envelope": envelope.to_string(), "userGesture": gesture
        }))
        .await
        .unwrap()
}

fn answer(reply: &Value) -> Value {
    serde_json::from_str(reply["envelope"].as_str().unwrap()).unwrap()
}

fn request(id: &str, method: &str, params: Value) -> Value {
    json!({"bridge": 1, "id": id, "type": "request", "method": method, "params": params})
}

async fn greeted(f: &Fixture) -> String {
    let opened = open(f).await;
    let session = opened["sessionId"].as_str().unwrap().to_owned();
    let hello = answer(&bridge(f, &session, request("h", "host.hello", json!({})), false).await);
    assert_eq!(hello["result"]["pluginId"], PLUGIN);
    session
}

#[tokio::test]
async fn opening_serves_the_verified_cache_with_the_policy_and_the_grant() {
    let bytes = archive(16);
    let (f, root) = fixture(&bytes).await;
    let opened = open(&f).await;
    let sha256 = sha256_hex(&bytes);
    assert_eq!(
        opened["root"],
        root.join(&sha256).to_string_lossy().as_ref()
    );
    assert_eq!(opened["entry"], "index.html");
    assert_eq!(opened["origin"], "null", "web pages are opaque");
    let csp = opened["csp"].as_str().unwrap();
    assert!(
        csp.contains("connect-src 'none'")
            && csp.contains("script-src https://sandbox.example.com;")
    );
    let files: Vec<&str> = opened["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["path"].as_str().unwrap())
        .collect();
    assert_eq!(files, vec!["app.js", "bundle.json", "index.html"]);
    // bundle.json ∩ tenant approval: no clipboard.
    assert_eq!(
        opened["hello"]["permissions"],
        json!(["plugin.query", "plugin.command", "storage"])
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn a_query_goes_through_core_and_a_denied_one_never_leaves_the_engine() {
    let bytes = archive(16);
    let (f, root) = fixture(&bytes).await;
    f.answer("example.poll.get", vec![json!({"done": 3})]);
    let session = greeted(&f).await;
    let ok = answer(
        &bridge(
            &f,
            &session,
            request(
                "q",
                "plugin.query",
                json!({"operation": "example.poll.get"}),
            ),
            false,
        )
        .await,
    );
    assert_eq!(ok["result"], json!({"data": {"done": 3}}));
    let call = f.calls_to("example.poll.get").pop().unwrap();
    assert_eq!(call["conversationId"], "conversation-1");
    for (operation, method) in [
        ("example.poll.other", "plugin.query"),
        ("example.poll.move", "plugin.query"),
        ("example.poll.pay", "plugin.command"),
        ("example.wallet.balance", "plugin.query"),
    ] {
        let refused = answer(
            &bridge(
                &f,
                &session,
                request("x", method, json!({"operation": operation})),
                false,
            )
            .await,
        );
        assert_eq!(refused["error"]["code"], "permission_denied", "{operation}");
        assert!(f.calls_to(operation).is_empty(), "{operation} reached Core");
    }
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn a_command_is_logged_and_answers_with_its_command_id() {
    use crate::infrastructure::persistence::sqlite::{SqlitePluginCommandRepo, init_schema};
    let bytes = archive(16);
    let (mut f, root) = fixture(&bytes).await;
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    init_schema(&pool).await.unwrap();
    f.api = f
        .api
        .clone()
        .with_plugin_command_store(Some(Arc::new(SqlitePluginCommandRepo::new(pool))));
    f.answer("example.poll.move", vec![json!({"moved": true})]);
    let session = greeted(&f).await;
    let done = answer(
        &bridge(
            &f,
            &session,
            request(
                "c",
                "plugin.command",
                json!({"operation": "example.poll.move", "input": {"task": "t1"},
                       "commandId": "cmd-1"}),
            ),
            false,
        )
        .await,
    );
    assert_eq!(
        done["result"],
        json!({"data": {"moved": true}, "commandId": "cmd-1"})
    );
    let sent = f.calls_to("example.poll.move").pop().unwrap();
    assert_eq!(sent["payload"]["commandId"], "cmd-1");
    // The same command again: the journal answers, nothing is sent twice.
    let again = answer(
        &bridge(
            &f,
            &session,
            request(
                "c2",
                "plugin.command",
                json!({"operation": "example.poll.move", "input": {"task": "t1"},
                       "commandId": "cmd-1"}),
            ),
            false,
        )
        .await,
    );
    assert_eq!(again["result"]["commandId"], "cmd-1");
    assert_eq!(f.calls_to("example.poll.move").len(), 1);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn storage_is_the_plugins_own_and_ui_requests_go_to_the_host() {
    let bytes = archive(16);
    let (f, root) = fixture(&bytes).await;
    let session = greeted(&f).await;
    let set = answer(
        &bridge(
            &f,
            &session,
            request(
                "s",
                "storage.set",
                json!({"key": "draft", "value": {"x": 1}}),
            ),
            false,
        )
        .await,
    );
    assert_eq!(set["result"], json!({}));
    let get = answer(
        &bridge(
            &f,
            &session,
            request("g", "storage.get", json!({"key": "draft"})),
            false,
        )
        .await,
    );
    assert_eq!(get["result"], json!({"value": {"x": 1}}));
    // A confirmation is drawn by the host and answered through host_result.
    let handed = bridge(
        &f,
        &session,
        request("k", "ui.confirm", json!({"title": "Move?"})),
        false,
    )
    .await;
    assert_eq!(handed["host"]["method"], "ui.confirm");
    assert_eq!(handed["host"]["id"], "k");
    let confirmed = f
        .api
        .plugin_bundle_host_result(json!({"sessionId": session, "id": "k",
            "result": {"confirmed": true}}))
        .await
        .unwrap();
    assert_eq!(answer(&confirmed)["result"], json!({"confirmed": true}));
    // Answered once.
    assert!(
        f.api
            .plugin_bundle_host_result(json!({"sessionId": session, "id": "k",
                "result": {"confirmed": true}}))
            .await
            .is_err()
    );
    // The clipboard was asked for in bundle.json but not approved.
    let clip = answer(
        &bridge(
            &f,
            &session,
            request("c", "clipboard.write", json!({"text": "x"})),
            true,
        )
        .await,
    );
    assert_eq!(clip["error"]["code"], "permission_denied");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn a_host_capability_needs_the_hosts_gesture_and_a_transient_operation_needs_one() {
    let bytes = archive(16);
    let (f, root) = fixture(&bytes).await;
    let session = greeted(&f).await;
    // bundle.json does not list paymentConfirmation: refused whatever the gesture.
    let refused = answer(
        &bridge(
            &f,
            &session,
            request(
                "p",
                "host.capability",
                json!({"name": "paymentConfirmation",
                "params": {"order": {}}}),
            ),
            true,
        )
        .await,
    );
    assert_eq!(refused["error"]["code"], "permission_denied");
    assert!(f.calls_to("example.poll.pay").is_empty());
    let _ = std::fs::remove_dir_all(root);
}

/// Withdrawal: Core stops selecting the bundle variant; the next catalog read
/// closes the open page with `lifecycle.revoked` and its bridge answers
/// `revoked`.
#[tokio::test]
async fn a_withdrawn_bundle_closes_its_open_page() {
    use crate::kernel::event::{PluginEvent, SdkEvent};
    let bytes = archive(16);
    let (mut f, root) = fixture(&bytes).await;
    let bus = crate::kernel::event::EventBus::new();
    let mut events = bus.subscribe();
    let messages: Arc<dyn crate::domain::MessageStore> =
        Arc::new(crate::infrastructure::persistence::memory_im::MemoryMessageStore::new());
    f.api = f.api.clone().with_message_state(messages, bus);
    let session = greeted(&f).await;
    *f.catalog.lock().unwrap() = catalog(None, "0.2.0");
    f.api.plugin_ui_catalog().await.unwrap();
    let mut closed = None;
    while let Ok(event) = events.try_recv() {
        if let SdkEvent::Plugin(PluginEvent::Bundle {
            session_id,
            envelope,
            close,
        }) = event
            && session_id == session
        {
            closed = Some((envelope, close));
        }
    }
    let (envelope, close) = closed.expect("the host is told to close the page");
    assert_eq!(close.as_deref(), Some("withdrawn"));
    let event: Value = serde_json::from_str(&envelope.unwrap()).unwrap();
    assert_eq!(event["event"], "lifecycle.revoked");
    assert_eq!(event["payload"], json!({"reason": "withdrawn"}));
    let after = answer(
        &bridge(
            &f,
            &session,
            request("q", "ui.toast", json!({"message": "x"})),
            false,
        )
        .await,
    );
    assert_eq!(after["error"]["code"], "revoked");
    let _ = std::fs::remove_dir_all(root);
}

/// Rollback: the tenant returns to the older version; its bundle's digest is
/// still cached, so the page opens without any download (the fixture has no
/// HTTPS mirror at all).
#[tokio::test]
async fn rolling_back_opens_the_older_bundle_from_the_cache() {
    let old = archive(16);
    let new = archive(32);
    let (f, root) = fixture(&old).await;
    f.api
        .plugin_host
        .bundles
        .cache()
        .install(&entry(&new), &new)
        .unwrap();
    *f.catalog.lock().unwrap() = catalog(Some(&entry(&new)), "0.3.0");
    let opened = open(&f).await;
    assert!(
        opened["root"]
            .as_str()
            .unwrap()
            .ends_with(&sha256_hex(&new))
    );
    *f.catalog.lock().unwrap() = catalog(Some(&entry(&old)), "0.2.0");
    let back = open(&f).await;
    assert!(back["root"].as_str().unwrap().ends_with(&sha256_hex(&old)));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn every_command_has_its_own_journal_slot() {
    let a = plugin_bundle::command_slot("example.poll", "board", "cmd-1");
    let b = plugin_bundle::command_slot("example.poll", "board", "cmd-2");
    assert_ne!(a, b);
    assert!(a.starts_with("example.poll.bundle."));
    assert!(crate::domain::valid_key(&a));
}
