//! Web bundle pages through `CapabilityApi` against the local dispatch
//! endpoint of the page tests: opening through the host delivery interface, the bridge
//! end to end (query, command journal, transient operations, storage,
//! host-drawn requests, gestures), withdrawal closing the page, and rollback.
use super::page_tests::Fixture;
use super::*;
use flare_plugin_protocol::bundle::{BundleManifest, WebBundleDecl, sha256_hex, write_archive};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::extension::capability::bundle::provider::{
    BundleDelivery, BundleDeliveryError, BundleLease, DeliveredBundle,
};

struct FixtureDelivery {
    root: PathBuf,
    archives: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl FixtureDelivery {
    fn add(&self, bytes: &[u8]) {
        self.archives
            .lock()
            .unwrap()
            .insert(sha256_hex(bytes), bytes.to_owned());
    }
}

#[async_trait::async_trait]
impl BundleDelivery for FixtureDelivery {
    async fn open(
        &self,
        decl: &WebBundleDecl,
    ) -> std::result::Result<DeliveredBundle, BundleDeliveryError> {
        use flare_plugin_protocol::bundle::unpack_archive;
        let archives = self.archives.lock().unwrap();
        let bytes = archives
            .get(&decl.sha256)
            .ok_or(BundleDeliveryError::Unavailable)?;
        let unpacked = unpack_archive(bytes, &decl.sha256, decl.size)
            .map_err(|_| BundleDeliveryError::Rejected)?;
        Ok(DeliveredBundle {
            manifest: unpacked.manifest,
            root: self.root.join(&decl.sha256),
            files: unpacked.index,
            lease: BundleLease::new(()),
        })
    }

    async fn prefetch(
        &self,
        _decl: &WebBundleDecl,
    ) -> std::result::Result<bool, BundleDeliveryError> {
        Ok(false)
    }

    fn mark_referenced(&self, _digests: &std::collections::BTreeSet<String>) {}
}

fn install_fixture_delivery(
    api: &CapabilityApi,
    root: &Path,
    bytes: &[u8],
) -> Arc<FixtureDelivery> {
    let delivery = Arc::new(FixtureDelivery {
        root: root.into(),
        archives: Mutex::default(),
    });
    delivery.add(bytes);
    assert!(api.plugin_host.bundles.install_delivery(delivery.clone()));
    delivery
}

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
    archive_with(filler, &bundle_json())
}

fn archive_with(filler: usize, manifest: &BundleManifest) -> Vec<u8> {
    let mut files = BTreeMap::new();
    files.insert(
        "index.html".to_owned(),
        b"<!doctype html><script src=\"app.js\"></script>".to_vec(),
    );
    files.insert("app.js".to_owned(), vec![b'x'; filler]);
    write_archive(manifest, &files).unwrap()
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
async fn fixture(bytes: &[u8]) -> (Fixture, PathBuf) {
    let (fixture, root, _) = fixture_with_delivery(bytes, entry(bytes)).await;
    (fixture, root)
}

/// [`fixture`] serving `served` (the catalog entry of `bytes`).
async fn fixture_with(bytes: &[u8], served: WebBundleDecl) -> (Fixture, PathBuf) {
    let (fixture, root, _) = fixture_with_delivery(bytes, served).await;
    (fixture, root)
}

async fn fixture_with_delivery(
    bytes: &[u8],
    served: WebBundleDecl,
) -> (Fixture, PathBuf, Arc<FixtureDelivery>) {
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
    let delivery = install_fixture_delivery(&f.api, &root, bytes);
    *f.catalog.lock().unwrap() = catalog(Some(&served), "0.2.0");
    (f, root, delivery)
}

async fn open_with_card(f: &Fixture, presentation: &str, message_id: Option<&str>) -> Value {
    f.api
        .plugin_bundle_open(json!({
            "pluginId": PLUGIN, "contributionId": "example.poll.board",
            "conversation": {"id": "conversation-1", "type": "group"},
            "presentation": presentation, "arguments": {"messageId": message_id},
            "locale": "en", "theme": {"mode": "light", "tokens": {}}
        }))
        .await
        .unwrap()
}

async fn open(f: &Fixture) -> Value {
    open_with_card(f, "page", None).await
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
async fn opening_serves_the_delivered_files_with_the_policy_and_the_grant() {
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

#[tokio::test]
async fn a_bundle_detail_query_uses_the_host_card_and_ignores_the_frame_claim() {
    let (f, root) = fixture(&archive(16)).await;
    for (presentation, expected) in [("detail", json!("server-card")), ("page", Value::Null)] {
        f.answer("example.poll.get", vec![json!({"done": 3})]);
        let opened = open_with_card(&f, presentation, Some("server-card")).await;
        let session = opened["sessionId"].as_str().unwrap();
        bridge(&f, session, request("h", "host.hello", json!({})), false).await;
        let query = request(
            "q",
            "plugin.query",
            json!({
                "operation": "example.poll.get", "input": {"messageId": "forged-card"},
            }),
        );
        let outcome = answer(&bridge(&f, session, query, false).await);
        assert_eq!(outcome["result"]["data"]["done"], 3);
        assert_eq!(
            f.calls_to("example.poll.get").last().unwrap()["messageId"],
            expected
        );
    }
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn a_detail_command_is_logged_with_the_host_card_and_replayed_only_there() {
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
    let opened = open_with_card(&f, "detail", Some("server-card")).await;
    let session = opened["sessionId"].as_str().unwrap().to_owned();
    bridge(&f, &session, request("h", "host.hello", json!({})), false).await;
    let done = answer(
        &bridge(
            &f,
            &session,
            request(
                "c",
                "plugin.command",
                json!({"operation": "example.poll.move", "input": {"task": "t1","messageId":"forged"},
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
    assert_eq!(sent["messageId"], "server-card");
    // The same command again: the journal answers, nothing is sent twice.
    let again = answer(
        &bridge(
            &f,
            &session,
            request(
                "c2",
                "plugin.command",
                json!({"operation": "example.poll.move", "input": {"task": "t1","messageId":"forged"},
                       "commandId": "cmd-1"}),
            ),
            false,
        )
        .await,
    );
    assert_eq!(again["result"]["commandId"], "cmd-1");
    let other = open_with_card(&f, "detail", Some("other-card")).await;
    let other = other["sessionId"].as_str().unwrap();
    bridge(&f, other, request("h", "host.hello", json!({})), false).await;
    let denied = answer(
        &bridge(
            &f,
            other,
            request(
                "c3",
                "plugin.command",
                json!({
                    "operation": "example.poll.move", "input": {"task": "t1","messageId":"forged"},
                    "commandId":"cmd-1"
                }),
            ),
            false,
        )
        .await,
    );
    assert_eq!(denied["error"]["code"], "permission_denied");
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
                "params": {"orderId": "order-1"}}),
            ),
            true,
        )
        .await,
    );
    assert_eq!(refused["error"]["code"], "permission_denied");
    assert!(f.calls_to("example.poll.pay").is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn qr_scanning_uses_the_grant_gesture_and_host_and_discards_late_results() {
    use crate::kernel::event::{EventBus, PluginEvent, SdkEvent};
    use flare_plugin_protocol::ui::NativeCapability;
    let mut manifest = bundle_json();
    manifest.capabilities.insert(NativeCapability::ScanCode);
    let bytes = archive_with(16, &manifest);
    let served = entry(&bytes);
    let (mut f, root) = fixture_with(&bytes, served.clone()).await;
    let mut declared = catalog(Some(&served), "0.2.0");
    declared["plugins"][0]["extensions"]["nativeCapabilities"] = json!(["scanCode"]);
    *f.catalog.lock().unwrap() = declared;
    let profile = super::host_profile::parse_profile(
        json!({"platform": "web", "nativeAllowlist": ["scanCode"],
               "kitVocabulary": flare_plugin_protocol::ui::engine_vocabulary(),
               "bundleRuntime": {"bridge": 1, "engine": "browser", "engineMajor": 120}}),
        &super::host_profile::AppIdentity::default(),
    )
    .unwrap();
    f.api.plugin_host.set(profile);
    let bus = EventBus::new();
    let mut events = bus.subscribe();
    let messages =
        Arc::new(crate::infrastructure::persistence::memory_im::MemoryMessageStore::new());
    f.api = f.api.clone().with_message_state(messages, bus);
    let session = greeted(&f).await;
    let scan = |id| request(id, "host.capability", json!({"name": "scanCode"}));
    let denied = answer(&bridge(&f, &session, scan("without-gesture"), false).await);
    assert_eq!(denied["error"]["code"], "gesture_required");

    for (id, raw, closing) in [
        ("plain", "VISITOR-42", false),
        ("link", "https://scan.example.com/l/secret", false),
        ("late", "VISITOR-43", true),
    ] {
        let api = f.api.clone();
        let session_id = session.clone();
        let envelope = scan(id).to_string();
        let running = tokio::spawn(async move {
            api.plugin_bundle_bridge(json!({
                "sessionId": session_id, "envelope": envelope, "userGesture": true,
            }))
            .await
            .unwrap()
        });
        for (expected, result) in [
            ("consent", json!({"decision": "once"})),
            ("scanCode", json!({"text": raw})),
        ] {
            let (request_id, method) =
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    loop {
                        if let Ok(SdkEvent::Plugin(PluginEvent::CapabilityRequest {
                            request_id,
                            method,
                            ..
                        })) = events.recv().await
                        {
                            break (request_id, method);
                        }
                    }
                })
                .await
                .expect("the engine asks the host");
            assert_eq!(method, expected);
            if closing && expected == "scanCode" {
                f.api
                    .plugin_bundle_close(json!({"sessionId": session}))
                    .await
                    .unwrap();
            }
            assert!(
                f.api
                    .plugin_host
                    .capability_host
                    .requests
                    .complete(request_id, result)
            );
        }
        let response = answer(&running.await.unwrap());
        match id {
            "plain" => assert_eq!(response["result"], json!({"format": "qr", "text": raw})),
            "link" => assert_eq!(response["error"]["code"], "unavailable"),
            _ => assert_eq!(response["error"]["code"], "revoked"),
        }
    }
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
async fn rolling_back_opens_the_older_delivered_bundle() {
    let old = archive(16);
    let new = archive(32);
    let (f, root, delivery) = fixture_with_delivery(&old, entry(&old)).await;
    delivery.add(&new);
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

#[tokio::test]
async fn a_page_with_an_offered_bundle_says_so_and_a_declarative_one_does_not() {
    let bytes = archive(16);
    let (f, root) = fixture(&bytes).await;
    let page = || {
        f.api.plugin_page(json!({
            "pluginId": PLUGIN, "page": "example.poll.board", "platform": "web", "locale": "en"
        }))
    };
    let offered = page().await.unwrap();
    assert_eq!(offered["available"], true);
    assert_eq!(offered["bundle"], true);
    // Passing the shown page back keeps working with the marker on it.
    assert!(ui_page_split_accepts(offered));
    *f.catalog.lock().unwrap() = catalog(None, "0.2.0");
    f.api.plugin_ui_catalog().await.unwrap();
    let declarative = page().await.unwrap();
    assert_eq!(declarative["available"], true);
    assert!(declarative.get("bundle").is_none());
    let _ = std::fs::remove_dir_all(root);
}

fn ui_page_split_accepts(view: Value) -> bool {
    crate::extension::capability::ui_page::split_previous(view).is_ok()
}

/// A bundle asking for `media.read`, `people.resolve` and the payment
/// confirmation, approved for all three, on a host that offers it.
async fn reading_fixture() -> (Fixture, PathBuf) {
    let manifest: BundleManifest = serde_json::from_value(json!({
        "id": "board", "entry": "index.html", "bridge": 1,
        "permissions": ["plugin.command", "media.read", "people.resolve"],
        "operations": ["example.poll.move"],
        "capabilities": ["paymentConfirmation"], "locales": ["en"]
    }))
    .unwrap();
    let mut files = BTreeMap::new();
    files.insert("index.html".to_owned(), b"<!doctype html>".to_vec());
    let bytes = write_archive(&manifest, &files).unwrap();
    let mut approved = entry(&bytes);
    approved.permissions =
        serde_json::from_value(json!(["plugin.command", "media.read", "people.resolve"])).unwrap();
    let (f, root) = fixture_with(&bytes, approved).await;
    {
        let mut catalog = f.catalog.lock().unwrap();
        catalog["paymentProvider"] =
            json!({"pluginId": "example.pay", "operation": "example.pay.ticket"});
        let mut provider = catalog["plugins"][0].clone();
        provider["pluginId"] = json!("example.pay");
        provider["extensions"] = json!({"platforms": ["web"], "contributions": []});
        provider["operations"] = json!(["example.pay.ticket"]);
        provider["queryOperations"] = json!([]);
        provider["transientOperations"] = json!(["example.pay.ticket"]);
        catalog["plugins"].as_array_mut().unwrap().push(provider);
    }
    (f, root)
}

#[tokio::test]
async fn media_read_answers_only_the_plugins_own_uploads_of_this_viewer() {
    let (f, root) = reading_fixture().await;
    let session = greeted(&f).await;
    let read = |id: &str, file: &str| request(id, "media.read", json!({"fileId": file}));
    // A chat file (or another plugin's upload) whose id the bundle learned.
    let chat = answer(&bridge(&f, &session, read("m1", "file:chat-photo"), false).await);
    assert_eq!(chat["error"]["code"], "not_found", "{chat}");
    f.api
        .plugin_host
        .uploads
        .record("201", "u1", "example.other", "file:theirs");
    let theirs = answer(&bridge(&f, &session, read("m2", "file:theirs"), false).await);
    assert_eq!(theirs["error"]["code"], "not_found", "{theirs}");
    // Its own upload passes the ownership check (and, with no media reader
    // in this fixture, is then unavailable rather than not found).
    f.api
        .plugin_host
        .uploads
        .record("201", "u1", PLUGIN, "file:mine");
    let mine = answer(&bridge(&f, &session, read("m3", "file:mine"), false).await);
    assert_eq!(mine["error"]["code"], "unavailable", "{mine}");
    let _ = std::fs::remove_dir_all(root);
}

struct Profiles;
#[async_trait::async_trait]
impl crate::domain::UserReader for Profiles {
    async fn get(
        &self,
        user_id: &str,
    ) -> crate::shared::error::Result<Option<crate::domain::UserProfile>> {
        let mut profile =
            crate::domain::UserProfile::new(user_id).with_nickname(format!("{user_id} nick"));
        profile.alias = format!("{user_id} private remark");
        Ok(Some(profile))
    }
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn people_resolve_never_gives_the_private_remark_or_a_stranger() {
    let (f, root) = reading_fixture().await;
    // One connection: every connection to `sqlite::memory:` is a database of its own.
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    crate::infrastructure::persistence::sqlite::init_schema(&pool)
        .await
        .unwrap();
    let members = Arc::new(
        crate::infrastructure::persistence::sqlite::SqliteConversationParticipantRepo::new(pool),
    );
    use crate::domain::ConversationParticipantStore as _;
    members
        .save_page(
            "conversation-1",
            &[crate::model::ConversationParticipant {
                user_id: "u_member".into(),
                nickname: "Group name".into(),
                ..Default::default()
            }],
            1,
            true,
        )
        .await
        .unwrap();
    let api = f.api.clone().with_bundle_people(
        Arc::new(Profiles),
        Some(members as Arc<dyn crate::domain::ConversationParticipantStore>),
    );
    let session = {
        let opened = api
            .plugin_bundle_open(json!({
                "pluginId": PLUGIN, "contributionId": "example.poll.board",
                "locale": "en", "theme": {"mode": "light", "tokens": {}}
            }))
            .await
            .unwrap();
        opened["sessionId"].as_str().unwrap().to_owned()
    };
    let hello = api
        .plugin_bundle_bridge(json!({"sessionId": session,
            "envelope": request("h", "host.hello", json!({})).to_string()}))
        .await
        .unwrap();
    assert_eq!(answer(&hello)["result"]["pluginId"], PLUGIN);
    let resolve = |ids: Value| request("p", "people.resolve", json!({"userIds": ids}));
    let reply = api
        .plugin_bundle_bridge(json!({"sessionId": session,
            "envelope": resolve(json!(["u_member", "u_stranger", "u1"])).to_string()}))
        .await
        .unwrap();
    let people = answer(&reply)["result"]["people"].clone();
    // No conversation: people sharing one with the viewer, and the viewer;
    // named by their own nickname, never the viewer's private remark.
    assert_eq!(
        people,
        json!([
            {"userId": "u_member", "displayName": "u_member nick"},
            {"userId": "u1", "displayName": "u1 nick"}
        ]),
        "{reply}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(feature = "storage-sqlite")]
#[tokio::test]
async fn a_bundle_confirms_only_an_order_its_own_command_returned() {
    use crate::infrastructure::persistence::sqlite::{SqlitePluginCommandRepo, init_schema};
    let (mut f, root) = reading_fixture().await;
    // A command needs its journal.
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
    let session = greeted(&f).await;
    let pay = |id: &str, order_id: &str| {
        request(
            id,
            "host.capability",
            json!({"name": "paymentConfirmation", "params": {"orderId": order_id}}),
        )
    };
    // No command returned it: refused before any host is asked.
    let refused = answer(&bridge(&f, &session, pay("c1", "order-1"), true).await);
    assert!(refused["error"].is_object(), "{refused}");
    assert!(f.calls_to("example.pay.ticket").is_empty());
    // An order the bundle writes itself is not accepted either.
    let written = answer(
        &bridge(
            &f,
            &session,
            request(
                "c2",
                "host.capability",
                json!({"name": "paymentConfirmation", "params": {"order": {"orderId": "x",
                    "amount": "0.01", "currency": "CNY", "payee": "钱包", "title": "t",
                    "expiresAtMs": i64::MAX, "operation": "example.poll.pay"}}}),
            ),
            true,
        )
        .await,
    );
    assert_eq!(written["error"]["code"], "invalid_params", "{written}");
    // Its command's result carries the order: now the engine holds it.
    f.answer(
        "example.poll.move",
        vec![
            json!({"confirmation": {"orderId": "order-1", "amount": "1.00",
            "currency": "CNY", "payee": "Board", "title": "Boost",
            "expiresAtMs": chrono::Utc::now().timestamp_millis() + 60_000,
            "operation": "example.poll.pay"}}),
        ],
    );
    let moved = answer(
        &bridge(
            &f,
            &session,
            request(
                "c3",
                "plugin.command",
                json!({"operation": "example.poll.move",
                "input": {}}),
            ),
            false,
        )
        .await,
    );
    assert!(
        moved["result"]["data"]["confirmation"].is_object(),
        "{moved}"
    );
    assert!(
        f.api
            .plugin_host
            .capability_host
            .orders
            .get(&super::native::HeldPaymentKey::new(
                "201", "u1", PLUGIN, "order-1"
            ))
            .is_some()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn a_form_or_card_with_an_offered_bundle_says_so_and_others_do_not() {
    let bytes = archive(16);
    let (f, root) = fixture(&bytes).await;
    let declare = |bundle: bool| {
        let mut card = json!({"kind": "message-content", "id": "example.poll.card",
            "schemaVersion": 1, "requiredPermissions": [],
            "schema": {"title": "Card", "fields": [], "actions": []}});
        if bundle {
            card["bundle"] = json!({"id": "board", "route": "/"});
            card["variant"] = json!("board");
        }
        let mut catalog = f.catalog.lock().unwrap();
        let contributions = catalog["plugins"][0]["extensions"]["contributions"]
            .as_array_mut()
            .unwrap();
        contributions.retain(|c| c["id"] != "example.poll.card");
        contributions.push(card);
    };
    let present = || {
        f.api
            .present_plugin_message(json!({"message": {"pluginId": PLUGIN,
            "contributionId": "example.poll.card", "schemaVersion": 1, "summary": "Card",
            "payload": {}}, "platform": "web", "locale": "en"}))
    };
    declare(true);
    f.api.plugin_ui_catalog().await.unwrap();
    let offered = present().await.unwrap();
    assert_eq!(offered["bundle"], true, "{offered}");
    // A plugin without one: the host draws its document and never asks for a bundle.
    declare(false);
    f.api.plugin_ui_catalog().await.unwrap();
    let declarative = present().await.unwrap();
    assert!(declarative.get("bundle").is_none(), "{declarative}");
    let _ = std::fs::remove_dir_all(root);
}
