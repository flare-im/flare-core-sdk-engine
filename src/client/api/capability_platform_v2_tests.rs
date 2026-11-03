//! Plugin platform v2 through `CapabilityApi`: viewer layers (`viewerState`)
//! read in the background and merged by version, card calls carrying their
//! message, read-only unbound copies, inert views with a reason, the lenient
//! and cached catalog, account entries and localized plugin failures, against
//! a local dispatch endpoint.
use super::*;
use crate::domain::{MessageReader, MessageStore, MessageWriter};
use crate::infrastructure::persistence::memory_im::MemoryMessageStore;
use crate::kernel::event::{EventBus, EventReceiver, MessageEvent, SdkEvent};
use flare_plugin_protocol::{PluginManifest, state::SharedState};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex as SyncMutex;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

const CATALOG: &str = flare_plugin_protocol::ui::UI_CATALOG_OPERATION;

/// The protocol's v2 acceptance manifest plus a viewer query, a wallet page
/// with its account entry and query, as a catalog plugin (JSON, so tests can
/// also serve what this client cannot read).
fn catalog_plugin(refresh_on: Value) -> Value {
    let mut manifest: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../flare-protocol/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
    )))
    .unwrap();
    let operations = manifest["capabilities"][0]["operations"]
        .as_array_mut()
        .unwrap();
    for id in ["view", "wallet"] {
        operations.push(json!({
            "id": id, "idempotent": true, "inputSchema": {"type": "object"},
            "outputSchema": {"type": "object"}, "permissionIds": ["example.poll.read"],
            "timeoutMs": 5000, "mode": "query"
        }));
    }
    let contributions = manifest["clientExtensions"]["contributions"]
        .as_array_mut()
        .unwrap();
    for contribution in contributions.iter_mut() {
        if contribution["id"] == "example.poll.card" {
            contribution["viewerState"] = json!({
                "operation": "example.poll.view", "input": {"pollId": "/pollId"},
                "refreshOn": refresh_on, "minIntervalMs": 1000
            });
        }
    }
    contributions.push(json!({
        "kind": "page", "id": "example.poll.wallet", "schemaVersion": 1,
        "requiredPermissions": [], "schema": {"title": "钱包"}
    }));
    contributions.push(json!({
        "kind": "account-entry", "id": "example.poll.wallet-entry", "schemaVersion": 1,
        "requiredPermissions": [], "icon": "wallet", "order": 100,
        "schema": {"title": "钱包", "translations": {"en": {"title": "Wallet"}},
                   "actions": [{"id": "open", "label": "钱包",
                                "target": {"kind": "page", "page": "example.poll.wallet"}}]},
        "summary": {"operation": "example.poll.wallet", "path": "/balanceText"}
    }));
    let manifest = PluginManifest::from_json(&serde_json::to_vec(&manifest).unwrap()).unwrap();
    let extensions = manifest.client_extensions.clone();
    json!({
        "pluginId": manifest.plugin_id, "version": manifest.version,
        "manifestDigest": format!("sha256:{}", "b".repeat(64)), "grantRevision": 1,
        "extensions": extensions, "operations": manifest.operation_ids()
    })
}
fn catalog(plugins: Vec<Value>) -> Value {
    json!({
        "tenantId": "201", "version": 1,
        "validUntilMs": chrono::Utc::now().timestamp_millis() + 600_000,
        "plugins": plugins
    })
}

struct Fixture {
    api: CapabilityApi,
    catalog: Arc<SyncMutex<Value>>,
    calls: Arc<SyncMutex<Vec<Value>>>,
    results: Arc<SyncMutex<HashMap<String, Value>>>,
    /// HTTP status and machine reason per capability id.
    errors: Arc<SyncMutex<HashMap<String, (u16, String)>>>,
    store: Arc<MemoryMessageStore>,
    bus: EventBus,
    events: EventReceiver,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new(refresh_on: Value) -> Self {
        let catalog = Arc::new(SyncMutex::new(catalog(vec![catalog_plugin(refresh_on)])));
        let calls = Arc::new(SyncMutex::new(Vec::new()));
        let results = Arc::new(SyncMutex::new(HashMap::<String, Value>::new()));
        let errors = Arc::new(SyncMutex::new(HashMap::<String, (u16, String)>::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (c, q, r, e) = (
            catalog.clone(),
            calls.clone(),
            results.clone(),
            errors.clone(),
        );
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let mut len = 0;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).await.unwrap();
                    if header.trim().is_empty() {
                        break;
                    }
                    let (name, value) = header.trim().split_once(':').unwrap();
                    if name.eq_ignore_ascii_case("content-length") {
                        len = value.trim().parse().unwrap();
                    }
                }
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes).await.unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                q.lock().unwrap().push(body.clone());
                let capability = body["capabilityId"].as_str().unwrap().to_string();
                let (status, raw) = match e.lock().unwrap().get(&capability).cloned() {
                    Some((status, reason)) => (
                        status,
                        serde_json::to_vec(
                            &json!({"code": status, "reason": reason, "message": "x"}),
                        )
                        .unwrap(),
                    ),
                    None => {
                        let data = if capability == CATALOG {
                            c.lock().unwrap().clone()
                        } else {
                            r.lock()
                                .unwrap()
                                .get(&capability)
                                .cloned()
                                .unwrap_or_else(|| json!({}))
                        };
                        let result = CapabilityDispatchResult {
                            request_id: body["requestId"].as_str().unwrap().into(),
                            success: true,
                            plugin_id: "example.poll".into(),
                            capability_id: capability,
                            data,
                            error: None,
                        };
                        (
                            200,
                            serde_json::to_vec(&json!({"code": 0, "data": result})).unwrap(),
                        )
                    }
                };
                let mut socket = reader.into_inner();
                let header = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    raw.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&raw).await;
            }
        });
        let store = Arc::new(MemoryMessageStore::new());
        let bus = EventBus::new();
        let events = bus.subscribe();
        let user = Arc::new(RwLock::new("u1".to_string()));
        let api = CapabilityApi::new(
            "http://127.0.0.1:1",
            user.clone(),
            "201",
            Arc::new(HttpRequestContext::new()),
        )
        .with_http_transport(HttpClient::new(base))
        .with_message_state(store.clone(), bus.clone())
        .with_viewer_state(user);
        Self {
            api,
            catalog,
            calls,
            results,
            errors,
            store,
            bus,
            events,
            server,
        }
    }
    fn calls_to(&self, capability: &str) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c["capabilityId"] == capability)
            .cloned()
            .collect()
    }
    fn answer(&self, capability: &str, data: Value) {
        self.results.lock().unwrap().insert(capability.into(), data);
    }
    fn fail(&self, capability: &str, status: u16, reason: &str) {
        self.errors
            .lock()
            .unwrap()
            .insert(capability.into(), (status, reason.into()));
    }
    fn state_changes(&mut self) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            if let SdkEvent::Message(MessageEvent::PluginStateChanged {
                message_id,
                version,
                ..
            }) = event
            {
                out.push((message_id, version));
            }
        }
        out
    }
    async fn local(
        &self,
        message_id: &str,
    ) -> Option<flare_plugin_protocol::ui::PluginMessageState> {
        self.store
            .get(message_id)
            .await
            .unwrap()
            .and_then(|m| m.plugin_message_state())
    }
}

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    for _ in 0..300 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn poll() -> Value {
    json!({
        "pluginId": "example.poll", "contributionId": "example.poll.card", "schemaVersion": 1,
        "summary": "[Poll] Lunch?",
        "payload": {
            "pollId": "poll-1", "question": "Lunch?", "options": ["Rice", "Noodles"],
            "counts": [0, 0], "voterCount": 0, "multiple": false, "closed": false,
            "creatorId": "u2", "deadlineMs": 1_900_000_000_000_i64
        }
    })
}
fn card(server_id: &str) -> crate::model::IMMessage {
    let mut card = crate::model::IMMessage::new(flare_proto::common::Message::default());
    card.server_id = server_id.into();
    card.client_msg_id = format!("client-{server_id}");
    card.conversation_id = "group-1".into();
    card.content = Some(crate::model::Elem::Custom(
        crate::content::message_elem::CustomElem {
            r#type: "flare.plugin.message.v1".into(),
            payload: serde_json::to_vec(&poll()).unwrap(),
            description: String::new(),
            metadata: Default::default(),
        },
    ));
    card
}
fn present(message_id: &str) -> Value {
    json!({"message": poll(), "platform": "ios", "locale": "en-US", "messageId": message_id})
}
fn shared(version: u64, values: Value) -> SharedState {
    SharedState {
        entity: "poll-1".into(),
        version,
        values: values.as_object().unwrap().clone(),
    }
}

#[tokio::test]
async fn a_presented_card_reads_its_viewer_layer_once_and_merges_it() {
    let mut f = Fixture::new(json!(["present", "sharedVersion"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.answer(
        "example.poll.view",
        json!({"canClose": true, "hasVoted": false, "basedOnVersion": 0}),
    );

    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(view["available"], true);
    eventually("the viewer layer", async || {
        f.local("server-1").await.is_some()
    })
    .await;
    let layer = f.local("server-1").await.unwrap();
    assert_eq!(layer.based_on_version, Some(0));
    assert_eq!(layer.values["canClose"], true);
    assert!(layer.values.get("basedOnVersion").is_none());
    let reads = f.calls_to("example.poll.view");
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0]["payload"], json!({"pollId": "poll-1"}));
    assert_eq!(
        reads[0]["messageId"], "server-1",
        "a card call names its message"
    );
    assert_eq!(reads[0]["conversationId"], "group-1");
    eventually("the announcement", async || {
        f.state_changes().contains(&("server-1".to_string(), 0))
    })
    .await;

    // Presented again (by the client message id too): fresh, nothing more to read.
    let view = f
        .api
        .present_plugin_message(present("client-server-1"))
        .await
        .unwrap();
    assert_eq!(view["data"]["canClose"], true);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(f.calls_to("example.poll.view").len(), 1);
}

#[tokio::test]
async fn a_newer_shared_version_rereads_a_recent_card_and_stale_keys_yield() {
    let mut f = Fixture::new(json!(["present", "sharedVersion"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.answer(
        "example.poll.view",
        json!({"counts": [1, 0], "canClose": true, "basedOnVersion": 2}),
    );
    f.store
        .apply_plugin_shared_state("server-1", &shared(2, json!({"voterCount": 1})))
        .await
        .unwrap();
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    eventually("the first layer", async || {
        f.local("server-1")
            .await
            .is_some_and(|l| l.based_on_version == Some(2))
    })
    .await;
    let _ = f.state_changes();
    // Past the card's interval, so the next read is not deferred.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // Core publishes the final counts and the event of that change arrives: the
    // card, presented recently, is read again without being presented.
    f.answer(
        "example.poll.view",
        json!({"counts": [1, 1], "canClose": false, "basedOnVersion": 3}),
    );
    f.store
        .apply_plugin_shared_state(
            "server-1",
            &shared(
                3,
                json!({"voterCount": 2, "counts": [1, 1], "closed": true}),
            ),
        )
        .await
        .unwrap();
    f.bus
        .publish(SdkEvent::Message(MessageEvent::PluginStateChanged {
            conversation_id: "group-1".into(),
            message_id: "server-1".into(),
            version: 3,
        }));
    eventually("the second layer", async || {
        f.local("server-1")
            .await
            .is_some_and(|l| l.based_on_version == Some(3))
    })
    .await;
    assert_eq!(f.calls_to("example.poll.view").len(), 2);
    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(view["data"]["canClose"], false);
    assert_eq!(view["data"]["counts"], json!([1, 1]));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        f.calls_to("example.poll.view").len(),
        2,
        "fresh: not read again"
    );
}

#[tokio::test]
async fn a_plugin_cannot_pin_the_local_layer_fresh_past_the_shared_versions_known() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.store
        .apply_plugin_shared_state("server-1", &shared(2, json!({"voterCount": 1})))
        .await
        .unwrap();
    // A viewer read claiming a shared version this client never saw: capped at the one it saw.
    f.answer(
        "example.poll.view",
        json!({"counts": [9, 9], "basedOnVersion": 1_000_000_000_u64}),
    );
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    eventually("the viewer layer", async || {
        f.local("server-1").await.is_some()
    })
    .await;
    assert_eq!(f.local("server-1").await.unwrap().based_on_version, Some(2));
    // The next shared version makes it stale, so the shared counts win.
    f.store
        .apply_plugin_shared_state(
            "server-1",
            &shared(3, json!({"voterCount": 2, "counts": [1, 1]})),
        )
        .await
        .unwrap();
    f.fail("example.poll.view", 422, "POLL_GONE");
    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(view["data"]["counts"], json!([1, 1]));

    // An action result's messageState is capped the same way, at the shared version it
    // carries (or the stored one).
    f.answer(
        "example.poll.vote",
        json!({
            "sharedState": {"entity": "poll-1", "version": 4,
                            "values": {"voterCount": 3, "counts": [2, 1]}},
            "messageState": {"version": 6, "basedOnVersion": 1_000_000_000_u64,
                             "values": {"counts": [9, 9], "mySelection": [0]}}
        }),
    );
    f.api
        .invoke_plugin_action(json!({
            "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
            "conversationId": "group-1", "messageId": "server-1", "selection": [0]
        }))
        .await
        .unwrap();
    let layer = f.local("server-1").await.unwrap();
    assert_eq!((layer.version, layer.based_on_version), (6, Some(4)));
    f.answer(
        "example.poll.vote",
        json!({"messageState": {"version": 7, "basedOnVersion": 1_000_000_000_u64,
                                "values": {"mySelection": [1]}}}),
    );
    f.api
        .invoke_plugin_action(json!({
            "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
            "conversationId": "group-1", "messageId": "server-1", "selection": [1]
        }))
        .await
        .unwrap();
    let layer = f.local("server-1").await.unwrap();
    assert_eq!((layer.version, layer.based_on_version), (7, Some(4)));
}

#[tokio::test]
async fn a_stale_layer_never_hides_the_shared_value_of_its_keys() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.store
        .apply_plugin_shared_state(
            "server-1",
            &shared(
                3,
                json!({"voterCount": 2, "counts": [1, 1], "closed": true}),
            ),
        )
        .await
        .unwrap();
    // A layer computed from version 2 (the read is answered with a failure, so it stays).
    f.fail("example.poll.view", 422, "POLL_GONE");
    f.store
        .apply_plugin_message_state(
            "server-1",
            &flare_plugin_protocol::ui::PluginMessageState {
                version: 1,
                based_on_version: Some(2),
                values: json!({"counts": [1, 0], "canClose": true})
                    .as_object()
                    .unwrap()
                    .clone(),
            },
        )
        .await
        .unwrap();
    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(
        view["data"]["counts"],
        json!([1, 1]),
        "the shared counts win"
    );
    assert_eq!(
        view["data"]["canClose"], true,
        "a local-only key still applies"
    );
    assert_eq!(view["stateVersion"], 1);
    assert_eq!(view["sharedStateVersion"], 3);
}

#[tokio::test]
async fn without_shared_version_refresh_only_presenting_reads() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.answer(
        "example.poll.view",
        json!({"canClose": true, "basedOnVersion": 0}),
    );
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    eventually("the first layer", async || {
        f.local("server-1").await.is_some()
    })
    .await;
    f.store
        .apply_plugin_shared_state("server-1", &shared(1, json!({"voterCount": 1})))
        .await
        .unwrap();
    f.bus
        .publish(SdkEvent::Message(MessageEvent::PluginStateChanged {
            conversation_id: "group-1".into(),
            message_id: "server-1".into(),
            version: 1,
        }));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        f.calls_to("example.poll.view").len(),
        1,
        "no event-driven read"
    );
    // Presenting the now stale card reads it.
    f.answer(
        "example.poll.view",
        json!({"canClose": true, "basedOnVersion": 1}),
    );
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    eventually("the second read", async || {
        f.calls_to("example.poll.view").len() == 2
    })
    .await;
}

#[tokio::test]
async fn a_failed_viewer_read_writes_nothing_and_backs_off() {
    let f = Fixture::new(json!(["present", "sharedVersion"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.fail("example.poll.view", 503, "CAPABILITY_OUTCOME_UNKNOWN");
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    eventually("the read", async || {
        f.calls_to("example.poll.view").len() == 1
    })
    .await;
    for _ in 0..5 {
        f.api
            .present_plugin_message(present("server-1"))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(f.calls_to("example.poll.view").len(), 1, "backing off");
    assert!(f.local("server-1").await.is_none());
}

#[tokio::test]
async fn card_actions_and_details_name_their_message() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.answer(
        "example.poll.vote",
        json!({"posted": {"conversationId": "group-1"}}),
    );
    let result = f
        .api
        .invoke_plugin_action(json!({
            "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
            "conversationId": "group-1", "messageId": "client-server-1", "selection": [0]
        }))
        .await
        .unwrap();
    assert_eq!(result["kind"], "operation");
    assert_eq!(
        result["response"]["data"]["posted"]["conversationId"],
        "group-1"
    );
    let vote = f.calls_to("example.poll.vote");
    assert_eq!(
        vote[0]["messageId"], "server-1",
        "the stored server id, not the client id"
    );
    f.answer("example.poll.details", json!({"voters": []}));
    f.api
        .plugin_detail(json!({
            "message": poll(), "platform": "ios", "userGesture": true,
            "conversationId": "group-1", "messageId": "server-1"
        }))
        .await
        .unwrap();
    assert_eq!(
        f.calls_to("example.poll.details")[0]["messageId"],
        "server-1"
    );
    // A call not made from a card names no message.
    f.api
        .dispatch(
            "example.poll.create",
            json!({}),
            Some("group-1"),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        f.calls_to("example.poll.create")[0]
            .get("messageId")
            .is_none()
    );
}

#[tokio::test]
async fn an_unbound_copy_is_read_only_and_never_calls_the_plugin() {
    let f = Fixture::new(json!(["present"])).await;
    let mut copy = card("server-9");
    copy.attributes.insert(
        crate::model::message::PLUGIN_CARD_UNBOUND_KEY.into(),
        "true".into(),
    );
    f.store.save_one(&copy).await.unwrap();
    let view = f
        .api
        .present_plugin_message(present("server-9"))
        .await
        .unwrap();
    assert_eq!(view["available"], true);
    assert_eq!(view["unavailableReason"], "unbound_copy");
    assert!(view["actions"].as_array().unwrap().is_empty());
    assert!(view.get("menuActions").is_none());
    assert!(view.get("detail").is_none());
    for block in view["blocks"].as_array().unwrap() {
        assert!(block.get("action").is_none(), "{block}");
    }
    let refused = f
        .api
        .invoke_plugin_action(json!({
            "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
            "messageId": "server-9", "selection": [0]
        }))
        .await
        .unwrap_err();
    assert!(
        refused.to_string().contains("sdk.plugin.card_unbound"),
        "{refused}"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(f.calls_to("example.poll.view").is_empty());
    assert!(f.calls_to("example.poll.vote").is_empty());
    // The marker never goes back on the wire.
    assert!(
        !copy
            .to_proto()
            .attributes
            .contains_key(crate::model::message::PLUGIN_CARD_UNBOUND_KEY)
    );
}

#[tokio::test]
async fn an_unreadable_catalog_gives_an_inert_view_not_an_error() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.fail(CATALOG, 503, "UPSTREAM_DOWN");
    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(view["available"], false);
    assert_eq!(view["unavailableReason"], "catalog_unreachable");
    assert_eq!(view["title"], "[Poll] Lunch?");
    assert_eq!(view["stateVersion"], 0);
    // Acting needs the catalog: a localized failure.
    let error = f
        .api
        .invoke_plugin_action(json!({
            "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
            "selection": [0]
        }))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("sdk.plugin.catalog_unreachable"),
        "{error}"
    );
}

#[tokio::test]
async fn one_unreadable_plugin_costs_only_itself() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    let mut good = catalog_plugin(json!(["present"]));
    // A contribution kind this client does not know: dropped, the rest stays.
    good["extensions"]["contributions"]
        .as_array_mut()
        .unwrap()
        .push(
            json!({"kind": "future-kind", "id": "example.poll.future", "schemaVersion": 1,
                     "requiredPermissions": [], "schema": {"title": "x"}}),
        );
    let broken = json!({"pluginId": "example.broken", "version": "1.0.0", "unknownField": 1});
    *f.catalog.lock().unwrap() = json!({
        "tenantId": "201", "version": 2,
        "validUntilMs": chrono::Utc::now().timestamp_millis() + 600_000,
        "plugins": [good, broken], "futureTopLevel": true,
        "unavailable": [{"pluginId": "example.off", "reason": "disabled"}, {"bad": 1}]
    });
    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(view["available"], true, "{view}");
    let mut broken_card = poll();
    broken_card["pluginId"] = json!("example.broken");
    broken_card["contributionId"] = json!("example.broken.card");
    let view = f
        .api
        .present_plugin_message(json!({"message": broken_card, "platform": "ios"}))
        .await
        .unwrap();
    assert_eq!(view["unavailableReason"], "incompatible_version");
    let mut off = poll();
    off["pluginId"] = json!("example.off");
    off["contributionId"] = json!("example.off.card");
    let view = f
        .api
        .present_plugin_message(json!({"message": off, "platform": "ios"}))
        .await
        .unwrap();
    assert_eq!(view["unavailableReason"], "disabled");
}

#[tokio::test]
async fn concurrent_presentations_share_one_catalog_read_and_keep_none() {
    let mut f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    f.store.save_one(&card("server-2")).await.unwrap();
    f.answer("example.poll.view", json!({"basedOnVersion": 0}));
    let (a, b) = tokio::join!(
        f.api.present_plugin_message(present("server-1")),
        f.api.present_plugin_message(present("server-2"))
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        f.calls_to(CATALOG).len(),
        1,
        "one read shared by a screen of cards"
    );
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(
        f.calls_to(CATALOG).len(),
        2,
        "no catalog is kept after its read"
    );
    eventually("the viewer layers", async || {
        f.local("server-1").await.is_some() && f.local("server-2").await.is_some()
    })
    .await;
    let _ = f.state_changes();

    // The host says the catalog may have changed: every card presented recently is announced.
    f.api.invalidate_plugin_catalog().await.unwrap();
    let announced: Vec<String> = f.state_changes().into_iter().map(|(id, _)| id).collect();
    assert!(announced.contains(&"server-1".to_string()), "{announced:?}");
    assert!(announced.contains(&"server-2".to_string()), "{announced:?}");

    // A plugin answering "disabled" does the same for its cards by itself.
    f.fail("example.poll.vote", 403, "PLUGIN_DISABLED");
    let error = f
        .api
        .invoke_plugin_action(json!({
            "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
            "messageId": "server-1", "selection": [0]
        }))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("sdk.plugin.disabled"), "{error}");
    assert!(f.state_changes().iter().any(|(id, _)| id == "server-1"));
    // A card that turned inert is still remembered, and announced again on the next change.
    f.fail(CATALOG, 503, "UPSTREAM_DOWN");
    let inert = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(inert["unavailableReason"], "catalog_unreachable");
    f.api.invalidate_plugin_catalog().await.unwrap();
    assert!(f.state_changes().iter().any(|(id, _)| id == "server-1"));
}

#[tokio::test]
async fn plugin_rejections_come_back_as_localized_keys() {
    let f = Fixture::new(json!(["present"])).await;
    f.store.save_one(&card("server-1")).await.unwrap();
    for (status, reason, key) in [
        (422, "POLL_CLOSED", "sdk.plugin.rejected"),
        (409, "PLUGIN_CARD_UNBOUND", "sdk.plugin.card_unbound"),
        (429, "CAPABILITY_RATE_LIMITED", "sdk.plugin.rate_limited"),
    ] {
        f.fail("example.poll.vote", status, reason);
        let error = f
            .api
            .invoke_plugin_action(json!({
                "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
                "messageId": "server-1", "selection": [0]
            }))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(key), "{reason}: {error}");
        assert_eq!(
            crate::extension::capability::plugin_errors::machine_reason(&error),
            Some(reason)
        );
    }
    // A plugin answering a platform code is only a rejection: the card is not said to be
    // rolled back, and the plugin's recent cards are not re-announced as if it were disabled.
    let mut f = f;
    f.answer("example.poll.view", json!({"basedOnVersion": 0}));
    f.api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    eventually("the viewer layer", async || {
        f.local("server-1").await.is_some()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    for reason in ["PLUGIN_CARD_NOT_POSTED", "PLUGIN_DISABLED"] {
        let _ = f.state_changes();
        f.fail("example.poll.vote", 422, reason);
        let error = f
            .api
            .invoke_plugin_action(json!({
                "message": poll(), "platform": "ios", "actionId": "vote", "userGesture": true,
                "messageId": "server-1", "selection": [0]
            }))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("sdk.plugin.rejected"),
            "{reason}: {error}"
        );
        assert_eq!(
            crate::extension::capability::plugin_errors::machine_reason(&error),
            Some("PLUGIN_REJECTED")
        );
        assert!(f.state_changes().is_empty(), "{reason}");
    }
}

#[tokio::test]
async fn account_entries_list_the_catalog_entries_for_the_platform() {
    let f = Fixture::new(json!(["present"])).await;
    let listed = f
        .api
        .plugin_account_entries(json!({"platform": "ios", "locale": "en-US"}))
        .await
        .unwrap();
    assert_eq!(
        listed,
        json!({"entries": [{
            "pluginId": "example.poll", "contributionId": "example.poll.wallet-entry",
            "title": "Wallet", "icon": "wallet", "order": 100, "page": "example.poll.wallet",
            "summary": {"operation": "example.poll.wallet", "path": "/balanceText"}
        }]})
    );
    assert!(
        f.calls_to("example.poll.wallet").is_empty(),
        "listing never calls the summary"
    );
    let zh = f
        .api
        .account_entries(
            flare_plugin_protocol::manifest::ClientPlatform::Ios,
            "zh-CN",
        )
        .await
        .unwrap();
    assert_eq!(zh[0].title, "钱包");
    // Another platform the plugin does not list: nothing.
    let mut catalog = f.catalog.lock().unwrap().clone();
    catalog["plugins"][0]["extensions"]["platforms"] = json!(["web"]);
    *f.catalog.lock().unwrap() = catalog;
    assert!(
        f.api
            .account_entries(flare_plugin_protocol::manifest::ClientPlatform::Ios, "en")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        f.api
            .plugin_account_entries(json!({"platform": "nope"}))
            .await
            .is_err()
    );
}
