//! Card protocol v2 through `CapabilityApi`: two state layers, blocks, forms, choices, detail,
//! audience, forwardability and the client-visible configuration, against the protocol's v2
//! acceptance manifest served by a local dispatch endpoint.
use super::*;
use crate::domain::{MessageReader, MessageStore, MessageWriter, OperationApplyResult};
use crate::infrastructure::persistence::memory_im::MemoryMessageStore;
use crate::kernel::event::{EventBus, EventReceiver, MessageEvent, SdkEvent};
use flare_plugin_protocol::{
    PluginManifest,
    state::SharedState,
    ui::{ClientPlugin, PluginMessageState, UiCatalog},
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex as SyncMutex;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

struct Fixture {
    api: CapabilityApi,
    catalog: Arc<SyncMutex<UiCatalog>>,
    calls: Arc<SyncMutex<Vec<Value>>>,
    /// `data` returned per capability id; a missing entry answers `{}`.
    results: Arc<SyncMutex<HashMap<String, Value>>>,
    /// `data` per page, keyed `<capability id>#<payload cursor>` (`#` alone for the first
    /// page); takes precedence over `results`.
    pages: Arc<SyncMutex<HashMap<String, Value>>>,
    /// Capability ids, or page keys as in `pages`, that answer `success: false`.
    failing: Arc<SyncMutex<Vec<String>>>,
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
    async fn new() -> Self {
        let manifest = PluginManifest::from_json(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../flare-protocol/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
        )))
        .unwrap();
        let extensions = manifest.client_extensions;
        let catalog = Arc::new(SyncMutex::new(UiCatalog {
            tenant_id: "201".into(),
            version: 1,
            valid_until_ms: chrono::Utc::now().timestamp_millis() + 60000,
            plugins: vec![ClientPlugin {
                plugin_id: manifest.plugin_id,
                version: manifest.version,
                manifest_digest: format!("sha256:{}", "b".repeat(64)),
                grant_revision: 1,
                extensions,
                operations: [
                    "example.poll.create",
                    "example.poll.vote",
                    "example.poll.close",
                    "example.poll.details",
                ]
                .into_iter()
                .map(String::from)
                .collect(),
                configuration: json!({"maxOptions": 10, "allowAnonymous": true})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
            unavailable: vec![],
        }));
        let calls = Arc::new(SyncMutex::new(Vec::new()));
        let results = Arc::new(SyncMutex::new(HashMap::<String, Value>::new()));
        let pages = Arc::new(SyncMutex::new(HashMap::<String, Value>::new()));
        let failing = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (c, q, r, p, x) = (
            catalog.clone(),
            calls.clone(),
            results.clone(),
            pages.clone(),
            failing.clone(),
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
                let page = format!(
                    "{capability}#{}",
                    body["payload"]["cursor"].as_str().unwrap_or_default()
                );
                let data = if capability == flare_plugin_protocol::ui::UI_CATALOG_OPERATION {
                    serde_json::to_value(c.lock().unwrap().clone()).unwrap()
                } else if let Some(data) = p.lock().unwrap().get(&page).cloned() {
                    data
                } else {
                    r.lock()
                        .unwrap()
                        .get(&capability)
                        .cloned()
                        .unwrap_or_else(|| json!({}))
                };
                let failed = {
                    let failing = x.lock().unwrap();
                    failing.contains(&capability) || failing.contains(&page)
                };
                let result = CapabilityDispatchResult {
                    request_id: body["requestId"].as_str().unwrap().into(),
                    success: !failed,
                    plugin_id: "example.poll".into(),
                    capability_id: capability,
                    data,
                    error: None,
                };
                let raw = serde_json::to_vec(&json!({"code":0,"data":result})).unwrap();
                let mut socket = reader.into_inner();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    raw.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&raw).await;
            }
        });
        let store = Arc::new(MemoryMessageStore::new());
        let bus = EventBus::new();
        let events = bus.subscribe();
        let api = CapabilityApi::new(
            "http://127.0.0.1:1",
            Arc::new(RwLock::new("u1".into())),
            "201",
            Arc::new(HttpRequestContext::new()),
        )
        .with_http_transport(HttpClient::new(base))
        .with_message_state(store.clone(), bus.clone());
        Self {
            api,
            catalog,
            calls,
            results,
            pages,
            failing,
            store,
            bus,
            events,
            server,
        }
    }
    fn business_calls(&self) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c["capabilityId"] != flare_plugin_protocol::ui::UI_CATALOG_OPERATION)
            .cloned()
            .collect()
    }
    fn state_changes(&mut self) -> Vec<u64> {
        let mut out = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            if let SdkEvent::Message(MessageEvent::PluginStateChanged { version, .. }) = event {
                out.push(version);
            }
        }
        out
    }
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

fn card(server_id: &str, envelope: &Value) -> crate::model::IMMessage {
    let mut card = crate::model::IMMessage::new(flare_proto::common::Message::default());
    card.server_id = server_id.into();
    card.client_msg_id = format!("client-{server_id}");
    card.conversation_id = "group-1".into();
    card.content = Some(crate::model::Elem::Custom(
        crate::content::message_elem::CustomElem {
            r#type: "flare.plugin.message.v1".into(),
            payload: serde_json::to_vec(envelope).unwrap(),
            description: String::new(),
            metadata: Default::default(),
        },
    ));
    card
}

fn shared(entity: &str, version: u64, values: Value) -> SharedState {
    SharedState {
        entity: entity.into(),
        version,
        values: values.as_object().unwrap().clone(),
    }
}

fn local(version: u64, values: Value) -> PluginMessageState {
    PluginMessageState {
        version,
        values: values.as_object().unwrap().clone(),
        based_on_version: None,
    }
}

fn present(message_id: &str) -> Value {
    json!({"message": poll(), "platform": "ios", "locale": "en-US", "messageId": message_id})
}

fn block<'a>(view: &'a Value, kind: &str) -> &'a Value {
    view["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["kind"] == kind)
        .unwrap_or_else(|| panic!("no {kind} block in {view}"))
}

#[tokio::test]
async fn present_resolves_v2_blocks_over_payload_shared_and_local_layers() {
    let f = Fixture::new().await;
    f.store.save_one(&card("server-1", &poll())).await.unwrap();

    // The card as sent: no state yet.
    let fresh = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(fresh["available"], true);
    assert_eq!(
        (
            fresh["stateVersion"].clone(),
            fresh["sharedStateVersion"].clone()
        ),
        (json!(0), json!(0))
    );
    assert_eq!(fresh["detail"], true);
    let choices = block(&fresh, "choices");
    assert_eq!(choices["voted"], false);
    assert_eq!(
        choices["showResults"], false,
        "results show once the viewer voted"
    );
    assert_eq!(
        choices["action"]["id"], "vote",
        "the block owns the vote action"
    );
    assert!(
        fresh["actions"].as_array().unwrap().is_empty(),
        "vote belongs to the block; close is hidden from non-creators"
    );
    assert_eq!(block(&fresh, "note")["text"], "Anonymous poll");

    // Shared layer from Core, then the viewer's own selection on top.
    assert_eq!(
        f.store
            .apply_plugin_shared_state(
                "server-1",
                &shared("poll-1", 3, json!({"counts": [2, 1], "voterCount": 3}))
            )
            .await
            .unwrap(),
        OperationApplyResult::Applied
    );
    f.store
        .apply_plugin_message_state("server-1", &local(1, json!({"mySelection": [0]})))
        .await
        .unwrap();
    let view = f
        .api
        .present_plugin_message(present("client-server-1"))
        .await
        .unwrap();
    assert_eq!(
        (
            view["stateVersion"].clone(),
            view["sharedStateVersion"].clone()
        ),
        (json!(1), json!(3))
    );
    assert_eq!(view["data"]["counts"], json!([2, 1]));
    assert_eq!(view["data"]["mySelection"], json!([0]));
    assert_eq!(
        view["data"]["question"], "Lunch?",
        "the payload stays underneath"
    );
    let choices = block(&view, "choices");
    assert_eq!(choices["voted"], true);
    assert_eq!(choices["total"], 3);
    assert_eq!(
        choices["options"],
        json!([
            {"index": 0, "label": "Rice", "selected": true, "count": 2, "percent": 67},
            {"index": 1, "label": "Noodles", "selected": false, "count": 1, "percent": 33}
        ])
    );

    // Core closes the poll: the choices stop accepting votes everywhere.
    f.store
        .apply_plugin_shared_state(
            "server-1",
            &shared(
                "poll-1",
                4,
                json!({"counts": [2, 1], "voterCount": 3, "closed": true}),
            ),
        )
        .await
        .unwrap();
    let closed = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(block(&closed, "choices")["closed"], true);
    assert!(block(&closed, "choices").get("action").is_none());
    assert_eq!(block(&closed, "status")["text"], "Closed");

    // History delivers the snapshot without an entity: the card's own entity applies.
    let mut from_history = card("server-2", &poll());
    from_history.attributes.insert(
        "pluginSharedStateJson".into(),
        r#"{"counts":[5,0],"voterCount":5}"#.into(),
    );
    from_history
        .attributes
        .insert("pluginSharedStateVersion".into(), "9".into());
    f.store.save_one(&from_history).await.unwrap();
    let mut viewer_voted = f
        .api
        .present_plugin_message(present("server-2"))
        .await
        .unwrap();
    assert_eq!(viewer_voted["sharedStateVersion"], 9);
    assert_eq!(viewer_voted["data"]["voterCount"], 5);
    viewer_voted = block(&viewer_voted, "choices").clone();
    assert_eq!(viewer_voted["voted"], false);

    // Another entity's state never shows on this card.
    f.store.save_one(&card("server-3", &poll())).await.unwrap();
    f.store
        .apply_plugin_shared_state(
            "server-3",
            &shared("poll-other", 2, json!({"voterCount": 99})),
        )
        .await
        .unwrap();
    let foreign = f
        .api
        .present_plugin_message(present("server-3"))
        .await
        .unwrap();
    assert_eq!(foreign["data"]["voterCount"], 0);

    // Console pages are no contribution kind.
    let mut admin = present("server-1");
    admin["kind"] = json!("admin-page");
    assert!(f.api.present_plugin_message(admin).await.is_err());
    assert!(
        f.business_calls().is_empty(),
        "presenting never invokes the plugin"
    );
}

#[tokio::test]
async fn invoke_binds_the_selection_and_applies_the_result_shared_state() {
    let mut f = Fixture::new().await;
    f.store.save_one(&card("server-1", &poll())).await.unwrap();
    let vote = |selection: Value| {
        json!({"message": poll(), "platform": "android", "actionId": "vote", "userGesture": true,
            "conversationId": "group-1", "messageId": "server-1", "selection": selection})
    };
    f.results.lock().unwrap().insert(
        "example.poll.vote".into(),
        json!({
            "sharedState": {"entity": "poll-1", "version": 4, "values": {"counts": [0, 1], "voterCount": 1}},
            "messageState": {"version": 1, "values": {"mySelection": [1]}}
        }),
    );
    let result = f.api.invoke_plugin_action(vote(json!([1]))).await.unwrap();
    assert_eq!(result["kind"], "operation");
    let calls = f.business_calls();
    assert_eq!(calls[0]["capabilityId"], "example.poll.vote");
    assert_eq!(calls[0]["conversationId"], "group-1");
    assert_eq!(
        calls[0]["payload"],
        json!({"pollId": "poll-1", "selection": [1]})
    );
    assert_eq!(f.state_changes(), [1], "one announcement for both layers");
    let stored = f.store.get("server-1").await.unwrap().unwrap();
    assert_eq!(stored.plugin_shared_state(None).unwrap().version, 4);
    assert_eq!(stored.plugin_message_state().unwrap().version, 1);
    let view = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(block(&view, "choices")["options"][1]["selected"], true);
    assert_eq!(block(&view, "choices")["options"][1]["count"], 1);

    // The broadcast of the same version, an older one, or another entity's state change nothing.
    for state in [
        json!({"entity": "poll-1", "version": 4, "values": {"voterCount": 50}}),
        json!({"entity": "poll-1", "version": 2, "values": {"voterCount": 50}}),
        json!({"entity": "poll-2", "version": 9, "values": {"voterCount": 50}}),
        json!({"entity": "poll-1", "version": 9, "values": {"nested": {"a": 1}}}),
    ] {
        f.results
            .lock()
            .unwrap()
            .insert("example.poll.vote".into(), json!({"sharedState": state}));
        f.api.invoke_plugin_action(vote(json!([0]))).await.unwrap();
    }
    assert!(f.state_changes().is_empty());
    let stored = f.store.get("server-1").await.unwrap().unwrap();
    assert_eq!(
        stored.plugin_shared_state(None).unwrap().values["voterCount"],
        1
    );
    // Without the message, a result's shared state is not applied anywhere.
    f.results.lock().unwrap().insert(
        "example.poll.vote".into(),
        json!({"sharedState": {"entity": "poll-1", "version": 8, "values": {"voterCount": 8}}}),
    );
    let mut detached = vote(json!([0]));
    detached.as_object_mut().unwrap().remove("messageId");
    f.api.invoke_plugin_action(detached).await.unwrap();
    assert!(f.state_changes().is_empty());

    // Bad selections never reach the plugin.
    let before = f.business_calls().len();
    let mut missing = vote(json!([0]));
    missing.as_object_mut().unwrap().remove("selection");
    assert!(f.api.invoke_plugin_action(missing).await.is_err());
    for bad in [json!([]), json!([2]), json!([0, 1]), json!([0, 0])] {
        assert!(f.api.invoke_plugin_action(vote(bad)).await.is_err());
    }
    let mut not_choices = vote(json!([0]));
    not_choices["actionId"] = json!("close");
    assert!(f.api.invoke_plugin_action(not_choices).await.is_err());
    assert_eq!(f.business_calls().len(), before);
}

#[tokio::test]
async fn invoke_validates_the_form_before_sending_anything() {
    let f = Fixture::new().await;
    let create = |values: Value| {
        json!({
            "message": {"pluginId": "example.poll", "contributionId": "example.poll.create", "schemaVersion": 1,
                "summary": "Poll", "payload": {"conversationId": "group-1"}},
            "platform": "web", "actionId": "create", "userGesture": true, "conversationId": "group-1",
            "formValues": values
        })
    };
    let rejected = f.api.invoke_plugin_action(create(json!({}))).await.unwrap();
    assert_eq!(rejected["kind"], "formErrors");
    assert_eq!(
        rejected["errors"],
        json!([
            {"input": "question", "issue": "required"},
            {"input": "options", "issue": "invalidItem"}
        ]),
        "every problem at once; the placeholder options are not answers"
    );
    let unknown = f
        .api
        .invoke_plugin_action(create(
            json!({"question": "Lunch?", "options": ["A", "B"], "tenantId": "202"}),
        ))
        .await
        .unwrap();
    assert_eq!(
        unknown["errors"],
        json!([{"input": "tenantId", "issue": "unknown"}])
    );
    let duplicate = f
        .api
        .invoke_plugin_action(create(
            json!({"question": "Lunch?", "options": ["A", " A "]}),
        ))
        .await
        .unwrap();
    assert_eq!(
        duplicate["errors"],
        json!([{"input": "options", "issue": "duplicateItem"}])
    );
    assert!(f.business_calls().is_empty(), "invalid forms send nothing");

    f.results.lock().unwrap().insert(
        "example.poll.create".into(),
        json!({"message": {"pluginId": "example.poll", "contributionId": "example.poll.card"}}),
    );
    let created = f
        .api
        .invoke_plugin_action(create(
            json!({"question": "Lunch?", "options": ["Rice", "Noodles"], "multiple": true}),
        ))
        .await
        .unwrap();
    assert_eq!(created["kind"], "operation");
    let calls = f.business_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["payload"],
        json!({"question": "Lunch?", "options": ["Rice", "Noodles"], "multiple": true})
    );
    // A form value cannot target an action without a form.
    let card_vote = json!({"message": poll(), "platform": "web", "actionId": "vote", "userGesture": true,
        "selection": [0], "formValues": {"pollId": "poll-9"}});
    let forged = f.api.invoke_plugin_action(card_vote).await.unwrap();
    assert_eq!(
        forged["errors"],
        json!([{"input": "pollId", "issue": "unknown"}])
    );
    assert_eq!(f.business_calls().len(), 1);
}

#[tokio::test]
async fn detail_runs_the_detail_operation_and_resolves_its_document() {
    let f = Fixture::new().await;
    f.store.save_one(&card("server-1", &poll())).await.unwrap();
    f.results.lock().unwrap().insert(
        "example.poll.details".into(),
        json!({"voters": [{"option": "Rice", "userId": "u2"}, {"option": "Noodles", "userId": "u3"}]}),
    );
    let request = json!({"message": poll(), "platform": "web", "locale": "en-US", "userGesture": true,
        "messageId": "server-1", "conversationId": "group-1"});
    let view = f.api.plugin_detail(request.clone()).await.unwrap();
    assert_eq!(view["available"], true);
    assert_eq!(view["title"], "Poll details");
    assert_eq!(
        block(&view, "entries")["items"],
        json!([
            {"index": 1, "text": "Rice", "userId": "u2"},
            {"index": 2, "text": "Noodles", "userId": "u3"}
        ])
    );
    let calls = f.business_calls();
    assert_eq!(calls[0]["capabilityId"], "example.poll.details");
    assert_eq!(calls[0]["payload"], json!({"pollId": "poll-1"}));
    assert_eq!(calls[0]["conversationId"], "group-1");

    let mut empty = request.clone();
    f.results
        .lock()
        .unwrap()
        .insert("example.poll.details".into(), json!({"voters": []}));
    let none = f.api.plugin_detail(empty.clone()).await.unwrap();
    assert_eq!(block(&none, "entries")["emptyText"], "No votes yet");

    empty["userGesture"] = json!(false);
    assert!(
        f.api.plugin_detail(empty).await.is_err(),
        "detail needs a gesture"
    );
    let mut no_detail = request.clone();
    no_detail["message"]["contributionId"] = json!("example.poll.create");
    assert!(f.api.plugin_detail(no_detail).await.is_err());
    f.failing
        .lock()
        .unwrap()
        .push("example.poll.details".into());
    assert!(
        f.api.plugin_detail(request.clone()).await.is_err(),
        "a plugin failure is an error"
    );
    f.catalog.lock().unwrap().plugins[0]
        .operations
        .remove("example.poll.details");
    assert!(f.api.plugin_detail(request).await.is_err());
}

#[tokio::test]
async fn catalog_carries_audience_configuration_and_forwardability() {
    use flare_plugin_protocol::manifest::ClientPlatform;
    let f = Fixture::new().await;
    let actions = f
        .api
        .plugin_composer_actions(json!({"platform": "ios", "locale": "en-US"}))
        .await
        .unwrap();
    assert_eq!(
        actions["actions"],
        json!([{"pluginId": "example.poll", "contributionId": "example.poll.create", "title": "Poll",
            "icon": "poll", "order": 110, "audience": {"conversationTypes": ["group"]}}])
    );
    let catalog = f.api.plugin_ui_catalog().await.unwrap();
    assert_eq!(
        catalog["plugins"][0]["configuration"],
        json!({"maxOptions": 10, "allowAnonymous": true}),
        "hosts read the client-visible configuration from the catalog"
    );

    let message: flare_plugin_protocol::ui::PluginMessage = serde_json::from_value(poll()).unwrap();
    assert!(
        !f.api.plugin_card_forwardable(&message).await,
        "plugin cards stay put by default"
    );
    f.catalog.lock().unwrap().plugins[0]
        .extensions
        .contributions[0]
        .forwardable = true;
    assert!(f.api.plugin_card_forwardable(&message).await);
    f.catalog.lock().unwrap().valid_until_ms = 1;
    assert!(
        f.api
            .composer_actions(ClientPlatform::Ios, "")
            .await
            .unwrap()
            .is_empty()
    );
    f.catalog.lock().unwrap().tenant_id = "202".into();
    let mut other = message;
    other.contribution_id = "example.poll.unknown".into();
    assert!(!f.api.plugin_card_forwardable(&other).await);
}

fn composer() -> Value {
    json!({"pluginId": "example.poll", "contributionId": "example.poll.create", "schemaVersion": 1,
        "summary": "Poll", "payload": {"conversationId": "group-1"}})
}
fn contribution_mut<'a>(
    catalog: &'a mut UiCatalog,
    id: &str,
) -> &'a mut flare_plugin_protocol::manifest::UiContribution {
    catalog.plugins[0]
        .extensions
        .contributions
        .iter_mut()
        .find(|c| c.id == id)
        .unwrap()
}
fn input<'a>(view: &'a Value, id: &str) -> &'a Value {
    view["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|input| input["id"] == id)
        .unwrap_or_else(|| panic!("no {id} input in {view}"))
}

/// A composer form is resolved, validated and bound over `{"config": ...}`, the plugin's
/// client-visible configuration, so `maxPath` and `defaultPath` read `/config/<key>` both in the
/// form the host renders and in the check before sending.
#[tokio::test]
async fn composer_forms_read_the_client_visible_configuration() {
    let f = Fixture::new().await;
    {
        let mut catalog = f.catalog.lock().unwrap();
        catalog.plugins[0].configuration = json!({"maxOptions": 3, "allowAnonymous": true})
            .as_object()
            .unwrap()
            .clone();
        let create = contribution_mut(&mut catalog, "example.poll.create");
        for input in &mut create.schema.inputs {
            match input.id.as_str() {
                "options" => input.max_path = Some("/config/maxOptions".into()),
                "multiple" => input.default_path = Some("/config/allowAnonymous".into()),
                _ => {}
            }
        }
    }
    let present = json!({"message": composer(), "platform": "web", "kind": "composer-action"});
    let view = f.api.present_plugin_message(present.clone()).await.unwrap();
    assert_eq!(view["available"], true);
    assert_eq!(
        view["data"],
        json!({"config": {"maxOptions": 3, "allowAnonymous": true}}),
        "the data a composer form is resolved against, not its message payload"
    );
    assert_eq!(
        input(&view, "options")["max"],
        3,
        "maxPath over the configuration"
    );
    assert!(input(&view, "options").get("maxPath").is_none());
    assert_eq!(
        input(&view, "multiple")["default"],
        true,
        "defaultPath over the configuration"
    );

    let create = |values: Value| {
        json!({"message": composer(), "platform": "web", "actionId": "create", "userGesture": true,
            "conversationId": "group-1", "formValues": values})
    };
    let too_many = f
        .api
        .invoke_plugin_action(create(
            json!({"question": "Lunch?", "options": ["A", "B", "C", "D"]}),
        ))
        .await
        .unwrap();
    assert_eq!(
        too_many["errors"],
        json!([{"input": "options", "issue": "tooManyItems"}]),
        "the configured maximum, not the declared 20"
    );
    assert!(f.business_calls().is_empty());
    let created = f
        .api
        .invoke_plugin_action(create(
            json!({"question": "Lunch?", "options": ["A", "B", "C"]}),
        ))
        .await
        .unwrap();
    assert_eq!(created["kind"], "operation");
    assert_eq!(
        f.business_calls()[0]["payload"],
        json!({"question": "Lunch?", "options": ["A", "B", "C"], "multiple": true}),
        "an unsubmitted toggle takes the default its defaultPath resolved"
    );

    // Without the configured keys the declared values apply.
    f.catalog.lock().unwrap().plugins[0].configuration = Default::default();
    let view = f.api.present_plugin_message(present).await.unwrap();
    assert_eq!(view["data"], json!({"config": {}}));
    assert_eq!(input(&view, "options")["max"], 20);
    assert_eq!(input(&view, "multiple")["default"], false);
    let four = f
        .api
        .invoke_plugin_action(create(
            json!({"question": "Lunch?", "options": ["A", "B", "C", "D"]}),
        ))
        .await
        .unwrap();
    assert_eq!(four["kind"], "operation");
}

/// A paged detail: the first page comes with `nextCursor`; the next one is requested with that
/// cursor and the view shown so far, and comes back appended to it.
#[tokio::test]
async fn detail_pages_are_requested_with_the_cursor_and_appended_by_core() {
    let f = Fixture::new().await;
    contribution_mut(&mut f.catalog.lock().unwrap(), "example.poll.card")
        .detail
        .as_mut()
        .unwrap()
        .cursor_path = Some("/nextCursor".into());
    f.results.lock().unwrap().insert(
        "example.poll.details".into(),
        json!({"voters": [{"option": "Rice", "userId": "u2"}], "nextCursor": "page-2"}),
    );
    let request = json!({"message": poll(), "platform": "web", "locale": "en-US", "userGesture": true,
        "conversationId": "group-1"});
    let first = f.api.plugin_detail(request.clone()).await.unwrap();
    assert_eq!(first["nextCursor"], "page-2");
    assert_eq!(
        block(&first, "entries")["items"],
        json!([{"index": 1, "text": "Rice", "userId": "u2"}])
    );
    assert_eq!(
        f.business_calls()[0]["payload"],
        json!({"pollId": "poll-1"})
    );

    f.results.lock().unwrap().insert(
        "example.poll.details".into(),
        json!({"voters": [{"option": "Noodles", "userId": "u3"}]}),
    );
    let mut next = request.clone();
    next["cursor"] = json!("page-2");
    next["previous"] = first.clone();
    let merged = f.api.plugin_detail(next.clone()).await.unwrap();
    assert_eq!(
        block(&merged, "entries")["items"],
        json!([
            {"index": 1, "text": "Rice", "userId": "u2"},
            {"index": 2, "text": "Noodles", "userId": "u3"}
        ]),
        "the page follows the shown entries, numbered on"
    );
    assert!(
        merged.get("nextCursor").is_none(),
        "the last page ends paging"
    );
    assert_eq!(merged["title"], first["title"]);
    assert_eq!(
        f.business_calls()[1]["payload"],
        json!({"pollId": "poll-1", "cursor": "page-2"}),
        "the first page's input plus the cursor"
    );

    let mut page_only = request.clone();
    page_only["cursor"] = json!("page-2");
    let page = f.api.plugin_detail(page_only).await.unwrap();
    assert_eq!(
        block(&page, "entries")["items"],
        json!([{"index": 1, "text": "Noodles", "userId": "u3"}]),
        "without previous only the page comes back"
    );

    let calls = f.business_calls().len();
    let mut orphan = request.clone();
    orphan["previous"] = first.clone();
    assert!(
        f.api.plugin_detail(orphan).await.is_err(),
        "previous needs a cursor"
    );
    let mut blank = next.clone();
    blank["cursor"] = json!("  ");
    assert!(f.api.plugin_detail(blank).await.is_err(), "a blank cursor");
    let mut long = next.clone();
    long["cursor"] = json!("c".repeat(257));
    assert!(
        f.api.plugin_detail(long).await.is_err(),
        "a cursor over 256 bytes"
    );
    let mut forged = next.clone();
    forged["previous"]["injected"] = json!(true);
    assert!(
        f.api.plugin_detail(forged).await.is_err(),
        "previous is a view as Core returned it"
    );
    contribution_mut(&mut f.catalog.lock().unwrap(), "example.poll.card")
        .detail
        .as_mut()
        .unwrap()
        .cursor_path = None;
    assert!(
        f.api.plugin_detail(next).await.is_err(),
        "a detail without cursorPath does not page"
    );
    assert_eq!(
        f.business_calls().len(),
        calls,
        "refused pages send nothing"
    );
}

fn voters(entries: &[(&str, &str)], next: Option<&str>) -> Value {
    let mut data = json!({
        "voters": entries
            .iter()
            .map(|(option, user)| json!({"option": option, "userId": user}))
            .collect::<Vec<_>>()
    });
    if let Some(next) = next {
        data["nextCursor"] = json!(next);
    }
    data
}

fn entry_texts(view: &Value) -> Vec<(u64, String)> {
    block(view, "entries")["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| {
            (
                item["index"].as_u64().unwrap(),
                format!(
                    "{}:{}",
                    item["text"].as_str().unwrap(),
                    item["userId"].as_str().unwrap()
                ),
            )
        })
        .collect()
}

/// A host re-reading a shown sheet after a state change: `refresh` with the shown view re-reads
/// the first page and follows the cursors until the sheet holds as many entries as before, so
/// the pages the viewer had loaded stay open.
#[tokio::test]
async fn detail_refresh_rereads_as_many_pages_as_the_sheet_had_loaded() {
    let f = Fixture::new().await;
    contribution_mut(&mut f.catalog.lock().unwrap(), "example.poll.card")
        .detail
        .as_mut()
        .unwrap()
        .cursor_path = Some("/nextCursor".into());
    let serve = |pages: &[(&str, Value)]| {
        let mut served = f.pages.lock().unwrap();
        served.clear();
        for (cursor, data) in pages {
            served.insert(format!("example.poll.details#{cursor}"), data.clone());
        }
    };
    serve(&[
        ("", voters(&[("Rice", "u2")], Some("page-2"))),
        ("page-2", voters(&[("Noodles", "u3")], Some("page-3"))),
        ("page-3", voters(&[("Rice", "u4")], None)),
    ]);
    let request = json!({"message": poll(), "platform": "web", "locale": "en-US", "userGesture": true,
        "conversationId": "group-1"});
    // The viewer opened the sheet and loaded one more page.
    let first = f.api.plugin_detail(request.clone()).await.unwrap();
    let mut more = request.clone();
    more["cursor"] = json!("page-2");
    more["previous"] = first;
    let shown = f.api.plugin_detail(more).await.unwrap();
    assert_eq!(shown["nextCursor"], "page-3");

    // A new vote lands at the top; every page shifts by one.
    serve(&[
        ("", voters(&[("Noodles", "u5")], Some("page-2"))),
        ("page-2", voters(&[("Rice", "u2")], Some("page-3"))),
        ("page-3", voters(&[("Noodles", "u3")], Some("page-4"))),
        ("page-4", voters(&[("Rice", "u4")], None)),
    ]);
    let sent = f.business_calls().len();
    let mut refresh = request.clone();
    refresh["refresh"] = json!(true);
    refresh["previous"] = shown.clone();
    let view = f.api.plugin_detail(refresh.clone()).await.unwrap();
    assert_eq!(
        entry_texts(&view),
        vec![(1, "Noodles:u5".into()), (2, "Rice:u2".into())],
        "the fresh first page, then pages until the sheet is as long as before"
    );
    assert_eq!(
        view["nextCursor"], "page-3",
        "the rest still loads on demand"
    );
    assert_eq!(view["title"], shown["title"]);
    let calls = f.business_calls();
    assert_eq!(
        calls[sent..]
            .iter()
            .map(|call| call["payload"].clone())
            .collect::<Vec<_>>(),
        vec![
            json!({"pollId": "poll-1"}),
            json!({"pollId": "poll-1", "cursor": "page-2"})
        ],
        "from the first page, the bound input plus each cursor"
    );

    // Fewer entries than shown (a vote withdrawn): read to the last page and stop there.
    serve(&[("", voters(&[("Rice", "u2")], None))]);
    let sent = f.business_calls().len();
    let shrunk = f.api.plugin_detail(refresh.clone()).await.unwrap();
    assert_eq!(entry_texts(&shrunk), vec![(1, "Rice:u2".into())]);
    assert!(shrunk.get("nextCursor").is_none());
    assert_eq!(f.business_calls().len(), sent + 1);

    // A page failing fails the refresh; the host keeps the sheet it shows.
    serve(&[
        ("", voters(&[("Rice", "u2")], Some("page-2"))),
        ("page-2", voters(&[("Noodles", "u3")], None)),
    ]);
    f.failing
        .lock()
        .unwrap()
        .push("example.poll.details#page-2".into());
    assert!(f.api.plugin_detail(refresh.clone()).await.is_err());
    f.failing.lock().unwrap().clear();

    // Refresh is of a shown sheet, from its first page.
    let sent = f.business_calls().len();
    let mut orphan = request.clone();
    orphan["refresh"] = json!(true);
    assert!(
        f.api.plugin_detail(orphan).await.is_err(),
        "refresh needs the shown view"
    );
    let mut paged = refresh.clone();
    paged["cursor"] = json!("page-2");
    assert!(
        f.api.plugin_detail(paged).await.is_err(),
        "refresh starts from the first page, never from a cursor"
    );
    assert_eq!(
        f.business_calls().len(),
        sent,
        "refused refreshes send nothing"
    );
}

/// A refresh reads at most `MAX_DETAIL_REFRESH_PAGES` pages, however long the shown sheet was;
/// the view it returns keeps `nextCursor` for the rest.
#[tokio::test]
async fn detail_refresh_reads_a_bounded_number_of_pages() {
    use crate::extension::capability::ui::MAX_DETAIL_REFRESH_PAGES;
    let f = Fixture::new().await;
    contribution_mut(&mut f.catalog.lock().unwrap(), "example.poll.card")
        .detail
        .as_mut()
        .unwrap()
        .cursor_path = Some("/nextCursor".into());
    {
        let mut served = f.pages.lock().unwrap();
        for page in 0..(MAX_DETAIL_REFRESH_PAGES + 5) {
            let cursor = if page == 0 {
                String::new()
            } else {
                format!("c{page}")
            };
            served.insert(
                format!("example.poll.details#{cursor}"),
                voters(&[("Rice", "u2")], Some(&format!("c{}", page + 1))),
            );
        }
    }
    let request = json!({"message": poll(), "platform": "web", "locale": "en-US", "userGesture": true,
        "conversationId": "group-1"});
    // A sheet the viewer had scrolled far into.
    let mut shown = f.api.plugin_detail(request.clone()).await.unwrap();
    shown["blocks"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|block| block["kind"] == "entries")
        .unwrap()["items"] = Value::Array(
        (1..=200)
            .map(|index| json!({"index": index, "text": "Rice", "userId": "u2"}))
            .collect(),
    );
    let sent = f.business_calls().len();
    let mut refresh = request.clone();
    refresh["refresh"] = json!(true);
    refresh["previous"] = shown;
    let view = f.api.plugin_detail(refresh).await.unwrap();
    assert_eq!(f.business_calls().len() - sent, MAX_DETAIL_REFRESH_PAGES);
    assert_eq!(entry_texts(&view).len(), MAX_DETAIL_REFRESH_PAGES);
    assert_eq!(
        view["nextCursor"],
        format!("c{MAX_DETAIL_REFRESH_PAGES}"),
        "the rest loads on demand"
    );
}

/// Serves one card back with the shared layer Reader attaches, counting the reads.
struct ServedCard {
    remote: crate::model::IMMessage,
    calls: std::sync::atomic::AtomicUsize,
}
impl crate::application::services::RemoteMessageLookup for ServedCard {
    fn message_at_seq<'a>(
        &'a self,
        _conversation_id: &'a str,
        seq: u64,
    ) -> crate::application::services::plugin_state_refetch::BoxFuture<
        'a,
        crate::shared::error::Result<Option<crate::model::IMMessage>>,
    > {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(
            async move { Ok((self.remote.conversation_seq == seq).then(|| self.remote.clone())) },
        )
    }
}

/// A card cached before the client knew shared state has no shared layer on its message and
/// missed the events. Presenting it reads the message back once; the layer found is stored and
/// announced, and the next presentation shows it. Only cards of an entity are read back.
#[tokio::test]
async fn presenting_a_card_without_its_shared_layer_reads_it_back_once() {
    use crate::model::message::{
        PLUGIN_SHARED_ENTITY_KEY, PLUGIN_SHARED_STATE_JSON_KEY, PLUGIN_SHARED_STATE_VERSION_KEY,
    };
    use std::sync::atomic::Ordering;
    let mut f = Fixture::new().await;
    let mut cached = card("server-1", &poll());
    cached.conversation_seq = 7;
    let mut remote = cached.clone();
    remote.attributes.extend([
        (
            PLUGIN_SHARED_STATE_JSON_KEY.to_string(),
            r#"{"counts":[2,1],"voterCount":3}"#.to_string(),
        ),
        (PLUGIN_SHARED_STATE_VERSION_KEY.to_string(), "5".to_string()),
        (PLUGIN_SHARED_ENTITY_KEY.to_string(), "poll-1".to_string()),
    ]);
    let lookup = Arc::new(ServedCard {
        remote,
        calls: Default::default(),
    });
    f.api = f.api.clone().with_plugin_state_refetch(Arc::new(
        crate::application::services::PluginStateRefetch::new(
            lookup.clone(),
            f.store.clone(),
            f.bus.clone(),
            Arc::new(RwLock::new("u1".into())),
        ),
    ));
    f.store.save_one(&cached).await.unwrap();

    let stale = f
        .api
        .present_plugin_message(present("server-1"))
        .await
        .unwrap();
    assert_eq!(stale["sharedStateVersion"], 0);
    let mut announced = Vec::new();
    for _ in 0..200 {
        announced = f.state_changes();
        if !announced.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(announced, vec![5]);
    let healed = f
        .api
        .present_plugin_message(present("client-server-1"))
        .await
        .unwrap();
    assert_eq!(healed["sharedStateVersion"], 5);
    assert_eq!(healed["data"]["counts"], json!([2, 1]));
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 1);

    // A card without an entity has no shared layer to heal.
    contribution_mut(&mut f.catalog.lock().unwrap(), "example.poll.card").entity_path = None;
    let mut plain = card("server-2", &poll());
    plain.conversation_seq = 8;
    f.store.save_one(&plain).await.unwrap();
    f.api
        .present_plugin_message(present("server-2"))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 1);
}
