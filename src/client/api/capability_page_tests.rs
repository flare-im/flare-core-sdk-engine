//! Plugin pages through `CapabilityApi`: the source read with arguments,
//! filters and paging, refresh, inert views for an unavailable plugin, and
//! page actions without `messageId`, against a local dispatch endpoint.
use super::*;
use flare_plugin_protocol::PluginManifest;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex as SyncMutex;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

const CATALOG: &str = flare_plugin_protocol::ui::UI_CATALOG_OPERATION;

/// The protocol's v2 acceptance manifest plus a wallet page and a paged
/// ledger page with a rows block, as a catalog plugin.
fn catalog_plugin() -> Value {
    let mut manifest: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../flare-protocol/flare-plugin-protocol/tests/fixtures/plugin-ui-v2-manifest.json"
    )))
    .unwrap();
    let operations = manifest["capabilities"][0]["operations"]
        .as_array_mut()
        .unwrap();
    for (id, mode) in [
        ("wallet", "query"),
        ("history", "query"),
        ("hide", "command"),
    ] {
        operations.push(json!({
            "id": id, "idempotent": true, "inputSchema": {"type": "object"},
            "outputSchema": {"type": "object"}, "permissionIds": ["example.poll.read"],
            "timeoutMs": 5000, "mode": mode
        }));
    }
    let contributions = manifest["clientExtensions"]["contributions"]
        .as_array_mut()
        .unwrap();
    contributions.push(json!({
        "kind": "page", "id": "example.poll.wallet", "schemaVersion": 1,
        "requiredPermissions": [], "source": {"operation": "example.poll.wallet"},
        "schema": {"title": "钱包", "fields": [{"label": "余额", "path": "/balanceText"}],
                   "actions": [{"id": "history", "label": "明细", "input": {"currency": "/currency"},
                                "target": {"kind": "page", "page": "example.poll.history"}}]}
    }));
    contributions.push(json!({
        "kind": "page", "id": "example.poll.history", "schemaVersion": 1,
        "requiredPermissions": [],
        "source": {"operation": "example.poll.history", "input": {"currency": "/currency"},
                   "cursorPath": "/nextCursor"},
        "schema": {
            "title": "明细",
            "blocks": [{"kind": "rows", "source": "example.poll.history", "path": "/lines",
                        "keyPath": "/id", "filters": ["kind"], "actions": ["hide"],
                        "columns": [{"label": "金额", "path": "/amountText"}],
                        "valueLabels": {"path": "/kind", "values": {
                            "send": {"label": "发出红包"}, "claim": {"label": "收到红包", "tone": "success"}}}}],
            "inputs": [{"id": "kind", "kind": "select", "label": "类型",
                        "choices": [{"value": "send", "label": "发出"}, {"value": "claim", "label": "收到"}]},
                       {"id": "note", "kind": "text", "label": "备注"}],
            "actions": [{"id": "hide", "label": "隐藏", "input": {"lineId": "/id"}, "form": ["note"],
                         "target": {"kind": "operation", "operation": "example.poll.hide"}}]
        }
    }));
    let manifest = PluginManifest::from_json(&serde_json::to_vec(&manifest).unwrap()).unwrap();
    json!({
        "pluginId": manifest.plugin_id, "version": manifest.version,
        "manifestDigest": format!("sha256:{}", "b".repeat(64)), "grantRevision": 1,
        "extensions": manifest.client_extensions, "operations": manifest.operation_ids()
    })
}
fn catalog(plugins: Vec<Value>, unavailable: Value) -> Value {
    json!({
        "tenantId": "201", "version": 1,
        "validUntilMs": chrono::Utc::now().timestamp_millis() + 600_000,
        "plugins": plugins, "unavailable": unavailable
    })
}

type Answers = Arc<SyncMutex<HashMap<String, Vec<Value>>>>;
struct Fixture {
    api: CapabilityApi,
    catalog: Arc<SyncMutex<Value>>,
    calls: Arc<SyncMutex<Vec<Value>>>,
    /// Results per capability id, served in turn (the last one repeats).
    answers: Answers,
    errors: Arc<SyncMutex<HashMap<String, (u16, String)>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let catalog = Arc::new(SyncMutex::new(catalog(vec![catalog_plugin()], json!([]))));
        let calls = Arc::new(SyncMutex::new(Vec::new()));
        let answers: Answers = Arc::new(SyncMutex::new(HashMap::new()));
        let errors = Arc::new(SyncMutex::new(HashMap::<String, (u16, String)>::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (c, q, a, e) = (
            catalog.clone(),
            calls.clone(),
            answers.clone(),
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
                            let mut answers = a.lock().unwrap();
                            match answers.get_mut(&capability) {
                                Some(queue) if queue.len() > 1 => queue.remove(0),
                                Some(queue) => queue.first().cloned().unwrap_or_else(|| json!({})),
                                None => json!({}),
                            }
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
        let user = Arc::new(RwLock::new("u1".to_string()));
        let api = CapabilityApi::new(
            "http://127.0.0.1:1",
            user,
            "201",
            Arc::new(HttpRequestContext::new()),
        )
        .with_http_transport(HttpClient::new(base));
        Self {
            api,
            catalog,
            calls,
            answers,
            errors,
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
    fn answer(&self, capability: &str, data: Vec<Value>) {
        self.answers.lock().unwrap().insert(capability.into(), data);
    }
    fn fail(&self, capability: &str, status: u16, reason: &str) {
        self.errors
            .lock()
            .unwrap()
            .insert(capability.into(), (status, reason.into()));
    }
}

fn ledger(ids: &[&str], next: Option<&str>) -> Value {
    let lines: Vec<Value> = ids
        .iter()
        .map(|id| json!({"id": id, "kind": "claim", "amountText": format!("+{id}")}))
        .collect();
    let mut data = json!({"lines": lines});
    if let Some(next) = next {
        data["nextCursor"] = json!(next);
    }
    data
}
fn history(extra: Value) -> Value {
    let mut request = json!({"pluginId": "example.poll", "page": "example.poll.history",
                             "platform": "web", "locale": "en-US",
                             "arguments": {"currency": "CNY"}});
    for (key, value) in extra.as_object().unwrap() {
        request[key] = value.clone();
    }
    request
}

#[tokio::test]
async fn a_page_reads_its_source_with_arguments_and_filters_then_pages() {
    let f = Fixture::new().await;
    f.answer(
        "example.poll.history",
        vec![
            ledger(&["l1", "l2"], Some("c2")),
            ledger(&["l2", "l3"], None),
        ],
    );
    let first = f
        .api
        .plugin_page(history(json!({"filters": {"kind": "claim"}})))
        .await
        .unwrap();
    assert_eq!(first["available"], true);
    assert_eq!(first["nextCursor"], "c2");
    assert_eq!(first["data"]["lines"][0]["id"], "l1");
    assert_eq!(first["blocks"][0]["rows"][0]["badge"]["text"], "收到红包");
    assert_eq!(first["blocks"][0]["rows"][0]["badge"]["tone"], "success");
    let call = &f.calls_to("example.poll.history")[0];
    assert_eq!(call["payload"], json!({"currency": "CNY", "kind": "claim"}));
    assert!(call.get("messageId").is_none(), "a page is no card");
    assert!(call["conversationId"].is_null());

    let next = f
        .api
        .plugin_page(history(json!({
            "filters": {"kind": "claim"}, "cursor": "c2", "previous": first.clone()
        })))
        .await
        .unwrap();
    assert_eq!(
        f.calls_to("example.poll.history")[1]["payload"],
        json!({"currency": "CNY", "kind": "claim", "cursor": "c2"})
    );
    let keys: Vec<&str> = next["blocks"][0]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["l1", "l2", "l3"], "appended, shown keys dropped");
    assert!(next.get("nextCursor").is_none());
    assert_eq!(next["data"], first["data"], "data stays the first page's");

    // Filters are validated like a form, before anything is sent.
    let sent = f.calls.lock().unwrap().len();
    assert!(
        f.api
            .plugin_page(history(json!({"filters": {"kind": "gift"}})))
            .await
            .is_err()
    );
    // A cursor on the first page alone, or a refresh without the view, is invalid.
    assert!(
        f.api
            .plugin_page(history(json!({"previous": first.clone()})))
            .await
            .is_err()
    );
    assert!(
        f.api
            .plugin_page(history(json!({"refresh": true})))
            .await
            .is_err()
    );
    assert_eq!(
        f.calls_to("example.poll.history").len(),
        2,
        "nothing more sent ({sent} calls before)"
    );
}

#[tokio::test]
async fn a_refresh_keeps_the_pages_the_viewer_loaded() {
    let f = Fixture::new().await;
    f.answer(
        "example.poll.history",
        vec![
            ledger(&["l1", "l2"], Some("c2")),
            ledger(&["l3"], None),
            ledger(&["l0", "l1"], Some("c2")),
            ledger(&["l2", "l3"], None),
        ],
    );
    let first = f.api.plugin_page(history(json!({}))).await.unwrap();
    let shown = f
        .api
        .plugin_page(history(json!({"cursor": "c2", "previous": first})))
        .await
        .unwrap();
    let refreshed = f
        .api
        .plugin_page(history(json!({"refresh": true, "previous": shown})))
        .await
        .unwrap();
    assert_eq!(f.calls_to("example.poll.history").len(), 4);
    assert_eq!(refreshed["blocks"][0]["rows"].as_array().unwrap().len(), 4);
    assert_eq!(
        refreshed["data"]["lines"][0]["id"], "l0",
        "the new first page"
    );
}

#[tokio::test]
async fn an_unavailable_page_is_an_inert_view_and_a_next_page_fails() {
    let f = Fixture::new().await;
    f.answer("example.poll.wallet", vec![json!({"balanceText": "¥1.00"})]);
    let wallet = json!({"pluginId": "example.poll", "page": "example.poll.wallet",
                        "platform": "ios", "locale": "zh-CN"});
    let view = f.api.plugin_page(wallet.clone()).await.unwrap();
    assert_eq!(view["fields"][0]["value"], "¥1.00");
    assert_eq!(f.calls_to("example.poll.wallet")[0]["payload"], json!({}));

    // The source answers that the plugin is disabled.
    f.fail("example.poll.wallet", 403, "PLUGIN_DISABLED");
    let view = f.api.plugin_page(wallet.clone()).await.unwrap();
    assert_eq!(view["available"], false);
    assert_eq!(view["unavailableReason"], "disabled");
    assert_eq!(view["title"], "");
    assert!(view.get("data").is_none());
    // Another failure of the source is an error.
    f.fail("example.poll.wallet", 422, "WALLET_LOCKED");
    assert!(f.api.plugin_page(wallet.clone()).await.is_err());

    // The catalog lists the plugin as disabled.
    *f.catalog.lock().unwrap() = catalog(
        vec![],
        json!([{"pluginId": "example.poll", "reason": "disabled"}]),
    );
    let view = f.api.plugin_page(wallet.clone()).await.unwrap();
    assert_eq!(view["unavailableReason"], "disabled");
    // An unknown page of an installed plugin.
    *f.catalog.lock().unwrap() = catalog(vec![catalog_plugin()], json!([]));
    let mut missing = wallet.clone();
    missing["page"] = json!("example.poll.missing");
    let view = f.api.plugin_page(missing).await.unwrap();
    assert_eq!(view["unavailableReason"], "incompatible_version");
    // The catalog cannot be read: inert on the first page, an error on the next.
    f.fail(CATALOG, 503, "UPSTREAM_DOWN");
    let view = f.api.plugin_page(wallet).await.unwrap();
    assert_eq!(view["unavailableReason"], "catalog_unreachable");
    let shown = json!({"available": true, "title": "明细", "fields": [], "actions": [],
                       "nextCursor": "c2", "data": {}});
    assert!(
        f.api
            .plugin_page(history(json!({"cursor": "c2", "previous": shown})))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn page_actions_bind_over_the_page_and_never_name_a_message() {
    let f = Fixture::new().await;
    f.answer(
        "example.poll.wallet",
        vec![json!({"balanceText": "¥1.00", "currency": "CNY"})],
    );
    let wallet = f
        .api
        .plugin_page(
            json!({"pluginId": "example.poll", "page": "example.poll.wallet",
                            "platform": "web"}),
        )
        .await
        .unwrap();
    // A page target comes back to the host with the next page's arguments.
    let opened = f
        .api
        .invoke_plugin_action(json!({
            "page": {"pluginId": "example.poll", "page": "example.poll.wallet"},
            "platform": "web", "actionId": "history", "userGesture": true,
            "data": wallet["data"]
        }))
        .await
        .unwrap();
    assert_eq!(opened["kind"], "host");
    assert_eq!(
        opened["target"],
        json!({"kind": "page", "page": "example.poll.history"})
    );
    assert_eq!(opened["payload"], json!({"currency": "CNY"}));

    // A row action takes the row's bound input plus its form.
    let row_action = |input: Value, form: Value| {
        json!({
            "page": {"pluginId": "example.poll", "page": "example.poll.history"},
            "platform": "web", "actionId": "hide", "userGesture": true,
            "row": {"key": "l1", "input": input}, "formValues": form
        })
    };
    let done = f
        .api
        .invoke_plugin_action(row_action(json!({"lineId": "l1"}), json!({"note": "dup"})))
        .await
        .unwrap();
    assert_eq!(done["kind"], "operation");
    let call = &f.calls_to("example.poll.hide")[0];
    assert_eq!(call["payload"], json!({"lineId": "l1", "note": "dup"}));
    assert!(call.get("messageId").is_none());
    // The row input names exactly the declared inputs.
    for input in [json!({"lineId": "l1", "amount": 1}), json!({})] {
        assert!(
            f.api
                .invoke_plugin_action(row_action(input, json!({"note": "x"})))
                .await
                .is_err()
        );
    }
    // A form problem comes back without a call.
    let errors = f
        .api
        .invoke_plugin_action(row_action(json!({"lineId": "l1"}), json!({"note": 7})))
        .await
        .unwrap();
    assert_eq!(errors["kind"], "formErrors");
    // A row action without its row, a gesture-less call, both page and message.
    let mut no_row = row_action(json!({"lineId": "l1"}), json!({"note": "x"}));
    no_row.as_object_mut().unwrap().remove("row");
    assert!(f.api.invoke_plugin_action(no_row).await.is_err());
    let mut no_gesture = row_action(json!({"lineId": "l1"}), json!({"note": "x"}));
    no_gesture["userGesture"] = json!(false);
    assert!(f.api.invoke_plugin_action(no_gesture).await.is_err());
    assert_eq!(f.calls_to("example.poll.hide").len(), 1);
}
