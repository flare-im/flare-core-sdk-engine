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
type Uploads = Arc<SyncMutex<Vec<(String, Vec<(String, String)>, Vec<u8>)>>>;
struct Fixture {
    api: CapabilityApi,
    catalog: Arc<SyncMutex<Value>>,
    calls: Arc<SyncMutex<Vec<Value>>>,
    /// Results per capability id, served in turn (the last one repeats).
    answers: Answers,
    errors: Arc<SyncMutex<HashMap<String, (u16, String)>>>,
    uploads: Uploads,
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
        let uploads: Uploads = Arc::new(SyncMutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (c, q, a, e, u) = (
            catalog.clone(),
            calls.clone(),
            answers.clone(),
            errors.clone(),
            uploads.clone(),
        );
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let mut len = 0;
                let mut headers = Vec::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).await.unwrap();
                    if header.trim().is_empty() {
                        break;
                    }
                    let (name, value) = header.trim().split_once(':').unwrap();
                    headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
                    if name.eq_ignore_ascii_case("content-length") {
                        len = value.trim().parse().unwrap();
                    }
                }
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes).await.unwrap();
                if line.starts_with("PUT ") {
                    u.lock()
                        .unwrap()
                        .push((line.trim().to_owned(), headers, bytes));
                    let mut socket = reader.into_inner();
                    socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                    continue;
                }
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
            uploads,
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

fn declare_upload(f: &Fixture) {
    let mut catalog = f.catalog.lock().unwrap();
    let plugin = &mut catalog["plugins"][0];
    for operation in [
        "example.poll.material_prepare",
        "example.poll.material_complete",
    ] {
        plugin["operations"]
            .as_array_mut()
            .unwrap()
            .push(json!(operation));
    }
    plugin["extensions"]["contributions"].as_array_mut().unwrap().push(json!({
        "kind":"page","id":"example.poll.upload","schemaVersion":1,"requiredPermissions":[],
        "source":{"operation":"example.poll.wallet"},
        "schema":{"title":"Receipt","inputs":[
            {"id":"method","kind":"select","label":"Method","default":"bank",
             "choices":[{"value":"bank","label":"Bank"},{"value":"qr","label":"QR"}]},
            {"id":"receipt","kind":"file","label":"Receipt","required":true,
             "showWhen":{"field":"method","anyOf":["qr"]},
             "file":{"accept":["image/png"],"maxBytes":5242880,
               "upload":{"prepareOperation":"example.poll.material_prepare",
                 "completeOperation":"example.poll.material_complete","purpose":"privateReceipt"}}}],
          "actions":[{"id":"save","label":"Save","form":["method","receipt"],
            "target":{"kind":"operation","operation":"example.poll.hide"}}]}}));
}
fn upload_request() -> Value {
    json!({"page":{"pluginId":"example.poll","page":"example.poll.upload"},
        "inputId":"receipt","platform":"web","locale":"en-US","commandId":"upload-one",
        "fileName":"receipt.png","contentType":"image/png","bytes":[137,80,78,71],
        "formValues":{"method":"qr"}})
}
fn prepared_upload(ready: bool) -> Value {
    use sha2::{Digest, Sha256};
    json!({"fileId":"file:receipt-one","ready":ready,"contentType":"image/png","sizeBytes":4,
        "sha256":hex::encode(Sha256::digest([137,80,78,71])),
        "uploadUrl":"/private/example/materials/file-1?grant=ephemeral","expiresAtMs":chrono::Utc::now().timestamp_millis()+60000})
}

#[tokio::test]
async fn file_upload_uses_declared_purpose_private_put_and_strict_completed_reference() {
    let mut f = Fixture::new().await;
    declare_upload(&f);
    let base = f.api.http.as_ref().unwrap().url_for("/").await.unwrap();
    let context = Arc::new(HttpRequestContext::new());
    context
        .set_auth_context("core-test-token".into(), None)
        .await;
    context
        .set_gateway_context("gateway-test-token".into(), "201".into(), "u1".into(), None)
        .await;
    f.api.http = Some(HttpClient::with_context(base, context));
    f.answer(
        "example.poll.material_prepare",
        vec![prepared_upload(false)],
    );
    f.answer(
        "example.poll.material_complete",
        vec![json!({"fileId":"file:receipt-one"})],
    );
    assert_eq!(
        f.api.plugin_file_upload(upload_request()).await.unwrap(),
        json!({"fileId":"file:receipt-one"})
    );
    let prepare = f.calls_to("example.poll.material_prepare");
    assert_eq!(prepare.len(), 1);
    assert_eq!(
        prepare[0]["payload"],
        json!({"commandId":"upload-one","purpose":"privateReceipt",
        "fileName":"receipt.png","contentType":"image/png","sizeBytes":4,
        "sha256":prepared_upload(false)["sha256"]})
    );
    assert_eq!(prepare[0]["expectedUserId"], "u1");
    assert_eq!(prepare[0]["expectedTenantId"], "201");
    let uploads = f.uploads.lock().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(
        uploads[0].0,
        "PUT /private/example/materials/file-1?grant=ephemeral HTTP/1.1"
    );
    assert_eq!(uploads[0].2, [137, 80, 78, 71]);
    assert!(
        uploads[0]
            .1
            .iter()
            .any(|(key, value)| key == "content-type" && value == "image/png")
    );
    assert!(!uploads[0].1.iter().any(|(key, _)| matches!(
        key.as_str(),
        "authorization" | "cookie" | "x-user-id" | "x-tenant-id" | "referer"
    )));
    assert_eq!(
        f.calls_to("example.poll.material_complete")[0]["payload"],
        json!({"commandId":"upload-one","fileId":"file:receipt-one"})
    );
    // Upload transport commands do not invoke the form's financial operation.
    assert!(f.calls_to("example.poll.hide").is_empty());
}

#[tokio::test]
async fn file_upload_ready_retry_keeps_command_and_skips_expired_grant_and_complete() {
    let f = Fixture::new().await;
    declare_upload(&f);
    let mut prepared = prepared_upload(true);
    prepared.as_object_mut().unwrap().remove("uploadUrl");
    prepared["expiresAtMs"] = json!(1);
    f.answer("example.poll.material_prepare", vec![prepared]);
    for _ in 0..2 {
        assert_eq!(
            f.api.plugin_file_upload(upload_request()).await.unwrap(),
            json!({"fileId":"file:receipt-one"})
        );
    }
    assert_eq!(f.calls_to("example.poll.material_prepare").len(), 2);
    for call in f.calls_to("example.poll.material_prepare") {
        assert_eq!(call["payload"]["commandId"], "upload-one");
    }
    assert!(f.uploads.lock().unwrap().is_empty());
    assert!(f.calls_to("example.poll.material_complete").is_empty());
}

#[tokio::test]
async fn file_upload_refuses_hidden_unknown_forged_or_disallowed_inputs_before_prepare() {
    let f = Fixture::new().await;
    declare_upload(&f);
    for (key, value) in [
        ("formValues", json!({"method":"bank"})),
        ("formValues", json!({"method":"qr","unknown":"stale"})),
        ("contentType", json!("application/pdf")),
        ("inputId", json!("unknown")),
        (
            "data",
            json!({"_session":{"tenantId":"201","userId":"someone-else"}}),
        ),
        ("purpose", json!("forged")),
    ] {
        let mut request = upload_request();
        request[key] = value;
        assert!(f.api.plugin_file_upload(request).await.is_err(), "{key}");
    }
    assert!(f.calls_to("example.poll.material_prepare").is_empty());
    assert!(f.uploads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn file_upload_refuses_mismatched_metadata_and_external_or_expired_grants() {
    let f = Fixture::new().await;
    declare_upload(&f);
    for (key, value) in [
        ("contentType", json!("application/pdf")),
        ("sizeBytes", json!(5)),
        ("sha256", json!("wrong")),
        ("fileId", json!("https://host/file")),
        ("uploadUrl", json!("https://external/private/file?grant=x")),
        ("uploadUrl", json!("/private/../file?grant=x")),
        ("expiresAtMs", json!(1)),
    ] {
        let mut prepared = prepared_upload(false);
        prepared[key] = value;
        f.answer("example.poll.material_prepare", vec![prepared]);
        assert!(
            f.api.plugin_file_upload(upload_request()).await.is_err(),
            "{key}"
        );
    }
    assert!(f.uploads.lock().unwrap().is_empty());
    assert!(f.calls_to("example.poll.material_complete").is_empty());
}

#[tokio::test]
async fn file_upload_does_not_accept_another_file_or_grant_from_complete() {
    let f = Fixture::new().await;
    declare_upload(&f);
    f.answer(
        "example.poll.material_prepare",
        vec![prepared_upload(false)],
    );
    for completed in [
        json!({"fileId":"other-file"}),
        json!({"fileId":"file:receipt-one","uploadUrl":"/private/grant"}),
    ] {
        f.answer("example.poll.material_complete", vec![completed]);
        assert!(f.api.plugin_file_upload(upload_request()).await.is_err());
    }
    assert_eq!(f.uploads.lock().unwrap().len(), 2);
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

    // The source answers that the plugin, or the feature it belongs to, is disabled.
    for reason in ["PLUGIN_DISABLED", "PLUGIN_FEATURE_DISABLED"] {
        f.fail("example.poll.wallet", 403, reason);
        let view = f.api.plugin_page(wallet.clone()).await.unwrap();
        assert_eq!(view["available"], false, "{reason}");
        assert_eq!(view["unavailableReason"], "disabled", "{reason}");
        assert_eq!(view["title"], "", "{reason}");
        assert!(view.get("data").is_none(), "{reason}");
    }
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

#[tokio::test]
async fn conversation_page_context_follows_navigation_without_entering_business_payload() {
    let f = Fixture::new().await;
    f.answer(
        "example.poll.wallet",
        vec![json!({"currency":"CNY","conversationId":"other-group"})],
    );
    let first=f.api.plugin_page(json!({"pluginId":"example.poll","page":"example.poll.wallet","platform":"web","conversationId":"group-1","arguments":{"conversationId":"other-group"}})).await.unwrap();
    assert_eq!(first["conversationId"], "group-1");
    let call = &f.calls_to("example.poll.wallet")[0];
    assert_eq!(call["conversationId"], "group-1");
    assert_eq!(
        call["payload"],
        json!({}),
        "context is outside the source DTO"
    );
    let opened=f.api.invoke_plugin_action(json!({"page":{"pluginId":"example.poll","page":"example.poll.wallet"},"platform":"web","actionId":"history","userGesture":true,"conversationId":first["conversationId"],"data":first["data"]})).await.unwrap();
    assert_eq!(opened["conversationId"], "group-1");
    assert_eq!(opened["payload"], json!({"currency":"CNY"}));
    f.answer("example.poll.history", vec![ledger(&["l1"], Some("next"))]);
    let next=f.api.plugin_page(json!({"pluginId":"example.poll","page":opened["target"]["page"],"platform":"web","conversationId":opened["conversationId"],"arguments":opened["payload"]})).await.unwrap();
    assert_eq!(
        f.calls_to("example.poll.history")[0]["conversationId"],
        "group-1"
    );
    assert_eq!(
        f.calls_to("example.poll.history")[0]["payload"],
        json!({"currency":"CNY"})
    );
    f.api.invoke_plugin_action(json!({"page":{"pluginId":"example.poll","page":"example.poll.history"},"platform":"web","actionId":"hide","userGesture":true,"conversationId":next["conversationId"],"data":{"conversationId":"other-group"},"row":{"key":"l1","input":{"lineId":"l1"}},"formValues":{"note":"done"}})).await.unwrap();
    assert_eq!(
        f.calls_to("example.poll.hide")[0]["conversationId"],
        "group-1"
    );
    assert_eq!(
        f.calls_to("example.poll.hide")[0]["payload"],
        json!({"lineId":"l1","note":"done"})
    );
    let sent = f.calls_to("example.poll.history").len();
    assert!(f.api.plugin_page(json!({"pluginId":"example.poll","page":"example.poll.history","platform":"web","conversationId":"other-group","arguments":{"currency":"CNY"},"cursor":"next","previous":next})).await.is_err(),"cannot append another conversation to a page");
    assert_eq!(f.calls_to("example.poll.history").len(), sent);
}

#[tokio::test]
async fn page_payload_cannot_select_the_invocation_conversation() {
    let f = Fixture::new().await;
    f.api.invoke_plugin_action(json!({"page":{"pluginId":"example.poll","page":"example.poll.history"},"platform":"web","actionId":"hide","userGesture":true,"data":{"conversationId":"injected"},"row":{"key":"l1","input":{"lineId":"l1"}},"formValues":{"note":"done"}})).await.unwrap();
    assert!(f.calls_to("example.poll.hide")[0]["conversationId"].is_null());
}

/// Snapshots of the declarative Check-in and Task page contracts, with no
/// compile-time dependency on commercial services. Calls use the real SDK
/// HTTP transport; the endpoint records the authorization envelope separately
/// from each operation's business input.
#[tokio::test]
async fn checkin_and_task_group_menu_pages_keep_context_through_the_real_transport() {
    let f = Fixture::new().await;
    let fixture: Value = serde_json::from_slice(include_bytes!(
        "../../../tests/vectors/plugin_group_pages.json"
    ))
    .unwrap();
    *f.catalog.lock().unwrap() = catalog(fixture["plugins"].as_array().unwrap().clone(), json!([]));
    let open_entry = |plugin: &str, entry: &str| json!({"message":{"pluginId":plugin,"contributionId":entry,"schemaVersion":1,"summary":"Group","payload":{"conversationId":"injected"}},"platform":"web","actionId":"open","userGesture":true,"conversationId":"group-1"});
    let open_page = |plugin: &str, navigation: &Value| json!({"pluginId":plugin,"page":navigation["target"]["page"],"platform":"web","conversationId":navigation["conversationId"],"arguments":navigation["payload"]});
    let menu = f
        .api
        .invoke_plugin_action(open_entry("flare.checkin", "flare.checkin.menu"))
        .await
        .unwrap();
    assert_eq!(menu["conversationId"], "group-1");
    assert_eq!(menu["payload"], json!({}));
    f.answer("flare.checkin.plan.list",vec![json!({"items":[{"planId":"p1","title":"Daily","status":"open","kind":"daily"}],"nextCursor":null,"conversationId":"injected"})]);
    let plans = f
        .api
        .plugin_page(open_page("flare.checkin", &menu))
        .await
        .unwrap();
    assert_eq!(
        f.calls_to("flare.checkin.plan.list")[0]["payload"],
        json!({})
    );
    let plan_navigation=f.api.invoke_plugin_action(json!({"page":{"pluginId":"flare.checkin","page":"flare.checkin.plans"},"platform":"web","conversationId":plans["conversationId"],"actionId":"open","userGesture":true,"row":{"key":"p1","input":{"planId":"p1"}}})).await.unwrap();
    f.answer("flare.checkin.plan.get",vec![json!({"plan":{"planId":"p1","title":"Daily","kind":"daily","status":"open"},"occurrence":null,"conversationId":"injected"})]);
    let plan = f
        .api
        .plugin_page(open_page("flare.checkin", &plan_navigation))
        .await
        .unwrap();
    assert_eq!(
        f.calls_to("flare.checkin.plan.get")[0]["payload"],
        json!({"planId":"p1"})
    );
    let actions_navigation=f.api.invoke_plugin_action(json!({"page":{"pluginId":"flare.checkin","page":"flare.checkin.plan.page"},"platform":"web","conversationId":plan["conversationId"],"actionId":"myActions","userGesture":true,"data":plan["data"]})).await.unwrap();
    f.answer("flare.checkin.my.actions",vec![json!({"items":[{"actionId":"a1","planId":"p1","status":"unavailable"}],"nextCursor":null,"conversationId":"injected"})]);
    let actions = f
        .api
        .plugin_page(open_page("flare.checkin", &actions_navigation))
        .await
        .unwrap();
    f.api.invoke_plugin_action(json!({"page":{"pluginId":"flare.checkin","page":"flare.checkin.my.actions.page"},"platform":"web","conversationId":actions["conversationId"],"actionId":"resume","userGesture":true,"row":{"key":"a1","input":{"actionId":"a1"}}})).await.unwrap();
    assert_eq!(
        f.calls_to("flare.checkin.action.resume")[0]["payload"],
        json!({"actionId":"a1"})
    );
    let task_menu = f
        .api
        .invoke_plugin_action(open_entry("flare.task", "flare.task.mine.menu"))
        .await
        .unwrap();
    f.answer("flare.task.mine",vec![json!({"items":[{"taskId":"t1","title":"Follow up","status":"open","doneUserIds":[]}],"nextCursor":null,"conversationId":"injected"})]);
    let tasks = f
        .api
        .plugin_page(open_page("flare.task", &task_menu))
        .await
        .unwrap();
    assert_eq!(f.calls_to("flare.task.mine")[0]["payload"], json!({}));
    f.api.invoke_plugin_action(json!({"page":{"pluginId":"flare.task","page":"flare.task.mine.page"},"platform":"web","conversationId":tasks["conversationId"],"actionId":"complete","userGesture":true,"row":{"key":"t1","input":{"taskId":"t1"}}})).await.unwrap();
    assert_eq!(
        f.calls_to("flare.task.complete")[0]["payload"],
        json!({"taskId":"t1"})
    );
    // A foreign entity cannot cause fallback to another conversation: Core's
    // refusal is surfaced while the route context stays the original group.
    f.fail(
        "flare.checkin.action.resume",
        403,
        "NOT_CONVERSATION_MEMBER",
    );
    assert!(f.api.invoke_plugin_action(json!({"page":{"pluginId":"flare.checkin","page":"flare.checkin.my.actions.page"},"platform":"web","conversationId":"group-1","actionId":"resume","userGesture":true,"data":{"conversationId":"other-group"},"row":{"key":"foreign","input":{"actionId":"foreign"}}})).await.is_err());
    assert_eq!(
        f.calls_to("flare.checkin.action.resume").last().unwrap()["conversationId"],
        "group-1"
    );
    for operation in [
        "flare.checkin.plan.list",
        "flare.checkin.plan.get",
        "flare.checkin.my.actions",
        "flare.checkin.action.resume",
        "flare.task.mine",
        "flare.task.complete",
    ] {
        assert_eq!(
            f.calls_to(operation)[0]["conversationId"],
            "group-1",
            "{operation}"
        );
    }
}
