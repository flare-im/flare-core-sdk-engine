//! `plugin_capability_invoke` against a local dispatch endpoint and a fake
//! host: the three allowlists (manifest ∩ host profile ∩ consent), the
//! consent exemptions, a payment order refused once closed, consent asked
//! again after a revoke or a new major, a captured document uploaded into
//! its input, a location truncated to its precision and written into its
//! input, and an order confirmed only for a transient operation.
use std::sync::Mutex as SyncMutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Map, json};

use super::native::{
    CapabilityConsentPrompt, CapabilityHostAnswer, CapturedFile, ConsentDecision,
    DocumentCapturePrompt, FilePickPrompt, LocationFix, LocationPrompt, PaymentConfirmationPrompt,
    PluginCapabilityHost,
};
use super::page_tests::Fixture;
use super::*;
use flare_plugin_protocol::ui::NativeCapability;

/// The host: records what it was asked and answers as set.
struct FakeHost {
    decision: SyncMutex<ConsentDecision>,
    consents: SyncMutex<Vec<String>>,
    captures: AtomicUsize,
    payments: AtomicUsize,
    proof: SyncMutex<CapabilityHostAnswer<Map<String, Value>>>,
    file: CapturedFile,
    fix: LocationFix,
}

impl FakeHost {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            decision: SyncMutex::new(ConsentDecision::Always),
            consents: SyncMutex::new(Vec::new()),
            captures: AtomicUsize::new(0),
            payments: AtomicUsize::new(0),
            proof: SyncMutex::new(CapabilityHostAnswer::Done(
                json!({"method": "paymentPassword", "password": "123456"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )),
            file: CapturedFile {
                file_name: "front.jpg".into(),
                content_type: "image/jpeg".into(),
                bytes: vec![0xFF, 0xD8, 0xFF, 0xD9],
            },
            fix: LocationFix {
                latitude: 31.230_416_7,
                longitude: 121.473_701_9,
                accuracy_m: Some(8.0),
            },
        })
    }
    fn decide(&self, decision: ConsentDecision) {
        *self.decision.lock().unwrap() = decision;
    }
    fn consents(&self) -> Vec<String> {
        self.consents.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl PluginCapabilityHost for FakeHost {
    async fn consent(&self, prompt: CapabilityConsentPrompt) -> ConsentDecision {
        let level = match &prompt.params {
            Some(params) => serde_json::to_value(params).unwrap().to_string(),
            None => String::new(),
        };
        self.consents
            .lock()
            .unwrap()
            .push(format!("{}{level}", prompt.capability.name()));
        *self.decision.lock().unwrap()
    }
    async fn capture_document(
        &self,
        prompt: DocumentCapturePrompt,
    ) -> CapabilityHostAnswer<CapturedFile> {
        assert_eq!(prompt.accept, vec!["image/jpeg".to_string()]);
        self.captures.fetch_add(1, Ordering::SeqCst);
        CapabilityHostAnswer::Done(self.file.clone())
    }
    async fn pick_file(&self, _: FilePickPrompt) -> CapabilityHostAnswer<CapturedFile> {
        CapabilityHostAnswer::Done(self.file.clone())
    }
    async fn confirm_payment(
        &self,
        _: PaymentConfirmationPrompt,
    ) -> CapabilityHostAnswer<Map<String, Value>> {
        self.payments.fetch_add(1, Ordering::SeqCst);
        self.proof.lock().unwrap().clone()
    }
    async fn locate_once(&self, _: LocationPrompt) -> CapabilityHostAnswer<LocationFix> {
        CapabilityHostAnswer::Done(self.fix.clone())
    }
}

/// The plugin declares `native` in its manifest, a page with a front-side
/// document input and its private upload (when it declares
/// `documentCapture`), a check-in page with a coarse and a fine location
/// input (when it declares `locationOnce`), a transient payment operation
/// and a pay page whose operation answers with an order; the deployment's
/// payment provider is another plugin's transient `example.pay.ticket`; the
/// host profile allows `allowed`; `host` does the host's part.
fn setup(f: &Fixture, declared: &[&str], allowed: &[&str], host: Arc<FakeHost>) {
    declare_pay_page(f);
    {
        let mut catalog = f.catalog.lock().unwrap();
        catalog["paymentProvider"] =
            json!({"pluginId": "example.pay", "operation": "example.pay.ticket"});
        let mut provider = catalog["plugins"][0].clone();
        provider["pluginId"] = json!("example.pay");
        provider["extensions"]["contributions"] = json!([]);
        provider["operations"] = json!(["example.pay.ticket"]);
        provider["transientOperations"] = json!(["example.pay.ticket"]);
        catalog["plugins"].as_array_mut().unwrap().push(provider);
        let plugin = &mut catalog["plugins"][0];
        plugin["extensions"]["nativeCapabilities"] = json!(declared);
        for operation in [
            "example.poll.material_prepare",
            "example.poll.material_complete",
            "example.poll.pay_confirm",
        ] {
            plugin["operations"]
                .as_array_mut()
                .unwrap()
                .push(json!(operation));
        }
        plugin["transientOperations"] = json!(["example.poll.pay_confirm"]);
        // Only a plugin that declares a capability holds the page using it.
        if declared.contains(&"locationOnce") {
            plugin["extensions"]["contributions"].as_array_mut().unwrap().push(json!({
                "kind": "page", "id": "example.poll.checkin", "schemaVersion": 1,
                "requiredPermissions": [], "source": {"operation": "example.poll.wallet"},
                "schema": {"title": "Check in",
                    "inputs": [
                        {"id": "area", "kind": "location", "label": "Area", "precision": "coarse"},
                        {"id": "spot", "kind": "location", "label": "Spot", "precision": "fine"},
                        {"id": "note", "kind": "text", "label": "Note"}],
                    "actions": [
                        {"id": "locate_area", "label": "Locate", "target": {"kind": "native",
                            "capability": "locationOnce", "params": {"precision": "coarse"},
                            "into": "area"}},
                        {"id": "locate_spot", "label": "Locate", "target": {"kind": "native",
                            "capability": "locationOnce", "params": {"precision": "fine"},
                            "into": "spot"}},
                        {"id": "submit", "label": "Submit", "form": ["area", "spot", "note"],
                         "target": {"kind": "operation", "operation": "example.poll.hide"}}]}
            }));
        }
        if !declared.contains(&"documentCapture") {
            drop(catalog);
            return allow(f, allowed, host);
        }
        plugin["extensions"]["contributions"].as_array_mut().unwrap().push(json!({
            "kind": "page", "id": "example.poll.identity", "schemaVersion": 1,
            "requiredPermissions": [], "source": {"operation": "example.poll.wallet"},
            "schema": {"title": "Identity",
                "inputs": [{"id": "front", "kind": "document", "label": "Front", "side": "front",
                    "required": true,
                    "file": {"accept": ["image/jpeg"], "maxBytes": 1048576, "upload": {
                        "prepareOperation": "example.poll.material_prepare",
                        "completeOperation": "example.poll.material_complete",
                        "purpose": "identity"}}}],
                "actions": [
                    {"id": "scan", "label": "Scan", "target": {"kind": "native",
                        "capability": "documentCapture", "params": {"side": "front"},
                        "into": "front"}},
                    {"id": "submit", "label": "Submit", "form": ["front"],
                     "target": {"kind": "operation", "operation": "example.poll.hide"}}]}
        }));
    }
    allow(f, allowed, host);
}

/// The host profile allows `allowed`; `host` does the host's part.
fn allow(f: &Fixture, allowed: &[&str], host: Arc<FakeHost>) {
    let kit = serde_json::to_value(flare_plugin_protocol::ui::engine_vocabulary()).unwrap();
    let profile = super::host_profile::parse_profile(
        json!({"platform": "web", "kitVocabulary": kit, "nativeAllowlist": allowed}),
        &super::host_profile::AppIdentity::default(),
    )
    .unwrap();
    f.api.plugin_host.set(profile);
    f.api
        .plugin_host
        .capability_host
        .set(Some(host as Arc<dyn PluginCapabilityHost>));
}

fn capture_request() -> Value {
    json!({"pluginId": "example.poll", "capability": "documentCapture",
        "params": {"side": "front"}, "into": "front", "platform": "web", "locale": "en-US",
        "page": {"pluginId": "example.poll", "page": "example.poll.identity"},
        "commandId": "capture-front", "userGesture": true})
}

/// A fix of `precision` into the check-in input of that precision.
fn location_request(precision: &str) -> Value {
    let into = if precision == "coarse" {
        "area"
    } else {
        "spot"
    };
    location_into(precision, into)
}

fn location_into(precision: &str, into: &str) -> Value {
    json!({"pluginId": "example.poll", "capability": "locationOnce",
        "params": {"precision": precision}, "into": into, "platform": "web", "locale": "en-US",
        "page": {"pluginId": "example.poll", "page": "example.poll.checkin"},
        "userGesture": true})
}

fn order(expires_at_ms: i64) -> Value {
    json!({"orderId": "order-1", "amount": "8.88", "currency": "CNY", "payee": "Alice",
        "title": "Red packet", "expiresAtMs": expires_at_ms, "operation": "example.poll.pay_confirm"})
}

/// The host names an order by its id only.
fn payment_request(order_id: &str) -> Value {
    json!({"pluginId": "example.poll", "capability": "paymentConfirmation", "orderId": order_id,
        "platform": "web", "userGesture": true})
}

/// The pay page's `order` action runs an operation that answers with
/// `order`: the engine records it from that result.
async fn receive_order(f: &Fixture, order: Value) -> Value {
    f.answer("example.poll.hide", vec![json!({"confirmation": order})]);
    press(f, "order").await
}

/// The provider's ticket for a password it accepted.
fn ticket() -> Value {
    json!({"ticket": "ticket_1-a", "expiresAtMs": now_ms() + 60_000})
}

fn ready_upload() -> Value {
    use sha2::{Digest, Sha256};
    json!({"fileId": "file:front-1", "ready": true, "contentType": "image/jpeg", "sizeBytes": 4,
        "sha256": hex::encode(Sha256::digest([0xFF, 0xD8, 0xFF, 0xD9]))})
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[tokio::test]
async fn a_capability_runs_only_when_manifest_host_and_viewer_all_allow_it() {
    // The host profile does not allow it: refused before anyone is asked.
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(&f, &["documentCapture"], &["locationOnce"], host.clone());
    let error = f
        .api
        .plugin_capability_invoke(capture_request())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sdk.plugin.native_capability_unavailable"),
        "{error}"
    );
    assert!(host.consents().is_empty());
    assert_eq!(host.captures.load(Ordering::SeqCst), 0);

    // The manifest does not declare it: refused the same way.
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(&f, &["locationOnce"], &["documentCapture"], host.clone());
    let error = f
        .api
        .plugin_capability_invoke(capture_request())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sdk.plugin.native_capability_unavailable"),
        "{error}"
    );
    assert!(host.consents().is_empty());

    // Both allow it, the viewer does not: nothing is captured or uploaded.
    let f = Fixture::new().await;
    let host = FakeHost::new();
    host.decide(ConsentDecision::Deny);
    setup(&f, &["documentCapture"], &["documentCapture"], host.clone());
    assert_eq!(
        f.api
            .plugin_capability_invoke(capture_request())
            .await
            .unwrap(),
        json!({"kind": "denied"})
    );
    assert_eq!(host.consents(), vec![r#"documentCapture{"side":"front"}"#]);
    assert_eq!(host.captures.load(Ordering::SeqCst), 0);
    assert!(f.calls_to("example.poll.material_prepare").is_empty());

    // All three allow it: the document is captured, uploaded privately
    // through the input's declared upload and handed back for the input.
    host.decide(ConsentDecision::Always);
    f.answer("example.poll.material_prepare", vec![ready_upload()]);
    assert_eq!(
        f.api
            .plugin_capability_invoke(capture_request())
            .await
            .unwrap(),
        json!({"kind": "input", "input": "front", "value": {"fileId": "file:front-1"}})
    );
    assert_eq!(host.captures.load(Ordering::SeqCst), 1);
    let prepare = f.calls_to("example.poll.material_prepare");
    assert_eq!(prepare.len(), 1);
    assert_eq!(prepare[0]["payload"]["commandId"], "capture-front");
    assert_eq!(prepare[0]["payload"]["purpose"], "identity");
    // The grant is kept: the next capture asks no more.
    f.api
        .plugin_capability_invoke(capture_request())
        .await
        .unwrap();
    assert_eq!(host.consents().len(), 2);
    assert_eq!(host.captures.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn picking_and_the_clipboard_need_no_consent() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(&f, &["clipboardWrite"], &["clipboardWrite"], host.clone());
    let request = json!({"pluginId": "example.poll", "capability": "clipboardWrite",
        "platform": "web", "userGesture": true});
    assert_eq!(
        f.api.plugin_capability_invoke(request).await.unwrap(),
        json!({"kind": "host", "capability": "clipboardWrite"})
    );
    assert!(host.consents().is_empty());
    assert!(!NativeCapability::SelectImage.needs_consent());
    assert!(!NativeCapability::SelectFile.needs_consent());
    assert!(NativeCapability::Camera.needs_consent());
}

#[tokio::test]
async fn a_document_photo_with_exif_or_of_another_type_is_refused() {
    let f = Fixture::new().await;
    let mut host = FakeHost::new();
    {
        let host = Arc::get_mut(&mut host).unwrap();
        let mut bytes = vec![0xFF, 0xD8, 0xFF, 0xE1, 0x00, 0x08];
        bytes.extend_from_slice(b"Exif\0\0");
        bytes.extend_from_slice(&[0xFF, 0xD9]);
        host.file.bytes = bytes;
    }
    setup(&f, &["documentCapture"], &["documentCapture"], host.clone());
    assert!(
        f.api
            .plugin_capability_invoke(capture_request())
            .await
            .is_err()
    );
    assert!(f.calls_to("example.poll.material_prepare").is_empty());

    let f = Fixture::new().await;
    let mut host = FakeHost::new();
    Arc::get_mut(&mut host).unwrap().file.content_type = "image/png".into();
    setup(&f, &["documentCapture"], &["documentCapture"], host.clone());
    assert!(
        f.api
            .plugin_capability_invoke(capture_request())
            .await
            .is_err()
    );
    assert!(f.calls_to("example.poll.material_prepare").is_empty());
}

#[tokio::test]
async fn an_expired_order_is_refused_and_an_open_one_calls_its_transient_operation() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["paymentConfirmation"],
        &["paymentConfirmation"],
        host.clone(),
    );

    // Closed: refused before the viewer is asked anything.
    receive_order(&f, order(now_ms() - 1)).await;
    let error = f
        .api
        .plugin_capability_invoke(payment_request("order-1"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sdk.plugin.payment_order_expired"),
        "{error}"
    );
    assert!(host.consents().is_empty());
    assert_eq!(host.payments.load(Ordering::SeqCst), 0);
    assert!(f.calls_to("example.poll.pay_confirm").is_empty());

    // An operation of another plugin (never recorded), one the catalog does
    // not offer, or one it offers that is not transient.
    for (id, operation) in [
        ("order-a", "example.other.pay"),
        ("order-b", "example.poll.not_granted"),
        ("order-c", "example.poll.material_prepare"),
    ] {
        let mut foreign = order(now_ms() + 60_000);
        foreign["orderId"] = json!(id);
        foreign["operation"] = json!(operation);
        receive_order(&f, foreign).await;
        assert!(
            f.api
                .plugin_capability_invoke(payment_request(id))
                .await
                .is_err(),
            "{operation}"
        );
    }
    assert_eq!(host.payments.load(Ordering::SeqCst), 0);

    // Open: the host draws the recorded order, the password goes to the
    // provider only, and the transient operation gets the provider's ticket.
    receive_order(&f, order(now_ms() + 60_000)).await;
    f.answer("example.pay.ticket", vec![ticket()]);
    f.answer("example.poll.pay_confirm", vec![json!({"paid": true})]);
    let outcome = f
        .api
        .plugin_capability_invoke(payment_request("order-1"))
        .await
        .unwrap();
    assert_eq!(outcome["kind"], "operation");
    assert_eq!(outcome["response"]["data"], json!({"paid": true}));
    let asked = f.calls_to("example.pay.ticket");
    assert_eq!(asked.len(), 1);
    let expected = order(now_ms() + 60_000);
    assert_eq!(asked[0]["payload"]["pluginId"], "example.poll");
    assert_eq!(asked[0]["payload"]["orderId"], "order-1");
    assert_eq!(asked[0]["payload"]["amount"], expected["amount"]);
    assert_eq!(asked[0]["payload"]["currency"], expected["currency"]);
    assert_eq!(asked[0]["payload"]["payee"], expected["payee"]);
    assert_eq!(
        asked[0]["payload"]["confirmation"],
        json!({"method": "paymentPassword", "password": "123456"})
    );
    let calls = f.calls_to("example.poll.pay_confirm");
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["payload"],
        json!({"orderId": "order-1", "ticket": "ticket_1-a"}),
        "the plugin gets the ticket, never the password"
    );

    // Paid: the order is forgotten, so it cannot be confirmed again.
    assert!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .is_err()
    );

    // The viewer backs out: nothing is called.
    receive_order(&f, order(now_ms() + 60_000)).await;
    *host.proof.lock().unwrap() = CapabilityHostAnswer::Cancelled;
    assert_eq!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .unwrap(),
        json!({"kind": "cancelled"})
    );
    assert_eq!(f.calls_to("example.pay.ticket").len(), 1);
    assert_eq!(f.calls_to("example.poll.pay_confirm").len(), 1);

    // A payment confirmation is never a declared target, and a host cannot
    // hand the engine an order of its own.
    let mut declared = payment_request("order-1");
    declared.as_object_mut().unwrap().remove("orderId");
    assert!(f.api.plugin_capability_invoke(declared).await.is_err());
    let mut supplied = payment_request("order-1");
    supplied["order"] = order(now_ms() + 60_000);
    assert!(f.api.plugin_capability_invoke(supplied).await.is_err());
}

#[tokio::test]
async fn an_order_the_engine_did_not_record_is_never_drawn() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["paymentConfirmation"],
        &["paymentConfirmation"],
        host.clone(),
    );
    // Recorded for another plugin's page? Never: only this viewer's order of
    // this plugin. An id nobody returned is refused before anyone is asked.
    let error = f
        .api
        .plugin_capability_invoke(payment_request("order-made-up"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sdk.plugin.ui_action_unavailable"),
        "{error}"
    );
    assert!(host.consents().is_empty());
    assert_eq!(host.payments.load(Ordering::SeqCst), 0);

    // A received order is recorded for its viewer and plugin only.
    receive_order(&f, order(now_ms() + 60_000)).await;
    let orders = &f.api.plugin_host.capability_host.orders;
    let key = |viewer: &str, plugin: &str| {
        super::native::HeldPaymentKey::new("201", viewer, plugin, "order-1")
    };
    assert!(orders.get(&key("u1", "example.poll")).is_some());
    assert!(orders.get(&key("u2", "example.poll")).is_none());
    assert!(orders.get(&key("u1", "example.pay")).is_none());
}

#[tokio::test]
async fn without_a_payment_provider_no_credential_is_asked_for() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["paymentConfirmation"],
        &["paymentConfirmation"],
        host.clone(),
    );
    receive_order(&f, order(now_ms() + 60_000)).await;
    for provider in [
        Value::Null,
        // Named, but the operation is not a transient one of that plugin.
        json!({"pluginId": "example.pay", "operation": "example.pay.other"}),
        // The plugin asking for the payment cannot be made the provider by
        // naming an operation of its own that the catalog does not mark.
        json!({"pluginId": "example.poll", "operation": "example.poll.pay_confirm2"}),
    ] {
        f.catalog.lock().unwrap()["paymentProvider"] = provider;
        let error = f
            .api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("sdk.plugin.native_capability_unavailable"),
            "{error}"
        );
    }
    assert!(host.consents().is_empty());
    assert_eq!(host.payments.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_refused_password_reaches_no_plugin_and_the_order_stays_open() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["paymentConfirmation"],
        &["paymentConfirmation"],
        host.clone(),
    );
    receive_order(&f, order(now_ms() + 60_000)).await;
    f.errors.lock().unwrap().insert(
        "example.pay.ticket".into(),
        (409, "BUSINESS_CONFLICT".into()),
    );
    assert!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .is_err()
    );
    assert!(f.calls_to("example.poll.pay_confirm").is_empty());
    // A ticket that does not read as one goes nowhere either.
    f.errors.lock().unwrap().remove("example.pay.ticket");
    f.answer("example.pay.ticket", vec![json!({"ticket": "a b"})]);
    assert!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .is_err()
    );
    assert!(f.calls_to("example.poll.pay_confirm").is_empty());
    // The same order is confirmed on the next try.
    f.answer("example.pay.ticket", vec![ticket()]);
    let outcome = f
        .api
        .plugin_capability_invoke(payment_request("order-1"))
        .await
        .unwrap();
    assert_eq!(outcome["kind"], "operation");
    assert_eq!(f.calls_to("example.poll.pay_confirm").len(), 1);
}

#[tokio::test]
async fn a_bundle_gets_a_location_without_a_page_input_and_its_payment_result() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["locationOnce", "paymentConfirmation"],
        &["locationOnce", "paymentConfirmation"],
        host.clone(),
    );
    let fix = f
        .api
        .bundle_capability_invoke(json!({"pluginId": "example.poll",
            "capability": "locationOnce", "params": {"precision": "coarse"},
            "platform": "web", "userGesture": true}))
        .await
        .unwrap();
    assert_eq!(
        fix,
        json!({"kind": "location", "location": {"latitude": 31.23, "longitude": 121.47,
            "accuracyM": 1000.0, "precision": "coarse"}})
    );
    // A host still names the page input a fix goes into.
    assert!(
        f.api
            .plugin_capability_invoke(json!({"pluginId": "example.poll",
                "capability": "locationOnce", "params": {"precision": "coarse"},
                "platform": "web", "userGesture": true}))
            .await
            .is_err()
    );

    // A pay action's order confirmed from a bundle answers with the order
    // operation's result, never the action's continuation, which stays held.
    f.answer(
        "example.poll.hide",
        vec![json!({"confirmation": order(now_ms() + 60_000)})],
    );
    press(&f, "pay").await;
    assert_eq!(f.api.plugin_host.capability_host.payments.len(), 1);
    f.answer("example.pay.ticket", vec![ticket()]);
    f.answer("example.poll.pay_confirm", vec![json!({"currency": "USD"})]);
    let paid = f
        .api
        .bundle_capability_invoke(payment_request("order-1"))
        .await
        .unwrap();
    assert_eq!(paid["kind"], "operation", "{paid}");
    assert_eq!(paid["response"]["data"], json!({"currency": "USD"}));
    assert_eq!(f.api.plugin_host.capability_host.payments.len(), 1);
}

#[tokio::test]
async fn an_operation_result_with_an_order_hands_it_to_the_host_and_records_it() {
    let f = Fixture::new().await;
    let response = CapabilityDispatchResult {
        request_id: "r".into(),
        success: true,
        plugin_id: "example.poll".into(),
        capability_id: "example.poll.send".into(),
        data: json!({"confirmation": order(i64::MAX)}),
        error: None,
    };
    let outcome = f
        .api
        .operation_outcome(Some("u1"), "example.poll", &response);
    assert_eq!(outcome["kind"], "operation");
    assert_eq!(outcome["confirmation"]["orderId"], "order-1");
    let recorded = f
        .api
        .plugin_host
        .capability_host
        .orders
        .get(&super::native::HeldPaymentKey::new(
            "201",
            "u1",
            "example.poll",
            "order-1",
        ))
        .unwrap();
    assert_eq!(recorded.operation, "example.poll.pay_confirm");
    // Another plugin's operation is not this plugin's order.
    assert!(
        f.api
            .operation_outcome(Some("u1"), "example.other", &response)
            .get("confirmation")
            .is_none()
    );
    assert!(
        f.api
            .plugin_host
            .capability_host
            .orders
            .get(&super::native::HeldPaymentKey::new(
                "201",
                "u1",
                "example.other",
                "order-1",
            ))
            .is_none()
    );
}

#[tokio::test]
async fn consent_is_asked_again_after_a_revoke_a_finer_precision_or_a_new_major() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(&f, &["locationOnce"], &["locationOnce"], host.clone());

    let coarse = f
        .api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    assert_eq!(
        coarse,
        json!({"kind": "input", "input": "area", "value": {"latitude": 31.23,
            "longitude": 121.47, "accuracyM": 1000.0, "precision": "coarse"}})
    );
    assert_eq!(host.consents().len(), 1);
    // Kept: not asked again for the same precision.
    f.api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    assert_eq!(host.consents().len(), 1);
    // A finer precision is more sensitive: asked again, then kept.
    let fine = f
        .api
        .plugin_capability_invoke(location_request("fine"))
        .await
        .unwrap();
    assert_eq!(fine["input"], "spot");
    assert_eq!(fine["value"]["latitude"], 31.23042);
    assert_eq!(fine["value"]["accuracyM"], 8.0);
    assert_eq!(host.consents().len(), 2);
    f.api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    assert_eq!(host.consents().len(), 2);

    let listed = f.api.plugin_capability_consents(json!({})).await.unwrap();
    assert_eq!(listed["consents"].as_array().unwrap().len(), 1);
    assert_eq!(listed["consents"][0]["capability"], "locationOnce");
    assert_eq!(listed["consents"][0]["manifestMajor"], "0.2");
    assert_eq!(listed["consents"][0]["level"], 1);

    // Revoked: asked again.
    assert_eq!(
        f.api
            .revoke_plugin_capability_consent(
                json!({"pluginId": "example.poll", "capability": "locationOnce"})
            )
            .await
            .unwrap(),
        json!({"revoked": 1})
    );
    f.api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    assert_eq!(host.consents().len(), 3);

    // A new major of the plugin: asked again.
    f.catalog.lock().unwrap()["plugins"][0]["version"] = json!("1.0.0");
    f.api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    assert_eq!(host.consents().len(), 4);

    // Refused: not kept, asked again next time.
    host.decide(ConsentDecision::Deny);
    f.api
        .revoke_plugin_capability_consent(json!({"pluginId": "example.poll"}))
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            f.api
                .plugin_capability_invoke(location_request("coarse"))
                .await
                .unwrap(),
            json!({"kind": "denied"})
        );
    }
    assert_eq!(host.consents().len(), 6);
}

#[tokio::test]
async fn allowed_once_runs_this_request_and_keeps_nothing() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    host.decide(ConsentDecision::Once);
    setup(&f, &["locationOnce"], &["locationOnce"], host.clone());

    for asked in 1..=2 {
        let located = f
            .api
            .plugin_capability_invoke(location_request("coarse"))
            .await
            .unwrap();
        assert_eq!(located["kind"], "input");
        assert_eq!(located["input"], "area");
        // Nothing was kept: the next request asks again.
        assert_eq!(host.consents().len(), asked);
        let listed = f.api.plugin_capability_consents(json!({})).await.unwrap();
        assert!(listed["consents"].as_array().unwrap().is_empty());
    }

    // "Always" is kept; a later "once" answer is never asked for.
    host.decide(ConsentDecision::Always);
    f.api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    host.decide(ConsentDecision::Once);
    f.api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    assert_eq!(host.consents().len(), 3);
    let listed = f.api.plugin_capability_consents(json!({})).await.unwrap();
    assert_eq!(listed["consents"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn without_a_gesture_or_with_a_media_session_nothing_runs() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(&f, &["locationOnce"], &["locationOnce"], host.clone());
    let mut request = location_request("coarse");
    request["userGesture"] = json!(false);
    assert!(f.api.plugin_capability_invoke(request).await.is_err());
    let rtc = json!({"pluginId": "example.poll", "capability": "rtcSession",
        "params": {"media": ["audio"]}, "platform": "web", "userGesture": true});
    assert!(f.api.plugin_capability_invoke(rtc).await.is_err());
    assert!(host.consents().is_empty());
}

#[tokio::test]
async fn a_location_goes_only_into_a_location_input_of_its_precision() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(&f, &["locationOnce"], &["locationOnce"], host.clone());
    for request in [
        // A fine fix into the coarse input, a coarse one into the fine input.
        location_into("fine", "area"),
        location_into("coarse", "spot"),
        // Not a location input, not an input of the page.
        location_into("coarse", "note"),
        location_into("coarse", "missing"),
    ] {
        assert!(
            f.api
                .plugin_capability_invoke(request.clone())
                .await
                .is_err(),
            "{request}"
        );
    }
    // No into, or no page to write into.
    let mut bare = location_request("coarse");
    bare.as_object_mut().unwrap().remove("into");
    assert!(f.api.plugin_capability_invoke(bare).await.is_err());
    let mut pageless = location_request("coarse");
    pageless.as_object_mut().unwrap().remove("page");
    assert!(f.api.plugin_capability_invoke(pageless).await.is_err());
    // Nothing was asked of the viewer for a broken target.
    assert!(host.consents().is_empty());

    // The value the engine writes passes the page form's own check.
    let outcome = f
        .api
        .plugin_capability_invoke(location_request("coarse"))
        .await
        .unwrap();
    let value: flare_plugin_protocol::ui::UiLocationValue =
        serde_json::from_value(outcome["value"].clone()).unwrap();
    assert!(value.fits(flare_plugin_protocol::ui::UiLocationPrecision::Coarse));
}

#[tokio::test]
async fn an_order_is_refused_while_the_catalog_lists_no_transient_operation() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["paymentConfirmation"],
        &["paymentConfirmation"],
        host.clone(),
    );
    receive_order(&f, order(now_ms() + 60_000)).await;
    // An older Core lists none.
    f.catalog.lock().unwrap()["plugins"][0]
        .as_object_mut()
        .unwrap()
        .remove("transientOperations");
    assert!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .is_err()
    );
    assert!(host.consents().is_empty());
    assert_eq!(host.payments.load(Ordering::SeqCst), 0);
    assert!(f.calls_to("example.poll.pay_confirm").is_empty());
}

/// A pay page: its `pay` action's operation answers with an order and
/// continues to the history page with the currency of its result; its
/// `order` action runs the same operation with no continuation.
fn declare_pay_page(f: &Fixture) {
    let mut catalog = f.catalog.lock().unwrap();
    catalog["plugins"][0]["extensions"]["contributions"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "kind": "page", "id": "example.poll.pay", "schemaVersion": 1,
            "requiredPermissions": [], "source": {"operation": "example.poll.wallet"},
            "schema": {"title": "Pay",
                "actions": [{"id": "pay", "label": "Pay", "style": "primary",
                    "target": {"kind": "operation", "operation": "example.poll.hide"},
                    "continuation": {"page": "example.poll.history",
                        "input": {"currency": "/currency"}}},
                    {"id": "order", "label": "Order",
                    "target": {"kind": "operation", "operation": "example.poll.hide"}}]}
        }));
}

/// Press `action` on the pay page.
async fn press(f: &Fixture, action: &str) -> Value {
    f.answer(
        "example.poll.wallet",
        vec![json!({"balanceText": "¥1.00", "currency": "CNY"})],
    );
    let page = f
        .api
        .plugin_page(
            json!({"pluginId": "example.poll", "page": "example.poll.pay",
            "platform": "web"}),
        )
        .await
        .unwrap();
    f.api
        .invoke_plugin_action(json!({
            "page": {"pluginId": "example.poll", "page": "example.poll.pay"},
            "platform": "web", "actionId": action, "userGesture": true, "data": page["data"]
        }))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_pay_actions_continuation_runs_only_after_its_order_is_confirmed() {
    let f = Fixture::new().await;
    let host = FakeHost::new();
    setup(
        &f,
        &["paymentConfirmation"],
        &["paymentConfirmation"],
        host.clone(),
    );
    f.answer(
        "example.poll.hide",
        vec![json!({"confirmation": order(now_ms() + 60_000)})],
    );
    // The action's operation answered with an order: the host gets it to
    // confirm, and the continuation does not run yet.
    let outcome = press(&f, "pay").await;
    assert_eq!(outcome["kind"], "operation", "{outcome}");
    assert_eq!(outcome["confirmation"]["orderId"], "order-1");
    assert_eq!(
        f.api.plugin_host.capability_host.payments.len(),
        1,
        "the continuation is held for the order"
    );

    // Refused: nothing is paid and nothing continues.
    host.decide(ConsentDecision::Deny);
    assert_eq!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .unwrap(),
        json!({"kind": "denied"})
    );
    // Cancelled: the same.
    host.decide(ConsentDecision::Always);
    *host.proof.lock().unwrap() = CapabilityHostAnswer::Cancelled;
    assert_eq!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .unwrap(),
        json!({"kind": "cancelled"})
    );
    assert!(f.calls_to("example.poll.pay_confirm").is_empty());

    // The transient operation fails: no continuation, and it is still held
    // for another try of the same order.
    *host.proof.lock().unwrap() = CapabilityHostAnswer::Done(
        json!({"method": "paymentPassword", "password": "000000"})
            .as_object()
            .unwrap()
            .clone(),
    );
    f.answer("example.pay.ticket", vec![ticket()]);
    f.errors.lock().unwrap().insert(
        "example.poll.pay_confirm".into(),
        (409, "BUSINESS_CONFLICT".into()),
    );
    assert!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .is_err()
    );
    assert_eq!(f.calls_to("example.poll.pay_confirm").len(), 1);
    assert_eq!(f.api.plugin_host.capability_host.payments.len(), 1);

    // Confirmed and accepted: the continuation runs against the transient
    // operation's result, as it would have after the action's own.
    f.errors.lock().unwrap().remove("example.poll.pay_confirm");
    f.answer(
        "example.poll.pay_confirm",
        vec![json!({"currency": "USD", "commandId": "cmd-1"})],
    );
    let continued = f
        .api
        .plugin_capability_invoke(payment_request("order-1"))
        .await
        .unwrap();
    assert_eq!(continued["kind"], "host", "{continued}");
    assert_eq!(
        continued["target"],
        json!({"kind": "page", "page": "example.poll.history"})
    );
    assert_eq!(continued["payload"], json!({"currency": "USD"}));
    let calls = f.calls_to("example.poll.pay_confirm");
    assert_eq!(
        calls[1]["payload"],
        json!({"orderId": "order-1", "ticket": "ticket_1-a"})
    );
    // The password went to the provider each time, never to the plugin.
    assert!(
        f.calls_to("example.pay.ticket")
            .iter()
            .all(|call| call["payload"]["confirmation"]["password"] == "000000")
    );

    // It runs once, and the paid order is forgotten.
    assert_eq!(f.api.plugin_host.capability_host.payments.len(), 0);
    assert!(
        f.api
            .plugin_capability_invoke(payment_request("order-1"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_held_continuation_belongs_to_its_viewer_and_order_only() {
    use super::native::{HeldPaymentKey, PaymentContinuations};
    use super::page::PageContinuationOrigin;
    let held = PaymentContinuations::default();
    let origin = PageContinuationOrigin {
        plugin_id: "example.poll".into(),
        platform: flare_plugin_protocol::manifest::ClientPlatform::Web,
        conversation_id: None,
        user_gesture: true,
    };
    let transition: flare_plugin_protocol::ui::UiPageTransition =
        serde_json::from_value(json!({"page": "example.poll.history", "input": {}})).unwrap();
    let key = |viewer: &str, order: &str| HeldPaymentKey::new("201", viewer, "example.poll", order);
    held.hold(
        key("u1", "order-1"),
        origin.clone(),
        transition.clone(),
        1_000,
        0,
    );
    // Another viewer or another order takes nothing.
    assert!(held.take(&key("u2", "order-1"), 0).is_none());
    assert!(held.take(&key("u1", "order-2"), 0).is_none());
    // Shortly after the order ran out it is still there (grace), once.
    assert!(held.take(&key("u1", "order-1"), 2_000).is_some());
    assert!(held.take(&key("u1", "order-1"), 2_000).is_none());
    // Long run-out entries are dropped when another is held.
    held.hold(key("u1", "old"), origin.clone(), transition.clone(), 0, 0);
    held.hold(
        key("u1", "new"),
        origin.clone(),
        transition.clone(),
        i64::MAX,
        3_600_000,
    );
    assert_eq!(held.len(), 1);
    // At most a bounded number are held.
    for i in 0..100 {
        held.hold(
            key("u1", &format!("o{i}")),
            origin.clone(),
            transition.clone(),
            i64::MAX,
            0,
        );
    }
    assert!(held.len() <= 32);
}
