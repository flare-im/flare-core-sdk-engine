//! Review Focus 4, the bridge privilege table: what a bundle may and may not
//! reach through the broker, and that every refusal is a fixed code.
use super::*;
use flare_plugin_protocol::bridge::{
    BridgeConversation, BridgePresentation, BridgeTheme, BridgeThemeMode,
};
use flare_plugin_protocol::bundle::BundleManifest;
use flare_plugin_protocol::ui::ClientPlugin;

const DIGEST: &str = "abababababababababababababababababababababababababababababababab";

fn catalog_plugin(approved: &[&str]) -> ClientPlugin {
    serde_json::from_value(json!({
        "pluginId": "example.board", "version": "1.0.0", "manifestDigest": "sha256:00",
        "grantRevision": 1,
        "extensions": {
            "platforms": ["web"], "contributions": [],
            "nativeCapabilities": ["paymentConfirmation", "locationOnce"],
            "webBundles": [{"id": "board", "sha256": DIGEST, "size": 4096, "bridge": 1,
                "urls": ["https://m1.example.com/board.fwb"], "permissions": approved}]
        },
        "operations": ["example.board.get", "example.board.move", "example.board.pay",
                       "example.board.other"],
        "queryOperations": ["example.board.get"],
        "transientOperations": ["example.board.pay"]
    }))
    .unwrap()
}

/// What `bundle.json` asks for: every permission, three operations (one
/// transient), and the payment confirmation capability.
fn bundle_json() -> BundleManifest {
    serde_json::from_value(json!({
        "id": "board", "entry": "index.html", "bridge": 1,
        "permissions": ["plugin.query", "plugin.command", "ui.navigate", "storage",
                        "clipboard", "ui.openExternal"],
        "operations": ["example.board.get", "example.board.move", "example.board.pay"],
        "capabilities": ["paymentConfirmation"],
        "locales": ["en"]
    }))
    .unwrap()
}

fn session_with(approved: &[&str], host_native: &[&str]) -> BridgeSession {
    let plugin = catalog_plugin(approved);
    let host: BTreeSet<String> = host_native.iter().map(|n| (*n).to_owned()).collect();
    let grant = BridgeGrant::new(
        &bundle_json(),
        plugin.web_bundle("board").unwrap(),
        &plugin,
        &host,
    );
    let hello = HelloResult {
        bridge: 1,
        plugin_id: "example.board".into(),
        version: "1.0.0".into(),
        contribution_id: "example.board.page".into(),
        route: "/".into(),
        arguments: Default::default(),
        locale: "en".into(),
        theme: BridgeTheme {
            mode: BridgeThemeMode::Light,
            tokens: Default::default(),
        },
        presentation: BridgePresentation::Page,
        conversation: Some(BridgeConversation {
            id: "c1".into(),
            kind: "group".into(),
        }),
        permissions: grant.permissions.clone(),
        capabilities: grant.capabilities.clone(),
        saved_state: None,
    };
    BridgeSession::new(
        "s1".into(),
        SessionContext {
            tenant_id: "t1".into(),
            viewer: "u1".into(),
            plugin_id: "example.board".into(),
            plugin_version: "1.0.0".into(),
            contribution_id: "example.board.page".into(),
            route: "/".into(),
            bundle_id: "board".into(),
            sha256: DIGEST.into(),
            conversation_id: Some("c1".into()),
            message_id: None,
        },
        grant,
        hello,
    )
}

/// Approved by the tenant: query, command, navigation and storage; not the
/// clipboard nor external links.
fn session() -> BridgeSession {
    session_with(
        &["plugin.query", "plugin.command", "ui.navigate", "storage"],
        &["paymentConfirmation"],
    )
}

fn request(id: &str, method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(
        &json!({"bridge": 1, "id": id, "type": "request", "method": method,
        "params": params}),
    )
    .unwrap()
}

fn greeted() -> BridgeSession {
    let mut session = session();
    assert!(matches!(
        session.admit(&request("h", "host.hello", json!({})), 1, 0),
        Admission::Answer(BridgeEnvelope {
            result: Some(_),
            ..
        })
    ));
    session
}

fn refused(admission: &Admission) -> Option<BridgeErrorCode> {
    match admission {
        Admission::Answer(BridgeEnvelope {
            error: Some(error), ..
        }) => Some(error.code),
        _ => None,
    }
}

#[test]
fn nothing_but_hello_is_served_before_the_handshake() {
    let mut session = session();
    let admission = session.admit(
        &request(
            "r1",
            "plugin.query",
            json!({"operation": "example.board.get"}),
        ),
        1,
        0,
    );
    assert_eq!(refused(&admission), Some(BridgeErrorCode::NotReady));
    let Admission::Answer(hello) = session.admit(&request("h", "host.hello", json!({})), 1, 0)
    else {
        panic!("hello is answered by the engine");
    };
    let result = hello.result.unwrap();
    assert_eq!(result["pluginId"], "example.board");
    assert_eq!(
        result["permissions"],
        json!(["plugin.query", "plugin.command", "ui.navigate", "storage"])
    );
    assert!(matches!(
        session.admit(
            &request(
                "r2",
                "plugin.query",
                json!({"operation": "example.board.get"})
            ),
            1,
            1
        ),
        Admission::Run { .. }
    ));
}

/// The privilege table, request by request.
#[test]
fn the_bridge_privilege_table() {
    use BridgeErrorCode::*;
    let cases: Vec<(&str, &str, Value, Option<BridgeErrorCode>)> = vec![
        // Query: in bundle.json, authorized by the catalog, a query operation.
        (
            "q-ok",
            "plugin.query",
            json!({"operation": "example.board.get"}),
            None,
        ),
        // A command operation through plugin.query.
        (
            "q-cmd",
            "plugin.query",
            json!({"operation": "example.board.move"}),
            Some(PermissionDenied),
        ),
        // Authorized by the catalog but not asked for in bundle.json.
        (
            "q-other",
            "plugin.query",
            json!({"operation": "example.board.other"}),
            Some(PermissionDenied),
        ),
        // Another plugin's operation.
        (
            "q-foreign",
            "plugin.query",
            json!({"operation": "example.wallet.balance"}),
            Some(PermissionDenied),
        ),
        (
            "c-ok",
            "plugin.command",
            json!({"operation": "example.board.move", "input": {"id": "x"}}),
            None,
        ),
        // A transient (money) operation: only through the host's payment confirmation.
        (
            "c-pay",
            "plugin.command",
            json!({"operation": "example.board.pay"}),
            Some(PermissionDenied),
        ),
        (
            "c-other",
            "plugin.command",
            json!({"operation": "example.board.other"}),
            Some(PermissionDenied),
        ),
        // Asked for in bundle.json, not approved by the tenant.
        (
            "clip",
            "clipboard.write",
            json!({"text": "hi"}),
            Some(PermissionDenied),
        ),
        (
            "ext",
            "ui.openExternal",
            json!({"url": "https://example.com/"}),
            Some(PermissionDenied),
        ),
        // Not asked for in bundle.json at all.
        (
            "people",
            "people.resolve",
            json!({"userIds": ["u2"]}),
            Some(PermissionDenied),
        ),
        (
            "media",
            "media.read",
            json!({"fileId": "f1"}),
            Some(PermissionDenied),
        ),
        // Approved and asked for.
        (
            "nav",
            "ui.navigate",
            json!({"page": "example.board.list"}),
            None,
        ),
        ("get", "storage.get", json!({"key": "draft"}), None),
        // Capabilities: listed in bundle.json, in the catalog, offered by the host.
        (
            "cap-pay",
            "host.capability",
            json!({"name": "paymentConfirmation", "params": {}}),
            None,
        ),
        // In the catalog but not in bundle.json.
        (
            "cap-loc",
            "host.capability",
            json!({"name": "locationOnce"}),
            Some(PermissionDenied),
        ),
        (
            "cap-cam",
            "host.capability",
            json!({"name": "camera"}),
            Some(PermissionDenied),
        ),
        // No permission needed.
        ("toast", "ui.toast", json!({"message": "Saved"}), None),
        ("confirm", "ui.confirm", json!({"title": "Delete?"}), None),
        // Unknown method, bad parameters, oversized input.
        ("nope", "plugin.eval", json!({}), Some(MethodNotFound)),
        (
            "bad",
            "plugin.query",
            json!({"operation": "example.board.get", "extra": 1}),
            Some(InvalidParams),
        ),
        (
            "big",
            "plugin.command",
            json!({"operation": "example.board.move",
            "input": {"x": "y".repeat(17 * 1024)}}),
            Some(PayloadTooLarge),
        ),
        (
            "http",
            "ui.openExternal",
            json!({"url": "http://example.com/"}),
            Some(InvalidParams),
        ),
    ];
    for (index, (id, method, params, expected)) in cases.into_iter().enumerate() {
        // A fresh session each time: the rate limit is its own case.
        let mut session = greeted();
        let admission = session.admit(&request(id, method, params), 1, index as i64 * 1000);
        assert_eq!(refused(&admission), expected, "{id} {method}");
        if expected.is_none() {
            assert!(matches!(admission, Admission::Run { .. }), "{id}");
        }
    }
}

#[test]
fn the_host_offering_less_takes_the_capability_away() {
    let mut session = session_with(&["plugin.query"], &[]);
    session.admit(&request("h", "host.hello", json!({})), 1, 0);
    assert_eq!(
        refused(&session.admit(
            &request(
                "p",
                "host.capability",
                json!({"name": "paymentConfirmation"})
            ),
            1,
            0
        )),
        Some(BridgeErrorCode::PermissionDenied)
    );
}

#[test]
fn malformed_envelopes_get_fixed_codes_and_keep_their_id_when_readable() {
    let mut session = greeted();
    let cases: Vec<(Vec<u8>, BridgeErrorCode)> = vec![
        (b"not json".to_vec(), BridgeErrorCode::InvalidEnvelope),
        (
            vec![b' '; flare_plugin_protocol::bridge::MAX_BRIDGE_ENVELOPE_BYTES + 1],
            BridgeErrorCode::PayloadTooLarge,
        ),
        (
            serde_json::to_vec(&json!({"bridge": 2, "id": "r", "type": "request",
                "method": "ui.toast", "params": {"message": "x"}}))
            .unwrap(),
            BridgeErrorCode::UnsupportedBridge,
        ),
        (
            serde_json::to_vec(&json!({"bridge": 1, "id": "r", "type": "response",
                "result": {}}))
            .unwrap(),
            BridgeErrorCode::InvalidEnvelope,
        ),
        (
            serde_json::to_vec(&json!({"bridge": 1, "id": "r", "type": "request",
                "method": "ui.toast", "params": {"message": "x"}, "sneaky": true}))
            .unwrap(),
            BridgeErrorCode::InvalidEnvelope,
        ),
    ];
    for (bytes, code) in cases {
        let admission = session.admit(&bytes, 1, 0);
        assert_eq!(refused(&admission), Some(code));
    }
    let Admission::Answer(answer) = session.admit(
        &serde_json::to_vec(&json!({"bridge": 2, "id": "keep-me", "type": "request",
            "method": "ui.toast"}))
        .unwrap(),
        1,
        0,
    ) else {
        panic!()
    };
    assert_eq!(answer.id.as_deref(), Some("keep-me"));
    assert_eq!(
        answer.error.unwrap().message_key,
        "plugin.bridge.unsupportedBridge"
    );
}

#[test]
fn twenty_requests_a_second_and_four_operations_at_once() {
    let mut session = greeted();
    // The hello took one slot of the first second.
    for i in 0..19 {
        let admission = session.admit(
            &request(&format!("t{i}"), "ui.toast", json!({"message": "x"})),
            1,
            10,
        );
        assert!(matches!(admission, Admission::Run { .. }), "{i}");
        session.finish(&format!("t{i}"));
    }
    assert_eq!(
        refused(&session.admit(&request("over", "ui.toast", json!({"message": "x"})), 1, 20)),
        Some(BridgeErrorCode::RateLimited)
    );
    // A second later the window has moved on.
    assert!(matches!(
        session.admit(
            &request("later", "ui.toast", json!({"message": "x"})),
            1,
            1010
        ),
        Admission::Run { .. }
    ));
    session.finish("later");
    // Four operations run at once; the fifth waits for one to finish.
    for i in 0..4 {
        assert!(matches!(
            session.admit(
                &request(
                    &format!("op{i}"),
                    "plugin.query",
                    json!({"operation": "example.board.get"})
                ),
                1,
                2000 + i
            ),
            Admission::Run { .. }
        ));
    }
    assert_eq!(
        refused(&session.admit(
            &request(
                "op4",
                "plugin.query",
                json!({"operation": "example.board.get"})
            ),
            1,
            2010
        )),
        Some(BridgeErrorCode::RateLimited)
    );
    // Non-operations are not held back by running operations.
    assert!(matches!(
        session.admit(&request("t", "ui.toast", json!({"message": "x"})), 1, 2011),
        Admission::Run { .. }
    ));
    session.finish("op0");
    assert!(matches!(
        session.admit(
            &request(
                "op5",
                "plugin.query",
                json!({"operation": "example.board.get"})
            ),
            1,
            2012
        ),
        Admission::Run { .. }
    ));
}

#[test]
fn a_running_request_id_cannot_be_reused() {
    let mut session = greeted();
    let first = request(
        "same",
        "plugin.query",
        json!({"operation": "example.board.get"}),
    );
    assert!(matches!(session.admit(&first, 1, 0), Admission::Run { .. }));
    assert_eq!(
        refused(&session.admit(&first, 1, 1)),
        Some(BridgeErrorCode::InvalidEnvelope)
    );
}

#[test]
fn a_revoked_session_answers_revoked_and_the_bundle_hears_why() {
    let mut session = greeted();
    let event = session.revoke(RevokeReason::Withdrawn).unwrap();
    assert_eq!(event.event.as_deref(), Some("lifecycle.revoked"));
    assert_eq!(event.payload, Some(json!({"reason": "withdrawn"})));
    assert!(session.revoke(RevokeReason::Disabled).is_none(), "once");
    assert_eq!(
        refused(&session.admit(&request("r", "ui.toast", json!({"message": "x"})), 1, 0)),
        Some(BridgeErrorCode::Revoked)
    );
}

#[test]
fn subscriptions_follow_events_subscribe() {
    let mut session = greeted();
    assert!(!session.subscribed(BridgeEvent::ThemeChanged));
    session.admit(
        &request(
            "s",
            "events.subscribe",
            json!({"topics": ["theme.changed", "entity.state"]}),
        ),
        1,
        0,
    );
    assert!(session.subscribed(BridgeEvent::ThemeChanged));
    assert!(session.subscribed(BridgeEvent::EntityState));
    assert!(!session.subscribed(BridgeEvent::LocaleChanged));
}

/// No engine, Core or plugin text reaches the bundle.
#[test]
fn engine_errors_map_to_fixed_codes_without_text() {
    let cases = [
        (
            FlareError::localized(ErrorCode::PermissionDenied, "internal detail"),
            BridgeErrorCode::PermissionDenied,
        ),
        (
            FlareError::localized(ErrorCode::ServiceUnavailable, "sdk.plugin.outcome_unknown"),
            BridgeErrorCode::Unavailable,
        ),
        (
            FlareError::system("database path /data/x.db failed"),
            BridgeErrorCode::Internal,
        ),
    ];
    for (error, code) in cases {
        let mapped = bridge_error_of(&error);
        assert_eq!(mapped.code, code);
        assert_eq!(mapped.message_key, code.message_key());
        assert!(mapped.plugin_code.is_none());
    }
}

#[test]
fn sessions_are_bounded_and_saved_states_are_used_once() {
    let sessions = BundleSessions::default();
    let mut ids = Vec::new();
    for i in 0..3 {
        let mut session = session();
        session.id = format!("s{i}");
        ids.push(session.id.clone());
        let evicted = sessions.insert(session);
        if i < 2 {
            assert!(evicted.is_empty());
        } else {
            assert_eq!(evicted.len(), 1);
            assert_eq!(evicted[0].id, "s0", "the oldest goes");
        }
    }
    assert!(sessions.with("s0", |_| ()).is_none());
    let context = session().context;
    sessions.save_state(&context, json!({"tab": 2}));
    assert_eq!(sessions.take_saved_state(&context), Some(json!({"tab": 2})));
    assert_eq!(sessions.take_saved_state(&context), None);
    assert_eq!(sessions.drain().len(), 2);
    assert!(sessions.is_empty());
}
