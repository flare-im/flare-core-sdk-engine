//! 黄金向量：故障转移决策表、失败分类、去相关抖动退避与重试安排、内部阶段到对外连接状态的投影。
//!
//! 做法与 flare-access 的向量相同：输入写死在本文件的生成函数里，期望由实现算出后落盘；测试回放
//! 文件里的每条输入并与期望比较，再重新生成整份文件与磁盘比较——行为变化必须伴随向量更新。
//! 选路打分、熔断迁移、UDP 封锁、恢复切回的向量在 `access/tests/vectors/`。
//!
//! 重新生成：`FLARE_LINK_WRITE_VECTORS=1 cargo test -p flare-im-core-sdk --lib golden_vectors`，
//! 然后审阅 diff。

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use flare_access::manifest::BackoffPolicy;
use flare_access::{FailureClass, NeutralReason, Outcome, Protocol};
use flare_core::client::link::{
    AttemptFailure, CloseKind, DisconnectReason, ServerClose, ServerRejection,
};
use serde_json::{Value, json};

use super::failover::{
    Backoff, FailoverAction, FailoverEvent, LinkLoss, RejectionKind, RetryContext,
    attempt_observation, classify_loss, classify_rejection, decide, plan_retry,
};
use super::status::{
    ActionRequiredReason, ConnectionPhase, LinkFacts, LinkPhase, SwitchReason, project,
};

const VECTOR_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors");

fn write_mode() -> bool {
    std::env::var_os("FLARE_LINK_WRITE_VECTORS").is_some()
}

fn load(name: &str) -> Value {
    let path = format!("{VECTOR_DIR}/{name}");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {path}; run with FLARE_LINK_WRITE_VECTORS=1"));
    serde_json::from_str(&text).unwrap()
}

/// 写模式下落盘；否则要求磁盘上的文件与重新生成的一致。
fn ensure_current(name: &str, generated: &Value) {
    let path = format!("{VECTOR_DIR}/{name}");
    if write_mode() {
        std::fs::create_dir_all(VECTOR_DIR).unwrap();
        std::fs::write(
            &path,
            serde_json::to_string_pretty(generated).unwrap() + "\n",
        )
        .unwrap();
        return;
    }
    assert_eq!(
        &load(name),
        generated,
        "{name} is out of date; regenerate with FLARE_LINK_WRITE_VECTORS=1 and review the diff"
    );
}

fn replay(name: &str, evaluate: impl Fn(&Value) -> Value) -> Vec<Value> {
    let file = load(name);
    let mut expected = Vec::new();
    for case in file["cases"].as_array().unwrap() {
        let actual = evaluate(&case["input"]);
        assert_eq!(
            actual, case["expected"],
            "{name} / {}: input {}",
            case["name"], case["input"]
        );
        expected.push(case["expected"].clone());
    }
    expected
}

fn cases(evaluate: impl Fn(&Value) -> Value, inputs: Vec<(&str, Value)>) -> Vec<Value> {
    inputs
        .into_iter()
        .map(|(name, input)| {
            let expected = evaluate(&input);
            json!({ "name": name, "input": input, "expected": expected })
        })
        .collect()
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn window(value: &Value) -> Option<(u64, u64)> {
    let pair = value.as_array()?;
    Some((pair[0].as_u64()?, pair[1].as_u64()?))
}

// ---------------------------------------------------------------------------
// 名字 <-> 值（向量里的字符串是跨端约定，穷举匹配：加了变体就必须在这里起名）
// ---------------------------------------------------------------------------

fn failure_class_name(class: FailureClass) -> &'static str {
    match class {
        FailureClass::Unreachable => "unreachable",
        FailureClass::TlsFailed => "tlsFailed",
        FailureClass::HandshakeTimeout => "handshakeTimeout",
        FailureClass::ConnectAckTimeout => "connectAckTimeout",
        FailureClass::HeartbeatDead => "heartbeatDead",
        FailureClass::ProbeInvalid => "probeInvalid",
        FailureClass::HttpConnectFailed => "httpConnectFailed",
    }
}

fn parse_failure_class(name: &str) -> FailureClass {
    [
        FailureClass::Unreachable,
        FailureClass::TlsFailed,
        FailureClass::HandshakeTimeout,
        FailureClass::ConnectAckTimeout,
        FailureClass::HeartbeatDead,
        FailureClass::ProbeInvalid,
        FailureClass::HttpConnectFailed,
    ]
    .into_iter()
    .find(|class| failure_class_name(*class) == name)
    .unwrap_or_else(|| panic!("unknown failure class {name}"))
}

fn neutral_name(reason: NeutralReason) -> &'static str {
    match reason {
        NeutralReason::Overloaded => "overloaded",
        NeutralReason::AuthRejected => "authRejected",
        NeutralReason::Draining => "draining",
        NeutralReason::NoNetwork => "noNetwork",
        NeutralReason::Cancelled => "cancelled",
        NeutralReason::ServerResponse => "serverResponse",
    }
}

/// 健康簿观察的结果：`success` / `failure:<class>` / `neutral:<reason>`（与 flare-access 向量同写法）。
fn outcome_name(outcome: Outcome) -> String {
    match outcome {
        Outcome::Success { .. } => "success".to_owned(),
        Outcome::Failure(class) => format!("failure:{}", failure_class_name(class)),
        Outcome::Neutral(reason) => format!("neutral:{}", neutral_name(reason)),
    }
}

fn action_reason(name: &str) -> ActionRequiredReason {
    serde_json::from_value(Value::from(name)).unwrap()
}

fn switch_reason(value: &Value) -> Option<SwitchReason> {
    value
        .as_str()
        .map(|name| serde_json::from_value(Value::from(name)).unwrap())
}

const INTERNAL_PHASES: [(&str, LinkPhase); 12] = [
    ("idle", LinkPhase::Idle),
    ("resolving", LinkPhase::Resolving),
    ("connecting", LinkPhase::Connecting),
    ("authenticating", LinkPhase::Authenticating),
    ("syncing", LinkPhase::Syncing),
    ("ready", LinkPhase::Ready),
    ("degraded", LinkPhase::Degraded),
    ("backoff", LinkPhase::Backoff),
    ("recovering", LinkPhase::Recovering),
    ("suspended", LinkPhase::Suspended),
    ("parked", LinkPhase::Parked),
    ("terminated", LinkPhase::Terminated),
];

fn phase_name(phase: LinkPhase) -> &'static str {
    INTERNAL_PHASES
        .iter()
        .find(|(_, candidate)| *candidate == phase)
        .map(|(name, _)| *name)
        .unwrap()
}

fn parse_phase(name: &str) -> LinkPhase {
    INTERNAL_PHASES
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, phase)| *phase)
        .unwrap_or_else(|| panic!("unknown link phase {name}"))
}

// ---------------------------------------------------------------------------
// 决策表（§3.6）
// ---------------------------------------------------------------------------

fn loss_json(loss: &LinkLoss) -> Value {
    match loss {
        LinkLoss::ClientRequested => json!({ "kind": "clientRequested" }),
        LinkLoss::Failed(class) => json!({ "kind": "failed", "class": failure_class_name(*class) }),
        LinkLoss::Planned { window_ms, prefer } => json!({
            "kind": "planned",
            "windowMs": window_ms.map(|(min, max)| json!([min, max])),
            "prefer": prefer,
        }),
        LinkLoss::SessionEnded(reason) => {
            json!({ "kind": "sessionEnded", "reason": reason.as_str() })
        }
        LinkLoss::Kicked => json!({ "kind": "kicked" }),
    }
}

fn parse_loss(value: &Value) -> LinkLoss {
    match value["kind"].as_str().unwrap() {
        "clientRequested" => LinkLoss::ClientRequested,
        "failed" => LinkLoss::Failed(parse_failure_class(value["class"].as_str().unwrap())),
        "planned" => LinkLoss::Planned {
            window_ms: window(&value["windowMs"]),
            prefer: strings(&value["prefer"]),
        },
        "sessionEnded" => LinkLoss::SessionEnded(action_reason(value["reason"].as_str().unwrap())),
        "kicked" => LinkLoss::Kicked,
        other => panic!("unknown loss {other}"),
    }
}

fn parse_event(value: &Value) -> FailoverEvent {
    match value["event"].as_str().unwrap() {
        "linkLost" => FailoverEvent::LinkLost(parse_loss(&value["loss"])),
        "roundExhausted" => FailoverEvent::RoundExhausted,
        "overloaded" => FailoverEvent::Overloaded {
            access_point: value["accessPoint"].as_str().unwrap().to_owned(),
            retry_after_ms: value["retryAfterMs"].as_u64(),
        },
        "networkAvailable" => FailoverEvent::NetworkAvailable {
            link_alive: value["linkAlive"].as_bool().unwrap(),
            current_unhealthy: value["currentUnhealthy"].as_bool().unwrap(),
        },
        "networkUnavailable" => FailoverEvent::NetworkUnavailable,
        other => panic!("unknown event {other}"),
    }
}

fn action_json(action: &FailoverAction) -> Value {
    match action {
        FailoverAction::ConnectNow { prefer } => {
            json!({ "action": "connectNow", "prefer": prefer })
        }
        FailoverAction::ConnectAfter { delay_ms, prefer } => {
            json!({ "action": "connectAfter", "delayMs": delay_ms, "prefer": prefer })
        }
        FailoverAction::RetrySameAfter {
            access_point,
            delay_ms,
        } => {
            json!({ "action": "retrySameAfter", "accessPoint": access_point, "delayMs": delay_ms })
        }
        FailoverAction::Backoff => json!({ "action": "backoff" }),
        FailoverAction::ProbeWake => json!({ "action": "probeWake" }),
        FailoverAction::Suspend => json!({ "action": "suspend" }),
        FailoverAction::Terminate(reason) => {
            json!({ "action": "terminate", "reason": reason.as_str() })
        }
        FailoverAction::Stay => json!({ "action": "stay" }),
    }
}

fn eval_decision(input: &Value) -> Value {
    let event = parse_event(&input["event"]);
    action_json(&decide(&event, input["windowUnit"].as_f64().unwrap_or(0.0)))
}

fn lost(loss: Value) -> Value {
    json!({ "event": { "event": "linkLost", "loss": loss }, "windowUnit": 0.0 })
}

fn decision_vectors() -> Value {
    let mut drain =
        lost(json!({ "kind": "planned", "windowMs": [1_000, 5_000], "prefer": ["ap-b"] }));
    drain["windowUnit"] = 0.25.into();
    json!({
        "description": "Failover decision table. input.event: linkLost{loss} | roundExhausted | overloaded{accessPoint, retryAfterMs?} | networkAvailable{linkAlive, currentUnhealthy} | networkUnavailable. loss.kind: failed{class} | planned{windowMs?, prefer} | sessionEnded{reason} | kicked | clientRequested. windowUnit in [0,1) picks the reconnect moment inside a server window: min + round((max - min) x unit). A failed current access point is replaced at once (no backoff); a round in which every candidate failed or is breaker-open backs off; OVERLOADED waits on the same access point (retryAfterMs, default 2000) and never switches.",
        "cases": cases(eval_decision, vec![
            ("a dead heartbeat switches at once", lost(json!({ "kind": "failed", "class": "heartbeatDead" }))),
            ("a peer close switches at once", lost(json!({ "kind": "failed", "class": "unreachable" }))),
            ("a drain reconnects inside its window to the preferred access point", drain),
            ("a drain without a window reconnects at once", lost(json!({ "kind": "planned", "windowMs": null, "prefer": ["ap-b", "ap-c"] }))),
            ("a session-ending kick terminates", lost(json!({ "kind": "sessionEnded", "reason": "kicked" }))),
            ("an unavailable tenant terminates", lost(json!({ "kind": "sessionEnded", "reason": "tenantUnavailable" }))),
            ("a kick that keeps the session backs off", lost(json!({ "kind": "kicked" }))),
            ("our own close does nothing", lost(json!({ "kind": "clientRequested" }))),
            ("every candidate failed or open: back off", json!({ "event": { "event": "roundExhausted" } })),
            ("overloaded waits on the same access point", json!({ "event": { "event": "overloaded", "accessPoint": "ap-a", "retryAfterMs": 2_500 } })),
            ("overloaded without retry-after waits the default on the same access point", json!({ "event": { "event": "overloaded", "accessPoint": "ap-a" } })),
            ("network back with a healthy link probes it", json!({ "event": { "event": "networkAvailable", "linkAlive": true, "currentUnhealthy": false } })),
            ("network changed and the current access point is unhealthy there", json!({ "event": { "event": "networkAvailable", "linkAlive": true, "currentUnhealthy": true } })),
            ("network back without a link connects at once", json!({ "event": { "event": "networkAvailable", "linkAlive": false, "currentUnhealthy": false } })),
            ("no network suspends", json!({ "event": { "event": "networkUnavailable" } })),
        ]),
    })
}

#[test]
fn decision_vectors_are_current() {
    ensure_current("link_decisions.json", &decision_vectors());
    let expected = replay("link_decisions.json", eval_decision);
    let actions: BTreeSet<String> = expected
        .iter()
        .map(|action| action["action"].as_str().unwrap().to_owned())
        .collect();
    let all = [
        "connectNow",
        "connectAfter",
        "retrySameAfter",
        "backoff",
        "probeWake",
        "suspend",
        "terminate",
        "stay",
    ];
    for action in all {
        assert!(actions.contains(action), "no vector reaches {action}");
    }
}

// ---------------------------------------------------------------------------
// 失败分类：拒绝、握手失败、连接结束 → 处理方式与健康簿观察
// ---------------------------------------------------------------------------

fn rejection_of(value: &Value) -> ServerRejection {
    let mut rejection = ServerRejection::classify(
        value["reason"].as_str().unwrap_or_default(),
        value["code"].as_str(),
    );
    rejection.retry_after_ms = value["retryAfterMs"].as_u64();
    rejection
}

fn handling_json(kind: &RejectionKind) -> Value {
    match kind {
        RejectionKind::Overloaded { retry_after_ms } => {
            json!({ "kind": "overloaded", "retryAfterMs": retry_after_ms })
        }
        RejectionKind::Auth => json!({ "kind": "auth" }),
        RejectionKind::Terminal(reason) => json!({ "kind": "terminal", "reason": reason.as_str() }),
        RejectionKind::NextCandidate { counted } => {
            json!({ "kind": "nextCandidate", "counted": counted })
        }
    }
}

fn attempt_of(value: &Value) -> AttemptFailure {
    let detail = || value["detail"].as_str().unwrap_or_default().to_owned();
    match value["kind"].as_str().unwrap() {
        "unreachable" => AttemptFailure::Unreachable { detail: detail() },
        "tls" => AttemptFailure::Tls { detail: detail() },
        "timeout" => AttemptFailure::Timeout,
        "connectAckTimeout" => AttemptFailure::ConnectAckTimeout,
        "closed" => AttemptFailure::Closed { detail: detail() },
        "rejected" => AttemptFailure::Rejected(rejection_of(value)),
        "unsupported" => AttemptFailure::Unsupported { detail: detail() },
        other => panic!("unknown attempt failure {other}"),
    }
}

fn disconnect_of(value: &Value) -> DisconnectReason {
    let detail = || value["detail"].as_str().unwrap_or_default().to_owned();
    match value["kind"].as_str().unwrap() {
        "clientRequested" => DisconnectReason::ClientRequested,
        "serverClose" => {
            let kind = CloseKind::parse(value["close"].as_str().unwrap()).unwrap();
            let mut close = ServerClose::new(kind).with_prefer_ap(strings(&value["prefer"]));
            if let Some((min, max)) = window(&value["windowMs"]) {
                close =
                    close.with_retry_window(Duration::from_millis(min), Duration::from_millis(max));
            }
            DisconnectReason::ServerClose(close)
        }
        "kicked" => DisconnectReason::Kicked {
            reason: value["reason"].as_str().unwrap().to_owned(),
            ends_session: value["endsSession"].as_bool().unwrap(),
        },
        "heartbeatTimeout" => DisconnectReason::HeartbeatTimeout,
        "peerClosed" => DisconnectReason::PeerClosed { detail: detail() },
        "transportError" => DisconnectReason::TransportError { detail: detail() },
        other => panic!("unknown disconnect {other}"),
    }
}

fn eval_classification(input: &Value) -> Value {
    if let Some(rejection) = input.get("rejection") {
        let rejection = rejection_of(rejection);
        let health =
            attempt_observation(Protocol::Ws, &AttemptFailure::Rejected(rejection.clone()))
                .map(|observation| outcome_name(observation.outcome));
        return json!({
            "code": rejection.code.as_str(),
            "handling": handling_json(&classify_rejection(&rejection)),
            "health": health,
        });
    }
    if let Some(attempt) = input.get("attempt") {
        let failure = attempt_of(attempt);
        return json!({
            "health": attempt_observation(Protocol::Quic, &failure)
                .map(|observation| outcome_name(observation.outcome)),
        });
    }
    let loss = classify_loss(&disconnect_of(&input["disconnect"]));
    let action = decide(
        &FailoverEvent::LinkLost(loss.clone()),
        input["windowUnit"].as_f64().unwrap_or(0.0),
    );
    json!({ "loss": loss_json(&loss), "action": action_json(&action) })
}

fn rejection(reason: &str, code: Option<&str>, retry_after_ms: Option<u64>) -> Value {
    json!({ "rejection": { "reason": reason, "code": code, "retryAfterMs": retry_after_ms } })
}

fn attempt(kind: &str) -> Value {
    json!({ "attempt": { "kind": kind, "detail": "lab" } })
}

fn disconnect(value: Value) -> Value {
    json!({ "disconnect": value, "windowUnit": 0.5 })
}

fn classification_vectors() -> Value {
    json!({
        "description": "Failure classification. input.rejection {reason, code?, retryAfterMs?}: the CONNECT rejection as read from the wire (code from reason, else from code text, TENANT_UNAVAILABLE -> PERMISSION_DENIED, unknown -> CONNECTION_REFUSED) -> handling (overloaded: same access point after retryAfterMs, never counted; auth: renew once; terminal: actionRequired; nextCandidate: try the next candidate, counted against the breaker or not) and the health observation it records. input.attempt {kind}: a handshake failure -> health observation (null: not recorded). input.disconnect: why an established link ended -> loss and the decision for it (windowUnit 0.5). health: success | failure:<class> | neutral:<reason>.",
        "cases": cases(eval_classification, vec![
            ("overloaded with a wait", rejection("OVERLOADED", None, Some(2_500))),
            ("overloaded without a wait", rejection("OVERLOADED", None, None)),
            ("overloaded named only by the code", rejection("too many handshakes", Some("OVERLOADED"), Some(800))),
            ("upgrade required", rejection("UPGRADE_REQUIRED", None, None)),
            ("protocol version mismatch", rejection("PROTOCOL_VERSION_MISMATCH", None, None)),
            ("tenant unavailable", rejection("TENANT_UNAVAILABLE", None, None)),
            ("token expired", rejection("TOKEN_EXPIRED", None, None)),
            ("authentication failed", rejection("AUTHENTICATION_FAILED", None, None)),
            ("drain during the handshake", rejection("DRAIN", Some("SERVICE_UNAVAILABLE"), None)),
            ("server shutdown during the handshake", rejection("SERVER_SHUTDOWN", Some("SERVICE_UNAVAILABLE"), None)),
            ("service unavailable", rejection("SERVICE_UNAVAILABLE", None, None)),
            ("unrecognised refusal", rejection("go away", None, None)),
            ("handshake unreachable", attempt("unreachable")),
            ("handshake tls", attempt("tls")),
            ("handshake timeout", attempt("timeout")),
            ("connect ack timeout", attempt("connectAckTimeout")),
            ("closed before connect ack", attempt("closed")),
            ("protocol not supported here", attempt("unsupported")),
            ("we closed it", disconnect(json!({ "kind": "clientRequested" }))),
            ("drain with a window and preferred access points", disconnect(json!({ "kind": "serverClose", "close": "DRAIN", "windowMs": [400, 900], "prefer": ["ap-b"] }))),
            ("server shutdown without a window", disconnect(json!({ "kind": "serverClose", "close": "SERVER_SHUTDOWN", "windowMs": null, "prefer": [] }))),
            ("rebalance", disconnect(json!({ "kind": "serverClose", "close": "REBALANCE", "windowMs": [1_000, 5_000], "prefer": [] }))),
            ("session-ending kick", disconnect(json!({ "kind": "kicked", "reason": "DEVICE_CONFLICT", "endsSession": true }))),
            ("tenant unavailable kick", disconnect(json!({ "kind": "kicked", "reason": "TENANT_UNAVAILABLE", "endsSession": true }))),
            ("kick that keeps the session", disconnect(json!({ "kind": "kicked", "reason": "ADMIN", "endsSession": false }))),
            ("heartbeat timeout", disconnect(json!({ "kind": "heartbeatTimeout" }))),
            ("peer closed", disconnect(json!({ "kind": "peerClosed", "detail": "eof" }))),
            ("transport error", disconnect(json!({ "kind": "transportError", "detail": "reset" }))),
        ]),
    })
}

#[test]
fn classification_vectors_are_current() {
    ensure_current("link_classification.json", &classification_vectors());
    let expected = replay("link_classification.json", eval_classification);
    // OVERLOADED 在任何写法下都不切换、不计熔断。
    for case in expected
        .iter()
        .filter(|case| case["handling"]["kind"] == "overloaded")
    {
        assert_eq!(case["health"], "neutral:overloaded", "{case}");
    }
}

// ---------------------------------------------------------------------------
// 退避：去相关抖动序列，与一轮全部失败后的重试安排
// ---------------------------------------------------------------------------

fn eval_backoff(input: &Value) -> Value {
    if let Some(units) = input.get("units") {
        let base = input["baseMs"].as_u64().unwrap();
        let cap = input["capMs"].as_u64().unwrap();
        let mut backoff = Backoff::default();
        let sleeps: Vec<u64> = units
            .as_array()
            .unwrap()
            .iter()
            .map(|unit| backoff.next(base, cap, unit.as_f64().unwrap()))
            .collect();
        return json!({ "sleepsMs": sleeps });
    }
    let policy = &input["policy"];
    let policy = BackoffPolicy {
        base_ms: policy["baseMs"].as_u64().unwrap() as u32,
        cap_foreground_ms: policy["capForegroundMs"].as_u64().unwrap() as u32,
        cap_background_ms: policy["capBackgroundMs"].as_u64().unwrap() as u32,
    };
    let mut backoff = Backoff::default();
    let rounds: Vec<Value> = input["rounds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|round| {
            let now_ms = round["nowMs"].as_i64().unwrap();
            let plan = plan_retry(
                &mut backoff,
                &policy,
                RetryContext {
                    foreground: round["foreground"].as_bool().unwrap(),
                    failed_rounds: round["failedRounds"].as_u64().unwrap() as u32,
                    server_window_ms: window(&round["serverWindowMs"]),
                    next_half_open_at_ms: round["nextHalfOpenAtMs"].as_i64(),
                    now_ms,
                },
                round["jitterUnit"].as_f64().unwrap(),
                round["windowUnit"].as_f64().unwrap(),
            );
            json!({ "atMs": plan.at_ms, "waitMs": plan.at_ms - now_ms, "phase": phase_name(plan.phase) })
        })
        .collect();
    json!({ "rounds": rounds })
}

const NOW: i64 = 1_793_779_200_000;

fn round(failed_rounds: u32, jitter: f64) -> Value {
    json!({
        "foreground": true, "failedRounds": failed_rounds, "serverWindowMs": null,
        "nextHalfOpenAtMs": null, "nowMs": NOW, "jitterUnit": jitter, "windowUnit": 0.0,
    })
}

fn with(mut base: Value, patch: Value) -> Value {
    for (key, value) in patch.as_object().unwrap() {
        base[key] = value.clone();
    }
    base
}

fn backoff_vectors() -> Value {
    let policy = json!({ "baseMs": 1_000, "capForegroundMs": 60_000, "capBackgroundMs": 300_000 });
    let plan = |rounds: Vec<Value>| json!({ "policy": policy, "rounds": rounds });
    json!({
        "description": "Decorrelated jitter: sleep = min(cap, between(base, max(prev, base) x 3, unit)) with between(lo, hi, u) = lo + round((hi - lo) x clamp(u, 0, 1)); prev starts at 0; cap is raised to base, base to 1 ms. input.units: one sleep per unit. input.rounds: successive rounds in which every candidate failed (plan_retry): cap = 60 s foreground / 300 s background, 300 s once failedRounds >= 2 (phase recovering, else backoff); a server window (overload / drain) raises the wait to between(min, max, windowUnit); the next round is never earlier than the earliest half-open moment (every access point breaker-open).",
        "cases": cases(eval_backoff, vec![
            ("low draws stay at base", json!({ "baseMs": 1_000, "capMs": 60_000, "units": [0.0, 0.0, 0.0] })),
            ("high draws triple up to the cap", json!({ "baseMs": 1_000, "capMs": 60_000, "units": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0] })),
            ("middle draws", json!({ "baseMs": 1_000, "capMs": 60_000, "units": [0.5, 0.5, 0.5, 0.5, 0.5] })),
            ("draws outside [0,1] clamp", json!({ "baseMs": 1_000, "capMs": 60_000, "units": [1.5, -0.5] })),
            ("a cap below base is raised to base", json!({ "baseMs": 1_000, "capMs": 500, "units": [1.0, 1.0] })),
            ("background cap", json!({ "baseMs": 1_000, "capMs": 300_000, "units": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0] })),
            ("every access point open: wait for the earliest half-open", plan(vec![
                with(round(1, 0.0), json!({ "nextHalfOpenAtMs": NOW + 30_000 })),
            ])),
            ("a half-open sooner than the backoff does not shorten it", plan(vec![
                with(round(1, 1.0), json!({ "nextHalfOpenAtMs": NOW + 500 })),
            ])),
            ("an overload window longer than the backoff wins", plan(vec![
                with(round(1, 0.0), json!({ "serverWindowMs": [10_000, 20_000], "windowUnit": 0.5 })),
            ])),
            ("a backoff longer than the window wins", plan(vec![
                with(round(1, 1.0), json!({ "serverWindowMs": [500, 900], "windowUnit": 1.0 })),
            ])),
            ("two exhausted rounds enter recovering with a five minute cap", plan(vec![
                round(1, 1.0), round(2, 1.0), round(3, 1.0), round(4, 1.0), round(5, 1.0), round(6, 1.0), round(7, 1.0),
            ])),
            ("the foreground cap is one minute", plan(vec![
                round(1, 1.0), round(1, 1.0), round(1, 1.0), round(1, 1.0), round(1, 1.0),
            ])),
            ("the background cap is five minutes", plan(
                (0..7).map(|_| with(round(1, 1.0), json!({ "foreground": false }))).collect(),
            )),
        ]),
    })
}

#[test]
fn backoff_vectors_are_current() {
    ensure_current("link_backoff.json", &backoff_vectors());
    replay("link_backoff.json", eval_backoff);
}

// ---------------------------------------------------------------------------
// 内部阶段 → 对外 connection://status（§3.11 表）
// ---------------------------------------------------------------------------

fn eval_status(input: &Value) -> Value {
    let facts = &input["facts"];
    let link_facts = LinkFacts {
        was_online: facts["wasOnline"].as_bool().unwrap_or(false),
        access_point_id: facts["accessPointId"].as_str().map(str::to_owned),
        cell_id: facts["cellId"].as_str().map(str::to_owned),
        region_id: facts["regionId"].as_str().map(str::to_owned),
        protocol: facts["protocol"].as_str().map(str::to_owned),
        rtt_ms: facts["rttMs"].as_u64().map(|rtt| rtt as u32),
        attempt: facts["attempt"].as_u64().unwrap_or(0) as u32,
        next_retry_at: facts["nextRetryAt"].as_i64(),
        switch_reason: switch_reason(&facts["switchReason"]),
        action_reason: facts["actionReason"].as_str().map(action_reason),
        development_mode: facts["developmentMode"].as_bool().unwrap_or(false),
    };
    serde_json::to_value(project(
        parse_phase(input["phase"].as_str().unwrap()),
        &link_facts,
    ))
    .unwrap()
}

fn status_vectors() -> Value {
    let located = json!({
        "wasOnline": false, "accessPointId": "ap-sg-1", "cellId": "cell-sg-a", "regionId": "sg",
        "protocol": "quic", "rttMs": 42, "attempt": 2, "nextRetryAt": NOW + 4_000,
        "switchReason": null, "actionReason": null, "developmentMode": false,
    });
    let facts = |patch: Value| with(located.clone(), patch);
    let at = |phase: &str, facts: Value| json!({ "phase": phase, "facts": facts });
    json!({
        "description": "Internal link phase + link facts -> the connection://status payload (every field present, null when absent). resolving / connecting / authenticating show connecting before the session was first online and switching (with the switch reason) after; syncing carries rttMs; ready and degraded are online with quality good / poor; backoff is reconnecting with the access point (cell, region, protocol) it will try next, attempt and nextRetryAt; recovering is serviceUnavailable with attempt and nextRetryAt and without the access point; suspended is waitingForNetwork; parked (the link handed over to the push channel while the app is in the background) is online with quality good, field for field what ready gives; terminated is actionRequired with its reason (expired when none was recorded). developmentMode is carried in every phase.",
        "cases": cases(eval_status, vec![
            ("idle", at("idle", facts(json!({})))),
            ("idle in development mode", at("idle", facts(json!({ "developmentMode": true })))),
            ("resolving before the first connect", at("resolving", facts(json!({})))),
            ("connecting before the first connect", at("connecting", facts(json!({})))),
            ("authenticating before the first connect", at("authenticating", facts(json!({})))),
            ("connecting after a failure", at("connecting", facts(json!({ "wasOnline": true, "switchReason": "failure" })))),
            ("resolving after a drain", at("resolving", facts(json!({ "wasOnline": true, "switchReason": "drain" })))),
            ("authenticating while failing back", at("authenticating", facts(json!({ "wasOnline": true, "switchReason": "failback" })))),
            ("connecting to a pinned access point", at("connecting", facts(json!({ "wasOnline": true, "switchReason": "manual", "protocol": "ws" })))),
            ("connecting after a manifest change", at("connecting", facts(json!({ "wasOnline": true, "switchReason": "manifest" })))),
            ("syncing", at("syncing", facts(json!({})))),
            ("ready", at("ready", facts(json!({})))),
            ("degraded", at("degraded", facts(json!({ "rttMs": 1_800 })))),
            ("backoff", at("backoff", facts(json!({ "wasOnline": true })))),
            ("backoff in development mode", at("backoff", facts(json!({ "developmentMode": true, "attempt": 1 })))),
            ("recovering", at("recovering", facts(json!({ "attempt": 3 })))),
            ("suspended", at("suspended", facts(json!({})))),
            ("parked", at("parked", facts(json!({})))),
            ("terminated by a kick", at("terminated", facts(json!({ "actionReason": "kicked" })))),
            ("terminated by an expired token", at("terminated", facts(json!({ "actionReason": "expired" })))),
            ("terminated for an upgrade", at("terminated", facts(json!({ "actionReason": "upgradeRequired" })))),
            ("terminated by an unavailable tenant", at("terminated", facts(json!({ "actionReason": "tenantUnavailable" })))),
            ("terminated without a recorded reason", at("terminated", facts(json!({})))),
        ]),
    })
}

#[test]
fn status_vectors_are_current() {
    ensure_current("link_status.json", &status_vectors());
    let file = load("link_status.json");
    let expected = replay("link_status.json", eval_status);
    let internal: BTreeSet<&str> = file["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| case["input"]["phase"].as_str().unwrap())
        .collect();
    for (name, _) in INTERNAL_PHASES {
        assert!(internal.contains(name), "no vector starts from {name}");
    }
    let external: HashSet<ConnectionPhase> = expected
        .iter()
        .map(|status| serde_json::from_value(status["phase"].clone()).unwrap())
        .collect();
    for phase in [
        ConnectionPhase::Idle,
        ConnectionPhase::Connecting,
        ConnectionPhase::Syncing,
        ConnectionPhase::Online,
        ConnectionPhase::Switching,
        ConnectionPhase::Reconnecting,
        ConnectionPhase::WaitingForNetwork,
        ConnectionPhase::ServiceUnavailable,
        ConnectionPhase::ActionRequired,
    ] {
        assert!(
            external.contains(&phase),
            "no vector reaches {}",
            phase.as_str()
        );
    }
}

// ---------------------------------------------------------------------------
// ipHints 与 DoH：DNS 优先，DNS 失败或结果可疑才直连，DoH 答复排最前（原生端，IM 与 HTTP 共用）
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
mod ip_hint_dial {
    use std::io;
    use std::net::{IpAddr, SocketAddr};

    use flare_core::client::dial::{MAX_DIRECT_ADDRESSES, choose_addresses};
    use serde_json::{Value, json};

    use super::{cases, ensure_current, load, replay, strings};

    fn parse_error(name: &str) -> io::ErrorKind {
        match name {
            "notFound" => io::ErrorKind::NotFound,
            "timedOut" => io::ErrorKind::TimedOut,
            other => panic!("unknown DNS failure {other}"),
        }
    }

    fn error_name(kind: io::ErrorKind) -> &'static str {
        match kind {
            io::ErrorKind::NotFound => "notFound",
            io::ErrorKind::TimedOut => "timedOut",
            other => panic!("unexpected DNS failure {other:?}"),
        }
    }

    fn eval(input: &Value) -> Value {
        let dns: io::Result<Vec<SocketAddr>> = match input["dns"]["error"].as_str() {
            Some(name) => Err(parse_error(name).into()),
            None => Ok(strings(&input["dns"]["addresses"])
                .iter()
                .map(|address| address.parse().unwrap())
                .collect()),
        };
        let ips = |key: &str| -> Vec<IpAddr> {
            strings(&input[key])
                .iter()
                .map(|ip| ip.parse().unwrap())
                .collect()
        };
        let port = input["port"].as_u64().unwrap() as u16;
        match choose_addresses(dns, &ips("ipHints"), &ips("doh"), port) {
            Ok(chosen) => json!({
                "addresses": chosen.addresses.iter().map(ToString::to_string).collect::<Vec<_>>(),
                "direct": chosen.direct,
            }),
            Err(error) => json!({ "error": error_name(error.kind()) }),
        }
    }

    fn input(dns: Value, hints: &[&str], port: u16) -> Value {
        json!({ "dns": dns, "ipHints": hints, "port": port })
    }

    fn doh_input(dns: Value, doh: &[&str], hints: &[&str], port: u16) -> Value {
        json!({ "dns": dns, "doh": doh, "ipHints": hints, "port": port })
    }

    /// 期望里有几个地址来自输入的这一组。
    fn dialed_from(case: &Value, key: &str) -> usize {
        let listed = strings(&case["input"][key]);
        strings(&case["expected"]["addresses"])
            .iter()
            .filter(|address| {
                let address: SocketAddr = address.parse().unwrap();
                listed.contains(&address.ip().to_string())
            })
            .count()
    }

    fn vectors() -> Value {
        json!({
            "description": "Which addresses to dial for a host (flare_core::client::dial::choose_addresses, shared by IM and HTTP). Public DoH answers, when there are any, go first (the first two), then the first two public hints, then the DNS answer, each address once (direct = true); private DoH answers do not count. Without them: a DNS answer with a public address is always used and the IP hints are ignored. A failed lookup, a timed-out lookup or an empty answer falls back to the first two hints on the URL port (direct = true); with no hints the failure stands, an empty answer reporting notFound. An answer with no public address (private, loopback, link-local or unspecified) gives way to the first two public hints and stays after them; when no hint is public it is used as is, so a private deployment with private hints keeps working. TLS SNI, certificate checks and SPKI pins stay on the host name in every case.",
            "cases": cases(eval, vec![
                ("a resolved host ignores the hints", input(json!({ "addresses": ["198.51.100.7:443"] }), &["203.0.113.10"], 443)),
                ("the DNS answer keeps its order and size", input(json!({ "addresses": ["198.51.100.7:443", "[2001:db8::7]:443", "198.51.100.8:443"] }), &["203.0.113.10"], 443)),
                ("a resolved host with no hints", input(json!({ "addresses": ["198.51.100.7:8443"] }), &[], 8443)),
                ("a failed lookup uses the hints on the URL port", input(json!({ "error": "notFound" }), &["203.0.113.10", "203.0.113.11"], 8443)),
                ("a timed-out lookup is a failure", input(json!({ "error": "timedOut" }), &["203.0.113.10"], 443)),
                ("an empty answer is a failure", input(json!({ "addresses": [] }), &["203.0.113.10"], 443)),
                ("IPv6 hints", input(json!({ "error": "notFound" }), &["2001:db8::10"], 443)),
                ("at most two hints are dialed", input(json!({ "error": "notFound" }), &["203.0.113.10", "203.0.113.11", "203.0.113.12"], 443)),
                ("no hints: a failed lookup stands", input(json!({ "error": "notFound" }), &[], 443)),
                ("no hints: a timed-out lookup stands", input(json!({ "error": "timedOut" }), &[], 443)),
                ("no hints: an empty answer is not found", input(json!({ "addresses": [] }), &[], 443)),
                ("a private answer gives way to a public hint", input(json!({ "addresses": ["10.1.2.3:443"] }), &["203.0.113.10"], 443)),
                ("loopback and link-local answers are just as suspicious", input(json!({ "addresses": ["127.0.0.1:443", "169.254.1.1:443", "[fe80::1]:443"] }), &["203.0.113.10"], 443)),
                ("a unique local answer gives way to a public IPv6 hint", input(json!({ "addresses": ["[fd00::1]:8443"] }), &["2001:db8::10"], 8443)),
                ("a suspicious answer only gives way to public hints", input(json!({ "addresses": ["10.1.2.3:443"] }), &["10.0.0.9", "203.0.113.10", "203.0.113.11", "203.0.113.12"], 443)),
                ("a private deployment keeps its private answer", input(json!({ "addresses": ["10.1.2.3:443"] }), &["10.0.0.9"], 443)),
                ("one public address clears the answer", input(json!({ "addresses": ["10.1.2.3:443", "198.51.100.7:443"] }), &["203.0.113.10"], 443)),
                ("a private answer with no hints stands", input(json!({ "addresses": ["10.1.2.3:443"] }), &[], 443)),
                ("DoH answers go first, then public hints, then the DNS answer", doh_input(json!({ "addresses": ["198.51.100.7:443"] }), &["198.51.100.20"], &["203.0.113.10"], 443)),
                ("DoH answers rescue a failed lookup with no hints", doh_input(json!({ "error": "notFound" }), &["198.51.100.20"], &[], 8443)),
                ("at most two DoH answers are dialed", doh_input(json!({ "error": "timedOut" }), &["198.51.100.20", "2001:db8::20", "198.51.100.21"], &[], 443)),
                ("private DoH answers do not count", doh_input(json!({ "addresses": ["198.51.100.7:443"] }), &["10.0.0.9", "127.0.0.1"], &[], 443)),
                ("private hints stay out behind a DoH answer", doh_input(json!({ "error": "notFound" }), &["198.51.100.20"], &["10.0.0.9", "203.0.113.10"], 443)),
                ("a suspicious answer stays last behind a DoH answer", doh_input(json!({ "addresses": ["10.1.2.3:443"] }), &["198.51.100.20"], &["203.0.113.10"], 443)),
                ("an address listed twice is dialed once", doh_input(json!({ "addresses": ["198.51.100.20:443", "198.51.100.7:443"] }), &["198.51.100.20"], &["198.51.100.20"], 443)),
            ]),
        })
    }

    #[test]
    fn ip_hint_vectors_are_current() {
        ensure_current("link_ip_hints.json", &vectors());
        let expected = replay("link_ip_hints.json", eval);
        assert!(
            expected.iter().any(|case| case["direct"] == true),
            "no vector falls back to the hints"
        );
        let file = load("link_ip_hints.json");
        let file_cases = file["cases"].as_array().unwrap();
        assert!(
            file_cases
                .iter()
                .filter(|case| case["expected"]["direct"] == true)
                .all(|case| dialed_from(case, "ipHints") <= MAX_DIRECT_ADDRESSES
                    && dialed_from(case, "doh") <= MAX_DIRECT_ADDRESSES),
            "a vector dials more hints or DoH answers than allowed"
        );
    }
}
