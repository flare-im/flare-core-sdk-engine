//! 黄金向量：选路打分与分层、熔断状态迁移、UDP 封锁、恢复切回、clientPolicy 夹紧、清单验签正负用例、
//! 灰度分桶、发布清单评估、更新提示频率、更新流程中的提示、更新说明语言、资源包选择与校验、URL 解析。
//!
//! 每个向量文件由本文件中的生成函数产出（输入写死在代码里，期望值由实现计算），
//! 测试做两件事：
//! 1. 回放：逐条读取文件里的输入重新计算，与文件里的期望比较（其它语言的实现按同样方式消费）；
//! 2. 新鲜度：重新生成整份文件并与磁盘上的比较，行为变化必须伴随向量更新。
//!
//! 重新生成：`FLARE_ACCESS_WRITE_VECTORS=1 cargo test -p flare-access --test golden_vectors`，
//! 然后审阅 diff。签名用固定种子的测试专用密钥（Ed25519 签名是确定性的），重新生成字节不变。

use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use flare_access::health::{
    AttemptKind, BreakerView, FailureClass, HealthBook, HealthView, NetPrint, NeutralReason,
    Observation, Outcome,
};
use flare_access::manifest::{ClientPolicy, POLICY_BOUNDS, ReleaseManifest, ResourcePack};
use flare_access::parsed_url::ParsedUrl;
use flare_access::platform::RandomSource;
use flare_access::release::{
    ArtifactRecord, CheckResult, Decision, EvaluationInput, HeldPack, InstallOutcome, PackInput,
    PersistedUpdate, PromptHistory, PromptSession, ServerGate, UpdateApp, UpdateMachine,
    UpdatePrompt, Urgency, bucket, decide_prompt, evaluate, plan_packs, select_notes, select_packs,
    verify_pack,
};
use flare_access::select::failback::{FailbackDecision, FailbackInput, evaluate_failback};
use flare_access::select::score::{ApInput, RankParams, pick, rank};
use flare_access::trust::sign::{
    Signer, encode_public_key, issue_key_cert, sign_envelope, sign_keystate,
};
use flare_access::trust::{
    Envelope, KeyCert, KeyScope, KeyState, SerialCheck, TrustRoots, TrustStore,
    VerifiedReleaseManifest, check_serial,
};
use flare_access::types::{ApStatus, Protocol};

const VECTOR_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors");
const FLOAT_TOLERANCE: f64 = 1e-9;

fn write_mode() -> bool {
    std::env::var_os("FLARE_ACCESS_WRITE_VECTORS").is_some()
}

fn load(name: &str) -> Value {
    let path = format!("{VECTOR_DIR}/{name}");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {path}; run with FLARE_ACCESS_WRITE_VECTORS=1"));
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
    assert!(
        approx_eq(&load(name), generated),
        "{name} is out of date; regenerate with FLARE_ACCESS_WRITE_VECTORS=1 and review the diff"
    );
}

/// JSON 比较，浮点按容差。
fn approx_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) => (a - b).abs() <= FLOAT_TOLERANCE * a.abs().max(1.0),
            _ => a == b,
        },
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| approx_eq(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| approx_eq(value, other)))
        }
        _ => left == right,
    }
}

fn replay(name: &str, evaluate: impl Fn(&Value) -> Value) {
    let file = load(name);
    for case in file["cases"].as_array().unwrap() {
        let actual = evaluate(&case["input"]);
        assert!(
            approx_eq(&actual, &case["expected"]),
            "{name} / {}: expected {}, got {}",
            case["name"],
            case["expected"],
            actual
        );
    }
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

// ---------------------------------------------------------------------------
// clientPolicy 夹紧
// ---------------------------------------------------------------------------

fn eval_policy(input: &Value) -> Value {
    serde_json::to_value(ClientPolicy::resolve(input)).unwrap()
}

fn policy_vectors() -> Value {
    let bounds: Vec<Value> = POLICY_BOUNDS
        .iter()
        .map(|(path, bound)| json!({ "path": path, "min": bound.min, "default": bound.default, "max": bound.max }))
        .collect();
    json!({
        "description": "clientPolicy resolution: missing or non-numeric fields take the default, numbers clamp to [min, max] and round half away from zero to integers where the field is integral. Cross-field rules: heartbeat.timeoutSec >= 2 x intervalSec (capped at its max), backgroundIntervalSec >= intervalSec, cooldownMaxSec >= cooldownBaseSec, capForegroundMs >= baseMs, capBackgroundMs >= capForegroundMs. protocolOrder keeps known values in order without duplicates, falling back to [quic, ws] when empty.",
        "bounds": bounds,
        "cases": cases(eval_policy, vec![
            ("empty", json!({})),
            ("null", Value::Null),
            ("design sample", json!({ "raceStaggerMs": 300, "breaker": { "failureThreshold": 3 } })),
            ("below minimums", json!({
                "raceStaggerMs": 0, "connectTimeoutMs": 1,
                "heartbeat": { "intervalSec": 1, "timeoutSec": 1, "backgroundIntervalSec": 1 },
                "probeWakeTimeoutMs": 0,
                "breaker": { "failureThreshold": 0, "failureRatio": 0, "cooldownBaseSec": 0, "cooldownMaxSec": 0 },
                "backoff": { "baseMs": 0, "capForegroundMs": 0, "capBackgroundMs": 0 },
                "probe": { "budgetPerHour": -5, "backgroundIntervalMin": 0 },
                "failback": { "minHealthySpanSec": 0, "minImprovementRatio": 0, "minIntervalSec": 0 },
                "udpBlockedTtlHours": 0, "pinTtlHours": 0
            })),
            ("above maximums", json!({
                "raceStaggerMs": 1e9, "connectTimeoutMs": 1e9,
                "heartbeat": { "intervalSec": 1e9, "timeoutSec": 1e9, "backgroundIntervalSec": 1e9 },
                "probeWakeTimeoutMs": 1e9,
                "breaker": { "failureThreshold": 1e9, "failureRatio": 7, "cooldownBaseSec": 1e9, "cooldownMaxSec": 1e9 },
                "backoff": { "baseMs": 1e9, "capForegroundMs": 1e9, "capBackgroundMs": 1e9 },
                "probe": { "budgetPerHour": 100000, "backgroundIntervalMin": 1e9 },
                "failback": { "minHealthySpanSec": 1e9, "minImprovementRatio": 5, "minIntervalSec": 1e9 },
                "udpBlockedTtlHours": 1e9, "pinTtlHours": 1e9
            })),
            ("cross-field repair", json!({
                "heartbeat": { "intervalSec": 200, "timeoutSec": 30, "backgroundIntervalSec": 60 },
                "breaker": { "cooldownBaseSec": 300, "cooldownMaxSec": 60 },
                "backoff": { "baseMs": 10000, "capForegroundMs": 5000, "capBackgroundMs": 30000 }
            })),
            ("wrong types fall back to defaults", json!({
                "raceStaggerMs": "fast", "connectTimeoutMs": null, "heartbeat": [1, 2],
                "breaker": { "failureRatio": "half" }, "probe": true, "protocolOrder": "quic"
            })),
            ("fractions round", json!({ "raceStaggerMs": 300.6, "breaker": { "failureThreshold": 2.5, "failureRatio": 0.333 } })),
            ("protocol order ws only", json!({ "protocolOrder": ["ws"] })),
            ("protocol order dedupes and drops unknown", json!({ "protocolOrder": ["tcp", "ws", "quic", "ws"] })),
            ("protocol order all unknown", json!({ "protocolOrder": ["tcp", 3] })),
        ]),
    })
}

#[test]
fn client_policy_clamp_vectors() {
    ensure_current("client_policy_clamp.json", &policy_vectors());
    replay("client_policy_clamp.json", eval_policy);
}

// ---------------------------------------------------------------------------
// 选路打分与分层
// ---------------------------------------------------------------------------

fn parse_breaker(value: &Value) -> BreakerView {
    match value.as_str().unwrap_or("closed") {
        "open" => BreakerView::Open { until_ms: 1 },
        "halfOpen" => BreakerView::HalfOpen {
            trial_available: true,
        },
        "halfOpenBusy" => BreakerView::HalfOpen {
            trial_available: false,
        },
        _ => BreakerView::Closed,
    }
}

fn parse_status(value: &Value) -> ApStatus {
    serde_json::from_value(value.clone()).unwrap_or(ApStatus::Active)
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

fn eval_select(input: &Value) -> Value {
    let aps_json = input["accessPoints"].as_array().unwrap();
    let ids: Vec<String> = aps_json
        .iter()
        .map(|ap| ap["id"].as_str().unwrap().to_owned())
        .collect();
    let tags: Vec<Vec<String>> = aps_json.iter().map(|ap| strings(&ap["tags"])).collect();
    let aps: Vec<ApInput<'_>> = aps_json
        .iter()
        .enumerate()
        .map(|(index, ap)| ApInput {
            id: &ids[index],
            weight: ap["weight"].as_u64().unwrap_or(100) as u32,
            priority: ap["priority"].as_u64().unwrap_or(0) as u32,
            status: parse_status(&ap["status"]),
            tags: &tags[index],
            offers_service: ap["offersService"].as_bool().unwrap_or(true),
            excluded: ap["excluded"].as_bool().unwrap_or(false),
            health: HealthView {
                rtt_ms: ap["health"]["rttMs"].as_f64(),
                failure_ratio: ap["health"]["failureRatio"].as_f64().unwrap_or(0.0),
                breaker: parse_breaker(&ap["health"]["breaker"]),
            },
        })
        .collect();
    let net_tags = strings(&input["netTags"]);
    let current = input["current"].as_str();
    let ranking = rank(
        &aps,
        &RankParams {
            net_tags: &net_tags,
            current,
            pinned: input["pinned"].as_str(),
            stickiness: true,
        },
    );
    let chosen = pick(
        &ranking,
        &aps,
        current,
        input["draw"].as_f64().unwrap_or(0.0),
    )
    .map(|position| Value::from(aps[ranking.entries[position].index].id));
    json!({
        "fallbackRttMs": ranking.fallback_rtt_ms,
        "ranking": ranking.entries.iter().map(|entry| json!({
            "id": aps[entry.index].id,
            "score": entry.score,
            "group": entry.group.as_str(),
            "rttMs": entry.rtt_ms,
            "rttKnown": entry.rtt_known,
        })).collect::<Vec<_>>(),
        "tier": ranking.tier().iter().map(|entry| aps[entry.index].id).collect::<Vec<_>>(),
        "chosen": chosen.unwrap_or(Value::Null),
        "pin": ranking.pin.as_str(),
        "filtered": ranking.filtered.iter().map(|(index, reason)| json!({
            "id": aps[*index].id, "reason": reason.as_str(),
        })).collect::<Vec<_>>(),
    })
}

fn ap(id: &str, rtt: Option<f64>) -> Value {
    json!({ "id": id, "weight": 100, "priority": 0, "status": "active", "tags": [], "health": { "rttMs": rtt, "failureRatio": 0.0, "breaker": "closed" } })
}

fn with(mut base: Value, patch: Value) -> Value {
    for (key, value) in patch.as_object().unwrap() {
        if key == "health" {
            for (field, inner) in value.as_object().unwrap() {
                base["health"][field] = inner.clone();
            }
        } else {
            base[key] = value.clone();
        }
    }
    base
}

fn select_vectors() -> Value {
    let four = vec![
        ap("a", Some(40.0)),
        with(
            ap("b", Some(60.0)),
            json!({ "health": { "failureRatio": 0.1 } }),
        ),
        with(ap("c", None), json!({ "priority": 1 })),
        ap("d", Some(100.0)),
    ];
    let weighted = vec![
        ap("a", Some(50.0)),
        with(ap("b", Some(55.0)), json!({ "weight": 300 })),
        ap("c", Some(500.0)),
    ];
    let tagged = vec![
        with(ap("fast-other", Some(10.0)), json!({ "tags": ["isp:cm"] })),
        with(
            ap("slow-matched", Some(200.0)),
            json!({ "tags": ["isp:ct"] }),
        ),
        ap("plain", Some(50.0)),
    ];
    let request = |aps: &Vec<Value>, extra: Value| {
        let mut input = json!({ "netTags": [], "current": null, "pinned": null, "draw": 0.0, "accessPoints": aps });
        for (key, value) in extra.as_object().unwrap() {
            input[key] = value.clone();
        }
        input
    };
    json!({
        "description": "Candidate filtering, scoring and tiered random pick. score = rtt x (1 + 3 x failureRatio) + 50 x priority - 30 (current, stickiness on). Unknown rtt uses the median of known rtts in the input (mean of the two middle values for an even count), else 300. Entries sort by tag group (matched < untagged < other), then score, then id. Tier = leading entries of the best group with score <= sMin + 0.2 x max(sMin, 0) + 20. Pick: current if inside the tier, else weighted by weight using draw in [0,1): first entry whose cumulative weight exceeds draw x total (uniform floor(draw x n) when all weights are 0). breaker: closed | open | halfOpen (trial available) | halfOpenBusy.",
        "floatTolerance": FLOAT_TOLERANCE,
        "cases": cases(eval_select, vec![
            ("fastest alone in tier", request(&four, json!({}))),
            ("sticky current joins and wins tier", request(&four, json!({ "current": "b", "draw": 0.0 }))),
            ("weighted pick low draw", request(&weighted, json!({ "draw": 0.249 }))),
            ("weighted pick boundary draw", request(&weighted, json!({ "draw": 0.25 }))),
            ("weighted pick high draw", request(&weighted, json!({ "draw": 0.999 }))),
            ("zero weights pick uniformly", request(&vec![
                with(ap("a", Some(50.0)), json!({ "weight": 0 })),
                with(ap("b", Some(52.0)), json!({ "weight": 0 })),
            ], json!({ "draw": 0.6 }))),
            ("zero weight loses to weighted peer", request(&vec![
                with(ap("a", Some(50.0)), json!({ "weight": 0 })),
                ap("b", Some(52.0)),
            ], json!({ "draw": 0.0 }))),
            ("matched tag beats faster untagged and other", request(&tagged, json!({ "netTags": ["isp:ct"] }))),
            ("untagged beats other-network tag", request(&tagged, json!({}))),
            ("pin applied", request(&tagged, json!({ "pinned": "fast-other" }))),
            ("pin unavailable falls back", request(&vec![
                with(ap("fast-other", Some(10.0)), json!({ "health": { "breaker": "open" } })),
                ap("plain", Some(50.0)),
            ], json!({ "pinned": "fast-other" }))),
            ("pin unknown access point", request(&tagged, json!({ "pinned": "gone" }))),
            ("filters", request(&vec![
                with(ap("open", Some(10.0)), json!({ "health": { "breaker": "open" } })),
                with(ap("draining", Some(10.0)), json!({ "status": "draining" })),
                with(ap("disabled", Some(10.0)), json!({ "status": "disabled" })),
                with(ap("busy", Some(10.0)), json!({ "health": { "breaker": "halfOpenBusy" } })),
                with(ap("tried", Some(10.0)), json!({ "excluded": true })),
                with(ap("no-service", Some(10.0)), json!({ "offersService": false })),
                with(ap("half-open", Some(80.0)), json!({ "health": { "breaker": "halfOpen", "failureRatio": 0.5 } })),
                ap("ok", Some(90.0)),
            ], json!({}))),
            ("no samples anywhere", request(&vec![ap("a", None), with(ap("b", None), json!({ "priority": 2 }))], json!({}))),
            ("even count median", request(&vec![ap("a", Some(40.0)), ap("b", Some(80.0)), ap("c", None)], json!({}))),
            ("negative best score keeps slack only", request(&vec![ap("a", Some(10.0)), ap("b", Some(25.0)), ap("c", Some(30.0))], json!({ "current": "a" }))),
            ("everything filtered", request(&vec![with(ap("a", Some(10.0)), json!({ "health": { "breaker": "open" } }))], json!({}))),
            ("every access point open", request(&vec![
                with(ap("a", Some(10.0)), json!({ "health": { "breaker": "open" } })),
                with(ap("b", Some(20.0)), json!({ "health": { "breaker": "open" } })),
                with(ap("c", None), json!({ "health": { "breaker": "open" } })),
            ], json!({ "current": "a" }))),
            ("every access point open or with its trial taken", request(&vec![
                with(ap("a", Some(10.0)), json!({ "health": { "breaker": "open" } })),
                with(ap("b", Some(20.0)), json!({ "health": { "breaker": "halfOpenBusy" } })),
            ], json!({}))),
            ("only the half-open trial is left", request(&vec![
                with(ap("a", Some(10.0)), json!({ "health": { "breaker": "open" } })),
                with(ap("b", Some(200.0)), json!({ "health": { "breaker": "halfOpen", "failureRatio": 0.5 } })),
            ], json!({ "current": "a" }))),
            ("a current just outside the tier is left", request(&vec![ap("a", Some(40.0)), ap("b", Some(100.0))], json!({ "current": "b" }))),
            ("a current just inside the tier stays", request(&vec![ap("a", Some(40.0)), ap("b", Some(98.0))], json!({ "current": "b", "draw": 0.0 }))),
        ]),
    })
}

#[test]
fn select_ranking_vectors() {
    ensure_current("select_ranking.json", &select_vectors());
    replay("select_ranking.json", eval_select);
}

// ---------------------------------------------------------------------------
// 熔断状态迁移 与 UDP 封锁（经 HealthBook 回放）
// ---------------------------------------------------------------------------

/// 每步只装入一个抽样值；记录是否被消费，向量据此说明「只有打开/重开才抽样」。
#[derive(Clone, Default)]
struct StepRandom(Arc<Mutex<(Option<f64>, bool)>>);

impl RandomSource for StepRandom {
    fn next_unit(&mut self) -> f64 {
        let mut slot = self.0.lock().unwrap();
        slot.1 = true;
        slot.0.take().unwrap_or(0.5)
    }
}

fn parse_observation(step: &Value) -> Observation {
    let kind = match step["kind"].as_str().unwrap_or("http") {
        "handshake:quic" => AttemptKind::Handshake(Protocol::Quic),
        "handshake:ws" => AttemptKind::Handshake(Protocol::Ws),
        "heartbeat" => AttemptKind::Heartbeat,
        "probe" => AttemptKind::Probe,
        _ => AttemptKind::Http,
    };
    let rtt = step["rttMs"].as_u64().map(|rtt| rtt as u32);
    let outcome = match step["outcome"].as_str().unwrap_or("success") {
        "failure" => Outcome::Failure(FailureClass::Unreachable),
        "neutral:overloaded" => Outcome::Neutral(NeutralReason::Overloaded),
        "neutral:authRejected" => Outcome::Neutral(NeutralReason::AuthRejected),
        "neutral:draining" => Outcome::Neutral(NeutralReason::Draining),
        "neutral:noNetwork" => Outcome::Neutral(NeutralReason::NoNetwork),
        "neutral:cancelled" => Outcome::Neutral(NeutralReason::Cancelled),
        _ => Outcome::Success { rtt_ms: rtt },
    };
    Observation::new(kind, outcome)
}

fn eval_health(input: &Value) -> Value {
    let policy = ClientPolicy::resolve(&input["policy"]);
    let random = StepRandom::default();
    let book = HealthBook::with_random(&policy, Box::new(random.clone()));
    let net_of = |step: &Value| match step["net"].as_str() {
        Some(hint) => NetPrint::from_parts(Some("wifi"), Some(hint)),
        None => NetPrint::UNKNOWN,
    };
    let mut results = Vec::new();
    for step in input["steps"].as_array().unwrap() {
        let at = step["atMs"].as_i64().unwrap();
        let ap = step["ap"].as_str().unwrap_or("ap-1");
        let net = net_of(step);
        *random.0.lock().unwrap() = (step["draw"].as_f64(), false);
        let mut out = json!({});
        if step["action"].as_str() == Some("claimTrial") {
            out["claimed"] = book.claim_trial(ap, net, at).into();
        } else {
            let effect = book.record(ap, net, parse_observation(step), at);
            out["transition"] = match effect.breaker {
                None => Value::Null,
                Some(flare_access::health::BreakerTransition::Opened {
                    until_ms,
                    cooldown_ms,
                }) => {
                    json!({ "kind": "opened", "untilMs": until_ms, "cooldownMs": cooldown_ms })
                }
                Some(flare_access::health::BreakerTransition::Reopened {
                    until_ms,
                    cooldown_ms,
                }) => {
                    json!({ "kind": "reopened", "untilMs": until_ms, "cooldownMs": cooldown_ms })
                }
                Some(flare_access::health::BreakerTransition::Closed) => {
                    json!({ "kind": "closed" })
                }
            };
            out["drawConsumed"] = random.0.lock().unwrap().1.into();
        }
        let report = book.report(ap, net, at);
        out["phase"] = report
            .as_ref()
            .map_or("closed", |report| report.breaker.as_str())
            .into();
        out["openUntilMs"] = report
            .as_ref()
            .and_then(|report| report.open_until_ms)
            .into();
        out["consecutiveFailures"] = report
            .as_ref()
            .map_or(0, |report| report.consecutive_failures)
            .into();
        out["samples"] = report.as_ref().map_or(0, |report| report.samples).into();
        out["failureRatio"] = report
            .as_ref()
            .map_or(0.0, |report| report.failure_ratio)
            .into();
        out["rttMs"] = report.as_ref().and_then(|report| report.rtt_ms).into();
        out["udpBlockedUntilMs"] = book.udp_blocked_until(net, at).into();
        results.push(out);
    }
    json!({ "steps": results })
}

fn step(at: i64, outcome: &str) -> Value {
    json!({ "atMs": at, "kind": "http", "outcome": outcome })
}

fn with_draw(mut value: Value, draw: f64) -> Value {
    value["draw"] = draw.into();
    value
}

fn breaker_vectors() -> Value {
    let fast = json!({ "breaker": { "cooldownBaseSec": 5, "cooldownMaxSec": 60 } });
    json!({
        "description": "HealthBook replay per (access point, network). Each step either records an observation (kind: http | handshake:ws | handshake:quic | heartbeat | probe; outcome: success | failure | neutral:<reason>) or claims a half-open trial. `draw` is the random value offered for this step; it is consumed only when the breaker opens or reopens (cooldown x [0.8 + 0.4 x draw]). Opening: consecutive failures >= failureThreshold, or >= 10 samples with failure ratio >= failureRatio. Half-open failure doubles the cooldown up to cooldownMaxSec; half-open success closes and clears the window. Results while open do not change the breaker. Neutral results release a trial claim and change nothing else. Heartbeat successes only feed the RTT EWMA (alpha 0.3). Trial claims expire after 30 s.",
        "floatTolerance": FLOAT_TOLERANCE,
        "cases": cases(eval_health, vec![
            ("open, failed trial doubles, successful trial closes", json!({ "policy": {}, "steps": [
                step(0, "failure"), step(1, "failure"), with_draw(step(2, "failure"), 0.0),
                json!({ "atMs": 23_999, "action": "claimTrial" }),
                json!({ "atMs": 24_002, "action": "claimTrial" }),
                json!({ "atMs": 24_003, "action": "claimTrial" }),
                with_draw(step(24_010, "failure"), 1.0),
                json!({ "atMs": 96_010, "action": "claimTrial" }),
                step(96_020, "success"),
                step(96_030, "failure"),
            ]})),
            ("neutral results never count", json!({ "policy": {}, "steps": [
                step(0, "failure"), step(1, "neutral:overloaded"), step(2, "neutral:authRejected"),
                step(3, "failure"), step(4, "neutral:draining"), step(5, "neutral:noNetwork"),
                step(6, "neutral:cancelled"), step(7, "success"), step(8, "failure"), step(9, "failure"),
            ]})),
            ("failure ratio needs ten samples", json!({ "policy": {}, "steps": [
                step(0, "success"), step(1, "failure"), step(2, "success"), step(3, "failure"),
                step(4, "success"), step(5, "failure"), step(6, "success"), step(7, "failure"),
                step(8, "success"), with_draw(step(9, "failure"), 0.5),
            ]})),
            ("cooldown caps at max", json!({ "policy": fast, "steps": [
                step(0, "failure"), step(1, "failure"), with_draw(step(2, "failure"), 0.5),
                with_draw(step(5_002, "failure"), 0.5),
                with_draw(step(15_002, "failure"), 0.5),
                with_draw(step(35_002, "failure"), 0.5),
                with_draw(step(75_002, "failure"), 0.5),
                with_draw(step(135_002, "failure"), 0.5),
            ]})),
            ("an overload storm never opens the breaker", json!({ "policy": {}, "steps": [
                step(0, "neutral:overloaded"), step(1, "neutral:overloaded"), step(2, "neutral:overloaded"),
                step(3, "neutral:overloaded"), step(4, "neutral:overloaded"), step(5, "neutral:overloaded"),
                step(6, "neutral:overloaded"), step(7, "neutral:overloaded"), step(8, "neutral:overloaded"),
                step(9, "neutral:overloaded"), step(10, "neutral:overloaded"), step(11, "neutral:overloaded"),
                step(12, "failure"), step(13, "neutral:overloaded"), step(14, "failure"),
            ]})),
            ("results while open do not move the breaker", json!({ "policy": {}, "steps": [
                step(0, "failure"), step(1, "failure"), with_draw(step(2, "failure"), 0.5),
                step(10, "success"), step(20, "failure"),
            ]})),
            ("trial lease expires", json!({ "policy": {}, "steps": [
                step(0, "failure"), step(1, "failure"), with_draw(step(2, "failure"), 0.5),
                json!({ "atMs": 30_002, "action": "claimTrial" }),
                json!({ "atMs": 60_001, "action": "claimTrial" }),
                json!({ "atMs": 60_002, "action": "claimTrial" }),
                step(60_003, "neutral:overloaded"),
                json!({ "atMs": 60_004, "action": "claimTrial" }),
            ]})),
            ("rtt ewma", json!({ "policy": {}, "steps": [
                json!({ "atMs": 0, "kind": "handshake:ws", "outcome": "success", "rttMs": 100 }),
                json!({ "atMs": 1_000, "kind": "heartbeat", "outcome": "success", "rttMs": 200 }),
                json!({ "atMs": 2_000, "kind": "http", "outcome": "success", "rttMs": 50 }),
                json!({ "atMs": 1_802_001, "kind": "heartbeat", "outcome": "success", "rttMs": 70 }),
            ]})),
            ("udp blocked after three quic failures with ws success", json!({ "policy": { "udpBlockedTtlHours": 24 }, "steps": [
                json!({ "atMs": 0, "net": "home", "kind": "handshake:quic", "outcome": "failure" }),
                json!({ "atMs": 1, "net": "home", "kind": "handshake:quic", "outcome": "failure", "ap": "ap-2" }),
                json!({ "atMs": 2, "net": "home", "kind": "handshake:ws", "outcome": "success", "rttMs": 80, "ap": "ap-2" }),
                json!({ "atMs": 3, "net": "home", "kind": "handshake:quic", "outcome": "failure", "ap": "ap-3", "draw": 0.5 }),
                json!({ "atMs": 4, "net": "office", "kind": "handshake:ws", "outcome": "success", "rttMs": 30 }),
                json!({ "atMs": 86_400_003, "net": "home", "kind": "heartbeat", "outcome": "success", "rttMs": 70 }),
            ]})),
            ("quic success resets udp streak", json!({ "policy": {}, "steps": [
                json!({ "atMs": 0, "kind": "handshake:quic", "outcome": "failure" }),
                json!({ "atMs": 1, "kind": "handshake:quic", "outcome": "failure" }),
                json!({ "atMs": 2, "kind": "handshake:quic", "outcome": "success", "rttMs": 40 }),
                json!({ "atMs": 3, "kind": "handshake:quic", "outcome": "failure", "ap": "ap-2" }),
                json!({ "atMs": 4, "kind": "handshake:ws", "outcome": "success", "rttMs": 60, "ap": "ap-2" }),
            ]})),
        ]),
    })
}

#[test]
fn breaker_transition_vectors() {
    ensure_current("breaker_transitions.json", &breaker_vectors());
    replay("breaker_transitions.json", eval_health);
}

// ---------------------------------------------------------------------------
// 恢复切回
// ---------------------------------------------------------------------------

fn eval_failback(input: &Value) -> Value {
    let policy = ClientPolicy::resolve(&input["policy"]).failback;
    let decision = evaluate_failback(
        &FailbackInput {
            probe_successes: input["probeSuccesses"].as_u64().unwrap() as u32,
            probe_span_ms: input["probeSpanMs"].as_i64().unwrap(),
            preferred_score: input["preferredScore"].as_f64().unwrap(),
            current_score: input["currentScore"].as_f64().unwrap(),
            foregrounded: input["foregrounded"].as_bool().unwrap(),
            idle_ms: input["idleMs"].as_i64().unwrap(),
            since_last_switch_ms: input["sinceLastSwitchMs"].as_i64().unwrap(),
        },
        &policy,
    );
    match decision {
        FailbackDecision::Switch => json!({ "decision": "switch" }),
        FailbackDecision::Stay(hold) => json!({ "decision": "stay", "hold": hold.as_str() }),
    }
}

fn failback_vectors() -> Value {
    let base = json!({
        "policy": {}, "probeSuccesses": 3, "probeSpanMs": 300_000, "preferredScore": 80.0,
        "currentScore": 100.0, "foregrounded": false, "idleMs": 60_000, "sinceLastSwitchMs": 600_000
    });
    let vary = |patch: Value| {
        let mut input = base.clone();
        for (key, value) in patch.as_object().unwrap() {
            input[key] = value.clone();
        }
        input
    };
    json!({
        "description": "Failback to the preferred access point. Checks in order: probeSuccesses >= 3, probeSpanMs >= minHealthySpanSec, preferredScore <= currentScore x (1 - minImprovementRatio), sinceLastSwitchMs >= minIntervalSec, and a natural boundary (foregrounded, or idleMs >= 60000). The first failing check is reported as `hold`.",
        "cases": cases(eval_failback, vec![
            ("all conditions met at the boundaries", base.clone()),
            ("two probes", vary(json!({ "probeSuccesses": 2 }))),
            ("span too short", vary(json!({ "probeSpanMs": 299_999 }))),
            ("improvement just short", vary(json!({ "preferredScore": 80.1 }))),
            ("switched recently", vary(json!({ "sinceLastSwitchMs": 599_999 }))),
            ("busy in foreground session", vary(json!({ "idleMs": 59_999 }))),
            ("foreground transition counts as boundary", vary(json!({ "idleMs": 0, "foregrounded": true }))),
            ("busy in background session", vary(json!({ "idleMs": 1_000, "foregrounded": false }))),
            ("a foreground boundary does not cut the minimum interval", vary(json!({ "idleMs": 0, "foregrounded": true, "sinceLastSwitchMs": 599_999 }))),
            ("not better at all", vary(json!({ "preferredScore": 100.0 }))),
            ("custom policy", vary(json!({ "policy": { "failback": { "minHealthySpanSec": 60, "minImprovementRatio": 0.5, "minIntervalSec": 60 } }, "probeSpanMs": 60_000, "preferredScore": 50.0, "sinceLastSwitchMs": 60_000 }))),
            ("current unavailable scores infinite", vary(json!({ "currentScore": 1e300 }))),
        ]),
    })
}

#[test]
fn failback_vectors_are_current() {
    ensure_current("failback.json", &failback_vectors());
    replay("failback.json", eval_failback);
}

// ---------------------------------------------------------------------------
// 清单验签
// ---------------------------------------------------------------------------

const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
const NOW_MS: i64 = 1_793_779_200_000; // 2026-11-04T08:00:00Z
const DAY_MS: i64 = 86_400_000;

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

struct Keys {
    root_a: SigningKey,
    root_b: SigningKey,
    root_x: SigningKey,
    online: SigningKey,
    online_2: SigningKey,
}

fn keys() -> Keys {
    Keys {
        root_a: key(0x11),
        root_b: key(0x22),
        root_x: key(0x55),
        online: key(0x33),
        online_2: key(0x44),
    }
}

fn manifest_payload(serial: u64) -> Value {
    json!({
        "schema": "flare.access.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "appId": "app_01J9ZK3X",
        "serial": serial,
        "issuedAt": "2026-11-04T08:00:00Z",
        "expiresAt": "2026-12-04T08:00:00Z",
        "regions": [{
            "id": "cn-east",
            "displayName": { "zh-CN": "华东", "en": "East China" },
            "cells": [{
                "id": "cell-cne-1",
                "accessPoints": [{
                    "id": "ap-cne1-ct",
                    "host": "im-ct.example.com",
                    "ipHints": ["203.0.113.10"],
                    "weight": 100,
                    "priority": 0,
                    "status": "active",
                    "tags": ["isp:ct"],
                    "services": {
                        "imWs": { "path": "/ws" },
                        "imQuic": { "port": 443 },
                        "coreApi": { "path": "/api" },
                        "socialApi": { "path": "/social-api" },
                        "media": { "path": "/flare-media" },
                        "ping": { "path": "/.well-known/flare/ping" }
                    }
                }]
            }]
        }],
        "placement": { "homeCell": "cell-cne-1", "allowedRegions": ["cn-east"] },
        "bootstrap": { "sources": [
            { "kind": "https", "url": "https://cfg-a.example-cdn.com/flare/v1/b7f3c9a1e2d4b6a8/" },
            { "kind": "https", "url": "https://cfg-b.other-cloud.net/flare/v1/b7f3c9a1e2d4b6a8/" }
        ], "refreshIntervalSec": 21600 },
        "clientPolicy": { "raceStaggerMs": 300 }
    })
}

fn cert(key_id: &str, online: &SigningKey, root_id: &str) -> KeyCert {
    KeyCert {
        schema: "flare.keycert.v1".into(),
        key_id: key_id.into(),
        public_key: encode_public_key(&online.verifying_key()),
        usages: vec!["access".into(), "release".into()],
        scope: KeyScope {
            bootstrap_ids: vec!["*".into()],
        },
        not_before: "2026-07-01T00:00:00Z".into(),
        not_after: "2026-12-28T00:00:00Z".into(),
        root_key_id: root_id.into(),
    }
}

fn issue(root_id: &str, root: &SigningKey, cert: &KeyCert) -> String {
    issue_key_cert(
        &Signer {
            key_id: root_id,
            key: root,
        },
        cert,
    )
}

fn sign_payload(
    payload: &[u8],
    payload_type: &str,
    signers: &[(&str, &SigningKey)],
    certs: &[String],
) -> String {
    let signers: Vec<Signer<'_>> = signers
        .iter()
        .map(|(key_id, key)| Signer { key_id, key })
        .collect();
    String::from_utf8(sign_envelope(payload_type, payload, &signers, certs).to_json_bytes())
        .unwrap()
}

const ACCESS_TYPE: &str = "application/vnd.flare.access.v1+json";

fn signed(payload: &Value, keys: &Keys, certs: &[String]) -> String {
    sign_payload(
        &serde_json::to_vec(payload).unwrap(),
        ACCESS_TYPE,
        &[("ok-2026h2-01", &keys.online)],
        certs,
    )
}

fn tamper_payload(envelope: &str) -> String {
    let mut parsed: Envelope = serde_json::from_str(envelope).unwrap();
    let mut payload = URL_SAFE_NO_PAD.decode(&parsed.payload).unwrap();
    let position = payload
        .windows(5)
        .position(|window| window == b"im-ct")
        .unwrap();
    payload[position] = b'x';
    parsed.payload = URL_SAFE_NO_PAD.encode(&payload);
    String::from_utf8(parsed.to_json_bytes()).unwrap()
}

fn tamper_cert(encoded: &str) -> String {
    let bytes = URL_SAFE_NO_PAD.decode(encoded).unwrap();
    let mut parsed: Envelope = serde_json::from_slice(&bytes).unwrap();
    let mut payload: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&parsed.payload).unwrap()).unwrap();
    payload["notAfter"] = "2030-01-01T00:00:00Z".into();
    parsed.payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
    URL_SAFE_NO_PAD.encode(parsed.to_json_bytes())
}

fn keystate(serial: u64, revoked: &[&str], certificates: Vec<String>) -> KeyState {
    KeyState {
        schema: "flare.keystate.v1".into(),
        serial,
        issued_at: "2026-11-01T00:00:00Z".into(),
        certificates,
        revoked_key_ids: revoked.iter().map(|id| id.to_string()).collect(),
    }
}

fn trust_roots_json(keys: &Keys) -> Value {
    json!([
        { "keyId": "root-a", "publicKey": encode_public_key(&keys.root_a.verifying_key()) },
        { "keyId": "root-b", "publicKey": encode_public_key(&keys.root_b.verifying_key()) },
    ])
}

fn eval_verify(input: &Value) -> Value {
    let roots: Vec<flare_access::trust::TrustRootConfig> =
        serde_json::from_value(input["trustRoots"].clone()).unwrap();
    let mut trust = TrustStore::new(
        TrustRoots::from_config(&roots).unwrap(),
        input["bootstrapId"].as_str().unwrap(),
    )
    .unwrap();
    let now = input["nowMs"].as_i64().unwrap();
    if let Some(keystate) = input["keystate"].as_str() {
        let verified = trust.verify_keystate(keystate.as_bytes(), now).unwrap();
        trust = trust.with_keystate(&verified);
    }
    let bytes: Vec<u8> = match &input["envelope"] {
        Value::String(text) => text.as_bytes().to_vec(),
        other => vec![b' '; other["oversizeBytes"].as_u64().unwrap() as usize],
    };
    let max_serial = input["maxSerial"].as_u64();
    let error = |error: flare_access::trust::TrustError| json!({ "error": { "code": error.code(), "kind": error.kind().as_str() } });
    let serial_check = |serial: u64| match check_serial(serial, max_serial) {
        Ok(SerialCheck::Newer) => Ok("newer"),
        Ok(SerialCheck::Same) => Ok("same"),
        Err(rollback) => Err(rollback),
    };
    match input["docType"].as_str().unwrap_or("access") {
        "keystate" => match trust.verify_keystate(&bytes, now) {
            Ok(verified) => match serial_check(verified.serial) {
                Ok(check) => json!({ "ok": {
                    "serial": verified.serial,
                    "signedBy": verified.signed_by,
                    "revoked": verified.revoked.iter().collect::<Vec<_>>(),
                    "serialCheck": check,
                }}),
                Err(rollback) => error(rollback),
            },
            Err(failure) => error(failure),
        },
        _ => match trust.verify_access(&bytes, now) {
            Ok(verified) => match serial_check(verified.serial) {
                Ok(check) => json!({ "ok": {
                    "serial": verified.serial,
                    "keyId": verified.key_id,
                    "stale": verified.is_stale(now),
                    "serialCheck": check,
                }}),
                Err(rollback) => error(rollback),
            },
            Err(failure) => error(failure),
        },
    }
}

fn verify_vectors() -> Value {
    let k = keys();
    let good_cert = issue(
        "root-a",
        &k.root_a,
        &cert("ok-2026h2-01", &k.online, "root-a"),
    );
    let good = signed(
        &manifest_payload(1042),
        &k,
        std::slice::from_ref(&good_cert),
    );
    let roots = trust_roots_json(&k);
    let base = |envelope: Value| json!({ "docType": "access", "trustRoots": roots, "bootstrapId": BOOTSTRAP_ID, "nowMs": NOW_MS, "envelope": envelope });
    let vary = |envelope: Value, patch: Value| {
        let mut input = base(envelope);
        for (key, value) in patch.as_object().unwrap() {
            input[key] = value.clone();
        }
        input
    };
    let with_cert = |cert: KeyCert, root_id: &str, root: &SigningKey| {
        signed(&manifest_payload(1042), &k, &[issue(root_id, root, &cert)])
    };
    let mut usage_cert = cert("ok-2026h2-01", &k.online, "root-a");
    usage_cert.usages = vec!["release".into()];
    let mut scoped_out = cert("ok-2026h2-01", &k.online, "root-a");
    scoped_out.scope.bootstrap_ids = vec!["0000000000000000".into()];
    let mut scoped_in = cert("ok-2026h2-01", &k.online, "root-a");
    scoped_in.scope.bootstrap_ids = vec![BOOTSTRAP_ID.into()];
    // 由 root-a 签发却声称 rootKeyId = root-b。
    let liar_cert = cert("ok-2026h2-01", &k.online, "root-b");
    let revoked_keystate = String::from_utf8(sign_keystate(
        &keystate(3, &["ok-2026h2-01"], vec![]),
        &Signer {
            key_id: "root-a",
            key: &k.root_a,
        },
    ))
    .unwrap();
    let cert_supplying_keystate = String::from_utf8(sign_keystate(
        &keystate(
            4,
            &[],
            vec![issue(
                "root-b",
                &k.root_b,
                &cert("ok-2026h2-02", &k.online_2, "root-b"),
            )],
        ),
        &Signer {
            key_id: "root-b",
            key: &k.root_b,
        },
    ))
    .unwrap();
    let rotated = sign_payload(
        &serde_json::to_vec(&manifest_payload(1043)).unwrap(),
        ACCESS_TYPE,
        &[("ok-2026h2-02", &k.online_2)],
        &[],
    );
    let two_signatures = sign_payload(
        &serde_json::to_vec(&manifest_payload(1042)).unwrap(),
        ACCESS_TYPE,
        &[("ok-unknown", &k.online_2), ("ok-2026h2-01", &k.online)],
        std::slice::from_ref(&good_cert),
    );
    let mut wrong_bootstrap = manifest_payload(1042);
    wrong_bootstrap["bootstrapId"] = "c0ffee0000000000".into();
    let mut v2 = manifest_payload(1042);
    v2["schema"] = "flare.access.v2".into();
    let mut ip_host = manifest_payload(1042);
    ip_host["regions"][0]["cells"][0]["accessPoints"][0]["host"] = "203.0.113.10".into();

    let keystate_ok = String::from_utf8(sign_keystate(
        &keystate(7, &["ok-2026h1-01"], vec![]),
        &Signer {
            key_id: "root-a",
            key: &k.root_a,
        },
    ))
    .unwrap();
    let keystate_by_online = String::from_utf8(sign_keystate(
        &keystate(7, &[], vec![]),
        &Signer {
            key_id: "ok-2026h2-01",
            key: &k.online,
        },
    ))
    .unwrap();

    json!({
        "description": "Access manifest and keystate verification. Steps: size <= 65536 bytes; DSSE envelope; payloadType; for access documents every keyCerts entry (plus certificates from the installed keystate) is checked against the built-in roots (root signature, rootKeyId, notBefore <= now + 24h, notAfter >= now - 24h, usage, scope, not revoked) and at least one signature must verify with a valid certificate; keystate documents must be signed directly by a root. Then payload schema, structure and bootstrapId. Expired access manifests are accepted with stale = true. Finally serial is compared with maxSerial (absent = never accepted). On failure the first signature's failure is reported. Signing keys are test-only Ed25519 keys derived from fixed 32-byte seeds.",
        "testKeys": {
            "root-a": "seed 0x11 x 32", "root-b": "seed 0x22 x 32", "root-x (not trusted)": "seed 0x55 x 32",
            "ok-2026h2-01": "seed 0x33 x 32", "ok-2026h2-02": "seed 0x44 x 32"
        },
        "cases": cases(eval_verify, vec![
            ("valid", base(good.clone().into())),
            ("valid but expired is stale", vary(good.clone().into(), json!({ "nowMs": NOW_MS + 31 * DAY_MS }))),
            ("certificate within clock skew", vary(good.clone().into(), json!({ "nowMs": 1_798_416_000_000i64 + 23 * 3_600_000 }))),
            ("same serial", vary(good.clone().into(), json!({ "maxSerial": 1042 }))),
            ("rollback", vary(good.clone().into(), json!({ "maxSerial": 1043 }))),
            ("tampered payload", base(tamper_payload(&good).into())),
            ("tampered certificate", base(signed(&manifest_payload(1042), &k, &[tamper_cert(&good_cert)]).into())),
            ("certificate from untrusted root", base(with_cert(cert("ok-2026h2-01", &k.online, "root-x"), "root-x", &k.root_x).into())),
            ("certificate rootKeyId mismatch", base(with_cert(liar_cert, "root-a", &k.root_a).into())),
            ("certificate expired", vary(good.clone().into(), json!({ "nowMs": 1_798_416_000_000i64 + DAY_MS + 1 }))),
            ("certificate not yet valid", vary(good.clone().into(), json!({ "nowMs": 1_782_864_000_000i64 - DAY_MS - 1 }))),
            ("certificate usage excludes access", base(with_cert(usage_cert, "root-a", &k.root_a).into())),
            ("certificate scope excludes app", base(with_cert(scoped_out, "root-a", &k.root_a).into())),
            ("certificate scope names app", base(with_cert(scoped_in, "root-a", &k.root_a).into())),
            ("signing key revoked", vary(good.clone().into(), json!({ "keystate": revoked_keystate }))),
            ("no certificate for key", base(signed(&manifest_payload(1042), &k, &[]).into())),
            ("root must not sign manifests directly", base(sign_payload(&serde_json::to_vec(&manifest_payload(1042)).unwrap(), ACCESS_TYPE, &[("root-a", &k.root_a)], &[]).into())),
            ("second signature verifies", base(two_signatures.into())),
            ("certificate supplied by keystate (root-b)", vary(rotated.into(), json!({ "keystate": cert_supplying_keystate }))),
            ("wrong payloadType", base(sign_payload(&serde_json::to_vec(&manifest_payload(1042)).unwrap(), "application/vnd.flare.release.v1+json", &[("ok-2026h2-01", &k.online)], std::slice::from_ref(&good_cert)).into())),
            ("wrong bootstrapId", base(signed(&wrong_bootstrap, &k, std::slice::from_ref(&good_cert)).into())),
            ("unsupported schema", base(signed(&v2, &k, std::slice::from_ref(&good_cert)).into())),
            ("structurally invalid payload", base(signed(&ip_host, &k, std::slice::from_ref(&good_cert)).into())),
            ("oversize", base(json!({ "oversizeBytes": 65_537 }))),
            ("not an envelope", base("{\"hello\":1}".into())),
            ("keystate valid", vary(keystate_ok.clone().into(), json!({ "docType": "keystate" }))),
            ("keystate rollback", vary(keystate_ok.clone().into(), json!({ "docType": "keystate", "maxSerial": 8 }))),
            ("keystate signed by online key", vary(keystate_by_online.into(), json!({ "docType": "keystate" }))),
            ("keystate tampered", vary(tamper_keystate(&keystate_ok).into(), json!({ "docType": "keystate" }))),
            ("keystate with access payloadType", vary(good.into(), json!({ "docType": "keystate" }))),
        ]),
    })
}

fn tamper_keystate(envelope: &str) -> String {
    let mut parsed: Envelope = serde_json::from_str(envelope).unwrap();
    let mut payload: Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&parsed.payload).unwrap()).unwrap();
    payload["revokedKeyIds"] = json!([]);
    parsed.payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
    String::from_utf8(parsed.to_json_bytes()).unwrap()
}

#[test]
fn manifest_verify_vectors() {
    ensure_current("manifest_verify.json", &verify_vectors());
    replay("manifest_verify.json", eval_verify);
}

// ---------------------------------------------------------------------------
// 灰度分桶（六端共用）
// ---------------------------------------------------------------------------

fn eval_bucket(input: &Value) -> Value {
    let salt = input["salt"].as_str().unwrap();
    match input["count"].as_u64() {
        // 分布：`count` 个连续 ID 里落在 `basisPoints` 以内的个数。
        Some(count) => {
            let prefix = input["prefix"].as_str().unwrap();
            let basis_points = input["basisPoints"].as_u64().unwrap() as u32;
            let hits = (0..count)
                .filter(|index| bucket(&format!("{prefix}{index}"), salt) < basis_points)
                .count();
            json!({ "hits": hits })
        }
        None => json!({ "bucket": bucket(input["installId"].as_str().unwrap(), salt) }),
    }
}

fn bucket_vectors() -> Value {
    let id = |install_id: &str, salt: &str| json!({ "installId": install_id, "salt": salt });
    json!({
        "description": "Rollout bucket: u32 big-endian of the first 4 bytes of SHA-256(installId ':' salt), modulo 10000. The strings are hashed as UTF-8. A release is offered when bucket < rollout.basisPoints. A case with count asks how many of the ids prefix0 .. prefix(count-1) fall below basisPoints.",
        "cases": cases(eval_bucket, vec![
            ("uuid id, design salt", id("8f14e45f-ceea-467f-a8d2-6f1c2c1e7a11", "rel_01JC8W")),
            ("same id, next release", id("8f14e45f-ceea-467f-a8d2-6f1c2c1e7a11", "rel_01JB2Q")),
            ("another id", id("00000000-0000-4000-8000-000000000000", "rel_01JC8W")),
            ("all f id", id("ffffffff-ffff-4fff-bfff-ffffffffffff", "rel_01JC8W")),
            ("short id", id("a", "b")),
            ("non-ascii salt", id("8f14e45f-ceea-467f-a8d2-6f1c2c1e7a11", "灰度-2.4.0")),
            ("salt with colon", id("a:b", "c")),
            ("id with colon moved", id("a", "b:c")),
            ("one percent share", json!({ "prefix": "install-", "count": 10000, "salt": "rel_01JC8W", "basisPoints": 100 })),
            ("five percent share", json!({ "prefix": "install-", "count": 10000, "salt": "rel_01JC8W", "basisPoints": 500 })),
            ("half share", json!({ "prefix": "install-", "count": 10000, "salt": "rel_01JC8W", "basisPoints": 5000 })),
        ]),
    })
}

#[test]
fn release_bucket_vectors() {
    ensure_current("release_bucket.json", &bucket_vectors());
    replay("release_bucket.json", eval_bucket);
}

// ---------------------------------------------------------------------------
// 发布清单评估
// ---------------------------------------------------------------------------

const INSTALL_ID: &str = "8f14e45f-ceea-467f-a8d2-6f1c2c1e7a11";
const RELEASE_DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// `bucket(INSTALL_ID, "rel_new")`：灰度比例取它是没命中，加一是刚好命中。
const NEW_BUCKET: u32 = 1507;

fn eval_release(input: &Value) -> Value {
    let manifest: ReleaseManifest = serde_json::from_value(input["manifest"].clone()).unwrap();
    let server = input.get("server").map(|server| ServerGate {
        min_build: server["minBuild"].as_u64(),
    });
    let decision = evaluate(
        &manifest,
        &EvaluationInput {
            current_build: input["currentBuild"].as_u64(),
            current_build_id: input["currentBuildId"].as_str(),
            distribution: input["distribution"].as_str().unwrap(),
            target: input["target"].as_str(),
            install_id: input["installId"].as_str().unwrap(),
            stale: input["stale"].as_bool().unwrap_or(false),
            server,
        },
    );
    match decision {
        Decision::UpToDate => json!({ "decision": "upToDate" }),
        Decision::Available {
            release,
            recommended,
        } => json!({
            "decision": "available",
            "releaseId": release.release_id,
            "recommended": recommended,
        }),
        Decision::Required { release, block_ui } => json!({
            "decision": "required",
            "releaseId": release.map(|release| release.release_id.clone()),
            "blockUi": block_ui,
        }),
    }
}

fn rollout(id: &str, basis_points: u32, halted: bool) -> Value {
    json!({ "basisPoints": basis_points, "salt": id, "halted": halted })
}

fn entry(id: &str, build: u64, basis_points: u32) -> Value {
    json!({
        "releaseId": id,
        "version": format!("2.{build}"),
        "build": build,
        "rollout": rollout(id, basis_points, false),
        "artifacts": {
            "play": { "kind": "store", "packageName": "com.acme.chat" },
            "direct": {
                "kind": "apk",
                "urls": ["https://dl.example.com/a.apk"],
                "sizeBytes": 1024,
                "sha256": RELEASE_DIGEST,
                "signingCertSha256": RELEASE_DIGEST
            },
            "tauri": {
                "kind": "tauri",
                "targets": { "darwin-aarch64": {
                    "urls": ["https://dl.example.com/a.tar.gz"],
                    "sizeBytes": 1024,
                    "sha256": RELEASE_DIGEST
                } }
            }
        }
    })
}

fn web_entry(id: &str, build: u64, build_id: &str, basis_points: u32) -> Value {
    json!({
        "releaseId": id,
        "version": format!("2.{build}"),
        "build": build,
        "rollout": rollout(id, basis_points, false),
        "artifacts": { "web": {
            "kind": "web_bundle",
            "buildId": build_id,
            "urls": ["https://app.example.com/assets.json"],
            "sha256": RELEASE_DIGEST
        } }
    })
}

fn release_case(policy: Value, releases: Vec<Value>, facts: Value) -> Value {
    let mut input = json!({
        "manifest": {
            "schema": "flare.release.v1",
            "bootstrapId": "b7f3c9a1e2d4b6a8",
            "platform": "android",
            "channel": "stable",
            "serial": 88,
            "issuedAt": "2026-11-04T08:00:00Z",
            "expiresAt": "2026-11-18T08:00:00Z",
            "policy": policy,
            "releases": releases
        },
        "distribution": "play",
        "installId": INSTALL_ID
    });
    for (key, value) in facts.as_object().unwrap() {
        input[key] = value.clone();
    }
    input
}

fn evaluate_vectors() -> Value {
    let hit = NEW_BUCKET + 1;
    let miss = NEW_BUCKET;
    let new_and_full = |basis_points| {
        vec![
            entry("rel_new", 20400, basis_points),
            entry("rel_full", 20302, 10_000),
        ]
    };
    let halted = {
        let mut release = entry("rel_new", 20400, 10_000);
        release["rollout"]["halted"] = true.into();
        release
    };
    let unknown_kind = {
        let mut release = entry("rel_new", 20400, 10_000);
        release["artifacts"] = json!({ "play": { "kind": "hologram" } });
        release
    };
    json!({
        "description": "Release evaluation (flare.release.v1). Native compares build only; the web compares buildId and takes the first release in manifest order that hits the rollout. Required comes from blockedBuilds, minBuild or a server rejection; an expired manifest (stale) only ever yields available, a server rejection still forces. A forced update prefers a rollout hit but never leaves the user without a target. releaseId is null when no usable release exists.",
        "cases": cases(eval_release, vec![
            ("newest release hits the rollout", release_case(json!({}), new_and_full(hit), json!({ "currentBuild": 20000 }))),
            ("outside the rollout falls back to the full release", release_case(json!({}), new_and_full(miss), json!({ "currentBuild": 20000 }))),
            ("already on the newest build", release_case(json!({}), new_and_full(hit), json!({ "currentBuild": 20400 }))),
            ("halted release is skipped", release_case(json!({}), vec![halted, entry("rel_full", 20302, 10_000)], json!({ "currentBuild": 20000 }))),
            ("no artifact for this distribution", release_case(json!({}), new_and_full(10_000), json!({ "currentBuild": 20000, "distribution": "appgallery" }))),
            ("unknown artifact kind counts as none", release_case(json!({}), vec![unknown_kind], json!({ "currentBuild": 20000 }))),
            ("recommended below the threshold", release_case(json!({ "recommendBelowBuild": 20300 }), new_and_full(hit), json!({ "currentBuild": 20200 }))),
            ("not recommended at the threshold", release_case(json!({ "recommendBelowBuild": 20300 }), new_and_full(hit), json!({ "currentBuild": 20300 }))),
            ("below minBuild", release_case(json!({ "minBuild": 20100 }), new_and_full(miss), json!({ "currentBuild": 20000 }))),
            ("at minBuild", release_case(json!({ "minBuild": 20100 }), new_and_full(miss), json!({ "currentBuild": 20100 }))),
            ("inside a blocked range", release_case(json!({ "blockedBuilds": [{ "from": 20210, "to": 20212 }] }), new_and_full(hit), json!({ "currentBuild": 20211 }))),
            ("just past a blocked range", release_case(json!({ "blockedBuilds": [{ "from": 20210, "to": 20212 }] }), new_and_full(miss), json!({ "currentBuild": 20213 }))),
            ("blockUi travels with required", release_case(json!({ "minBuild": 20100, "blockUi": true }), new_and_full(hit), json!({ "currentBuild": 20000 }))),
            ("forced target skips a blocked release", release_case(json!({ "minBuild": 20100, "blockedBuilds": [{ "from": 20302, "to": 20302 }] }), new_and_full(miss), json!({ "currentBuild": 20000 }))),
            ("forced update ignores a rollout nobody hits", release_case(json!({ "minBuild": 20100 }), vec![entry("rel_new", 20400, 0)], json!({ "currentBuild": 20000 }))),
            ("forced with no usable release", release_case(json!({ "minBuild": 20100 }), new_and_full(10_000), json!({ "currentBuild": 20000, "distribution": "appgallery" }))),
            ("expired manifest only offers", release_case(json!({ "minBuild": 20100 }), new_and_full(hit), json!({ "currentBuild": 20000, "stale": true }))),
            ("expired manifest on the newest build", release_case(json!({ "minBuild": 30000 }), new_and_full(hit), json!({ "currentBuild": 20400, "stale": true }))),
            ("server rejection beats the expiry and the floor", release_case(json!({}), new_and_full(miss), json!({ "currentBuild": 20000, "stale": true, "server": { "minBuild": 20400 } }))),
            ("server rejection without a floor", release_case(json!({}), vec![entry("rel_new", 20400, hit)], json!({ "currentBuild": 20302, "server": {} }))),
            ("server rejection with nothing newer", release_case(json!({}), vec![entry("rel_full", 20302, 10_000)], json!({ "currentBuild": 20302, "server": {} }))),
            ("desktop target listed", release_case(json!({}), new_and_full(hit), json!({ "currentBuild": 20000, "distribution": "tauri", "target": "darwin-aarch64" }))),
            ("desktop target missing", release_case(json!({}), new_and_full(hit), json!({ "currentBuild": 20000, "distribution": "tauri", "target": "windows-x86_64" }))),
            ("web new build id", release_case(json!({}), vec![web_entry("rel_new", 20, "bundle-2", hit)], json!({ "currentBuildId": "bundle-1", "distribution": "web" }))),
            ("web same build id", release_case(json!({}), vec![web_entry("rel_new", 20, "bundle-2", hit)], json!({ "currentBuildId": "bundle-2", "distribution": "web" }))),
            ("web rollback points at an older build", release_case(json!({}), vec![web_entry("rel_back", 10, "bundle-old", 10_000), web_entry("rel_bad", 20, "bundle-bad", 10_000)], json!({ "currentBuildId": "bundle-bad", "distribution": "web" }))),
            ("web outside the rollout stays on its build", release_case(json!({}), vec![web_entry("rel_new", 20, "bundle-2", miss), web_entry("rel_full", 10, "bundle-1", 10_000)], json!({ "currentBuildId": "bundle-1", "distribution": "web" }))),
            ("web forced by build", release_case(json!({ "minBuild": 15 }), vec![web_entry("rel_new", 20, "bundle-2", hit)], json!({ "currentBuildId": "bundle-1", "currentBuild": 10, "distribution": "web" }))),
            ("web forced past the rollout", release_case(json!({}), vec![web_entry("rel_full", 10, "bundle-1", 10_000), web_entry("rel_new", 20, "bundle-2", 0)], json!({ "currentBuildId": "bundle-1", "distribution": "web", "server": {} }))),
        ]),
    })
}

#[test]
fn release_evaluate_vectors() {
    ensure_current("release_evaluate.json", &evaluate_vectors());
    replay("release_evaluate.json", eval_release);
}

// ---------------------------------------------------------------------------
// 更新提示频率
// ---------------------------------------------------------------------------

fn eval_prompt(input: &Value) -> Value {
    let urgency = match input["urgency"].as_str().unwrap() {
        "optional" => Urgency::Optional,
        "recommended" => Urgency::Recommended,
        "required" => Urgency::Required { block_ui: false },
        "blocking" => Urgency::Required { block_ui: true },
        other => panic!("unknown urgency {other}"),
    };
    let history: PromptHistory = serde_json::from_value(input["history"].clone()).unwrap();
    let session = PromptSession {
        shown_builds: input["session"]["shownBuilds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|build| build.as_u64().unwrap())
            .collect(),
        required_shown: input["session"]["requiredShown"].as_bool().unwrap(),
    };
    let prompt: UpdatePrompt = decide_prompt(
        urgency,
        input["build"].as_u64(),
        input["nowMs"].as_i64().unwrap(),
        &history,
        &session,
    );
    json!({ "prompt": prompt.as_str() })
}

const PROMPT_NOW_MS: i64 = 1_793_779_200_000;
const PROMPT_DAY_MS: i64 = 86_400_000;

fn prompt_case(urgency: &str, history: Value, session: Value) -> Value {
    let mut session_value = json!({ "shownBuilds": [], "requiredShown": false });
    for (key, value) in session.as_object().unwrap() {
        session_value[key] = value.clone();
    }
    json!({
        "urgency": urgency,
        "build": 20400,
        "nowMs": PROMPT_NOW_MS,
        "history": history,
        "session": session_value
    })
}

fn prompt_vectors() -> Value {
    let day_ago = PROMPT_NOW_MS - PROMPT_DAY_MS;
    json!({
        "description": "Update prompt frequency. optional: badge, one sheet per version. recommended: modal at most once in 24 hours, deferring silences it for 24 hours. required: modal at each launch and foreground (requiredShown resets), then badge; blocking stays fullscreen. Within one session a version never pops twice. A skipped version stays quiet. build is null when a forced update has no usable release.",
        "cases": cases(eval_prompt, vec![
            ("optional, first time", prompt_case("optional", json!({}), json!({}))),
            ("optional, sheet already shown", prompt_case("optional", json!({ "sheetShownBuilds": [20400] }), json!({}))),
            ("optional, sheet shown for an older version", prompt_case("optional", json!({ "sheetShownBuilds": [20302] }), json!({}))),
            ("optional, shown this session", prompt_case("optional", json!({}), json!({ "shownBuilds": [20400] }))),
            ("optional, skipped", prompt_case("optional", json!({ "skippedBuilds": [20400] }), json!({}))),
            ("recommended, first time", prompt_case("recommended", json!({}), json!({}))),
            ("recommended, shown an hour ago", prompt_case("recommended", json!({ "lastModalAtMs": PROMPT_NOW_MS - 3_600_000 }), json!({}))),
            ("recommended, shown just under a day ago", prompt_case("recommended", json!({ "lastModalAtMs": day_ago + 1 }), json!({}))),
            ("recommended, shown a day ago", prompt_case("recommended", json!({ "lastModalAtMs": day_ago }), json!({}))),
            ("recommended, deferred", prompt_case("recommended", json!({ "deferredUntilMs": PROMPT_NOW_MS + 1 }), json!({}))),
            ("recommended, deferral over", prompt_case("recommended", json!({ "deferredUntilMs": PROMPT_NOW_MS }), json!({}))),
            ("recommended, shown this session", prompt_case("recommended", json!({}), json!({ "shownBuilds": [20400] }))),
            ("recommended, skipped", prompt_case("recommended", json!({ "skippedBuilds": [20400] }), json!({}))),
            ("required, launch", prompt_case("required", json!({}), json!({}))),
            ("required, shown since the last foreground", prompt_case("required", json!({}), json!({ "requiredShown": true }))),
            ("required ignores deferral and skip", prompt_case("required", json!({ "deferredUntilMs": PROMPT_NOW_MS + 1, "skippedBuilds": [20400] }), json!({}))),
            ("required without a release", {
                let mut input = prompt_case("required", json!({}), json!({}));
                input["build"] = Value::Null;
                input
            }),
            ("blocking, launch", prompt_case("blocking", json!({}), json!({}))),
            ("blocking, already shown", prompt_case("blocking", json!({}), json!({ "requiredShown": true }))),
        ]),
    })
}

#[test]
fn release_prompt_vectors() {
    ensure_current("release_prompt.json", &prompt_vectors());
    replay("release_prompt.json", eval_prompt);
}

// ---------------------------------------------------------------------------
// 更新流程中的提示
// ---------------------------------------------------------------------------

fn flow_machine(input: &Value) -> UpdateMachine {
    let text = |key: &str| input[key].as_str().map(str::to_owned);
    let mut machine = UpdateMachine::new(UpdateApp {
        platform: "android".into(),
        channel: "stable".into(),
        version: Some("2.3.2".into()),
        build: input["currentBuild"].as_u64(),
        build_id: text("currentBuildId"),
        distribution: text("distribution"),
        target: text("target"),
        locale: None,
    });
    let manifest: ReleaseManifest = serde_json::from_value(input["manifest"].clone()).unwrap();
    let verified = VerifiedReleaseManifest {
        serial: manifest.serial,
        manifest: Arc::new(manifest),
        key_id: "vector".into(),
        issued_at_ms: 0,
        expires_at_ms: i64::MAX,
        envelope: Arc::from(&b""[..]),
        digest: String::new(),
    };
    machine.restore(
        INSTALL_ID.into(),
        PersistedUpdate::default(),
        None,
        PROMPT_NOW_MS,
    );
    machine.finish_check(CheckResult::Manifest(verified), PROMPT_NOW_MS);
    machine
}

fn flow_step(machine: &mut UpdateMachine, step: &str) {
    let target = 20400;
    match step {
        "markSheet" => machine.mark_prompted(Some(target), UpdatePrompt::Sheet, PROMPT_NOW_MS),
        "markModal" => machine.mark_prompted(Some(target), UpdatePrompt::Modal, PROMPT_NOW_MS),
        "download" => {
            machine.begin_download(false, false);
        }
        "downloadOnMetered" => {
            machine.begin_download(true, false);
        }
        "downloadOnMeteredAllowed" => {
            machine.begin_download(true, true);
        }
        "verifying" => machine.begin_verifying(),
        "ready" => machine.download_ready(
            ArtifactRecord {
                path: "/data/2.4.0-20400.apk".into(),
                sha256: RELEASE_DIGEST.into(),
                signing_cert_sha256: None,
                build: target,
                size_bytes: 1024,
            },
            PROMPT_NOW_MS,
        ),
        "downloadFailed" => machine.download_failed("update.downloadFailed", true),
        "defer" => machine.defer(PROMPT_NOW_MS),
        "install" => {
            machine.begin_install();
        }
        "handOver" => machine.package_handed_over(),
        "installCompleted" => machine.report_install(InstallOutcome::Completed, None),
        "installCancelled" => machine.report_install(InstallOutcome::Cancelled, None),
        "installFailed" => machine.report_install(InstallOutcome::Failed, None),
        "foreground" => machine.foregrounded(PROMPT_NOW_MS),
        other => panic!("unknown step {other}"),
    }
}

fn eval_flow(input: &Value) -> Value {
    let mut machine = flow_machine(input);
    let steps: Vec<Value> = input["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| {
            flow_step(&mut machine, step.as_str().unwrap());
            let status = serde_json::to_value(machine.status(PROMPT_NOW_MS)).unwrap();
            json!({
                "state": status["state"],
                "prompt": status["prompt"],
                "error": status["error"]["code"],
                "fallbackUrl": status["error"]["fallbackUrl"],
            })
        })
        .collect();
    json!({ "steps": steps })
}

fn flow_case(policy: Value, artifacts: Value, facts: Value, steps: &[&str]) -> Value {
    let mut release = entry("rel_new", 20400, 10_000);
    if !artifacts.is_null() {
        release["artifacts"] = artifacts;
    }
    let mut input = release_case(policy, vec![release], facts);
    input["steps"] = json!(steps);
    input
}

fn flow_vectors() -> Value {
    let direct = json!({ "distribution": "direct", "currentBuild": 20302 });
    let recommended = json!({ "recommendBelowBuild": 20400 });
    let required = json!({ "minBuild": 20400 });
    let blocking = json!({ "minBuild": 20400, "blockUi": true });
    let with_store_url = json!({ "direct": {
        "kind": "apk",
        "urls": ["https://dl.example.com/a.apk"],
        "storeUrl": "https://www.example.com/download",
        "sizeBytes": 1024,
        "sha256": RELEASE_DIGEST,
        "signingCertSha256": RELEASE_DIGEST
    } });
    let web_bundle = json!({ "web": {
        "kind": "web_bundle",
        "buildId": "bundle-2",
        "urls": ["https://app.example.com/assets.json"],
        "sha256": RELEASE_DIGEST
    } });
    json!({
        "description": "The prompt across an update flow, driven by the engine so a dialog does not vanish when the user taps Update. While downloading, verifying, readyToInstall, installing or failed, and while available or required carry an error, prompt is the form last marked in this session, or the one the release class implies (optional sheet, recommended and required modal, blocking fullscreen). It returns to the frequency rule on a clean stable state, on install completed or cancelled, or on defer. Defer while downloading or verifying only sets prompt to none, the download goes on and nothing is deferred; readyToInstall brings the form back. fallbackUrl: the web has none; a direct package prefers storeUrl, then its first download address. Each step reports the status right after it. One return to the foreground counts once however many times it is reported.",
        "cases": cases(eval_flow, vec![
            ("optional with consent on a metered network, then cancelled", flow_case(json!({}), Value::Null, direct.clone(), &["markSheet", "downloadOnMetered", "downloadOnMeteredAllowed", "verifying", "ready", "install", "handOver", "installCancelled"])),
            ("started from the about page", flow_case(json!({}), Value::Null, direct.clone(), &["download", "verifying", "ready"])),
            ("recommended started from the about page", flow_case(recommended.clone(), Value::Null, direct.clone(), &["download"])),
            ("later while downloading", flow_case(recommended.clone(), Value::Null, direct.clone(), &["markModal", "download", "defer", "verifying", "ready", "install", "handOver", "installCompleted"])),
            ("later while verifying", flow_case(json!({}), Value::Null, direct.clone(), &["markSheet", "download", "verifying", "defer", "ready"])),
            ("later after a failed download", flow_case(recommended, Value::Null, direct.clone(), &["markModal", "download", "downloadFailed", "defer"])),
            ("hidden prompt comes back on failure", flow_case(json!({}), Value::Null, direct.clone(), &["markSheet", "download", "defer", "downloadFailed"])),
            ("required retries after a failure", flow_case(required.clone(), Value::Null, direct.clone(), &["markModal", "download", "downloadFailed", "download", "verifying", "ready"])),
            ("required later continues read only", flow_case(required.clone(), Value::Null, direct.clone(), &["markModal", "defer", "foreground"])),
            ("one return to the foreground reported twice", flow_case(required, Value::Null, direct.clone(), &["foreground", "markModal", "foreground", "defer"])),
            ("blocking stays fullscreen through later", flow_case(blocking, Value::Null, direct.clone(), &["download", "defer", "verifying", "ready", "install", "handOver"])),
            ("store opens and returns", flow_case(json!({}), Value::Null, json!({ "currentBuild": 20302 }), &["markSheet", "install", "installCompleted"])),
            ("store cancelled", flow_case(json!({}), Value::Null, json!({ "currentBuild": 20302 }), &["markSheet", "install", "installCancelled"])),
            ("failed install names the store", flow_case(json!({}), Value::Null, json!({ "currentBuild": 20302 }), &["install", "installFailed"])),
            ("direct falls back to storeUrl first", flow_case(json!({}), with_store_url, direct, &["download", "downloadFailed"])),
            ("web has no fallback", flow_case(json!({}), web_bundle, json!({ "currentBuildId": "bundle-1", "distribution": "web" }), &["install", "installFailed"])),
        ]),
    })
}

#[test]
fn release_flow_vectors() {
    ensure_current("release_flow.json", &flow_vectors());
    replay("release_flow.json", eval_flow);
}

// ---------------------------------------------------------------------------
// 更新说明语言
// ---------------------------------------------------------------------------

fn eval_notes(input: &Value) -> Value {
    let notes = input["languages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|language| {
            let language = language.as_str().unwrap().to_owned();
            (language.clone(), vec![language])
        })
        .collect();
    json!({ "notes": select_notes(&notes, input["locale"].as_str()) })
}

fn notes_vectors() -> Value {
    let case =
        |languages: &[&str], locale: Value| json!({ "languages": languages, "locale": locale });
    json!({
        "description": "Release notes language. The locale is cut one subtag at a time (zh-Hant-TW, zh-Hant, zh); then any tag of the same primary language, in sorted order; then en; then the first tag in sorted order. Tags compare without regard to case, and _ reads as -. The input lists the tags a release has notes for; each tag's note is the tag itself.",
        "cases": cases(eval_notes, vec![
            ("exact", case(&["en", "zh-Hant-TW"], "zh-Hant-TW".into())),
            ("drops the region", case(&["en", "zh-Hant"], "zh-Hant-TW".into())),
            ("drops the script", case(&["en", "zh"], "zh-Hant-TW".into())),
            ("other language falls to en", case(&["en", "ja"], "zh-Hant-TW".into())),
            ("same language under another tag", case(&["en", "zh-CN"], "zh-Hans-CN".into())),
            ("same language before en", case(&["en", "zh-CN"], "zh-Hant-TW".into())),
            ("underscore tag", case(&["en", "zh-CN"], "zh_CN".into())),
            ("case is ignored", case(&["en", "zh-cn"], "zh-CN".into())),
            ("regional english", case(&["en", "ja"], "en-GB".into())),
            ("regional english only", case(&["en-US", "ja"], "en-GB".into())),
            ("no locale", case(&["en", "zh-CN"], Value::Null)),
            ("nothing matches", case(&["ja", "ko"], "de".into())),
            ("no notes", case(&[], "en".into())),
        ]),
    })
}

#[test]
fn release_notes_vectors() {
    ensure_current("release_notes.json", &notes_vectors());
    replay("release_notes.json", eval_notes);
}

// ---------------------------------------------------------------------------
// 资源包
// ---------------------------------------------------------------------------

const PACK_DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PACK_DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn eval_packs(input: &Value) -> Value {
    let manifest: ReleaseManifest = serde_json::from_value(input["manifest"].clone()).unwrap();
    let held: Vec<HeldPack> = serde_json::from_value(input["held"].clone()).unwrap();
    let target = PackInput {
        current_build: input["currentBuild"].as_u64(),
        install_id: input["installId"].as_str().unwrap().to_owned(),
    };
    let selected = select_packs(&manifest, &target);
    let plan = plan_packs(&selected, &held);
    let summary: Vec<Value> = selected
        .iter()
        .map(|pack| json!({ "id": pack.id, "packVersion": pack.pack_version, "sha256": pack.sha256 }))
        .collect();
    let fetch: Vec<&str> = plan.fetch.iter().map(|pack| pack.id.as_str()).collect();
    json!({ "selected": summary, "fetch": fetch, "remove": plan.remove })
}

fn pack(id: &str, kind: &str, version: u64, min_build: u64, digest: &str) -> Value {
    json!({
        "id": id,
        "kind": kind,
        "packVersion": version,
        "minBuild": min_build,
        "url": ["https://cdn.example.com/pack.json"],
        "sha256": digest,
        "sizeBytes": 1024
    })
}

fn carrying(mut release: Value, packs: Vec<Value>) -> Value {
    release["resourcePacks"] = packs.into();
    release
}

fn pack_vectors() -> Value {
    let (a, b) = (PACK_DIGEST_A, PACK_DIGEST_B);
    let hit = NEW_BUCKET + 1;
    let newer = |basis_points: u32| {
        carrying(
            entry("rel_new", 20400, basis_points),
            vec![pack("strings-main", "strings", 5, 0, a)],
        )
    };
    let full = || {
        carrying(
            entry("rel_full", 20302, 10_000),
            vec![pack("strings-main", "strings", 3, 0, b)],
        )
    };
    let halted = || {
        let mut release = newer(10_000);
        release["rollout"]["halted"] = true.into();
        release
    };
    let held = |id: &str, digest: &str| json!({ "id": id, "sha256": digest });
    let case = |releases: Vec<Value>, facts: Value| {
        let mut input = release_case(
            json!({}),
            releases,
            json!({ "currentBuild": 20400, "held": [] }),
        );
        for (key, value) in facts.as_object().unwrap() {
            input[key] = value.clone();
        }
        input
    };
    let min_build = |build: u64| {
        carrying(
            entry("rel_new", 20400, 10_000),
            vec![pack("strings-main", "strings", 5, build, a)],
        )
    };
    let no_build = {
        let mut input = case(vec![min_build(20000), full()], json!({}));
        input.as_object_mut().unwrap().remove("currentBuild");
        input
    };
    json!({
        "description": "Resource pack selection and the plan that follows it. Walk the releases, skipping halted ones and those the bucket misses; skip packs of an unknown kind and packs whose minBuild is above the current build (without a current build, any pack with a minBuild); for each id keep the highest packVersion, the first in the manifest on a tie. selected is sorted by id. The selection is the only truth: a held pack with the same sha256 stays, a different sha256 is fetched whatever its version, a held id missing from the selection is removed. Rollback protection comes from the manifest signature and serial, not from packVersion.",
        "cases": cases(eval_packs, vec![
            ("highest version across releases", case(vec![newer(hit), full()], json!({}))),
            ("outside the rollout takes the full release", case(vec![newer(NEW_BUCKET), full()], json!({}))),
            ("halted release does not count", case(vec![halted(), full()], json!({}))),
            ("minBuild above the build is skipped", case(vec![min_build(20401), full()], json!({}))),
            ("minBuild at the build counts", case(vec![min_build(20400), full()], json!({}))),
            ("without a build only packs without minBuild", no_build),
            ("unknown kind is ignored", case(vec![carrying(entry("rel_new", 20400, 10_000), vec![pack("a", "hologram", 1, 0, a), pack("b", "tokens", 1, 0, b)])], json!({}))),
            ("same version keeps the first in the manifest", case(vec![carrying(entry("rel_new", 20400, 10_000), vec![pack("strings-main", "strings", 5, 0, a)]), carrying(entry("rel_full", 20302, 10_000), vec![pack("strings-main", "strings", 5, 0, b)])], json!({}))),
            ("ids come out sorted", case(vec![carrying(entry("rel_new", 20400, 10_000), vec![pack("tokens-main", "tokens", 1, 0, a), pack("emoji-main", "emoji", 1, 0, a), pack("help-main", "help", 1, 0, a)])], json!({}))),
            ("same hash stays", case(vec![newer(hit), full()], json!({ "held": [held("strings-main", a)] }))),
            ("another hash is fetched", case(vec![newer(hit), full()], json!({ "held": [held("strings-main", b)] }))),
            ("halted version falls back to a lower one", case(vec![halted(), full()], json!({ "held": [held("strings-main", a)] }))),
            ("version dropped from the manifest falls back to a lower one", case(vec![full()], json!({ "held": [held("strings-main", a)] }))),
            ("held id without a selection is removed", case(vec![halted()], json!({ "held": [held("strings-main", a)] }))),
            ("minBuild no longer met removes the pack", case(vec![min_build(20401)], json!({ "held": [held("strings-main", a)] }))),
            ("no packs in the manifest removes everything", case(vec![entry("rel_new", 20400, 10_000)], json!({ "held": [held("b", a), held("a", b)] }))),
            ("nothing held and nothing offered", case(vec![entry("rel_new", 20400, 10_000)], json!({}))),
        ]),
    })
}

#[test]
fn release_pack_vectors() {
    ensure_current("release_packs.json", &pack_vectors());
    replay("release_packs.json", eval_packs);
}

fn eval_pack_body(input: &Value) -> Value {
    let pack: ResourcePack = serde_json::from_value(input["pack"].clone()).unwrap();
    match verify_pack(&pack, input["body"].as_str().unwrap().as_bytes()) {
        Ok(_) => json!({ "accepted": true }),
        Err(error) => json!({ "accepted": false, "code": error.code() }),
    }
}

fn pack_body_vectors() -> Value {
    let digest = |text: &str| -> String {
        Sha256::digest(text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    };
    // 清单描述 listed 这份文件，实际收到的是 body。
    let received = |listed: &str, body: &str| {
        let mut entry = pack("strings-main", "strings", 1, 0, &digest(listed));
        entry["sizeBytes"] = listed.len().into();
        json!({ "pack": entry, "body": body })
    };
    let exact = |body: &str| received(body, body);
    let good =
        r#"{"schema":"flare.resource.v1","kind":"strings","locales":{"en":{"send":"Send"}}}"#;
    let swapped =
        r#"{"schema":"flare.resource.v1","kind":"strings","locales":{"en":{"send":"Sent"}}}"#;
    json!({
        "description": "Checks on a downloaded pack file, in this order: sizeBytes, sha256, JSON, schema, kind. The manifest entry is pack; body is the file as text. Codes: sizeMismatch, hashMismatch, malformed, wrongSchema, wrongKind.",
        "cases": cases(eval_pack_body, vec![
            ("the file the manifest lists", exact(good)),
            ("unknown fields are fine", exact(r#"{"schema":"flare.resource.v1","kind":"strings","extra":1}"#)),
            ("shorter than listed", received(good, &good[..good.len() - 1])),
            ("same size, other bytes", received(good, swapped)),
            ("not JSON", exact("{not json")),
            ("trailing garbage", exact(r#"{"schema":"flare.resource.v1","kind":"strings"} x"#)),
            ("an array is no pack", exact(r#"["flare.resource.v1","strings"]"#)),
            ("no schema", exact(r#"{"kind":"strings"}"#)),
            ("another schema", exact(r#"{"schema":"flare.resource.v2","kind":"strings"}"#)),
            ("another kind", exact(r#"{"schema":"flare.resource.v1","kind":"tokens"}"#)),
            ("no kind", exact(r#"{"schema":"flare.resource.v1"}"#)),
        ]),
    })
}

#[test]
fn release_pack_body_vectors() {
    ensure_current("release_pack_body.json", &pack_body_vectors());
    replay("release_pack_body.json", eval_pack_body);
}

// ---------------------------------------------------------------------------
// URL 解析：与 `url` crate 对照
// ---------------------------------------------------------------------------

const REPAIRED: &str = "url 删掉或改写后放行，这里拒绝";
const NO_AUTHORITY: &str = "没有 // 权限段，调用点都要主机";
const OTHER_SCHEME: &str = "file、ftp 不是这批调用点的协议";
const USERINFO: &str = "userinfo 没有正当用途，还能伪装主机";
const HOST_CHARS: &str = "主机只收 [A-Za-z0-9._-]，不带 IDNA";
const UNCHECKED_ACE: &str = "比 url 宽：xn-- 标签按普通 ASCII 处理，不校验 Punycode";
const EMPTY_HOST: &str = "空主机：url 跳过多余的斜杠或留空，这里拒绝";
const EMPTY_LABEL: &str = "空标签不是可解析的主机名";
const LOOSE_IPV4: &str = "只收规范的四段十进制 IPv4";
const EMPTY_PORT: &str = "空端口多半是拼接错误";
const DOT_SEGMENT: &str = "路径穿越：url 折叠后放行，这里直接拒绝";

fn eval_url(input: &Value) -> Value {
    match ParsedUrl::parse(input["url"].as_str().unwrap()) {
        Ok(url) => json!({
            "scheme": url.scheme(),
            "host": url.host_str(),
            "ip": url.ip().map(|ip| ip.to_string()),
            "port": url.port(),
            "path": url.path(),
            "query": url.query(),
            "fragment": url.fragment(),
            "queryPairs": url.query_pairs().collect::<Vec<_>>(),
            "serialized": url.to_string(),
        }),
        Err(error) => json!({ "rejected": format!("{error:?}") }),
    }
}

#[rustfmt::skip]
fn url_vectors() -> Value {
    // （名称，输入，与 `url` 判定不同时的理由）。理由为空表示两者判定相同。
    let inputs: &[(&str, &str, Option<&str>)] = &[
        // 正常地址
        ("mirror", "https://cdn.example.com/access.json", None),
        ("all parts", "https://cdn.example.com:8443/a/b?x=1&y=2#frag", None),
        ("no path", "https://cdn.example.com", None),
        ("query without path", "https://cdn.example.com?x=1", None),
        ("fragment without path", "https://cdn.example.com#x", None),
        ("empty query and fragment", "https://cdn.example.com/?#", None),
        ("doh resolver", "https://dns.google/dns-query", None),
        ("dev http", "http://localhost:8080/", None),
        ("dev ws", "ws://127.0.0.1:60051/ws", None),
        ("wss", "wss://im.example.com/ws", None),
        ("quic", "quic://im.example.com:60052", None),
        ("quic ipv4", "quic://127.0.0.1:60052", None),
        ("recovery link", "flare-recover://v1?p=abc&k=root-1&s=sig", None),
        ("recovery link with slash", "flare-recover://v1/?p=abc", None),
        ("underscore host", "http://flare_gateway:8080/", None),
        ("single label host", "http://gateway/", None),
        ("trailing dot host", "https://example.com./", None),
        ("edge hyphens", "https://-a-.example/", None),
        ("double hyphen", "https://ab--cd.example/", None),
        ("double slash in path", "https://a.example//b//c", None),
        // 大小写
        ("upper scheme and host", "HTTPS://CDN.EXAMPLE.COM/Path", None),
        ("non-special host keeps case", "QUIC://EXAMPLE.com:1", None),
        ("recovery host keeps case", "flare-recover://V1?p=a", None),
        // 端口边界
        ("https default port", "https://a.example:443/", None),
        ("http default port", "http://a.example:80/", None),
        ("ws default port", "ws://a.example:80/", None),
        ("wss default port", "wss://a.example:443/", None),
        ("cross default port", "https://a.example:80/", None),
        ("non-special keeps 443", "quic://a.example:443", None),
        ("port zero", "https://a.example:0/", None),
        ("port max", "https://a.example:65535/", None),
        ("port too large", "https://a.example:65536/", None),
        ("port leading zeros", "https://a.example:00443/", None),
        ("port plus sign", "https://a.example:+80/", None),
        ("port letters", "https://a.example:80a/", None),
        ("port twice", "https://a.example:80:80/", None),
        ("port empty", "https://a.example:/", Some(EMPTY_PORT)),
        // IPv4
        ("ipv4", "https://203.0.113.10/", None),
        ("ipv4 max", "https://255.255.255.255/", None),
        ("ipv4 octet too large", "https://1.2.3.256/", None),
        ("ipv4 short", "https://127.1/", Some(LOOSE_IPV4)),
        ("ipv4 hex", "https://0x7f.0.0.1/", Some(LOOSE_IPV4)),
        ("ipv4 octal", "https://0177.0.0.1/", Some(LOOSE_IPV4)),
        ("ipv4 integer", "https://2130706433/", Some(LOOSE_IPV4)),
        ("ipv4 octal integer", "https://017700000001/", Some(LOOSE_IPV4)),
        ("ipv4 trailing dot", "https://1.2.3.4./", Some(LOOSE_IPV4)),
        ("numeric last label", "https://foo.1/", None),
        ("hex last label", "https://foo.0x1/", None),
        ("non-special numeric host", "quic://1.2.3.256:1", None),
        // IPv6
        ("ipv6 loopback", "https://[::1]/", None),
        ("ipv6 port", "https://[::1]:8443/", None),
        ("ipv6 upper", "https://[2001:DB8::1]/", None),
        ("ipv6 full", "https://[1:2:3:4:5:6:7:8]/", None),
        ("ipv6 mapped", "https://[::ffff:1.2.3.4]/", None),
        ("ipv6 non-special", "quic://[::1]:9000", None),
        ("ipv6 unclosed", "https://[::1/", None),
        ("ipv6 bad digit", "https://[::g]/", None),
        ("ipv6 zone", "https://[fe80::1%25en0]/", None),
        ("ipv6 tail", "https://[::1]x/", None),
        ("ipv4 in brackets", "https://[127.0.0.1]/", None),
        // 空主机
        ("scheme only", "https://", None),
        ("empty host with port", "https://:443/", None),
        ("empty host with query", "https://?x", None),
        ("empty host with path", "https:///a.example/", Some(EMPTY_HOST)),
        ("non-special empty host", "quic://", Some(EMPTY_HOST)),
        ("non-special empty host with path", "quic:///a", Some(EMPTY_HOST)),
        // userinfo
        ("user and password", "https://user:pass@a.example/", Some(USERINFO)),
        ("empty userinfo", "https://@a.example/", Some(USERINFO)),
        ("host as userinfo", "https://good.example@evil.example/", Some(USERINFO)),
        ("port as userinfo", "https://good.example:443@evil.example/", Some(USERINFO)),
        ("encoded slash in userinfo", "https://evil.example%2f@good.example/", Some(USERINFO)),
        ("at sign in path", "https://a.example/@b", None),
        ("at sign in query", "https://good.example?@evil.example/", None),
        ("at sign in fragment", "https://good.example#@evil.example/", None),
        // 反斜杠
        ("backslash path", "https://a.example\\b", Some(REPAIRED)),
        ("backslash authority", "https:\\\\a.example\\", Some(REPAIRED)),
        ("backslash before userinfo", "https://evil.example\\@good.example/", Some(REPAIRED)),
        ("backslash in query", "https://a.example/?a=\\b", Some(REPAIRED)),
        ("backslash in non-special host", "quic://a\\b", None),
        ("backslash in non-special path", "quic://a/b\\c", Some(REPAIRED)),
        // 缺斜杠或多斜杠
        ("no slashes", "https:a.example/", Some(NO_AUTHORITY)),
        ("one slash", "https:/a.example/", Some(NO_AUTHORITY)),
        ("four slashes", "https:////a.example/", Some(EMPTY_HOST)),
        ("non-special no authority", "quic:a", Some(NO_AUTHORITY)),
        ("host and port read as scheme", "example.com:80", Some(NO_AUTHORITY)),
        ("mailto", "mailto:a@example.com", Some(NO_AUTHORITY)),
        ("data", "data:text/plain,x", Some(NO_AUTHORITY)),
        ("javascript", "javascript:alert(1)", Some(NO_AUTHORITY)),
        ("protocol relative", "//a.example/", None),
        ("bare host", "a.example", None),
        ("empty", "", None),
        ("colon", ":", None),
        ("digit scheme", "1https://a.example/", None),
        ("space in scheme", "ht tp://a.example/", None),
        // 不支持的 special scheme
        ("ftp", "ftp://a.example/", Some(OTHER_SCHEME)),
        ("file", "file:///etc/passwd", Some(OTHER_SCHEME)),
        // 控制字符与空白
        ("leading space", " https://a.example/", Some(REPAIRED)),
        ("trailing newline", "https://a.example/\n", Some(REPAIRED)),
        ("tab in host", "https://a.ex\tample/", Some(REPAIRED)),
        ("newline in host", "https://a.ex\nample/", Some(REPAIRED)),
        ("newline in path", "https://a.example/a\nb", Some(REPAIRED)),
        ("carriage return in query", "https://a.example/?a\rb", Some(REPAIRED)),
        ("tab in scheme", "ht\ttps://a.example/", Some(REPAIRED)),
        ("nul in host", "https://a\u{0}b.example/", None),
        ("nul in path", "https://a.example/a\u{0}b", Some(REPAIRED)),
        ("del in path", "https://a.example/a\u{7f}b", Some(REPAIRED)),
        ("unit separator in path", "https://a.example/a\u{1f}b", Some(REPAIRED)),
        ("space in path", "https://a.example/a b", Some(REPAIRED)),
        ("space in query", "https://a.example/?a b", Some(REPAIRED)),
        ("space in host", "https://a b.example/", None),
        // 百分号编码
        ("encoded letter in host", "https://ex%61mple.com/", Some(HOST_CHARS)),
        ("encoded slash in host", "https://a%2fb.example/", None),
        ("encoded nul in host", "https://a%00b.example/", None),
        ("encoded percent in non-special host", "quic://a%41b", Some(HOST_CHARS)),
        ("encoded slash in path", "https://a.example/a%2fb", None),
        ("encoded backslash in path", "https://a.example/a%5cb", None),
        ("encoded nul in path", "https://a.example/a%00b", None),
        ("bad escape in path", "https://a.example/%zz", None),
        ("bare percent in path", "https://a.example/100%", None),
        ("bare percent in query", "https://a.example/?a=100%", None),
        ("escapes in query pairs", "https://a.example/?a=%E4%BD%A0+b&c=%2B&d", None),
        // 路径穿越
        ("parent segment", "https://a.example/a/../b", Some(DOT_SEGMENT)),
        ("current segment", "https://a.example/a/./b", Some(DOT_SEGMENT)),
        ("parent at root", "https://a.example/..", Some(DOT_SEGMENT)),
        ("parent at end", "https://a.example/a/..", Some(DOT_SEGMENT)),
        ("encoded parent", "https://a.example/%2e%2e/etc/passwd", Some(DOT_SEGMENT)),
        ("upper encoded parent", "https://a.example/%2E%2E/b", Some(DOT_SEGMENT)),
        ("half encoded parent", "https://a.example/.%2e/b", Some(DOT_SEGMENT)),
        ("encoded current", "https://a.example/%2e/b", Some(DOT_SEGMENT)),
        ("non-special parent", "quic://a.example/a/../b", Some(DOT_SEGMENT)),
        ("dots inside a segment", "https://a.example/a..b/...", None),
        ("dot-dot in query", "https://a.example/?x=../..", None),
        ("dot-dot in fragment", "https://a.example/#../..", None),
        ("double encoded parent", "https://a.example/%252e%252e/b", None),
        ("encoded slash after dots", "https://a.example/%2e%2e%2fb", None),
        // 按 `url` 的编码集转义
        ("path punctuation", "https://a.example/\"<>`{}|^[]'!$&()*+,;=:@~", None),
        ("special query punctuation", "https://a.example/?\"<>'`{}|^[]", None),
        ("non-special query punctuation", "quic://a.example/?\"<>'", None),
        ("fragment punctuation", "https://a.example/#\"<>`'{}|^", None),
        ("question mark in query", "https://a.example/?a?b", None),
        ("hash in fragment", "https://a.example/#a#b", None),
        // 非 ASCII
        ("non-ascii path", "https://a.example/你好", None),
        ("non-ascii query", "https://a.example/?q=你好", None),
        ("non-ascii fragment", "https://a.example/#你好", None),
        ("ideographic space in path", "https://a.example/a\u{3000}b", None),
        ("non-ascii host", "https://例子.cn/", Some(HOST_CHARS)),
        ("non-ascii non-special host", "quic://例子.cn:1", Some(HOST_CHARS)),
        ("full-width host", "https://ＥＸＡＭＰＬＥ.com/", Some(HOST_CHARS)),
        ("ideographic full stop", "https://a。example/", Some(HOST_CHARS)),
        ("ace label", "https://xn--fiqs8s.example/", None),
        ("upper ace label", "https://XN--fiqs8s.example/", None),
        ("bad ace label", "https://xn--a.example/", Some(UNCHECKED_ACE)),
        ("ace prefix only", "https://xn--.example/", Some(UNCHECKED_ACE)),
        ("ace label in non-special host", "quic://xn--a.example:1", None),
        // 主机字符
        ("exclamation in host", "https://a!b.example/", Some(HOST_CHARS)),
        ("tilde in host", "https://a~b.example/", Some(HOST_CHARS)),
        ("dollar in host", "https://a$b.example/", Some(HOST_CHARS)),
        ("empty label", "https://a..b.example/", Some(EMPTY_LABEL)),
        ("leading dot", "https://.a.example/", Some(EMPTY_LABEL)),
        ("only a dot", "https://./", Some(EMPTY_LABEL)),
        ("angle bracket in host", "https://a<b.example/", None),
        ("pipe in host", "https://a|b.example/", None),
        ("caret in host", "https://a^b.example/", None),
    ];
    let inputs = inputs
        .iter()
        .map(|(name, url, differs)| (*name, json!({ "url": url, "differs": differs })))
        .collect();
    json!({
        "description": "ParsedUrl 的判定与解析结果。input.differs 非空：与 url crate 判定不同，值是理由。除 xn-- 一类外都是 url 接受而 ParsedUrl 有意拒绝。",
        "cases": cases(eval_url, inputs),
    })
}

#[test]
fn url_parse_vectors() {
    let generated = url_vectors();
    ensure_current("url_parse.json", &generated);
    replay("url_parse.json", eval_url);
    for case in generated["cases"].as_array().unwrap() {
        let name = &case["name"];
        let input = case["input"]["url"].as_str().unwrap();
        let differs = case["input"]["differs"].as_str();
        match (ParsedUrl::parse(input), url::Url::parse(input)) {
            (Ok(ours), Ok(theirs)) => {
                assert_eq!(
                    differs, None,
                    "{name}: both accept, but marked as different"
                );
                assert_eq!(ours.scheme(), theirs.scheme(), "{name}");
                assert_eq!(Some(ours.host_str()), theirs.host_str(), "{name}");
                assert_eq!(ours.port(), theirs.port(), "{name}");
                assert_eq!(ours.path(), theirs.path(), "{name}");
                assert_eq!(ours.query(), theirs.query(), "{name}");
                assert_eq!(ours.fragment(), theirs.fragment(), "{name}");
                assert_eq!(ours.to_string(), theirs.as_str(), "{name}");
                let pairs: Vec<_> = theirs.query_pairs().into_owned().collect();
                assert_eq!(ours.query_pairs().collect::<Vec<_>>(), pairs, "{name}");
            }
            // 比 `url` 宽的只允许 `xn--` 这一类。
            (Ok(_), Err(error)) => {
                assert_eq!(differs, Some(UNCHECKED_ACE), "{name}: url says {error}")
            }
            (Err(_), Ok(_)) => assert!(
                differs.is_some_and(|reason| reason != UNCHECKED_ACE),
                "{name}: tightened without a reason"
            ),
            (Err(_), Err(_)) => assert_eq!(differs, None, "{name}: url rejects this too"),
        }
    }
}
