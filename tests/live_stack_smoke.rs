//! 真栈冒烟：本机隔离的真实服务栈（`scripts/live_stack.py`，一个 Cell、两个信令网关作两个接入点），
//! SDK 以开发模式双接入点接入，走完整链路（链路状态机 → 选路 → 交错竞速 → CONNECT → 发件箱 → SendAck），
//! 令牌由 SDK 托管（`auth: {}`），向真实 api-gateway 签发。
//!
//! 全部 `#[ignore]`，地址从环境变量读：
//!
//! ```sh
//! python3 scripts/live_stack.py up
//! eval "$(python3 scripts/live_stack.py env)"
//! RUSTC_WRAPPER= cargo test -j 2 --test live_stack_smoke -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! 每个用例结束时打印一行 `EVIDENCE {json}`；用例会经 `LIVE_SMOKE_CTL` 杀掉、停掉、重启网关，
//! 结束前把网关恢复成默认配置。服务端历史用 `LIVE_SMOKE_CORE_DB` 上的 psql 按 seq 查 `messages`。

#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flare_core::client::builder::flare::{FlareClientBuilder, MessageListener};
use flare_core::client::link::DisconnectReason;
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::protocol::Frame;
use flare_im_core_sdk::client::config::SdkAuthConfig;
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::prelude::*;
use serde_json::{Value, json};

use link_lab::{Observed, wait_phase, wait_until};

const PRIMARY: &str = "development";
const SECONDARY: &str = "dev-b";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} is not set: eval \"$(python3 scripts/live_stack.py env)\" first")
    })
}

fn run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{millis:x}")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// 接入点 ID → 栈里的网关进程名。
fn gateway_of(access_point: &str) -> &'static str {
    match access_point {
        PRIMARY => "gw-a",
        SECONDARY => "gw-b",
        other => panic!("unknown access point {other}"),
    }
}

fn other_access_point(access_point: &str) -> &'static str {
    if access_point == PRIMARY {
        SECONDARY
    } else {
        PRIMARY
    }
}

/// `live_stack.py ctl <args>`：杀、停、起网关（只按栈记下的 PID 发信号）。
fn ctl(args: &[&str]) {
    let line = env("LIVE_SMOKE_CTL");
    let mut parts = line.split_whitespace();
    let program = parts.next().expect("LIVE_SMOKE_CTL");
    let status = Command::new(program)
        .args(parts)
        .arg("ctl")
        .args(args)
        .status()
        .expect("run live_stack.py ctl");
    assert!(status.success(), "ctl {args:?} failed");
}

fn psql(sql: &str) -> String {
    let output = Command::new("psql")
        .args([
            "-X",
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            &env("LIVE_SMOKE_CORE_DB"),
            "-c",
            sql,
        ])
        .output()
        .expect("run psql");
    assert!(
        output.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn log_path(service: &str) -> PathBuf {
    PathBuf::from(env("LIVE_SMOKE_DIR"))
        .join("logs")
        .join(format!("{service}.log"))
}

/// 服务日志里同时含 `needles` 的行（跳过 ANSI 颜色码）。
fn log_lines(service: &str, needles: &[&str]) -> Vec<String> {
    let text = std::fs::read_to_string(log_path(service)).unwrap_or_default();
    let ansi = |line: &str| {
        let mut out = String::with_capacity(line.len());
        let mut chars = line.chars();
        while let Some(c) = chars.next() {
            if c == '\u{1b}' {
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    };
    text.lines()
        .map(ansi)
        .filter(|line| needles.iter().all(|needle| line.contains(needle)))
        .collect()
}

fn issued_for(user: &str) -> usize {
    log_lines("api-gateway", &["access token issued", user]).len()
}

/// 开发模式双接入点：主接入点（`development`）= gw-a，另一个（`dev-b`）= gw-b，同一个 api-gateway。
fn two_access_points(policy: Option<Value>) -> AccessConfig {
    let core_api = env("LIVE_SMOKE_CORE_API");
    let social_api = env("LIVE_SMOKE_SOCIAL_API");
    AccessConfig::development(DevelopmentEndpoints {
        ws_url: Some(env("LIVE_SMOKE_WS_A")),
        core_api_url: Some(core_api.clone()),
        social_api_url: Some(social_api.clone()),
        access_points: vec![DevelopmentAccessPoint {
            id: SECONDARY.to_owned(),
            ws_url: Some(env("LIVE_SMOKE_WS_B")),
            core_api_url: Some(core_api),
            social_api_url: Some(social_api),
            ..Default::default()
        }],
        client_policy: policy,
        ..Default::default()
    })
}

/// 只有一个接入点（`ap` 对应的网关）的开发模式接入。
fn only_access_point(ap: &str) -> AccessConfig {
    let ws = if ap == PRIMARY {
        env("LIVE_SMOKE_WS_A")
    } else {
        env("LIVE_SMOKE_WS_B")
    };
    AccessConfig::development(DevelopmentEndpoints {
        ws_url: Some(ws),
        core_api_url: Some(env("LIVE_SMOKE_CORE_API")),
        ..Default::default()
    })
}

fn data_root(case: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("flare-live-stack-smoke")
        .join(format!("{case}-{}-{}", std::process::id(), run_id()))
}

/// SDK 托管令牌（`auth: {}`）的客户端，已 init、未登录。
async fn managed_client(case: &str, access: AccessConfig) -> IMClient {
    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(data_root(case).display().to_string()),
                access: Some(access),
                tenant_id: Some(env("LIVE_SMOKE_TENANT")),
                auth: Some(SdkAuthConfig::default()),
                ack_timeout_secs: Some(10),
                ..Default::default()
            }),
        )
        .await
        .expect("init");
    client
}

async fn login(client: &IMClient, user: &str) {
    if let Err(error) = client
        .login(
            user,
            None,
            LoginDbKind::IndexedDb(in_memory_im_provider()),
            |_, _| {},
        )
        .await
    {
        panic!("login {user}: {error}");
    }
}

async fn wait_online(client: &IMClient, deadline: Duration) -> ConnectionStatus {
    wait_phase(client, ConnectionPhase::Online, deadline).await
}

async fn single_chat(client: &IMClient, peer: &str) -> String {
    client
        .conversation()
        .expect("conversation api")
        .get_one(peer, &ConversationType::Single)
        .await
        .expect("single chat")
        .conversation_id
}

async fn send_text(client: &IMClient, conversation: &str, text: &str) -> String {
    let draft = client
        .message_build()
        .expect("builder")
        .create_text(conversation, text, false, &[])
        .await
        .expect("build text");
    let id = draft.client_msg_id.clone();
    client
        .message()
        .expect("message api")
        .send(draft)
        .await
        .expect("enqueue");
    id
}

/// 服务端（PostgreSQL）里这个会话按 seq 排好的 `client_msg_id`。
fn server_history(conversation: &str) -> Vec<String> {
    let tenant = env("LIVE_SMOKE_TENANT");
    let rows = psql(&format!(
        "SELECT client_msg_id FROM messages WHERE tenant_id = '{tenant}' AND conversation_id = '{conversation}' ORDER BY seq"
    ));
    rows.lines().map(str::to_owned).collect()
}

/// `at - origin`，毫秒，早于 origin 为负。
fn signed_ms(at: Instant, origin: Instant) -> i64 {
    if at >= origin {
        at.duration_since(origin).as_millis() as i64
    } else {
        -(origin.duration_since(at).as_millis() as i64)
    }
}

/// SendAck 只说明已被接受（WAL/消息总线），写库是异步的：等到行数够了（或超时）再按 seq 读。
async fn persisted_history(conversation: &str, expected: usize) -> Vec<String> {
    let started = Instant::now();
    loop {
        let rows = server_history(conversation);
        if rows.len() >= expected || started.elapsed() > Duration::from_secs(30) {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// 服务端按 seq 的第 i 行不是第 i 条发出的消息时，记下 (seq 位置, 实际是第几条发出的)。
fn out_of_order(history: &[String], sent: &[String]) -> Vec<(usize, Option<usize>)> {
    history
        .iter()
        .enumerate()
        .filter(|(position, id)| sent.get(*position) != Some(*id))
        .map(|(position, id)| (position, sent.iter().position(|sent| sent == id)))
        .collect()
}

fn statuses_json(observed: &Observed, origin: Instant) -> Vec<Value> {
    observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .map(|(at, status)| {
            json!({ "atMs": signed_ms(*at, origin), "status": serde_json::to_value(status).unwrap() })
        })
        .collect()
}

fn switched_json(observed: &Observed, origin: Instant) -> Vec<Value> {
    observed
        .switched
        .lock()
        .unwrap()
        .iter()
        .map(|(at, switched)| {
            json!({
                "atMs": at.saturating_duration_since(origin).as_millis(),
                "switched": serde_json::to_value(switched).unwrap(),
            })
        })
        .collect()
}

/// `origin` 之后第一次连上 `access_point`（syncing/online）的时刻。
fn first_connected_after(
    observed: &Observed,
    origin: Instant,
    access_point: &str,
) -> Option<Instant> {
    observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .find(|(at, status)| {
            *at >= origin
                && matches!(
                    status.phase,
                    ConnectionPhase::Syncing | ConnectionPhase::Online
                )
                && status.access_point_id.as_deref() == Some(access_point)
        })
        .map(|(at, _)| *at)
}

fn evidence(case: &str, value: Value) {
    println!("EVIDENCE {}", json!({ "case": case, "result": value }));
}

/// 传输层直连一次：读服务端在 CONNECT_ACK 里填的字段（不经 SDK）。
struct Silent;

#[async_trait::async_trait]
impl MessageListener for Silent {
    async fn on_message(&self, _frame: &Frame) -> flare_core::common::error::Result<Option<Frame>> {
        Ok(None)
    }

    async fn on_disconnect(
        &self,
        _reason: &DisconnectReason,
    ) -> flare_core::common::error::Result<()> {
        Ok(())
    }
}

async fn dev_token(user: &str) -> String {
    let response: Value = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/api/v1/auth/tokens", env("LIVE_SMOKE_CORE_API")))
        .json(&json!({ "userId": user, "tenantId": env("LIVE_SMOKE_TENANT") }))
        .send()
        .await
        .expect("issue request")
        .json()
        .await
        .expect("issue response");
    response["data"]["token"]
        .as_str()
        .unwrap_or_else(|| panic!("no token: {response}"))
        .to_owned()
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "warn,flare_im_core_sdk::runtime::link=info".into()),
        )
        .with_test_writer()
        .try_init();
}

/// S1 CONNECT_ACK：服务端在 CONNECT_ACK 里填了 gateway_id / ap_id / 服务端时间 / access_serial / 心跳提示，
/// SDK 连上后在状态、接入状态、诊断与服务端时钟估计里能看到哪些。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn s1_connect_ack_reaches_the_client() {
    init_tracing();
    let run = run_id();
    let user = format!("m1-s1-{run}");

    // 传输层：两个网关各握手一次，看 CONNECT_ACK 原样带回了什么。
    let token = dev_token(&format!("{user}-probe")).await;
    let mut acks = Vec::new();
    for (ap, url) in [
        ("gw-a", env("LIVE_SMOKE_WS_A")),
        ("gw-b", env("LIVE_SMOKE_WS_B")),
    ] {
        let sent_at = now_ms();
        let probe = FlareClientBuilder::new(url.clone())
            .with_user_id(format!("{user}-probe"))
            .with_token(token.clone())
            .with_device_info(DeviceInfo::new(
                format!("probe-{ap}-{run}"),
                DevicePlatform::PC,
            ))
            .with_connection_epoch(7)
            .with_access_serial(0)
            .with_listener(Arc::new(Silent))
            .build_with_race()
            .await
            .unwrap_or_else(|failure| panic!("probe {ap}: {failure:?}"));
        let report = probe.connect_report().clone();
        let ack = report.ack.clone();
        let server_time = ack.server_time_ms.expect("server_time_ms") as i64;
        assert!(
            (server_time - sent_at).abs() < 5_000,
            "server time {server_time} vs local {sent_at}"
        );
        assert!(ack.gateway_id.is_some(), "{ap}: gateway_id");
        assert!(ack.ap_id.is_some(), "{ap}: ap_id");
        assert!(ack.access_serial.is_some(), "{ap}: access_serial");
        assert!(
            ack.heartbeat_interval_hint_sec.is_some(),
            "{ap}: heartbeat hint"
        );
        acks.push(json!({
            "gateway": ap,
            "ack": serde_json::to_value(&ack).unwrap(),
            "serverMinusLocalMs": server_time - sent_at,
            "protocol": format!("{:?}", report.protocol),
            "handshakeMs": report.handshake.as_millis(),
        }));
        let _ = probe.disconnect().await;
    }

    // SDK：双接入点、SDK 托管令牌，看状态、接入状态、诊断、服务端时钟。
    let client = managed_client("s1", two_access_points(None)).await;
    login(&client, &user).await;
    // 事件订阅要有会话，登录后再挂。
    let (observed, _subscriptions) = Observed::record(&client);
    let online = wait_online(&client, Duration::from_secs(30)).await;
    let clock = client.server_clock().await;
    let access = client.access_status().await.expect("access status");
    let points = client.list_access_points().await.expect("access points");
    let diagnostics = client
        .export_access_diagnostics()
        .await
        .expect("diagnostics");
    let ap = online
        .access_point_id
        .clone()
        .expect("current access point");
    let gateway = gateway_of(&ap);
    let registered = log_lines(gateway, &["Connection registered", &user]);
    assert!(online.development_mode);
    assert_eq!(online.protocol.as_deref(), Some("ws"));
    assert_eq!(access.access_point_id.as_deref(), Some(ap.as_str()));
    let status_json = serde_json::to_value(&access).unwrap();
    let ack = status_json["connectAck"].clone();
    let diagnostics_ack = diagnostics["status"]["connectAck"].clone();
    let visible = json!({
        "getStatusConnectAck": ack,
        "diagnosticsConnectAck": diagnostics_ack,
        "warnings": diagnostics["warnings"],
        "serverClockEstimated": clock.estimated,
    });
    evidence(
        "S1",
        json!({
            "transportConnectAck": acks,
            "status": serde_json::to_value(&online).unwrap(),
            "serverClock": { "offsetMs": clock.offset_ms, "estimated": clock.estimated },
            "accessStatus": serde_json::to_value(&access).unwrap(),
            "accessPoints": serde_json::to_value(&points).unwrap(),
            "diagnostics": diagnostics,
            "visibleInClientDiagnostics": visible,
            "gatewayLog": registered,
            "statusEvents": observed.statuses.lock().unwrap().len(),
        }),
    );
    assert!(clock.estimated, "the server time correction is applied");
    let expected_gateway = if ap == PRIMARY { "m1-gw-a" } else { "m1-gw-b" };
    for (place, ack) in [
        ("access.get_status", &ack),
        ("export_diagnostics", &diagnostics_ack),
    ] {
        assert_eq!(
            ack["gatewayId"],
            json!(expected_gateway),
            "{place}: gatewayId"
        );
        assert_eq!(ack["accessPointId"], json!(ap), "{place}: accessPointId");
        assert_eq!(ack["accessSerial"], json!(0), "{place}: accessSerial");
        assert_eq!(
            ack["heartbeatIntervalHintSec"],
            json!(30),
            "{place}: heartbeatIntervalHintSec"
        );
        assert_eq!(
            ack["accessPointMismatch"],
            json!(false),
            "{place}: accessPointMismatch"
        );
    }
    let _ = client.logout().await;
}

/// S2 杀接入点：连续发 100 条，中途 SIGKILL 当前接入点的信令网关。切到另一个接入点、不重新登录；
/// 100 条 0 失败 0 重复，服务端按会话 seq 有序。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn s2_killing_the_access_point_loses_nothing() {
    init_tracing();
    let run = run_id();
    let user = format!("m1-s2-{run}");
    let peer = format!("m1-s2-peer-{run}");
    let client = managed_client("s2", two_access_points(None)).await;
    login(&client, &user).await;
    // 事件订阅要有会话，登录后再挂。
    let (observed, _subscriptions) = Observed::record(&client);
    let online = wait_online(&client, Duration::from_secs(30)).await;
    let generation = client.session_generation().await;
    let ap = online.access_point_id.clone().unwrap();
    let other = other_access_point(&ap);
    let conversation = single_chat(&client, &peer).await;
    let issued_before = issued_for(&user);

    let mut sent = Vec::with_capacity(100);
    let mut killed_at = None;
    for index in 0..100 {
        sent.push(send_text(&client, &conversation, &format!("s2 {run} #{index:03}")).await);
        if index == 29 {
            killed_at = Some(Instant::now());
            ctl(&["kill", gateway_of(&ap)]);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let killed_at = killed_at.unwrap();
    wait_until("all 100 acknowledged", Duration::from_secs(90), || async {
        observed.accepted_distinct().len() == 100
    })
    .await;
    let recovered_at = first_connected_after(&observed, killed_at, other);
    let history = persisted_history(&conversation, sent.len()).await;
    let failed = observed.failed.lock().unwrap().clone();
    let acks = observed.accepted.lock().unwrap().len();
    let issued_after = issued_for(&user);
    let generation_after = client.session_generation().await;
    let status = client.connection_status().await;
    evidence(
        "S2",
        json!({
            "killedGateway": gateway_of(&ap), "from": ap, "to": other,
            "recoveryMs": recovered_at.map(|at| at.duration_since(killed_at).as_millis()),
            "sent": sent.len(), "acceptedDistinct": observed.accepted_distinct().len(), "ackEvents": acks,
            "failed": failed,
            "serverRows": history.len(),
            "serverDistinct": history.iter().collect::<std::collections::HashSet<_>>().len(),
            "serverOrderEqualsSendOrder": history == sent,
            "outOfOrder": out_of_order(&history, &sent),
            "sessionGeneration": [generation, generation_after],
            "tokensIssued": [issued_before, issued_after],
            "finalStatus": serde_json::to_value(&status).unwrap(),
            "switched": switched_json(&observed, killed_at),
            "statusesAfterKill": statuses_json(&observed, killed_at)
                .into_iter().filter(|value| value["atMs"].as_i64().unwrap_or(-1) >= 0).collect::<Vec<_>>(),
        }),
    );
    let _ = client.logout().await;
    ctl(&["start", gateway_of(&ap)]);
    assert!(failed.is_empty(), "no send failed: {failed:?}");
    assert_eq!(history.len(), 100, "100 rows on the server");
    assert_eq!(history, sent, "server order is the send order");
    assert_eq!(generation_after, generation, "a switch is not a relogin");
    assert_eq!(issued_after, issued_before, "no new token was issued");
    assert!(
        recovered_at.is_some(),
        "connected to the other access point"
    );
}

/// S3 停机排空：SIGTERM 当前接入点的网关。客户端收到 CLOSE（SERVER_SHUTDOWN，1–5 s 窗口），
/// 在窗口内重连到另一个接入点；期间一直在发，不丢消息。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn s3_graceful_shutdown_reconnects_inside_the_window() {
    init_tracing();
    let run = run_id();
    let user = format!("m1-s3-{run}");
    let peer = format!("m1-s3-peer-{run}");
    let client = managed_client("s3", two_access_points(None)).await;
    login(&client, &user).await;
    // 事件订阅要有会话，登录后再挂。
    let (observed, _subscriptions) = Observed::record(&client);
    let online = wait_online(&client, Duration::from_secs(30)).await;
    let generation = client.session_generation().await;
    let ap = online.access_point_id.clone().unwrap();
    let other = other_access_point(&ap);
    let conversation = single_chat(&client, &peer).await;

    let mut sent = Vec::new();
    let mut terminated_at = None;
    for index in 0..60 {
        sent.push(send_text(&client, &conversation, &format!("s3 {run} #{index:03}")).await);
        if index == 19 {
            terminated_at = Some(Instant::now());
            ctl(&["term", gateway_of(&ap)]);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let terminated_at = terminated_at.unwrap();
    wait_until("all 60 acknowledged", Duration::from_secs(90), || async {
        observed.accepted_distinct().len() == 60
    })
    .await;
    let reconnected_at = first_connected_after(&observed, terminated_at, other);
    let history = persisted_history(&conversation, sent.len()).await;
    let failed = observed.failed.lock().unwrap().clone();
    let close_lines = log_lines(gateway_of(&ap), &["SERVER_SHUTDOWN"]);
    // 发出 CLOSE 的接入点这一轮不该再拨：收到 CLOSE 之后的每次尝试（connecting）与等待中报的
    // 下一个接入点（switching / reconnecting）都应是另一个。
    let after_close: Vec<(i64, ConnectionPhase, Option<String>)> = observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, status)| {
            *at >= terminated_at
                && matches!(
                    status.phase,
                    ConnectionPhase::Connecting
                        | ConnectionPhase::Switching
                        | ConnectionPhase::Reconnecting
                )
        })
        .map(|(at, status)| {
            (
                signed_ms(*at, terminated_at),
                status.phase,
                status.access_point_id.clone(),
            )
        })
        .collect();
    let dialed_original = after_close
        .iter()
        .filter(|(_, phase, target)| {
            *phase == ConnectionPhase::Connecting && target.as_deref() == Some(ap.as_str())
        })
        .count();
    evidence(
        "S3",
        json!({
            "terminatedGateway": gateway_of(&ap), "from": ap, "to": other,
            "reconnectMs": reconnected_at.map(|at| at.duration_since(terminated_at).as_millis()),
            "sent": sent.len(), "acceptedDistinct": observed.accepted_distinct().len(),
            "failed": failed, "serverRows": history.len(), "serverOrderEqualsSendOrder": history == sent,
            "outOfOrder": out_of_order(&history, &sent),
            "sessionGeneration": [generation, client.session_generation().await],
            "switched": switched_json(&observed, terminated_at),
            "statusesAfterTerm": statuses_json(&observed, terminated_at)
                .into_iter().filter(|value| value["atMs"].as_i64().unwrap_or(-1) >= 0).collect::<Vec<_>>(),
            "gatewayShutdownLog": close_lines.iter().rev().take(5).collect::<Vec<_>>(),
            "attemptsAndTargetsAfterClose": after_close.iter()
                .map(|(at, phase, target)| json!({ "atMs": at, "phase": phase, "ap": target }))
                .collect::<Vec<_>>(),
            "dialedClosingAccessPoint": dialed_original,
        }),
    );
    let _ = client.logout().await;
    // 网关收到 SIGTERM 后自己退出；等它退干净再按默认配置起回来。
    ctl(&["stop", gateway_of(&ap)]);
    ctl(&["start", gateway_of(&ap)]);
    assert!(failed.is_empty(), "no send failed: {failed:?}");
    assert_eq!(
        history, sent,
        "nothing lost, server order is the send order"
    );
    assert!(
        reconnected_at.is_some(),
        "reconnected to the other access point"
    );
    assert_eq!(
        dialed_original, 0,
        "the access point that closed is not dialled again"
    );
    assert!(
        after_close
            .iter()
            .all(|(_, _, target)| target.as_deref() != Some(ap.as_str())),
        "every status after the close points at {other}: {after_close:?}"
    );
}

/// S4 过载：gw-a（`development`）重启成只放一个连接（`ACCESS_GATEWAY_MAX_CONNECTIONS=1`）并被另一个
/// 客户端占住；gw-b 先停，让客户端落到 gw-a 上收 OVERLOADED，收到之后再把 gw-b 起来。期望客户端在 gw-a
/// 上按 retry_after 等待后重试，gw-b 可用也不切过去，不计入熔断；名额让出后在 gw-a 上连上。
///
/// 等待中的 reconnecting 状态报的就是在等的接入点；每轮尝试看 connecting 状态，等待期间的当前连接看
/// `access.get_status`（没有当前连接，为 None）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn s4_overload_waits_on_the_same_access_point() {
    init_tracing();
    let run = run_id();
    let user = format!("m1-s4-{run}");
    let (x, y) = (PRIMARY, SECONDARY);
    ctl(&[
        "restart",
        gateway_of(x),
        "--env",
        "ACCESS_GATEWAY_MAX_CONNECTIONS=1",
    ]);
    let occupant = managed_client("s4-occupant", only_access_point(x)).await;
    login(&occupant, &format!("m1-s4-occupant-{run}")).await;
    wait_online(&occupant, Duration::from_secs(30)).await;
    ctl(&["stop", gateway_of(y)]);

    let client = managed_client("s4", two_access_points(None)).await;
    let started = Instant::now();
    login(&client, &user).await;
    let (observed, _subscriptions) = Observed::record(&client);
    let connecting_on = |observed: &Observed, ap: &str| {
        observed
            .statuses
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, status)| {
                status.phase == ConnectionPhase::Connecting
                    && status.access_point_id.as_deref() == Some(ap)
            })
            .count()
    };
    wait_until(
        "an attempt on gw-a followed by a wait",
        Duration::from_secs(30),
        || async {
            connecting_on(&observed, x) >= 1
                && client.connection_status().await.phase == ConnectionPhase::Reconnecting
        },
    )
    .await;
    let rejected = client.connection_status().await;
    ctl(&["start", gateway_of(y)]);
    let y_up_at = Instant::now();
    // gw-b 可用、gw-a 名额一直被占着的 10 s 里，客户端不该迁移到 gw-b：每 250 ms 看一次
    // access.get_status 的当前接入点（等待期间没有当前连接，为 None）。
    let mut current_samples: Vec<Option<String>> = Vec::new();
    while y_up_at.elapsed() < Duration::from_secs(10) {
        current_samples.push(
            client
                .access_status()
                .await
                .expect("access status")
                .access_point_id,
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let during_wait = client.connection_status().await;
    let on_y = first_connected_after(&observed, started, y);
    let y_registrations = log_lines(gateway_of(y), &["Connection registered", &user]).len();
    let points_during = client.list_access_points().await.expect("access points");
    let _ = occupant.logout().await;
    let released_at = Instant::now();
    let online = wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(30)).await;
    let connected_at = first_connected_after(&observed, released_at, x);
    let points_after = client.list_access_points().await.expect("access points");
    let attempts: Vec<Value> = observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, status)| *at < released_at && status.phase == ConnectionPhase::Connecting)
        .map(
            |(at, status)| json!({ "atMs": signed_ms(*at, started), "ap": status.access_point_id }),
        )
        .collect();
    let waits: Vec<Value> = observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, status)| status.phase == ConnectionPhase::Reconnecting)
        .map(|(at, status)| {
            json!({
                "atMs": signed_ms(*at, started),
                "ap": status.access_point_id,
                "attempt": status.attempt,
                "nextRetryAt": status.next_retry_at,
            })
        })
        .collect();
    evidence(
        "S4",
        json!({
            "overloadedAccessPoint": x, "other": y,
            "statusAfterFirstRejection": serde_json::to_value(&rejected).unwrap(),
            "otherUpAfterMs": y_up_at.duration_since(started).as_millis(),
            "statusAfter10sWithOtherUp": serde_json::to_value(&during_wait).unwrap(),
            "connectedToOther": on_y.map(|at| at.duration_since(started).as_millis()),
            "otherGatewayRegistrations": y_registrations,
            "currentAccessPointSamplesDuringWait": {
                "samples": current_samples.len(),
                "none": current_samples.iter().filter(|ap| ap.is_none()).count(),
                "onOverloaded": current_samples.iter().filter(|ap| ap.as_deref() == Some(x)).count(),
                "onOther": current_samples.iter().filter(|ap| ap.as_deref() == Some(y)).count(),
            },
            "accessPointsDuringWait": serde_json::to_value(&points_during).unwrap(),
            "accessPointsAfter": serde_json::to_value(&points_after).unwrap(),
            "connectedAfterReleaseMs": connected_at.map(|at| at.duration_since(released_at).as_millis()),
            "finalAccessPoint": online.access_point_id,
            "attempts": attempts,
            "waits": waits,
        }),
    );
    let _ = client.logout().await;
    ctl(&["restart", gateway_of(x)]);
    let tried_after_y_up = observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, status)| {
            *at >= y_up_at && *at < released_at && status.phase == ConnectionPhase::Connecting
        })
        .map(|(_, status)| status.access_point_id.clone())
        .collect::<Vec<_>>();
    let waiting_targets: Vec<Option<String>> = observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .filter(|(at, status)| *at < released_at && status.phase == ConnectionPhase::Reconnecting)
        .map(|(_, status)| status.access_point_id.clone())
        .collect();
    assert_eq!(rejected.phase, ConnectionPhase::Reconnecting);
    assert_eq!(
        rejected.access_point_id.as_deref(),
        Some(x),
        "the wait names the access point"
    );
    assert!(
        !waiting_targets.is_empty() && waiting_targets.iter().all(|ap| ap.as_deref() == Some(x)),
        "every reconnecting status names {x}: {waiting_targets:?}"
    );
    assert!(on_y.is_none(), "overload must not migrate to {y}");
    assert_eq!(y_registrations, 0, "no connection registered on {y}");
    assert!(
        current_samples.iter().all(|ap| ap.as_deref() != Some(y)),
        "access.get_status never reports {y} while waiting"
    );
    assert!(
        !tried_after_y_up.is_empty(),
        "kept retrying while overloaded"
    );
    assert!(
        tried_after_y_up.iter().all(|ap| ap.as_deref() == Some(x)),
        "every retry went to {x}: {tried_after_y_up:?}"
    );
    assert_eq!(online.access_point_id.as_deref(), Some(x));
    let point = points_after
        .iter()
        .find(|point| point.access_point_id == x)
        .unwrap();
    assert_eq!(
        point.breaker, "closed",
        "overload does not count against the breaker"
    );
}

/// S6 托管令牌离线登录：`auth: {}`，先报离线再 login。登录成功、进入 waitingForNetwork、不发签发请求；
/// 报在线后先向真实 api-gateway 签发令牌，再握手上线。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn s6_managed_token_offline_login() {
    init_tracing();
    let run = run_id();
    let user = format!("m1-s6-{run}");
    let client = managed_client("s6", two_access_points(None)).await;
    client
        .notify_network_change(NetworkChangeEvent {
            available: false,
            ..Default::default()
        })
        .await
        .unwrap();
    let started = Instant::now();
    login(&client, &user).await;
    // 事件订阅要有会话，登录后再挂。
    let (observed, _subscriptions) = Observed::record(&client);
    let waiting = wait_phase(
        &client,
        ConnectionPhase::WaitingForNetwork,
        Duration::from_secs(5),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let issued_offline = issued_for(&user);
    let registered_offline = log_lines("gw-a", &["Connection registered", &user]).len()
        + log_lines("gw-b", &["Connection registered", &user]).len();
    let token_offline = client.access_token().await.is_some();

    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let online_at = Instant::now();
    let online = wait_online(&client, Duration::from_secs(30)).await;
    let issued = log_lines("api-gateway", &["access token issued", &user]);
    let registered: Vec<String> = log_lines("gw-a", &["Connection registered", &user])
        .into_iter()
        .chain(log_lines("gw-b", &["Connection registered", &user]))
        .collect();
    evidence(
        "S6",
        json!({
            "waiting": serde_json::to_value(&waiting).unwrap(),
            "issuedWhileOffline": issued_offline,
            "connectionsWhileOffline": registered_offline,
            "tokenWhileOffline": token_offline,
            "onlineAfterMs": first_connected_after(&observed, online_at, online.access_point_id.as_deref().unwrap())
                .map(|at| at.duration_since(online_at).as_millis()),
            "issuedLog": issued, "registeredLog": registered,
            "statuses": statuses_json(&observed, started),
        }),
    );
    let _ = client.logout().await;
    assert_eq!(issued_offline, 0, "nothing is issued offline");
    assert_eq!(registered_offline, 0, "no handshake offline");
    assert!(!token_offline);
    assert_eq!(issued.len(), 1, "one token issued once online");
    assert_eq!(registered.len(), 1);
}

/// S2 对照：健康连接上背靠背发 100 条（入队之间不留间隔），不杀网关。用来区分顺序问题出在切换还是出在健康连接上。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn s2_control_order_on_a_healthy_connection() {
    init_tracing();
    let run = run_id();
    let user = format!("m1-s2c-{run}");
    let peer = format!("m1-s2c-peer-{run}");
    let client = managed_client("s2c", two_access_points(None)).await;
    login(&client, &user).await;
    let (observed, _subscriptions) = Observed::record(&client);
    wait_online(&client, Duration::from_secs(30)).await;
    let conversation = single_chat(&client, &peer).await;
    let mut sent = Vec::with_capacity(100);
    for index in 0..100 {
        // 背靠背：入队之间不留间隔。
        sent.push(send_text(&client, &conversation, &format!("s2c {run} #{index:03}")).await);
    }
    wait_until("all 100 acknowledged", Duration::from_secs(60), || async {
        observed.accepted_distinct().len() == 100
    })
    .await;
    let history = persisted_history(&conversation, sent.len()).await;
    let failed = observed.failed.lock().unwrap().clone();
    evidence(
        "S2-control",
        json!({
            "sent": sent.len(), "acceptedDistinct": observed.accepted_distinct().len(), "failed": failed,
            "serverRows": history.len(), "serverOrderEqualsSendOrder": history == sent,
            "outOfOrder": out_of_order(&history, &sent),
        }),
    );
    let _ = client.logout().await;
    assert_eq!(history, sent, "server order is the send order");
}
