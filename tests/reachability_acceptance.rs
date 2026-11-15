//! 移动端可达性的全栈验收：本机隔离的真实服务栈（`scripts/live_stack.py`，含 Push Proxy、Push Server、
//! Push Worker），离线任务落到开发用的 `outbox` 后端（推送 Redis 里的一条 Stream）。用例读出任务信封，
//! 断言合并键、处置、条数与时刻，对应设计文档第 19 节完成定义里服务端看得到的条目。
//!
//! - 「应用被杀的手机」：登记了推送令牌后断开的客户端，服务端只剩它的推送绑定；
//! - 「电脑」：一直在线、在前台的客户端。
//!
//! 全部 `#[ignore]`，地址从环境变量读：
//!
//! ```sh
//! python3 scripts/live_stack.py up
//! eval "$(python3 scripts/live_stack.py env)"
//! RUSTC_WRAPPER= cargo test -j 2 --test reachability_acceptance -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! 每个用例结束时打印一行 `EVIDENCE {json}`。杀网关、换 Push Server 参数的用例结束前把进程恢复成默认配置。

#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flare_im_core_sdk::client::config::SdkAuthConfig;
use flare_im_core_sdk::client::push_lifecycle::PushTokenInput;
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::model::notification::{
    MultiDeviceAlertPolicy, QuietHours, UpdateUserNotificationSettingsRequest,
};
use flare_im_core_sdk::prelude::*;
use flare_proto::PushTaskEnvelope;
use flare_proto::common::{AlertClass, PushDisposition};
use prost::Message as _;
use serde_json::{Value, json};

use link_lab::wait_phase;

/// 接入点：电脑与发送方走 gw-b；网关故障用例里的手机单独走 gw-a。
const PRIMARY: &str = "development";
const SECONDARY: &str = "dev-b";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} is not set: eval \"$(python3 scripts/live_stack.py env)\" first")
    })
}

fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", nanos / 1_000)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn evidence(case: &str, value: Value) {
    println!("EVIDENCE {}", json!({ "case": case, "result": value }));
}

/// `live_stack.py ctl <args>`：按栈记下的 PID 停、起服务。
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
    assert!(status.success(), "live_stack.py ctl {args:?} failed");
}

fn psql(sql: &str) -> String {
    let output = Command::new("psql")
        .args(["-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql])
        .arg(env("LIVE_SMOKE_CORE_DB"))
        .output()
        .expect("run psql");
    assert!(
        output.status.success(),
        "psql failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

// ------------------------------------------------------------------ Redis --

/// 推送 Redis 的一次应答（RESP2）。
#[derive(Debug)]
enum Resp {
    Simple(String),
    Error(String),
    Int(#[allow(dead_code)] i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    fn items(self) -> Vec<Resp> {
        match self {
            Resp::Array(Some(items)) => items,
            Resp::Array(None) => Vec::new(),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    fn bytes(self) -> Vec<u8> {
        match self {
            Resp::Bulk(Some(bytes)) => bytes,
            Resp::Simple(text) => text.into_bytes(),
            other => panic!("expected a bulk string, got {other:?}"),
        }
    }

    fn text(self) -> String {
        String::from_utf8(self.bytes()).expect("utf-8")
    }
}

fn read_resp(reader: &mut BufReader<TcpStream>) -> Resp {
    let mut line = String::new();
    reader.read_line(&mut line).expect("redis read");
    let line = line.trim_end_matches(['\r', '\n']);
    let (kind, rest) = line.split_at(1);
    match kind {
        "+" => Resp::Simple(rest.to_owned()),
        "-" => Resp::Error(rest.to_owned()),
        ":" => Resp::Int(rest.parse().expect("redis integer")),
        "$" => {
            let len: i64 = rest.parse().expect("redis bulk length");
            if len < 0 {
                return Resp::Bulk(None);
            }
            let mut buf = vec![0; len as usize + 2];
            reader.read_exact(&mut buf).expect("redis bulk");
            buf.truncate(len as usize);
            Resp::Bulk(Some(buf))
        }
        "*" => {
            let len: i64 = rest.parse().expect("redis array length");
            if len < 0 {
                return Resp::Array(None);
            }
            Resp::Array(Some((0..len).map(|_| read_resp(reader)).collect()))
        }
        other => panic!("unexpected redis reply type {other}"),
    }
}

/// 在推送 Redis（`LIVE_SMOKE_PUSH_REDIS` = `host:port/db`）上执行一条命令。
fn redis(args: &[&[u8]]) -> Resp {
    let address = env("LIVE_SMOKE_PUSH_REDIS");
    let (host, db) = address.split_once('/').expect("host:port/db");
    let stream = TcpStream::connect(host).expect("connect push redis");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    for command in [&[b"SELECT".as_slice(), db.as_bytes()][..], args] {
        let mut frame = format!("*{}\r\n", command.len()).into_bytes();
        for arg in command {
            frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            frame.extend_from_slice(arg);
            frame.extend_from_slice(b"\r\n");
        }
        writer.write_all(&frame).expect("redis write");
    }
    match read_resp(&mut reader) {
        Resp::Simple(_) => {}
        other => panic!("SELECT {db}: {other:?}"),
    }
    match read_resp(&mut reader) {
        Resp::Error(error) => panic!("redis {:?}: {error}", String::from_utf8_lossy(args[0])),
        reply => reply,
    }
}

/// 一条离线推送任务：outbox 写入时刻（Stream ID 的毫秒部分，Redis 与测试同一台机器的时钟）与信封。
#[derive(Clone)]
struct Task {
    at_ms: i64,
    envelope: PushTaskEnvelope,
}

impl Task {
    fn disposition(&self) -> PushDisposition {
        self.envelope.disposition()
    }

    fn json(&self, origin_ms: i64) -> Value {
        let e = &self.envelope;
        json!({
            "atMs": self.at_ms - origin_ms,
            "device": e.device_id,
            "disposition": format!("{:?}", e.disposition()),
            "alert": format!("{:?}", e.alert_class()),
            "collapse": e.collapse_key,
            "count": e.count,
            "badge": e.badge,
            "seq": e.conversation_seq,
            "digestConversations": e.digest_conversations,
        })
    }
}

/// 这个用户的全部离线任务，按写入顺序。
fn outbox(user: &str) -> Vec<Task> {
    let stream = env("LIVE_SMOKE_PUSH_OUTBOX");
    let entries = redis(&[b"XRANGE", stream.as_bytes(), b"-", b"+"]).items();
    let mut tasks = Vec::new();
    for entry in entries {
        let mut parts = entry.items().into_iter();
        let id = parts.next().expect("entry id").text();
        let fields = parts.next().expect("entry fields").items();
        let mut owner = None;
        let mut envelope = None;
        let mut fields = fields.into_iter();
        while let (Some(name), Some(value)) = (fields.next(), fields.next()) {
            match name.text().as_str() {
                "user_id" => owner = Some(value.text()),
                "envelope" => envelope = Some(value.bytes()),
                _ => {}
            }
        }
        if owner.as_deref() != Some(user) {
            continue;
        }
        let at_ms = id
            .split('-')
            .next()
            .and_then(|ms| ms.parse().ok())
            .expect("stream id");
        let envelope =
            PushTaskEnvelope::decode(envelope.expect("envelope").as_slice()).expect("decode");
        tasks.push(Task { at_ms, envelope });
    }
    tasks
}

fn device_tasks(user: &str, device: &str) -> Vec<Task> {
    outbox(user)
        .into_iter()
        .filter(|task| task.envelope.device_id == device)
        .collect()
}

/// 等到这台设备有满足 `accept` 的任务，返回它；超时返回 None。
async fn wait_task(
    user: &str,
    device: &str,
    deadline: Duration,
    accept: impl Fn(&Task) -> bool,
) -> Option<Task> {
    let started = Instant::now();
    loop {
        if let Some(task) = device_tasks(user, device).into_iter().find(|t| accept(t)) {
            return Some(task);
        }
        if started.elapsed() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 决策链：这个用户这条消息（`trace_key` = 服务端消息 ID）的阶段名，按记录顺序。
fn trace_stages(user: &str, trace_key: &str) -> Vec<String> {
    let tenant = env("LIVE_SMOKE_TENANT");
    let key = format!("push:trace:{tenant}:{user}:{trace_key}");
    redis(&[b"LRANGE", key.as_bytes(), b"0", b"-1"])
        .items()
        .into_iter()
        .map(|entry| {
            let value: Value = serde_json::from_slice(&entry.bytes()).expect("trace json");
            value["st"].as_str().unwrap_or_default().to_owned()
        })
        .collect()
}

// --------------------------------------------------------------- clients --

fn access(ws: String) -> AccessConfig {
    AccessConfig::development(DevelopmentEndpoints {
        ws_url: Some(ws),
        core_api_url: Some(env("LIVE_SMOKE_CORE_API")),
        ..Default::default()
    })
}

fn data_root(case: &str, device: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("flare-reachability-acceptance")
        .join(format!("{case}-{device}-{}", run_id()))
}

/// SDK 托管令牌的客户端，已登录、已在线。
async fn online(case: &str, user: &str, device: &str, ap: &str) -> IMClient {
    let ws = if ap == PRIMARY {
        env("LIVE_SMOKE_WS_A")
    } else {
        env("LIVE_SMOKE_WS_B")
    };
    online_at(case, user, device, ws).await
}

async fn online_at(case: &str, user: &str, device: &str, ws: String) -> IMClient {
    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(data_root(case, device).display().to_string()),
                access: Some(access(ws)),
                tenant_id: Some(env("LIVE_SMOKE_TENANT")),
                device_id: Some(device.to_owned()),
                auth: Some(SdkAuthConfig::default()),
                ack_timeout_secs: Some(10),
                ..Default::default()
            }),
        )
        .await
        .expect("init");
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
    wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(30)).await;
    client
}

/// 登记一个 Android FCM 推送令牌（测试值，outbox 后端不会真的发给厂商），等到服务端确认。
async fn register_push(client: &IMClient) {
    client.set_push_permission(true).await.expect("permission");
    let input = PushTokenInput {
        provider: "fcm".into(),
        platform: "android".into(),
        token: format!("acceptance-token-{}", run_id()),
        app_bundle: Some("im.flare.acceptance".into()),
        environment: None,
        locale: Some("zh-CN".into()),
        time_zone: Some("Asia/Shanghai".into()),
        app_version: Some("1.0.0".into()),
        capabilities: Some(vec!["dataMessages".into()]),
    };
    let mut status = client.update_push_token(input).await.expect("push token");
    let started = Instant::now();
    while status.state != "registered" {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "push binding not registered: {} {:?}",
            status.state,
            status.last_error
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        status = client.reconcile_push().await.expect("reconcile push");
    }
}

/// 只绑 127.0.0.1 的可开关 TCP 中继：关上时切断现有连接并拒绝新连接，打开后照常转发。
/// 手机经它连网关；关上等于应用进程被杀（套接字被系统关掉，客户端会话与推送绑定都还在）。
struct Switch {
    address: String,
    open: Arc<AtomicBool>,
    connections: Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    accept_loop: tokio::task::JoinHandle<()>,
}

impl Switch {
    async fn start(target: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind switch");
        let address = listener.local_addr().expect("switch address").to_string();
        let open = Arc::new(AtomicBool::new(true));
        let connections = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let accept_loop = tokio::spawn({
            let open = open.clone();
            let connections = connections.clone();
            async move {
                while let Ok((mut client, _)) = listener.accept().await {
                    if !open.load(Ordering::SeqCst) {
                        drop(client);
                        continue;
                    }
                    let Ok(mut server) = tokio::net::TcpStream::connect(&target).await else {
                        continue;
                    };
                    let pump = tokio::spawn(async move {
                        let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                    });
                    connections.lock().await.push(pump);
                }
            }
        });
        Self {
            address,
            open,
            connections,
            accept_loop,
        }
    }

    fn ws_url(&self) -> String {
        format!("ws://{}", self.address)
    }

    async fn close(&self) {
        self.open.store(false, Ordering::SeqCst);
        for pump in self.connections.lock().await.drain(..) {
            pump.abort();
        }
    }

    fn reopen(&self) {
        self.open.store(true, Ordering::SeqCst);
    }
}

impl Drop for Switch {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// 一台装着应用的手机：客户端经中继连 gw-a，登记了推送令牌。
struct Phone {
    client: IMClient,
    switch: Switch,
}

/// 手机上线并登记推送令牌（应用还活着）。返回设备 ID 与手机。
async fn phone(case: &str, user: &str) -> (String, Phone) {
    let device = format!("phone-{}", run_id());
    let target = env("LIVE_SMOKE_WS_A")
        .trim_start_matches("ws://")
        .to_owned();
    let switch = Switch::start(target).await;
    let client = online_at(case, user, &device, switch.ws_url()).await;
    register_push(&client).await;
    (device, Phone { client, switch })
}

/// 应用被杀的手机：上线、登记推送令牌后进程被杀（中继关上），服务端只剩推送绑定。
async fn killed_phone(case: &str, user: &str) -> (String, Phone) {
    let (device, phone) = phone(case, user).await;
    phone.switch.close().await;
    // 让 Online 记下这台设备离线。
    tokio::time::sleep(Duration::from_secs(1)).await;
    (device, phone)
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

async fn send(client: &IMClient, conversation: &str, text: &str, mentions: &[String]) -> String {
    let draft = client
        .message_build()
        .expect("builder")
        .create_text(conversation, text, false, mentions)
        .await
        .expect("build text");
    let id = draft.client_msg_id.clone();
    client
        .message()
        .expect("message api")
        .send(draft)
        .await
        .expect("send");
    id
}

/// 服务端落库后的 (server_id, seq)。
async fn persisted(client_msg_id: &str) -> (String, u64) {
    let tenant = env("LIVE_SMOKE_TENANT");
    let started = Instant::now();
    loop {
        let row = psql(&format!(
            "SELECT server_id || '|' || seq FROM messages WHERE tenant_id = '{tenant}' AND client_msg_id = '{client_msg_id}'"
        ));
        if let Some((id, seq)) = row.split_once('|') {
            return (id.to_owned(), seq.parse().expect("seq"));
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{client_msg_id} not persisted"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 等到这个客户端本地有了这条 seq 的消息（用户在电脑上看得到它）。
async fn wait_local(client: &IMClient, conversation: &str, seq: u64) {
    let api = client.message().expect("message api");
    let started = Instant::now();
    loop {
        let messages = api.list(conversation, 0, 50).await.unwrap_or_default();
        if messages.iter().any(|m| m.conversation_seq >= seq) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "seq {seq} never reached the client"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn read(client: &IMClient, conversation: &str, seq: u64) {
    client
        .conversation()
        .expect("conversation api")
        .mark_read(conversation, seq)
        .await
        .expect("read");
}

async fn set_policy(client: &IMClient, policy: MultiDeviceAlertPolicy) {
    client
        .update_notification_settings(UpdateUserNotificationSettingsRequest {
            multi_device: Some(policy),
            ..Default::default()
        })
        .await
        .expect("notification settings");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()))
        .with_test_writer()
        .try_init();
}

fn loud(task: &Task) -> bool {
    task.disposition() == PushDisposition::Loud
}

// ------------------------------------------------------------------ cases --

/// 完成定义 1（前半）与 13：手机应用被杀、别的设备都不在线，单聊消息 3 秒内成为一条响铃任务；
/// 决策链按消息查得到路由、决策、闸门与任务四步，设备报的到达回执也记在同一条链上。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r01_killed_phone_rings_within_three_seconds_and_the_chain_is_traceable() {
    init_tracing();
    let case = "r01";
    let (a, b) = (format!("r01a-{}", run_id()), format!("r01b-{}", run_id()));
    let (phone, _app) = killed_phone(case, &b).await;
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &b).await;

    let sent_at = now_ms();
    let local_id = send(&sender, &conversation, "r01 hello", &[]).await;
    let task = wait_task(&b, &phone, Duration::from_secs(10), loud)
        .await
        .expect("a loud task for the killed phone");
    let latency = task.at_ms - sent_at;
    let (server_id, seq) = persisted(&local_id).await;

    // 到达回执：设备（令牌绑定这台手机）报给 API 网关，进决策链。
    let token = dev_token(&b, &phone).await;
    let receipt: Value = http_client()
        .post(format!(
            "{}/api/v1/push/receipts",
            env("LIVE_SMOKE_CORE_API")
        ))
        .bearer_auth(token)
        .json(&json!({
            "deviceId": phone,
            "appVersion": "1.0.0",
            "receipts": [{
                "conversationId": conversation,
                "messageId": server_id,
                "seq": seq,
                "collapseId": task.envelope.collapse_key,
                "disposition": "loud",
                "provider": "fcm",
                "path": "app",
                "receivedAtMs": now_ms(),
            }],
        }))
        .send()
        .await
        .expect("receipt request")
        .json()
        .await
        .expect("receipt response");
    tokio::time::sleep(Duration::from_millis(800)).await;
    let stages = trace_stages(&b, &server_id);

    evidence(
        case,
        json!({
            "latencyMs": latency,
            "task": task.json(sent_at),
            "seq": seq,
            "receipt": receipt,
            "traceStages": stages,
        }),
    );
    assert!(latency <= 3_000, "loud task after {latency} ms");
    assert_eq!(task.envelope.conversation_seq, seq);
    assert!(task.envelope.collapse_key.starts_with("c:"));
    assert_eq!(task.envelope.count.max(1), 1);
    for stage in ["route", "decide", "gate", "task", "receipt"] {
        assert!(
            stages.iter().any(|s| s == stage),
            "trace has no {stage}: {stages:?}"
        );
    }
}

/// 完成定义 1（后半）：电脑在线时按多端策略。默认（延迟）= 15 秒内在电脑上读了手机不响，没读则窗口后响；
/// 立即 = 3 秒内响。推送服务端把用户设置缓存 60 秒（改了最多一分钟后在推送上生效），立即策略用另一个
/// 用户在第一条消息之前设好。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r02_desktop_in_use_follows_the_multi_device_policy() {
    init_tracing();
    let case = "r02";
    let (a, b, c) = (
        format!("r02a-{}", run_id()),
        format!("r02b-{}", run_id()),
        format!("r02c-{}", run_id()),
    );
    let (phone, _app) = killed_phone(case, &b).await;
    let desktop = online(case, &b, &format!("pc-{}", run_id()), SECONDARY).await;
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &b).await;

    // 默认策略，电脑上没读：窗口（约 15 秒）后响。
    let unread_at = now_ms();
    let unread_id = send(&sender, &conversation, "r02 unread", &[]).await;
    let (_, unread_seq) = persisted(&unread_id).await;
    let delayed = wait_task(&b, &phone, Duration::from_secs(25), |t| {
        loud(t) && t.envelope.conversation_seq == unread_seq
    })
    .await;
    let delayed_ms = delayed.as_ref().map(|t| t.at_ms - unread_at);

    // 默认策略，电脑上收到后 2 秒读了：手机不响。
    wait_local(&desktop, &conversation, unread_seq).await;
    read(&desktop, &conversation, unread_seq).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let read_at = now_ms();
    let read_id = send(&sender, &conversation, "r02 read on desktop", &[]).await;
    let (_, read_seq) = persisted(&read_id).await;
    wait_local(&desktop, &conversation, read_seq).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    read(&desktop, &conversation, read_seq).await;
    tokio::time::sleep(Duration::from_secs(20)).await;
    let after_read: Vec<Task> = device_tasks(&b, &phone)
        .into_iter()
        .filter(|t| t.at_ms >= read_at)
        .collect();

    // 立即：另一个用户，第一条消息之前设好。
    let (c_phone, _c_app) = killed_phone(case, &c).await;
    let c_desktop = online(case, &c, &format!("pc-{}", run_id()), SECONDARY).await;
    set_policy(&c_desktop, MultiDeviceAlertPolicy::Immediate).await;
    let c_conversation = single_chat(&sender, &c).await;
    let immediate_at = now_ms();
    let immediate_id = send(&sender, &c_conversation, "r02 immediate", &[]).await;
    let (_, immediate_seq) = persisted(&immediate_id).await;
    let immediate = wait_task(&c, &c_phone, Duration::from_secs(10), |t| {
        loud(t) && t.envelope.conversation_seq == immediate_seq
    })
    .await;
    let immediate_ms = immediate.as_ref().map(|t| t.at_ms - immediate_at);

    evidence(
        case,
        json!({
            "delayedLoudAfterMs": delayed_ms,
            "afterReadTasks": after_read.iter().map(|t| t.json(read_at)).collect::<Vec<_>>(),
            "immediateLoudAfterMs": immediate_ms,
        }),
    );
    let delayed_ms = delayed_ms.expect("a delayed loud task when the desktop does not read");
    assert!(
        (12_000..=20_000).contains(&delayed_ms),
        "default policy rang after {delayed_ms} ms"
    );
    assert!(
        !after_read.iter().any(loud),
        "the phone rang although the desktop read the conversation"
    );
    let immediate_ms = immediate_ms.expect("a loud task under the immediate policy");
    assert!(
        immediate_ms <= 3_000,
        "immediate policy rang after {immediate_ms} ms"
    );
}

/// 完成定义 4：强制结束手机所在的网关进程，网关失联后的窗口期消息 30 秒内补提示。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r04_killed_gateway_catch_up_within_thirty_seconds() {
    init_tracing();
    let case = "r04";
    let (a, b) = (format!("r04a-{}", run_id()), format!("r04b-{}", run_id()));
    let device = format!("phone-{}", run_id());
    // 手机只认 gw-a；发送方在 gw-b。
    let phone = online(case, &b, &device, PRIMARY).await;
    register_push(&phone).await;
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &b).await;

    let killed_at = now_ms();
    ctl(&["kill", "gw-a"]);
    // 网关一死，手机的链路断了也连不上别处；窗口期里来的消息只能靠推送补。
    let window_id = send(&sender, &conversation, "r04 in the window", &[]).await;
    let (_, window_seq) = persisted(&window_id).await;
    let task = wait_task(&b, &device, Duration::from_secs(40), |t| {
        t.envelope.conversation_seq >= window_seq
            && matches!(
                t.disposition(),
                PushDisposition::Loud | PushDisposition::Quiet | PushDisposition::Digest
            )
    })
    .await;
    let tasks: Vec<Value> = device_tasks(&b, &device)
        .iter()
        .map(|t| t.json(killed_at))
        .collect();
    drop(phone);
    ctl(&["start", "gw-a"]);

    evidence(case, json!({ "windowSeq": window_seq, "tasks": tasks }));
    let task = task.expect("a catch-up task for the device on the killed gateway");
    let after = task.at_ms - killed_at;
    assert!(after <= 30_000, "catch-up after {after} ms");
}

/// 完成定义 5：单会话持续刷屏，响铃按阶梯稀疏、厂商调用有上界、合并键唯一、最后一条的计数正确。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r05_one_conversation_flood_is_bounded() {
    init_tracing();
    let case = "r05";
    let (a, b) = (format!("r05a-{}", run_id()), format!("r05b-{}", run_id()));
    let (phone, _app) = killed_phone(case, &b).await;
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &b).await;

    let started = now_ms();
    let mut last = String::new();
    let total = 150;
    for i in 0..total {
        last = send(&sender, &conversation, &format!("r05 flood {i}"), &[]).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    let (_, last_seq) = persisted(&last).await;
    // 尾部刷新落定：静默阶梯 2s、5s、15s、60s、300s 升到顶档后，最后一批只计数，到期（最近一次
    // 推送后 300 秒）发一条带最终计数的静默更新。
    let settled = wait_task(&b, &phone, Duration::from_secs(400), |t| {
        t.envelope.conversation_seq == last_seq
    })
    .await;
    let window_ms = now_ms() - started;
    let tasks = device_tasks(&b, &phone);
    let louds = tasks.iter().filter(|t| loud(t)).count();
    let keys: std::collections::BTreeSet<&str> = tasks
        .iter()
        .map(|t| t.envelope.collapse_key.as_str())
        .collect();
    let max_per_minute = per_minute_max(&tasks, |_| true);

    evidence(
        case,
        json!({
            "messages": total,
            "windowMs": window_ms,
            "tasks": tasks.len(),
            "loud": louds,
            "maxTasksPerMinute": max_per_minute,
            "collapseKeys": keys.len(),
            "last": settled.as_ref().map(|t| t.json(started)),
        }),
    );
    let settled = settled.expect("the newest message is represented in the end");
    // 单聊响铃阶梯 10s、30s、120s、300s：一分多钟里至多 3 次。
    assert!(louds <= 3, "{louds} rings in {window_ms} ms");
    assert!(
        max_per_minute <= 30,
        "{max_per_minute} vendor calls in a minute"
    );
    assert_eq!(keys.len(), 1, "one notification per conversation");
    assert_eq!(settled.envelope.count as usize, total, "final count");
}

/// 一分钟滑动窗口里满足条件的任务最多几条。
fn per_minute_max(tasks: &[Task], accept: impl Fn(&Task) -> bool) -> usize {
    let times: Vec<i64> = tasks
        .iter()
        .filter(|t| accept(t))
        .map(|t| t.at_ms)
        .collect();
    times
        .iter()
        .map(|start| {
            times
                .iter()
                .filter(|at| **at >= *start && **at < start + 60_000)
                .count()
        })
        .max()
        .unwrap_or(0)
}

/// 完成定义 6：同一时间多个会话活跃，进入摘要模式，之后每分钟至多响一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r06_many_active_conversations_switch_to_digest() {
    init_tracing();
    let case = "r06";
    let b = format!("r06b-{}", run_id());
    let (phone, _app) = killed_phone(case, &b).await;
    let mut senders = Vec::new();
    for i in 0..8 {
        let user = format!("r06s{i}-{}", run_id());
        let client = online(case, &user, &format!("pc-{}", run_id()), SECONDARY).await;
        let conversation = single_chat(&client, &b).await;
        senders.push((client, conversation));
    }
    let started = now_ms();
    // 摘要模式的第一次响铃在进入后满一个摘要间隔（60 秒）才有：活跃要持续过这段。
    for round in 0..9 {
        for (client, conversation) in &senders {
            send(client, conversation, &format!("r06 round {round}"), &[]).await;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    tokio::time::sleep(Duration::from_secs(15)).await;
    let tasks = device_tasks(&b, &phone);
    let digest_at = tasks
        .iter()
        .find(|t| t.disposition() == PushDisposition::Digest)
        .map(|t| t.at_ms);
    let rings_after_digest = digest_at
        .map(|from| {
            per_minute_max(&tasks, |t| {
                t.at_ms >= from
                    && matches!(
                        t.disposition(),
                        PushDisposition::Loud | PushDisposition::Digest
                    )
            })
        })
        .unwrap_or(usize::MAX);

    evidence(
        case,
        json!({
            "conversations": senders.len(),
            "tasks": tasks.iter().map(|t| t.json(started)).collect::<Vec<_>>(),
            "digestAtMs": digest_at.map(|at| at - started),
            "maxRingsPerMinuteAfterDigest": rings_after_digest,
        }),
    );
    assert!(digest_at.is_some(), "no digest task");
    assert!(
        rings_after_digest <= 1,
        "{rings_after_digest} rings in a minute after entering digest mode"
    );
}

/// 完成定义 7、8：撤回最新一条，同一合并键的通知被替换且不响铃；手机上展示着通知时在电脑上读完，5 秒内给手机
/// 一条清除。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r07_read_elsewhere_clears_and_recall_replaces_quietly() {
    init_tracing();
    let case = "r07";
    let (a, b, c) = (
        format!("r07a-{}", run_id()),
        format!("r07b-{}", run_id()),
        format!("r07c-{}", run_id()),
    );
    let (phone, _app) = killed_phone(case, &b).await;
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &b).await;

    // 撤回：最新一条响过以后撤回。
    let recall_id = send(&sender, &conversation, "r07 to be recalled", &[]).await;
    let (_, recall_seq) = persisted(&recall_id).await;
    let rang = wait_task(&b, &phone, Duration::from_secs(10), |t| {
        loud(t) && t.envelope.conversation_seq == recall_seq
    })
    .await
    .expect("the message rang first");
    let recalled_at = now_ms();
    sender
        .message()
        .expect("message api")
        .recall(&recall_id)
        .await
        .expect("recall");
    let replaced = wait_task(&b, &phone, Duration::from_secs(10), |t| {
        t.disposition() == PushDisposition::RecallReplace
    })
    .await;
    let after_recall: Vec<Task> = device_tasks(&b, &phone)
        .into_iter()
        .filter(|t| t.at_ms >= recalled_at)
        .collect();

    // 读：另一个用户，手机先收到通知（电脑不在线），然后电脑上线读完。
    let (c_phone, _c_app) = killed_phone(case, &c).await;
    let c_conversation = single_chat(&sender, &c).await;
    let shown_id = send(&sender, &c_conversation, "r07 read me", &[]).await;
    let (_, shown_seq) = persisted(&shown_id).await;
    let shown = wait_task(&c, &c_phone, Duration::from_secs(10), |t| {
        t.envelope.conversation_seq == shown_seq
            && matches!(
                t.disposition(),
                PushDisposition::Loud | PushDisposition::Quiet
            )
    })
    .await
    .expect("the phone shows the message first");
    let desktop = online(case, &c, &format!("pc-{}", run_id()), SECONDARY).await;
    wait_local(&desktop, &c_conversation, shown_seq).await;
    let read_at = now_ms();
    read(&desktop, &c_conversation, shown_seq).await;
    // 电脑上线时同步上报的已读也算「在别处读了」：清除可能在显式已读之前就到。
    let cleared = wait_task(&c, &c_phone, Duration::from_secs(10), |t| {
        t.at_ms > shown.at_ms
            && matches!(
                t.disposition(),
                PushDisposition::Clear | PushDisposition::BadgeOnly
            )
    })
    .await;

    evidence(
        case,
        json!({
            "rang": rang.json(recalled_at),
            "afterRecall": after_recall.iter().map(|t| t.json(recalled_at)).collect::<Vec<_>>(),
            "shown": shown.json(read_at),
            "cleared": cleared.as_ref().map(|t| t.json(read_at)),
        }),
    );
    let replaced = replaced.expect("a recall replacement task");
    assert_eq!(replaced.envelope.collapse_key, rang.envelope.collapse_key);
    // 替换的声音由处置决定：RECALL_REPLACE 在各厂商上都是被动、无声的覆盖。撤回之后不能再有响铃。
    assert!(
        !after_recall.iter().any(loud),
        "a recall must not ring the phone again"
    );
    let cleared = cleared.expect("a clear task after reading elsewhere");
    assert_eq!(cleared.envelope.collapse_key, shown.envelope.collapse_key);
    assert!(
        cleared.at_ms - read_at <= 5_000,
        "clear after {} ms",
        cleared.at_ms - read_at
    );
}

/// 完成定义 9：勿扰时段里普通消息只静默出现；允许点名时 @ 照常响。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r09_quiet_hours_deliver_quietly_except_mentions() {
    init_tracing();
    let case = "r09";
    let (a, b, c) = (
        format!("r09a-{}", run_id()),
        format!("r09b-{}", run_id()),
        format!("r09c-{}", run_id()),
    );
    let (phone, _app) = killed_phone(case, &b).await;
    let settings = online(case, &b, &format!("pc-{}", run_id()), SECONDARY).await;
    settings
        .update_notification_settings(UpdateUserNotificationSettingsRequest {
            // 起止相同 = 全天。
            quiet_hours: Some(QuietHours {
                start_minute: 0,
                end_minute: 0,
                weekdays: 0,
                allow_mentions: true,
            }),
            multi_device: Some(MultiDeviceAlertPolicy::Immediate),
            ..Default::default()
        })
        .await
        .expect("quiet hours");
    settings.disconnect().await.expect("disconnect");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let direct = single_chat(&sender, &b).await;
    let started = now_ms();
    let normal_id = send(&sender, &direct, "r09 at night", &[]).await;
    let (_, normal_seq) = persisted(&normal_id).await;
    let normal = wait_task(&b, &phone, Duration::from_secs(10), |t| {
        t.envelope.conversation_seq == normal_seq
    })
    .await;

    let third = online(case, &c, &format!("pc-{}", run_id()), SECONDARY).await;
    drop(third);
    let group = sender
        .conversation()
        .expect("conversation api")
        .get_group_by_user_ids(&[a.clone(), b.clone(), c.clone()], Some("r09 group"))
        .await
        .expect("group")
        .conversation_id;
    let mention_id = send(&sender, &group, &format!("r09 @{b}"), &[b.clone()]).await;
    let (_, mention_seq) = persisted(&mention_id).await;
    let mention = wait_task(&b, &phone, Duration::from_secs(10), |t| {
        t.envelope.conversation_seq == mention_seq && t.envelope.conversation_id == group
    })
    .await;

    evidence(
        case,
        json!({
            "normal": normal.as_ref().map(|t| t.json(started)),
            "mention": mention.as_ref().map(|t| t.json(started)),
        }),
    );
    let normal = normal.expect("a task inside quiet hours");
    assert_eq!(
        normal.disposition(),
        PushDisposition::Quiet,
        "quiet hours must not ring"
    );
    let mention = mention.expect("a task for the mention");
    assert_eq!(
        mention.disposition(),
        PushDisposition::Loud,
        "allowed mentions ring"
    );
    assert_eq!(mention.envelope.alert_class(), AlertClass::Mention);
}

/// 完成定义 10：成员数超过上限的群（上限调到 2 模拟千人群）只给订阅者与被 @ 的人推。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r10_large_group_pushes_only_subscribers_and_mentions() {
    init_tracing();
    let case = "r10";
    ctl(&[
        "restart",
        "push-server",
        "--env",
        "PUSH_SERVER_OFFLINE_FANOUT_MEMBER_CAP=2",
    ]);
    let (a, b, c, d) = (
        format!("r10a-{}", run_id()),
        format!("r10b-{}", run_id()),
        format!("r10c-{}", run_id()),
        format!("r10d-{}", run_id()),
    );
    let (phone_b, _b_app) = killed_phone(case, &b).await;
    let (phone_d, _d_app) = killed_phone(case, &d).await;
    // C 订阅这个群的全部消息，然后应用被杀。
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let group = sender
        .conversation()
        .expect("conversation api")
        .get_group_by_user_ids(
            &[a.clone(), b.clone(), c.clone(), d.clone()],
            Some("r10 group"),
        )
        .await
        .expect("group")
        .conversation_id;
    send(&sender, &group, "r10 hello", &[]).await;
    let (c_device, c_phone) = phone(case, &c).await;
    c_phone
        .client
        .conversation()
        .expect("conversation api")
        .set_notify_all(&group, true)
        .await
        .expect("notify all");
    c_phone.switch.close().await;
    // 大群的接收者（订阅者名单）在推送服务端缓存 15 秒：订阅变化等过这段再生效。
    tokio::time::sleep(Duration::from_secs(16)).await;

    let started = now_ms();
    let normal_id = send(&sender, &group, "r10 normal", &[]).await;
    let (_, normal_seq) = persisted(&normal_id).await;
    let to_subscriber = wait_task(&c, &c_device, Duration::from_secs(15), |t| {
        t.envelope.conversation_seq == normal_seq
    })
    .await;
    let mention_id = send(&sender, &group, &format!("r10 @{b}"), &[b.clone()]).await;
    let (_, mention_seq) = persisted(&mention_id).await;
    let to_mentioned = wait_task(&b, &phone_b, Duration::from_secs(15), |t| {
        t.envelope.conversation_seq == mention_seq
    })
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let b_normal: Vec<Value> = device_tasks(&b, &phone_b)
        .iter()
        .filter(|t| t.envelope.conversation_seq == normal_seq)
        .map(|t| t.json(started))
        .collect();
    let d_tasks: Vec<Value> = device_tasks(&d, &phone_d)
        .iter()
        .filter(|t| t.at_ms >= started)
        .map(|t| t.json(started))
        .collect();
    ctl(&["restart", "push-server"]);

    evidence(
        case,
        json!({
            "subscriber": to_subscriber.as_ref().map(|t| t.json(started)),
            "mentioned": to_mentioned.as_ref().map(|t| t.json(started)),
            "nonSubscriberNormal": b_normal,
            "bystander": d_tasks,
        }),
    );
    assert!(to_subscriber.is_some(), "the subscriber got no push");
    assert!(to_mentioned.is_some(), "the mentioned member got no push");
    assert!(
        b_normal.is_empty(),
        "a non-subscriber got the normal message"
    );
    assert!(d_tasks.is_empty(), "a bystander got a push");
}

/// 完成定义 11：登出、被踢下线之后，这台设备的推送数为零。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r11_logged_out_or_kicked_device_gets_nothing() {
    init_tracing();
    let case = "r11";
    let (a, b, c) = (
        format!("r11a-{}", run_id()),
        format!("r11b-{}", run_id()),
        format!("r11c-{}", run_id()),
    );
    // B 登出。
    let b_device = format!("phone-{}", run_id());
    let b_phone = online(case, &b, &b_device, SECONDARY).await;
    register_push(&b_phone).await;
    b_phone.logout().await.expect("logout");
    // C 被管理端踢下线。
    let c_device = format!("phone-{}", run_id());
    let c_phone = online(case, &c, &c_device, SECONDARY).await;
    register_push(&c_phone).await;
    let token = dev_token(&c, &c_device).await;
    let kicked = http_client()
        .post(format!(
            "{}/api/v1/presence/devices/{c_device}/kick",
            env("LIVE_SMOKE_CORE_API")
        ))
        .bearer_auth(token)
        .json(&json!({ "reason": "acceptance" }))
        .send()
        .await
        .expect("kick request")
        .status()
        .as_u16();
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(c_phone);

    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let started = now_ms();
    for peer in [&b, &c] {
        let conversation = single_chat(&sender, peer).await;
        send(&sender, &conversation, "r11 after sign-out", &[]).await;
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    let b_tasks = device_tasks(&b, &b_device);
    let c_tasks: Vec<Task> = device_tasks(&c, &c_device)
        .into_iter()
        .filter(|t| t.at_ms >= started)
        .collect();

    evidence(
        case,
        json!({
            "kickStatus": kicked,
            "loggedOut": b_tasks.iter().map(|t| t.json(started)).collect::<Vec<_>>(),
            "kicked": c_tasks.iter().map(|t| t.json(started)).collect::<Vec<_>>(),
        }),
    );
    assert!(b_tasks.is_empty(), "a logged-out device got a push");
    assert!(c_tasks.is_empty(), "a kicked device got a push");
}

/// 完成定义 12：应用被杀期间来的消息，回到前台后本地按 seq 连续。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the live stack (scripts/live_stack.py)"]
async fn r12_back_in_foreground_local_history_is_contiguous() {
    init_tracing();
    let case = "r12";
    let (a, b) = (format!("r12a-{}", run_id()), format!("r12b-{}", run_id()));
    let (_, app) = phone(case, &b).await;
    let sender = online(case, &a, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &b).await;
    send(&sender, &conversation, "r12 before", &[]).await;
    app.switch.close().await;
    let mut last = String::new();
    for i in 0..40 {
        last = send(&sender, &conversation, &format!("r12 while away {i}"), &[]).await;
    }
    let (_, last_seq) = persisted(&last).await;
    app.switch.reopen();
    app.client.reconnect_now().await.expect("reconnect");
    wait_phase(
        &app.client,
        ConnectionPhase::Online,
        Duration::from_secs(60),
    )
    .await;

    let api = app.client.message().expect("message api");
    let started = Instant::now();
    let seqs = loop {
        let messages = api
            .list(&conversation, 0, 200)
            .await
            .expect("local messages");
        let mut seqs: Vec<u64> = messages
            .iter()
            .map(|m| m.conversation_seq)
            .filter(|s| *s > 0)
            .collect();
        seqs.sort_unstable();
        seqs.dedup();
        if seqs.last() == Some(&last_seq) || started.elapsed() > Duration::from_secs(30) {
            break seqs;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    let gaps: Vec<(u64, u64)> = seqs
        .windows(2)
        .filter(|w| w[1] != w[0] + 1)
        .map(|w| (w[0], w[1]))
        .collect();

    evidence(
        case,
        json!({ "localSeqs": seqs.len(), "lastSeq": last_seq, "gaps": gaps }),
    );
    assert_eq!(seqs.last(), Some(&last_seq), "not caught up");
    assert!(gaps.is_empty(), "gaps in local history: {gaps:?}");
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

/// API 网关开发签发的、绑定到这台设备的访问令牌。
async fn dev_token(user: &str, device: &str) -> String {
    let response: Value = http_client()
        .post(format!("{}/api/v1/auth/tokens", env("LIVE_SMOKE_CORE_API")))
        .json(&json!({ "userId": user, "tenantId": env("LIVE_SMOKE_TENANT"), "deviceId": device }))
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

/// 手动驱动（模拟器或真机验收用）：一个新的发送方给 `REACH_TARGET_USER` 发 `REACH_SEND_COUNT`（缺省 1）条
/// 单聊消息，间隔 `REACH_SEND_GAP_MS`（缺省 500），然后打印这个账号在 outbox 里的推送任务。设备上装着的应用
/// 用这个账号登录、登记推送令牌后切到后台或被杀，就能看到服务端为它产生了什么。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual driver for device checks; needs the live stack and REACH_TARGET_USER"]
async fn manual_send_to_the_device_under_test() {
    init_tracing();
    let target = env("REACH_TARGET_USER");
    let count: usize = std::env::var("REACH_SEND_COUNT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let gap = Duration::from_millis(
        std::env::var("REACH_SEND_GAP_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(500),
    );
    let sender_id = format!("sim-sender-{}", run_id());
    let sender = online("manual", &sender_id, &format!("pc-{}", run_id()), SECONDARY).await;
    let conversation = single_chat(&sender, &target).await;
    let started = now_ms();
    for i in 0..count {
        send(&sender, &conversation, &format!("hello {}", i + 1), &[]).await;
        tokio::time::sleep(gap).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    evidence(
        "manual",
        json!({
            "sender": sender_id,
            "conversation": conversation,
            "tasks": outbox(&target).iter().map(|t| t.json(started)).collect::<Vec<_>>(),
        }),
    );
}
