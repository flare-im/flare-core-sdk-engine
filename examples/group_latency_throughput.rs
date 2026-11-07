use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flare_im_core_sdk::SdkConfigOverlay;
use flare_im_core_sdk::content::message_elem::Elem;
use flare_im_core_sdk::event::Subscription;
use flare_im_core_sdk::model::message::{SendAck, SendAckDurability, send_ack};
use flare_im_core_sdk::prelude::*;
use futures::StreamExt;
use tokio::sync::mpsc;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn env_users() -> Vec<String> {
    if let Ok(raw) = env::var("FLARE_GROUP_E2E_USERS") {
        let users = raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if users.len() >= 2 {
            return users;
        }
    }
    (10..20).map(|value| value.to_string()).collect()
}

fn run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("group-load-{millis}")
}

fn data_root_for_run(run: &str) -> PathBuf {
    env::var("FLARE_GROUP_E2E_DATA_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            env::temp_dir()
                .join("flare-core-sdk-engine")
                .join("group-latency-throughput")
                .join(run)
        })
}

fn default_token_secret_path() -> Option<String> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.parent()?;
    Some(
        root.join("flare-core-server")
            .join("logs")
            .join(".dev-token-secret")
            .to_string_lossy()
            .to_string(),
    )
}

fn token_secret() -> std::result::Result<String, Box<dyn std::error::Error>> {
    if let Ok(secret) = env::var("TOKEN_SECRET")
        .or_else(|_| env::var("ACCESS_GATEWAY_TOKEN_SECRET"))
        .or_else(|_| env::var("FLARE_CORE_GATEWAY_TOKEN_SECRET"))
    {
        let trimmed = secret.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    if let Some(path) = default_token_secret_path()
        && let Ok(secret) = std::fs::read_to_string(&path)
    {
        let trimmed = secret.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    Err("missing TOKEN_SECRET / ACCESS_GATEWAY_TOKEN_SECRET and flare-core-server/logs/.dev-token-secret".into())
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((sorted.len() - 1) * numerator) / denominator;
    sorted[index]
}

fn fmt_duration(duration: Duration) -> String {
    format!("{:.3} ms", duration.as_secs_f64() * 1000.0)
}

fn fmt_rate(count: usize, duration: Duration) -> String {
    if duration.is_zero() {
        return "inf msg/s".to_string();
    }
    format!("{:.2} msg/s", count as f64 / duration.as_secs_f64())
}

fn ack_label(ack: &SendAck) -> String {
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => {
            format!(
                "accepted:{:?}:seq={}:server={}",
                accepted.durability(),
                accepted.conversation_seq,
                accepted.server_msg_id
            )
        }
        Some(send_ack::Result::Error(error)) => {
            format!("error:{}:{}:{}", error.code, error.reason, error.message)
        }
        None => "missing-result".to_string(),
    }
}

fn ack_result_label(ack: &SendAck) -> String {
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => {
            format!("accepted:{:?}", accepted.durability())
        }
        Some(send_ack::Result::Error(error)) => format!("error:{}", error.code),
        None => "missing-result".to_string(),
    }
}

fn is_durable_ack(ack: &SendAck) -> bool {
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => {
            matches!(accepted.durability(), SendAckDurability::Persisted)
        }
        _ => false,
    }
}

fn message_text(message: &IMMessage) -> Option<String> {
    if let Some(Elem::Text(text)) = message.content.as_ref()
        && !text.text.is_empty()
    {
        return Some(text.text.clone());
    }
    if !message.text_preview.is_empty() {
        return Some(message.text_preview.clone());
    }
    None
}

struct LoadClient {
    user_id: String,
    client: IMClient,
    apis: ConnectedApis,
    _message_subscription: Subscription,
    _send_ack_subscription: Subscription,
    _send_failed_subscription: Subscription,
    _connection_subscriptions: Vec<Subscription>,
}

struct PlannedMessage {
    sender_index: usize,
    seq_index: usize,
    client_msg_id: String,
    message: IMMessage,
}

struct ReceivedEvent {
    receiver_id: String,
    sender_id: String,
    client_msg_id: String,
    had_text: bool,
    matched_prefix: bool,
}

struct ConnectionEventObserved {
    user_id: String,
    kind: &'static str,
    reason: Option<String>,
}

struct SendAckObserved {
    user_id: String,
    client_msg_id: String,
    ack: SendAck,
}

struct SendFailedObserved {
    user_id: String,
    client_msg_id: String,
    reason: String,
}

struct SendOutcomeStats {
    terminal_send_ids: HashSet<String>,
    durable_ack_ids: HashSet<String>,
    enqueue_failed_ids: HashMap<String, String>,
    failed_ids: HashMap<String, String>,
    acked_ids: HashMap<String, String>,
    error_ack_ids: HashSet<String>,
    ack_result_counts: HashMap<String, usize>,
    ack_latencies: Vec<Duration>,
    first_ack_labels: Vec<String>,
    first_enqueue_errors: Vec<String>,
    first_failed_events: Vec<String>,
    first_unknown_acks: Vec<String>,
    first_unknown_failures: Vec<String>,
    duplicate_ack_count: usize,
    ack_update_count: usize,
    duplicate_failed_count: usize,
    duplicate_enqueue_failure_count: usize,
    durable_ack_count: usize,
    unknown_ack_count: usize,
    unknown_failed_count: usize,
    ack_user_mismatch_count: usize,
}

impl SendOutcomeStats {
    fn new(total: usize) -> Self {
        Self {
            terminal_send_ids: HashSet::with_capacity(total),
            durable_ack_ids: HashSet::with_capacity(total),
            enqueue_failed_ids: HashMap::new(),
            failed_ids: HashMap::new(),
            acked_ids: HashMap::with_capacity(total),
            error_ack_ids: HashSet::new(),
            ack_result_counts: HashMap::new(),
            ack_latencies: Vec::with_capacity(total),
            first_ack_labels: Vec::new(),
            first_enqueue_errors: Vec::new(),
            first_failed_events: Vec::new(),
            first_unknown_acks: Vec::new(),
            first_unknown_failures: Vec::new(),
            duplicate_ack_count: 0,
            ack_update_count: 0,
            duplicate_failed_count: 0,
            duplicate_enqueue_failure_count: 0,
            durable_ack_count: 0,
            unknown_ack_count: 0,
            unknown_failed_count: 0,
            ack_user_mismatch_count: 0,
        }
    }

    fn record_enqueue_error(&mut self, client_msg_id: String, error: String) {
        self.terminal_send_ids.insert(client_msg_id.clone());
        if self
            .enqueue_failed_ids
            .insert(client_msg_id.clone(), error.clone())
            .is_some()
        {
            self.duplicate_enqueue_failure_count += 1;
        }
        if self.first_enqueue_errors.len() < 5 {
            self.first_enqueue_errors
                .push(format!("{client_msg_id}: {error}"));
        }
    }

    fn record_ack(
        &mut self,
        event: SendAckObserved,
        planned_ids: &HashSet<String>,
        sender_by_client_msg_id: &HashMap<String, String>,
        sent_at: &HashMap<String, Instant>,
    ) {
        if !planned_ids.contains(&event.client_msg_id) {
            self.unknown_ack_count += 1;
            if self.first_unknown_acks.len() < 5 {
                self.first_unknown_acks.push(format!(
                    "{}:{}:{}",
                    event.user_id,
                    event.client_msg_id,
                    ack_label(&event.ack)
                ));
            }
            return;
        }

        if sender_by_client_msg_id
            .get(&event.client_msg_id)
            .is_some_and(|sender| sender != &event.user_id)
        {
            self.ack_user_mismatch_count += 1;
        }

        *self
            .ack_result_counts
            .entry(ack_result_label(&event.ack))
            .or_default() += 1;
        let label = ack_label(&event.ack);
        if self.first_ack_labels.len() < 5 {
            self.first_ack_labels.push(label.clone());
        }
        let previous_ack = self
            .acked_ids
            .insert(event.client_msg_id.clone(), label.clone());
        let is_duplicate = previous_ack.is_some();
        if let Some(previous_ack) = previous_ack {
            if previous_ack == label {
                self.duplicate_ack_count += 1;
            } else {
                self.ack_update_count += 1;
            }
        }
        let is_durable = is_durable_ack(&event.ack);
        if is_durable {
            self.terminal_send_ids.insert(event.client_msg_id.clone());
            self.failed_ids.remove(&event.client_msg_id);
            self.error_ack_ids.remove(&event.client_msg_id);
            if self.durable_ack_ids.insert(event.client_msg_id.clone()) {
                self.durable_ack_count += 1;
            }
        } else if matches!(event.ack.result.as_ref(), Some(send_ack::Result::Error(_))) {
            self.terminal_send_ids.insert(event.client_msg_id.clone());
            self.error_ack_ids.insert(event.client_msg_id.clone());
        }
        if !is_duplicate && let Some(started_at) = sent_at.get(&event.client_msg_id).copied() {
            self.ack_latencies.push(started_at.elapsed());
        }
    }

    fn record_failed(&mut self, event: SendFailedObserved, planned_ids: &HashSet<String>) {
        if !planned_ids.contains(&event.client_msg_id) {
            self.unknown_failed_count += 1;
            if self.first_unknown_failures.len() < 5 {
                self.first_unknown_failures.push(format!(
                    "{}:{}:{}",
                    event.user_id, event.client_msg_id, event.reason
                ));
            }
            return;
        }

        self.terminal_send_ids.insert(event.client_msg_id.clone());
        if self
            .failed_ids
            .insert(event.client_msg_id.clone(), event.reason.clone())
            .is_some()
        {
            self.duplicate_failed_count += 1;
        }
        if self.first_failed_events.len() < 5 {
            self.first_failed_events.push(format!(
                "{}:{}:{}",
                event.user_id, event.client_msg_id, event.reason
            ));
        }
    }

    fn send_errors(&self) -> usize {
        self.enqueue_failed_ids.len() + self.failed_ids.len() + self.error_ack_ids.len()
    }
}

fn record_connection_event(
    event: ConnectionEventObserved,
    connection_event_counts: &mut HashMap<String, usize>,
    first_connection_events: &mut Vec<String>,
) {
    let label = format!("{}:{}", event.user_id, event.kind);
    *connection_event_counts.entry(label).or_default() += 1;
    if first_connection_events.len() < 10 {
        first_connection_events.push(match event.reason {
            Some(reason) => format!("{}:{}:{reason}", event.user_id, event.kind),
            None => format!("{}:{}", event.user_id, event.kind),
        });
    }
}

// 压测入口，14 个参数对应命令行可调的各项负载维度；
// 包成结构体只会在调用处多一层样板，对一次性基准脚本不划算。
#[allow(clippy::too_many_arguments)]
async fn login_client(
    run: &str,
    user_id: &str,
    ws_url: &str,
    data_root: &Path,
    tenant_id: &str,
    issuer: &str,
    secret: &str,
    ack_max_in_flight: usize,
    ack_timeout_secs: u64,
    received_tx: mpsc::UnboundedSender<ReceivedEvent>,
    connection_tx: mpsc::UnboundedSender<ConnectionEventObserved>,
    send_ack_tx: mpsc::UnboundedSender<SendAckObserved>,
    send_failed_tx: mpsc::UnboundedSender<SendFailedObserved>,
    text_prefix: String,
) -> std::result::Result<LoadClient, Box<dyn std::error::Error>> {
    let client = IMClient::new();
    let device_id = format!("{run}-{user_id}-device");
    let overlay = SdkConfigOverlay {
        data_url: Some(data_root.to_string_lossy().to_string()),
        access: Some(AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some(ws_url.to_string()),
            ..Default::default()
        })),
        tenant_id: Some(tenant_id.to_string()),
        device_id: Some(device_id.clone()),
        ack_max_in_flight: Some(ack_max_in_flight),
        ack_timeout_secs: Some(ack_timeout_secs),
        ..Default::default()
    };
    client
        .init(Some(format!("{run}-{user_id}-app")), Some(overlay))
        .await?;
    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret: secret.to_string(),
        issuer: issuer.to_string(),
        user_id: user_id.to_string(),
        ttl_secs: 3600,
        device_id: Some(device_id),
        tenant_id: Some(tenant_id.to_string()),
    })?;
    // 压测可选内存库（FLARE_GROUP_E2E_INMEM=1）：消除每客户端 SQLite 落盘 I/O，隔离服务端吞吐。
    let db_kind = if env_bool("FLARE_GROUP_E2E_INMEM", false) {
        LoginDbKind::IndexedDb(in_memory_im_provider())
    } else {
        LoginDbKind::Sqlite
    };
    let apis = client
        .login(user_id, Some(&token), db_kind, |_, _| {})
        .await?;

    let receiver_id = user_id.to_string();
    let message_subscription = client.on_message_batch(move |messages| {
        for message in messages {
            let text = message_text(message);
            let matched_prefix = text
                .as_deref()
                .is_some_and(|text| text.starts_with(&text_prefix));
            let _ = received_tx.send(ReceivedEvent {
                receiver_id: receiver_id.clone(),
                sender_id: message.sender_id.clone(),
                client_msg_id: message.client_msg_id.clone(),
                had_text: text.is_some(),
                matched_prefix,
            });
        }
    })?;

    // 只记阶段变化：同一阶段里质量、RTT 的刷新也会发状态事件，逐条计数会淹没真正的断连。
    let status_user_id = user_id.to_string();
    let last_phase = std::sync::Mutex::new(None::<ConnectionPhase>);
    let status_subscription = client.on_connection_status(move |status| {
        let Ok(mut last) = last_phase.lock() else {
            return;
        };
        if *last == Some(status.phase) {
            return;
        }
        *last = Some(status.phase);
        let _ = connection_tx.send(ConnectionEventObserved {
            user_id: status_user_id.clone(),
            kind: status.phase.as_str(),
            reason: status.reason.clone(),
        });
    })?;

    let send_ack_user_id = user_id.to_string();
    let send_ack_subscription = client.on_send_ack(move |ack| {
        let _ = send_ack_tx.send(SendAckObserved {
            user_id: send_ack_user_id.clone(),
            client_msg_id: ack.client_msg_id.clone(),
            ack: ack.clone(),
        });
    })?;

    let send_failed_user_id = user_id.to_string();
    let send_failed_subscription = client.on_send_failed(move |client_msg_id, reason| {
        let _ = send_failed_tx.send(SendFailedObserved {
            user_id: send_failed_user_id.clone(),
            client_msg_id: client_msg_id.to_string(),
            reason: reason.to_string(),
        });
    })?;

    Ok(LoadClient {
        user_id: user_id.to_string(),
        client,
        apis,
        _message_subscription: message_subscription,
        _send_ack_subscription: send_ack_subscription,
        _send_failed_subscription: send_failed_subscription,
        _connection_subscriptions: vec![status_subscription],
    })
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let users = env_users();
    let per_user = env_usize("FLARE_GROUP_E2E_PER_USER", 1_000);
    let in_flight = env_usize("FLARE_GROUP_E2E_IN_FLIGHT", 128).min(users.len() * per_user);
    let ack_max_in_flight = env_usize("FLARE_GROUP_E2E_ACK_MAX_IN_FLIGHT", 128);
    let client_ack_timeout_secs = env_u64("FLARE_GROUP_E2E_CLIENT_ACK_TIMEOUT_SECS", 120);
    let receive_timeout =
        Duration::from_millis(env_u64("FLARE_GROUP_E2E_RECEIVE_TIMEOUT_MS", 180_000));
    let ack_timeout = Duration::from_millis(env_u64(
        "FLARE_GROUP_E2E_ACK_TIMEOUT_MS",
        receive_timeout.as_millis() as u64,
    ));
    let send_timeout = Duration::from_millis(env_u64("FLARE_GROUP_E2E_SEND_TIMEOUT_MS", 30_000));
    let settle_delay = Duration::from_millis(env_u64("FLARE_GROUP_E2E_SETTLE_MS", 2_000));
    let tenant_id = env::var("TENANT_ID").unwrap_or_else(|_| "0".to_string());
    let issuer = env::var("TOKEN_ISSUER").unwrap_or_else(|_| "flare-im-core".to_string());
    let secret = token_secret()?;
    let run = env::var("FLARE_GROUP_E2E_RUN_ID").unwrap_or_else(|_| run_id());
    let data_root = data_root_for_run(&run);
    std::fs::create_dir_all(&data_root)?;
    let ws_url =
        env::var("FLARE_IM_SERVER_URL").unwrap_or_else(|_| "ws://localhost:60051".to_string());
    let text_prefix = format!("group-load {run}");
    // 分布式多进程压测：所有进程传相同 FLARE_GROUP_E2E_ROSTER(完整成员表,用于解析同一群)
    // + 相同 FLARE_GROUP_E2E_RUN_ID(共享文本前缀,跨进程识别对方消息);各进程 FLARE_GROUP_E2E_USERS
    // 为本进程登录的成员切片。单进程上限 ~100-200 客户端,多进程可把单个大群推到更高规模。
    let roster: Vec<String> = env::var("FLARE_GROUP_E2E_ROSTER")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let distributed = roster.len() >= 2;
    let unique_group = !distributed && env_bool("FLARE_GROUP_E2E_UNIQUE_GROUP", true);
    let mut group_user_ids = if distributed {
        roster.clone()
    } else {
        users.clone()
    };
    if unique_group {
        group_user_ids.push(format!("load-member-{run}"));
    }

    if users.len() < 2 {
        return Err("FLARE_GROUP_E2E_USERS requires at least 2 users".into());
    }
    if distributed {
        println!(
            "  distributed: true (roster={} members, this process users={})",
            roster.len(),
            users.len()
        );
    }

    println!("group latency throughput e2e");
    println!("  ws_url: {ws_url}");
    println!("  users: {}", users.join(","));
    println!("  run: {run}");
    println!("  per_user: {per_user}");
    println!("  total_messages: {}", users.len() * per_user);
    println!(
        "  expected_remote_deliveries: {}",
        users.len() * per_user * (users.len() - 1)
    );
    println!("  in_flight: {in_flight}");
    println!("  ack_max_in_flight: {ack_max_in_flight}");
    println!("  client_ack_timeout_secs: {client_ack_timeout_secs}");
    println!("  send_timeout: {}", fmt_duration(send_timeout));
    println!("  ack_timeout: {}", fmt_duration(ack_timeout));
    println!("  settle_delay: {}", fmt_duration(settle_delay));
    println!("  data_root: {}", data_root.display());
    println!("  unique_group: {unique_group}");

    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let (connection_tx, mut connection_rx) = mpsc::unbounded_channel();
    let (send_ack_tx, mut send_ack_rx) = mpsc::unbounded_channel();
    let (send_failed_tx, mut send_failed_rx) = mpsc::unbounded_channel();
    // 连接建立限速:OS listen backlog(somaxconn,常 128)下突发并发连接会被 reset;
    // 多进程分布式压测默认每登录间隔 15ms,并对瞬时连接失败重试,保持在途连接数受控。
    let login_stagger = Duration::from_millis(env_u64(
        "FLARE_GROUP_E2E_LOGIN_STAGGER_MS",
        if distributed { 15 } else { 0 },
    ));
    let mut clients = Vec::with_capacity(users.len());
    for user_id in &users {
        let mut attempt = 0u32;
        let client = loop {
            match login_client(
                &run,
                user_id,
                &ws_url,
                &data_root,
                &tenant_id,
                &issuer,
                &secret,
                ack_max_in_flight,
                client_ack_timeout_secs,
                received_tx.clone(),
                connection_tx.clone(),
                send_ack_tx.clone(),
                send_failed_tx.clone(),
                text_prefix.clone(),
            )
            .await
            {
                Ok(client) => break client,
                Err(error) if attempt < 5 => {
                    attempt += 1;
                    eprintln!("login {user_id} attempt {attempt} failed: {error}; retrying");
                    tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
                }
                Err(error) => return Err(error),
            }
        };
        clients.push(client);
        if !login_stagger.is_zero() {
            tokio::time::sleep(login_stagger).await;
        }
    }
    drop(received_tx);
    drop(connection_tx);
    drop(send_ack_tx);
    drop(send_failed_tx);

    if !settle_delay.is_zero() {
        tokio::time::sleep(settle_delay).await;
    }

    let display_name = format!("群聊压测 {run}");
    let mut conversation_id = String::new();
    for client in &clients {
        let conversation = client
            .apis
            .conversation_api
            .get_group_by_user_ids(&group_user_ids, Some(&display_name))
            .await?;
        if conversation_id.is_empty() {
            conversation_id = conversation.conversation_id.clone();
        } else if conversation_id != conversation.conversation_id {
            return Err(format!(
                "group conversation id mismatch: expected {conversation_id}, got {} for {}",
                conversation.conversation_id, client.user_id
            )
            .into());
        }
    }
    println!("  conversation_id: {conversation_id}");

    if !settle_delay.is_zero() {
        tokio::time::sleep(settle_delay).await;
    }

    let build_started = Instant::now();
    let total = users.len() * per_user;
    let mut planned = Vec::with_capacity(total);
    for index in 0..per_user {
        for (sender_index, client) in clients.iter().enumerate() {
            let text = format!("{text_prefix} sender={} seq={index}", client.user_id);
            let message = client
                .apis
                .message_build_api
                .create_text(&conversation_id, &text, false, &[])
                .await?;
            planned.push(PlannedMessage {
                sender_index,
                seq_index: index,
                client_msg_id: message.client_msg_id.clone(),
                message,
            });
            if planned.len() % 1_000 == 0 || planned.len() == total {
                println!("progress build {}/{}", planned.len(), total);
            }
        }
    }
    let build_elapsed = build_started.elapsed();

    let planned_client_msg_ids = planned
        .iter()
        .map(|message| message.client_msg_id.clone())
        .collect::<HashSet<_>>();
    let mut sent_at = HashMap::with_capacity(total);
    let mut sender_by_client_msg_id = HashMap::with_capacity(total);
    let mut send_outcomes = SendOutcomeStats::new(total);
    let mut enqueue_result_counts: HashMap<String, usize> = HashMap::new();
    let mut enqueue_return_latencies = Vec::with_capacity(total);
    let send_started = Instant::now();
    let mut next = 0usize;
    let mut completed_sends = 0usize;
    let mut pending = futures::stream::FuturesUnordered::new();

    while next < planned.len() || !pending.is_empty() {
        while next < planned.len() && pending.len() < in_flight {
            let item = &planned[next];
            let sender = &clients[item.sender_index];
            let api = sender.apis.message_api.clone();
            let sender_id = sender.user_id.clone();
            let message = item.message.clone();
            let client_msg_id = item.client_msg_id.clone();
            let timeout = send_timeout;
            sent_at.insert(client_msg_id.clone(), Instant::now());
            sender_by_client_msg_id.insert(client_msg_id.clone(), sender_id);
            pending.push(async move {
                let started = Instant::now();
                match tokio::time::timeout(timeout, api.send_no_oss(message)).await {
                    Ok(Ok(ack)) => Ok((client_msg_id, ack, started.elapsed())),
                    Ok(Err(error)) => Err((client_msg_id, error.to_string())),
                    Err(_) => Err((
                        client_msg_id,
                        format!("send timed out after {}", fmt_duration(timeout)),
                    )),
                }
            });
            next += 1;
        }

        if let Some(result) = pending.next().await {
            completed_sends += 1;
            match result {
                Ok((client_msg_id, ack, latency)) => {
                    let _ = client_msg_id;
                    *enqueue_result_counts
                        .entry(ack_result_label(&ack))
                        .or_default() += 1;
                    enqueue_return_latencies.push(latency);
                }
                Err((client_msg_id, error)) => {
                    send_outcomes.record_enqueue_error(client_msg_id, error);
                }
            }
            if completed_sends.is_multiple_of(500) || completed_sends == total {
                println!(
                    "progress enqueue {completed_sends}/{total} errors={} server_terminal={}",
                    send_outcomes.enqueue_failed_ids.len(),
                    send_outcomes.terminal_send_ids.len()
                );
            }
        }
    }
    let send_elapsed = send_started.elapsed();

    let mut connection_event_counts: HashMap<String, usize> = HashMap::new();
    let mut first_connection_events = Vec::new();
    let ack_wait_started = Instant::now();
    let ack_deadline = Instant::now() + ack_timeout;
    let mut last_ack_progress_terminal = usize::MAX;
    while send_outcomes.terminal_send_ids.len() < total && Instant::now() < ack_deadline {
        let remaining = ack_deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            Some(event) = send_ack_rx.recv() => {
                send_outcomes.record_ack(
                    event,
                    &planned_client_msg_ids,
                    &sender_by_client_msg_id,
                    &sent_at,
                );
                let terminal = send_outcomes.terminal_send_ids.len();
                if terminal != last_ack_progress_terminal
                    && (terminal.is_multiple_of(500) || terminal == total)
                {
                    last_ack_progress_terminal = terminal;
                    println!(
                        "progress server_ack {}/{} durable={} failed={}",
                        terminal,
                        total,
                        send_outcomes.durable_ack_count,
                        send_outcomes.send_errors()
                    );
                }
            }
            Some(event) = send_failed_rx.recv() => {
                send_outcomes.record_failed(event, &planned_client_msg_ids);
                println!(
                    "progress server_failed terminal={}/{} failed={}",
                    send_outcomes.terminal_send_ids.len(),
                    total,
                    send_outcomes.send_errors()
                );
            }
            Some(event) = connection_rx.recv() => {
                record_connection_event(
                    event,
                    &mut connection_event_counts,
                    &mut first_connection_events,
                );
            }
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(500))) => {}
        }
    }
    let ack_wait_elapsed = ack_wait_started.elapsed();

    let receive_started = Instant::now();
    let deadline = Instant::now() + receive_timeout;
    // 分布式模式:本进程无法本地计算总投递数(发送方分布在别的进程),故跑满窗口并统计去重接收;
    // 全局丢0/对账由协调方(脚本)按"Σ接收 == Σ发送 ×(roster-1)"核验。
    let expected_remote_deliveries = if distributed {
        0
    } else {
        total * (users.len() - 1)
    };
    let receive_capacity = if distributed {
        total.max(4096)
    } else {
        expected_remote_deliveries
    };
    let mut received_remote_keys = HashSet::with_capacity(receive_capacity);
    let mut remote_received_by_client_msg_id: HashMap<String, usize> =
        HashMap::with_capacity(total);
    let mut received_per_user: HashMap<String, usize> =
        users.iter().map(|user| (user.clone(), 0usize)).collect();
    let mut raw_per_user: HashMap<String, usize> =
        users.iter().map(|user| (user.clone(), 0usize)).collect();
    let mut duplicate_remote_count = 0usize;
    let mut unknown_remote_count = 0usize;
    let mut own_echo_count = 0usize;
    let mut raw_received_events = 0usize;
    let mut raw_without_text_count = 0usize;
    let mut raw_prefix_miss_count = 0usize;
    let mut receive_latencies = Vec::with_capacity(expected_remote_deliveries);
    let mut first_unknown_received = Vec::new();

    while (distributed || received_remote_keys.len() < expected_remote_deliveries)
        && Instant::now() < deadline
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            Some(event) = received_rx.recv() => {
                raw_received_events += 1;
                if let Some(count) = raw_per_user.get_mut(&event.receiver_id) {
                    *count += 1;
                }
                if !event.had_text {
                    raw_without_text_count += 1;
                }
                if !event.matched_prefix {
                    raw_prefix_miss_count += 1;
                    continue;
                }
                // 分布式:直接用消息自带的 sender_id(对端真实发送者),不依赖本地 plan 映射,
                // 从而跨进程统计;单进程:沿用本地 client_msg_id→sender 映射做严格校验。
                let sender_id: &str = if distributed {
                    if event.sender_id.is_empty() {
                        unknown_remote_count += 1;
                        continue;
                    }
                    event.sender_id.as_str()
                } else {
                    match sender_by_client_msg_id.get(&event.client_msg_id) {
                        Some(s) => s.as_str(),
                        None => {
                            unknown_remote_count += 1;
                            if first_unknown_received.len() < 5 {
                                first_unknown_received.push(event.client_msg_id);
                            }
                            continue;
                        }
                    }
                };
                if sender_id == event.receiver_id || event.sender_id == event.receiver_id {
                    own_echo_count += 1;
                    continue;
                }
                let key = format!("{}:{}", event.receiver_id, event.client_msg_id);
                if !received_remote_keys.insert(key) {
                    duplicate_remote_count += 1;
                    continue;
                }
                *remote_received_by_client_msg_id
                    .entry(event.client_msg_id.clone())
                    .or_default() += 1;
                if let Some(count) = received_per_user.get_mut(&event.receiver_id) {
                    *count += 1;
                }
                if let Some(started_at) = sent_at.get(&event.client_msg_id).copied() {
                    receive_latencies.push(started_at.elapsed());
                }
                if received_remote_keys.len() % 5_000 == 0
                    || received_remote_keys.len() == expected_remote_deliveries
                {
                    println!(
                        "progress receive {}/{} duplicate={} unknown={}",
                        received_remote_keys.len(),
                        expected_remote_deliveries,
                        duplicate_remote_count,
                        unknown_remote_count
                    );
                }
            }
            Some(event) = send_ack_rx.recv() => {
                send_outcomes.record_ack(
                    event,
                    &planned_client_msg_ids,
                    &sender_by_client_msg_id,
                    &sent_at,
                );
            }
            Some(event) = send_failed_rx.recv() => {
                send_outcomes.record_failed(event, &planned_client_msg_ids);
            }
            Some(event) = connection_rx.recv() => {
                record_connection_event(
                    event,
                    &mut connection_event_counts,
                    &mut first_connection_events,
                );
            }
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(500))) => {}
        }
    }

    while let Ok(event) = received_rx.try_recv() {
        raw_received_events += 1;
        if let Some(count) = raw_per_user.get_mut(&event.receiver_id) {
            *count += 1;
        }
        if !event.had_text {
            raw_without_text_count += 1;
        }
        if !event.matched_prefix {
            raw_prefix_miss_count += 1;
            continue;
        }
        let Some(sender_id) = sender_by_client_msg_id.get(&event.client_msg_id) else {
            unknown_remote_count += 1;
            continue;
        };
        if *sender_id == event.receiver_id || event.sender_id == event.receiver_id {
            own_echo_count += 1;
            continue;
        }
        let key = format!("{}:{}", event.receiver_id, event.client_msg_id);
        if !received_remote_keys.insert(key) {
            duplicate_remote_count += 1;
            continue;
        }
        *remote_received_by_client_msg_id
            .entry(event.client_msg_id.clone())
            .or_default() += 1;
        if let Some(count) = received_per_user.get_mut(&event.receiver_id) {
            *count += 1;
        }
        if let Some(started_at) = sent_at.get(&event.client_msg_id).copied() {
            receive_latencies.push(started_at.elapsed());
        }
    }
    while let Ok(event) = connection_rx.try_recv() {
        record_connection_event(
            event,
            &mut connection_event_counts,
            &mut first_connection_events,
        );
    }
    while let Ok(event) = send_ack_rx.try_recv() {
        send_outcomes.record_ack(
            event,
            &planned_client_msg_ids,
            &sender_by_client_msg_id,
            &sent_at,
        );
    }
    while let Ok(event) = send_failed_rx.try_recv() {
        send_outcomes.record_failed(event, &planned_client_msg_ids);
    }

    enqueue_return_latencies.sort_unstable();
    send_outcomes.ack_latencies.sort_unstable();
    receive_latencies.sort_unstable();
    let server_terminal = send_outcomes.terminal_send_ids.len();
    let acked = send_outcomes.durable_ack_count;
    let unacked = total.saturating_sub(server_terminal);
    let send_errors = send_outcomes.send_errors();
    let received_remote = received_remote_keys.len();
    let lost_remote = expected_remote_deliveries.saturating_sub(received_remote);
    let receive_elapsed = receive_started.elapsed();
    let end_to_end_elapsed = send_started.elapsed();

    println!();
    println!("summary");
    println!("  users: {}", users.len());
    println!("  per_user: {per_user}");
    println!("  built: {total}");
    println!("  sent: {total}");
    println!(
        "  send_enqueued: {}",
        total.saturating_sub(send_outcomes.enqueue_failed_ids.len())
    );
    println!(
        "  send_enqueue_errors: {}",
        send_outcomes.enqueue_failed_ids.len()
    );
    println!("  server_terminal_sends: {server_terminal}");
    println!("  send_errors: {send_errors}");
    println!("  send_acked: {acked}");
    println!("  send_unacked: {unacked}");
    println!("  durable_acks: {}", send_outcomes.durable_ack_count);
    println!("  enqueue_result_counts: {enqueue_result_counts:?}");
    println!(
        "  server_ack_result_counts: {:?}",
        send_outcomes.ack_result_counts
    );
    println!("  send_failed_events: {}", send_outcomes.failed_ids.len());
    println!(
        "  send_error_ack_events: {}",
        send_outcomes.error_ack_ids.len()
    );
    println!("  duplicate_acks: {}", send_outcomes.duplicate_ack_count);
    println!("  ack_updates: {}", send_outcomes.ack_update_count);
    println!(
        "  duplicate_send_failed_events: {}",
        send_outcomes.duplicate_failed_count
    );
    println!(
        "  duplicate_enqueue_failures: {}",
        send_outcomes.duplicate_enqueue_failure_count
    );
    println!("  unknown_acks: {}", send_outcomes.unknown_ack_count);
    println!(
        "  unknown_send_failed_events: {}",
        send_outcomes.unknown_failed_count
    );
    println!(
        "  ack_user_mismatch_count: {}",
        send_outcomes.ack_user_mismatch_count
    );
    println!("  expected_remote_deliveries: {expected_remote_deliveries}");
    println!("  received_remote_deliveries: {received_remote}");
    println!("  lost_remote_deliveries: {lost_remote}");
    println!("  duplicate_remote_deliveries: {duplicate_remote_count}");
    println!("  unknown_remote_deliveries: {unknown_remote_count}");
    println!("  own_echo_count: {own_echo_count}");
    println!("  raw_received_events: {raw_received_events}");
    println!("  raw_without_text_count: {raw_without_text_count}");
    println!("  raw_prefix_miss_count: {raw_prefix_miss_count}");
    println!("  raw_per_user: {raw_per_user:?}");
    println!("  connection_event_counts: {connection_event_counts:?}");
    println!("  build_elapsed: {}", fmt_duration(build_elapsed));
    println!("  send_enqueue_elapsed: {}", fmt_duration(send_elapsed));
    println!(
        "  server_ack_wait_elapsed: {}",
        fmt_duration(ack_wait_elapsed)
    );
    println!("  receive_wait_elapsed: {}", fmt_duration(receive_elapsed));
    println!("  end_to_end_elapsed: {}", fmt_duration(end_to_end_elapsed));
    println!("  build_throughput: {}", fmt_rate(total, build_elapsed));
    println!(
        "  send_enqueue_throughput: {}",
        fmt_rate(total, send_elapsed)
    );
    println!(
        "  remote_receive_throughput: {}",
        fmt_rate(received_remote, receive_elapsed)
    );
    println!(
        "  end_to_end_remote_delivery_throughput: {}",
        fmt_rate(received_remote, end_to_end_elapsed)
    );
    println!(
        "  ack_latency_min: {}",
        fmt_duration(
            *send_outcomes
                .ack_latencies
                .first()
                .unwrap_or(&Duration::ZERO)
        )
    );
    println!(
        "  ack_latency_p50: {}",
        fmt_duration(percentile(&send_outcomes.ack_latencies, 50, 100))
    );
    println!(
        "  ack_latency_p95: {}",
        fmt_duration(percentile(&send_outcomes.ack_latencies, 95, 100))
    );
    println!(
        "  ack_latency_p99: {}",
        fmt_duration(percentile(&send_outcomes.ack_latencies, 99, 100))
    );
    println!(
        "  ack_latency_max: {}",
        fmt_duration(
            *send_outcomes
                .ack_latencies
                .last()
                .unwrap_or(&Duration::ZERO)
        )
    );
    println!(
        "  remote_latency_min: {}",
        fmt_duration(*receive_latencies.first().unwrap_or(&Duration::ZERO))
    );
    println!(
        "  remote_latency_p50: {}",
        fmt_duration(percentile(&receive_latencies, 50, 100))
    );
    println!(
        "  remote_latency_p95: {}",
        fmt_duration(percentile(&receive_latencies, 95, 100))
    );
    println!(
        "  remote_latency_p99: {}",
        fmt_duration(percentile(&receive_latencies, 99, 100))
    );
    println!(
        "  remote_latency_max: {}",
        fmt_duration(*receive_latencies.last().unwrap_or(&Duration::ZERO))
    );
    println!("  received_per_user: {received_per_user:?}");
    if !send_outcomes.first_ack_labels.is_empty() {
        println!("  first_ack_labels: {:?}", send_outcomes.first_ack_labels);
    }
    if !first_connection_events.is_empty() {
        println!("  first_connection_events: {first_connection_events:?}");
    }
    if !send_outcomes.first_enqueue_errors.is_empty() {
        println!(
            "  first_enqueue_errors: {:?}",
            send_outcomes.first_enqueue_errors
        );
    }
    if !send_outcomes.first_failed_events.is_empty() {
        println!(
            "  first_send_failed_events: {:?}",
            send_outcomes.first_failed_events
        );
    }
    if !send_outcomes.first_unknown_acks.is_empty() {
        println!(
            "  first_unknown_acks: {:?}",
            send_outcomes.first_unknown_acks
        );
    }
    if !send_outcomes.first_unknown_failures.is_empty() {
        println!(
            "  first_unknown_send_failed_events: {:?}",
            send_outcomes.first_unknown_failures
        );
    }
    if !first_unknown_received.is_empty() {
        println!("  first_unknown_received_client_msg_ids: {first_unknown_received:?}");
    }
    if lost_remote > 0 {
        let expected_per_message = users.len().saturating_sub(1);
        let mut messages_with_missing_remote = 0usize;
        let mut first_missing_by_message = Vec::new();
        for message in &planned {
            let received = remote_received_by_client_msg_id
                .get(&message.client_msg_id)
                .copied()
                .unwrap_or(0);
            if received >= expected_per_message {
                continue;
            }
            messages_with_missing_remote += 1;
            if first_missing_by_message.len() < 10 {
                let sender_id = &users[message.sender_index];
                let missing_receivers = users
                    .iter()
                    .filter(|receiver_id| *receiver_id != sender_id)
                    .filter(|receiver_id| {
                        !received_remote_keys
                            .contains(&format!("{}:{}", receiver_id, message.client_msg_id))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                first_missing_by_message.push(format!(
                    "sender={} seq={} client_msg_id={} received={} missing={} receivers={}",
                    sender_id,
                    message.seq_index,
                    message.client_msg_id,
                    received,
                    expected_per_message - received,
                    missing_receivers.join("|")
                ));
            }
        }
        println!("  messages_with_missing_remote: {messages_with_missing_remote}");
        println!("  first_missing_by_message: {first_missing_by_message:?}");
    }
    if server_terminal < total {
        let missing = planned
            .iter()
            .filter(|message| {
                !send_outcomes
                    .terminal_send_ids
                    .contains(&message.client_msg_id)
            })
            .take(5)
            .map(|message| message.client_msg_id.as_str())
            .collect::<Vec<_>>();
        println!("  first_unterminal_client_msg_ids: {missing:?}");
    }

    let _keep_clients_alive = clients
        .iter()
        .map(|client| &client.client)
        .collect::<Vec<_>>();

    if send_errors > 0
        || unacked > 0
        || send_outcomes.durable_ack_count < total
        || lost_remote > 0
        || duplicate_remote_count > 0
    {
        return Err(format!(
            "group e2e failed: send_errors={send_errors}, unacked={unacked}, durable_acks={}, lost_remote={lost_remote}, duplicate_acks={}, duplicate_remote={duplicate_remote_count}",
            send_outcomes.durable_ack_count,
            send_outcomes.duplicate_ack_count
        )
        .into());
    }

    Ok(())
}
