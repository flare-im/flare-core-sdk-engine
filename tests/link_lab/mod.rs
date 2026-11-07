//! 链路集成测试的本机实验台：每个「接入点」是一个只绑 127.0.0.1 的真实 flare-core WebSocket 服务端，
//! 前面挂一个可控的 TCP 中继（切断、冻结成半开、拒绝新连接）；SDK 以开发模式接入（多接入点），
//! 走自己的完整链路：链路状态机 → 选路 → 交错竞速 → CONNECT → 发件箱 → SendAck。
//!
//! 「集群」的消息幂等由一个共享账本模拟：同一 `client_msg_id` 不管从哪个接入点重发，都返回首次的序号，
//! 测试据此断言「零重复」与「会话内有序」。

#![allow(dead_code)]

pub mod bootstrap;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::device::DeviceInfo;
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, frame_with_payload_command,
    payload_command::Type as PayloadType,
};
use flare_core::server::{
    AuthResult, Authenticator, FlareServer, FlareServerBuilder, ServerEventHandler,
};
use flare_im_core_sdk::event::{ConnectionEvent, MessageEvent, SdkEvent, SdkEventKind};
use flare_im_core_sdk::prelude::*;
use flare_proto::common::{Ack, Message, SendAccepted, SendAckDurability, ack, send_ack};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub const USER_ID: &str = "alice";
pub const CONVERSATION_ID: &str = "group-link-lab";

/// 服务端看到的发送：首次到达的顺序、每条消息的序号、总共收到几次（含重发）。
#[derive(Default)]
pub struct Ledger {
    pub first_arrivals: Vec<String>,
    pub seq_of: HashMap<String, u64>,
    pub received_total: usize,
    next_seq: u64,
}

impl Ledger {
    /// 幂等接收：同一 `client_msg_id` 返回首次的序号（集群的幂等账本）。
    fn accept(&mut self, client_msg_id: &str) -> u64 {
        self.received_total += 1;
        if let Some(seq) = self.seq_of.get(client_msg_id) {
            return *seq;
        }
        self.next_seq += 1;
        self.first_arrivals.push(client_msg_id.to_owned());
        self.seq_of.insert(client_msg_id.to_owned(), self.next_seq);
        self.next_seq
    }
}

/// message-ingest 的替身：每条 SEND 按已持久化接收，序号来自共享账本。
pub struct Ingest {
    pub ledger: Arc<Mutex<Ledger>>,
    /// 为真时不回 SendAck（模拟服务端吞掉回执）。
    pub silent: AtomicBool,
}

#[async_trait]
impl ServerEventHandler for Ingest {
    async fn handle_message(
        &self,
        command: &PayloadCommand,
        _connection_id: &str,
    ) -> flare_core::common::error::Result<Option<Frame>> {
        let Ok(message) = Message::decode(command.payload.as_slice()) else {
            return Ok(None);
        };
        let seq = self.ledger.lock().unwrap().accept(&message.client_msg_id);
        if self.silent.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let ack = Ack {
            ack_id: None,
            ack_at: None,
            payload: Some(ack::Payload::Send(flare_proto::common::SendAck {
                client_msg_id: message.client_msg_id.clone(),
                conversation_id: message.conversation_id,
                ack_id: None,
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: format!("server-{}", message.client_msg_id),
                    conversation_seq: seq,
                    server_time: 1_700_000_000_000,
                    durability: SendAckDurability::Persisted as i32,
                })),
            })),
        };
        Ok(Some(frame_with_payload_command(
            PayloadCommand {
                r#type: PayloadType::Ack as i32,
                message_id: command.message_id.clone(),
                payload: ack.encode_to_vec(),
                metadata: Default::default(),
                seq: 0,
            },
            Reliability::AtLeastOnce,
        )))
    }
}

/// 认证器的放行方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    Admit,
    /// 暂时放不了行（`SERVICE_UNAVAILABLE`）。
    Retryable,
    /// 租户不可用（终止）。
    TenantUnavailable,
}

/// 认证器：按模式放行或拒绝，记下每条放行连接的 ID（供定向下发 CLOSE）。
pub struct Gate {
    pub mode: Mutex<GateMode>,
    pub admitted: AtomicUsize,
    pub attempts: AtomicUsize,
    pub connections: Mutex<Vec<String>>,
    /// 最近一次 CONNECT 的设备信息与元数据（应用版本、连接代数等）。
    pub last_device: Mutex<Option<DeviceInfo>>,
    pub last_metadata: Mutex<HashMap<String, String>>,
    /// CONNECT_ACK 里报的最新清单 serial。
    pub access_serial: AtomicU64,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            mode: Mutex::new(GateMode::Admit),
            admitted: AtomicUsize::new(0),
            attempts: AtomicUsize::new(0),
            connections: Mutex::new(Vec::new()),
            last_device: Mutex::new(None),
            last_metadata: Mutex::new(HashMap::new()),
            access_serial: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Authenticator for Gate {
    async fn authenticate(
        &self,
        _token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> flare_core::common::error::Result<AuthResult> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        *self.last_device.lock().unwrap() = device_info.cloned();
        *self.last_metadata.lock().unwrap() = metadata
            .map(|metadata| {
                metadata
                    .iter()
                    .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).into_owned()))
                    .collect()
            })
            .unwrap_or_default();
        match *self.mode.lock().unwrap() {
            GateMode::Retryable => Ok(AuthResult::retryable_failure(
                "SERVICE_UNAVAILABLE".to_string(),
            )),
            GateMode::TenantUnavailable => {
                Ok(AuthResult::failure("TENANT_UNAVAILABLE".to_string()))
            }
            GateMode::Admit => {
                self.admitted.fetch_add(1, Ordering::SeqCst);
                self.connections
                    .lock()
                    .unwrap()
                    .push(connection_id.to_owned());
                Ok(AuthResult::success(Some(USER_ID.to_string()))
                    .with_tenant_id("tenant-a")
                    .with_access_serial(self.access_serial.load(Ordering::SeqCst)))
            }
        }
    }
}

pub fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

/// 一条中继连接的两个方向的转发任务。
type PumpPair = (JoinHandle<()>, JoinHandle<()>);

/// 只绑 127.0.0.1 的 TCP 中继。
pub struct Relay {
    pub address: String,
    accepted_at: Arc<Mutex<Vec<Instant>>>,
    frozen: Arc<AtomicBool>,
    /// 单向时延（毫秒）：每段字节读到之后这么久才转发，两个方向都加（往返即两倍）。
    latency_ms: Arc<AtomicU64>,
    connections: Arc<tokio::sync::Mutex<Vec<PumpPair>>>,
    accept_loop: Mutex<Option<JoinHandle<()>>>,
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    frozen: Arc<AtomicBool>,
    latency_ms: Arc<AtomicU64>,
) {
    // 读与写分开：时延只推迟每段字节的转发时刻，不让后面的字节排在前一段的等待之后。
    let (queued, mut due) =
        tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let reader = async move {
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            let read = match from.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            // 冻结：照收不误、一个字节也不转发——两端都以为连接还在（半开）。
            if frozen.load(Ordering::SeqCst) {
                continue;
            }
            let at = tokio::time::Instant::now()
                + Duration::from_millis(latency_ms.load(Ordering::SeqCst));
            if queued.send((at, buffer[..read].to_vec())).is_err() {
                break;
            }
        }
    };
    let writer = async move {
        while let Some((at, bytes)) = due.recv().await {
            tokio::time::sleep_until(at).await;
            if to.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = to.shutdown().await;
    };
    tokio::join!(reader, writer);
}

impl Relay {
    pub async fn start(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let address = listener.local_addr().expect("relay address").to_string();
        let accepted_at = Arc::new(Mutex::new(Vec::new()));
        let frozen = Arc::new(AtomicBool::new(false));
        let latency_ms = Arc::new(AtomicU64::new(0));
        let connections = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let accept_loop = tokio::spawn({
            let accepted_at = accepted_at.clone();
            let frozen = frozen.clone();
            let latency_ms = latency_ms.clone();
            let connections = connections.clone();
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    accepted_at.lock().unwrap().push(Instant::now());
                    let Ok(server) = TcpStream::connect(&target).await else {
                        continue;
                    };
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    let upstream = tokio::spawn(pump(
                        client_read,
                        server_write,
                        frozen.clone(),
                        latency_ms.clone(),
                    ));
                    let downstream = tokio::spawn(pump(
                        server_read,
                        client_write,
                        frozen.clone(),
                        latency_ms.clone(),
                    ));
                    connections.lock().await.push((upstream, downstream));
                }
            }
        });
        Self {
            address,
            accepted_at,
            frozen,
            latency_ms,
            connections,
            accept_loop: Mutex::new(Some(accept_loop)),
        }
    }

    pub fn accepted(&self) -> usize {
        self.accepted_at.lock().unwrap().len()
    }

    pub fn accepted_at(&self) -> Vec<Instant> {
        self.accepted_at.lock().unwrap().clone()
    }

    /// 断网：中止所有在转发的连接，两端套接字随之关闭。中继照常接受新连接。
    pub async fn cut(&self) {
        for (upstream, downstream) in self.connections.lock().await.drain(..) {
            upstream.abort();
            downstream.abort();
        }
    }

    /// 半开：现有连接不关、不再转发任何字节。
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::SeqCst);
    }

    /// 慢链路：此后每段字节在每个方向上都晚 `one_way` 才送到（现有连接与新连接都生效）。
    pub fn set_latency(&self, one_way: Duration) {
        self.latency_ms
            .store(one_way.as_millis() as u64, Ordering::SeqCst);
    }

    /// 这个接入点整体不可用：不再接受新连接（连接被拒），并切断现有连接。
    pub async fn take_down(&self) {
        if let Some(accept_loop) = self.accept_loop.lock().unwrap().take() {
            accept_loop.abort();
        }
        self.cut().await;
    }
}

/// 一个接入点：服务端 + 中继。`id` 与开发模式接入配置里的接入点 ID 一致。
pub struct AccessPoint {
    pub id: String,
    pub server: FlareServer,
    pub server_address: String,
    pub relay: Relay,
    pub gate: Arc<Gate>,
    pub ingest: Arc<Ingest>,
}

/// 服务端的可选设置。
#[derive(Default, Clone, Copy)]
pub struct ServerOptions {
    /// 连接数上限（达到后握手直接回 OVERLOADED）。
    pub max_connections: Option<usize>,
    /// OVERLOADED 的建议等待基数。
    pub overload_retry_after: Option<Duration>,
    /// CONNECT_ACK 里的网关 ID。
    pub gateway_id: Option<&'static str>,
    /// CONNECT_ACK 里报的接入点 ID（不给就报自己的 ID；给别的值即模拟「连到的不是选的那个」）。
    pub reported_ap_id: Option<&'static str>,
    /// 服务端心跳间隔（CONNECT_ACK 的 heartbeat_interval_hint_sec 取它）。
    pub heartbeat_interval: Option<Duration>,
}

impl AccessPoint {
    pub async fn start(id: &str, ledger: Arc<Mutex<Ledger>>, options: ServerOptions) -> Self {
        let gate = Arc::new(Gate::default());
        let ingest = Arc::new(Ingest {
            ledger,
            silent: AtomicBool::new(false),
        });
        let server_address = format!("127.0.0.1:{}", free_tcp_port());
        let mut builder = FlareServerBuilder::new(server_address.clone(), ingest.clone())
            .with_protocols(vec![TransportProtocol::WebSocket])
            .with_protocol_address(TransportProtocol::WebSocket, server_address.clone())
            .with_ap_id(options.reported_ap_id.unwrap_or(id))
            .enable_auth()
            .with_authenticator(gate.clone());
        if let Some(gateway_id) = options.gateway_id {
            builder = builder.with_gateway_id(gateway_id);
        }
        if let Some(interval) = options.heartbeat_interval {
            builder = builder.with_heartbeat(
                flare_core::common::config_types::HeartbeatConfig::default()
                    .with_interval(interval),
            );
        }
        if let Some(max) = options.max_connections {
            builder = builder.with_max_connections(max);
        }
        if let Some(base) = options.overload_retry_after {
            builder = builder.with_overload_retry_after(base);
        }
        let server = builder.build().expect("build WebSocket server");
        server.start().await.expect("start WebSocket server");
        let relay = Relay::start(server_address.clone()).await;
        Self {
            id: id.to_owned(),
            server,
            server_address,
            relay,
            gate,
            ingest,
        }
    }

    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.relay.address)
    }
}

/// 演练用的链路参数：退避短（下限 500 ms）、握手时限 3 s。
pub fn drill_policy() -> serde_json::Value {
    serde_json::json!({
        "backoff": { "baseMs": 500, "capForegroundMs": 5_000 },
        "connectTimeoutMs": 3_000,
        "raceStaggerMs": 300
    })
}

/// 开发模式接入：第一个是主接入点（ID 固定为 `development`），其余为额外接入点。
pub fn development_access(points: &[(&str, String)]) -> AccessConfig {
    let (primary_id, primary_url) = &points[0];
    assert_eq!(
        *primary_id, "development",
        "主接入点的 ID 固定为 development"
    );
    AccessConfig::development(DevelopmentEndpoints {
        ws_url: Some(primary_url.clone()),
        access_points: points[1..]
            .iter()
            .map(|(id, url)| DevelopmentAccessPoint {
                id: (*id).to_owned(),
                ws_url: Some(url.clone()),
                ..Default::default()
            })
            .collect(),
        client_policy: Some(drill_policy()),
        ..Default::default()
    })
}

/// Core API 上的签发端（`POST /api/v1/auth/tokens`）：每次签发一枚一小时有效的真 JWT，记下签发时刻。
/// 挂成开发模式的 `coreApiUrl`，SDK 托管的签发请求就经 HttpRouter 走到这里。
pub struct TokenIssuer {
    pub url: String,
    issued_at: Arc<Mutex<Vec<Instant>>>,
    accept_loop: JoinHandle<()>,
}

impl TokenIssuer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind token issuer");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let issued_at = Arc::new(Mutex::new(Vec::new()));
        let log = issued_at.clone();
        let accept_loop = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(answer_token_request(stream, log.clone()));
            }
        });
        Self {
            url,
            issued_at,
            accept_loop,
        }
    }

    pub fn issued_at(&self) -> Vec<Instant> {
        self.issued_at.lock().unwrap().clone()
    }
}

impl Drop for TokenIssuer {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

async fn answer_token_request(mut stream: TcpStream, issued_at: Arc<Mutex<Vec<Instant>>>) {
    // 读完请求头与请求体（按 Content-Length），一次一问一答，答完关连接。
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    let (head_end, body_len) = loop {
        let Ok(read) = stream.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
            let body_len = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break (end + 4, body_len);
        }
    };
    while request.len() < head_end + body_len {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    let request_line = String::from_utf8_lossy(&request[..head_end])
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    let (status, body) = if request_line.starts_with("POST /api/v1/auth/tokens ") {
        issued_at.lock().unwrap().push(Instant::now());
        let token = IMClient::generate_core_token(CoreTokenConfig {
            secret: "link-lab-token-secret-with-more-than-32-bytes".into(),
            issuer: "flare-im-core".into(),
            user_id: USER_ID.into(),
            ttl_secs: 3_600,
            device_id: None,
            tenant_id: None,
        })
        .expect("token");
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3_600;
        (
            "200 OK",
            serde_json::json!({
                "code": 0,
                "data": { "token": token, "expiresAt": expires_at, "userId": USER_ID },
            })
            .to_string(),
        )
    } else {
        (
            "404 Not Found",
            serde_json::json!({ "code": 404 }).to_string(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

pub fn data_root(suite: &str, case: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("flare-im-core-sdk-{suite}"))
        .join(format!("{case}-{}", std::process::id()))
}

/// 内存存储的客户端（未连接）。`tweak` 可改配置（发送超时、在途窗口等）。
pub async fn lab_client(
    access: AccessConfig,
    root: &Path,
    tweak: impl FnOnce(&mut SdkConfig),
) -> IMClient {
    lab_client_with(access, root, tweak, None).await
}

/// 同 [`lab_client`]，`runtime` 是链路要用的接入运行时（生产模式的目录用它连到本机）。
pub async fn lab_client_with(
    access: AccessConfig,
    root: &Path,
    tweak: impl FnOnce(&mut SdkConfig),
    runtime: Option<Arc<flare_im_core_sdk::link::AccessRuntime>>,
) -> IMClient {
    let mut config = SdkConfig::with_access(access.clone());
    config.ack_timeout_secs = Some(60);
    tweak(&mut config);
    let mut builder = IMClient::builder()
        .config(config)
        .stores(in_memory_im_provider());
    if let Some(runtime) = runtime {
        builder = builder.access_runtime(runtime);
    }
    let client = builder.build().expect("build IMClient");
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                access: Some(access),
                ..Default::default()
            }),
        )
        .await
        .expect("init SDK config");
    client
}

/// 应用看得到的连接与发送事件。
#[derive(Default)]
pub struct Observed {
    pub statuses: Mutex<Vec<(Instant, ConnectionStatus)>>,
    pub switched: Mutex<Vec<(Instant, AccessSwitched)>>,
    pub accepted: Mutex<Vec<String>>,
    pub failed: Mutex<Vec<(String, String)>>,
}

impl Observed {
    pub fn record(client: &IMClient) -> (Arc<Self>, Vec<Subscription>) {
        let observed = Arc::new(Self::default());
        let sink = observed.clone();
        let connection = client
            .on_event_kind(SdkEventKind::Connection, move |event| {
                match event.as_ref() {
                    SdkEvent::Connection(ConnectionEvent::Status(status)) => {
                        sink.statuses
                            .lock()
                            .unwrap()
                            .push((Instant::now(), status.clone()));
                    }
                    SdkEvent::Connection(ConnectionEvent::AccessSwitched(switched)) => {
                        sink.switched
                            .lock()
                            .unwrap()
                            .push((Instant::now(), switched.clone()));
                    }
                    _ => {}
                }
            })
            .expect("subscribe connection events");
        let sink = observed.clone();
        let message = client
            .on_event_kind(SdkEventKind::Message, move |event| match event.as_ref() {
                SdkEvent::Message(MessageEvent::SendAck { ack }) => {
                    if matches!(ack.result, Some(send_ack::Result::Accepted(_))) {
                        sink.accepted
                            .lock()
                            .unwrap()
                            .push(ack.client_msg_id.clone());
                    }
                }
                SdkEvent::Message(MessageEvent::SendFailed {
                    client_msg_id,
                    reason,
                    ..
                }) => {
                    sink.failed
                        .lock()
                        .unwrap()
                        .push((client_msg_id.clone(), reason.clone()));
                }
                _ => {}
            })
            .expect("subscribe message events");
        (observed, vec![connection, message])
    }

    pub fn phases(&self) -> Vec<ConnectionPhase> {
        self.statuses
            .lock()
            .unwrap()
            .iter()
            .map(|(_, status)| status.phase)
            .collect()
    }

    pub fn accepted_distinct(&self) -> std::collections::HashSet<String> {
        self.accepted.lock().unwrap().iter().cloned().collect()
    }
}

pub async fn wait_until<F, Fut>(what: &str, deadline: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while !check().await {
        assert!(
            started.elapsed() < deadline,
            "{what} not reached within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// 等到对外阶段为 `phase`，返回那时的状态。
pub async fn wait_phase(
    client: &IMClient,
    phase: ConnectionPhase,
    deadline: Duration,
) -> ConnectionStatus {
    let started = Instant::now();
    loop {
        let status = client.connection_status().await;
        if status.phase == phase {
            return status;
        }
        assert!(
            started.elapsed() < deadline,
            "phase {phase:?} not reached within {deadline:?}; last {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 等到连上（收到 CONNECT_ACK：`syncing` 或 `online`）。替身服务端不答同步请求，补同步要到上限才算就绪，
/// 关心「连没连上」的用例不必等它。
pub async fn wait_connected(client: &IMClient, deadline: Duration) -> ConnectionStatus {
    let started = Instant::now();
    loop {
        let status = client.connection_status().await;
        if matches!(
            status.phase,
            ConnectionPhase::Syncing | ConnectionPhase::Online
        ) {
            return status;
        }
        assert!(
            started.elapsed() < deadline,
            "not connected within {deadline:?}; last {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 建连并等到上线（收到 CONNECT_ACK 即返回；补同步在后台继续）。
pub async fn connect(client: &IMClient) {
    client
        .connect(USER_ID, Some("token"))
        .await
        .expect("connect");
}

pub async fn send_text(client: &IMClient, text: &str) -> String {
    let draft = client
        .message_build()
        .expect("message builder")
        .create_text(CONVERSATION_ID, text, false, &[])
        .await
        .expect("build text message");
    let client_msg_id = draft.client_msg_id.clone();
    client
        .message()
        .expect("message api")
        .send(draft)
        .await
        .expect("enqueue");
    client_msg_id
}
