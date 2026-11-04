//! 「看起来连着、其实早已死掉」的半开连接要被及时戳穿，以及 CONNECT 带上真实的应用版本。
//!
//! 本机回环（只绑 127.0.0.1）起一个真实的 flare-core WebSocket 服务端，SDK 经一个本机 TCP 中继连上去。
//! 中继的「冻结」模拟半开连接：两端套接字都不关，只是再也不转发任何字节——本端写入照样成功，
//! PONG、SendAck 却永远不来，传输层也收不到任何关闭信号。只靠周期心跳（前台 30 s 一次、90 s 判死）
//! 要一分多钟才发现；这里验证两个触发点在验活窗口（8 s）内把它断开，交给重连监视器重连：
//!
//! - 平台报告网络可用（切网、恢复）时，对仍处 Ready 的连接立即验活；
//! - 消息发出 5 s 仍没等到 SendAck 时验活一次。

#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use flare_core::client::heartbeat::manager::HEARTBEAT_TIMEOUT_REASON;
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::device::DeviceInfo;
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, frame_with_payload_command,
    payload_command::Type as PayloadType,
};
use flare_core::server::{
    AuthResult, Authenticator, FlareServer, FlareServerBuilder, ServerEventHandler,
};
use flare_im_core_sdk::prelude::*;
use flare_proto::common::{Ack, Message, SendAccepted, SendAckDurability, ack, send_ack};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const USER_ID: &str = "alice";
const CONVERSATION_ID: &str = "group-connection-probe";

/// 一律放行，记下每次 CONNECT 带来的元数据。
#[derive(Default)]
struct AdmitAll {
    connect_metadata: Mutex<Vec<HashMap<String, String>>>,
}

impl AdmitAll {
    fn connects(&self) -> Vec<HashMap<String, String>> {
        self.connect_metadata.lock().unwrap().clone()
    }
}

#[async_trait]
impl Authenticator for AdmitAll {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> flare_core::common::error::Result<AuthResult> {
        let metadata = metadata
            .map(|metadata| {
                metadata
                    .iter()
                    .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).into_owned()))
                    .collect()
            })
            .unwrap_or_default();
        self.connect_metadata.lock().unwrap().push(metadata);
        Ok(AuthResult::success(Some(USER_ID.to_string())).with_tenant_id("tenant-a"))
    }
}

/// message-ingest 的替身：每条 SEND 都按已持久化接收。
#[derive(Default)]
struct AcceptAll {
    received: Mutex<Vec<String>>,
}

#[async_trait]
impl ServerEventHandler for AcceptAll {
    async fn handle_message(
        &self,
        command: &PayloadCommand,
        _connection_id: &str,
    ) -> flare_core::common::error::Result<Option<Frame>> {
        let message = Message::decode(command.payload.as_slice()).expect("SEND carries a Message");
        self.received
            .lock()
            .unwrap()
            .push(message.client_msg_id.clone());
        let ack = Ack {
            ack_id: None,
            ack_at: None,
            payload: Some(ack::Payload::Send(flare_proto::common::SendAck {
                client_msg_id: message.client_msg_id.clone(),
                conversation_id: message.conversation_id,
                ack_id: None,
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: format!("server-{}", message.client_msg_id),
                    conversation_seq: 1,
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

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

async fn start_server(gate: Arc<AdmitAll>, ingest: Arc<AcceptAll>) -> (FlareServer, String) {
    let address = format!("127.0.0.1:{}", free_tcp_port());
    let server = FlareServerBuilder::new(address.clone(), ingest)
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_protocol_address(TransportProtocol::WebSocket, address.clone())
        .enable_auth()
        .with_authenticator(gate)
        .build()
        .expect("build WebSocket server");
    server.start().await.expect("start WebSocket server");
    (server, address)
}

/// 只绑 127.0.0.1 的 TCP 中继。[`Relay::freeze`] 让已有连接变成半开：不关任何套接字，
/// 只是此后读到的字节一律丢弃、不再转发。之后新建的连接照常转发。
struct Relay {
    address: String,
    accepted: Arc<AtomicUsize>,
    frozen: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept_loop: JoinHandle<()>,
}

async fn forward<R, W>(mut from: R, mut to: W, frozen: Arc<AtomicBool>)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if frozen.load(Ordering::SeqCst) {
            // 半开：两端都还开着，字节就此消失。继续读，免得对端写满缓冲后阻塞。
            continue;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    // 中继里转发结束的一侧照常关闭；冻结期间不会走到这里（读到的字节被吞掉，连接一直挂着）。
    let _ = to.shutdown().await;
}

impl Relay {
    async fn start(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let address = listener.local_addr().expect("relay address").to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let frozen: Arc<Mutex<Vec<Arc<AtomicBool>>>> = Arc::default();
        let tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let accept_loop = tokio::spawn({
            let accepted = accepted.clone();
            let frozen = frozen.clone();
            let tasks = tasks.clone();
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    let Ok(server) = TcpStream::connect(&target).await else {
                        continue;
                    };
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let flag = Arc::new(AtomicBool::new(false));
                    frozen.lock().unwrap().push(flag.clone());
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    let upstream = tokio::spawn(forward(client_read, server_write, flag.clone()));
                    let downstream = tokio::spawn(forward(server_read, client_write, flag));
                    tasks.lock().unwrap().extend([upstream, downstream]);
                }
            }
        });
        Self {
            address,
            accepted,
            frozen,
            tasks,
            accept_loop,
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// 已有连接全部变成半开。
    fn freeze(&self) {
        for flag in self.frozen.lock().unwrap().iter() {
            flag.store(true, Ordering::SeqCst);
        }
    }

    fn stop(self) {
        self.accept_loop.abort();
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

fn data_root(case: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("flare-im-core-sdk-connection-probe")
        .join(format!("{case}-{}", std::process::id()))
}

/// 走各端包的真实路径建连：`init`（overlay 带重连间隔 2 s，`app` 给了就带应用版本信息）→
/// `login`（prepare 把 overlay 合进构建期配置、装配引擎，再建连）。
async fn connected_client(
    ws_url: &str,
    data_root: &std::path::Path,
    ack_timeout_secs: u64,
    app: Option<(&str, u64, &str)>,
) -> IMClient {
    let mut config = SdkConfig::new(ws_url);
    config.ack_timeout_secs = Some(ack_timeout_secs);
    let client = IMClient::builder()
        .config(config)
        .stores(in_memory_im_provider())
        .build()
        .expect("build IMClient");
    let (app_version, app_build, distribution) = match app {
        Some((version, build, distribution)) => (
            Some(version.to_string()),
            Some(build),
            Some(distribution.to_string()),
        ),
        None => (None, None, None),
    };
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(data_root.display().to_string()),
                ws_url: Some(ws_url.to_string()),
                reconnect_interval_secs: Some(2),
                app_version,
                app_build,
                distribution,
                ..Default::default()
            }),
        )
        .await
        .expect("init SDK config");
    client
        .login(
            USER_ID,
            Some("token"),
            LoginDbKind::IndexedDb(in_memory_im_provider()),
            |_, _| {},
        )
        .await
        .expect("log in");
    wait_until("Ready after login", Duration::from_secs(10), || async {
        client.state() == SdkState::Ready
    })
    .await;
    client
}

async fn wait_until<F, Fut>(what: &str, deadline: Duration, mut check: F)
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

/// 应用看得到的断开原因与被接收的消息，带到达时刻。
#[derive(Default)]
struct Observed {
    disconnected: Mutex<Vec<(Instant, String)>>,
    accepted: Mutex<Vec<String>>,
}

impl Observed {
    fn record(client: &IMClient) -> (Arc<Self>, Vec<Subscription>) {
        let observed = Arc::new(Self::default());
        let sink = observed.clone();
        let connection = client
            .on_event_kind(SdkEventKind::Connection, move |event| {
                if let SdkEvent::Connection(ConnectionEvent::Disconnected { reason }) =
                    event.as_ref()
                {
                    sink.disconnected
                        .lock()
                        .unwrap()
                        .push((Instant::now(), reason.clone()));
                }
            })
            .expect("subscribe connection events");
        let sink = observed.clone();
        let message = client
            .on_event_kind(SdkEventKind::Message, move |event| {
                if let SdkEvent::Message(MessageEvent::SendAck { ack }) = event.as_ref()
                    && matches!(ack.result, Some(send_ack::Result::Accepted(_)))
                {
                    sink.accepted
                        .lock()
                        .unwrap()
                        .push(ack.client_msg_id.clone());
                }
            })
            .expect("subscribe message events");
        (observed, vec![connection, message])
    }

    fn first_disconnect(&self) -> Option<(Instant, String)> {
        self.disconnected.lock().unwrap().first().cloned()
    }
}

async fn send_text(client: &IMClient, text: &str) -> String {
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

/// 网络切换报告到达时连接已半开（仍处 Ready、传输仍报已连）：SDK 立即验活，8 s 验活窗口内
/// 没有 PONG 就以心跳超时断开，重连监视器随即重连回到 Ready。不验活的话，要等前台 90 s 的
/// 心跳超时才会发现。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_network_change_probes_a_half_open_connection() {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest).await;
    let relay = Relay::start(server_address).await;
    let root = data_root("network-change");
    let client = connected_client(&format!("ws://{}", relay.address), &root, 60, None).await;
    let (observed, _subscriptions) = Observed::record(&client);

    relay.freeze();
    let reported_at = Instant::now();
    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            interface: Some("wifi".to_string()),
            ..Default::default()
        })
        .await
        .expect("network change");

    wait_until(
        "the half-open connection is closed by the probe",
        Duration::from_secs(12),
        || async { observed.first_disconnect().is_some() },
    )
    .await;
    let (disconnected_at, reason) = observed.first_disconnect().unwrap();
    let after = disconnected_at.duration_since(reported_at);
    assert!(
        reason.contains(HEARTBEAT_TIMEOUT_REASON),
        "closed by the heartbeat verdict, so the watcher reconnects: {reason}"
    );
    assert!(
        after <= Duration::from_secs(10),
        "closed within the 8 s probe window, not the 90 s heartbeat timeout: {after:?}"
    );

    wait_until("Ready again", Duration::from_secs(10), || async {
        relay.accepted() >= 2 && client.state() == SdkState::Ready
    })
    .await;

    let _ = client.disconnect().await;
    relay.stop();
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// 消息发出后连接才变成半开：帧「发送成功」，SendAck 却不来。5 s 后发件箱请求验活，再过不到
/// 8 s 连接以心跳超时断开、重连；这条消息在新连接上重发并被接收，不必等重试在死连接上耗尽。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unanswered_send_probes_the_connection() {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest.clone()).await;
    let relay = Relay::start(server_address).await;
    let root = data_root("unanswered-send");
    // 发送超时 15 s：远长于验活的 5 s 触发点，验活先于任何超时重发发生。
    let client = connected_client(&format!("ws://{}", relay.address), &root, 15, None).await;
    let (observed, _subscriptions) = Observed::record(&client);

    relay.freeze();
    let sent_at = Instant::now();
    let client_msg_id = send_text(&client, "sent into a half-open connection").await;

    wait_until(
        "the unanswered send gets the connection probed and closed",
        Duration::from_secs(18),
        || async { observed.first_disconnect().is_some() },
    )
    .await;
    let (disconnected_at, reason) = observed.first_disconnect().unwrap();
    let after = disconnected_at.duration_since(sent_at);
    assert!(
        reason.contains(HEARTBEAT_TIMEOUT_REASON),
        "closed by the heartbeat verdict: {reason}"
    );
    assert!(
        after >= Duration::from_secs(5),
        "no probe before the send has gone unanswered for 5 s: {after:?}"
    );
    assert!(
        after <= Duration::from_secs(15),
        "5 s stall (checked once a second) + 8 s probe window, not the 90 s heartbeat timeout: {after:?}"
    );

    wait_until(
        "the message is resent on the new connection and accepted",
        Duration::from_secs(15),
        || async { observed.accepted.lock().unwrap().contains(&client_msg_id) },
    )
    .await;
    assert!(relay.accepted() >= 2);

    let _ = client.disconnect().await;
    relay.stop();
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// CONNECT 带上宿主给的真实版本（`app_version` / `app_build` / `distribution`）与引擎版本
/// `sdk_version`；没给版本的宿主不再被冒充成 "1.0.0"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_reports_the_real_app_version() {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest).await;
    let ws_url = format!("ws://{server_address}");

    let root = data_root("app-version");
    let client = connected_client(&ws_url, &root, 60, Some(("3.2.1", 30201, "appstore"))).await;
    let _ = client.disconnect().await;
    let unversioned_root = data_root("no-app-version");
    let unversioned = connected_client(&ws_url, &unversioned_root, 60, None).await;
    let _ = unversioned.disconnect().await;

    let connects = gate.connects();
    assert_eq!(connects.len(), 2);
    let versioned = &connects[0];
    assert_eq!(
        versioned.get("app_version").map(String::as_str),
        Some("3.2.1")
    );
    assert_eq!(
        versioned.get("app_build").map(String::as_str),
        Some("30201")
    );
    assert_eq!(
        versioned.get("distribution").map(String::as_str),
        Some("appstore")
    );
    let sdk_version = versioned
        .get("sdk_version")
        .expect("engine version reported");
    assert!(!sdk_version.is_empty());

    let unversioned = &connects[1];
    assert!(!unversioned.contains_key("app_version"), "{unversioned:?}");
    assert!(!unversioned.contains_key("app_build"), "{unversioned:?}");
    assert!(!unversioned.contains_key("distribution"), "{unversioned:?}");
    assert_eq!(unversioned.get("sdk_version"), Some(sdk_version));

    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&unversioned_root);
}
