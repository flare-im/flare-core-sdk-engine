//! 长连接断开后只有一条重连路径：会话的重连监视器。
//!
//! 本机回环（只绑 127.0.0.1）起一个真实的 flare-core WebSocket 服务端，SDK 经一个本机 TCP 中继
//! 连上去；在中继上切断连接模拟断网（服务端一直在），或向客户端注入一个非法帧模拟连接以错误终止。
//!
//! 此前 flare-core 客户端会在断开后的下一次发送时自己重连，与重连监视器新建的客户端并行：旧客户端
//! 带着建连时的令牌连上，监视器又新建一个，同一用户可能出现两条连接（网关按设备冲突处理），旧客户端
//! 连上后也没有重连后的同步。连接以错误终止时 SDK 只收到一个 ServerError，监视器不会动，只能等下一次
//! 发送把旧客户端拉起来。现在断开后的发送直接失败，重连只由监视器发起；断线期间发出的消息在重连后
//! 立即重发，不等发送超时。
//!
//! 协议竞速（原生端、Tauri 的默认：同时配了 ws_url 与 quic_url）也要走到监视器：QUIC 不可达、
//! WebSocket 胜出时，选中的连接以错误终止同样报断开。
//!
//! 过期的断开事件（传输层其实连着）不让监视器报 Reconnecting；断线期间不经可靠队列的操作（撤回、
//! 已读回执）立即以 `NOT_CONNECTED` 失败。

#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
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
use flare_im_core_sdk::prelude::*;
use flare_proto::common::{Ack, Message, SendAccepted, SendAckDurability, ack, send_ack};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const USER_ID: &str = "alice";
const CONVERSATION_ID: &str = "group-single-reconnect";

/// 一律放行，记下放行次数（每条建好的连接一次）。
#[derive(Default)]
struct AdmitAll {
    admitted: AtomicUsize,
}

#[async_trait]
impl Authenticator for AdmitAll {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        _metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> flare_core::common::error::Result<AuthResult> {
        self.admitted.fetch_add(1, Ordering::SeqCst);
        Ok(AuthResult::success(Some(USER_ID.to_string())).with_tenant_id("tenant-a"))
    }
}

/// message-ingest 的替身：每条 SEND 都按已持久化接收，记下到达的 `client_msg_id`。
#[derive(Default)]
struct AcceptAll {
    received: Mutex<Vec<String>>,
}

impl AcceptAll {
    fn received(&self) -> Vec<String> {
        self.received.lock().unwrap().clone()
    }
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

/// 一条经中继的连接：两个方向各一个转发任务；客户端方向的写端留着，用来注入字节。
struct RelayedConnection {
    to_client: Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
    upstream: JoinHandle<()>,
    downstream: JoinHandle<()>,
}

/// 只绑 127.0.0.1 的 TCP 中继：记下接受的连接数与同时在转发的连接数峰值。
struct Relay {
    address: String,
    accepted: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    accepted_at: Arc<Mutex<Vec<Instant>>>,
    connections: Arc<tokio::sync::Mutex<Vec<RelayedConnection>>>,
    accept_loop: JoinHandle<()>,
}

/// 转发任务结束（正常结束或被中止）时把在转发的连接数减一。
struct ActiveGuard(Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Relay {
    async fn start(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let address = listener.local_addr().expect("relay address").to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let accepted_at = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let accept_loop = tokio::spawn({
            let accepted = accepted.clone();
            let active = active.clone();
            let max_active = max_active.clone();
            let accepted_at = accepted_at.clone();
            let connections = connections.clone();
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    accepted_at.lock().unwrap().push(Instant::now());
                    let Ok(server) = TcpStream::connect(&target).await else {
                        continue;
                    };
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, Ordering::SeqCst);
                    let guard = Arc::new(ActiveGuard(active.clone()));
                    let (mut client_read, client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let to_client = Arc::new(tokio::sync::Mutex::new(client_write));
                    let upstream = tokio::spawn({
                        let guard = guard.clone();
                        async move {
                            let _guard = guard;
                            let _ = tokio::io::copy(&mut client_read, &mut server_write).await;
                        }
                    });
                    let downstream = tokio::spawn({
                        let to_client = to_client.clone();
                        async move {
                            let _guard = guard;
                            let mut buffer = vec![0u8; 16 * 1024];
                            loop {
                                let read = match server_read.read(&mut buffer).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(read) => read,
                                };
                                if to_client
                                    .lock()
                                    .await
                                    .write_all(&buffer[..read])
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            let _ = to_client.lock().await.shutdown().await;
                        }
                    });
                    connections.lock().await.push(RelayedConnection {
                        to_client,
                        upstream,
                        downstream,
                    });
                }
            }
        });
        Self {
            address,
            accepted,
            max_active,
            accepted_at,
            connections,
            accept_loop,
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn accepted_at(&self) -> Vec<Instant> {
        self.accepted_at.lock().unwrap().clone()
    }

    /// 断网：中止所有在转发的连接，两端的套接字随之关闭。中继照常接受新连接。
    async fn cut(&self) {
        for connection in self.connections.lock().await.drain(..) {
            connection.upstream.abort();
            connection.downstream.abort();
        }
    }

    /// 连接以错误终止：停止向客户端转发，注入一个保留操作码的 WebSocket 帧（客户端判协议错误），
    /// 再断开服务端一侧。客户端一侧的套接字不主动关，让客户端先读到这一帧。已经结束的连接（例如竞速
    /// 时建好又被客户端丢掉的那条）跳过。
    async fn corrupt(&self) -> Vec<Arc<tokio::sync::Mutex<OwnedWriteHalf>>> {
        let mut kept = Vec::new();
        for connection in self.connections.lock().await.drain(..) {
            if connection.downstream.is_finished() {
                connection.upstream.abort();
                continue;
            }
            connection.downstream.abort();
            let _ = connection.downstream.await;
            let injected = {
                let mut to_client = connection.to_client.lock().await;
                // FIN + 操作码 3（保留的数据帧），服务端到客户端不加掩码，载荷长度 0
                to_client.write_all(&[0x83, 0x00]).await.is_ok() && to_client.flush().await.is_ok()
            };
            tokio::time::sleep(Duration::from_millis(200)).await;
            connection.upstream.abort();
            if injected {
                kept.push(connection.to_client);
            }
        }
        assert!(
            !kept.is_empty(),
            "no live connection to inject a malformed frame into"
        );
        kept
    }

    async fn stop(self) {
        self.accept_loop.abort();
        self.cut().await;
    }
}

/// 本用例的 SDK 数据根：在 Cargo 给集成测试的临时目录下，用例结束时删掉。
fn data_root(case: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("flare-im-core-sdk-single-reconnect-path")
        .join(format!("{case}-{}", std::process::id()))
}

/// 重连间隔 2 s：监视器第一次重连在断开后 1.6–2.4 s 之间；发送超时 60 s，远长于用例时长。
async fn connected_client(ws_url: &str, data_root: &Path) -> IMClient {
    connect_client(ws_url, None, data_root).await
}

/// `quic_url` 给了就走协议竞速，顺序取 SDK 默认（QUIC 第一、WebSocket 第二）。
async fn connect_client(ws_url: &str, quic_url: Option<String>, data_root: &Path) -> IMClient {
    let mut config = SdkConfig::new(ws_url);
    config.ack_timeout_secs = Some(60);
    if let Some(quic_url) = quic_url {
        config.quic_url = Some(quic_url);
        // 连接超时用默认值（30 s）：开了 quic 特性时不可达的 QUIC 握手要等到连接超时，竞速不等它，
        // WebSocket 一建好就胜出。没开 quic 特性时 QUIC 立即失败。
        assert!(
            config.effective_protocol_race_order().is_some(),
            "ws_url + quic_url under the default policy is a protocol race"
        );
    }
    let client = IMClient::builder()
        .config(config)
        .stores(in_memory_im_provider())
        .build()
        .expect("build IMClient");
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(data_root.display().to_string()),
                ws_url: Some(ws_url.to_string()),
                reconnect_interval_secs: Some(2),
                ..Default::default()
            }),
        )
        .await
        .expect("init SDK config");
    client
        .connect(USER_ID, Some("token"))
        .await
        .expect("connect");
    wait_until("Ready after connect", Duration::from_secs(10), || async {
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

/// 应用看得到的连接与发送事件，带到达时刻。
#[derive(Default)]
struct Observed {
    disconnected: Mutex<Vec<(Instant, String)>>,
    reconnecting: Mutex<Vec<(Instant, u32)>>,
    kicked_off: Mutex<Vec<String>>,
    accepted: Mutex<Vec<String>>,
    failed: Mutex<Vec<(String, String)>>,
}

impl Observed {
    fn record(client: &IMClient) -> (Arc<Self>, Vec<Subscription>) {
        let observed = Arc::new(Self::default());
        let sink = observed.clone();
        let connection = client
            .on_event_kind(SdkEventKind::Connection, move |event| {
                match event.as_ref() {
                    SdkEvent::Connection(ConnectionEvent::Disconnected { reason }) => {
                        sink.disconnected
                            .lock()
                            .unwrap()
                            .push((Instant::now(), reason.clone()));
                    }
                    SdkEvent::Connection(ConnectionEvent::Reconnecting { attempt }) => {
                        sink.reconnecting
                            .lock()
                            .unwrap()
                            .push((Instant::now(), *attempt));
                    }
                    SdkEvent::Connection(ConnectionEvent::KickedOff { reason }) => {
                        sink.kicked_off.lock().unwrap().push(reason.clone());
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

    fn first_disconnect(&self) -> Option<(Instant, String)> {
        self.disconnected.lock().unwrap().first().cloned()
    }

    fn reconnecting(&self) -> Vec<(Instant, u32)> {
        self.reconnecting.lock().unwrap().clone()
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

/// 断网期间发消息：旧客户端不在发送时自己重连，第二条连接只由重连监视器在退避之后建立；
/// 全程最多一条转发中的连接；断线期间发出的消息在重连后立即重发并被接收，不等 60 s 的发送超时。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_send_during_an_outage_waits_for_the_reconnect_watcher() {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest.clone()).await;
    let relay = Relay::start(server_address).await;
    let root = data_root("outage-send");
    let client = connected_client(&format!("ws://{}", relay.address), &root).await;
    assert_eq!(relay.accepted(), 1);
    let (observed, _subscriptions) = Observed::record(&client);

    relay.cut().await;
    wait_until(
        "SDK sees the disconnect",
        Duration::from_secs(5),
        || async { observed.first_disconnect().is_some() },
    )
    .await;
    let (disconnected_at, reason) = observed.first_disconnect().unwrap();

    let client_msg_id = send_text(&client, "sent while offline").await;

    // 监视器最早在断开后 1.6 s 重连；在那之前不能有任何新连接。
    tokio::time::sleep(Duration::from_millis(1_200).saturating_sub(disconnected_at.elapsed()))
        .await;
    assert_eq!(
        relay.accepted(),
        1,
        "a send must not reconnect the old client (disconnect reason: {reason})"
    );

    wait_until("Ready again", Duration::from_secs(10), || async {
        relay.accepted() >= 2 && client.state() == SdkState::Ready
    })
    .await;
    let second = relay.accepted_at()[1];
    let first_attempt = observed
        .reconnecting()
        .first()
        .copied()
        .expect("the watcher reports its attempt");
    assert_eq!(first_attempt.1, 1);
    assert!(
        first_attempt.0 <= second,
        "the second connection comes from the watcher's attempt"
    );
    assert!(
        second.duration_since(disconnected_at) >= Duration::from_millis(1_500),
        "the reconnect waits for the watcher's backoff: {:?}",
        second.duration_since(disconnected_at)
    );

    wait_until(
        "the offline message is resent and accepted",
        Duration::from_secs(5),
        || async { observed.accepted.lock().unwrap().contains(&client_msg_id) },
    )
    .await;
    assert_eq!(ingest.received(), vec![client_msg_id.clone()]);
    assert!(observed.failed.lock().unwrap().is_empty());
    assert!(observed.kicked_off.lock().unwrap().is_empty());

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(relay.accepted(), 2, "exactly one reconnect");
    assert_eq!(gate.admitted.load(Ordering::SeqCst), 2);
    assert_eq!(
        relay.max_active.load(Ordering::SeqCst),
        1,
        "never two connections for the same user at once"
    );

    let _ = client.disconnect().await;
    relay.stop().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// 连接以错误终止（客户端读到非法帧）：没有任何发送，重连监视器照样接手，在退避后重连回到 Ready。
/// 此前 SDK 只收到 ServerError，监视器不动，状态停在 Ready、连接却已经死了。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_error_hands_over_to_the_reconnect_watcher() {
    transport_error_hands_over_to_the_reconnect_watcher("transport-error", None).await;
}

/// 协议竞速下 QUIC 不可达（UDP 端口上没有服务）、WebSocket 胜出，选中的连接以错误终止：同样交给监视器。
/// 此前 flare-core 在竞速模式下按文本把这个错误当作关闭未选中连接的正常错误吞掉，SDK 什么也收不到，
/// 停在 Ready、连接却已经死了；发送前重连关掉之后，再没有别的路径把它拉起来。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_error_in_a_protocol_race_hands_over_to_the_reconnect_watcher() {
    let unreachable_quic = std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("bind a probe UDP socket")
        .local_addr()
        .expect("probe address")
        .port();
    transport_error_hands_over_to_the_reconnect_watcher(
        "race-transport-error",
        Some(format!("quic://127.0.0.1:{unreachable_quic}")),
    )
    .await;
}

async fn transport_error_hands_over_to_the_reconnect_watcher(case: &str, quic_url: Option<String>) {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest.clone()).await;
    let relay = Relay::start(server_address).await;
    let root = data_root(case);
    let client = connect_client(&format!("ws://{}", relay.address), quic_url, &root).await;
    // 一次建连经中继只拨一次：竞速胜出的 WebSocket 直接在竞速建好的连接上发 CONNECT，不重新拨号
    let dials_per_connect = relay.accepted();
    assert_eq!(dials_per_connect, 1, "one dial per connect");
    let (observed, _subscriptions) = Observed::record(&client);

    let corrupted_at = Instant::now();
    let _client_sockets = relay.corrupt().await;
    wait_until(
        "SDK sees the disconnect",
        Duration::from_secs(5),
        || async { observed.first_disconnect().is_some() },
    )
    .await;
    let (_, reason) = observed.first_disconnect().unwrap();
    assert!(
        reason.starts_with("transport error: "),
        "the error is handed over as a disconnect: {reason}"
    );

    wait_until("Ready again", Duration::from_secs(10), || async {
        relay.accepted() >= 2 * dials_per_connect && client.state() == SdkState::Ready
    })
    .await;
    let second = relay.accepted_at()[dials_per_connect];
    assert!(
        second.duration_since(corrupted_at) >= Duration::from_millis(1_500),
        "the watcher reconnects after its backoff: {:?}",
        second.duration_since(corrupted_at)
    );
    assert_eq!(
        observed.reconnecting().first().map(|(_, attempt)| *attempt),
        Some(1)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        relay.accepted(),
        2 * dials_per_connect,
        "exactly one reconnect"
    );
    assert!(observed.kicked_off.lock().unwrap().is_empty());

    // 重连后的连接可用
    let client_msg_id = send_text(&client, "after the reconnect").await;
    wait_until("message accepted", Duration::from_secs(5), || async {
        observed.accepted.lock().unwrap().contains(&client_msg_id)
    })
    .await;

    let _ = client.disconnect().await;
    relay.stop().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// 过期的断开事件（它在队列里排着的时候，之前的一次重连已经成功）：传输层仍连着时，重连监视器不重连，
/// 也不报 Reconnecting，状态停在 Ready。此前监视器照常报一次 Reconnecting，退避之后才发现已经连上，
/// 界面上平白闪一次「重连中」。这里直接向会话总线发一条这样的断开事件来模拟。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_disconnect_does_not_announce_a_reconnect() {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest.clone()).await;
    let relay = Relay::start(server_address).await;
    let root = data_root("stale-disconnect");
    let client = connected_client(&format!("ws://{}", relay.address), &root).await;
    let (observed, _subscriptions) = Observed::record(&client);

    client
        .bus()
        .await
        .expect("session bus")
        .publish(SdkEvent::Connection(ConnectionEvent::Disconnected {
            reason: "transport error: left over from an earlier connection".to_string(),
        }));
    wait_until(
        "the stale disconnect is delivered",
        Duration::from_secs(2),
        || async { observed.first_disconnect().is_some() },
    )
    .await;

    // 重连间隔 2 s：第一次重连若会发生，在 1.6–2.4 s 之间
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        observed.reconnecting().is_empty(),
        "a stale disconnect must not announce a reconnect: {:?}",
        observed.reconnecting()
    );
    assert_eq!(client.state(), SdkState::Ready);
    assert_eq!(relay.accepted(), 1, "no reconnect");

    // 连接照常可用
    let client_msg_id = send_text(&client, "after a stale disconnect").await;
    wait_until("message accepted", Duration::from_secs(5), || async {
        observed.accepted.lock().unwrap().contains(&client_msg_id)
    })
    .await;

    let _ = client.disconnect().await;
    relay.stop().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// 断线期间，不经可靠队列的操作（撤回、阅后即焚的已读回执）立即失败，错误码是 `NOT_CONNECTED`，
/// 应用据此提示未连接、等会话回到 Ready 后再试；输入状态是有损信令，断线时直接丢弃、不报错。
/// 此前 flare-core 返回的 `NOT_CONNECTED` 被 SDK 一律改成 `CONNECTION_FAILED`。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operations_outside_the_send_queue_fail_fast_with_not_connected_while_offline() {
    let gate = Arc::new(AdmitAll::default());
    let ingest = Arc::new(AcceptAll::default());
    let (server, server_address) = start_server(gate.clone(), ingest.clone()).await;
    let relay = Relay::start(server_address).await;
    let root = data_root("offline-direct-ops");
    let client = connected_client(&format!("ws://{}", relay.address), &root).await;
    let (observed, _subscriptions) = Observed::record(&client);

    let client_msg_id = send_text(&client, "sent before the outage").await;
    wait_until("message accepted", Duration::from_secs(5), || async {
        observed.accepted.lock().unwrap().contains(&client_msg_id)
    })
    .await;

    relay.cut().await;
    wait_until(
        "SDK sees the disconnect",
        Duration::from_secs(5),
        || async { observed.first_disconnect().is_some() },
    )
    .await;

    // 重连监视器最早在断开后 1.6 s 重连：在那之前做完
    let message = client.message().expect("message api");
    let started = Instant::now();
    let recall = message
        .recall(&client_msg_id)
        .await
        .expect_err("recall needs the connection");
    assert_eq!(recall.code(), Some(ErrorCode::NotConnected), "{recall}");
    let read = message
        .mark_read_and_burn(&client_msg_id)
        .await
        .expect_err("a read receipt needs the connection");
    assert_eq!(read.code(), Some(ErrorCode::NotConnected), "{read}");
    message
        .typing(CONVERSATION_ID, true)
        .await
        .expect("typing is best effort and dropped while offline");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "operations outside the send queue fail fast: {:?}",
        started.elapsed()
    );
    assert_eq!(relay.accepted(), 1, "none of them reconnects");

    let _ = client.disconnect().await;
    relay.stop().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}
