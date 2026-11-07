//! 服务端暂时性拒发时，SDK 用同一个 `client_msg_id` 有界退避重发，而不是把消息判失败。
//!
//! message-ingest 读不到租户投影、上游暂不可用、同一条消息首发还在处理中等情况下，网关回
//! `SendAck.error`，码为 `UNAVAILABLE`（还有超时、限流、要求重试）。这些拒绝过一会儿重发即可，
//! ingest 按 `client_msg_id` 做摄入幂等，重发不会多出一条消息。其余拒绝（无权限、租户不可用、
//! 参数非法等）重发也不会变，仍然立即判失败。
//!
//! 本机回环（只绑 127.0.0.1）起一个真实的 flare-core WebSocket 服务端，消息处理器按用例脚本
//! 回 SendAck；SDK 走自己的完整发送链路：可靠队列 → SEND 帧 → SendAck 下行 → 队列收敛。

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
use flare_proto::common::{
    Ack, ErrorCode as ProtoErrorCode, ErrorDetail, Message, SendAccepted, SendAckDurability, ack,
    send_ack,
};
use prost::Message as _;

const USER_ID: &str = "alice";
const CONVERSATION_ID: &str = "group-send-retry";
const SERVER_MSG_ID: &str = "server-msg-1";

struct AdmitAll;

#[async_trait]
impl Authenticator for AdmitAll {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        _metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> flare_core::common::error::Result<AuthResult> {
        Ok(AuthResult::success(Some(USER_ID.to_string())).with_tenant_id("tenant-a"))
    }
}

/// message-ingest 的替身：前 `rejections` 次 SEND 回给定的错误，之后按已持久化接收。
/// 同一 `client_msg_id` 只分配一次服务端 id，与 ingest 的摄入幂等一致。
struct ScriptedIngest {
    rejections: AtomicUsize,
    error: ErrorDetail,
    received: Mutex<Vec<String>>,
}

impl ScriptedIngest {
    fn new(rejections: usize, code: ProtoErrorCode, reason: &str, message: &str) -> Self {
        Self {
            rejections: AtomicUsize::new(rejections),
            error: ErrorDetail {
                code: code as i32,
                reason: reason.to_string(),
                message: message.to_string(),
                track: String::new(),
            },
            received: Mutex::new(Vec::new()),
        }
    }

    /// 收到的 SEND 帧，按到达顺序记各自的 `client_msg_id`。
    fn received(&self) -> Vec<String> {
        self.received.lock().unwrap().clone()
    }
}

#[allow(
    deprecated,
    reason = "fetch_update is renamed try_update on newer toolchains, and try_update is unstable at the declared MSRV"
)]
#[async_trait]
impl ServerEventHandler for ScriptedIngest {
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
        let reject = self
            .rejections
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        let result = if reject {
            send_ack::Result::Error(self.error.clone())
        } else {
            send_ack::Result::Accepted(SendAccepted {
                server_msg_id: SERVER_MSG_ID.to_string(),
                conversation_seq: 1,
                server_time: 1_700_000_000_000,
                durability: SendAckDurability::Persisted as i32,
            })
        };
        let ack = Ack {
            ack_id: None,
            ack_at: None,
            payload: Some(ack::Payload::Send(flare_proto::common::SendAck {
                client_msg_id: message.client_msg_id,
                conversation_id: message.conversation_id,
                ack_id: None,
                result: Some(result),
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

async fn start_server(ingest: Arc<ScriptedIngest>) -> (FlareServer, String) {
    let address = format!("127.0.0.1:{}", free_tcp_port());
    let server = FlareServerBuilder::new(address.clone(), ingest)
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_protocol_address(TransportProtocol::WebSocket, address.clone())
        .enable_auth()
        .with_authenticator(Arc::new(AdmitAll))
        .build()
        .expect("build WebSocket server");
    server.start().await.expect("start WebSocket server");
    (server, format!("ws://{address}"))
}

/// 本用例的 SDK 数据根：在 Cargo 给集成测试的临时目录下，用例结束时删掉。
fn data_root(case: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("flare-im-core-sdk-transient-send-rejection")
        .join(format!("{case}-{}", std::process::id()))
}

/// 开发模式接入：单个接入点，端点就是本机服务端。
fn development_access(ws_url: &str) -> AccessConfig {
    AccessConfig::development(DevelopmentEndpoints {
        ws_url: Some(ws_url.to_string()),
        ..Default::default()
    })
}

/// 内存存储、已连上的客户端；`ack_max_retries` 是一条消息的总尝试次数。
async fn connected_client(ws_url: &str, data_root: &Path, ack_max_retries: u32) -> IMClient {
    let mut config = SdkConfig::with_access(development_access(ws_url));
    config.ack_max_retries = Some(ack_max_retries);
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
                access: Some(development_access(ws_url)),
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 应用看得到的发送结果：成功的 SendAck 与失败事件。
#[derive(Default)]
struct SendOutcomes {
    accepted: Mutex<Vec<String>>,
    error_acks: Mutex<Vec<String>>,
    failed: Mutex<Vec<(String, String)>>,
}

impl SendOutcomes {
    fn record(client: &IMClient) -> (Arc<Self>, Subscription) {
        let outcomes = Arc::new(Self::default());
        let sink = outcomes.clone();
        let subscription = client
            .on_event_kind(SdkEventKind::Message, move |event| match event.as_ref() {
                SdkEvent::Message(MessageEvent::SendAck { ack }) => match ack.result.as_ref() {
                    Some(send_ack::Result::Accepted(_)) => {
                        sink.accepted
                            .lock()
                            .unwrap()
                            .push(ack.client_msg_id.clone());
                    }
                    Some(send_ack::Result::Error(_)) => {
                        sink.error_acks
                            .lock()
                            .unwrap()
                            .push(ack.client_msg_id.clone());
                    }
                    None => {}
                },
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
        (outcomes, subscription)
    }

    fn accepted(&self) -> Vec<String> {
        self.accepted.lock().unwrap().clone()
    }

    fn error_acks(&self) -> Vec<String> {
        self.error_acks.lock().unwrap().clone()
    }

    fn failed(&self) -> Vec<(String, String)> {
        self.failed.lock().unwrap().clone()
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

async fn local_messages(client: &IMClient) -> Vec<IMMessage> {
    client
        .stores_async()
        .await
        .expect("stores")
        .messages
        .get_by_conversation(CONVERSATION_ID, 0, 50)
        .await
        .expect("local messages")
}

/// ingest 前两次回 `UNAVAILABLE`（投影读不到），第三次接收：SDK 退避后用同一个
/// `client_msg_id` 重发两次，消息最终成为一条已发送的消息，中途应用看不到失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_rejection_is_resent_with_the_same_id_until_accepted() {
    let ingest = Arc::new(ScriptedIngest::new(
        2,
        ProtoErrorCode::Unavailable,
        "SERVICE_UNAVAILABLE",
        "tenant runtime projection unavailable",
    ));
    let (server, ws_url) = start_server(ingest.clone()).await;
    let root = data_root("resent");
    let client = connected_client(&ws_url, &root, 3).await;
    let (outcomes, _subscription) = SendOutcomes::record(&client);

    let client_msg_id = send_text(&client, "hello after an outage").await;
    wait_until("message accepted", Duration::from_secs(15), || async {
        !outcomes.accepted().is_empty() || !outcomes.failed().is_empty()
    })
    .await;

    assert!(
        outcomes.failed().is_empty(),
        "a transient rejection must not fail the message: {:?}",
        outcomes.failed()
    );
    assert!(
        outcomes.error_acks().is_empty(),
        "the app sees no error ack while the SDK is still resending"
    );
    assert_eq!(outcomes.accepted(), std::slice::from_ref(&client_msg_id));
    assert_eq!(
        ingest.received(),
        vec![client_msg_id.clone(); 3],
        "two rejections then one acceptance, every attempt with the same client_msg_id"
    );

    let messages = local_messages(&client).await;
    assert_eq!(
        messages.len(),
        1,
        "resending must not create a second local message: {messages:?}"
    );
    let message = &messages[0];
    assert_eq!(message.client_msg_id, client_msg_id);
    assert_eq!(message.server_id, SERVER_MSG_ID);
    assert_eq!(message.status, MessageStatus::Sent as i32);
    assert!(!message.local_state.sending && !message.local_state.failed);

    // 接收之后不再重发。
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(ingest.received().len(), 3);

    let _ = client.disconnect().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// 暂时性拒绝一直不消失：重发次数以 `ack_max_retries` 为界，用完后按服务端给的原因判失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_rejections_are_retried_a_bounded_number_of_times() {
    let ingest = Arc::new(ScriptedIngest::new(
        usize::MAX,
        ProtoErrorCode::Unavailable,
        "SERVICE_UNAVAILABLE",
        "tenant runtime projection unavailable",
    ));
    let (server, ws_url) = start_server(ingest.clone()).await;
    let root = data_root("bounded");
    let client = connected_client(&ws_url, &root, 2).await;
    let (outcomes, _subscription) = SendOutcomes::record(&client);

    let client_msg_id = send_text(&client, "still unavailable").await;
    wait_until("message failed", Duration::from_secs(15), || async {
        !outcomes.failed().is_empty()
    })
    .await;

    assert_eq!(
        outcomes.failed(),
        [(
            client_msg_id.clone(),
            "tenant runtime projection unavailable".to_string()
        )]
    );
    assert_eq!(ingest.received(), vec![client_msg_id.clone(); 2]);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(
        ingest.received().len(),
        2,
        "no attempt after the budget is spent"
    );

    let messages = local_messages(&client).await;
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert_eq!(messages[0].status, MessageStatus::Failed as i32);

    let _ = client.disconnect().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}

/// 终态拒绝（租户不可用）：立即判失败，不重发。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_rejection_fails_at_once_without_resending() {
    let ingest = Arc::new(ScriptedIngest::new(
        usize::MAX,
        ProtoErrorCode::PermissionDenied,
        "TENANT_UNAVAILABLE",
        "TENANT_UNAVAILABLE",
    ));
    let (server, ws_url) = start_server(ingest.clone()).await;
    let root = data_root("terminal");
    let client = connected_client(&ws_url, &root, 3).await;
    let (outcomes, _subscription) = SendOutcomes::record(&client);

    let client_msg_id = send_text(&client, "tenant suspended").await;
    wait_until("message failed", Duration::from_secs(10), || async {
        !outcomes.failed().is_empty()
    })
    .await;

    assert_eq!(
        outcomes.failed(),
        [(client_msg_id.clone(), "TENANT_UNAVAILABLE".to_string())]
    );
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(ingest.received(), vec![client_msg_id]);

    let _ = client.disconnect().await;
    server.stop().await.expect("stop server");
    let _ = std::fs::remove_dir_all(&root);
}
