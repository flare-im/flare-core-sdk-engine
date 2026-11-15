//! 上行发送：与 flare-proto 对齐 — 消息=Message，事件=Event，回执=Ack；DATA 载荷=`DataPacket`（`common/data.proto`）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flare_core::client::builder::flare::FlareClient;
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, builder,
    flare::core::commands::command::Type as CommandType,
    flare::core::commands::payload_command::Type as PayloadType,
};
use flare_proto::common::ack::Payload as AckPayload;
use flare_proto::common::data_packet::Payload as DataPacketPayload;
use flare_proto::common::{
    Ack, AckBatch, ConversationAck, CustomData, DataPacket, DeviceStateReport,
    ErrorCode as ProtoErrorCode, Event, Message as ProtoMessage, RealtimeControlPacket, SyncRes,
};
use prost::Message;
use tokio::sync::Mutex;

use crate::infrastructure::protocol::Codec;
use crate::shared::error::{ErrorCode, FlareError, Result, from_rpc_status};
use crate::shared::util::{now_millis, timeout};

const CONTROL_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const BEST_EFFORT_CONTROL_SEND_TIMEOUT: Duration = Duration::from_millis(250);
/// 设备状态上报与送达确认的发送时限。服务端等确认只等一到三秒，过了就转推送，晚到的没有用，
/// 不值得像已读回执那样等十秒。
const REACH_SIGNAL_SEND_TIMEOUT: Duration = Duration::from_secs(3);

/// 发送帧失败时按原因给错误码，供应用按码提示，而不是解析错误文本。
///
/// - `NOT_CONNECTED`：长连接不在（断线、重连还没完成、被服务端以结束会话的原因踢掉）。SDK 建连时关掉了
///   flare-core 的发送前重连，断线时发送立即返回它，原样保留（含原因文本）。此前一律改成
///   `CONNECTION_FAILED`，应用分不清「没连上，等恢复后再试」与「发送途中出错」。
/// - flare-core 报的连接、网络超时（例如等 CONNECT 协商完成超时）：`OPERATION_TIMEOUT`，与发送本身
///   超时一致。
/// - 其余（写入失败、连接在写入时断开、编码失败等）：`fallback`（直发为 `CONNECTION_FAILED`，
///   同步请求为 `GENERAL_ERROR`，与此前一致）。
fn frame_send_error(error: FlareError, fallback: ErrorCode) -> FlareError {
    match error.code() {
        Some(ErrorCode::NotConnected) => error,
        Some(
            ErrorCode::ConnectionTimeout | ErrorCode::NetworkTimeout | ErrorCode::OperationTimeout,
        ) => FlareError::localized(ErrorCode::OperationTimeout, error.to_string()),
        _ => FlareError::localized(fallback, error.to_string()),
    }
}

/// 上行包发送器 — Message / Event / Ack / DataPacket（同步与用户扩展）
///
/// 除了可靠队列（`send_message` 的调用方之一）之外，这里的发送都是「直发」：不排队、不重试，长连接
/// 断开时立即失败（错误码见 [`frame_send_error`]）。撤回、编辑、删除、已读回执、同步请求、能力包等
/// 都走直发，应用在 `NOT_CONNECTED` 时提示未连接、等会话回到 `Ready` 后再试；输入状态是有损信令，
/// 断线时直接丢弃（见 [`Self::send_realtime_control_best_effort`]）。
pub struct PacketSender {
    client: Arc<Mutex<Option<FlareClient>>>,
    /// 串行化 `send_frame_and_wait`：`HybridClient` 按 message_id 匹配响应，不宜并发等待。
    rpc_wait: Arc<Mutex<()>>,
    /// 最近发出的已读 / 送达回执：换了新连接后补发（见 [`super::ack_ledger`]）。
    ack_ledger: Arc<std::sync::Mutex<super::ack_ledger::AckLedger>>,
    /// 最近一次上行帧的时刻（Unix 毫秒）：恢复切回据此判断是否处于「60 秒内无发送与同步」的自然边界。
    last_activity_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl PacketSender {
    /// Original official ErrorDetail code for typed DATA failures whose SDK equivalent is
    /// less specific (e.g. ALREADY_EXISTS/CONFLICT). This is metadata, not a parsed message.
    pub(crate) const SYNC_REMOTE_ERROR_CODE_PARAM: &'static str = "sync.error_code";

    pub fn new(client: Arc<Mutex<Option<FlareClient>>>, _codec: Arc<dyn Codec>) -> Self {
        Self {
            client,
            rpc_wait: Arc::new(Mutex::new(())),
            ack_ledger: Arc::default(),
            last_activity_ms: Arc::default(),
        }
    }

    /// 最近一次上行帧的时刻（Unix 毫秒）；还没发过时为 0。
    pub(crate) fn last_activity_ms(&self) -> u64 {
        self.last_activity_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn note_activity(&self) {
        self.last_activity_ms
            .store(now_millis(), std::sync::atomic::Ordering::Relaxed);
    }

    /// 新连接就绪后补发 `since_ms` 之后发过的已读 / 送达回执（每会话最大值）。返回补发条数；
    /// 单条失败不影响其它（回执可重复，下一次阅读也会自愈）。送达确认照旧合成一个批确认。
    pub(crate) async fn replay_acks_since(&self, since_ms: u64) -> usize {
        let due = self
            .ack_ledger
            .lock()
            .map(|ledger| ledger.since(since_ms))
            .unwrap_or_default();
        let mut replayed = 0;
        let mut delivered = Vec::new();
        for ack in due {
            match &ack.payload {
                Some(AckPayload::Conversation(confirmation)) => {
                    delivered.push(confirmation.clone());
                }
                _ => {
                    if self.send_ack(&ack).await.is_ok() {
                        replayed += 1;
                    }
                }
            }
        }
        if !delivered.is_empty() {
            let count = delivered.len();
            if self.send_delivery_acks(delivered).await.is_ok() {
                replayed += count;
            }
        }
        replayed
    }

    async fn client_snapshot(&self) -> Result<FlareClient> {
        self.client
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未连接"))
    }

    async fn send_frame_with_timeout(
        &self,
        frame: &Frame,
        timeout_duration: Duration,
        timeout_message: &'static str,
    ) -> Result<()> {
        self.write_frame(frame, timeout_duration, timeout_message, true)
            .await
    }

    /// 写出一帧。`counts_as_activity` 为假的是引擎自己发的状态上报与送达确认：收到消息就会发，
    /// 算进「最近活动」的话，消息多的账号永远等不到恢复切回要的那段空闲。
    async fn write_frame(
        &self,
        frame: &Frame,
        timeout_duration: Duration,
        timeout_message: &'static str,
        counts_as_activity: bool,
    ) -> Result<()> {
        let client = self.client_snapshot().await?;
        if counts_as_activity {
            self.note_activity();
        }
        timeout(timeout_duration, client.send_frame(frame))
            .await
            .map_err(|_| FlareError::localized(ErrorCode::OperationTimeout, timeout_message))?
            .map_err(|e| frame_send_error(e, ErrorCode::ConnectionFailed))?;
        Ok(())
    }

    /// 发送领域事件（event.proto Event）。PayloadCommand.type=Event，网关回 EventAck。
    #[tracing::instrument(skip(self, event, timeout_duration), fields(event_id = %event.event_id))]
    pub async fn send_event(&self, event: &Event, timeout_duration: Duration) -> Result<()> {
        let message_id = if event.event_id.is_empty() {
            builder::generate_message_id()
        } else {
            event.event_id.clone()
        };
        // 与服务端编排对齐：event_id/request_id 为空时补齐，避免“帧发出但事件本体缺少关联 ID”导致的静默失败。
        let mut wire_event = event.clone();
        if wire_event.event_id.is_empty() {
            wire_event.event_id = message_id.clone();
        }
        if wire_event.request_id.is_none() {
            wire_event.request_id = Some(message_id.clone());
        }
        if wire_event.created_at <= 0 {
            wire_event.created_at = now_millis() as i64;
        }
        let payload = wire_event.encode_to_vec();
        let cmd = builder::event_message(message_id, payload, None, None);
        let frame = builder::frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        self.send_frame_with_timeout(&frame, timeout_duration, "event send timeout")
            .await
    }

    /// 发送消息（message.proto Message）。PayloadCommand.type=Message，网关回 SendAck。
    #[tracing::instrument(skip(self, message, timeout_duration), fields(client_msg_id = %message.client_msg_id, conversation_id = %message.conversation_id))]
    pub async fn send_message(
        &self,
        message: &ProtoMessage,
        timeout_duration: Duration,
    ) -> Result<()> {
        let message_id = message.client_msg_id.clone();
        let mut metadata = HashMap::new();
        if !message.conversation_id.is_empty() {
            metadata.insert(
                "conversation_id".to_string(),
                message.conversation_id.as_bytes().to_vec(),
            );
        }
        tracing::debug!(message_id = %message_id, "send_message");
        // 发消息必须用 SEND (0)，网关才会走 handle_message 并回 ACK (1)；用 DATA (2) 会回 DATA (2)
        let msg_cmd =
            builder::send_message(message_id, message.encode_to_vec(), Some(metadata), None);
        let frame = builder::frame_with_payload_command(msg_cmd, Reliability::AtLeastOnce);
        self.send_frame_with_timeout(&frame, timeout_duration, "message send timeout")
            .await
    }

    /// 上报 ACK（ack.proto Ack：PushAck/ConversationAck/AckBatch）。PayloadCommand.type=Ack。
    pub async fn send_ack(&self, ack: &Ack) -> Result<()> {
        self.send_ack_frame(ack, CONTROL_SEND_TIMEOUT, true).await
    }

    /// 送达确认：各会话从链路收到并处理完的最大 seq，合成一个 `AckBatch` 发出。服务端据此停止等待，
    /// 不再把这些消息转给推送通道；发出即记进补发账本。
    pub(crate) async fn send_delivery_acks(&self, delivered: Vec<ConversationAck>) -> Result<()> {
        let ack = Ack {
            ack_id: None,
            ack_at: Some(now_millis() as i64),
            payload: Some(AckPayload::Batch(AckBatch {
                conversation_acks: delivered,
                ..Default::default()
            })),
        };
        self.send_ack_frame(&ack, REACH_SIGNAL_SEND_TIMEOUT, false)
            .await
    }

    async fn send_ack_frame(
        &self,
        ack: &Ack,
        timeout_duration: Duration,
        counts_as_activity: bool,
    ) -> Result<()> {
        let message_id = builder::generate_message_id();
        let payload = ack.encode_to_vec();
        let cmd = PayloadCommand {
            r#type: PayloadType::Ack as i32,
            message_id: message_id.clone(),
            payload,
            metadata: std::collections::HashMap::new(),
            seq: 0,
        };
        let frame = builder::frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        self.write_frame(
            &frame,
            timeout_duration,
            "ack send timeout",
            counts_as_activity,
        )
        .await?;
        if let Ok(mut ledger) = self.ack_ledger.lock() {
            ledger.record(ack, now_millis());
        }
        Ok(())
    }

    /// 上报设备状态（`DEVICE_STATE` 系统命令，载荷是 `DeviceStateReport`）：新消息的提示由这条链路
    /// 负责，还是交给推送通道。
    pub(crate) async fn send_device_state(&self, report: &DeviceStateReport) -> Result<()> {
        let command = builder::device_state(report.encode_to_vec());
        let frame = builder::frame_with_system_command(command, Reliability::AtLeastOnce);
        self.write_frame(
            &frame,
            REACH_SIGNAL_SEND_TIMEOUT,
            "device state send timeout",
            false,
        )
        .await
    }

    /// 发送用户扩展（`DataPacket` + `user_custom`）。PayloadCommand.type=Data。
    pub async fn send_custom_data(&self, data: &CustomData) -> Result<()> {
        let packet = DataPacket {
            payload: Some(DataPacketPayload::UserCustom(data.clone())),
        };
        let payload = packet.encode_to_vec();
        let cmd = builder::data_message(builder::generate_message_id(), payload, None, None);
        let frame = builder::frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        self.send_frame_with_timeout(&frame, CONTROL_SEND_TIMEOUT, "custom data send timeout")
            .await
    }

    /// 发送实时控制包（`DataPacket` + `realtime_control`）。PayloadCommand.type=Data；不占用 conversation_seq。
    pub async fn send_realtime_control(&self, control: &RealtimeControlPacket) -> Result<()> {
        self.send_realtime_control_with_timeout(control, CONTROL_SEND_TIMEOUT)
            .await
    }

    /// 发送可丢弃的实时控制包。输入状态等瞬态提示不应阻塞消息发送主链路。
    pub async fn send_realtime_control_best_effort(
        &self,
        control: &RealtimeControlPacket,
    ) -> Result<()> {
        if let Err(error) = self
            .send_realtime_control_with_timeout(control, BEST_EFFORT_CONTROL_SEND_TIMEOUT)
            .await
        {
            tracing::debug!(
                error = %error,
                control_type = %control.control_type,
                "dropped best-effort realtime control packet"
            );
        }
        Ok(())
    }

    async fn send_realtime_control_with_timeout(
        &self,
        control: &RealtimeControlPacket,
        timeout_duration: Duration,
    ) -> Result<()> {
        let data = DataPacket {
            payload: Some(DataPacketPayload::RealtimeControl(control.clone())),
        };
        let payload = data.encode_to_vec();
        let cmd = builder::data_message(builder::generate_message_id(), payload, None, None);
        let frame = builder::frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        self.send_frame_with_timeout(&frame, timeout_duration, "realtime control send timeout")
            .await
    }

    /// 发送同步请求并等待 DATA 回包（网关对 DATA 为 request-response，须 `send_frame_and_wait`）。
    pub async fn send_sync_and_wait(
        &self,
        sync: &flare_proto::common::Sync,
        timeout: Duration,
    ) -> Result<SyncRes> {
        let packet = DataPacket {
            payload: Some(DataPacketPayload::SyncRequest(sync.clone())),
        };
        let payload = packet.encode_to_vec();
        let cmd = builder::data_message(builder::generate_message_id(), payload, None, None);
        let frame = builder::frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        // 多会话同步会并发调用本方法；用 rpc_wait 排队，禁止 take 走 client（否则并行方得到「未连接」）。
        let _rpc = self.rpc_wait.lock().await;
        let client = self.client_snapshot().await?;
        self.note_activity();
        let response = client
            .send_frame_and_wait(&frame, timeout)
            .await
            .map_err(|e| frame_send_error(e, ErrorCode::GeneralError))?;
        Self::decode_sync_response_frame(&response)
    }

    pub(crate) fn decode_sync_response_frame(frame: &Frame) -> Result<SyncRes> {
        decode_sync_response_frame(frame)
    }

    /// 发送同步请求（仅发不等；遗留路径，新代码请用 [`Self::send_sync_and_wait`]）。
    pub async fn send_sync(&self, sync: &flare_proto::common::Sync) -> Result<()> {
        let packet = DataPacket {
            payload: Some(DataPacketPayload::SyncRequest(sync.clone())),
        };
        let payload = packet.encode_to_vec();
        let cmd = builder::data_message(builder::generate_message_id(), payload, None, None);
        let frame = builder::frame_with_payload_command(cmd, Reliability::AtLeastOnce);
        self.send_frame_with_timeout(&frame, CONTROL_SEND_TIMEOUT, "sync send timeout")
            .await
    }
}

fn decode_sync_response_frame(frame: &Frame) -> Result<SyncRes> {
    let payload = frame
        .command
        .as_ref()
        .and_then(|cmd| match &cmd.r#type {
            Some(CommandType::Payload(pc)) => Some(pc.payload.as_slice()),
            _ => None,
        })
        .ok_or_else(|| {
            FlareError::deserialization_error(
                "sync response frame has no payload command".to_string(),
            )
        })?;
    let packet = DataPacket::decode(payload).map_err(|e| {
        FlareError::deserialization_error(format!("decode sync DataPacket response: {e}"))
    })?;
    match packet.payload {
        Some(DataPacketPayload::SyncResponse(res)) => Ok(res),
        Some(DataPacketPayload::UserCustom(data)) if data.r#type == "error" => {
            Err(decode_sync_error(&data))
        }
        _ => Err(FlareError::general_error(
            "unexpected sync response payload".to_string(),
        )),
    }
}

fn decode_sync_error(data: &CustomData) -> FlareError {
    let text = String::from_utf8_lossy(&data.payload).into_owned();
    let code = data
        .attributes
        .get("error_code")
        .filter(|raw| !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|raw| raw.parse::<i32>().ok())
        .and_then(|code| ProtoErrorCode::try_from(code).ok())
        .filter(|code| {
            !matches!(
                code,
                ProtoErrorCode::Unspecified | ProtoErrorCode::Ok | ProtoErrorCode::Custom
            )
        });
    let Some(code) = code else {
        // Old servers have only Display text, and unknown custom business codes have no
        // official SDK mapping. Neither permits guessing authorization from a substring.
        return FlareError::general_error(text);
    };
    let mut error = from_rpc_status(code as i32, &text);
    if let FlareError::Localized {
        reason,
        details,
        params,
        ..
    } = &mut error
    {
        // Keep the readable legacy payload byte-for-byte (lossy UTF-8 as before), including
        // CANCELLED, whose general mapper otherwise replaces the server's reason.
        *reason = text;
        *details = data
            .attributes
            .get("error_reason")
            .filter(|value| !value.is_empty())
            .cloned();
        *params = Some(HashMap::from([(
            PacketSender::SYNC_REMOTE_ERROR_CODE_PARAM.into(),
            (code as i32).to_string(),
        )]));
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_frame(error_code: Option<&str>, text: &str, reason: Option<&str>) -> Frame {
        let mut attributes = HashMap::new();
        if let Some(code) = error_code {
            attributes.insert("error_code".into(), code.into());
        }
        if let Some(reason) = reason {
            attributes.insert("error_reason".into(), reason.into());
        }
        let packet = DataPacket {
            payload: Some(DataPacketPayload::UserCustom(CustomData {
                r#type: "error".into(),
                payload: text.as_bytes().to_vec(),
                attributes,
            })),
        };
        let command =
            builder::data_message("owned-sync-test".into(), packet.encode_to_vec(), None, None);
        builder::frame_with_payload_command(command, Reliability::AtLeastOnce)
    }

    #[test]
    fn typed_sync_error_frames_preserve_official_codes_and_readable_payloads() {
        for (wire, expected) in [
            (
                ProtoErrorCode::PermissionDenied,
                ErrorCode::PermissionDenied,
            ),
            (
                ProtoErrorCode::Unauthenticated,
                ErrorCode::AuthenticationFailed,
            ),
            (ProtoErrorCode::InvalidArgument, ErrorCode::InvalidParameter),
            (ProtoErrorCode::Timeout, ErrorCode::OperationTimeout),
            (ProtoErrorCode::Unavailable, ErrorCode::ServiceUnavailable),
            (ProtoErrorCode::AlreadyExists, ErrorCode::GeneralError),
            (ProtoErrorCode::Conflict, ErrorCode::GeneralError),
            (ProtoErrorCode::Cancelled, ErrorCode::GeneralError),
        ] {
            let code = (wire as i32).to_string();
            let frame = error_frame(
                Some(&code),
                "original readable wire error",
                Some("GROUP_ACCESS_DENIED"),
            );
            let error = PacketSender::decode_sync_response_frame(&frame).unwrap_err();
            assert_eq!(error.code(), Some(expected), "{wire:?}");
            assert_eq!(error.reason(), "original readable wire error");
            let localized = error.as_localized().unwrap();
            assert_eq!(localized.details.as_deref(), Some("GROUP_ACCESS_DENIED"));
            assert_eq!(
                localized
                    .params
                    .unwrap()
                    .get(PacketSender::SYNC_REMOTE_ERROR_CODE_PARAM),
                Some(&code)
            );
        }
    }

    #[test]
    fn legacy_malformed_or_unknown_sync_codes_cannot_be_guessed_as_permission_denied() {
        for code in [
            None,
            Some(""),
            Some("21junk"),
            Some("PERMISSION_DENIED"),
            Some("2004"),
            Some("999"),
            Some("2147483648"),
            Some("-21"),
            Some("+21"),
            Some("0"),
            Some("1"),
            Some("1000"),
        ] {
            let text = "PERMISSION_DENIED: text is not a typed authorization proof";
            let frame = error_frame(code, text, Some("PERMISSION_DENIED"));
            let error = PacketSender::decode_sync_response_frame(&frame).unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::GeneralError), "{code:?}");
            assert_eq!(error.reason(), text);
            assert!(error.as_localized().unwrap().params.is_none());
        }
    }

    #[test]
    fn normal_sync_responses_remain_typed_sync_responses() {
        let packet = DataPacket {
            payload: Some(DataPacketPayload::SyncResponse(SyncRes::default())),
        };
        let command = builder::data_message(
            "owned-success-test".into(),
            packet.encode_to_vec(),
            None,
            None,
        );
        let frame = builder::frame_with_payload_command(command, Reliability::AtLeastOnce);
        assert!(
            PacketSender::decode_sync_response_frame(&frame)
                .unwrap()
                .payload
                .is_none()
        );
    }

    /// 断线时 flare-core 的发送返回 `NOT_CONNECTED`（SDK 关掉了发送前重连）：原样交给调用方，
    /// 不再改成 `CONNECTION_FAILED`（直发与同步请求都是）。
    #[test]
    fn not_connected_is_kept_for_direct_sends_and_sync_requests() {
        let not_connected = || {
            FlareError::localized(
                ErrorCode::NotConnected,
                "not connected; reconnect_on_send is off, reconnecting is left to the caller",
            )
        };
        for fallback in [ErrorCode::ConnectionFailed, ErrorCode::GeneralError] {
            let mapped = frame_send_error(not_connected(), fallback);
            assert_eq!(mapped.code(), Some(ErrorCode::NotConnected), "{mapped}");
            assert!(
                mapped.to_string().contains("reconnect_on_send is off"),
                "{mapped}"
            );
        }
        // 被服务端以结束会话的原因踢掉之后同样是 NOT_CONNECTED，原因在文本开头
        let ended = FlareError::localized(
            ErrorCode::NotConnected,
            "DEVICE_CONFLICT: the server ended this session; not reconnecting",
        );
        let mapped = frame_send_error(ended, ErrorCode::ConnectionFailed);
        assert_eq!(mapped.code(), Some(ErrorCode::NotConnected));
        assert!(mapped.to_string().contains("DEVICE_CONFLICT"));
    }

    /// 连接、网络超时与发送超时一样是 `OPERATION_TIMEOUT`；其余错误按调用方给的兜底码。
    #[test]
    fn other_send_errors_map_by_cause() {
        let timeout = frame_send_error(
            FlareError::connection_timeout("Negotiation timeout"),
            ErrorCode::ConnectionFailed,
        );
        assert_eq!(timeout.code(), Some(ErrorCode::OperationTimeout));
        assert!(timeout.to_string().contains("Negotiation timeout"));

        let write_failed =
            frame_send_error(FlareError::io("broken pipe"), ErrorCode::ConnectionFailed);
        assert_eq!(write_failed.code(), Some(ErrorCode::ConnectionFailed));
        assert!(write_failed.to_string().contains("broken pipe"));

        let sync_failed = frame_send_error(
            FlareError::protocol_error("Response channel closed"),
            ErrorCode::GeneralError,
        );
        assert_eq!(sync_failed.code(), Some(ErrorCode::GeneralError));
    }

    /// 还没建连（没有 flare-core 客户端）时直发返回 `NOT_CONNECTED`。
    #[tokio::test]
    async fn a_direct_send_without_a_client_is_not_connected() {
        let sender = PacketSender::new(
            Arc::new(Mutex::new(None)),
            Arc::new(crate::infrastructure::protocol::ProtobufCodec),
        );
        let err = sender
            .send_ack(&Ack::default())
            .await
            .expect_err("no client");
        assert_eq!(err.code(), Some(ErrorCode::NotConnected), "{err}");
    }

    /// 没有连接时状态上报与送达确认同样是 `NOT_CONNECTED`：不记进补发账本，也不算一次活动。
    #[tokio::test]
    async fn reach_signals_without_a_client_are_not_connected() {
        let sender = PacketSender::new(
            Arc::new(Mutex::new(None)),
            Arc::new(crate::infrastructure::protocol::ProtobufCodec),
        );
        let err = sender
            .send_device_state(&DeviceStateReport::default())
            .await
            .expect_err("no client");
        assert_eq!(err.code(), Some(ErrorCode::NotConnected), "{err}");
        let err = sender
            .send_delivery_acks(vec![ConversationAck {
                conversation_id: "c1".into(),
                last_delivered_seq: 3,
                ..Default::default()
            }])
            .await
            .expect_err("no client");
        assert_eq!(err.code(), Some(ErrorCode::NotConnected), "{err}");
        assert_eq!(sender.replay_acks_since(0).await, 0);
        assert_eq!(sender.last_activity_ms(), 0);
    }
}
