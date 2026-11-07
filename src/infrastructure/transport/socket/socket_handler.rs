//! 与 flare-signaling/gateway 协议对齐：处理 PayloadCommand（Message/Event/Ack/Data）。
//! 网关下发：MESSAGE 推送=Payload(type=Message)、SendAck=Payload(type=Ack, payload=encoded Ack(Send))。

use std::sync::Arc;

use crate::shared::error::Result;
use async_trait::async_trait;
use flare_core::client::builder::flare::MessageListener;
use flare_core::client::link::DisconnectReason;
use flare_core::common::compression::CompressionUtil;
use flare_core::common::protocol::Frame;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::payload_command::Type as PayloadType;
use flare_proto::common::Ack;
use flare_proto::common::ack::Payload as AckPayload;
use prost::Message;
use tracing::{debug, warn};

use crate::infrastructure::protocol::downlink::DownlinkPayload;
use crate::infrastructure::protocol::{Codec, MAX_DOWNLINK_PAYLOAD_BYTES};
use crate::runtime::Dispatcher;
use crate::runtime::link::{LinkSignal, LinkSignals};

/// Socket 推送处理器 — 实现 flare-core `MessageListener`。
///
/// 每条连接一个处理器，带着那条连接的代数：链路状态机换了连接之后，旧连接迟到的帧与断开
/// 一律丢弃，不会被重复处理，也不会把刚连上的新连接当成断了。
pub struct SocketHandler {
    dispatcher: Arc<Dispatcher>,
    codec: Arc<dyn Codec>,
    epoch: u64,
    signals: LinkSignals,
}

impl SocketHandler {
    pub(crate) fn new(
        dispatcher: Arc<Dispatcher>,
        codec: Arc<dyn Codec>,
        epoch: u64,
        signals: LinkSignals,
    ) -> Self {
        Self {
            dispatcher,
            codec,
            epoch,
            signals,
        }
    }

    fn is_current(&self) -> bool {
        self.signals.epoch().is_current(self.epoch)
    }
}

#[async_trait]
impl MessageListener for SocketHandler {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if !self.is_current() {
            debug!(
                frame_id = %frame.message_id,
                epoch = self.epoch,
                "dropped a frame from a superseded connection"
            );
            return Ok(None);
        }
        // A payload frame was built by the server and carries its clock: a sample for the
        // server clock estimate. System frames are left out, the local transport synthesizes
        // some of them (a PONG for a WebSocket-level pong) with the device clock.
        if matches!(
            frame.command.as_ref().and_then(|cmd| cmd.r#type.as_ref()),
            Some(CommandType::Payload(_))
        ) {
            self.dispatcher
                .server_clock()
                .observe_server_ms(frame.timestamp);
        }

        let Some(ref cmd) = frame.command else {
            return Ok(None);
        };
        let Some(ref cmd_type) = cmd.r#type else {
            return Ok(None);
        };
        tracing::debug!(
            frame_id = %frame.message_id,
            cmd = ?cmd_type,
            "on_message: receive command",
        );

        // 与 flare-signaling gateway 一致：Frame.command 为 Payload(PayloadCommand)，type 1=Message 2=Event 3=Ack 4=Data
        if let CommandType::Payload(payload_cmd) = cmd_type {
            let pt = payload_cmd.r#type;
            // PayloadType::Ack (3)：网关对 MESSAGE 的回执，payload 为 flare_proto::common::Ack(SendAck)
            if pt == PayloadType::Ack as i32 {
                let payload = ensure_decompressed_payload(&payload_cmd.payload);
                if payload_exceeds_budget(&payload, frame.message_id.as_str(), pt, "ack") {
                    return Ok(None);
                }
                if let Ok(ack) = Ack::decode(payload.as_slice()) {
                    match ack.payload {
                        Some(AckPayload::Send(send_ack)) => {
                            let accepted =
                                send_ack.result.as_ref().and_then(|result| match result {
                                    flare_proto::common::send_ack::Result::Accepted(accepted) => {
                                        Some(accepted)
                                    }
                                    _ => None,
                                });
                            let error = send_ack.result.as_ref().and_then(|result| match result {
                                flare_proto::common::send_ack::Result::Error(error) => Some(error),
                                _ => None,
                            });
                            debug!(
                                frame_id = %frame.message_id,
                                client_msg_id = %send_ack.client_msg_id,
                                server_msg_id = %accepted.map(|a| a.server_msg_id.as_str()).unwrap_or_default(),
                                conversation_seq = accepted.map(|a| a.conversation_seq).unwrap_or_default(),
                                error_code = error.map(|e| e.code).unwrap_or_default(),
                                error_message = %error.map(|e| e.message.as_str()).unwrap_or_default(),
                                "received SendAck (PayloadCommand Ack), dispatching to bus"
                            );
                            let payload = DownlinkPayload::SendAck(send_ack.clone());
                            if let Err(error) = self.dispatcher.dispatch(payload).await {
                                warn!(
                                    frame_id = %frame.message_id,
                                    client_msg_id = %send_ack.client_msg_id,
                                    error = %error,
                                    "dispatch SendAck to bus failed"
                                );
                            }
                        }
                        Some(AckPayload::Event(event_ack)) => {
                            debug!(
                                frame_id = %frame.message_id,
                                event_id = %event_ack.event_id,
                                attributes = ?event_ack.attributes,
                                "received EventAck (PayloadCommand Ack)"
                            );
                        }
                        Some(_) | None => {}
                    }
                } else {
                    warn!(
                        frame_id = %frame.message_id,
                        payload_len = payload_cmd.payload.len(),
                        "payload decode as Ack failed, ignored"
                    );
                }
                return Ok(None);
            }
            // PayloadType::Message (1) / Data (4) / Event (2)：内层为 MessagePush / Event / EventEnvelope / DataPacket…（与网关下行约定一致）
            if pt == PayloadType::Message as i32
                || pt == PayloadType::Data as i32
                || pt == PayloadType::Event as i32
            {
                let payload = ensure_decompressed_payload(&payload_cmd.payload);
                if payload_exceeds_budget(&payload, frame.message_id.as_str(), pt, "downlink") {
                    return Ok(None);
                }
                match self.codec.decode_server_payload(pt, &payload) {
                    Ok(downlink) => {
                        debug!(
                            frame_id = %frame.message_id,
                            payload_type = pt,
                            "received push/data/event frame, dispatching to bus"
                        );
                        if let Err(error) = self.dispatcher.dispatch(downlink).await {
                            warn!(
                                frame_id = %frame.message_id,
                                payload_type = pt,
                                error = %error,
                                "dispatch push/data/event frame to bus failed"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            frame_id = %frame.message_id,
                            payload_len = payload_cmd.payload.len(),
                            error = %e,
                            "payload decode as downlink failed, ignored"
                        );
                    }
                }
            }
        }
        Ok(None)
    }

    /// flare-core 以类型化原因报连接结束（对端关闭、读写出错、心跳判死、被踢、计划性关闭、主动断开），
    /// 恰好一次。原样交给链路状态机：怎么恢复（换入口、窗口内重连、终止）由它按原因决定，这里不做判断。
    async fn on_disconnect(&self, reason: &DisconnectReason) -> Result<()> {
        if !self.is_current() {
            debug!(epoch = self.epoch, %reason, "ignored the end of a superseded connection");
            return Ok(());
        }
        self.signals.send(LinkSignal::Lost {
            epoch: self.epoch,
            reason: reason.clone(),
        });
        Ok(())
    }
}

/// 若 payload 为 Gzip 等压缩数据则解压，否则返回原数据（与 chatroom_client 一致，协商 Gzip 时服务端可能下发的压缩 payload）。
fn ensure_decompressed_payload(payload: &[u8]) -> Vec<u8> {
    match CompressionUtil::auto_decompress(payload) {
        Ok((decompressed, _)) => decompressed,
        Err(_) => payload.to_vec(),
    }
}

fn payload_exceeds_budget(payload: &[u8], frame_id: &str, payload_type: i32, label: &str) -> bool {
    if payload.len() <= MAX_DOWNLINK_PAYLOAD_BYTES {
        return false;
    }
    warn!(
        frame_id = %frame_id,
        payload_type,
        payload_len = payload.len(),
        max_payload_len = MAX_DOWNLINK_PAYLOAD_BYTES,
        label,
        "payload exceeds downlink budget, ignored"
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::notification::{
        NotificationHandlerRegistry, NotificationInboundPipeline,
    };
    use crate::application::services::{EventDeduper, MessageDeduper};
    use crate::infrastructure::protocol::ProtobufCodec;
    use crate::kernel::event::EventBus;
    use crate::spi::metrics::MetricsRecorder;
    use flare_core::common::protocol::{Reliability, builder, frame_with_system_command};
    use tokio::sync::RwLock;

    fn handler(
        bus: &EventBus,
    ) -> (
        SocketHandler,
        tokio::sync::mpsc::Receiver<LinkSignal>,
        crate::runtime::link::ConnectionEpoch,
    ) {
        let dispatcher = Dispatcher::new(
            bus.clone(),
            None,
            None,
            None,
            None,
            Arc::new(RwLock::new("u1".to_string())),
            EventDeduper::new(Some(64)),
            NotificationInboundPipeline::new(
                Arc::new(NotificationHandlerRegistry::new()),
                MessageDeduper::new(Some(64)),
                bus.clone(),
            ),
            MetricsRecorder::disabled(),
        );
        let (signals, rx) = LinkSignals::channel(8);
        let epoch = signals.epoch().clone();
        let current = epoch.advance();
        (
            SocketHandler::new(
                Arc::new(dispatcher),
                Arc::new(ProtobufCodec),
                current,
                signals,
            ),
            rx,
            epoch,
        )
    }

    /// 连接结束原样（类型化原因 + 连接代数）交给链路状态机，不再在这里按文本猜踢线、令牌过期。
    #[tokio::test]
    async fn the_end_of_the_current_connection_reaches_the_supervisor_typed() {
        let bus = EventBus::new();
        let (handler, mut rx, _epoch) = handler(&bus);
        let kicked = DisconnectReason::Kicked {
            reason: "DEVICE_CONFLICT".into(),
            ends_session: true,
        };
        handler.on_disconnect(&kicked).await.unwrap();
        match rx.try_recv().expect("the supervisor hears about it") {
            LinkSignal::Lost { epoch, reason } => {
                assert_eq!(epoch, handler.epoch);
                assert_eq!(reason, kicked);
            }
            other => panic!("unexpected signal {other:?}"),
        }
    }

    /// 换了连接之后，旧连接迟到的断开与帧一律丢弃。
    #[tokio::test]
    async fn a_superseded_connection_is_ignored() {
        let bus = EventBus::new();
        let (handler, mut rx, epoch) = handler(&bus);
        epoch.advance();
        handler
            .on_disconnect(&DisconnectReason::HeartbeatTimeout)
            .await
            .unwrap();
        assert!(rx.try_recv().is_err(), "旧连接的断开不能让新连接被当成断了");

        let mut stamped = flare_core::common::protocol::frame_with_payload_command(
            flare_core::common::protocol::PayloadCommand {
                r#type: PayloadType::Data as i32,
                ..Default::default()
            },
            Reliability::AtLeastOnce,
        );
        stamped.timestamp = crate::shared::util::now_millis() - 300_000;
        handler.on_message(&stamped).await.unwrap();
        assert_eq!(
            handler.dispatcher.server_clock().offset_ms(),
            0,
            "旧连接的帧连服务端时钟样本都不算"
        );
    }

    /// 服务端打包的载荷帧带着服务端时间戳，是服务端时钟的样本；系统帧不算（本机传输会用本机时钟
    /// 合成 PONG），没填时间戳的帧也不算。
    #[tokio::test]
    async fn payload_frames_sample_the_server_clock() {
        let bus = EventBus::new();
        let (handler, _rx, _epoch) = handler(&bus);
        let device_now = crate::shared::util::now_millis();
        let mut pong = frame_with_system_command(builder::pong(), Reliability::BestEffort);
        pong.timestamp = device_now + 3_600_000;
        handler.on_message(&pong).await.unwrap();
        assert_eq!(handler.dispatcher.server_clock().offset_ms(), 0);

        let mut unstamped = flare_core::common::protocol::frame_with_payload_command(
            flare_core::common::protocol::PayloadCommand {
                r#type: PayloadType::Data as i32,
                ..Default::default()
            },
            Reliability::AtLeastOnce,
        );
        unstamped.timestamp = 0;
        handler.on_message(&unstamped).await.unwrap();
        assert_eq!(handler.dispatcher.server_clock().offset_ms(), 0);

        // The server clock is five minutes behind the device.
        let mut stamped = unstamped.clone();
        stamped.timestamp = device_now - 300_000;
        handler.on_message(&stamped).await.unwrap();
        let offset = handler.dispatcher.server_clock().offset_ms();
        assert!(
            (-301_000..=-299_000).contains(&offset),
            "offset {offset} ms from a frame five minutes behind"
        );
    }
}
