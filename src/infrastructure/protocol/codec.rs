use crate::infrastructure::protocol::DownlinkPayload;
use crate::shared::error::{FlareError, Result};
use flare_core::common::protocol::flare::core::commands::payload_command::Type as PayloadType;
use flare_proto::common::data_packet::Payload as DataPacketPayload;
use flare_proto::common::{CustomData, DataPacket, Event, EventEnvelope, MessagePush, SyncRes};
use prost::Message;

pub const MAX_DOWNLINK_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// 服务端下行包解码（与 flare-proto 对齐：含 `DataPacket` 封装的同步响应与用户扩展）
pub trait Codec: Send + Sync {
    fn decode_server(&self, payload: &[u8]) -> Result<DownlinkPayload>;
    fn decode_server_payload(&self, payload_type: i32, payload: &[u8]) -> Result<DownlinkPayload> {
        let _ = payload_type;
        self.decode_server(payload)
    }
}

/// Protobuf 编解码实现：按协议约定顺序尝试解码
#[derive(Debug, Clone, Default)]
pub struct ProtobufCodec;

impl Codec for ProtobufCodec {
    fn decode_server(&self, payload: &[u8]) -> Result<DownlinkPayload> {
        ensure_payload_budget(payload)?;
        if let Ok(push) = MessagePush::decode(payload) {
            return Ok(DownlinkPayload::MessagePush(push));
        }
        if let Ok(ev) = Event::decode(payload) {
            return Ok(DownlinkPayload::Event(ev));
        }
        if let Ok(env) = EventEnvelope::decode(payload) {
            return Ok(DownlinkPayload::EventEnvelope(env));
        }
        if let Ok(packet) = DataPacket::decode(payload) {
            match packet.payload {
                Some(DataPacketPayload::SyncResponse(res)) => {
                    return Ok(DownlinkPayload::SyncResp(res));
                }
                Some(DataPacketPayload::UserCustom(c)) => {
                    return Ok(DownlinkPayload::CustomData(c));
                }
                Some(DataPacketPayload::Capability(c)) => {
                    return Ok(DownlinkPayload::Capability(c));
                }
                Some(DataPacketPayload::RealtimeControl(control)) => {
                    return Ok(DownlinkPayload::RealtimeControl(control));
                }
                Some(DataPacketPayload::TenantNotice(notice)) => {
                    return Ok(DownlinkPayload::TenantNotice(notice));
                }
                _ => {}
            }
        }
        if let Ok(data) = CustomData::decode(payload) {
            return Ok(DownlinkPayload::CustomData(data));
        }
        if let Ok(resp) = SyncRes::decode(payload) {
            return Ok(DownlinkPayload::SyncResp(resp));
        }
        Err(FlareError::deserialization_error(
            "payload is not MessagePush, Event, EventEnvelope, DataPacket, CustomData, or SyncRes"
                .to_string(),
        ))
    }

    fn decode_server_payload(&self, payload_type: i32, payload: &[u8]) -> Result<DownlinkPayload> {
        ensure_payload_budget(payload)?;
        if payload_type == PayloadType::Message as i32 {
            return decode_message_payload(payload);
        }
        if payload_type == PayloadType::Event as i32 {
            return decode_event_payload(payload);
        }
        if payload_type == PayloadType::Data as i32 {
            return decode_data_payload(payload);
        }
        Err(FlareError::deserialization_error(format!(
            "unsupported payload type {payload_type}"
        )))
    }
}

fn ensure_payload_budget(payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_DOWNLINK_PAYLOAD_BYTES {
        return Err(FlareError::deserialization_error(format!(
            "downlink payload exceeds {MAX_DOWNLINK_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

fn decode_message_payload(payload: &[u8]) -> Result<DownlinkPayload> {
    if let Ok(push) = MessagePush::decode(payload)
        && is_non_empty_message_push(&push)
    {
        return Ok(DownlinkPayload::MessagePush(push));
    }
    if let Ok(env) = EventEnvelope::decode(payload)
        && is_non_empty_event_envelope(&env)
    {
        return Ok(DownlinkPayload::EventEnvelope(env));
    }
    Err(FlareError::deserialization_error(
        "payload type Message is not MessagePush or EventEnvelope".to_string(),
    ))
}

fn decode_event_payload(payload: &[u8]) -> Result<DownlinkPayload> {
    if let Ok(env) = EventEnvelope::decode(payload)
        && is_non_empty_event_envelope(&env)
    {
        return Ok(DownlinkPayload::EventEnvelope(env));
    }
    if let Ok(ev) = Event::decode(payload)
        && is_non_empty_event(&ev)
    {
        return Ok(DownlinkPayload::Event(ev));
    }
    Err(FlareError::deserialization_error(
        "payload type Event is not EventEnvelope or Event".to_string(),
    ))
}

fn decode_data_payload(payload: &[u8]) -> Result<DownlinkPayload> {
    if let Ok(packet) = DataPacket::decode(payload) {
        match packet.payload {
            Some(DataPacketPayload::SyncResponse(res)) => {
                return Ok(DownlinkPayload::SyncResp(res));
            }
            Some(DataPacketPayload::UserCustom(c)) => {
                return Ok(DownlinkPayload::CustomData(c));
            }
            Some(DataPacketPayload::Capability(c)) => {
                return Ok(DownlinkPayload::Capability(c));
            }
            Some(DataPacketPayload::RealtimeControl(control)) => {
                return Ok(DownlinkPayload::RealtimeControl(control));
            }
            Some(DataPacketPayload::TenantNotice(notice)) => {
                return Ok(DownlinkPayload::TenantNotice(notice));
            }
            _ => {}
        }
    }
    if let Ok(data) = CustomData::decode(payload) {
        return Ok(DownlinkPayload::CustomData(data));
    }
    if let Ok(resp) = SyncRes::decode(payload) {
        return Ok(DownlinkPayload::SyncResp(resp));
    }
    Err(FlareError::deserialization_error(
        "payload type Data is not DataPacket, CustomData, or SyncRes".to_string(),
    ))
}

fn is_non_empty_message_push(push: &MessagePush) -> bool {
    !push.messages.is_empty() || !push.notifications.is_empty()
}

fn is_non_empty_event_envelope(env: &EventEnvelope) -> bool {
    !env.events.is_empty()
        || env.max_conversation_seq > 0
        || env.has_more
        || !env.next_cursor.is_empty()
        || !env.window_id.is_empty()
        || env.delivery_mode != 0
        || !env.conversation_id.is_empty()
        || env.min_conversation_seq > 0
        || env.inline_events_truncated
        || !env.attributes.is_empty()
}

fn is_non_empty_event(ev: &Event) -> bool {
    !ev.conversation_id.is_empty()
        || ev.conversation_seq > 0
        || ev.r#type != 0
        || ev.created_at != 0
        || !ev.event_id.is_empty()
        || ev.request_id.is_some()
        || ev.payload.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{
        EventEnvelopeDeliveryMode, EventType, Message, event::Payload as EventPayload,
    };
    use prost::Message as _;

    #[test]
    fn payload_type_event_decodes_event_envelope_without_message_push_guessing() {
        let envelope = EventEnvelope {
            events: vec![Event {
                conversation_id: "c1".to_string(),
                conversation_seq: 7,
                r#type: EventType::EventMessage as i32,
                created_at: 123,
                event_id: "e1".to_string(),
                request_id: None,
                payload: Some(EventPayload::Message(Message {
                    conversation_id: "c1".to_string(),
                    conversation_seq: 7,
                    client_msg_id: "m1".to_string(),
                    ..Default::default()
                })),
            }],
            max_conversation_seq: 7,
            window_id: "w1".to_string(),
            delivery_mode: EventEnvelopeDeliveryMode::Inline as i32,
            conversation_id: "c1".to_string(),
            min_conversation_seq: 7,
            ..Default::default()
        };
        let mut bytes = Vec::new();
        envelope.encode(&mut bytes).unwrap();

        let decoded = ProtobufCodec
            .decode_server_payload(PayloadType::Event as i32, &bytes)
            .unwrap();

        match decoded {
            DownlinkPayload::EventEnvelope(decoded) => {
                assert_eq!(decoded.conversation_id, "c1");
                assert_eq!(decoded.events.len(), 1);
            }
            _ => panic!("event payload must decode as EventEnvelope"),
        }
    }

    #[test]
    fn payload_type_message_decodes_gateway_event_envelope() {
        let envelope = EventEnvelope {
            events: vec![Event {
                conversation_id: "c1".to_string(),
                conversation_seq: 11,
                r#type: EventType::EventMessage as i32,
                created_at: 456,
                event_id: "e2".to_string(),
                request_id: None,
                payload: Some(EventPayload::Message(Message {
                    conversation_id: "c1".to_string(),
                    conversation_seq: 11,
                    client_msg_id: "m2".to_string(),
                    ..Default::default()
                })),
            }],
            max_conversation_seq: 11,
            window_id: "w2".to_string(),
            delivery_mode: EventEnvelopeDeliveryMode::Inline as i32,
            conversation_id: "c1".to_string(),
            min_conversation_seq: 11,
            ..Default::default()
        };
        let mut bytes = Vec::new();
        envelope.encode(&mut bytes).unwrap();

        let decoded = ProtobufCodec
            .decode_server_payload(PayloadType::Message as i32, &bytes)
            .unwrap();

        match decoded {
            DownlinkPayload::EventEnvelope(decoded) => {
                assert_eq!(decoded.conversation_id, "c1");
                assert_eq!(decoded.events.len(), 1);
            }
            _ => panic!("gateway message payload must decode as EventEnvelope"),
        }
    }

    #[test]
    fn payload_type_message_rejects_data_packet() {
        let packet = DataPacket {
            payload: Some(DataPacketPayload::UserCustom(CustomData {
                r#type: "typing".to_string(),
                payload: vec![1, 2, 3],
                attributes: Default::default(),
            })),
        };
        let mut bytes = Vec::new();
        packet.encode(&mut bytes).unwrap();

        let err = match ProtobufCodec.decode_server_payload(PayloadType::Message as i32, &bytes) {
            Ok(_) => panic!("message payload must not guess data packets"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("payload type Message"));
    }

    #[test]
    fn unknown_payload_type_is_rejected_without_guessing() {
        let push = MessagePush {
            messages: vec![Message {
                conversation_id: "c1".to_string(),
                conversation_seq: 1,
                client_msg_id: "m1".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut bytes = Vec::new();
        push.encode(&mut bytes).unwrap();

        let err = match ProtobufCodec.decode_server_payload(99, &bytes) {
            Ok(_) => panic!("unknown payload type must not fall back to guessing"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("unsupported payload type 99"));
    }

    #[test]
    fn oversized_downlink_payload_is_rejected_before_decode() {
        let bytes = vec![0; MAX_DOWNLINK_PAYLOAD_BYTES + 1];

        let err = match ProtobufCodec.decode_server_payload(PayloadType::Data as i32, &bytes) {
            Ok(_) => panic!("oversized payload must be rejected"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("downlink payload exceeds 8388608 bytes")
        );
    }
}
