//! Downlink `DataPacket.capability`: plugin realtime packets (the only
//! capability packets Core sends; tenant notices have their own payload).

use flare_proto::common::CapabilityPacket;
use serde_json::Value;

use crate::kernel::event::PluginRealtime;

const ATTR_CONVERSATION_ID: &str = "conversation_id";
const ATTR_DELIVERY_ID: &str = "delivery_id";
const ATTR_EXPIRES_AT_MS: &str = "expires_at_ms";

/// Why a capability packet was dropped (logged, never surfaced).
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Dropped {
    Expired,
    Malformed(&'static str),
}

/// Reads one downlink capability packet at `now_ms`, the server time estimate
/// (`expires_at_ms` is the server's clock).
pub(super) fn classify(
    packet: CapabilityPacket,
    now_ms: u64,
) -> Result<Box<PluginRealtime>, Dropped> {
    realtime(packet, now_ms).map(Box::new)
}

fn realtime(packet: CapabilityPacket, now_ms: u64) -> Result<PluginRealtime, Dropped> {
    let CapabilityPacket {
        capability_id,
        packet_type,
        payload,
        mut attributes,
        correlation_id,
        ..
    } = packet;
    if capability_id.is_empty() {
        return Err(Dropped::Malformed("plugin id"));
    }
    if packet_type.is_empty() {
        return Err(Dropped::Malformed("packet type"));
    }
    let expires_at_ms = attributes
        .get(ATTR_EXPIRES_AT_MS)
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(Dropped::Malformed("expires_at_ms"))?;
    if now_ms > expires_at_ms {
        return Err(Dropped::Expired);
    }
    let Ok(Value::Object(payload)) = serde_json::from_slice::<Value>(&payload) else {
        return Err(Dropped::Malformed("payload is not a JSON object"));
    };
    let delivery_id = attributes
        .remove(ATTR_DELIVERY_ID)
        .filter(|id| !id.is_empty())
        .or(correlation_id.filter(|id| !id.is_empty()))
        .ok_or(Dropped::Malformed("delivery id"))?;
    let conversation_id = attributes
        .remove(ATTR_CONVERSATION_ID)
        .filter(|id| !id.is_empty())
        .ok_or(Dropped::Malformed("conversation id"))?;
    Ok(PluginRealtime {
        plugin_id: capability_id,
        packet_type,
        conversation_id,
        payload,
        delivery_id,
        expires_at_ms,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    const NOW: u64 = 1_730_000_000_000;

    fn realtime_packet(payload: &[u8], expires_at_ms: u64) -> CapabilityPacket {
        CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            version: "1".into(),
            payload: payload.to_vec(),
            attributes: HashMap::from([
                ("conversation_id".into(), "conv-1".into()),
                ("plugin_id".into(), "flare.call".into()),
                ("delivery_id".into(), "d-1".into()),
                ("expires_at_ms".into(), expires_at_ms.to_string()),
            ]),
            correlation_id: Some("d-1".into()),
        }
    }

    #[test]
    fn a_live_realtime_packet_becomes_the_typed_event() {
        let packet = realtime_packet(br#"{"callId":"c-1","version":3}"#, NOW + 60_000);
        let Ok(realtime) = classify(packet, NOW) else {
            panic!("live packet must be delivered");
        };
        assert_eq!(
            *realtime,
            PluginRealtime {
                plugin_id: "flare.call".into(),
                packet_type: "call.snapshot".into(),
                conversation_id: "conv-1".into(),
                payload: json!({"callId": "c-1", "version": 3})
                    .as_object()
                    .cloned()
                    .unwrap(),
                delivery_id: "d-1".into(),
                expires_at_ms: NOW + 60_000,
            }
        );
    }

    #[test]
    fn an_expired_realtime_packet_is_dropped() {
        assert_eq!(
            classify(realtime_packet(br#"{"callId":"c-1"}"#, NOW - 1), NOW),
            Err(Dropped::Expired)
        );
        // Still live at the expiry millisecond itself.
        assert!(classify(realtime_packet(br#"{"callId":"c-1"}"#, NOW), NOW).is_ok());
    }

    #[test]
    fn a_payload_that_is_not_a_json_object_is_dropped() {
        for payload in [
            &br#"[1,2]"#[..],
            b"\"text\"",
            b"42",
            b"null",
            b"not json",
            b"",
        ] {
            assert_eq!(
                classify(realtime_packet(payload, NOW + 1_000), NOW),
                Err(Dropped::Malformed("payload is not a JSON object")),
                "payload {:?}",
                String::from_utf8_lossy(payload)
            );
        }
    }

    #[test]
    fn a_realtime_packet_without_expiry_or_conversation_is_dropped() {
        let mut packet = realtime_packet(b"{}", NOW + 1_000);
        packet.attributes.remove("expires_at_ms");
        assert_eq!(
            classify(packet, NOW),
            Err(Dropped::Malformed("expires_at_ms"))
        );
        let mut packet = realtime_packet(b"{}", NOW + 1_000);
        packet.attributes.remove("conversation_id");
        assert_eq!(
            classify(packet, NOW),
            Err(Dropped::Malformed("conversation id"))
        );
    }

    #[test]
    fn the_delivery_id_falls_back_to_the_correlation_id() {
        let mut packet = realtime_packet(b"{}", NOW + 1_000);
        packet.attributes.remove("delivery_id");
        packet.correlation_id = Some("d-9".into());
        let Ok(realtime) = classify(packet, NOW) else {
            panic!("packet must be delivered");
        };
        assert_eq!(realtime.delivery_id, "d-9");
    }
}

#[cfg(test)]
mod dispatch_tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use flare_proto::common::{CapabilityPacket, TenantNotice};
    use tokio::sync::RwLock;

    use super::super::Dispatcher;
    use crate::application::notification::{
        NotificationHandlerRegistry, NotificationInboundPipeline,
    };
    use crate::application::services::{EventDeduper, MessageDeduper};
    use crate::infrastructure::protocol::DownlinkPayload;
    use crate::kernel::TenantNoticePort;
    use crate::kernel::event::{EventBus, PluginEvent, SdkEvent};
    use crate::shared::util::now_millis;
    use crate::spi::metrics::MetricsRecorder;

    #[derive(Default)]
    struct Notices(Mutex<Vec<TenantNotice>>);

    impl TenantNoticePort for Notices {
        fn notice(&self, notice: TenantNotice) {
            self.0.lock().unwrap().push(notice);
        }
    }

    fn dispatcher(bus: &EventBus, notices: Arc<Notices>) -> Dispatcher {
        Dispatcher::new(
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
        )
        .with_tenant_notices(Some(notices))
    }

    fn realtime(payload: &[u8], expires_at_ms: u64) -> DownlinkPayload {
        DownlinkPayload::Capability(CapabilityPacket {
            capability_id: "flare.call".into(),
            packet_type: "call.snapshot".into(),
            version: "1".into(),
            payload: payload.to_vec(),
            attributes: HashMap::from([
                ("conversation_id".into(), "conv-1".into()),
                ("plugin_id".into(), "flare.call".into()),
                ("delivery_id".into(), "d-1".into()),
                ("expires_at_ms".into(), expires_at_ms.to_string()),
            ]),
            correlation_id: Some("d-1".into()),
        })
    }

    #[tokio::test]
    async fn capability_packets_become_plugin_events_and_tenant_notices_reach_the_port() {
        let bus = EventBus::new();
        let mut events = bus.subscribe();
        let notices = Arc::new(Notices::default());
        let dispatcher = dispatcher(&bus, notices.clone());
        let live = now_millis() + 60_000;

        dispatcher
            .dispatch(realtime(br#"{"callId":"c-1"}"#, live))
            .await
            .unwrap();
        dispatcher
            .dispatch(realtime(br#"{"callId":"c-2"}"#, now_millis() - 1))
            .await
            .unwrap();
        dispatcher.dispatch(realtime(b"[1]", live)).await.unwrap();
        let notice = TenantNotice {
            topic: "flare.plugin.catalog".into(),
            version: 9,
            spread_ms: 3_000,
            tenant_wide: false,
        };
        dispatcher
            .dispatch(DownlinkPayload::TenantNotice(notice.clone()))
            .await
            .unwrap();

        let mut delivered = Vec::new();
        while let Ok(event) = events.try_recv() {
            match event {
                SdkEvent::Plugin(PluginEvent::Realtime(realtime)) => {
                    delivered.push(realtime.payload["callId"].clone())
                }
                SdkEvent::Plugin(PluginEvent::CatalogChanged { .. }) => {
                    panic!("the dispatcher hands tenant notices to the port, never to the bus")
                }
                _ => {}
            }
        }
        assert_eq!(delivered, vec![serde_json::json!("c-1")]);
        assert_eq!(*notices.0.lock().unwrap(), vec![notice]);
    }

    /// The packet's expiry is the server's clock. A device clock minutes ahead
    /// of the server must not drop live packets, and one minutes behind must
    /// not keep stale ones; the event hands the expiry over on the device clock.
    #[tokio::test]
    async fn packet_expiry_is_judged_on_the_server_clock() {
        for ahead_ms in [5 * 60_000_i64, -5 * 60_000] {
            let bus = EventBus::new();
            let mut events = bus.subscribe();
            let clock =
                crate::kernel::server_clock::test_support::clock_with_device_ahead_by(ahead_ms);
            let dispatcher =
                dispatcher(&bus, Arc::new(Notices::default())).with_server_clock(clock.clone());
            let server_now = clock.now_millis();
            dispatcher
                .dispatch(realtime(br#"{"callId":"live"}"#, server_now + 30_000))
                .await
                .unwrap();
            dispatcher
                .dispatch(realtime(br#"{"callId":"stale"}"#, server_now - 30_000))
                .await
                .unwrap();
            let mut delivered = Vec::new();
            while let Ok(event) = events.try_recv() {
                if let SdkEvent::Plugin(PluginEvent::Realtime(realtime)) = event {
                    let device_expiry = realtime.expires_at_ms as i64;
                    let device_now = now_millis() as i64;
                    assert!(
                        (device_expiry - device_now - 30_000).abs() < 5_000,
                        "expiry handed over on the device clock (ahead by {ahead_ms} ms)"
                    );
                    delivered.push(realtime.payload["callId"].clone());
                }
            }
            assert_eq!(
                delivered,
                vec![serde_json::json!("live")],
                "device clock ahead by {ahead_ms} ms"
            );
        }
    }
}
