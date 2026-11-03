use flare_im_core_sdk::RawSdkEvent;
use flare_im_core_sdk::event::{
    ConnectionEvent, ConversationEvent, MessageEvent, NotificationEvent, PluginEvent,
    ReadinessStage, SdkEvent, SyncNotify, SyncPhase,
};
use flare_im_core_sdk::model::message::send_ack;
use flare_im_core_sdk::prelude::SyncRunContext;
use flare_im_core_sdk::serde_json::{self, Value, json};
use std::fmt::Write as _;

fn message_json(message: &flare_im_core_sdk::model::IMMessage) -> Value {
    serde_json::to_value(message).unwrap_or_else(|_| json!({}))
}

fn send_ack_id(ack: &flare_im_core_sdk::model::SendAck) -> &str {
    ack.ack_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(ack.client_msg_id.as_str())
}

fn send_ack_json(ack: &flare_im_core_sdk::model::SendAck) -> Value {
    let (server_msg_id, seq, timestamp, success, error_code, error_message, error_detail) =
        match ack.result.as_ref() {
            Some(send_ack::Result::Accepted(accepted)) => (
                accepted.server_msg_id.clone(),
                accepted.conversation_seq,
                accepted.server_time,
                true,
                0,
                String::new(),
                Value::Null,
            ),
            Some(send_ack::Result::Error(error)) => (
                String::new(),
                0,
                0,
                false,
                error.code,
                error.message.clone(),
                json!({
                    "code": error.code,
                    "reason": error.reason,
                    "message": error.message,
                    "track": error.track,
                }),
            ),
            // Reliable queue sends can emit an ack before the durable server result is
            // available. Match the dispatch response contract: empty result means the
            // message was accepted into the local send queue, not rejected.
            None => (String::new(), 0, 0, true, 0, String::new(), Value::Null),
        };
    json!({
        "clientMsgId": ack.client_msg_id,
        "serverId": server_msg_id,
        "seq": seq,
        "conversationId": ack.conversation_id,
        "ackId": send_ack_id(ack),
        "timestamp": timestamp,
        "success": success,
        "errorCode": error_code,
        "errorMessage": error_message,
        "errorDetail": error_detail,
    })
}

fn sync_run_json(run: &SyncRunContext) -> Value {
    json!({
        "runId": run.run_id,
        "trigger": run.trigger.as_str(),
        "scope": run.scope.as_str(),
        "visibility": run.visibility.as_str(),
        "reason": run.reason.as_str(),
    })
}

fn sync_phase_str(phase: &SyncPhase) -> &'static str {
    match phase {
        SyncPhase::Init => "Init",
        SyncPhase::Background => "Background",
    }
}

fn readiness_stage_str(stage: ReadinessStage) -> &'static str {
    match stage {
        ReadinessStage::LocalReady => "LocalReady",
        ReadinessStage::ForegroundFresh => "ForegroundFresh",
        ReadinessStage::Converged => "Converged",
        ReadinessStage::Degraded => "Degraded",
    }
}

fn with_sync_field(mut payload: Value, key: &'static str, value: Value) -> Value {
    if let Some(map) = payload.as_object_mut() {
        map.insert(key.to_string(), value);
    }
    payload
}

pub fn sdk_event_payload(ev: &SdkEvent) -> Option<(&'static str, Value)> {
    let row = match ev {
        SdkEvent::Connection(ConnectionEvent::Connected) => ("connection.connected", json!({})),
        SdkEvent::Connection(ConnectionEvent::Disconnected { reason }) => {
            ("connection.disconnected", json!({ "reason": reason }))
        }
        SdkEvent::Connection(ConnectionEvent::StateChanged { state }) => (
            "connection.state_changed",
            json!({ "state": format!("{state:?}") }),
        ),
        SdkEvent::Connection(ConnectionEvent::ServerError { code, message }) => (
            "connection.server_error",
            json!({ "code": code, "message": message }),
        ),
        SdkEvent::Connection(ConnectionEvent::Reconnecting { attempt }) => {
            ("connection.reconnecting", json!({ "attempt": attempt }))
        }
        SdkEvent::Connection(ConnectionEvent::KickedOff { reason }) => {
            ("connection.kicked_off", json!({ "reason": reason }))
        }
        SdkEvent::Connection(ConnectionEvent::TokenExpired { message }) => {
            ("connection.token_expired", json!({ "message": message }))
        }
        SdkEvent::Connection(ConnectionEvent::SyncStateChanged { state }) => (
            "connection.sync_state_changed",
            json!({ "state": format!("{state:?}") }),
        ),

        SdkEvent::Message(MessageEvent::Received { message }) => {
            ("message.received", message_json(message.as_ref()))
        }
        SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) => (
            "message.received_batch",
            json!({ "messages": messages.iter().map(message_json).collect::<Vec<_>>() }),
        ),
        SdkEvent::Message(MessageEvent::SendAck { ack }) => (
            "message.send_ack",
            json!({ "ack": send_ack_json(ack.as_ref()) }),
        ),
        SdkEvent::Message(MessageEvent::SendFailed {
            client_msg_id,
            reason,
            error,
        }) => (
            "message.send_failed",
            json!({ "clientMsgId": client_msg_id, "reason": reason, "error": error }),
        ),
        SdkEvent::Message(MessageEvent::Recalled {
            conversation_id,
            event,
        }) => (
            "message.recalled",
            json!({
                "conversationId": conversation_id,
                "messageId": event.server_msg_id,
                "serverMsgId": event.server_msg_id,
                "recallerId": ""
            }),
        ),
        SdkEvent::Message(MessageEvent::Edited {
            conversation_id,
            server_msg_id,
        }) => (
            "message.edited",
            json!({
                "conversationId": conversation_id,
                "messageId": server_msg_id,
                "serverMsgId": server_msg_id
            }),
        ),
        SdkEvent::Message(MessageEvent::PluginStateChanged {
            conversation_id,
            message_id,
            version,
        }) => (
            "message.plugin_state_changed",
            json!({
                "conversationId": conversation_id,
                "messageId": message_id,
                "version": version
            }),
        ),
        SdkEvent::Message(MessageEvent::ReadSummariesChanged {
            conversation_id,
            message_ids,
        }) => (
            "message.read_summaries_changed",
            json!({
                "conversationId": conversation_id,
                "messageIds": message_ids
            }),
        ),
        SdkEvent::Message(MessageEvent::ReactionChanged {
            conversation_id,
            server_msg_id,
            user_id,
            emoji,
            action,
        }) => (
            "message.reaction_changed",
            json!({
                "conversationId": conversation_id,
                "serverMsgId": server_msg_id,
                "userId": user_id,
                "emoji": emoji,
                "action": action
            }),
        ),
        SdkEvent::Message(MessageEvent::Typing {
            conversation_id,
            event,
        }) => (
            "message.typing",
            json!({
                "conversationId": conversation_id,
                "userId": event.user_id,
                "typing": event.typing
            }),
        ),
        SdkEvent::Message(MessageEvent::TypingAggregate {
            conversation_id,
            event,
        }) => (
            "message.typing_aggregate",
            json!({
                "conversationId": conversation_id,
                "typingUserIds": event.typing_user_ids,
                "typingCount": event.typing_count
            }),
        ),
        SdkEvent::Message(MessageEvent::Deleted {
            conversation_id,
            event,
        }) => (
            "message.deleted",
            json!({
                "conversationId": conversation_id,
                "messageId": event.server_msg_id,
                "serverMsgId": event.server_msg_id
            }),
        ),
        SdkEvent::Message(MessageEvent::ReadReceipt {
            conversation_id,
            event,
        }) => (
            "message.read_receipt",
            json!({
                "conversationId": conversation_id,
                "userId": event.user_id,
                "readSeq": event.read_seq,
            }),
        ),
        SdkEvent::Message(MessageEvent::PresenceChanged {
            conversation_id,
            event,
        }) => (
            "message.presence_changed",
            json!({
                "conversationId": conversation_id,
                "userId": event.user_id,
                "status": event.status,
                "extra": event.attributes
            }),
        ),
        SdkEvent::Message(MessageEvent::Custom {
            conversation_id,
            event,
        }) => (
            "message.custom",
            json!({
                "conversationId": conversation_id,
                "namespace": event.namespace,
                "name": event.name,
                "version": event.version,
                "payload": event.payload,
                "attributes": event.attributes
            }),
        ),

        SdkEvent::Notification(NotificationEvent::Received { message }) => {
            ("notification.received", message_json(message.as_ref()))
        }

        SdkEvent::Conversation(ConversationEvent::Synced { conversation_ids }) => (
            "conversation.synced",
            json!({ "conversationIds": conversation_ids }),
        ),
        SdkEvent::Conversation(ConversationEvent::Created { conversation_id }) => (
            "conversation.created",
            json!({ "conversationId": conversation_id }),
        ),
        SdkEvent::Conversation(ConversationEvent::Updated { conversation_id }) => (
            "conversation.updated",
            json!({ "conversationId": conversation_id }),
        ),
        SdkEvent::Conversation(ConversationEvent::Deleted { conversation_id }) => (
            "conversation.deleted",
            json!({ "conversationId": conversation_id }),
        ),
        SdkEvent::Conversation(ConversationEvent::UnreadCountChanged {
            conversation_id,
            unread_count,
        }) => (
            "conversation.unread_count_changed",
            json!({ "conversationId": conversation_id, "unreadCount": unread_count }),
        ),

        SdkEvent::Sync(SyncNotify::ResyncNeeded {
            scope,
            reason,
            dropped_events,
        }) => (
            "sync.resync_needed",
            json!({
                "scope": scope,
                "reason": reason,
                "droppedEvents": dropped_events
            }),
        ),
        SdkEvent::Sync(SyncNotify::Started { run }) => ("sync.started", sync_run_json(run)),
        SdkEvent::Sync(SyncNotify::Finished { run, phase }) => (
            "sync.finished",
            with_sync_field(sync_run_json(run), "phase", json!(sync_phase_str(phase))),
        ),
        SdkEvent::Sync(SyncNotify::Failed { run, task, message }) => (
            "sync.failed",
            with_sync_field(
                with_sync_field(sync_run_json(run), "task", json!(task)),
                "error",
                json!(message),
            ),
        ),
        SdkEvent::Sync(SyncNotify::Progress {
            run,
            task,
            progress,
            detail,
        }) => (
            "sync.progress",
            with_sync_field(
                with_sync_field(
                    with_sync_field(sync_run_json(run), "task", json!(task)),
                    "progress",
                    json!(progress),
                ),
                "detail",
                json!(detail),
            ),
        ),
        SdkEvent::Sync(SyncNotify::TaskCompleted { run, task }) => (
            "sync.task_completed",
            with_sync_field(sync_run_json(run), "task", json!(task)),
        ),
        SdkEvent::Sync(SyncNotify::StateChanged { run, state }) => (
            "sync.state_changed",
            with_sync_field(sync_run_json(run), "state", json!(format!("{state:?}"))),
        ),
        SdkEvent::Sync(SyncNotify::Readiness { run, stage }) => (
            "sync.readiness",
            with_sync_field(
                sync_run_json(run),
                "stage",
                json!(readiness_stage_str(*stage)),
            ),
        ),
        SdkEvent::View(update) => (
            "view.updated",
            serde_json::to_value(update).unwrap_or_else(|_| json!({})),
        ),
        SdkEvent::Plugin(PluginEvent::Realtime(realtime)) => (
            "plugin.realtime",
            json!({
                "pluginId": realtime.plugin_id,
                "packetType": realtime.packet_type,
                "conversationId": realtime.conversation_id,
                "payload": realtime.payload,
                "deliveryId": realtime.delivery_id,
                "expiresAtMs": realtime.expires_at_ms,
            }),
        ),
        SdkEvent::Plugin(PluginEvent::CatalogChanged { version }) => {
            ("plugin.catalog_changed", json!({ "version": version }))
        }
        _ => return None,
    };

    Some(row)
}

pub fn sdk_event_code(ev: &SdkEvent) -> i32 {
    sdk_event_payload(ev)
        .and_then(|(id, _)| crate::find_event_by_id(id).map(|event| event.c_code))
        .unwrap_or(crate::generated::event_codes::FLARE_EVENT_UNKNOWN)
}

pub fn sdk_event_channel_payload(ev: &SdkEvent) -> Option<(&'static str, Value)> {
    let (id, payload) = sdk_event_payload(ev)?;
    let channel = crate::find_event_by_id(id)?.tauri?;
    Some((channel, payload))
}

pub fn sdk_event_web_payload(ev: &SdkEvent) -> Option<Value> {
    let (channel, payload) = sdk_event_channel_payload(ev)?;
    Some(json!({
        "channel": channel,
        "payload": payload,
    }))
}

pub fn sdk_event_json(ev: &SdkEvent) -> String {
    sdk_event_payload(ev)
        .and_then(|(_, payload)| serde_json::to_string(&payload).ok())
        .unwrap_or_else(|| "{}".to_string())
}

pub fn sdk_event_batch_json<'a, I>(events: I) -> Option<(String, usize)>
where
    I: IntoIterator<Item = &'a RawSdkEvent>,
{
    let mut event_count = 0usize;
    let mut out = String::from("{\"events\":[");

    for event in events {
        let event_type = sdk_event_code(event.event());
        if event_type == crate::generated::event_codes::FLARE_EVENT_UNKNOWN {
            continue;
        }

        let payload = event.cached_json(sdk_event_json);
        if event_count > 0 {
            out.push(',');
        }
        write!(
            &mut out,
            "{{\"eventType\":{event_type},\"payload\":{}",
            payload.as_ref()
        )
        .expect("writing to String cannot fail");
        out.push('}');
        event_count += 1;
    }

    if event_count == 0 {
        return None;
    }

    out.push_str("]}");
    Some((out, event_count))
}

pub fn platform_event_bridge_resync_marker(dropped_events: u64) -> SdkEvent {
    SdkEvent::Sync(SyncNotify::ResyncNeeded {
        scope: "platform_event_bridge".to_string(),
        reason: "platform_event_bridge_lagged".to_string(),
        dropped_events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_core_sdk::EventBus;
    use flare_im_core_sdk::model::SendAck;

    /// `message.send_failed` carries the old `reason` text and the structured `error`, as the
    /// shared golden lays it out.
    #[test]
    fn send_failed_event_wire_matches_the_shared_golden() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../../../flare-core-sdk/sdk-spec/golden/events/send_failed_with_error.json",
        );
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let golden: Value = flare_im_core_sdk::serde_json::from_str(&text).expect("golden json");
        let error: flare_im_core_sdk::event::SendFailure =
            flare_im_core_sdk::serde_json::from_value(golden["error"].clone()).expect("error");
        let event = SdkEvent::Message(MessageEvent::SendFailed {
            client_msg_id: golden["clientMsgId"].as_str().unwrap().to_string(),
            reason: golden["reason"].as_str().unwrap().to_string(),
            error,
        });
        let (name, payload) = sdk_event_payload(&event).expect("send failed payload");
        assert_eq!(name, "message.send_failed");
        assert_eq!(payload, golden);
    }

    #[test]
    fn send_ack_event_uses_client_msg_id_when_ack_id_is_absent() {
        let payload = send_ack_json(&SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            ack_id: None,
            result: None,
        });

        assert_eq!(payload["ackId"], "client-1");
        assert_ne!(payload["ackId"], serde_json::Value::Null);
    }

    #[test]
    fn send_ack_event_payload_wraps_ack_contract() {
        let ack = SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            ack_id: None,
            result: None,
        };
        let (event, payload) = sdk_event_payload(&SdkEvent::Message(MessageEvent::SendAck {
            ack: Box::new(ack),
        }))
        .expect("send ack should map to sdk event payload");

        assert_eq!(event, "message.send_ack");
        assert!(payload.get("ack").is_some_and(Value::is_object));
        assert_eq!(payload["ack"]["ackId"], "client-1");
        assert_eq!(payload["ack"]["success"], true);
        assert_eq!(payload["ack"]["errorMessage"], "");
        assert!(payload.get("ackId").is_none());
    }

    #[test]
    fn plugin_state_changed_is_forwarded_as_a_message_update() {
        let event = SdkEvent::Message(MessageEvent::PluginStateChanged {
            conversation_id: "conversation-1".to_string(),
            message_id: "server-1".to_string(),
            version: 3,
        });
        let (channel, payload) =
            sdk_event_channel_payload(&event).expect("plugin state should map to channel");

        assert_eq!(channel, "im://message_plugin_state_changed");
        assert_eq!(sdk_event_code(&event), 2022);
        assert_eq!(
            payload,
            json!({"conversationId": "conversation-1", "messageId": "server-1", "version": 3})
        );
    }

    #[test]
    fn plugin_realtime_is_forwarded_with_the_wire_contract_fields() {
        let event = SdkEvent::Plugin(PluginEvent::Realtime(Box::new(
            flare_im_core_sdk::event::PluginRealtime {
                plugin_id: "flare.call".to_string(),
                packet_type: "call.snapshot".to_string(),
                conversation_id: "conversation-1".to_string(),
                payload: json!({"callId": "c-1", "version": 2})
                    .as_object()
                    .cloned()
                    .expect("object"),
                delivery_id: "d-1".to_string(),
                expires_at_ms: 1_730_000_000_000,
            },
        )));
        let (channel, payload) =
            sdk_event_channel_payload(&event).expect("plugin realtime should map to channel");

        assert_eq!(channel, "im://plugin_realtime");
        assert_eq!(sdk_event_code(&event), 7001);
        assert_eq!(
            payload,
            json!({
                "pluginId": "flare.call",
                "packetType": "call.snapshot",
                "conversationId": "conversation-1",
                "payload": {"callId": "c-1", "version": 2},
                "deliveryId": "d-1",
                "expiresAtMs": 1_730_000_000_000_u64
            })
        );
    }

    #[test]
    fn read_summaries_changed_names_the_conversation_and_its_messages() {
        let event = SdkEvent::Message(MessageEvent::ReadSummariesChanged {
            conversation_id: "group-1".to_string(),
            message_ids: vec!["s-1".to_string(), "s-2".to_string()],
        });
        let (channel, payload) =
            sdk_event_channel_payload(&event).expect("read summaries should map to channel");

        assert_eq!(channel, "im://message_read_summaries_changed");
        assert_eq!(sdk_event_code(&event), 2023);
        assert_eq!(
            payload,
            json!({"conversationId": "group-1", "messageIds": ["s-1", "s-2"]})
        );
        assert_eq!(
            sdk_event_web_payload(&event).expect("web payload")["channel"],
            "im://message_read_summaries_changed"
        );
    }

    #[test]
    fn plugin_catalog_changed_is_forwarded_with_its_version() {
        let event = SdkEvent::Plugin(PluginEvent::CatalogChanged {
            version: "42".to_string(),
        });
        let (channel, payload) =
            sdk_event_channel_payload(&event).expect("catalog change should map to channel");

        assert_eq!(channel, "im://plugin_catalog_changed");
        assert_eq!(sdk_event_code(&event), 7002);
        assert_eq!(payload, json!({"version": "42"}));
    }

    #[test]
    fn platform_event_bridge_resync_marker_uses_canonical_contract_event() {
        let event = platform_event_bridge_resync_marker(3);
        let (channel, payload) =
            sdk_event_channel_payload(&event).expect("resync marker should map to channel");

        assert_eq!(channel, "im://resync_needed");
        assert_eq!(
            payload.get("scope").and_then(Value::as_str),
            Some("platform_event_bridge")
        );
        assert_eq!(
            payload.get("reason").and_then(Value::as_str),
            Some("platform_event_bridge_lagged")
        );
        assert_eq!(
            payload.get("droppedEvents").and_then(Value::as_u64),
            Some(3)
        );
    }

    #[test]
    fn sync_started_payload_includes_run_context_for_warm_calibration() {
        let run = SyncRunContext::warm_start();
        let (_id, payload) =
            sdk_event_payload(&SdkEvent::Sync(SyncNotify::Started { run })).unwrap();

        assert_eq!(
            payload.get("trigger").and_then(Value::as_str),
            Some("WarmStartupCalibration")
        );
        assert_eq!(
            payload.get("visibility").and_then(Value::as_str),
            Some("NonBlocking")
        );
        assert_eq!(
            payload.get("reason").and_then(Value::as_str),
            Some("WarmStartupCalibration")
        );
        assert!(
            payload
                .get("runId")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.is_empty())
        );
    }

    #[test]
    fn sync_readiness_maps_to_contract_channel_with_stage() {
        let event = SdkEvent::Sync(SyncNotify::Readiness {
            run: SyncRunContext::warm_start(),
            stage: ReadinessStage::ForegroundFresh,
        });
        let (channel, payload) =
            sdk_event_channel_payload(&event).expect("readiness should map to channel");

        assert_eq!(channel, "im://sync_readiness");
        assert_eq!(
            payload.get("stage").and_then(Value::as_str),
            Some("ForegroundFresh")
        );
        assert_eq!(
            payload.get("trigger").and_then(Value::as_str),
            Some("WarmStartupCalibration")
        );
    }

    #[test]
    fn sdk_event_batch_json_uses_stable_event_codes_and_payloads() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe_shared_raw();

        bus.publish(SdkEvent::Connection(ConnectionEvent::Connected));
        bus.publish(SdkEvent::Connection(ConnectionEvent::Disconnected {
            reason: "network".to_string(),
        }));

        let first = rx.try_recv().expect("first event");
        let second = rx.try_recv().expect("second event");
        let (json, event_count) =
            sdk_event_batch_json([first.as_ref(), second.as_ref()]).expect("batch json");

        assert_eq!(event_count, 2);
        let value: Value = serde_json::from_str(&json).expect("batch JSON parses");
        let events = value
            .get("events")
            .and_then(Value::as_array)
            .expect("events array");
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].get("eventType").and_then(Value::as_i64),
            Some(1001)
        );
        assert_eq!(
            events[1].get("eventType").and_then(Value::as_i64),
            Some(1002)
        );
        assert_eq!(
            events[1]
                .get("payload")
                .and_then(|payload| payload.get("reason"))
                .and_then(Value::as_str),
            Some("network")
        );
    }
}
