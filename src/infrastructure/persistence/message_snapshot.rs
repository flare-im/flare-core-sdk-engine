use crate::domain::PendingSendVo;
use crate::model::IMMessage;
use crate::{ErrorCode, FlareError, Result};
use flare_proto::common::{MessageRetentionPolicy, MessageRetentionState};
use prost::Message;
use serde::{Deserialize, Serialize};

/// Storage snapshots retain fields intentionally omitted from the app-facing message JSON.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredMessage {
    #[serde(flatten)]
    message: IMMessage,
    #[serde(default)]
    encoded_content: Vec<u8>,
    #[serde(default)]
    retention_policy: Option<Vec<u8>>,
    #[serde(default)]
    retention_state: Option<Vec<u8>>,
}

impl From<IMMessage> for StoredMessage {
    fn from(mut message: IMMessage) -> Self {
        Self {
            encoded_content: std::mem::take(&mut message.encoded_content),
            retention_policy: message
                .retention_policy
                .take()
                .map(|policy| policy.encode_to_vec()),
            retention_state: message
                .retention_state
                .take()
                .map(|state| state.encode_to_vec()),
            message,
        }
    }
}

impl TryFrom<StoredMessage> for IMMessage {
    type Error = FlareError;
    fn try_from(stored: StoredMessage) -> Result<Self> {
        let mut message = stored.message;
        message.encoded_content = stored.encoded_content;
        message.retention_policy = stored
            .retention_policy
            .map(|bytes| MessageRetentionPolicy::decode(bytes.as_slice()))
            .transpose()
            .map_err(|error| FlareError::localized(ErrorCode::DatabaseError, error.to_string()))?;
        message.retention_state = stored
            .retention_state
            .map(|bytes| MessageRetentionState::decode(bytes.as_slice()))
            .transpose()
            .map_err(|error| FlareError::localized(ErrorCode::DatabaseError, error.to_string()))?;
        message.materialize_encoded_content_from_elem();
        Ok(message)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredPendingSend {
    client_msg_id: String,
    conversation_id: String,
    message: StoredMessage,
    enqueued_at_ms: u64,
}

impl From<PendingSendVo> for StoredPendingSend {
    fn from(entry: PendingSendVo) -> Self {
        Self {
            client_msg_id: entry.client_msg_id,
            conversation_id: entry.conversation_id,
            message: entry.message.into(),
            enqueued_at_ms: entry.enqueued_at_ms,
        }
    }
}

impl TryFrom<StoredPendingSend> for PendingSendVo {
    type Error = FlareError;
    fn try_from(entry: StoredPendingSend) -> Result<Self> {
        Ok(Self {
            client_msg_id: entry.client_msg_id,
            conversation_id: entry.conversation_id,
            message: entry.message.try_into()?,
            enqueued_at_ms: entry.enqueued_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_roundtrip_keeps_raw_content_and_retention_without_changing_app_json() {
        let mut message = IMMessage::new(Default::default());
        message.server_id = "m".into();
        message.client_msg_id = "local".into();
        message.conversation_id = "c".into();
        message.encoded_content = crate::content::ContentBuilder::text("raw content")
            .build()
            .encode();
        message.retention_policy = Some(MessageRetentionPolicy::default());
        message.retention_state = Some(MessageRetentionState {
            lifecycle: 2,
            content_visibility: 1,
            ..Default::default()
        });
        let app_json = serde_json::to_value(&message).unwrap();
        assert!(app_json.get("retentionState").is_none());
        assert!(app_json.get("encodedContent").is_none());
        let snapshot = serde_json::to_value(StoredMessage::from(message.clone())).unwrap();
        let decoded: StoredMessage = crate::json::from_value(snapshot.clone()).unwrap();
        let restored = IMMessage::try_from(decoded).unwrap();
        assert_eq!(restored.encoded_content, message.encoded_content);
        assert_eq!(restored.retention_policy, message.retention_policy);
        assert_eq!(restored.retention_state, message.retention_state);
        let mut corrupt = snapshot;
        corrupt["retentionState"] = serde_json::json!([255]);
        let corrupt: StoredMessage = crate::json::from_value(corrupt).unwrap();
        assert!(IMMessage::try_from(corrupt).is_err());
        let pending = PendingSendVo {
            client_msg_id: "local".into(),
            conversation_id: "c".into(),
            message: message.clone(),
            enqueued_at_ms: 1,
        };
        let json = serde_json::to_value(StoredPendingSend::from(pending)).unwrap();
        let decoded: StoredPendingSend = crate::json::from_value(json).unwrap();
        assert_eq!(
            PendingSendVo::try_from(decoded)
                .unwrap()
                .message
                .encoded_content,
            message.encoded_content
        );
    }
}
