use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Scoped client identity, stable from local enqueue through durable ACK.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageKey {
    pub conversation_id: String,
    pub sender_id: String,
    pub client_msg_id: String,
}

/// Remote outcome of a message that `send` accepted locally. `send` returns once the
/// message and its outbox entry are committed on this device; this reports what the
/// server decided.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum SendConfirmation {
    /// The server committed the message; the local row now carries its coordinates.
    #[serde(rename_all = "camelCase")]
    Persisted {
        server_id: String,
        conversation_seq: u64,
    },
    /// Terminal failure. A retryable rejection keeps the message queued instead.
    #[serde(rename_all = "camelCase")]
    Failed { reason: String },
}

/// Which queued message to wait for, and for how long.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmationRequest {
    #[serde(flatten)]
    pub key: MessageKey,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserRef {
    pub user_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MemberRef {
    pub conversation_id: String,
    pub user: UserRef,
}

impl super::IMMessage {
    pub fn message_key(&self) -> Option<MessageKey> {
        (!self.conversation_id.is_empty()
            && !self.sender_id.is_empty()
            && !self.client_msg_id.is_empty())
        .then(|| MessageKey {
            conversation_id: self.conversation_id.clone(),
            sender_id: self.sender_id.clone(),
            client_msg_id: self.client_msg_id.clone(),
        })
    }
}

impl super::ConversationParticipant {
    pub fn member_ref(&self, conversation_id: &str) -> MemberRef {
        MemberRef {
            conversation_id: conversation_id.into(),
            user: UserRef {
                user_id: self.user_id.clone(),
            },
        }
    }
}
