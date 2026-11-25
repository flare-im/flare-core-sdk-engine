//! Durable intent for explicitly replayable, server-idempotent plugin commands.
//! No business interpretation, credentials, authority or automatic retry.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ErrorCode, FlareError, Result};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginCommand {
    pub command_id: String,
    pub capability_id: String,
    pub conversation_id: Option<String>,
    /// Original server message of a host-opened bundle detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    pub payload: Value,
    /// Plugin-declared read-only lookup. Frozen with the intent before submit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation_capability_id: Option<String>,
    /// Plugin-owned error contract: only these failures guarantee no business
    /// effect. All other failures remain an unknown outcome, never ackable.
    /// At most [`MAX_TERMINAL_ERROR_CODES`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal_error_codes: Vec<String>,
    /// Frozen navigation after this explicitly submitted page command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<flare_plugin_protocol::ui::UiPageTransition>,
}
/// Bound on a command's declared terminal codes: room for a plugin's whole
/// rejection table (each code at most 128 bytes), well inside the 32 KiB
/// command bound.
pub const MAX_TERMINAL_ERROR_CODES: usize = 32;
impl PluginCommand {
    pub fn accepts_response(&self, response: &Value) -> bool {
        response["success"] == true
            || (response["success"] == false
                && response["data"]["errorCode"].as_str().is_some_and(|code| {
                    self.terminal_error_codes.iter().any(|known| known == code)
                }))
    }
    pub fn validate(&self) -> Result<()> {
        if !valid_key(&self.command_id)
            || !valid_key(&self.capability_id)
            || self
                .conversation_id
                .as_ref()
                .is_some_and(|id| !valid_key(id))
            || self
                .message_id
                .as_ref()
                .is_some_and(|id| !valid_key(id) || self.conversation_id.is_none())
            || self
                .reconciliation_capability_id
                .as_ref()
                .is_some_and(|id| !valid_key(id) || id == &self.capability_id)
            || !self.payload.is_object()
            || self.terminal_error_codes.len() > MAX_TERMINAL_ERROR_CODES
            || self.terminal_error_codes.iter().any(|s| !valid_key(s))
            || self
                .continuation
                .as_ref()
                .is_some_and(|t| t.validate().is_err())
            || serde_json::to_vec(self).map_or(true, |v| v.len() > 32768)
        {
            return Err(command_error("invalid_command"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct PluginCommandScope {
    pub tenant_id: String,
    pub user_id: String,
    pub slot: String,
}
impl PluginCommandScope {
    pub fn validate(&self) -> Result<()> {
        if [&self.tenant_id, &self.user_id, &self.slot]
            .iter()
            .any(|s| !valid_key(s))
        {
            return Err(command_error("invalid_scope"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginCommandRecord {
    pub command: PluginCommand,
    /// Successful response or an explicitly declared terminal rejection. All
    /// other outcomes remain None and cannot be acknowledged or replaced.
    pub response: Option<Value>,
}
pub fn valid_key(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}
pub fn command_error(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("sdk.plugin_command.{reason}"),
    )
}
#[async_trait]
pub trait PluginCommandStore: Send + Sync {
    /// Atomic insert or identical replay. Never overwrite an occupied slot.
    async fn prepare(
        &self,
        scope: &PluginCommandScope,
        command: &PluginCommand,
    ) -> Result<PluginCommandRecord>;
    async fn pending(&self, scope: &PluginCommandScope) -> Result<Option<PluginCommandRecord>>;
    /// Compare the full command. Concurrent submissions retain the first response.
    async fn complete(
        &self,
        scope: &PluginCommandScope,
        command: &PluginCommand,
        response: Value,
    ) -> Result<PluginCommandRecord>;
    /// Only a recorded response can be acknowledged. Unknown outcomes are retained.
    async fn acknowledge(&self, scope: &PluginCommandScope, command_id: &str) -> Result<()>;
}
