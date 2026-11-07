//! Business-neutral, explicit page commands over the authenticated journal.
use super::{CapabilityApi, CapabilityDispatchResult};
use crate::{
    ErrorCode, FlareError, Result,
    domain::{PluginCommand, PluginCommandScope},
};
use flare_plugin_protocol::ui::{UiPageCommand, UiPageCommandMode, UiPageTransition};
use serde_json::{Value, json};

pub(super) struct PageCommandCall<'a> {
    pub plugin: &'a str,
    pub viewer: &'a str,
    pub operation: &'a str,
    pub payload: Value,
    pub conversation: Option<&'a str>,
    pub policy: &'a UiPageCommand,
    pub continuation: Option<UiPageTransition>,
}
impl CapabilityApi {
    pub(super) async fn page_journal(
        &self,
        plugin: &str,
        slot: &str,
        viewer: &str,
    ) -> Result<Value> {
        let scope = self.page_command_scope(plugin, slot, viewer)?;
        let store = self.plugin_commands.as_deref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.plugin_command.durable_store_required",
            )
        })?;
        Ok(match store.pending(&scope).await? {
            None => Value::Null,
            Some(r) => {
                json!({"commandId":r.command.command_id,"capabilityId":r.command.capability_id,"known":r.response.is_some(),"response":r.response})
            }
        })
    }
    fn page_command_scope(
        &self,
        plugin: &str,
        slot: &str,
        viewer: &str,
    ) -> Result<PluginCommandScope> {
        if viewer.is_empty() || !slot.starts_with(&format!("{plugin}.")) {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.plugin_command.invalid_scope",
            ));
        }
        let s = PluginCommandScope {
            tenant_id: self.default_tenant_id.clone(),
            user_id: viewer.into(),
            slot: slot.into(),
        };
        s.validate()?;
        Ok(s)
    }
    pub(super) async fn page_command(
        &self,
        call: PageCommandCall<'_>,
    ) -> Result<(CapabilityDispatchResult, Option<UiPageTransition>)> {
        let PageCommandCall {
            plugin,
            viewer,
            operation,
            mut payload,
            conversation,
            policy,
            continuation,
        } = call;
        if !operation.starts_with(&format!("{plugin}.")) {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "sdk.plugin_command.invalid_scope",
            ));
        }
        let scope = self.page_command_scope(plugin, &policy.slot, viewer)?;
        let store = self.plugin_commands.as_deref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.plugin_command.durable_store_required",
            )
        })?;
        let pending = store.pending(&scope).await?;
        let command = match policy.mode {
            UiPageCommandMode::Retry | UiPageCommandMode::Acknowledge => {
                let r = pending.ok_or_else(|| {
                    FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "sdk.plugin_command.command_changed",
                    )
                })?;
                if policy.mode == UiPageCommandMode::Retry && r.command.capability_id != operation {
                    return Err(FlareError::localized(
                        ErrorCode::PermissionDenied,
                        "sdk.plugin_command.invalid_scope",
                    ));
                }
                if policy.mode == UiPageCommandMode::Acknowledge {
                    let original = r.response.ok_or_else(|| {
                        FlareError::system("unknown command cannot be acknowledged")
                    })?;
                    store.acknowledge(&scope, &r.command.command_id).await?;
                    return Ok((
                        CapabilityDispatchResult {
                            request_id: String::new(),
                            success: true,
                            plugin_id: plugin.into(),
                            capability_id: operation.into(),
                            data: json!({"acknowledged":true,"commandId":r.command.command_id,"originalResponse":original}),
                            error: None,
                        },
                        None,
                    ));
                }
                r.command
            }
            UiPageCommandMode::Submit => {
                if payload.get("commandId").is_none() {
                    payload["commandId"] = json!(uuid::Uuid::new_v4().to_string());
                }
                let conversation =
                    conversation.or_else(|| payload.get("conversationId").and_then(Value::as_str));
                let command:PluginCommand=serde_json::from_value(json!({"commandId":payload["commandId"],"capabilityId":operation,"conversationId":conversation,"payload":payload,"terminalErrorCodes":policy.terminal_error_codes,"continuation":continuation})).map_err(|_|FlareError::system("page command encoding"))?;
                command.validate()?;
                if let Some(old) = pending
                    && old.command != command
                {
                    if old.response.is_none() {
                        return Err(FlareError::localized(
                            ErrorCode::InvalidParameter,
                            "sdk.plugin.outcome_unknown",
                        ));
                    }
                    if let Err(error) = store.acknowledge(&scope, &old.command.command_id).await {
                        // Another submit may already have replaced this completed
                        // predecessor with our identical command. Join that original;
                        // never acknowledge or replace a different financial command.
                        let joined = store
                            .pending(&scope)
                            .await?
                            .is_some_and(|record| record.command == command);
                        if !joined {
                            return Err(error);
                        }
                    }
                }
                store.prepare(&scope, &command).await?;
                command
            }
        };
        let existing = store
            .pending(&scope)
            .await?
            .ok_or_else(|| FlareError::system("page command missing"))?;
        let response = if let Some(v) = existing.response {
            serde_json::from_value(v).map_err(|_| FlareError::system("invalid journal response"))?
        } else {
            let response = match self
                .dispatch_with(
                    &command.capability_id,
                    command.payload.clone(),
                    command.conversation_id.as_deref(),
                    Some(&scope.tenant_id),
                    Some(&scope.user_id),
                    None,
                )
                .await
            {
                Ok(r) => r,
                Err(error) => {
                    match crate::extension::capability::plugin_errors::plugin_rejection_code(&error)
                    {
                        Some(code) if command.terminal_error_codes.iter().any(|c| c == code) => {
                            CapabilityDispatchResult {
                                request_id: String::new(),
                                success: false,
                                plugin_id: plugin.into(),
                                capability_id: command.capability_id.clone(),
                                data: json!({"errorCode":code}),
                                error: None,
                            }
                        }
                        _ => {
                            tracing::warn!(operation=%command.capability_id,code=?error.code(),"plugin page command remains pending");
                            return Err(FlareError::localized(
                                ErrorCode::ServiceUnavailable,
                                "sdk.plugin.outcome_unknown",
                            ));
                        }
                    }
                }
            };
            let raw = serde_json::to_value(&response)
                .map_err(|_| FlareError::system("page response encoding"))?;
            if !command.accepts_response(&raw) {
                return Err(FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "sdk.plugin.outcome_unknown",
                ));
            }
            let saved = store.complete(&scope, &command, raw).await?;
            serde_json::from_value(
                saved
                    .response
                    .ok_or_else(|| FlareError::system("page response missing"))?,
            )
            .map_err(|_| FlareError::system("invalid journal response"))?
        };
        Ok((response, command.continuation))
    }
}
