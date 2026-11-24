//! Business-neutral, explicit page commands over the authenticated journal.
use super::{CapabilityApi, CapabilityDispatchResult};
use crate::{
    ErrorCode, FlareError, Result,
    domain::{PluginCommand, PluginCommandRecord, PluginCommandScope},
};
use flare_plugin_protocol::{
    command::{
        CommandObservation, CommandObservationRequest, CommandOutcome, ObservedCommandResult,
    },
    ui::{UiCatalog, UiPageCommand, UiPageCommandMode, UiPageTransition},
};
use serde_json::{Value, json};

pub(super) struct PageCommandCall<'a> {
    pub plugin: &'a str,
    pub catalog: &'a UiCatalog,
    pub viewer: &'a str,
    pub operation: &'a str,
    pub payload: Value,
    pub conversation: Option<&'a str>,
    pub message_id: Option<&'a str>,
    pub policy: &'a UiPageCommand,
    pub continuation: Option<UiPageTransition>,
}
impl CapabilityApi {
    /// Complete only a verified read of the immutable authenticated intent.
    /// The action's bound payload never replaces the saved payload or identity.
    async fn observe_page_command(
        &self,
        catalog: &UiCatalog,
        plugin: &str,
        operation: &str,
        scope: &PluginCommandScope,
        pending: Option<PluginCommandRecord>,
    ) -> Result<(CapabilityDispatchResult, Option<UiPageTransition>)> {
        let denied = || {
            FlareError::localized(
                ErrorCode::PermissionDenied,
                "sdk.plugin_command.invalid_scope",
            )
        };
        let unknown =
            || FlareError::localized(ErrorCode::ServiceUnavailable, "sdk.plugin.outcome_unknown");
        if catalog.tenant_id != scope.tenant_id
            || catalog.valid_until_ms <= chrono::Utc::now().timestamp_millis()
            || !catalog.plugins.iter().any(|p| {
                p.plugin_id == plugin
                    && p.operations.contains(operation)
                    && p.query_operations.contains(operation)
            })
            || self.session_guard.capture_user().await?.as_deref() != Some(scope.user_id.as_str())
        {
            return Err(denied());
        }
        let record = pending.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.plugin_command.command_changed",
            )
        })?;
        let command = &record.command;
        if !crate::shared::util::is_under_namespace(&command.capability_id, plugin) {
            return Err(denied());
        }
        if let Some(response) = record.response {
            let response = crate::shared::util::json::from_value(response)
                .map_err(|_| FlareError::system("invalid journal response"))?;
            return Ok((response, record.command.continuation));
        }
        let request = CommandObservationRequest {
            command_id: command.command_id.clone(),
            capability_id: command.capability_id.clone(),
            payload: command.payload.clone(),
        };
        let response = self
            .dispatch_with(
                operation,
                serde_json::to_value(request)
                    .map_err(|_| FlareError::system("observation encoding"))?,
                command.conversation_id.as_deref(),
                Some(&scope.tenant_id),
                Some(&scope.user_id),
                None,
            )
            .await?;
        if !response.success || response.plugin_id != plugin || response.capability_id != operation
        {
            return Err(unknown());
        }
        let observation: CommandObservation<ObservedCommandResult> =
            crate::shared::util::json::from_value(response.data).map_err(|_| unknown())?;
        let digest = flare_plugin_protocol::runtime::configuration_digest(&command.payload)
            .map_err(|_| FlareError::system("plugin command digest"))?;
        if observation.command_id != command.command_id
            || observation.capability_id != command.capability_id
            || observation.request_digest != digest
        {
            return Err(unknown());
        }
        let Some(result) = observation
            .result
            .filter(|_| observation.outcome == CommandOutcome::Committed)
        else {
            return Err(unknown());
        };
        let observed = CapabilityDispatchResult {
            request_id: response.request_id,
            success: result.success,
            plugin_id: plugin.into(),
            capability_id: command.capability_id.clone(),
            data: result.data,
            error: None,
        };
        let raw = serde_json::to_value(&observed)
            .map_err(|_| FlareError::system("page response encoding"))?;
        if !command.accepts_response(&raw) {
            return Err(unknown());
        }
        if self.session_guard.capture_user().await?.as_deref() != Some(scope.user_id.as_str()) {
            return Err(denied());
        }
        let saved = self
            .plugin_commands
            .as_deref()
            .ok_or_else(|| FlareError::system("page command store missing"))?
            .complete(scope, command, raw)
            .await?;
        let response = crate::shared::util::json::from_value(
            saved
                .response
                .ok_or_else(|| FlareError::system("page response missing"))?,
        )
        .map_err(|_| FlareError::system("invalid journal response"))?;
        Ok((response, saved.command.continuation))
    }

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
        if viewer.is_empty() || !crate::shared::util::is_under_namespace(slot, plugin) {
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
            catalog,
            viewer,
            operation,
            mut payload,
            conversation,
            message_id,
            policy,
            continuation,
        } = call;
        if !crate::shared::util::is_under_namespace(operation, plugin) {
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
        if pending
            .as_ref()
            .is_some_and(|record| record.command.message_id.as_deref() != message_id)
        {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "sdk.plugin_command.invalid_scope",
            ));
        }
        let command = match policy.mode {
            UiPageCommandMode::Observe => {
                return self
                    .observe_page_command(catalog, plugin, operation, &scope, pending)
                    .await;
            }
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
                let command = PluginCommand {
                    command_id: payload["commandId"].as_str().unwrap_or_default().to_owned(),
                    capability_id: operation.to_owned(),
                    conversation_id: conversation.map(str::to_owned),
                    message_id: message_id.map(str::to_owned),
                    payload,
                    reconciliation_capability_id: None,
                    terminal_error_codes: policy.terminal_error_codes.clone(),
                    continuation,
                };
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
            .filter(|record| record.command == command)
            .ok_or_else(|| FlareError::system("page command missing"))?;
        let response = if let Some(v) = existing.response {
            crate::shared::util::json::from_value(v)
                .map_err(|_| FlareError::system("invalid journal response"))?
        } else {
            let response = match self
                .dispatch_with(
                    &command.capability_id,
                    command.payload.clone(),
                    command.conversation_id.as_deref(),
                    Some(&scope.tenant_id),
                    Some(&scope.user_id),
                    command.message_id.as_deref(),
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
            crate::shared::util::json::from_value(
                saved
                    .response
                    .ok_or_else(|| FlareError::system("page response missing"))?,
            )
            .map_err(|_| FlareError::system("invalid journal response"))?
        };
        Ok((response, command.continuation))
    }
}
