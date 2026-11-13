//! Session-scoped orchestration. Storage commits before any network call. Only
//! an explicit submit replays; the server must enforce command idempotency.
use super::capability::{CapabilityApi, CapabilityDispatchResult, PluginCallFuture};
use crate::domain::{PluginCommand, PluginCommandScope};
use crate::extension::capability::plugin_errors::plugin_rejection_code;
use crate::{ErrorCode, FlareError, Result};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Slot {
    slot: String,
    expected_user_id: Option<String>,
    expected_tenant_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Prepare {
    slot: String,
    command: PluginCommand,
    expected_user_id: Option<String>,
    expected_tenant_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Submit {
    slot: String,
    command_id: String,
    expected_user_id: Option<String>,
    expected_tenant_id: Option<String>,
}
fn parse<T: serde::de::DeserializeOwned>(v: Value) -> Result<T> {
    crate::shared::util::json::from_value(v).map_err(|_| {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            "sdk.plugin_command.invalid_request",
        )
    })
}
impl CapabilityApi {
    fn command_scope(
        &self,
        slot: String,
        expected_user_id: Option<&str>,
        expected_tenant_id: Option<&str>,
        user_id: Option<String>,
    ) -> Result<PluginCommandScope> {
        let user_id = user_id.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::NotConnected,
                "sdk.plugin_command.session_required",
            )
        })?;
        if expected_tenant_id.is_some_and(|t| t != self.default_tenant_id)
            || expected_user_id.is_some_and(|expected| expected != user_id)
        {
            return Err(FlareError::localized(
                ErrorCode::NotConnected,
                "sdk.plugin_command.session_changed",
            ));
        }
        let scope = PluginCommandScope {
            tenant_id: self.default_tenant_id.clone(),
            user_id,
            slot,
        };
        scope.validate()?;
        Ok(scope)
    }
    fn command_store(&self) -> Result<&dyn crate::domain::PluginCommandStore> {
        self.plugin_commands.as_deref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.plugin_command.durable_store_required",
            )
        })
    }
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn prepare_plugin_command(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.prepare_plugin_command_now(request))
    }
    async fn prepare_plugin_command_now(&self, request: Value) -> Result<Value> {
        let request: Prepare = parse(request)?;
        self.session_guard
            .run_with_user(|user_id| async {
                let scope = self.command_scope(
                    request.slot,
                    request.expected_user_id.as_deref(),
                    request.expected_tenant_id.as_deref(),
                    user_id,
                )?;
                let record = self
                    .command_store()?
                    .prepare(&scope, &request.command)
                    .await?;
                serde_json::to_value(record)
                    .map_err(|_| FlareError::system("plugin command encoding"))
            })
            .await
    }
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn pending_plugin_command(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.pending_plugin_command_now(request))
    }
    async fn pending_plugin_command_now(&self, request: Value) -> Result<Value> {
        let request: Slot = parse(request)?;
        self.session_guard
            .run_with_user(|user_id| async {
                let scope = self.command_scope(
                    request.slot,
                    request.expected_user_id.as_deref(),
                    request.expected_tenant_id.as_deref(),
                    user_id,
                )?;
                let record = self.command_store()?.pending(&scope).await?;
                Ok(serde_json::json!({"record":record}))
            })
            .await
    }
    /// Observe the stored command through its immutable query capability. Only
    /// a verified committed observation can complete the durable journal. Apps
    /// cannot inject responses, clear unknown outcomes or mutate the intent.
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn reconcile_plugin_command(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.reconcile_plugin_command_now(request))
    }
    async fn reconcile_plugin_command_now(&self, request: Value) -> Result<Value> {
        use flare_plugin_protocol::{
            command::{CommandObservation, CommandOutcome},
            runtime::configuration_digest,
        };
        let request: Submit = parse(request)?;
        self.session_guard
            .run_with_user(|user_id| async {
                let scope = self.command_scope(
                    request.slot,
                    request.expected_user_id.as_deref(),
                    request.expected_tenant_id.as_deref(),
                    user_id,
                )?;
                let store = self.command_store()?;
                let record = store
                    .pending(&scope)
                    .await?
                    .filter(|r| r.command.command_id == request.command_id)
                    .ok_or_else(|| {
                        FlareError::localized(
                            ErrorCode::InvalidParameter,
                            "sdk.plugin_command.command_changed",
                        )
                    })?;
                if record.response.is_some() {
                    return Ok(serde_json::json!({"outcome":"committed","record":record}));
                }
                let command = &record.command;
                let query = command
                    .reconciliation_capability_id
                    .as_deref()
                    .ok_or_else(|| {
                        FlareError::localized(
                            ErrorCode::OperationNotSupported,
                            "sdk.plugin_command.reconciliation_unavailable",
                        )
                    })?;
                let response = self
                    .dispatch(
                        query,
                        command.payload.clone(),
                        command.conversation_id.as_deref(),
                        Some(&scope.tenant_id),
                        Some(&scope.user_id),
                    )
                    .await?;
                if !response.success {
                    return Err(FlareError::localized(
                        ErrorCode::ServiceUnavailable,
                        "sdk.plugin_command.outcome_unconfirmed",
                    ));
                }
                let observation: CommandObservation =
                    crate::shared::util::json::from_value(response.data).map_err(|_| {
                        FlareError::localized(
                            ErrorCode::ServiceUnavailable,
                            "sdk.plugin_command.invalid_observation",
                        )
                    })?;
                let digest = configuration_digest(&command.payload)
                    .map_err(|_| FlareError::system("plugin command digest"))?;
                if observation.command_id != command.command_id
                    || observation.capability_id != command.capability_id
                    || observation.request_digest != digest
                {
                    return Err(FlareError::localized(
                        ErrorCode::ServiceUnavailable,
                        "sdk.plugin_command.invalid_observation",
                    ));
                }
                match (observation.outcome, observation.result) {
                    (CommandOutcome::NotFound, None) => {
                        Ok(serde_json::json!({"outcome":"notFound","record":record}))
                    }
                    (CommandOutcome::Committed, Some(data)) if data.is_object() => {
                        let result = super::capability::CapabilityDispatchResult {
                            request_id: response.request_id,
                            success: true,
                            plugin_id: response.plugin_id,
                            capability_id: command.capability_id.clone(),
                            data,
                            error: None,
                        };
                        let saved = store
                            .complete(
                                &scope,
                                command,
                                serde_json::to_value(result).map_err(|_| {
                                    FlareError::system("plugin observation encoding")
                                })?,
                            )
                            .await?;
                        Ok(serde_json::json!({"outcome":"committed","record":saved}))
                    }
                    _ => Err(FlareError::localized(
                        ErrorCode::ServiceUnavailable,
                        "sdk.plugin_command.invalid_observation",
                    )),
                }
            })
            .await
    }
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn submit_plugin_command(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.submit_plugin_command_now(request))
    }
    async fn submit_plugin_command_now(&self, request: Value) -> Result<Value> {
        let request: Submit = parse(request)?;
        self.session_guard
            .run_with_user(|user_id| async {
                let scope = self.command_scope(
                    request.slot,
                    request.expected_user_id.as_deref(),
                    request.expected_tenant_id.as_deref(),
                    user_id,
                )?;
                let store = self.command_store()?;
                let record = store
                    .pending(&scope)
                    .await?
                    .filter(|r| r.command.command_id == request.command_id)
                    .ok_or_else(|| {
                        FlareError::localized(
                            ErrorCode::InvalidParameter,
                            "sdk.plugin_command.command_changed",
                        )
                    })?;
                if record.response.is_some() {
                    return serde_json::to_value(record)
                        .map_err(|_| FlareError::system("plugin command encoding"));
                }
                let c = &record.command;
                let response = match self
                    .dispatch(
                        &c.capability_id,
                        c.payload.clone(),
                        c.conversation_id.as_deref(),
                        Some(&scope.tenant_id),
                        Some(&scope.user_id),
                    )
                    .await
                {
                    Ok(response) => response,
                    // Core answers a plugin's own refusal with HTTP 422 and the plugin's
                    // code. A code the command declares terminal committed nothing: it is
                    // recorded as that rejection, as an answer with `success: false` would
                    // be. Any other refusal comes back as it is (`sdk.plugin.rejected` with
                    // the code in `machineReason`) and the intent stays.
                    Err(error) => match plugin_rejection_code(&error) {
                        Some(code) if c.terminal_error_codes.iter().any(|known| known == code) => {
                            CapabilityDispatchResult {
                                request_id: String::new(),
                                success: false,
                                plugin_id: String::new(),
                                capability_id: c.capability_id.clone(),
                                data: serde_json::json!({ "errorCode": code }),
                                error: None,
                            }
                        }
                        _ => return Err(error),
                    },
                };
                let response = serde_json::to_value(response)
                    .map_err(|_| FlareError::system("plugin response encoding"))?;
                if !c.accepts_response(&response) {
                    return Err(FlareError::localized(
                        ErrorCode::ServiceUnavailable,
                        "sdk.plugin_command.outcome_unconfirmed",
                    ));
                }
                let record = store.complete(&scope, c, response).await?;
                serde_json::to_value(record)
                    .map_err(|_| FlareError::system("plugin command encoding"))
            })
            .await
    }
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn acknowledge_plugin_command(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.acknowledge_plugin_command_now(request))
    }
    async fn acknowledge_plugin_command_now(&self, request: Value) -> Result<Value> {
        let request: Submit = parse(request)?;
        self.session_guard
            .run_with_user(|user_id| async {
                let scope = self.command_scope(
                    request.slot,
                    request.expected_user_id.as_deref(),
                    request.expected_tenant_id.as_deref(),
                    user_id,
                )?;
                self.command_store()?
                    .acknowledge(&scope, &request.command_id)
                    .await?;
                Ok(serde_json::json!({}))
            })
            .await
    }
}
