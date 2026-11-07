//! Plugin pages: `CapabilityApi::plugin_page` and page actions.
use serde_json::Value;

use super::{CapabilityApi, CapabilityDispatchResult};
use crate::extension::capability::ui_page;
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_plugin_protocol::{
    manifest::UiContributionKind,
    ui::{ResolvedPluginView, UiRenderContext, UiUnavailableReason},
};

impl CapabilityApi {
    /// A plugin page (a `page` contribution, such as a wallet), resolved like
    /// a card's detail: `{pluginId, page, platform, locale?, arguments?,
    /// filters?, cursor?, previous?, refresh?}` in, a v2 view out (`available`,
    /// `title`, `fields`, `actions`, `menuActions?`, `blocks?`, `inputs?`,
    /// `nextCursor?`, `unavailableReason?`) plus `data`, what it was resolved
    /// against, which a page action passes back.
    ///
    /// - `arguments`: the payload of the page target that opened the page
    ///   (`{kind: "host", target: {kind: "page"}, payload}` of an action);
    ///   `{}` from an account entry. The page's source (a declared query) is
    ///   called with its input bound from them; a page without a source is
    ///   resolved against them.
    /// - `filters`: values of the rows block's filter inputs, validated like a
    ///   form and joined to the source input.
    /// - Paging and refresh follow `plugin_detail`: `cursor` (the view's
    ///   `nextCursor`) with `previous` (the view as returned) appends the next
    ///   page to it; `refresh` with `previous` re-reads from the first page
    ///   until at least as many entries and rows as `previous` showed are
    ///   back, the source has no next page, or 10 pages were read.
    /// - The first page and a refresh never fail for an unavailable plugin:
    ///   an unreadable catalog, a disabled, uninstalled or not permitted
    ///   plugin, another platform or an unknown page give an inert view
    ///   (`available: false`, empty title, `unavailableReason`). A next page
    ///   fails instead, and any other failure of the source is an error.
    ///
    /// A page is no card: its calls carry no `messageId` and bind no entity.
    pub async fn plugin_page(&self, request: Value) -> Result<Value> {
        let mut request = ui_page::parse_page(request)?;
        let previous = request
            .previous
            .take()
            .map(ui_page::split_previous)
            .transpose()?;
        self.session_guard
            .run_with_user(|viewer| async move {
                let viewer = viewer.unwrap_or_default();
                let inert = |reason| {
                    ui_page::page_view(&ResolvedPluginView::unavailable_page(reason), None)
                };
                let catalog = match self.fetch_plugin_ui_catalog().await {
                    Ok((_, catalog)) => catalog,
                    Err(error) if error.code() == Some(ErrorCode::NotConnected) => {
                        return Err(error);
                    }
                    Err(_) if request.degrades() => {
                        return inert(UiUnavailableReason::CatalogUnreachable);
                    }
                    Err(error) => return Err(error),
                };
                let context = UiRenderContext {
                    tenant: &self.default_tenant_id,
                    platform: request.platform,
                    kind: UiContributionKind::Page,
                    locale: &request.locale,
                    viewer: &viewer,
                    now_ms: ui_page::now_ms(),
                };
                let contribution = match catalog.page(&request.plugin_id, &request.page, &context) {
                    Ok((_, contribution)) => contribution,
                    Err(reason) if request.degrades() => return inert(reason),
                    Err(_) => {
                        return Err(FlareError::localized(
                            ErrorCode::OperationNotSupported,
                            "sdk.plugin.ui_action_unavailable",
                        ));
                    }
                };
                let prepared = ui_page::prepare_page(contribution, &request)?;
                // A malformed cursor is refused before anything is sent.
                let input = prepared.input(request.cursor.as_deref())?;
                let resolve = |data: &Value| {
                    contribution
                        .resolve_page(data, &request.locale, &viewer)
                        .ok_or_else(|| {
                            FlareError::localized(
                                ErrorCode::OperationNotSupported,
                                "sdk.plugin.ui_action_unavailable",
                            )
                        })
                };
                let mut first = match self.page_data(&request, &prepared, input, &viewer).await {
                    Ok(data) => data,
                    Err(error) if request.degrades() => match ui_page::inert_reason(&error) {
                        Some(reason) => return inert(reason),
                        None => return Err(error),
                    },
                    Err(error) => return Err(error),
                };
                if let Some(object) = first.as_object_mut() {
                    object.insert(
                        "_session".into(),
                        serde_json::json!({"tenantId":self.default_tenant_id,"userId":viewer}),
                    );
                }
                let mut view = resolve(&first)?;
                let mut data = first;
                match previous {
                    Some((previous, _)) if request.refresh => {
                        let wanted = ui_page::page_items(&previous);
                        let mut pages = 1;
                        while let Some(cursor) =
                            ui_page::refresh_continues(&view, wanted, pages).map(str::to_owned)
                        {
                            let input = prepared.input(Some(&cursor))?;
                            let next = self.page_data(&request, &prepared, input, &viewer).await?;
                            view.append_page(resolve(&next)?);
                            pages += 1;
                        }
                    }
                    Some((mut previous, previous_data)) => {
                        previous.append_page(view);
                        view = previous;
                        data = previous_data;
                    }
                    None => {}
                }
                ui_page::page_view(&view, Some(data))
            })
            .await
    }

    /// One page of the source result (the arguments of a page without a
    /// source). A failure that means the plugin is no longer available
    /// invalidates the catalog like any plugin call.
    async fn page_data(
        &self,
        request: &ui_page::PageRequest,
        prepared: &ui_page::PreparedPage,
        input: Value,
        viewer: &str,
    ) -> Result<Value> {
        let Some(source) = &prepared.source else {
            return Ok(input);
        };
        let viewer = (!viewer.is_empty()).then_some(viewer);
        let conversation = input
            .get("conversationId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        match self
            .dispatch_with(
                &source.operation,
                input,
                conversation.as_deref(),
                Some(&self.default_tenant_id),
                viewer,
                None,
            )
            .await
            .and_then(CapabilityDispatchResult::fail_if_unsuccessful)
        {
            Ok(response) => {
                let mut data = response.data;
                if let Some(slot) = &source.command_slot {
                    let journal = self
                        .page_journal(&request.plugin_id, slot, viewer.unwrap_or_default())
                        .await?;
                    let object = data
                        .as_object_mut()
                        .ok_or_else(|| FlareError::system("page source must be an object"))?;
                    object.insert("journal".into(), journal);
                }
                Ok(data)
            }
            Err(error) => {
                self.after_plugin_error(&error, &request.plugin_id).await;
                Err(error)
            }
        }
    }

    /// A page action (`plugin_ui_invoke` with `page` instead of `message`):
    /// checked and bound by [`ui_page::prepare_page_action`], then an
    /// operation target is dispatched without `messageId` (Core's card binding
    /// guard concerns card calls only); page and native targets come back to
    /// the host as `{kind: "host", target, payload}` (a page target's payload
    /// is the next page's `arguments`).
    pub(super) async fn invoke_page_action(&self, request: Value) -> Result<Value> {
        use crate::extension::capability::ui::PreparedAction;
        use flare_plugin_protocol::ui::UiActionTarget;
        let request = ui_page::parse_page_action(request)?;
        self.session_guard
            .run_with_user(|user_id| async move {
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let context = UiRenderContext {
                    tenant: &self.default_tenant_id,
                    platform: request.platform,
                    kind: UiContributionKind::Page,
                    locale: "",
                    viewer: user_id.as_deref().unwrap_or_default(),
                    now_ms: ui_page::now_ms(),
                };
                let (_, declaration) = catalog
                    .page(&request.page.plugin_id, &request.page.page, &context)
                    .map_err(|_| {
                        FlareError::localized(
                            ErrorCode::OperationNotSupported,
                            "sdk.plugin.ui_action_unavailable",
                        )
                    })?;
                let action = declaration
                    .schema
                    .actions
                    .iter()
                    .find(|a| a.id == request.action_id)
                    .cloned()
                    .ok_or_else(|| FlareError::system("page action missing"))?;
                let has_password = declaration.schema.inputs.iter().any(|i| action.form.contains(&i.id) && i.kind == flare_plugin_protocol::ui::UiInputKind::Password);
                if has_password && action.command.is_some() {
                    return Err(FlareError::system("password inputs cannot enter durable commands"));
                }
                if (action.command.is_some() || has_password)
                    && request.data.as_ref().is_none_or(|d| {
                        d["_session"]["tenantId"] != self.default_tenant_id
                            || d["_session"]["userId"].as_str() != user_id.as_deref()
                    })
                {
                    return Err(FlareError::localized(
                        ErrorCode::NotConnected,
                        "sdk.plugin_command.session_changed",
                    ));
                }
                match ui_page::prepare_page_action(&self.default_tenant_id, &catalog, &request)? {
                    PreparedAction::FormErrors(errors) => {
                        Ok(serde_json::json!({"kind":"formErrors","errors":errors}))
                    }
                    PreparedAction::Ready {
                        target: UiActionTarget::Operation { operation },
                        payload,
                        ..
                    } => {
                        let conversation = request.conversation_id.as_deref().or_else(|| {
                            request
                                .data
                                .as_ref()
                                .and_then(|v| v["conversationId"].as_str())
                        });
                        let (mut response, mut continuation) = if let Some(policy) = &action.command {
                            self.page_command(crate::client::api::page_command::PageCommandCall {
                                plugin: &request.page.plugin_id,
                                viewer: user_id.as_deref().unwrap_or_default(),
                                operation: &operation,
                                payload,
                                conversation,
                                policy,
                                continuation: action.continuation.clone(),
                            })
                            .await?
                        } else {
                            let response = match self
                                .dispatch_with(
                                    &operation,
                                    payload,
                                    conversation,
                                    Some(&self.default_tenant_id),
                                    user_id.as_deref(),
                                    None,
                                )
                                .await
                            {
                                Ok(response) => response,
                                Err(error) => {
                                    self.after_plugin_error(&error, &request.page.plugin_id)
                                        .await;
                                    return Err(error);
                                }
                            };
                            (response, action.continuation.clone())
                        };
                        // A successful explicit operation may continue through one declared
                        // durable action. Read fresh target truth, recheck its current grant,
                        // and persist the financial command before dispatch. Secrets remain
                        // solely in the initiating non-journalled request.
                        if response.success
                            && let Some(transition) = continuation.as_ref()
                            && let Some(action_id) = transition.action_id.as_deref()
                        {
                            let next = transition.resolve(&response.data)
                                .map_err(|_| FlareError::system("invalid operation continuation"))?
                                .ok_or_else(|| FlareError::system("continuation unavailable"))?;
                            let plugin = transition.plugin_id.as_deref().unwrap_or(&request.page.plugin_id);
                            let (_, target) = catalog.page(plugin, &transition.page, &context)
                                .map_err(|_| FlareError::localized(ErrorCode::OperationNotSupported, "sdk.plugin.ui_action_unavailable"))?;
                            let follow = target.schema.actions.iter().find(|a| a.id == action_id)
                                .ok_or_else(|| FlareError::system("continuation action missing"))?;
                            let Some(policy) = follow.command.as_ref().filter(|p| p.mode == flare_plugin_protocol::ui::UiPageCommandMode::Submit) else {
                                return Err(FlareError::system("continuation must be a durable submit"));
                            };
                            if !follow.form.is_empty() || follow.confirm.is_some()
                                || follow.continuation.as_ref().is_some_and(|t| t.action_id.is_some())
                            {
                                return Err(FlareError::system("continuation cannot collect consent or chain"));
                            }
                            let target_request = ui_page::parse_page(serde_json::json!({
                                "pluginId":plugin,"page":transition.page,"platform":request.platform,
                                "arguments":next["payload"],
                            }))?;
                            let prepared = ui_page::prepare_page(target, &target_request)?;
                            let mut data = self.page_data(&target_request, &prepared, prepared.input(None)?, user_id.as_deref().unwrap_or_default()).await?;
                            data["_session"] = serde_json::json!({"tenantId":self.default_tenant_id,"userId":user_id});
                            // Hidden or otherwise unavailable actions never become executable
                            // merely because a source plugin named them.
                            let view = target.resolve_page(&data, "", user_id.as_deref().unwrap_or_default())
                                .ok_or_else(|| FlareError::system("continuation page unavailable"))?;
                            if !view.actions.iter().any(|a| a.id == action_id) {
                                return Err(FlareError::localized(ErrorCode::OperationNotSupported,"sdk.plugin.ui_action_unavailable"));
                            }
                            let call = ui_page::parse_page_action(serde_json::json!({
                                "page":{"pluginId":plugin,"page":transition.page},"platform":request.platform,
                                "actionId":action_id,"userGesture":request.user_gesture,"data":data,
                            }))?;
                            let PreparedAction::Ready {target:UiActionTarget::Operation {operation},mut payload,..} =
                                ui_page::prepare_page_action(&self.default_tenant_id,&catalog,&call)? else {
                                    return Err(FlareError::system("invalid continuation action"));
                                };
                            // A declared continuation may carry the stable identity of
                            // its accepted source result. Repeated password submissions
                            // then prepare the same durable command, including when two
                            // calls race before either observes a pending journal row.
                            if let Some(command) = next["payload"].get("commandId") {
                                if payload.get("commandId").is_some_and(|old| old != command) {
                                    return Err(FlareError::system("continuation command identity changed"));
                                }
                                payload["commandId"] = command.clone();
                            }
                            let follow_conversation = call.data.as_ref().and_then(|d| d["conversationId"].as_str()).or(conversation);
                            let followed = self.page_command(crate::client::api::page_command::PageCommandCall {
                                plugin,viewer:user_id.as_deref().unwrap_or_default(),operation:&operation,
                                payload,conversation:follow_conversation,policy,continuation:follow.continuation.clone(),
                            }).await;
                            match followed {
                                Ok((result, mut next)) => {
                                    if let Some(next) = next.as_mut() { next.plugin_id.get_or_insert_with(|| plugin.to_owned()); }
                                    response = result; continuation = next;
                                }
                                // The financial submit may have committed. Show its own
                                // journal/recovery UI; never leave a password retry on the
                                // initiating page after an unknown follow-up.
                                Err(_) => return Ok(serde_json::json!({"kind":"host","target":{"kind":"page","pluginId":plugin,"page":transition.page},"payload":next["payload"]})),
                            }
                        }
                        if response.success
                            && let Some(next) = continuation
                                .and_then(|t| t.resolve(&response.data).ok())
                                .flatten()
                        {
                            let target_plugin = next["target"]["pluginId"]
                                .as_str()
                                .unwrap_or(&request.page.plugin_id);
                            let target_page = next["target"]["page"].as_str().unwrap_or_default();
                            if catalog.page(target_plugin, target_page, &context).is_ok() {
                                return Ok(next);
                            }
                        }
                        Ok(serde_json::json!({"kind":"operation","response":response}))
                    }
                    PreparedAction::Ready {
                        target, payload, ..
                    } => Ok(serde_json::json!({"kind":"host","target":target,"payload":payload})),
                }
            })
            .await
    }
}
