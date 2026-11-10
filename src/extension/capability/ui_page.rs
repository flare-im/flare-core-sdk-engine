//! Plugin pages (a `page` contribution, such as a wallet): request parsing,
//! the source input with paging, and page actions. Binding-neutral; the
//! network calls are made by `CapabilityApi::plugin_page` and
//! `CapabilityApi::invoke_plugin_action`.
//!
//! A page is opened by plugin id and page id with its arguments (the payload
//! of the page target that opened it, `{}` from an account entry). It is
//! resolved against the result of its source, a declared `query`
//! ([`UiPageSource`]), or against its arguments when it has none. A page
//! belongs to no message and no entity: its calls carry no `messageId`.
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_plugin_protocol::{
    manifest::{ClientPlatform, UiContribution, UiContributionKind},
    ui::{
        DETAIL_CURSOR_INPUT, MAX_UI_ACTION_INPUT_BYTES, NativeCapability, ResolvedPluginView,
        ResolvedUiBlock, UiCatalog, UiUnavailableReason,
    },
};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

use super::ui::PreparedAction;

fn invalid() -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, "sdk.plugin.invalid_ui_request")
}
fn unavailable() -> FlareError {
    FlareError::localized(
        ErrorCode::OperationNotSupported,
        "sdk.plugin.ui_action_unavailable",
    )
}
fn admission_unavailable(stage: &'static str, reason: &'static str) -> FlareError {
    tracing::warn!(target: "flare_sdk.plugin", stage, reason, "plugin_ui_admission");
    // Native debug hosts may have no tracing subscriber. These two values are
    // local allowlisted tags, never a protocol error's arbitrary contents.
    #[cfg(debug_assertions)]
    eprintln!("plugin_ui_admission stage={stage} reason={reason}");
    unavailable()
}
pub(crate) fn page_admission_unavailable(reason: UiUnavailableReason) -> FlareError {
    let reason = match reason {
        UiUnavailableReason::Disabled => "disabled",
        UiUnavailableReason::NotInstalled => "not_installed",
        UiUnavailableReason::NoPermission => "no_permission",
        UiUnavailableReason::UnsupportedPlatform => "unsupported_platform",
        UiUnavailableReason::IncompatibleVersion => "incompatible_version",
        UiUnavailableReason::CatalogUnreachable => "catalog_unreachable",
        UiUnavailableReason::UnboundCopy => "unbound_copy",
    };
    admission_unavailable("page", reason)
}
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn bounded(value: &Value) -> Result<()> {
    if serde_json::to_vec(value).map_err(|_| invalid())?.len() > 2 * 1024 * 1024 {
        return Err(invalid());
    }
    Ok(())
}
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 160
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Pages one page refresh reads at most, the first included.
pub(crate) const MAX_PAGE_REFRESH_PAGES: usize = 10;

/// `plugin_page` input. `previous` and `refresh` follow the detail rules: a
/// next page (`cursor`) is appended to `previous`, the view shown so far
/// exactly as returned; `refresh` re-reads `previous` from the first page.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageRequest {
    pub plugin_id: String,
    pub page: String,
    pub platform: ClientPlatform,
    /// Host route context, independent of the source operation's business DTO.
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub locale: String,
    /// The payload of the page target that opened the page; `{}` when absent.
    #[serde(default)]
    pub arguments: Option<Value>,
    /// Values of the rows block's filter inputs, by input id.
    #[serde(default)]
    pub filters: Option<Value>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub previous: Option<Value>,
    #[serde(default)]
    pub refresh: bool,
}
impl PageRequest {
    pub fn arguments(&self) -> Value {
        self.arguments
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()))
    }
    /// The first page and a refresh degrade to an inert view when the page
    /// cannot be shown; a next page fails instead, so the host keeps what
    /// it shows and may retry.
    pub fn degrades(&self) -> bool {
        self.cursor.is_none()
    }
}

pub(crate) fn parse_page(request: Value) -> Result<PageRequest> {
    bounded(&request)?;
    let mut parsed: PageRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    if let Some(previous) = &parsed.previous {
        let previous_conversation = match previous.get("conversationId") {
            None | Some(Value::Null) => None,
            Some(Value::String(id)) if flare_plugin_protocol::resource::valid_resource_id(id) => {
                Some(id)
            }
            _ => return Err(invalid()),
        };
        match (parsed.conversation_id.as_ref(), previous_conversation) {
            (Some(current), Some(previous)) if current != previous => return Err(invalid()),
            (None, Some(previous)) => parsed.conversation_id = Some(previous.clone()),
            _ => {}
        }
    }
    if !valid_id(&parsed.plugin_id)
        || !valid_id(&parsed.page)
        || !parsed.page.starts_with(&format!("{}.", parsed.plugin_id))
        || parsed.locale.len() > 32
        || parsed
            .conversation_id
            .as_ref()
            .is_some_and(|id| !flare_plugin_protocol::resource::valid_resource_id(id))
        || parsed.locale.chars().any(char::is_control)
        || parsed.arguments.as_ref().is_some_and(|a| !a.is_object())
        || parsed.filters.as_ref().is_some_and(|f| !f.is_object())
        || (parsed.previous.is_some() && parsed.cursor.is_none() && !parsed.refresh)
        || (parsed.refresh && (parsed.previous.is_none() || parsed.cursor.is_some()))
    {
        return Err(invalid());
    }
    Ok(parsed)
}

/// A view a host passes back (`previous`): the view and the `data` it was
/// returned with.
pub(crate) fn split_previous(previous: Value) -> Result<(ResolvedPluginView, Value)> {
    let Value::Object(mut object) = previous else {
        return Err(invalid());
    };
    let data = object.remove("data").unwrap_or(Value::Null);
    object.remove("conversationId");
    let view = serde_json::from_value(Value::Object(object)).map_err(|_| invalid())?;
    Ok((view, data))
}

/// A page ready to read page by page.
pub(crate) struct PreparedPage {
    pub source: Option<flare_plugin_protocol::ui::UiPageSource>,
    /// The first page's source input, or the arguments of a page without a source.
    first: Value,
}
impl PreparedPage {
    /// The source input of the first page, or of the page after `cursor`
    /// (refused for a source without `cursorPath`, for a malformed cursor and
    /// for a page without a source).
    pub fn input(&self, cursor: Option<&str>) -> Result<Value> {
        match (cursor, &self.source) {
            (None, _) => Ok(self.first.clone()),
            (Some(cursor), Some(source)) => source
                .page_input(self.first.clone(), cursor)
                .map_err(|_| invalid()),
            (Some(_), None) => Err(invalid()),
        }
    }
}

/// Bind the page's first source input: arguments through the declared
/// bindings, plus the validated filter values (only on a page with a rows
/// block). A page without a source resolves against its arguments.
pub(crate) fn prepare_page(
    contribution: &UiContribution,
    request: &PageRequest,
) -> Result<PreparedPage> {
    let filters = match &request.filters {
        None => Map::new(),
        Some(_) if contribution.schema.rows().is_none() => return Err(invalid()),
        Some(values) => contribution
            .schema
            .validate_inputs(values)
            .map_err(|_| invalid())?,
    };
    let arguments = request.arguments();
    match &contribution.source {
        Some(source) => Ok(PreparedPage {
            source: Some(source.clone()),
            first: source
                .bind_input(&arguments, filters)
                .map_err(|_| invalid())?,
        }),
        None if filters.is_empty() => Ok(PreparedPage {
            source: None,
            first: arguments,
        }),
        None => Err(invalid()),
    }
}

/// Entries and rows a view shows: how far a refresh reads to keep the pages
/// the host had loaded.
pub(crate) fn page_items(view: &ResolvedPluginView) -> usize {
    view.blocks
        .iter()
        .map(|block| match block {
            ResolvedUiBlock::Entries(entries) => entries.items.len(),
            ResolvedUiBlock::Rows(rows) => rows.rows.len(),
            _ => 0,
        })
        .sum()
}

/// The cursor a refresh reads next, while the view is short of `wanted`
/// items, has a next page and fewer than [`MAX_PAGE_REFRESH_PAGES`] were read.
pub(crate) fn refresh_continues(
    view: &ResolvedPluginView,
    wanted: usize,
    pages: usize,
) -> Option<&str> {
    (pages < MAX_PAGE_REFRESH_PAGES && page_items(view) < wanted)
        .then_some(view.next_cursor.as_deref())
        .flatten()
}

/// A page view as the bindings return it: the view plus `data`, what it was
/// resolved against (the first page's source result, or the arguments), which
/// the host passes back with a page action.
pub(crate) fn page_view(
    view: &ResolvedPluginView,
    data: Option<Value>,
    conversation: Option<&str>,
) -> Result<Value> {
    let mut value = serde_json::to_value(view).map_err(|_| invalid())?;
    if view.available
        && let (Some(data), Some(object)) = (data, value.as_object_mut())
    {
        object.insert("data".into(), data);
    }
    if let Some(conversation) = conversation {
        value["conversationId"] = conversation.into();
    }
    Ok(value)
}

/// Carry host route context alongside navigation, never inside plugin arguments.
pub(crate) fn page_navigation(mut result: Value, conversation: Option<&str>) -> Value {
    if result["kind"] == "host"
        && result["target"]["kind"] == "page"
        && let Some(conversation) = conversation
    {
        result["conversationId"] = conversation.into();
    }
    result
}

/// Why a page whose source call failed with `error` is inert, when the
/// failure means the plugin is not available to the viewer any more.
pub(crate) fn inert_reason(error: &FlareError) -> Option<UiUnavailableReason> {
    let FlareError::Localized { reason, .. } = error else {
        return None;
    };
    Some(match reason.as_str() {
        "sdk.plugin.disabled" | "sdk.plugin.feature_disabled" => UiUnavailableReason::Disabled,
        "sdk.plugin.not_installed" => UiUnavailableReason::NotInstalled,
        "sdk.plugin.permission_denied" => UiUnavailableReason::NoPermission,
        "sdk.plugin.unsupported_platform" => UiUnavailableReason::UnsupportedPlatform,
        _ => return None,
    })
}

/// The page an action comes from.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageRef {
    pub plugin_id: String,
    pub page: String,
}
/// The row a row action comes from: its key and the input the view bound
/// for it (`rows[].actions[].input`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageRow {
    pub key: String,
    pub input: Value,
}
/// A page action: like a card action (`plugin_ui_invoke`), with `page`
/// instead of `message`, the page view's `data` instead of a payload, and
/// `row` for a row action. Never with `messageId`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PageActionRequest {
    pub page: PageRef,
    pub platform: ClientPlatform,
    pub action_id: String,
    pub user_gesture: bool,
    #[serde(default)]
    pub native_allowlist: BTreeSet<NativeCapability>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub row: Option<PageRow>,
    #[serde(default)]
    pub form_values: Option<Value>,
    #[serde(default)]
    pub selection: Option<Vec<u32>>,
}

/// Whether an invoke request is a page action (`page` instead of `message`).
pub(crate) fn is_page_action(request: &Value) -> bool {
    request.get("page").is_some() && request.get("message").is_none()
}

pub(crate) fn parse_page_action(request: Value) -> Result<PageActionRequest> {
    bounded(&request)?;
    let parsed: PageActionRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    if !valid_id(&parsed.page.plugin_id)
        || !valid_id(&parsed.page.page)
        || parsed.data.as_ref().is_some_and(|d| !d.is_object())
        || parsed
            .conversation_id
            .as_ref()
            .is_some_and(|id| !flare_plugin_protocol::resource::valid_resource_id(id))
        || parsed
            .row
            .as_ref()
            .is_some_and(|row| row.key.is_empty() || row.key.len() > 256 || !row.input.is_object())
    {
        return Err(invalid());
    }
    Ok(parsed)
}

/// Check a page action against the catalog (declaration, platform, gesture,
/// host consent) and bind its input: a document action over the page `data`
/// (form and choices selection as on a card); a row action from the row's
/// bound input, which must name exactly the action's declared inputs, plus
/// its form. Nothing is sent from here.
pub(crate) fn prepare_page_action(
    tenant: &str,
    catalog: &UiCatalog,
    request: &PageActionRequest,
) -> Result<PreparedAction> {
    let action_now_ms = now_ms();
    let target = catalog
        .action(
            tenant,
            &request.page.plugin_id,
            &request.page.page,
            &request.action_id,
            request.platform,
            &request.native_allowlist,
            request.user_gesture,
            action_now_ms,
        )
        .map_err(|error| {
            use flare_plugin_protocol::ProtocolError::Denied;
            let reason = match error {
                Denied("UI action unavailable") if tenant != catalog.tenant_id => "tenant_mismatch",
                Denied("UI action unavailable") if action_now_ms >= catalog.valid_until_ms => {
                    "catalog_expired"
                }
                Denied("UI action unavailable") if !request.user_gesture => "gesture_required",
                Denied("UI action unavailable") => "action_missing",
                Denied("UI plugin unavailable") => "plugin_or_platform_unavailable",
                Denied("UI contribution unavailable") => "contribution_unavailable",
                Denied("UI operation unavailable") => "operation_unavailable",
                Denied("UI page unavailable") => "target_page_unavailable",
                Denied("native capability unavailable") => "native_capability_unavailable",
                Denied("invalid UI document bounds") => "schema_document_bounds",
                Denied("invalid UI presentation") => "schema_presentation",
                Denied("invalid UI translation") => "schema_translation",
                Denied("invalid UI field") => "schema_field",
                Denied("invalid UI input") => "schema_input",
                Denied("invalid UI input condition") => "schema_input_condition",
                Denied("invalid UI action bindings") => "schema_action_bindings",
                Denied("invalid UI action condition") => "schema_action_condition",
                Denied("invalid UI action form") => "schema_action_form",
                Denied("invalid UI action") => "schema_action",
                Denied("invalid UI block") => "schema_block",
                Denied("invalid UI action placement") => "schema_action_placement",
                _ => "other_protocol_error",
            };
            admission_unavailable("action", reason)
        })?;
    let context = flare_plugin_protocol::ui::UiRenderContext {
        tenant,
        platform: request.platform,
        kind: UiContributionKind::Page,
        locale: "",
        viewer: "",
        now_ms: now_ms(),
    };
    let (_, contribution) = catalog
        .page(&request.page.plugin_id, &request.page.page, &context)
        .map_err(|_| unavailable())?;
    let document = &contribution.schema;
    let action = document
        .actions
        .iter()
        .find(|a| a.id == request.action_id)
        .ok_or_else(invalid)?;
    let data = request
        .data
        .clone()
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut extra = Map::new();
    if !action.form.is_empty() || request.form_values.is_some() {
        let values = request.form_values.as_ref().unwrap_or(&Value::Null);
        match document.validate_form_over(action, &data, values) {
            Ok(values) => extra.extend(values),
            Err(errors) => return Ok(PreparedAction::FormErrors(errors)),
        }
    }
    let row_action = document
        .rows()
        .is_some_and(|rows| rows.actions.contains(&action.id));
    let payload = match (row_action, &request.row) {
        (true, Some(row)) => {
            if request.selection.is_some() {
                return Err(invalid());
            }
            let Value::Object(input) = &row.input else {
                return Err(invalid());
            };
            let declared: BTreeSet<&str> = action.input.keys().map(String::as_str).collect();
            if input.keys().map(String::as_str).collect::<BTreeSet<_>>() != declared {
                return Err(invalid());
            }
            let mut output = input.clone();
            for (name, value) in extra {
                if name == DETAIL_CURSOR_INPUT || output.contains_key(&name) {
                    return Err(invalid());
                }
                output.insert(name, value);
            }
            let output = Value::Object(output);
            if serde_json::to_vec(&output).map_or(true, |v| v.len() > MAX_UI_ACTION_INPUT_BYTES) {
                return Err(invalid());
            }
            output
        }
        (false, None) => {
            match (
                document.choices_for(&action.id),
                request.selection.as_deref(),
            ) {
                (Some(choices), Some(selection)) => {
                    let (name, value) = choices
                        .selection_input(&data, selection)
                        .map_err(|_| invalid())?;
                    extra.insert(name, value);
                }
                (None, None) => {}
                _ => return Err(invalid()),
            }
            flare_plugin_protocol::ui::bind_action_input_with(action, &data, extra)
                .map_err(|_| invalid())?
        }
        // A row action needs its row; a row needs a row action.
        _ => return Err(invalid()),
    };
    Ok(PreparedAction::Ready {
        target,
        payload,
        entity: None,
    })
}
