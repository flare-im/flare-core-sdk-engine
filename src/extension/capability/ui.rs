//! Binding-neutral UI evaluation. Native adapters render the result through the
//! design kit and execute only a returned target after explicit host consent.
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_plugin_protocol::{
    manifest::{ClientPlatform, UiContribution, UiContributionKind, is_ui_icon_name},
    state::SharedState,
    ui::{
        ClientPlugin, NativeCapability, PluginMessage, PluginMessageState, ResolvedPluginView,
        ResolvedUiBlock, UiActionTarget, UiAudience, UiCatalog, UiDetail, UiEntryAvailability,
        UiEntrySummary, UiFormError, UiRenderContext, UiStateLayers, UiUnavailablePlugin,
        UiUnavailableReason,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResolveRequest {
    catalog: UiCatalog,
    message: PluginMessage,
    platform: ClientPlatform,
    #[serde(default)]
    locale: String,
    /// Signed-in user, set by the SDK session; never taken from plugin payload.
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    kind: Option<UiContributionKind>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActionRequest {
    catalog: UiCatalog,
    platform: ClientPlatform,
    plugin_id: String,
    contribution_id: String,
    action_id: String,
    native_allowlist: BTreeSet<NativeCapability>,
    user_gesture: bool,
    payload: Value,
}
fn invalid() -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, "sdk.plugin.invalid_ui_request")
}
/// The device clock. Every catalog evaluated here has had its validity moved
/// onto it first ([`localize_validity`]).
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
/// Moves a catalog's `validUntilMs`, the server's clock, onto the device clock
/// the evaluation here compares it with: a device clock running ahead of the
/// server must not expire the catalog early, nor one behind keep it late.
pub(crate) fn localize_validity(catalog: &mut UiCatalog, clock: &crate::kernel::ServerClock) {
    catalog.valid_until_ms = clock.to_local_ms(catalog.valid_until_ms);
}
fn bounded(value: &Value) -> Result<()> {
    if serde_json::to_vec(value).map_err(|_| invalid())?.len() > 2 * 1024 * 1024 {
        return Err(invalid());
    }
    Ok(())
}
/// Network orchestration inputs never accept a caller-provided catalog, tenant
/// or actor. Every click refreshes the authenticated catalog in CapabilityApi.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PresentRequest {
    pub message: PluginMessage,
    pub platform: ClientPlatform,
    #[serde(default)]
    pub locale: String,
    #[serde(default)]
    pub kind: Option<UiContributionKind>,
    /// Message the card belongs to (server id or client message id). Its stored
    /// plugin state is overlaid on the payload before resolving.
    #[serde(default)]
    pub message_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InvokeRequest {
    pub message: PluginMessage,
    pub platform: ClientPlatform,
    pub action_id: String,
    pub user_gesture: bool,
    #[serde(default)]
    pub native_allowlist: BTreeSet<NativeCapability>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    /// Message the action came from. Only then is a returned `data.messageState`
    /// written back to that message.
    #[serde(default)]
    pub message_id: Option<String>,
    /// Values of the action's `form`, keyed by input id; validated before any
    /// request is sent.
    #[serde(default)]
    pub form_values: Option<Value>,
    /// Option indices chosen in the choices block that owns the action.
    #[serde(default)]
    pub selection: Option<Vec<u32>>,
}
/// "View all" of a message card: the contribution's detail operation, bound
/// from the card data, then its detail document resolved against the result.
/// A paged detail (`cursorPath`) takes the next page with `cursor`, the
/// `nextCursor` of the view shown so far, and `previous`, that view itself:
/// the page is appended to it here, so hosts never merge pages themselves.
/// `refresh` with `previous` re-reads the shown view from its first page and
/// keeps the pages it had loaded ([`MAX_DETAIL_REFRESH_PAGES`]).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DetailRequest {
    pub message: PluginMessage,
    pub platform: ClientPlatform,
    #[serde(default)]
    pub locale: String,
    pub user_gesture: bool,
    #[serde(default)]
    pub conversation_id: Option<String>,
    /// Message the card belongs to; its state layers feed the detail input.
    #[serde(default)]
    pub message_id: Option<String>,
    /// The page after this cursor (the shown view's `nextCursor`); absent for
    /// the first page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// The view shown so far, exactly as an earlier call returned it. With
    /// `cursor` the page is appended to it; with `refresh` it is the view to
    /// bring up to date. Without either only the page comes back.
    #[serde(default)]
    pub previous: Option<ResolvedPluginView>,
    /// Re-read `previous` (required) from the first page, following the
    /// cursors until the entries are at least as many as `previous` showed,
    /// the detail has no next page, or [`MAX_DETAIL_REFRESH_PAGES`] pages were
    /// read; the merged view replaces `previous`. Not with `cursor`.
    #[serde(default)]
    pub refresh: bool,
}
/// Pages one detail refresh reads at most, the first included: a sheet that
/// had loaded more comes back with `nextCursor` to load the rest on demand.
pub(crate) const MAX_DETAIL_REFRESH_PAGES: usize = 10;
fn valid_locale(locale: &str) -> bool {
    locale.len() <= 32 && !locale.chars().any(char::is_control)
}
fn valid_message_id(id: &Option<String>) -> bool {
    id.as_deref().is_none_or(|id| {
        !id.is_empty()
            && id.len() <= 256
            && !id.chars().any(|c| c.is_control() || c.is_whitespace())
    })
}
pub(crate) fn parse_present(request: Value) -> Result<PresentRequest> {
    bounded(&request)?;
    let parsed: PresentRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    parsed.message.validate().map_err(|_| invalid())?;
    if !valid_message_id(&parsed.message_id) || !valid_locale(&parsed.locale) {
        return Err(invalid());
    }
    Ok(parsed)
}
pub(crate) fn parse_detail(request: Value) -> Result<DetailRequest> {
    bounded(&request)?;
    let parsed: DetailRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    parsed.message.validate().map_err(|_| invalid())?;
    if !valid_message_id(&parsed.message_id)
        || !valid_locale(&parsed.locale)
        || parsed
            .conversation_id
            .as_ref()
            .is_some_and(|id| !flare_plugin_protocol::resource::valid_resource_id(id))
        // A page is appended to the view it follows, never to nothing; a
        // refresh re-reads a shown view from its first page.
        || (parsed.previous.is_some() && parsed.cursor.is_none() && !parsed.refresh)
        || (parsed.refresh && (parsed.previous.is_none() || parsed.cursor.is_some()))
    {
        return Err(invalid());
    }
    Ok(parsed)
}
pub(crate) fn parse_invoke(request: Value) -> Result<InvokeRequest> {
    bounded(&request)?;
    let parsed: InvokeRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    parsed.message.validate().map_err(|_| invalid())?;
    if parsed
        .conversation_id
        .as_ref()
        .is_some_and(|id| !flare_plugin_protocol::resource::valid_resource_id(id))
        || !valid_message_id(&parsed.message_id)
    {
        return Err(invalid());
    }
    Ok(parsed)
}
/// A "+" panel entry declared by an authorized plugin (`composer-action`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginComposerAction {
    pub plugin_id: String,
    pub contribution_id: String,
    /// Contribution title in the requested locale (exact tag, then language).
    pub title: String,
    /// Kit icon name; absent when undeclared or malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    pub order: i32,
    /// Who sees the entry (conversation types, group roles); absent: everyone.
    /// Hosts filter with the open conversation; presentation only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<UiAudience>,
    /// The plugin's declaration of whether the entry can be used now, asked
    /// by the SDK before listing it (see [`UiEntryAvailability`]); never on
    /// the wire, hosts only see entries that passed.
    #[serde(skip)]
    pub availability: Option<UiEntryAvailability>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ComposerActionsRequest {
    pub platform: ClientPlatform,
    #[serde(default)]
    pub locale: String,
}
pub(crate) fn parse_composer_actions(request: Value) -> Result<ComposerActionsRequest> {
    bounded(&request)?;
    let parsed: ComposerActionsRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    if !valid_locale(&parsed.locale) {
        return Err(invalid());
    }
    Ok(parsed)
}
/// Composer actions of `catalog` for `platform`, by order then title. The
/// server already trimmed the catalog by grant, capability policy and user
/// consent, so this only drops what a client cannot render: another tenant's
/// or an expired catalog, other platforms, unknown schema versions and invalid
/// documents. An entry declaring an availability the catalog does not offer
/// (a malformed one, or an operation outside the plugin's offered operations)
/// is dropped too; the others carry it for the SDK to ask before listing.
pub(crate) fn composer_actions(
    tenant: &str,
    catalog: &UiCatalog,
    platform: ClientPlatform,
    locale: &str,
) -> Vec<PluginComposerAction> {
    if catalog.tenant_id != tenant || catalog.valid_until_ms <= now_ms() {
        return Vec::new();
    }
    let mut actions: Vec<_> = catalog
        .plugins
        .iter()
        .filter(|plugin| plugin.extensions.platforms.contains(&platform))
        .flat_map(|plugin| {
            plugin
                .extensions
                .contributions
                .iter()
                .filter(|c| {
                    c.kind == UiContributionKind::ComposerAction
                        && c.schema_version == 1
                        && c.schema.validate().is_ok()
                        // A malformed audience hides the entry rather than showing it to everyone.
                        && c.audience.as_ref().is_none_or(|a| a.validate().is_ok())
                        // An availability the client cannot ask hides the entry.
                        && c.availability.as_ref().is_none_or(|a| {
                            a.validate().is_ok() && plugin.operations.contains(&a.operation)
                        })
                })
                .map(|c| PluginComposerAction {
                    plugin_id: plugin.plugin_id.clone(),
                    contribution_id: c.id.clone(),
                    title: c.schema.localized_title(locale).to_owned(),
                    icon: c.icon.clone().filter(|icon| is_ui_icon_name(icon)),
                    order: plugin
                        .configuration
                        .get("composerOrder")
                        .and_then(Value::as_i64)
                        .and_then(|order| i32::try_from(order).ok())
                        .map_or(c.order, |order| order.saturating_add(c.order)),
                    audience: c.audience.clone().filter(|a| *a != UiAudience::default()),
                    availability: c.availability.clone(),
                })
        })
        .collect();
    actions.sort_by(|a, b| {
        (a.order, &a.title, &a.contribution_id).cmp(&(b.order, &b.title, &b.contribution_id))
    });
    actions
}
/// A "Me" (account) screen entry declared by an authorized plugin
/// (`account-entry`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginAccountEntry {
    pub plugin_id: String,
    pub contribution_id: String,
    /// Contribution title in the requested locale (exact tag, then language).
    pub title: String,
    /// Kit icon name; absent when undeclared or malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    pub order: i32,
    /// The `page` contribution of the same plugin the entry opens.
    pub page: String,
    /// The subtitle source: a `query` operation (called with `{}`) and the
    /// pointer of a scalar in its result data. Never called while listing;
    /// the host calls it when it shows the entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<UiEntrySummary>,
}
/// Account entries of `catalog` for `platform`, by order then title, with the
/// same rules as [`composer_actions`]: the server already trimmed the catalog,
/// so this only drops what a client cannot render (another tenant's or an
/// expired catalog, other platforms, unknown schema versions, invalid
/// documents, an entry whose page is not in the catalog, a summary operation
/// the catalog does not offer).
pub(crate) fn account_entries(
    tenant: &str,
    catalog: &UiCatalog,
    platform: ClientPlatform,
    locale: &str,
) -> Vec<PluginAccountEntry> {
    if catalog.tenant_id != tenant || catalog.valid_until_ms <= now_ms() {
        return Vec::new();
    }
    let mut entries: Vec<_> = catalog
        .plugins
        .iter()
        .filter(|plugin| plugin.extensions.platforms.contains(&platform))
        .flat_map(|plugin| {
            plugin
                .extensions
                .contributions
                .iter()
                .filter(|c| {
                    c.kind == UiContributionKind::AccountEntry
                        && c.schema_version == 1
                        && c.schema.validate().is_ok()
                })
                .filter_map(move |c| {
                    let page = match c.schema.actions.as_slice() {
                        [action] => match &action.target {
                            UiActionTarget::Page { page, .. } => page,
                            _ => return None,
                        },
                        _ => return None,
                    };
                    if !plugin
                        .extensions
                        .contributions
                        .iter()
                        .any(|p| &p.id == page && p.kind == UiContributionKind::Page)
                    {
                        return None;
                    }
                    Some(PluginAccountEntry {
                        plugin_id: plugin.plugin_id.clone(),
                        contribution_id: c.id.clone(),
                        title: c.schema.localized_title(locale).to_owned(),
                        icon: c.icon.clone().filter(|icon| is_ui_icon_name(icon)),
                        order: c.order,
                        page: page.clone(),
                        summary: c.summary.clone().filter(|summary| {
                            summary.validate().is_ok()
                                && plugin.operations.contains(&summary.operation)
                        }),
                    })
                })
        })
        .collect();
    entries.sort_by(|a, b| {
        (a.order, &a.title, &a.contribution_id).cmp(&(b.order, &b.title, &b.contribution_id))
    });
    entries
}

/// The catalog in a `flare.plugin.ui.catalog` result, read plugin by plugin so
/// one plugin a client cannot read (a newer manifest field, an unknown
/// contribution kind) costs only that plugin, or only that contribution:
/// - a contribution that does not parse is dropped from its plugin;
/// - a plugin that still does not parse is dropped and listed in
///   `unavailable` as `incompatible_version` (when its id is readable);
/// - an `unavailable` entry that does not parse is dropped.
///
/// `None` when the envelope itself (tenant, version, validity, plugin list)
/// is unreadable.
pub(crate) fn parse_catalog(data: &Value) -> Option<UiCatalog> {
    let object = data.as_object()?;
    let mut catalog = UiCatalog {
        tenant_id: object.get("tenantId")?.as_str()?.to_owned(),
        version: object.get("version")?.as_u64()?,
        valid_until_ms: object.get("validUntilMs")?.as_i64()?,
        plugins: Vec::new(),
        unavailable: object
            .get("unavailable")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| serde_json::from_value(item.clone()).ok())
                    .collect()
            })
            .unwrap_or_default(),
    };
    for raw in object.get("plugins")?.as_array()? {
        match serde_json::from_value::<ClientPlugin>(raw.clone()) {
            Ok(plugin) => catalog.plugins.push(plugin),
            Err(_) => {
                let mut trimmed = raw.clone();
                if let Some(contributions) = trimmed
                    .pointer_mut("/extensions/contributions")
                    .and_then(Value::as_array_mut)
                {
                    contributions
                        .retain(|c| serde_json::from_value::<UiContribution>(c.clone()).is_ok());
                }
                match serde_json::from_value::<ClientPlugin>(trimmed) {
                    Ok(plugin) => {
                        tracing::warn!(
                            target: "flare_sdk.plugin",
                            plugin_id = %plugin.plugin_id,
                            "plugin catalog: contributions this client cannot read were dropped"
                        );
                        catalog.plugins.push(plugin);
                    }
                    Err(_) => {
                        let plugin_id = raw.get("pluginId").and_then(Value::as_str);
                        tracing::warn!(
                            target: "flare_sdk.plugin",
                            plugin_id = plugin_id.unwrap_or_default(),
                            "plugin catalog: a plugin this client cannot read was dropped"
                        );
                        if let Some(plugin_id) = plugin_id.filter(|id| {
                            !id.is_empty()
                                && id.len() <= 128
                                && !catalog.unavailable.iter().any(|p| p.plugin_id == *id)
                        }) {
                            catalog.unavailable.push(UiUnavailablePlugin {
                                plugin_id: plugin_id.to_owned(),
                                reason: UiUnavailableReason::IncompatibleVersion,
                            });
                        }
                    }
                }
            }
        }
    }
    Some(catalog)
}

pub(crate) fn resolve(
    tenant: &str,
    request: Value,
    clock: &crate::kernel::ServerClock,
) -> Result<Value> {
    bounded(&request)?;
    let mut request: ResolveRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    localize_validity(&mut request.catalog, clock);
    serde_json::to_value(request.catalog.resolve_with_context(
        &request.message,
        flare_plugin_protocol::ui::UiRenderContext {
            tenant,
            platform: request.platform,
            kind: request.kind.unwrap_or(UiContributionKind::MessageContent),
            locale: &request.locale,
            viewer: &request.viewer,
            now_ms: now_ms(),
        },
    ))
    .map_err(|_| invalid())
}
pub(crate) fn action(
    tenant: &str,
    request: Value,
    clock: &crate::kernel::ServerClock,
) -> Result<Value> {
    bounded(&request)?;
    let mut request: ActionRequest = serde_json::from_value(request).map_err(|_| invalid())?;
    localize_validity(&mut request.catalog, clock);
    let target = request
        .catalog
        .action(
            tenant,
            &request.plugin_id,
            &request.contribution_id,
            &request.action_id,
            request.platform,
            &request.native_allowlist,
            request.user_gesture,
            now_ms(),
        )
        .map_err(|_| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.plugin.ui_action_unavailable",
            )
        })?;
    let action = request
        .catalog
        .plugins
        .iter()
        .find(|p| p.plugin_id == request.plugin_id)
        .and_then(|p| {
            p.extensions
                .contributions
                .iter()
                .find(|c| c.id == request.contribution_id)
        })
        .and_then(|c| c.schema.actions.iter().find(|a| a.id == request.action_id))
        .ok_or_else(invalid)?;
    let payload = flare_plugin_protocol::ui::bind_action_input(action, &request.payload)
        .map_err(|_| invalid())?;
    Ok(serde_json::json!({"target":target,"payload":payload}))
}

/// The contribution a card of `message` resolves through, on a client surface.
pub(crate) fn contribution_of<'a>(
    catalog: &'a UiCatalog,
    message: &PluginMessage,
    kind: Option<UiContributionKind>,
) -> Option<&'a UiContribution> {
    plugin_contribution_of(catalog, message, kind).map(|(_, contribution)| contribution)
}

/// [`contribution_of`] with the plugin that declares it, whose
/// [`ClientPlugin::document_data`] is the data the document is resolved,
/// validated and bound against.
pub(crate) fn plugin_contribution_of<'a>(
    catalog: &'a UiCatalog,
    message: &PluginMessage,
    kind: Option<UiContributionKind>,
) -> Option<(&'a ClientPlugin, &'a UiContribution)> {
    let plugin = catalog
        .plugins
        .iter()
        .find(|p| p.plugin_id == message.plugin_id)?;
    let contribution = plugin.extensions.contributions.iter().find(|c| {
        c.id == message.contribution_id
            && c.schema_version == message.schema_version
            && kind.is_none_or(|kind| c.kind == kind)
    })?;
    Some((plugin, contribution))
}

/// The two state layers stored on a card's message: the shared layer from Core
/// and the viewer's local layer. The shared layer's entity defaults to the
/// card's own (history carries values and version only). `unbound` is Core's
/// mark on a card that is not the one bound to its entity.
#[derive(Default)]
pub(crate) struct CardState {
    pub shared: Option<SharedState>,
    pub local: Option<PluginMessageState>,
    pub unbound: bool,
}
impl CardState {
    pub fn of(stored: Option<&crate::model::IMMessage>, entity: Option<&str>) -> Self {
        Self {
            shared: stored.and_then(|m| m.plugin_shared_state(entity)),
            local: stored.and_then(crate::model::IMMessage::plugin_message_state),
            unbound: stored.is_some_and(crate::model::IMMessage::plugin_card_unbound),
        }
    }
    pub fn layers(&self) -> UiStateLayers<'_> {
        UiStateLayers {
            shared: self.shared.as_ref(),
            local: self.local.as_ref(),
        }
    }
}

pub(crate) struct CardPresentation<'a> {
    pub tenant: &'a str,
    pub platform: ClientPlatform,
    pub kind: UiContributionKind,
    pub locale: &'a str,
    pub viewer: &'a str,
}
impl CardPresentation<'_> {
    fn context(&self) -> UiRenderContext<'_> {
        UiRenderContext {
            tenant: self.tenant,
            platform: self.platform,
            kind: self.kind,
            locale: self.locale,
            viewer: self.viewer,
            now_ms: now_ms(),
        }
    }
}

/// The v2 view of a card over its state layers (`payload ← shared ← local`),
/// plus `data`, the data it was resolved against, while the view is available:
/// the merged card data, or `{"config": ...}` for a composer-action form
/// ([`ClientPlugin::document_data`]). A card Core marked as an unbound copy
/// comes back read-only ([`ResolvedPluginView::into_read_only`]).
pub(crate) fn present_card(
    catalog: &UiCatalog,
    message: &PluginMessage,
    state: &CardState,
    presentation: &CardPresentation<'_>,
) -> Result<Value> {
    let mut view = catalog.resolve_with_state(message, state.layers(), presentation.context());
    if state.unbound {
        view = view.into_read_only();
    }
    let available = view.available;
    let mut view = serde_json::to_value(view).map_err(|_| invalid())?;
    if available
        && let Some((plugin, contribution)) =
            plugin_contribution_of(catalog, message, Some(presentation.kind))
        && let Some(object) = view.as_object_mut()
    {
        object.insert(
            "data".into(),
            plugin.document_data(contribution, &message.payload, state.layers()),
        );
    }
    Ok(view)
}

/// A card action ready to run, or the problems with its form.
pub(crate) enum PreparedAction {
    FormErrors(Vec<UiFormError>),
    Ready {
        target: UiActionTarget,
        /// The action input: declared bindings over the merged card data, plus
        /// the validated form values and the choices selection.
        payload: Value,
        /// The card's entity (`entityPath`), for the result's `sharedState`.
        entity: Option<String>,
    },
}

/// Check the action against the catalog (declaration, platform, gesture, host
/// consent), validate its form, read the choices selection, and bind the input,
/// all over the document's data ([`ClientPlugin::document_data`]): the merged
/// card data, or `{"config": ...}` for a composer-action form, so its inputs'
/// `maxPath` and `defaultPath` apply as the host rendered them. Nothing is
/// sent from here.
pub(crate) fn prepare_action(
    tenant: &str,
    catalog: &UiCatalog,
    request: &InvokeRequest,
    stored: Option<&crate::model::IMMessage>,
) -> Result<PreparedAction> {
    let message = &request.message;
    let target = catalog
        .action(
            tenant,
            &message.plugin_id,
            &message.contribution_id,
            &request.action_id,
            request.platform,
            &request.native_allowlist,
            request.user_gesture,
            now_ms(),
        )
        .map_err(|_| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.plugin.ui_action_unavailable",
            )
        })?;
    let (plugin, contribution) =
        plugin_contribution_of(catalog, message, None).ok_or_else(invalid)?;
    let action = contribution
        .schema
        .actions
        .iter()
        .find(|a| a.id == request.action_id)
        .ok_or_else(invalid)?;
    let entity = contribution.entity_of(&message.payload);
    let mut data = plugin.document_data(
        contribution,
        &message.payload,
        CardState::of(stored, entity.as_deref()).layers(),
    );
    // Composer documents use tenant configuration, never arbitrary envelope
    // payload. Their navigation may also bind the conversation the host is in.
    // Take that single context field from the request, so a page workflow can
    // open without allowing the envelope to replace configuration or identity.
    if contribution.kind == UiContributionKind::ComposerAction
        && let Some(conversation) = request.conversation_id.as_deref()
        && let Some(object) = data.as_object_mut()
    {
        object.insert("conversationId".into(), Value::String(conversation.into()));
    }
    let mut extra = Map::new();
    if !action.form.is_empty() || request.form_values.is_some() {
        let values = request.form_values.as_ref().unwrap_or(&Value::Null);
        match contribution
            .schema
            .validate_form_over(action, &data, values)
        {
            Ok(values) => extra.extend(values),
            Err(errors) => return Ok(PreparedAction::FormErrors(errors)),
        }
    }
    match (
        contribution.schema.choices_for(&action.id),
        request.selection.as_deref(),
    ) {
        (Some(choices), Some(selection)) => {
            let (name, value) = choices
                .selection_input(&data, selection)
                .map_err(|_| invalid())?;
            extra.insert(name, value);
        }
        (None, None) => {}
        // A choices action needs a selection; a selection needs a choices action.
        _ => return Err(invalid()),
    }
    let payload = flare_plugin_protocol::ui::bind_action_input_with(action, &data, extra)
        .map_err(|_| invalid())?;
    Ok(PreparedAction::Ready {
        target,
        payload,
        entity,
    })
}

/// The detail of a card, ready to request page by page.
pub(crate) struct PreparedDetail {
    detail: UiDetail,
    /// The first page's input, bound over the merged card data.
    first: Value,
}
impl PreparedDetail {
    pub fn operation(&self) -> &str {
        &self.detail.operation
    }
    /// The operation input of the first page, or with `cursor` of the page
    /// after it ([`UiDetail::page_input`]): refused for a detail without
    /// `cursorPath` and for a malformed cursor.
    pub fn input(&self, cursor: Option<&str>) -> Result<Value> {
        match cursor {
            None => Ok(self.first.clone()),
            Some(cursor) => self
                .detail
                .page_input(self.first.clone(), cursor)
                .map_err(|_| invalid()),
        }
    }
}

/// The detail operation of a card and its first page's input, bound over the
/// merged card data.
pub(crate) fn prepare_detail(
    tenant: &str,
    catalog: &UiCatalog,
    request: &DetailRequest,
    stored: Option<&crate::model::IMMessage>,
) -> Result<PreparedDetail> {
    let message = &request.message;
    let detail = catalog
        .detail(
            tenant,
            &message.plugin_id,
            &message.contribution_id,
            request.platform,
            request.user_gesture,
            now_ms(),
        )
        .map_err(|_| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.plugin.ui_action_unavailable",
            )
        })?;
    let contribution = contribution_of(catalog, message, Some(UiContributionKind::MessageContent))
        .ok_or_else(invalid)?;
    let data = CardState::of(stored, contribution.entity_of(&message.payload).as_deref())
        .layers()
        .merge(contribution, &message.payload);
    let first = detail.bind_input(&data).map_err(|_| invalid())?;
    Ok(PreparedDetail { detail, first })
}

/// One page of a card's detail: the detail document resolved against the
/// detail operation's data, with `nextCursor` while a paged detail has more.
pub(crate) fn detail_page(
    catalog: &UiCatalog,
    message: &PluginMessage,
    data: &Value,
    presentation: &CardPresentation<'_>,
) -> ResolvedPluginView {
    catalog.resolve_detail(message, data, presentation.context())
}

/// Entries a view shows, over all of its entries blocks: how far a refresh
/// reads to keep the pages the host had loaded.
pub(crate) fn detail_entries(view: &ResolvedPluginView) -> usize {
    view.blocks
        .iter()
        .map(|block| match block {
            ResolvedUiBlock::Entries(entries) => entries.items.len(),
            _ => 0,
        })
        .sum()
}

/// The cursor of the page a refresh reads next, while the view is short of
/// the `wanted` entries, has a next page, and fewer than
/// [`MAX_DETAIL_REFRESH_PAGES`] pages were read.
pub(crate) fn detail_refresh_continues(
    view: &ResolvedPluginView,
    wanted: usize,
    pages: usize,
) -> Option<&str> {
    (pages < MAX_DETAIL_REFRESH_PAGES && detail_entries(view) < wanted)
        .then_some(view.next_cursor.as_deref())
        .flatten()
}

/// A detail view as the bindings return it.
pub(crate) fn detail_view(view: &ResolvedPluginView) -> Result<Value> {
    serde_json::to_value(view).map_err(|_| invalid())
}
