//! Web bundles (L3) for hosts: open a bundle page, carry its bridge messages,
//! and close it. The checks are the broker's
//! ([`crate::extension::capability::bundle::broker`]); hosts only move bytes
//! and draw what the engine hands them.
//!
//! Routes (`flare_capability_dispatch_json:<op>`; platform SDKs
//! `pluginBundleOpen`, ...):
//!
//! - `plugin_bundle_open` `{pluginId, contributionId, route?, arguments?,
//!   presentation?, conversation?: {id, type}, locale, theme, hostOrigin?}` →
//!   `{sessionId, origin, root, entry, csp, files?, hello}`. `root` is the
//!   cache directory of the verified files (native) or the `https` base of the
//!   unpacked copy on the sandbox domain (Web). `files` lists every file a
//!   native interceptor may serve (`path`, `contentType`, `size`); anything
//!   else is answered 403. `origin` is `https://{key}.plugin.flare.invalid`
//!   (Android), `flare-plugin://{key}` (iOS, Tauri) or `null` (Web, opaque),
//!   `key` being the tenant and plugin's origin key (D-03).
//! - `plugin_bundle_bridge` `{sessionId, envelope, userGesture?}` →
//!   `{envelope}` (post it back as it is) or `{host: {id, method, params}}`
//!   (draw it: `ui.navigate`, `ui.close`, `ui.setTitle`, `ui.toast`,
//!   `ui.confirm`, `ui.resize`, `ui.openExternal`, `clipboard.write`; then
//!   `plugin_bundle_host_result`). `userGesture` is the host's own record of a
//!   recent touch, never the bundle's claim.
//! - `plugin_bundle_host_result` `{sessionId, id, result? | error?}` →
//!   `{envelope}`; `error` is `cancelled`, `unavailable`, `gesture_required`,
//!   `timeout` or `not_found`.
//! - `plugin_bundle_host_event` `{event, payload, sessionId?}` for
//!   `theme.changed`, `locale.changed`, `visibility.changed` →
//!   `{deliveries: [{sessionId, envelope}]}` for the pages that subscribed.
//! - `plugin_bundle_close` `{sessionId}` → `{closed}`.
//! - `plugin_bundle_prefetch` `{wifi, idle}` → `{fetched}`: entry bundles
//!   fetched ahead (native, on Wi-Fi when idle).
//!
//! Events: `plugin.bundle_event` `{sessionId, envelope?, close?}` carries
//! what the engine sends on its own: `catalog.changed`, `entity.state`,
//! `lifecycle.revoked` (with `close`), and `close: "evicted"` when a newer
//! page took the place of the oldest.
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use flare_plugin_protocol::bridge::{
    BRIDGE_VERSION, BridgeConversation, BridgeEnvelope, BridgeError, BridgeErrorCode, BridgeEvent,
    BridgeGrant, BridgeMethod, BridgePerson, BridgePresentation, BridgeRequest, BridgeTheme,
    HelloResult, PeopleResolveResult, PluginOperationResult, RevokeReason, UiConfirmResult,
};
use flare_plugin_protocol::bundle::{BundleManifest, WebBundleDecl};
use flare_plugin_protocol::fields::Fields;
use flare_plugin_protocol::ui::{
    NativeCapability, UiCatalog, UiPageCommand, UiPageCommandMode, valid_bundle_route,
};
use serde_json::{Map, Value, json};

use super::{CapabilityApi, PluginCallFuture};
#[cfg(not(target_arch = "wasm32"))]
use crate::extension::capability::bundle::provider::{BundleDeliveryError, BundleLease};
use crate::extension::capability::bundle::{
    self, broker::Admission, broker::BridgeSession, broker::SessionContext, events,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// Who `people.resolve` may name.
#[derive(Clone)]
pub(crate) struct BundlePeople {
    pub profiles: Arc<dyn crate::domain::UserReader>,
    pub participants: Option<Arc<dyn crate::domain::ConversationParticipantStore>>,
}

/// Members a conversation is read with for `people.resolve`.
const MAX_MEMBERS_READ: u32 = 2000;

#[derive(serde::Deserialize)]
struct BundleRosterPage {
    conversation_id: String,
    participants: Vec<BundleRosterMember>,
    next_cursor: String,
    has_more: bool,
}

#[derive(serde::Deserialize)]
struct BundleRosterMember {
    user_id: String,
    #[serde(default)]
    attributes: HashMap<String, String>,
}

fn bundle_error(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::OperationNotSupported,
        format!("sdk.plugin_bundle.{reason}"),
    )
}

fn invalid() -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        "sdk.plugin_bundle.invalid_request",
    )
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 192
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

// Host requests are read with the bridge's one field reader
// ([`flare_plugin_protocol::fields`]): only the named fields, each as its
// serde derive would read it.
struct OpenRequest {
    plugin_id: String,
    contribution_id: String,
    route: Option<String>,
    arguments: Map<String, Value>,
    presentation: BridgePresentation,
    conversation: Option<BridgeConversation>,
    locale: String,
    theme: BridgeTheme,
    /// Web: the app's origin, for the policy's `frame-ancestors`.
    host_origin: Option<String>,
}

impl OpenRequest {
    fn read(value: Value) -> Option<Self> {
        let mut f = Fields::new(
            value,
            &[
                "pluginId",
                "contributionId",
                "route",
                "arguments",
                "presentation",
                "conversation",
                "locale",
                "theme",
                "hostOrigin",
            ],
        )?;
        Some(Self {
            plugin_id: f.string("pluginId")?,
            contribution_id: f.string("contributionId")?,
            route: f.opt_string("route")?,
            arguments: f.object_or_empty("arguments")?,
            presentation: f.name_or(
                "presentation",
                BridgePresentation::parse,
                BridgePresentation::Page,
            )?,
            conversation: match f.optional("conversation") {
                None => None,
                Some(value) => Some(BridgeConversation::read(value)?),
            },
            locale: f.string_or_empty("locale")?,
            theme: BridgeTheme::read(f.take("theme")?)?,
            host_origin: f.opt_string("hostOrigin")?,
        })
    }
}

struct BridgeCall {
    session_id: String,
    envelope: String,
    user_gesture: bool,
}

impl BridgeCall {
    fn read(value: Value) -> Option<Self> {
        let mut f = Fields::new(value, &["sessionId", "envelope", "userGesture"])?;
        Some(Self {
            session_id: f.string("sessionId")?,
            envelope: f.string("envelope")?,
            user_gesture: f.bool_or_false("userGesture")?,
        })
    }
}

struct HostResult {
    session_id: String,
    id: String,
    result: Option<Value>,
    error: Option<BridgeErrorCode>,
}

impl HostResult {
    fn read(value: Value) -> Option<Self> {
        let mut f = Fields::new(value, &["sessionId", "id", "result", "error"])?;
        Some(Self {
            session_id: f.string("sessionId")?,
            id: f.string("id")?,
            result: f.optional("result"),
            error: f.opt_name("error", BridgeErrorCode::parse)?,
        })
    }
}

struct HostEvent {
    event: String,
    payload: Value,
    session_id: Option<String>,
}

impl HostEvent {
    fn read(value: Value) -> Option<Self> {
        let mut f = Fields::new(value, &["event", "payload", "sessionId"])?;
        Some(Self {
            event: f.string("event")?,
            payload: f.value_or("payload", || Value::Null),
            session_id: f.opt_string("sessionId")?,
        })
    }
}

/// `{sessionId}`.
fn session_ref(value: Value) -> Option<String> {
    Fields::new(value, &["sessionId"])?.string("sessionId")
}

/// `{wifi, idle}`, both `false` when absent.
fn prefetch_request(value: Value) -> Option<(bool, bool)> {
    let mut f = Fields::new(value, &["wifi", "idle"])?;
    Some((f.bool_or_false("wifi")?, f.bool_or_false("idle")?))
}

/// The answer of `plugin_bundle_bridge` and `plugin_bundle_host_result`.
fn answered(envelope: &BridgeEnvelope) -> Value {
    json!({ "envelope": serde_json::to_string(envelope).unwrap_or_default() })
}

/// Where the page's verified files are and what each one is.
struct Located {
    manifest: BundleManifest,
    root: String,
    files: Option<Value>,
    /// The policy's source (`script-src` and the rest).
    source: Option<String>,
    #[cfg(not(target_arch = "wasm32"))]
    lease: BundleLease,
}

/// The plugin, the bundle reference of its selected contribution
/// `contribution_id` and the served bundle that reference names, when the
/// catalog offers one (`not_offered` otherwise).
fn offered_entry<'a>(
    catalog: &'a UiCatalog,
    plugin_id: &str,
    contribution_id: &str,
) -> Option<(
    &'a flare_plugin_protocol::ui::ClientPlugin,
    &'a flare_plugin_protocol::ui::UiBundleRef,
    &'a WebBundleDecl,
)> {
    let plugin = catalog.plugins.iter().find(|p| p.plugin_id == plugin_id)?;
    let reference = plugin
        .extensions
        .contributions
        .iter()
        .find(|c| c.id == contribution_id)?
        .bundle
        .as_ref()?;
    let entry = plugin
        .extensions
        .web_bundle(&reference.id)
        .filter(|entry| entry.validate_catalog().is_ok())?;
    Some((plugin, reference, entry))
}

/// The slot a bundle command is logged under: one per command, so the four
/// operations a page may run at once never share one, and a retry with the
/// same `commandId` finds its record.
pub(crate) fn command_slot(plugin_id: &str, bundle_id: &str, command_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest([bundle_id, ":", command_id].concat().as_bytes());
    [plugin_id, ".bundle.", &hex::encode(digest)[..24]].concat()
}

impl CapabilityApi {
    /// Whether `plugin_bundle_open` would find a web bundle for
    /// `contribution_id` in `catalog`: the host reported a bundle runtime, the
    /// selected contribution opens a bundle the catalog serves, and the
    /// bundle's bridge is one this runtime speaks. Nothing is fetched and no
    /// session starts; the open itself can still fail (`unavailable`,
    /// `rejected`), and the host then falls back to the declarative page.
    pub(crate) fn bundle_offered(
        &self,
        catalog: &UiCatalog,
        plugin_id: &str,
        contribution_id: &str,
    ) -> bool {
        let Some(runtime) = self.plugin_client().descriptor.bundle.clone() else {
            return false;
        };
        offered_entry(catalog, plugin_id, contribution_id)
            .is_some_and(|(_, _, entry)| entry.bridge <= runtime.bridge.min(BRIDGE_VERSION))
    }

    #[inline(never)]
    pub fn plugin_bundle_open(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.plugin_bundle_open_now(request))
    }

    async fn plugin_bundle_open_now(&self, request: Value) -> Result<Value> {
        if !self.plugin_host.bundles.can_open() {
            return Err(bundle_error("unsupported"));
        }
        let request = OpenRequest::read(request).ok_or_else(invalid)?;
        if !valid_id(&request.plugin_id)
            || !valid_id(&request.contribution_id)
            || request.locale.len() > 64
            || request
                .route
                .as_deref()
                .is_some_and(|r| !valid_bundle_route(r))
            || request
                .conversation
                .as_ref()
                .is_some_and(|c| !valid_id(&c.id) || c.kind.len() > 32)
            || serde_json::to_vec(&request.arguments).map_or(true, |a| a.len() > 16 * 1024)
        {
            return Err(invalid());
        }
        let viewer = self.session_guard.capture_user().await?.ok_or_else(|| {
            FlareError::localized(ErrorCode::NotConnected, "sdk.capability.session_required")
        })?;
        let client = self.plugin_client();
        let runtime = client
            .descriptor
            .bundle
            .clone()
            .ok_or_else(|| bundle_error("unsupported"))?;
        let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
        let (plugin, reference, entry) =
            offered_entry(&catalog, &request.plugin_id, &request.contribution_id)
                .ok_or_else(|| bundle_error("not_offered"))?;
        if entry.bridge > runtime.bridge.min(BRIDGE_VERSION) {
            return Err(bundle_error("unsupported"));
        }
        let route = request.route.clone().unwrap_or(reference.route.clone());
        let located = self.locate_bundle(entry).await?;
        if located.manifest.id != entry.id || located.manifest.bridge != entry.bridge {
            return Err(bundle_error("rejected"));
        }
        let host_native: BTreeSet<String> = client
            .native
            .iter()
            .map(|capability| capability.name().to_owned())
            .collect();
        let grant = BridgeGrant::new(&located.manifest, entry, plugin, &host_native);
        // Capture card provenance from the host at open, never from a frame's bridge calls.
        let message_id = (request.presentation == BridgePresentation::Detail)
            .then(|| request.arguments.get("messageId").and_then(Value::as_str))
            .flatten()
            .filter(|id| valid_id(id))
            .map(str::to_owned);
        let context = SessionContext {
            tenant_id: self.default_tenant_id.clone(),
            viewer,
            plugin_id: plugin.plugin_id.clone(),
            plugin_version: plugin.version.clone(),
            contribution_id: request.contribution_id.clone(),
            route: route.clone(),
            bundle_id: entry.id.clone(),
            sha256: entry.sha256.clone(),
            conversation_id: request.conversation.as_ref().map(|c| c.id.clone()),
            message_id,
        };
        let sessions = &self.plugin_host.bundles.sessions;
        let hello = HelloResult {
            bridge: runtime.bridge.min(BRIDGE_VERSION),
            plugin_id: plugin.plugin_id.clone(),
            version: plugin.version.clone(),
            contribution_id: request.contribution_id.clone(),
            route,
            arguments: request.arguments,
            locale: request.locale,
            theme: request.theme,
            presentation: request.presentation,
            conversation: request.conversation,
            permissions: grant.permissions.clone(),
            capabilities: grant.capabilities.clone(),
            saved_state: sessions.take_saved_state(&context),
        };
        let key = bundle::plugin_key(&context.tenant_id, &context.plugin_id);
        let origin = bundle::bundle_origin(client.descriptor.platform, &key);
        let frame_ancestors = request
            .host_origin
            .as_deref()
            .and_then(bundle::https_origin_of)
            .unwrap_or_else(|| "'none'".to_owned());
        // An opaque web page loads its files from the sandbox domain, so the
        // policy names that domain; every other host serves its own origin.
        let source = located
            .source
            .clone()
            .or_else(|| {
                (origin == "null")
                    .then(|| entry.files.as_deref().and_then(bundle::https_origin_of))
                    .flatten()
            })
            .unwrap_or_else(|| origin.clone());
        let csp = bundle::content_security_policy(&source, &frame_ancestors);
        let session_id = uuid::Uuid::new_v4().to_string();
        let session = BridgeSession::new(session_id.clone(), context, grant, hello.clone());
        #[cfg(not(target_arch = "wasm32"))]
        let session = {
            let mut session = session;
            session.hold_files(located.lease);
            session
        };
        let evicted = sessions.insert(session);
        for session in evicted {
            self.ended(&session, None, "evicted");
        }
        self.watch_entity_state();
        let mut answer = json!({
            "sessionId": session_id,
            "origin": origin,
            "root": located.root,
            "entry": located.manifest.entry,
            "csp": csp,
            "hello": hello,
        });
        if let Some(files) = located.files {
            answer["files"] = files;
        }
        Ok(answer)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn locate_bundle(&self, entry: &WebBundleDecl) -> Result<Located> {
        let delivery = self
            .plugin_host
            .bundles
            .delivery()
            .ok_or_else(|| bundle_error("unsupported"))?;
        let delivered = delivery.open(entry).await.map_err(|error| match error {
            BundleDeliveryError::Unavailable => bundle_error("unavailable"),
            BundleDeliveryError::Rejected => bundle_error("rejected"),
        })?;
        let files: Vec<Value> = delivered.files.iter().map(|file| {
            json!({"path": file.path, "contentType": file.content_type, "size": file.size})
        }).collect();
        Ok(Located {
            manifest: delivered.manifest,
            root: delivered.root.to_string_lossy().into_owned(),
            files: Some(Value::Array(files)),
            source: None,
            lease: delivered.lease,
        })
    }

    #[cfg(target_arch = "wasm32")]
    async fn locate_bundle(&self, entry: &WebBundleDecl) -> Result<Located> {
        use flare_plugin_protocol::bundle::BUNDLE_MANIFEST_ENTRY;
        let files = entry
            .files
            .as_ref()
            .ok_or_else(|| bundle_error("unavailable"))?;
        let source = bundle::https_origin_of(files).ok_or_else(|| bundle_error("rejected"))?;
        let bytes = crate::infrastructure::transport::http::http_client_wasm::fetch_bytes(
            "GET",
            [files.as_str(), BUNDLE_MANIFEST_ENTRY].concat(),
            None,
            None,
            None,
            Default::default(),
            None,
        )
        .await
        .map_err(|_| bundle_error("unavailable"))?;
        let manifest = BundleManifest::from_json(&bytes).map_err(|_| bundle_error("rejected"))?;
        Ok(Located {
            manifest,
            source: Some(source),
            root: files.clone(),
            files: None,
        })
    }

    #[inline(never)]
    pub fn plugin_bundle_bridge(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.plugin_bundle_bridge_now(request))
    }

    async fn plugin_bundle_bridge_now(&self, request: Value) -> Result<Value> {
        let call = BridgeCall::read(request).ok_or_else(invalid)?;
        let host_bridge = self
            .plugin_client()
            .descriptor
            .bundle
            .map_or(BRIDGE_VERSION, |runtime| runtime.bridge.min(BRIDGE_VERSION));
        let now = chrono::Utc::now().timestamp_millis();
        let sessions = &self.plugin_host.bundles.sessions;
        let Some(admission) = sessions.with(&call.session_id, |session| {
            session.admit(call.envelope.as_bytes(), host_bridge, now)
        }) else {
            // The page outlived its session (closed, revoked, signed out).
            let id = serde_json::from_slice::<Value>(call.envelope.as_bytes())
                .ok()
                .and_then(|v| v.get("id").and_then(Value::as_str).map(str::to_owned));
            return Ok(answered(&bundle::broker::failure(
                id,
                BridgeErrorCode::Revoked,
            )));
        };
        let (id, request) = match admission {
            Admission::Answer(envelope) => return Ok(answered(&envelope)),
            Admission::Run { id, request } => (id, request),
        };
        let Some((context, hello)) =
            sessions.with(&call.session_id, |s| (s.context.clone(), s.hello().clone()))
        else {
            return Ok(answered(&bundle::broker::failure(
                Some(id),
                BridgeErrorCode::Revoked,
            )));
        };
        let outcome = self
            .run_bridge_request(
                &context,
                &call.session_id,
                &hello,
                request,
                call.user_gesture,
            )
            .await;
        match outcome {
            Outcome::Host { method, params } => {
                let handed = sessions
                    .with(&call.session_id, |s| s.hand_to_host(&id, method))
                    .unwrap_or(false);
                if !handed {
                    sessions.with(&call.session_id, |s| s.finish(&id));
                    return Ok(answered(&bundle::broker::failure(
                        Some(id),
                        BridgeErrorCode::RateLimited,
                    )));
                }
                Ok(json!({"host": {"id": id, "method": method.name(), "params": params}}))
            }
            Outcome::Done(result) => {
                let live = sessions
                    .with(&call.session_id, |s| {
                        s.finish(&id);
                        s.revoked().is_none()
                    })
                    .unwrap_or(false);
                if !live {
                    return Ok(answered(&bundle::broker::failure(
                        Some(id),
                        BridgeErrorCode::Revoked,
                    )));
                }
                let envelope = match result {
                    Ok(value) => BridgeEnvelope::success(id, value),
                    Err(error) => BridgeEnvelope::failure(Some(id), error),
                };
                Ok(answered(&envelope))
            }
        }
    }

    /// Runs a request that passed the broker's checks. Each kind of request
    /// runs as its own future, so this is a plain dispatch rather than one
    /// state machine holding every call a bundle can make.
    fn run_bridge_request<'a>(
        &'a self,
        context: &'a SessionContext,
        session_id: &'a str,
        hello: &'a HelloResult,
        request: BridgeRequest,
        user_gesture: bool,
    ) -> Running<'a> {
        let now = |outcome: Outcome| -> Running<'a> { Box::pin(std::future::ready(outcome)) };
        let fixed = |code: BridgeErrorCode| now(Outcome::Done(Err(BridgeError::new(code))));
        match request {
            BridgeRequest::PluginQuery(params) => Box::pin(async move {
                Outcome::Done(
                    self.bundle_query(context, &params.operation, params.input)
                        .await,
                )
            }),
            BridgeRequest::PluginCommand(params) => Box::pin(async move {
                Outcome::Done(
                    self.bundle_command(
                        context,
                        &params.operation,
                        params.input,
                        params.command_id,
                    )
                    .await,
                )
            }),
            BridgeRequest::UiNavigate(params) => match params.page.clone() {
                Some(page) => Box::pin(async move {
                    if self.offers_page(context, &page).await {
                        host(BridgeMethod::UiNavigate, &params)
                    } else {
                        Outcome::Done(Err(BridgeError::new(BridgeErrorCode::NotFound)))
                    }
                }),
                None => now(host(BridgeMethod::UiNavigate, &params)),
            },
            BridgeRequest::UiClose(params) => now(host(BridgeMethod::UiClose, &params)),
            BridgeRequest::UiSetTitle(params) => now(host(BridgeMethod::UiSetTitle, &params)),
            BridgeRequest::UiToast(params) => now(host(BridgeMethod::UiToast, &params)),
            BridgeRequest::UiConfirm(params) => now(host(BridgeMethod::UiConfirm, &params)),
            BridgeRequest::UiResize(params) => now(host(BridgeMethod::UiResize, &params)),
            BridgeRequest::UiOpenExternal(params) => {
                now(host(BridgeMethod::UiOpenExternal, &params))
            }
            BridgeRequest::ClipboardWrite(params) => {
                // The host's own record of a touch; the bundle cannot claim one.
                if !user_gesture {
                    return fixed(BridgeErrorCode::GestureRequired);
                }
                now(host(BridgeMethod::ClipboardWrite, &params))
            }
            BridgeRequest::UiSaveState(params) => {
                self.plugin_host
                    .bundles
                    .sessions
                    .save_state(context, params.state);
                now(Outcome::Done(Ok(json!({}))))
            }
            BridgeRequest::HostCapability(params) => {
                if !user_gesture {
                    return fixed(BridgeErrorCode::GestureRequired);
                }
                Box::pin(async move {
                    Outcome::Done(
                        self.bundle_capability(
                            context,
                            session_id,
                            hello,
                            params.name,
                            params.params,
                        )
                        .await,
                    )
                })
            }
            BridgeRequest::PeopleResolve(params) => Box::pin(async move {
                Outcome::Done(self.bundle_people(context, &params.user_ids).await)
            }),
            BridgeRequest::MediaRead(params) => Box::pin(async move {
                Outcome::Done(self.bundle_media_read(context, &params.file_id).await)
            }),
            request @ (BridgeRequest::StorageGet(_)
            | BridgeRequest::StorageSet(_)
            | BridgeRequest::StorageRemove(_)
            | BridgeRequest::StorageKeys(_)) => Box::pin(async move {
                let scope = crate::domain::PluginBundleStorageScope {
                    tenant_id: context.tenant_id.clone(),
                    user_id: context.viewer.clone(),
                    plugin_id: context.plugin_id.clone(),
                };
                Outcome::Done(
                    bundle::storage::run_storage(
                        self.plugin_bundle_storage.as_ref(),
                        &scope,
                        &request,
                    )
                    .await
                    .map_err(BridgeError::new),
                )
            }),
            // Answered by the broker itself.
            BridgeRequest::Hello(_)
            | BridgeRequest::EventsSubscribe(_)
            | BridgeRequest::DiagLog(_) => fixed(BridgeErrorCode::Internal),
        }
    }

    /// `plugin.query`: the same dispatch as a declarative page's query.
    async fn bundle_query(
        &self,
        context: &SessionContext,
        operation: &str,
        input: Value,
    ) -> std::result::Result<Value, BridgeError> {
        let response = self
            .dispatch_with(
                operation,
                input,
                context.conversation_id.as_deref(),
                Some(&context.tenant_id),
                Some(&context.viewer),
                context.message_id.as_deref(),
            )
            .await;
        operation_result(response, None)
    }

    /// `plugin.command`: logged in the page command journal under its own
    /// slot, so an unknown outcome is recovered with the same `commandId`.
    async fn bundle_command(
        &self,
        context: &SessionContext,
        operation: &str,
        mut input: Value,
        command_id: Option<String>,
    ) -> std::result::Result<Value, BridgeError> {
        let command_id = command_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        input["commandId"] = Value::String(command_id.clone());
        let catalog = self
            .presentation_catalog()
            .await
            .map_err(|e| bundle::broker::bridge_error_of(&e))?;
        let policy = UiPageCommand {
            slot: command_slot(&context.plugin_id, &context.bundle_id, &command_id),
            mode: UiPageCommandMode::Submit,
            terminal_error_codes: Vec::new(),
        };
        let response = self
            .page_command(crate::client::api::page_command::PageCommandCall {
                plugin: &context.plugin_id,
                catalog: &catalog,
                viewer: &context.viewer,
                operation,
                payload: input,
                conversation: context.conversation_id.as_deref(),
                message_id: context.message_id.as_deref(),
                policy: &policy,
                continuation: None,
            })
            .await
            .map(|(response, _)| response);
        // An order in the result is one the bundle may then confirm by id.
        if let Ok(response) = &response
            && response.success
        {
            self.record_payment_order(
                Some(context.viewer.as_str()),
                &context.plugin_id,
                None,
                &response.data,
            );
        }
        operation_result(response, Some(command_id))
    }

    /// `host.capability`: a payment confirmation (the only way to a transient
    /// operation) of an order a `plugin.command` of this plugin returned,
    /// named by `{orderId}`, or a location fix (`{precision}`), through the
    /// capability dispatch with its allowlists and consent. A confirmed
    /// payment answers with the order operation's `data`, a fix with
    /// `{location}`.
    async fn bundle_capability(
        &self,
        context: &SessionContext,
        session_id: &str,
        hello: &HelloResult,
        name: NativeCapability,
        params: Option<Value>,
    ) -> std::result::Result<Value, BridgeError> {
        let platform = self.plugin_client().descriptor.platform;
        let mut request = json!({
            "pluginId": context.plugin_id,
            "capability": name,
            "platform": platform,
            "userGesture": true,
        });
        match name {
            NativeCapability::PaymentConfirmation => {
                let order_id = params
                    .as_ref()
                    .and_then(|p| p.get("orderId"))
                    .filter(|id| id.is_string())
                    .cloned()
                    .ok_or_else(|| BridgeError::new(BridgeErrorCode::InvalidParams))?;
                request["orderId"] = order_id;
            }
            NativeCapability::LocationOnce => {
                if let Some(params) = params {
                    request["params"] = params;
                }
            }
            NativeCapability::Camera
            | NativeCapability::Microphone
            | NativeCapability::SelectImage
            | NativeCapability::SelectFile => {
                if hello.presentation != BridgePresentation::Page {
                    return Err(BridgeError::new(BridgeErrorCode::Unavailable));
                }
                let mut params = Fields::new(params.unwrap_or(Value::Null), &["into"])
                    .ok_or_else(|| BridgeError::new(BridgeErrorCode::InvalidParams))?;
                let into = params
                    .string("into")
                    .filter(|id| valid_id(id))
                    .ok_or_else(|| BridgeError::new(BridgeErrorCode::InvalidParams))?;
                request["page"] =
                    json!({"pluginId": context.plugin_id, "page": context.contribution_id});
                request["into"] = json!(into);
                request["locale"] = json!(hello.locale);
                request["arguments"] = json!(hello.arguments);
            }
            NativeCapability::ScanCode if params.is_none() => {}
            NativeCapability::ScanCode => {
                return Err(BridgeError::new(BridgeErrorCode::InvalidParams));
            }
            _ => return Err(BridgeError::new(BridgeErrorCode::Unavailable)),
        }
        if let Some(conversation) = &context.conversation_id {
            request["conversationId"] = Value::String(conversation.clone());
        }
        let mut outcome = self
            .bundle_capability_invoke(request, Some(session_id))
            .await
            .map_err(|e| bundle::broker::bridge_error_of(&e))?;
        match outcome.get("kind").and_then(Value::as_str) {
            Some("operation") => {
                let response = outcome["response"].take();
                if response["success"] != Value::Bool(true) {
                    let code = response["data"]["errorCode"].as_str();
                    return Err(BridgeError::operation_failed(code.filter(|code| {
                        !code.starts_with("PLUGIN_") && !code.starts_with("CAPABILITY_")
                    })));
                }
                Ok(json!({ "data": response["data"] }))
            }
            Some("location") => Ok(json!({ "location": outcome["location"] })),
            Some("input") => Ok(outcome["value"].take()),
            Some("scan") => Ok(json!({"text": outcome["text"], "format": outcome["format"]})),
            Some("denied" | "cancelled") => Err(BridgeError::new(BridgeErrorCode::Cancelled)),
            _ => Err(BridgeError::new(BridgeErrorCode::Unavailable)),
        }
    }

    /// `people.resolve`: display names and avatars of people the viewer may see.
    /// In a conversation, use its member store and resolve missing members through
    /// the authenticated roster. Outside a conversation, use local membership and
    /// profiles. Private remark names never leave.
    async fn bundle_people(
        &self,
        context: &SessionContext,
        user_ids: &[String],
    ) -> std::result::Result<Value, BridgeError> {
        let people = self
            .bundle_people
            .clone()
            .ok_or_else(|| BridgeError::new(BridgeErrorCode::Unavailable))?;
        let mut member_names = HashMap::new();
        let mut ids: Vec<String> = Vec::new();
        if let Some(conversation) = &context.conversation_id {
            let wanted: BTreeSet<&String> = user_ids.iter().collect();
            if let Some(store) = &people.participants {
                let members = store
                    .list(conversation, 0, MAX_MEMBERS_READ)
                    .await
                    .map_err(|e| bundle::broker::bridge_error_of(&e))?;
                for member in members {
                    if wanted.contains(&member.user_id) {
                        member_names.insert(member.user_id, member.nickname);
                    }
                }
            }
            let missing = wanted
                .into_iter()
                .filter(|id| !member_names.contains_key(*id))
                .collect::<BTreeSet<_>>();
            if !missing.is_empty() {
                match self
                    .bundle_roster_names(context, conversation, &missing)
                    .await
                {
                    Ok(names) => member_names.extend(names),
                    // On a transport failure, keep only the members already known locally.
                    // Authorization and response validation errors still fail closed.
                    Err(error)
                        if people.participants.is_some()
                            && matches!(
                                error.code,
                                BridgeErrorCode::Unavailable | BridgeErrorCode::Timeout
                            ) => {}
                    Err(error) => return Err(error),
                }
            }
            ids.extend(
                user_ids
                    .iter()
                    .filter(|id| member_names.contains_key(*id))
                    .cloned(),
            );
        } else {
            for id in user_ids {
                let shares = *id == context.viewer
                    || match &people.participants {
                        Some(store) => store.shares_conversation(id).await.unwrap_or(false),
                        None => false,
                    };
                if shares {
                    ids.push(id.clone());
                }
            }
        }
        let profiles = people
            .profiles
            .get_many(&ids)
            .await
            .map_err(|e| bundle::broker::bridge_error_of(&e))?;
        let mut out = Vec::new();
        for id in &ids {
            let profile = profiles.get(id);
            let name = [
                member_names.get(id).map(String::as_str),
                profile.map(|p| p.nickname.as_str()),
            ]
            .into_iter()
            .flatten()
            .find(|name| !name.trim().is_empty());
            let Some(name) = name else { continue };
            out.push(BridgePerson {
                user_id: id.clone(),
                display_name: name.to_owned(),
                avatar: profile
                    .map(|p| p.avatar_url.clone())
                    .filter(|url| !url.is_empty()),
            });
        }
        serde_json::to_value(PeopleResolveResult { people: out })
            .map_err(|_| BridgeError::new(BridgeErrorCode::Internal))
    }

    async fn bundle_roster_names(
        &self,
        context: &SessionContext,
        conversation: &str,
        wanted: &BTreeSet<&String>,
    ) -> std::result::Result<HashMap<String, String>, BridgeError> {
        let http = self
            .http
            .as_ref()
            .ok_or_else(|| BridgeError::new(BridgeErrorCode::Unavailable))?;
        self.session_guard
            .run_with_user(|identity| async move {
                if identity.as_deref() != Some(context.viewer.as_str())
                    || context.tenant_id != self.default_tenant_id
                {
                    return Err(invalid());
                }
                let mut names = HashMap::new();
                let mut cursor = String::new();
                const PAGE_SIZE: u32 = 200;
                for _ in 0..MAX_MEMBERS_READ.div_ceil(PAGE_SIZE) {
                    let query = HashMap::from([
                        ("conversation_id".to_owned(), conversation.to_owned()),
                        ("limit".to_owned(), PAGE_SIZE.to_string()),
                        ("cursor".to_owned(), cursor.clone()),
                    ]);
                    let envelope: super::HttpApiResponse<BundleRosterPage> = http
                        .get("/api/v1/conversations/participants", Some(&query))
                        .await?;
                    let page = super::unwrap_api_response(envelope, "plugin conversation roster")?;
                    if page.conversation_id != conversation
                        || page.participants.len() > PAGE_SIZE as usize
                    {
                        return Err(invalid());
                    }
                    for member in page.participants {
                        if wanted.contains(&member.user_id) {
                            let name = member
                                .attributes
                                .get("nickname")
                                .cloned()
                                .unwrap_or_default();
                            names.insert(member.user_id, name);
                        }
                    }
                    if names.len() == wanted.len() || !page.has_more {
                        break;
                    }
                    if page.next_cursor.is_empty() || page.next_cursor == cursor {
                        return Err(invalid());
                    }
                    cursor = page.next_cursor;
                }
                Ok(names)
            })
            .await
            .map_err(|error| {
                // The shared HTTP transport carries response status as metadata.
                // A generic server error may mask a denied roster, so never use
                // it to fall back to cached members.
                let status = error.as_localized().and_then(|e| {
                    e.params.and_then(|params| {
                        params
                            .get("httpStatus")
                            .and_then(|status| status.parse::<u16>().ok())
                    })
                });
                match (status, error.code()) {
                    (Some(401 | 403), _) => BridgeError::new(BridgeErrorCode::PermissionDenied),
                    (Some(502 | 503 | 504), _) | (_, Some(ErrorCode::NetworkUnreachable)) => {
                        BridgeError::new(BridgeErrorCode::Unavailable)
                    }
                    _ => bundle::broker::bridge_error_of(&error),
                }
            })
    }

    /// `media.read`: a private file this plugin uploaded for this viewer
    /// through the engine ([`super::file_upload::PluginUploads`]; any other
    /// id, a chat file's included, answers `not_found`), read through the
    /// path hosts read image blocks' files with (`resolve_media_access`),
    /// answered as a `data:`
    /// address, the one form a bundle's policy loads on every platform
    /// (`img-src` allows `data:`; the page's own origin and the app's
    /// `blob:` addresses are not the same). The bytes are read by the engine:
    /// the cached copy on native targets, the short-lived address otherwise.
    /// Files over [`MAX_BUNDLE_MEDIA_BYTES`] answer `payload_too_large`.
    async fn bundle_media_read(
        &self,
        context: &SessionContext,
        file_id: &str,
    ) -> std::result::Result<Value, BridgeError> {
        use base64::Engine as _;
        if !self.plugin_host.uploads.owns(
            &context.tenant_id,
            &context.viewer,
            &context.plugin_id,
            file_id,
        ) {
            return Err(BridgeError::new(BridgeErrorCode::NotFound));
        }
        let media = self
            .bundle_media
            .clone()
            .ok_or_else(|| BridgeError::new(BridgeErrorCode::Unavailable))?;
        let MediaBytes::Read(bytes, stored_type) = read_media_bytes(&media, file_id)
            .await
            .map_err(|e| bundle::broker::bridge_error_of(&e))?
        else {
            return Err(BridgeError::new(BridgeErrorCode::PayloadTooLarge));
        };
        if bytes.is_empty() {
            return Err(BridgeError::new(BridgeErrorCode::NotFound));
        }
        let content_type = sniff_content_type(&bytes)
            .map(str::to_owned)
            .or(stored_type.filter(|t| valid_content_type(t)))
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let url = format!(
            "data:{content_type};base64,{}",
            base64::engine::general_purpose::STANDARD.encode(&bytes)
        );
        serde_json::to_value(flare_plugin_protocol::bridge::MediaReadResult {
            url,
            content_type: Some(content_type),
            size: Some(bytes.len() as u64),
        })
        .map_err(|_| BridgeError::new(BridgeErrorCode::Internal))
    }

    async fn offers_page(&self, context: &SessionContext, page: &str) -> bool {
        let Ok(catalog) = self.presentation_catalog().await else {
            return false;
        };
        catalog.plugins.iter().any(|plugin| {
            plugin.plugin_id == context.plugin_id
                && plugin.extensions.contributions.iter().any(|c| {
                    c.id == page
                        && c.kind == flare_plugin_protocol::manifest::UiContributionKind::Page
                })
        })
    }

    #[inline(never)]
    pub fn plugin_bundle_host_result(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.plugin_bundle_host_result_now(request))
    }

    async fn plugin_bundle_host_result_now(&self, request: Value) -> Result<Value> {
        let answer = HostResult::read(request).ok_or_else(invalid)?;
        let sessions = &self.plugin_host.bundles.sessions;
        let method = sessions
            .with(&answer.session_id, |s| s.host_call(&answer.id))
            .flatten()
            .ok_or_else(|| bundle_error("unknown_request"))?;
        let envelope = match (answer.error, answer.result) {
            (Some(code), None) => {
                let allowed = matches!(
                    code,
                    BridgeErrorCode::Cancelled
                        | BridgeErrorCode::Unavailable
                        | BridgeErrorCode::GestureRequired
                        | BridgeErrorCode::Timeout
                        | BridgeErrorCode::NotFound
                );
                BridgeEnvelope::failure(
                    Some(answer.id.clone()),
                    BridgeError::new(if allowed {
                        code
                    } else {
                        BridgeErrorCode::Unavailable
                    }),
                )
            }
            (None, result) => {
                let result = match method {
                    BridgeMethod::UiConfirm => result
                        .and_then(UiConfirmResult::read)
                        .map(|r| json!({ "confirmed": r.confirmed }))
                        .ok_or_else(invalid)?,
                    _ => json!({}),
                };
                BridgeEnvelope::success(answer.id.clone(), result)
            }
            (Some(_), Some(_)) => return Err(invalid()),
        };
        sessions.with(&answer.session_id, |s| s.finish(&answer.id));
        Ok(answered(&envelope))
    }

    #[inline(never)]
    pub fn plugin_bundle_host_event(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(async move {
            let event = HostEvent::read(request).ok_or_else(invalid)?;
            let (topic, payload) =
                events::host_event(&event.event, &event.payload).ok_or_else(invalid)?;
            let envelope =
                serde_json::to_string(&events::envelope(topic, payload)).map_err(|_| invalid())?;
            let deliveries: Vec<Value> = self
                .plugin_host
                .bundles
                .sessions
                .each(|session| {
                    (event
                        .session_id
                        .as_deref()
                        .is_none_or(|id| id == session.id)
                        && session.subscribed(topic))
                    .then_some(())
                })
                .into_iter()
                .map(|(session_id, ())| json!({"sessionId": session_id, "envelope": envelope}))
                .collect();
            Ok(json!({ "deliveries": deliveries }))
        })
    }

    #[inline(never)]
    pub fn plugin_bundle_close(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(async move {
            let session_id = session_ref(request).ok_or_else(invalid)?;
            let closed = self
                .plugin_host
                .bundles
                .sessions
                .remove(&session_id)
                .is_some();
            if self.plugin_host.bundles.sessions.is_empty() {
                self.unwatch_entity_state();
            }
            Ok(json!({ "closed": closed }))
        })
    }

    #[inline(never)]
    pub fn plugin_bundle_prefetch(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(async move {
            let (wifi, idle) = prefetch_request(request).ok_or_else(invalid)?;
            if !wifi || !idle || self.plugin_client().descriptor.bundle.is_none() {
                return Ok(json!({ "fetched": 0 }));
            }
            self.prefetch_entry_bundles().await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prefetch_entry_bundles(&self) -> Result<Value> {
        let Some(delivery) = self.plugin_host.bundles.delivery() else {
            return Ok(json!({ "fetched": 0 }));
        };
        let catalog = self.presentation_catalog().await?;
        let mut fetched = 0;
        for (_, entry) in events::prefetch_candidates(&catalog) {
            if matches!(delivery.prefetch(&entry).await, Ok(true)) {
                fetched += 1;
            }
        }
        Ok(json!({ "fetched": fetched }))
    }

    #[cfg(target_arch = "wasm32")]
    async fn prefetch_entry_bundles(&self) -> Result<Value> {
        Ok(json!({ "fetched": 0 }))
    }

    /// After a successful catalog read: close the pages it no longer serves,
    /// tell the others the catalog changed, and let the cache know which
    /// bundles are still named.
    pub(crate) fn reconcile_bundle_sessions(&self, catalog: &UiCatalog) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(delivery) = self.plugin_host.bundles.delivery() {
            delivery.mark_referenced(&events::referenced_digests(catalog));
        }
        let sessions = &self.plugin_host.bundles.sessions;
        if sessions.is_empty() {
            return;
        }
        let revoked = sessions.each(|session| {
            events::revocation(&session.context, catalog).map(|reason| {
                let envelope = session.revoke(reason);
                (reason, envelope)
            })
        });
        for (session_id, (reason, envelope)) in revoked {
            if let Some(session) = sessions.remove(&session_id) {
                self.ended(&session, envelope.as_ref(), revoke_name(reason));
            }
        }
        let changed = serde_json::to_string(&events::catalog_changed()).unwrap_or_default();
        for (session_id, ()) in sessions.each(|session| {
            session
                .subscribed(BridgeEvent::CatalogChanged)
                .then_some(())
        }) {
            self.emit_bundle(&session_id, Some(changed.clone()), None);
        }
    }

    /// A catalog notice: with pages open, read the catalog now so a withdrawn
    /// bundle's pages close without waiting for the host to read it.
    pub(crate) async fn refresh_bundle_sessions(&self) {
        if self.plugin_host.bundles.sessions.is_empty() {
            return;
        }
        if let Err(error) = self.fetch_plugin_ui_catalog().await {
            tracing::debug!(target: "flare_sdk.plugin", %error, "bundle catalog refresh failed");
        }
    }

    /// Ends every page (signing out) and removes what bundles stored for
    /// `user`.
    pub(crate) async fn end_bundle_sessions(&self, user: Option<&str>) {
        for session in self.plugin_host.bundles.sessions.drain() {
            self.ended(&session, None, "signedOut");
        }
        self.unwatch_entity_state();
        if let Some(user) = user.filter(|u| !u.is_empty())
            && let Err(error) = self
                .plugin_bundle_storage
                .clear_user(&self.default_tenant_id, user)
                .await
        {
            tracing::warn!(target: "flare_sdk.plugin", %error, "bundle storage not cleared");
        }
    }

    /// Tell the host a page ended (`close`), with the bundle's last event.
    fn ended(&self, session: &BridgeSession, envelope: Option<&BridgeEnvelope>, close: &str) {
        let envelope = envelope.and_then(|e| serde_json::to_string(e).ok());
        self.emit_bundle(&session.id, envelope, Some(close.to_owned()));
    }

    fn emit_bundle(&self, session_id: &str, envelope: Option<String>, close: Option<String>) {
        if let Some(bus) = &self.bus {
            bus.publish(crate::kernel::event::SdkEvent::Plugin(
                crate::kernel::event::PluginEvent::Bundle {
                    session_id: session_id.to_owned(),
                    envelope,
                    close,
                },
            ));
        }
    }

    /// While a page is open, forward the shared state of the cards of its
    /// plugin in its conversation (`entity.state`).
    fn watch_entity_state(&self) {
        let Some(bus) = self.bus.clone() else {
            return;
        };
        let mut watch = self
            .plugin_host
            .bundles
            .entity_watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if watch.is_some() {
            return;
        }
        let api = self.clone();
        *watch = Some(bus.on_event_type(
            crate::kernel::event::SdkEventType::Message(
                crate::kernel::event::MessageEventType::PluginStateChanged,
            ),
            move |event| {
                if let crate::kernel::event::SdkEvent::Message(
                    crate::kernel::event::MessageEvent::PluginStateChanged {
                        conversation_id,
                        message_id,
                        ..
                    },
                ) = event.as_ref()
                {
                    let api = api.clone();
                    let (conversation_id, message_id) =
                        (conversation_id.clone(), message_id.clone());
                    crate::shared::util::background::spawn_background(async move {
                        api.forward_entity_state(&conversation_id, &message_id)
                            .await;
                    });
                }
            },
        ));
    }

    fn unwatch_entity_state(&self) {
        // Dropping the subscription ends it (and the API clone it held).
        let _ = self
            .plugin_host
            .bundles
            .entity_watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    async fn forward_entity_state(&self, conversation_id: &str, message_id: &str) {
        let Some(messages) = &self.messages else {
            return;
        };
        let Ok(Some(message)) = messages.get(message_id).await else {
            return;
        };
        let Some(card) = message.plugin_message() else {
            return;
        };
        let Some(state) = message.plugin_shared_state(None) else {
            return;
        };
        let deliveries = self.plugin_host.bundles.sessions.each(|session| {
            if !session.subscribed(BridgeEvent::EntityState) {
                return None;
            }
            events::entity_state(&session.context, &card.plugin_id, conversation_id, &state)
        });
        for (session_id, envelope) in deliveries {
            self.emit_bundle(&session_id, serde_json::to_string(&envelope).ok(), None);
        }
    }
}

/// The largest file `media.read` answers (the bundle file limit, D-05).
pub(crate) const MAX_BUNDLE_MEDIA_BYTES: usize =
    flare_plugin_protocol::bundle::MAX_WEB_BUNDLE_FILE_BYTES as usize;

/// How long the short-lived address `media.read` reads through lives, in seconds.
const MEDIA_READ_EXPIRES_SECS: i32 = 300;

/// What `media.read` read.
enum MediaBytes {
    /// The bytes and the type the local cache recorded, if any.
    Read(Vec<u8>, Option<String>),
    /// Over [`MAX_BUNDLE_MEDIA_BYTES`]; not read.
    TooLarge,
}

/// A private file's bytes: the cached copy when there is one, else the copy
/// cached now (native), or the bytes behind the short-lived address (the
/// browser).
#[cfg(not(target_arch = "wasm32"))]
async fn read_media_bytes(
    media: &crate::client::api::MediaApi,
    file_id: &str,
) -> Result<MediaBytes> {
    let access = media
        .resolve_media_access_opts(file_id, MEDIA_READ_EXPIRES_SECS, false)
        .await?;
    let (path, stored_type) = match access.local_path.filter(|p| !p.is_empty()) {
        Some(path) => (path, None),
        None => {
            let cached = media
                .cache_remote_media(file_id, MEDIA_READ_EXPIRES_SECS)
                .await?;
            let stored = Some(cached.mime_type).filter(|t| !t.is_empty());
            (cached.local_path, stored)
        }
    };
    let size = tokio::fs::metadata(&path)
        .await
        .map_err(|_| bundle_error("unavailable"))?
        .len();
    if size > MAX_BUNDLE_MEDIA_BYTES as u64 {
        return Ok(MediaBytes::TooLarge);
    }
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|_| bundle_error("unavailable"))?;
    Ok(MediaBytes::Read(bytes, stored_type))
}

#[cfg(target_arch = "wasm32")]
async fn read_media_bytes(
    media: &crate::client::api::MediaApi,
    file_id: &str,
) -> Result<MediaBytes> {
    let access = media
        .resolve_media_access_opts(file_id, MEDIA_READ_EXPIRES_SECS, false)
        .await?;
    let url = access
        .remote
        .map(|remote| remote.url)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| bundle_error("unavailable"))?;
    let bytes = crate::infrastructure::transport::http::http_client_wasm::fetch_bytes(
        "GET",
        url,
        None,
        None,
        None,
        Default::default(),
        None,
    )
    .await?;
    if bytes.len() > MAX_BUNDLE_MEDIA_BYTES {
        return Ok(MediaBytes::TooLarge);
    }
    Ok(MediaBytes::Read(bytes, None))
}

/// The type of an image by its first bytes: PNG, JPEG, GIF, WebP.
fn sniff_content_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

/// A recorded type usable in a `data:` address: `type/subtype` of
/// `[a-z0-9.+-]`, at most 64 bytes.
fn valid_content_type(value: &str) -> bool {
    let mut parts = value.split('/');
    let ok = |part: Option<&str>| {
        part.is_some_and(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b".+-".contains(&b))
        })
    };
    value.len() <= 64 && ok(parts.next()) && ok(parts.next()) && parts.next().is_none()
}

/// A request running ([`CapabilityApi::run_bridge_request`]).
#[cfg(not(target_arch = "wasm32"))]
type Running<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type Running<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + 'a>>;

/// What running a request produced.
enum Outcome {
    /// The answer.
    Done(std::result::Result<Value, BridgeError>),
    /// The host draws it and answers with `plugin_bundle_host_result`.
    Host { method: BridgeMethod, params: Value },
}

fn host(method: BridgeMethod, params: &impl serde::Serialize) -> Outcome {
    match serde_json::to_value(params) {
        Ok(params) => Outcome::Host { method, params },
        Err(_) => Outcome::Done(Err(BridgeError::new(BridgeErrorCode::Internal))),
    }
}

fn revoke_name(reason: RevokeReason) -> &'static str {
    match reason {
        RevokeReason::Withdrawn => "withdrawn",
        RevokeReason::Suspended => "suspended",
        RevokeReason::Disabled => "disabled",
        RevokeReason::Replaced => "replaced",
    }
}

/// A plugin operation's answer as the bridge result: `{data, commandId?}`,
/// or `operation_failed` with the plugin's own code; no other text.
fn operation_result(
    response: Result<super::CapabilityDispatchResult>,
    command_id: Option<String>,
) -> std::result::Result<Value, BridgeError> {
    let response = response.map_err(|e| bundle::broker::bridge_error_of(&e))?;
    if !response.success {
        let code = response.data.get("errorCode").and_then(Value::as_str);
        return Err(BridgeError::operation_failed(code.filter(|code| {
            !code.starts_with("PLUGIN_") && !code.starts_with("CAPABILITY_")
        })));
    }
    serde_json::to_value(PluginOperationResult {
        data: response.data,
        command_id,
    })
    .map_err(|_| BridgeError::new(BridgeErrorCode::Internal))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod roster_tests {
    use super::*;
    use crate::infrastructure::transport::http::{HttpClient, http_client::HttpRequestContext};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[cfg(feature = "storage-sqlite")]
    struct Profiles;

    #[cfg(feature = "storage-sqlite")]
    #[async_trait::async_trait]
    impl crate::domain::UserReader for Profiles {
        async fn get(&self, id: &str) -> Result<Option<crate::domain::UserProfile>> {
            let mut profile =
                crate::domain::UserProfile::new(id).with_nickname(format!("{id} profile"));
            profile.alias = "Private remark".into();
            Ok(Some(profile))
        }
    }

    #[cfg(feature = "storage-sqlite")]
    async fn roster_with_partial_cache(api: CapabilityApi) -> CapabilityApi {
        use crate::domain::ConversationParticipantStore as _;
        use crate::infrastructure::persistence::sqlite::{
            SqliteConversationParticipantRepo, init_schema,
        };
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_schema(&pool).await.unwrap();
        let members = Arc::new(SqliteConversationParticipantRepo::new(pool));
        members
            .save_page(
                "group",
                &[crate::model::ConversationParticipant {
                    user_id: "local".into(),
                    nickname: "Local member".into(),
                    ..Default::default()
                }],
                1,
                false,
            )
            .await
            .unwrap();
        api.with_bundle_people(Arc::new(Profiles), Some(members))
    }

    #[tokio::test]
    async fn roster_fallback_follows_pages_and_returns_only_requested_members() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (cursor, next, member) in [
                (
                    "",
                    "next",
                    json!({"user_id":"unrequested","attributes":{"nickname":"Ignored"}}),
                ),
                (
                    "next",
                    "",
                    json!({"user_id":"member","attributes":{"nickname":"Member","remark":"Private"}}),
                ),
            ] {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut start = String::new();
                reader.read_line(&mut start).await.unwrap();
                let target = start.split_ascii_whitespace().nth(1).unwrap();
                assert!(target.starts_with("/api/v1/conversations/participants?"));
                assert!(target.contains("conversation_id=group"));
                assert!(
                    target
                        .split(['?', '&'])
                        .any(|part| part == format!("cursor={cursor}"))
                );
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                }
                let body = json!({"code":0, "data": {
                    "conversation_id":"group", "participants":[member], "next_cursor":next, "has_more":!next.is_empty()
                }});
                let body = body.to_string();
                let mut socket = reader.into_inner();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        let api = CapabilityApi::new(
            "http://127.0.0.1:1",
            Arc::new(tokio::sync::RwLock::new("viewer".into())),
            "201",
            Arc::new(HttpRequestContext::new()),
        )
        .with_http_transport(HttpClient::new(base));
        let context = SessionContext {
            tenant_id: "201".into(),
            viewer: "viewer".into(),
            plugin_id: "example.schedule".into(),
            plugin_version: "0.1.0".into(),
            contribution_id: "example.schedule.compose".into(),
            route: "/compose".into(),
            bundle_id: "user-ui".into(),
            sha256: "digest".into(),
            conversation_id: Some("group".into()),
            message_id: None,
        };
        let member = "member".to_owned();
        let stranger = "stranger".to_owned();
        let wanted = BTreeSet::from([&member, &stranger]);
        let people = api
            .bundle_roster_names(&context, "group", &wanted)
            .await
            .unwrap();
        assert_eq!(people, HashMap::from([(member, "Member".to_owned())]));
        server.await.unwrap();
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn roster_cache_miss_filters_profiles_and_preserves_authorization_failures() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let body = json!({"code":0, "data": {
                "conversation_id":"group", "participants":[
                    {"user_id":"member","attributes":{"nickname":"Remote member","remark":"Private"}},
                    {"user_id":"profile","attributes":{}},
                    {"user_id":"unrequested","attributes":{"nickname":"Unrequested name"}}
                ], "next_cursor":"", "has_more":false
            }});
            for (status, body) in [
                ("200 OK", body.to_string()),
                ("403 Forbidden", "{}".into()),
                (
                    "500 Internal Server Error",
                    r#"{"code":500,"reason":"CONVERSATION_PARTICIPANTS_FAILED"}"#.into(),
                ),
                ("503 Service Unavailable", "{}".into()),
            ] {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut start = String::new();
                reader.read_line(&mut start).await.unwrap();
                assert!(start.starts_with("GET /api/v1/conversations/participants?"));
                assert!(start.contains("conversation_id=group"));
                let mut authenticated = false;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    if line.trim().is_empty() {
                        break;
                    }
                    authenticated |= line
                        .trim()
                        .eq_ignore_ascii_case("authorization: Bearer roster-token");
                }
                assert!(authenticated);
                let mut socket = reader.into_inner();
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        let http_context = Arc::new(HttpRequestContext::new());
        http_context
            .set_auth_context("roster-token".into(), None)
            .await;
        let api = roster_with_partial_cache(
            CapabilityApi::new(
                "http://127.0.0.1:1",
                Arc::new(tokio::sync::RwLock::new("viewer".into())),
                "201",
                Arc::clone(&http_context),
            )
            .with_http_transport(HttpClient::with_context(base, http_context)),
        )
        .await;
        let mut context = SessionContext {
            tenant_id: "201".into(),
            viewer: "viewer".into(),
            plugin_id: "example.schedule".into(),
            plugin_version: "0.1.0".into(),
            contribution_id: "example.schedule.details".into(),
            route: "/detail".into(),
            bundle_id: "user-ui".into(),
            sha256: "digest".into(),
            conversation_id: Some("group".into()),
            message_id: None,
        };
        let ids = ["local", "member", "profile", "stranger"].map(str::to_owned);
        let local = json!({"userId":"local","displayName":"Local member"});
        assert_eq!(
            api.bundle_people(&context, &ids[..1]).await.unwrap(),
            json!({"people":[local.clone()]})
        );
        context.tenant_id = "202".into();
        assert!(api.bundle_people(&context, &ids).await.is_err());
        context.tenant_id = "201".into();
        context.viewer = "different-viewer".into();
        assert!(api.bundle_people(&context, &ids).await.is_err());
        context.viewer = "viewer".into();
        assert_eq!(
            api.bundle_people(&context, &ids).await.unwrap(),
            json!({"people":[local.clone(),
                {"userId":"member","displayName":"Remote member"},
                {"userId":"profile","displayName":"profile profile"}
            ]})
        );
        assert_eq!(
            api.bundle_people(&context, &ids).await.unwrap_err().code,
            BridgeErrorCode::PermissionDenied
        );
        assert_eq!(
            api.bundle_people(&context, &ids).await.unwrap_err().code,
            BridgeErrorCode::Internal
        );
        assert_eq!(
            api.bundle_people(&context, &ids).await.unwrap(),
            json!({"people":[local]})
        );
        server.await.unwrap();
    }
}

#[cfg(test)]
mod media_read_tests {
    use super::{sniff_content_type, valid_content_type};

    #[test]
    fn image_types_are_read_from_the_first_bytes() {
        assert_eq!(
            sniff_content_type(b"\x89PNG\r\n\x1a\n\0\0"),
            Some("image/png")
        );
        assert_eq!(
            sniff_content_type(&[0xFF, 0xD8, 0xFF, 0xE0]),
            Some("image/jpeg")
        );
        assert_eq!(sniff_content_type(b"GIF89a.."), Some("image/gif"));
        assert_eq!(
            sniff_content_type(b"RIFF\0\0\0\0WEBPVP8 "),
            Some("image/webp")
        );
        assert_eq!(sniff_content_type(b"%PDF-1.7"), None);
        assert_eq!(sniff_content_type(b""), None);
    }

    #[test]
    fn only_plain_recorded_types_reach_a_data_address() {
        assert!(valid_content_type("application/pdf"));
        assert!(valid_content_type("image/svg+xml"));
        assert!(!valid_content_type("text/html;charset=utf-8"));
        assert!(!valid_content_type("image"));
        assert!(!valid_content_type("a/b/c"));
        assert!(!valid_content_type("Image/PNG"));
        assert!(!valid_content_type(&format!("a/{}", "b".repeat(80))));
    }
}
