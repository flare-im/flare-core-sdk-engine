//! 能力扩展 gRPC API（`flare.capability.v1.CapabilityService`）。
//!
//! 直连 `flare-capability` 或与接入网关转发的同一 gRPC 服务；生产环境通常经网关暴露，URI 指向网关后端。

use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use flare_grpc_proto::capability::DispatchCapabilityRequest;
#[cfg(not(target_arch = "wasm32"))]
use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Mutex;
use tokio::sync::RwLock;
#[cfg(not(target_arch = "wasm32"))]
use tonic::metadata::MetadataValue;
#[cfg(not(target_arch = "wasm32"))]
use tonic::transport::{Channel, Endpoint};

use super::session_guard::SessionGuard;
use crate::infrastructure::transport::http::http_client::HttpRequestContext;
use crate::infrastructure::transport::http::{HttpApiResponse, HttpClient, unwrap_api_response};
use crate::shared::error::{ErrorCode, FlareError, Result};

#[path = "capability_entry_availability.rs"]
mod entry_availability;
#[path = "capability_file_upload.rs"]
mod file_upload;
#[path = "capability_page.rs"]
mod page;
pub use file_upload::{PluginFileUploadPage, PluginFileUploadRequest};
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "capability_page_tests.rs"]
mod page_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "capability_platform_v2_tests.rs"]
mod platform_v2_tests;
#[path = "capability_tenant_notices.rs"]
mod tenant_notices;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "capability_ui_tests.rs"]
mod ui_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "capability_ui_v2_tests.rs"]
mod ui_v2_tests;
pub(crate) use tenant_notices::TenantNotices;
#[path = "capability_viewer.rs"]
mod viewer;

/// The last catalog read, kept only so presentations that were already
/// waiting for a read can share it.
struct CatalogRead {
    user: String,
    /// [`CatalogFlight::reads`] when it completed.
    read: u64,
    generation: u64,
    catalog: flare_plugin_protocol::ui::UiCatalog,
}
/// Catalog reads for presentation are coalesced, never cached: a
/// presentation reuses a read only when that read completed after the
/// presentation started waiting (a screen of cards shares one read), so no
/// positive grant is kept between calls. An invalidation
/// ([`CapabilityApi::invalidate_plugin_catalog`], or a plugin answering
/// disabled, not installed, no permission or unsupported platform) keeps a
/// read that started before it from being shared.
#[derive(Default)]
struct CatalogFlight {
    reads: std::sync::atomic::AtomicU64,
    generation: std::sync::atomic::AtomicU64,
    last: std::sync::Mutex<Option<CatalogRead>>,
    /// One presentation read at a time; the ones waiting reuse its result.
    lock: tokio::sync::Mutex<()>,
}
impl CatalogFlight {
    fn generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// The read that completed after `started` (a [`Self::reads`] value) for
    /// `user`, if no invalidation came since.
    fn shared_since(
        &self,
        user: &str,
        started: u64,
    ) -> Option<flare_plugin_protocol::ui::UiCatalog> {
        let last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        last.as_ref()
            .filter(|read| {
                read.user == user && read.read > started && read.generation == self.generation()
            })
            .map(|read| read.catalog.clone())
    }
    fn completed(
        &self,
        user: String,
        generation: u64,
        catalog: flare_plugin_protocol::ui::UiCatalog,
    ) {
        let read = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        *self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(CatalogRead {
            user,
            read,
            generation,
            catalog,
        });
    }
    fn invalidate(&self) {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

#[derive(Clone)]
pub struct CapabilityApi {
    pub(super) plugin_commands: Option<Arc<dyn crate::domain::PluginCommandStore>>,
    /// Local message store and bus for plugin card state (`messageId` requests).
    messages: Option<Arc<dyn crate::domain::MessageStore>>,
    bus: Option<crate::kernel::event::EventBus>,
    /// Background read of a presented card's shared layer when its message has none.
    plugin_state_refetch: Option<Arc<crate::application::services::PluginStateRefetch>>,
    /// Background reads of presented cards' viewer layers (`viewerState`).
    viewer_state: Option<Arc<viewer::ViewerStateDriver>>,
    catalog_flight: Arc<CatalogFlight>,
    /// Recent answers of composer entries' availability queries.
    entry_availability: Arc<entry_availability::EntryAvailability>,
    /// The server clock estimate: a catalog's `validUntilMs` is the server's clock.
    server_clock: crate::kernel::ServerClock,
    http: Option<HttpClient>,
    pub(super) session_guard: SessionGuard,
    #[cfg(not(target_arch = "wasm32"))]
    endpoint: String,
    #[cfg(not(target_arch = "wasm32"))]
    channel: Arc<Mutex<Option<Channel>>>,
    #[cfg(not(target_arch = "wasm32"))]
    current_user_id: Arc<RwLock<String>>,
    pub(super) default_tenant_id: String,
    #[cfg(not(target_arch = "wasm32"))]
    http_request_context: Arc<HttpRequestContext>,
}

impl CapabilityApi {
    pub fn new(
        grpc_endpoint: impl Into<String>,
        current_user_id: Arc<RwLock<String>>,
        default_tenant_id: impl Into<String>,
        http_request_context: Arc<HttpRequestContext>,
    ) -> Self {
        #[cfg(target_arch = "wasm32")]
        let _ = (grpc_endpoint.into(), http_request_context);
        Self {
            plugin_commands: None,
            messages: None,
            bus: None,
            plugin_state_refetch: None,
            viewer_state: None,
            catalog_flight: Arc::new(CatalogFlight::default()),
            entry_availability: Arc::default(),
            server_clock: crate::kernel::ServerClock::new(),
            http: None,
            session_guard: SessionGuard::new(current_user_id.clone(), "capability"),
            #[cfg(not(target_arch = "wasm32"))]
            endpoint: grpc_endpoint.into(),
            #[cfg(not(target_arch = "wasm32"))]
            channel: Arc::new(Mutex::new(None)),
            #[cfg(not(target_arch = "wasm32"))]
            current_user_id,
            default_tenant_id: crate::shared::util::normalize_tenant_id(default_tenant_id.into()),
            #[cfg(not(target_arch = "wasm32"))]
            http_request_context,
        }
    }
    /// Shares the client's server clock estimate, so catalogs the server
    /// issued expire on the server's clock, not the device's. Call it before
    /// [`Self::with_viewer_state`], whose driver keeps a clone of this API.
    pub(crate) fn with_server_clock(mut self, clock: crate::kernel::ServerClock) -> Self {
        self.server_clock = clock;
        self
    }
    pub fn with_plugin_command_store(
        mut self,
        store: Option<Arc<dyn crate::domain::PluginCommandStore>>,
    ) -> Self {
        self.plugin_commands = store;
        self
    }
    /// Enables plugin card state: present overlays the state stored on `messageId`,
    /// invoke writes a successful action's `data.messageState` back and publishes
    /// `MessageEvent::PluginStateChanged`.
    pub fn with_message_state(
        mut self,
        messages: Arc<dyn crate::domain::MessageStore>,
        bus: crate::kernel::event::EventBus,
    ) -> Self {
        self.messages = Some(messages);
        self.bus = Some(bus);
        self
    }
    /// Heals a card cached before the client knew shared state: presenting a card
    /// whose message has no shared layer reads that message back once
    /// ([`crate::application::services::PluginStateRefetch`]).
    pub(crate) fn with_plugin_state_refetch(
        mut self,
        refetch: Arc<crate::application::services::PluginStateRefetch>,
    ) -> Self {
        self.plugin_state_refetch = Some(refetch);
        self
    }
    /// Remembers presented cards (announced again when the catalog changes)
    /// and schedules their viewer layers (a message-content contribution's
    /// `viewerState`) in the background. Needs [`Self::with_message_state`]
    /// and the transport first, so call it last: the driver dispatches
    /// through a clone of this API taken here, without a driver of its own.
    pub(crate) fn with_viewer_state(mut self, current_user: Arc<RwLock<String>>) -> Self {
        let (Some(messages), Some(bus)) = (self.messages.clone(), self.bus.clone()) else {
            return self;
        };
        let driver = viewer::ViewerStateDriver::new(self.clone(), messages, bus, current_user);
        self.viewer_state = Some(driver);
        self
    }
    pub fn with_http_transport(mut self, http: HttpClient) -> Self {
        self.http = Some(http);
        self
    }
    async fn dispatch_http(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
        user_id: Option<&str>,
        message_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let expected_request_id = request_id.clone();
        let http = self.http.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                "sdk.capability.http_unavailable",
            )
        })?;
        let result: CapabilityDispatchResult = self
            .session_guard
            .run_with_user(|identity| async move {
                let user = identity.ok_or_else(|| {
                    FlareError::localized(
                        ErrorCode::NotConnected,
                        "sdk.capability.session_required",
                    )
                })?;
                if tenant_id.is_some_and(|tenant| tenant != self.default_tenant_id)
                    || user_id.is_some_and(|expected| expected != user)
                {
                    return Err(FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "sdk.capability.identity_override_denied",
                    ));
                }
                let mut request = serde_json::json!({
                    "capabilityId": capability_id, "payload": payload,
                    "conversationId": conversation_id, "requestId": request_id,
                    "expectedTenantId": self.default_tenant_id, "expectedUserId": user,
                });
                if let Some(message_id) = message_id {
                    request["messageId"] = Value::from(message_id);
                }
                let body: HttpApiResponse<CapabilityDispatchResult> = http
                    .post("/api/v1/capabilities/dispatch", &request)
                    .await
                    .map_err(crate::extension::capability::plugin_errors::localize_plugin_error)?;
                unwrap_api_response(body, "capability dispatch")
            })
            .await?;
        if result.request_id != expected_request_id || result.capability_id != capability_id {
            return Err(FlareError::system("capability response identity mismatch"));
        }
        Ok(result)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn connect(&self) -> Result<Channel> {
        let mut g = self.channel.lock().await;
        if g.is_none() {
            let ch = Endpoint::from_shared(self.endpoint.clone())
                .map_err(|e| FlareError::system(format!("capability endpoint: {e}")))?
                .connect()
                .await
                .map_err(|e| FlareError::system(format!("capability connect: {e}")))?;
            *g = Some(ch);
        }
        g.as_ref()
            .cloned()
            .ok_or_else(|| FlareError::system("capability channel unavailable"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn enrich_metadata<T>(&self, req: &mut tonic::Request<T>) -> Result<()> {
        let user_id = self.current_user_id.read().await.clone();
        if !user_id.trim().is_empty() {
            let v = MetadataValue::try_from(user_id.trim())
                .map_err(|e| FlareError::system(format!("x-user-id metadata: {e}")))?;
            req.metadata_mut().insert("x-user-id", v);
        }
        let tenant = crate::shared::util::normalize_tenant_id(self.default_tenant_id.trim());
        if !tenant.is_empty() {
            let v = MetadataValue::try_from(tenant.as_str())
                .map_err(|e| FlareError::system(format!("x-tenant-id metadata: {e}")))?;
            req.metadata_mut().insert("x-tenant-id", v);
        }
        let trace = uuid::Uuid::new_v4().to_string();
        if let Ok(v) = MetadataValue::try_from(trace.as_str()) {
            req.metadata_mut().insert("x-trace-id", v);
        }
        for (k, v) in self.http_request_context.build_headers().await {
            if k.eq_ignore_ascii_case("authorization")
                && let Ok(mv) = MetadataValue::try_from(v.as_str())
            {
                req.metadata_mut().insert("authorization", mv);
            }
        }
        Ok(())
    }

    pub async fn resolve_plugin_message(&self, request: Value) -> Result<Value> {
        self.session_guard
            .run(async {
                crate::extension::capability::ui::resolve(
                    &self.default_tenant_id,
                    request,
                    &self.server_clock,
                )
            })
            .await
    }
    pub async fn plugin_ui_action(&self, request: Value) -> Result<Value> {
        self.session_guard
            .run(async {
                crate::extension::capability::ui::action(
                    &self.default_tenant_id,
                    request,
                    &self.server_clock,
                )
            })
            .await
    }
    /// Native/Web hosts render the same Rust-evaluated presentation. No positive
    /// grant cache is kept between calls, and logout fences pending work.
    ///
    /// With `messageId`, the card is resolved over the state layers stored on
    /// that message, `payload ← shared ← local`: the shared layer is the entity
    /// state Core published (events and history), the local layer the viewer's
    /// own `data.messageState`. `hiddenWhen`, fields and blocks see the merged
    /// data. The result is the v2 view (`blocks`, `inputs`, `detail` when the
    /// document declares them) plus `data` (the merged data, only when
    /// `available`), `stateVersion` (local layer, 0 when none) and
    /// `sharedStateVersion` (shared layer, 0 when none).
    ///
    /// A card of an entity (`entityPath`) whose stored message has no shared
    /// layer, such as one cached before the client knew shared state, is read
    /// back from the server once in the background; a layer found there is
    /// stored and announced with `PluginStateChanged`, so the host presents
    /// the card again.
    ///
    /// A card whose contribution declares `viewerState` has its viewer layer
    /// read in the background while it is presented (missing, or computed from
    /// an older shared version than the one stored; again on a newer shared
    /// version when `refreshOn` has `sharedVersion`), throttled per card; the
    /// layer is stored and announced the same way.
    ///
    /// The view says why it is inert in `unavailableReason`; a catalog that
    /// cannot be read gives the inert summary with `catalog_unreachable`
    /// instead of an error, and a card Core marked as an unbound copy is
    /// read-only (`unbound_copy`). Concurrent presentations share one catalog
    /// read; none is kept after it.
    pub async fn present_plugin_message(&self, request: Value) -> Result<Value> {
        use crate::extension::capability::ui;
        let request = ui::parse_present(request)?;
        self.session_guard
            .run_with_user(|viewer| async move {
                let stored = match request.message_id.as_deref() {
                    Some(message_id) => {
                        self.stored_plugin_message(message_id, &request.message)
                            .await
                    }
                    None => None,
                };
                let viewer = viewer.unwrap_or_default();
                let catalog = match self.presentation_catalog().await {
                    Ok(catalog) => catalog,
                    Err(error) if error.code() == Some(ErrorCode::NotConnected) => {
                        return Err(error);
                    }
                    Err(error) => {
                        tracing::debug!(target: "flare_sdk.plugin", %error, "plugin catalog unavailable; card shown inert");
                        if let (Some(driver), Some(stored)) = (&self.viewer_state, &stored) {
                            driver.on_present(stored, None, &viewer, false);
                        }
                        let state = ui::CardState::of(stored.as_ref(), None);
                        return card_view(
                            flare_plugin_protocol::ui::ResolvedPluginView::unavailable(
                                &request.message,
                                flare_plugin_protocol::ui::UiUnavailableReason::CatalogUnreachable,
                            ),
                            &state,
                        );
                    }
                };
                let kind = request
                    .kind
                    .unwrap_or(flare_plugin_protocol::manifest::UiContributionKind::MessageContent);
                let contribution = ui::contribution_of(&catalog, &request.message, Some(kind));
                let entity =
                    contribution.and_then(|contribution| contribution.entity_of(&request.message.payload));
                let state = ui::CardState::of(stored.as_ref(), entity.as_deref());
                if let (Some(refetch), Some(stored), Some(entity)) =
                    (&self.plugin_state_refetch, &stored, entity.as_deref())
                {
                    refetch.on_present(stored, entity, &viewer);
                }
                let view = ui::present_card(
                    &catalog,
                    &request.message,
                    &state,
                    &ui::CardPresentation {
                        tenant: &self.default_tenant_id,
                        platform: request.platform,
                        kind,
                        locale: &request.locale,
                        viewer: &viewer,
                    },
                )?;
                if let (Some(driver), Some(stored)) = (&self.viewer_state, &stored) {
                    // Every presented card is remembered (to present again when the
                    // catalog changes); only an interactive one reads its viewer layer.
                    let interactive =
                        !state.unbound && view.get("available") == Some(&Value::Bool(true));
                    driver.on_present(stored, contribution, &viewer, interactive);
                }
                card_view_value(view, &state)
            })
            .await
    }
    /// The server id of the message a card call comes from, for Core to check
    /// that it is the message bound to the card's entity: the stored message's
    /// server id, else the id the host gave.
    fn card_message_id(
        requested: Option<&str>,
        stored: Option<&crate::model::IMMessage>,
    ) -> Option<String> {
        stored
            .map(|stored| stored.server_id.trim())
            .filter(|id| !id.is_empty())
            .or(requested)
            .map(str::to_owned)
    }
    /// The catalog to present with: a read that completed while this
    /// presentation waited for it (a screen of cards shares one read), else
    /// one read now. Nothing is kept after the reads ([`CatalogFlight`]).
    async fn presentation_catalog(&self) -> Result<flare_plugin_protocol::ui::UiCatalog> {
        let user = self.session_guard.capture_user().await?.unwrap_or_default();
        let started = self.catalog_flight.reads();
        let _flight = self.catalog_flight.lock.lock().await;
        if let Some(catalog) = self.catalog_flight.shared_since(&user, started) {
            return Ok(catalog);
        }
        Ok(self.fetch_plugin_ui_catalog().await?.1)
    }
    /// Keep any catalog read in flight from being shared with later
    /// presentations.
    pub(crate) fn invalidate_catalog_reads(&self) {
        self.catalog_flight.invalidate();
    }
    /// The plugin catalog changed (the app returned to the foreground, say):
    /// a catalog read in flight is not shared any more, and the cards
    /// presented in the last minutes are announced with `PluginStateChanged`,
    /// so hosts present them again against a fresh catalog (a disabled
    /// plugin's cards turn inert, a re-enabled one's come back). The SDK does
    /// the same by itself when a plugin call answers disabled (the plugin or
    /// one of its features), not installed, no permission or unsupported
    /// platform.
    pub async fn invalidate_plugin_catalog(&self) -> Result<()> {
        self.catalog_flight.invalidate();
        if let Some(driver) = &self.viewer_state {
            driver.announce_recent(None).await;
        }
        Ok(())
    }
    /// After a failed plugin call: an answer that means the catalog is out of
    /// date invalidates in-flight reads and re-announces that plugin's recent
    /// cards.
    async fn after_plugin_error(&self, error: &FlareError, plugin_id: &str) {
        if crate::extension::capability::plugin_errors::invalidates_catalog(error) {
            self.catalog_flight.invalidate();
            if let Some(driver) = &self.viewer_state {
                driver.announce_recent(Some(plugin_id)).await;
            }
        }
    }
    /// A card call (action, detail, viewer layer) dispatched with the message
    /// it comes from (`messageId`, the card message's server id).
    pub(crate) async fn dispatch_card(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        message_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        self.dispatch_with(
            capability_id,
            payload,
            conversation_id,
            Some(&self.default_tenant_id),
            None,
            message_id,
        )
        .await
    }
    /// The stored message for `message_id` (server id or client message id), only
    /// when it carries the same plugin card. Read failures degrade to no state.
    async fn stored_plugin_message(
        &self,
        message_id: &str,
        message: &flare_plugin_protocol::ui::PluginMessage,
    ) -> Option<crate::model::IMMessage> {
        let store = self.messages.as_ref()?;
        let stored = match store.get(message_id).await {
            Ok(Some(stored)) => Some(stored),
            Ok(None) => store.get_by_client_msg_id(message_id).await.unwrap_or_else(|error| {
                tracing::warn!(target = "flare_sdk.plugin", %error, "read plugin message failed");
                None
            }),
            Err(error) => {
                tracing::warn!(target = "flare_sdk.plugin", %error, "read plugin message failed");
                None
            }
        }?;
        stored
            .plugin_message()
            .is_some_and(|envelope| {
                envelope.plugin_id == message.plugin_id
                    && envelope.contribution_id == message.contribution_id
            })
            .then_some(stored)
    }
    /// Best effort, after a successful action from a message card:
    /// - `data.sharedState` of the card's own entity is applied at once as the
    ///   shared layer (newer versions only), so the caller does not wait for
    ///   Core's broadcast; that broadcast of the same version is then a duplicate.
    /// - `data.messageState` is validated and stored as the local layer by version.
    ///
    /// One `PluginStateChanged` announces whatever changed (its version is the
    /// local layer's when that changed, the shared layer's otherwise). Failures
    /// are logged only; they never change the action result.
    async fn write_plugin_message_state(
        &self,
        message_id: &str,
        message: &flare_plugin_protocol::ui::PluginMessage,
        entity: Option<&str>,
        response: &CapabilityDispatchResult,
    ) {
        use crate::domain::OperationApplyResult;
        if !response.success {
            return;
        }
        let local =
            response.data.get("messageState").and_then(|raw| {
                match serde_json::from_value::<flare_plugin_protocol::ui::PluginMessageState>(
                    raw.clone(),
                ) {
                    Ok(state) if state.validate().is_ok() => Some(state),
                    _ => {
                        tracing::warn!(
                            target = "flare_sdk.plugin",
                            plugin_id = %message.plugin_id,
                            "plugin returned an invalid message state"
                        );
                        None
                    }
                }
            });
        let shared = match flare_plugin_protocol::state::SharedState::from_result(&response.data) {
            Ok(Some(state)) if entity == Some(state.entity.as_str()) => Some(state),
            Ok(None) => None,
            Ok(Some(_)) | Err(_) => {
                tracing::warn!(
                    target = "flare_sdk.plugin",
                    plugin_id = %message.plugin_id,
                    "plugin returned a shared state that is invalid or not this card's entity"
                );
                None
            }
        };
        if local.is_none() && shared.is_none() {
            return;
        }
        let (Some(store), Some(stored)) = (
            self.messages.as_ref(),
            self.stored_plugin_message(message_id, message).await,
        ) else {
            tracing::debug!(
                target = "flare_sdk.plugin",
                "plugin message state skipped: no matching local message"
            );
            return;
        };
        let mut changed = None;
        // The newest shared version known for the card: the plugin's local layer is never
        // fresh beyond it (`PluginMessageState::capped_at`).
        let mut known_shared = stored.plugin_shared_state_version();
        if let Some(state) = &shared {
            match store.apply_plugin_shared_state(message_id, state).await {
                Ok(OperationApplyResult::Applied) => {
                    changed = Some(state.version);
                    known_shared = known_shared.max(state.version);
                }
                Ok(result) => tracing::debug!(
                    target = "flare_sdk.plugin",
                    ?result,
                    version = state.version,
                    "plugin shared state not applied"
                ),
                Err(error) => tracing::warn!(
                    target = "flare_sdk.plugin",
                    %error,
                    "write plugin shared state failed"
                ),
            }
        }
        if let Some(state) = local.map(|state| state.capped_at(known_shared)) {
            match store.apply_plugin_message_state(message_id, &state).await {
                Ok(OperationApplyResult::Applied) => changed = Some(state.version),
                Ok(result) => tracing::debug!(
                    target = "flare_sdk.plugin",
                    ?result,
                    version = state.version,
                    "plugin message state not applied"
                ),
                Err(error) => tracing::warn!(
                    target = "flare_sdk.plugin",
                    %error,
                    "write plugin message state failed"
                ),
            }
        }
        if let (Some(version), Some(bus)) = (changed, &self.bus) {
            let message_id = if stored.server_id.trim().is_empty() {
                stored.client_msg_id
            } else {
                stored.server_id
            };
            bus.publish(crate::kernel::event::SdkEvent::Message(
                crate::kernel::event::MessageEvent::PluginStateChanged {
                    conversation_id: stored.conversation_id,
                    message_id,
                    version,
                },
            ));
        }
    }
    /// Refresh authorization, bind declared action input, then dispatch through
    /// the normal capability policy. Native/page targets return an explicit
    /// host instruction; only the application's allowlisted bridge executes it.
    ///
    /// The input is bound over the merged card data. An action with a `form`
    /// takes `formValues`, validated first: problems come back as
    /// `{kind: "formErrors", errors: [{input, issue}]}` and nothing is sent. An
    /// action owned by a choices block takes `selection` (option indices).
    /// With `messageId`, a successful operation's `data.sharedState` (this
    /// card's entity) and `data.messageState` are stored on that message; calls
    /// not made from a message ignore them.
    ///
    /// A card call carries the card message's server id to Core
    /// (`messageId`), which accepts it only from the message bound to the
    /// card's entity. A card Core marked as an unbound copy is refused here
    /// with `sdk.plugin.card_unbound`. Failures come back as `sdk.plugin.*`
    /// keys (see [`crate::extension::capability::plugin_errors`]); one meaning
    /// the plugin is no longer available invalidates the catalog and
    /// re-announces the plugin's recent cards. A create result's `data.posted`
    /// (the card Core posted) is returned as it is.
    ///
    /// A page action names `page: {pluginId, page}` instead of `message`
    /// (see [`Self::plugin_page`]): it binds over the page view's `data`, a
    /// row action takes `row: {key, input}` as the view gave it, and it never
    /// carries `messageId`.
    pub async fn invoke_plugin_action(&self, request: Value) -> Result<Value> {
        use crate::extension::capability::ui;
        use flare_plugin_protocol::ui::UiActionTarget;
        if crate::extension::capability::ui_page::is_page_action(&request) {
            return self.invoke_page_action(request).await;
        }
        let request = ui::parse_invoke(request)?;
        self.session_guard
            .run_with_user(|user_id| async move {
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let stored = match request.message_id.as_deref() {
                    Some(message_id) => {
                        self.stored_plugin_message(message_id, &request.message)
                            .await
                    }
                    None => None,
                };
                if stored
                    .as_ref()
                    .is_some_and(crate::model::IMMessage::plugin_card_unbound)
                {
                    return Err(FlareError::localized(
                        ErrorCode::PermissionDenied,
                        "sdk.plugin.card_unbound",
                    ));
                }
                let card_message_id =
                    Self::card_message_id(request.message_id.as_deref(), stored.as_ref());
                match ui::prepare_action(
                    &self.default_tenant_id,
                    &catalog,
                    &request,
                    stored.as_ref(),
                )? {
                    ui::PreparedAction::FormErrors(errors) => {
                        Ok(serde_json::json!({"kind":"formErrors","errors":errors}))
                    }
                    ui::PreparedAction::Ready {
                        target: UiActionTarget::Operation { operation },
                        payload,
                        entity,
                    } => {
                        let response = match self
                            .dispatch_with(
                                &operation,
                                payload,
                                request.conversation_id.as_deref(),
                                Some(&self.default_tenant_id),
                                user_id.as_deref(),
                                card_message_id.as_deref(),
                            )
                            .await
                        {
                            Ok(response) => response,
                            Err(error) => {
                                self.after_plugin_error(&error, &request.message.plugin_id)
                                    .await;
                                return Err(error);
                            }
                        };
                        if let Some(message_id) = request.message_id.as_deref() {
                            self.write_plugin_message_state(
                                message_id,
                                &request.message,
                                entity.as_deref(),
                                &response,
                            )
                            .await;
                        }
                        Ok(serde_json::json!({"kind":"operation","response":response}))
                    }
                    ui::PreparedAction::Ready {
                        target, payload, ..
                    } => Ok(crate::extension::capability::ui_page::page_navigation(
                        serde_json::json!({"kind":"host","target":target,"payload":payload}),
                        stored
                            .as_ref()
                            .map(|message| message.conversation_id.as_str())
                            .filter(|id| !id.is_empty())
                            .or(request.conversation_id.as_deref()),
                    )),
                }
            })
            .await
    }
    /// "View all" of a message card: invokes the contribution's detail operation
    /// with its input bound over the merged card data (`messageId` adds the
    /// stored state layers), then resolves the detail document against the
    /// result `data`. Returns that v2 view (`available`, `title`, `fields`,
    /// `actions`, `blocks?`, `inputs?`, and `nextCursor` while a paged detail
    /// has more). A plugin failure is an error; a card without a detail, or one
    /// the catalog no longer offers, is `sdk.plugin.ui_action_unavailable`.
    ///
    /// Paging: the next page is requested with `cursor` (the view's
    /// `nextCursor`) and `previous` (that view, as returned); the operation
    /// gets the first page's input plus `cursor`, and the page comes back
    /// appended to `previous` (entries continue in order, everything else stays
    /// as on the first page, `nextCursor` is the new page's). Without
    /// `previous` only the page is returned. A `cursor` on a detail that does
    /// not page, or a malformed one, is an invalid request.
    ///
    /// Refresh: `refresh` with `previous` (the shown view) re-reads the detail
    /// from its first page and follows the cursors until the entries are at
    /// least as many as `previous` showed, there is no next page, or
    /// [`ui::MAX_DETAIL_REFRESH_PAGES`] pages were read, so a host that
    /// re-reads a sheet after a state change keeps the pages it had loaded.
    /// The merged view replaces `previous`; any page failing fails the call,
    /// and the host keeps showing `previous`.
    pub async fn plugin_detail(&self, request: Value) -> Result<Value> {
        use crate::extension::capability::ui;
        let mut request = ui::parse_detail(request)?;
        let previous = request.previous.take();
        self.session_guard
            .run_with_user(|user_id| async move {
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let stored = match request.message_id.as_deref() {
                    Some(message_id) => {
                        self.stored_plugin_message(message_id, &request.message)
                            .await
                    }
                    None => None,
                };
                if stored
                    .as_ref()
                    .is_some_and(crate::model::IMMessage::plugin_card_unbound)
                {
                    return Err(FlareError::localized(
                        ErrorCode::PermissionDenied,
                        "sdk.plugin.card_unbound",
                    ));
                }
                let detail = ui::prepare_detail(
                    &self.default_tenant_id,
                    &catalog,
                    &request,
                    stored.as_ref(),
                )?;
                // A malformed cursor is refused before anything is sent.
                let input = detail.input(request.cursor.as_deref())?;
                let user_id = user_id.as_deref();
                let card_message_id =
                    Self::card_message_id(request.message_id.as_deref(), stored.as_ref());
                let card = (user_id, card_message_id.as_deref());
                let mut view = self
                    .detail_page(&catalog, &request, &detail, input, card)
                    .await?;
                match previous {
                    Some(previous) if request.refresh => {
                        let wanted = ui::detail_entries(&previous);
                        let mut pages = 1;
                        while let Some(cursor) =
                            ui::detail_refresh_continues(&view, wanted, pages).map(str::to_owned)
                        {
                            let input = detail.input(Some(&cursor))?;
                            let page = self
                                .detail_page(&catalog, &request, &detail, input, card)
                                .await?;
                            view.append_page(page);
                            pages += 1;
                        }
                    }
                    Some(mut previous) => {
                        previous.append_page(view);
                        view = previous;
                    }
                    None => {}
                }
                ui::detail_view(&view)
            })
            .await
    }
    /// One page of a card's detail: the detail operation with `input`, its
    /// result resolved against the detail document. `card` is the signed-in
    /// user and the card message's server id.
    async fn detail_page(
        &self,
        catalog: &flare_plugin_protocol::ui::UiCatalog,
        request: &crate::extension::capability::ui::DetailRequest,
        detail: &crate::extension::capability::ui::PreparedDetail,
        input: Value,
        card: (Option<&str>, Option<&str>),
    ) -> Result<flare_plugin_protocol::ui::ResolvedPluginView> {
        use crate::extension::capability::ui;
        let (user_id, message_id) = card;
        let response = match self
            .dispatch_with(
                detail.operation(),
                input,
                request.conversation_id.as_deref(),
                Some(&self.default_tenant_id),
                user_id,
                message_id,
            )
            .await
            .and_then(CapabilityDispatchResult::fail_if_unsuccessful)
        {
            Ok(response) => response,
            Err(error) => {
                self.after_plugin_error(&error, &request.message.plugin_id)
                    .await;
                return Err(error);
            }
        };
        Ok(ui::detail_page(
            catalog,
            &request.message,
            &response.data,
            &ui::CardPresentation {
                tenant: &self.default_tenant_id,
                platform: request.platform,
                kind: flare_plugin_protocol::manifest::UiContributionKind::MessageContent,
                locale: &request.locale,
                viewer: user_id.unwrap_or_default(),
            },
        ))
    }
    /// Whether a plugin card offers "forward": only when the current catalog's
    /// message-content contribution declares `forwardable`. Plugin cards are
    /// bound to their conversation by default, so an unknown card, an
    /// unreadable catalog or a signed-out session all answer `false`.
    pub async fn plugin_card_forwardable(
        &self,
        message: &flare_plugin_protocol::ui::PluginMessage,
    ) -> bool {
        match self.presentation_catalog().await {
            Ok(catalog) => crate::extension::capability::ui::contribution_of(
                &catalog,
                message,
                Some(flare_plugin_protocol::manifest::UiContributionKind::MessageContent),
            )
            .is_some_and(|contribution| contribution.forwardable),
            Err(error) => {
                tracing::debug!(target = "flare_sdk.plugin", %error, "plugin catalog unavailable; forward hidden");
                false
            }
        }
    }
    /// Composer ("+" panel) entries of the plugins the current user may use on
    /// `platform`, titles in `locale`, sorted by order then title. Reads a fresh
    /// catalog each call; no further grant check, the server already trimmed it.
    /// An entry declaring `availability` is listed only when its plugin says
    /// it can be used now (see `capability_entry_availability.rs`).
    pub async fn composer_actions(
        &self,
        platform: flare_plugin_protocol::manifest::ClientPlatform,
        locale: &str,
    ) -> Result<Vec<crate::extension::capability::ui::PluginComposerAction>> {
        self.session_guard
            .run(async {
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let actions = crate::extension::capability::ui::composer_actions(
                    &self.default_tenant_id,
                    &catalog,
                    platform,
                    locale,
                );
                self.admitted_composer_actions(actions).await
            })
            .await
    }
    /// Binding form of [`Self::composer_actions`]: `{platform, locale}` in,
    /// `{actions: [{pluginId, contributionId, title, icon?, order, audience?}]}`
    /// out. `audience` (`{conversationTypes?, groupRoles?}`) is for the host to
    /// filter by the open conversation and the viewer's role in it.
    pub async fn plugin_composer_actions(&self, request: Value) -> Result<Value> {
        let request = crate::extension::capability::ui::parse_composer_actions(request)?;
        let actions = self
            .composer_actions(request.platform, &request.locale)
            .await?;
        Ok(serde_json::json!({ "actions": actions }))
    }
    /// "Me" (account) screen entries of the plugins the current user may use
    /// on `platform` (`account-entry` contributions), titles in `locale`,
    /// sorted by order then title. Reads a fresh catalog each call; no further
    /// grant check, the server already trimmed it. A `summary` is never called
    /// here: the host dispatches its `query` operation (input `{}`) when it
    /// shows the entry and reads the scalar at `summary.path`.
    pub async fn account_entries(
        &self,
        platform: flare_plugin_protocol::manifest::ClientPlatform,
        locale: &str,
    ) -> Result<Vec<crate::extension::capability::ui::PluginAccountEntry>> {
        self.session_guard
            .run(async {
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                Ok(crate::extension::capability::ui::account_entries(
                    &self.default_tenant_id,
                    &catalog,
                    platform,
                    locale,
                ))
            })
            .await
    }
    /// Binding form of [`Self::account_entries`]: `{platform, locale}` in,
    /// `{entries: [{pluginId, contributionId, title, icon?, order, page,
    /// summary?: {operation, path}}]}` out. `page` is the plugin's page
    /// contribution the entry opens.
    pub async fn plugin_account_entries(&self, request: Value) -> Result<Value> {
        let request = crate::extension::capability::ui::parse_composer_actions(request)?;
        let entries = self
            .account_entries(request.platform, &request.locale)
            .await?;
        Ok(serde_json::json!({ "entries": entries }))
    }
    /// Refresh on login, tenant change and plugin state change. The returned
    /// catalog expires; callers must drop it on logout and never persist secrets.
    /// Each plugin carries `configuration`: the client-visible subset of its
    /// tenant configuration (keys marked `x-flare-client-visible`), absent when
    /// empty.
    pub async fn plugin_ui_catalog(&self) -> Result<Value> {
        Ok(self.fetch_plugin_ui_catalog().await?.0)
    }
    /// A fresh catalog read; presentations waiting meanwhile share it
    /// ([`CatalogFlight`]). The catalog is read plugin by plugin
    /// ([`crate::extension::capability::ui::parse_catalog`]), so a plugin this
    /// client cannot read costs only that plugin. A catalog that cannot be read
    /// at all is `sdk.plugin.catalog_unreachable`.
    async fn fetch_plugin_ui_catalog(
        &self,
    ) -> Result<(Value, flare_plugin_protocol::ui::UiCatalog)> {
        use crate::extension::capability::plugin_errors::PLUGIN_CATALOG_UNREACHABLE_KEY;
        let unreachable = || {
            FlareError::localized(
                ErrorCode::ServiceUnavailable,
                PLUGIN_CATALOG_UNREACHABLE_KEY,
            )
        };
        let generation = self.catalog_flight.generation();
        let user = self.session_guard.capture_user().await?.unwrap_or_default();
        let result = self
            .dispatch(
                flare_plugin_protocol::ui::UI_CATALOG_OPERATION,
                Value::Null,
                None,
                None,
                None,
            )
            .await
            .map_err(|error| match error.code() {
                Some(ErrorCode::NotConnected) => error,
                _ => {
                    tracing::debug!(target: "flare_sdk.plugin", %error, "plugin catalog read failed");
                    unreachable()
                }
            })?;
        if !result.success {
            return Err(unreachable());
        }
        let mut catalog = crate::extension::capability::ui::parse_catalog(&result.data)
            .ok_or_else(unreachable)?;
        // Evaluated against the device clock from here on: move the server's deadline onto it.
        crate::extension::capability::ui::localize_validity(&mut catalog, &self.server_clock);
        if catalog.plugins.len() > 128 {
            return Err(FlareError::system("plugin UI catalog limit"));
        }
        self.catalog_flight
            .completed(user, generation, catalog.clone());
        Ok((result.data, catalog))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_capabilities(&self) -> Result<Vec<CapabilityDescriptorDto>> {
        let api = self.clone();
        self.session_guard
            .run(async move {
                let ch = api.connect().await?;
                let mut client = CapabilityServiceClient::new(ch);
                let mut req =
                    tonic::Request::new(flare_grpc_proto::capability::ListCapabilitiesRequest {});
                api.enrich_metadata(&mut req).await?;
                let resp = client.list_capabilities(req).await.map_err(|s| {
                    FlareError::system(format!("ListCapabilities: {}", s.message()))
                })?;
                let out = resp
                    .into_inner()
                    .capabilities
                    .into_iter()
                    .map(|c| CapabilityDescriptorDto {
                        capability_id: c.capability_id,
                        plugin_id: c.plugin_id,
                        version: c.version,
                        scope: c.scope,
                        visibility: c.visibility,
                        permissions: c.permissions,
                        message_types: c.message_types,
                        timeout_ms: c.timeout_ms,
                        description: c.description,
                    })
                    .collect();
                Ok(out)
            })
            .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn list_capabilities(&self) -> Result<Vec<CapabilityDescriptorDto>> {
        Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "sdk.capability.wasm_not_supported",
        ))
    }

    pub async fn dispatch(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        self.dispatch_with(
            capability_id,
            payload,
            conversation_id,
            tenant_id,
            user_id,
            None,
        )
        .await
    }

    /// [`Self::dispatch`] with the plugin card message a call comes from
    /// (`message_id`, a server message id): HTTP `messageId`, gRPC
    /// `message_id`. Core accepts a card call only from the message bound to
    /// the card's entity.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn dispatch_with(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
        user_id: Option<&str>,
        message_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        if self.http.is_some() {
            return self
                .dispatch_http(
                    capability_id,
                    payload,
                    conversation_id,
                    tenant_id,
                    user_id,
                    message_id,
                )
                .await;
        }
        let api = self.clone();
        let capability_id = capability_id.to_string();
        let conversation_id = conversation_id.map(str::to_string);
        let tenant_id = tenant_id.map(str::to_string);
        let user_id = user_id.map(str::to_string);
        let message_id = message_id.map(str::to_string);
        self.session_guard
            .run_with_user(move |session_user_id| async move {
                let user = session_user_id
                    .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未连接"))?;
                if tenant_id
                    .as_deref()
                    .is_some_and(|id| id != api.default_tenant_id)
                    || user_id.as_deref().is_some_and(|id| id != user)
                {
                    return Err(FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "sdk.capability.identity_override_denied",
                    ));
                }
                let tenant = api.default_tenant_id.clone();
                let payload_json = if payload.is_null() {
                    String::new()
                } else {
                    serde_json::to_string(&payload)
                        .map_err(|e| FlareError::system(format!("capability payload json: {e}")))?
                };
                let ch = api.connect().await?;
                let mut client = CapabilityServiceClient::new(ch);
                let mut req = tonic::Request::new(DispatchCapabilityRequest {
                    capability_id,
                    tenant_id: tenant,
                    user_id: user,
                    conversation_id: conversation_id.unwrap_or_default(),
                    payload_json,
                    request_id: uuid::Uuid::new_v4().to_string(),
                    message_id: message_id.unwrap_or_default(),
                });
                api.enrich_metadata(&mut req).await?;
                let resp = client
                    .dispatch(req)
                    .await
                    .map_err(|s| FlareError::system(format!("Dispatch: {}", s.message())))?;
                let r = resp
                    .into_inner()
                    .result
                    .ok_or_else(|| FlareError::system("Dispatch: empty result"))?;
                let data = if r.result_json.trim().is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(&r.result_json).unwrap_or(Value::Null)
                };
                let err = if r.error_message.is_empty() {
                    None
                } else {
                    Some(r.error_message)
                };
                Ok(CapabilityDispatchResult {
                    request_id: r.request_id,
                    success: r.success,
                    plugin_id: r.plugin_id,
                    capability_id: r.capability_id,
                    data,
                    error: err,
                })
            })
            .await
    }

    #[cfg(target_arch = "wasm32")]
    pub(super) async fn dispatch_with(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
        user_id: Option<&str>,
        message_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        self.dispatch_http(
            capability_id,
            payload,
            conversation_id,
            tenant_id,
            user_id,
            message_id,
        )
        .await
    }
}

/// A presented card as the bindings return it: the view plus `stateVersion`
/// (local layer, 0 when none) and `sharedStateVersion` (shared layer, 0 when
/// none).
fn card_view_value(
    mut view: Value,
    state: &crate::extension::capability::ui::CardState,
) -> Result<Value> {
    if let Some(view) = view.as_object_mut() {
        view.insert(
            "stateVersion".into(),
            Value::from(state.local.as_ref().map_or(0, |state| state.version)),
        );
        view.insert(
            "sharedStateVersion".into(),
            Value::from(state.shared.as_ref().map_or(0, |state| state.version)),
        );
    }
    Ok(view)
}
fn card_view(
    view: flare_plugin_protocol::ui::ResolvedPluginView,
    state: &crate::extension::capability::ui::CardState,
) -> Result<Value> {
    card_view_value(
        serde_json::to_value(view).map_err(|_| FlareError::system("plugin view encoding"))?,
        state,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityDescriptorDto {
    pub capability_id: String,
    pub plugin_id: String,
    pub version: String,
    pub scope: String,
    pub visibility: String,
    pub permissions: Vec<String>,
    pub message_types: Vec<String>,
    pub timeout_ms: u64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityDispatchResult {
    pub request_id: String,
    pub success: bool,
    pub plugin_id: String,
    pub capability_id: String,
    pub data: Value,
    pub error: Option<String>,
}

impl CapabilityDispatchResult {
    /// gRPC 已返回但业务层 `success == false`（SFU/插件不可用或拒绝）时转为 [`FlareError`]，便于宿主直接提示用户。
    pub fn fail_if_unsuccessful(self) -> Result<Self> {
        if self.success {
            return Ok(self);
        }
        let msg = self
            .error
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "capability service returned failure".to_string());
        Err(FlareError::localized(ErrorCode::ServiceUnavailable, msg))
    }
}
