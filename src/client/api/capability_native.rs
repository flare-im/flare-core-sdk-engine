//! Generic dispatch of plugin host capabilities (`plugin_capability_invoke`):
//! the three allowlists, the host's part, and what the engine does with its
//! result. No plugin is known here; capabilities are named for what they do.
//!
//! A capability runs only when all three allow it:
//! 1. the plugin's signed manifest declares it (`clientExtensions.nativeCapabilities`,
//!    as the tenant-approved catalog carries it);
//! 2. the host profile allows it (`plugin.set_host_profile` `nativeAllowlist`);
//! 3. the viewer consents ([`super::consent`]; none asked for `clipboardWrite`,
//!    `selectImage` and `selectFile`).
//!
//! Then the host does its part through [`PluginCapabilityHost`] and the
//! engine handles the result:
//! - `documentCapture`, and `selectImage` / `camera` / `selectFile` with an
//!   `into`: the host's file is uploaded privately through the page input's
//!   declared upload ([`CapabilityApi::plugin_file_upload`]) and the
//!   reference comes back for the `into` input: `{kind: "input", input,
//!   value: {fileId}}`.
//! - `locationOnce`: the fix, truncated to the declared precision
//!   ([`UiLocationValue::truncate`]), comes back for the `location` input of
//!   the same precision its `into` names: `{kind: "input", input, value:
//!   {latitude, longitude, accuracyM, precision}}`.
//! - `paymentConfirmation`: the order is one the engine recorded from an
//!   operation result of the same plugin and viewer (`data.confirmation`,
//!   returned with that result as `confirmation`); the host or bundle names
//!   it by `orderId` and the host draws the recorded order. Its operation is
//!   one of the plugin's catalog `transientOperations`. The credential the
//!   host collects goes only to the catalog's payment provider, which
//!   answers with a one-time ticket; the order's operation is then called
//!   with `{orderId, ticket}`: `{kind: "operation", response}`.
//! - `clipboardWrite`, `microphone`, and a pick without `into`: nothing for
//!   the engine to do; `{kind: "host", capability}` tells the host to go on.
//!
//! Every outcome is a value except a broken request or a closed order:
//! `{kind: "denied"}` (no consent), `{kind: "cancelled"}` (the viewer backed
//! out), `{kind: "unavailable"}` (the host could not, e.g. the system
//! permission was refused).
//!
//! The host's part reaches every binding the same way: by default the
//! engine publishes `plugin.capability_request` events (`{requestId, method,
//! request}`) and waits for `plugin.capability_complete` (`{requestId,
//! result}`); a Rust embedder may install its own [`PluginCapabilityHost`].
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use flare_plugin_protocol::manifest::{ClientPlatform, UiContributionKind};
use flare_plugin_protocol::ui::{
    ClientPlugin, NativeCapability, NativeParams, PaymentConfirmationOrder, UiDocumentSide,
    UiInput, UiLocationPrecision, UiLocationValue, UiPageTransition, UiRenderContext,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::oneshot;

use super::{CapabilityApi, CapabilityDispatchResult, PluginCallFuture, PluginFileUploadPage};
use crate::client::IMClient;
use crate::kernel::event::{EventBus, PluginEvent, PublishOutcome, SdkEvent};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// How long the engine waits for the host's part: it is the viewer's time
/// (framing a document, typing a payment password).
const HOST_ANSWER_TIMEOUT: Duration = Duration::from_secs(600);
/// Requests waiting for the host at once; more means the host does not
/// answer, and new ones fail as unavailable.
const MAX_PENDING_HOST_REQUESTS: usize = 8;
/// Largest payment proof the host may hand back (serialized).
const MAX_PAYMENT_PROOF_BYTES: usize = 4096;
/// Page action continuations held for payment orders at once.
const MAX_HELD_PAYMENT_CONTINUATIONS: usize = 32;
/// Payment orders recorded at once.
const MAX_RECORDED_PAYMENT_ORDERS: usize = 32;
/// How long after its order runs out a held continuation is still kept: the
/// transient operation may have been accepted just before the expiry.
const HELD_CONTINUATION_GRACE_MS: i64 = 10 * 60 * 1000;

// ---------- The host's part ----------

/// What the host did with a request.
#[derive(Debug, Clone, PartialEq)]
pub enum CapabilityHostAnswer<T> {
    Done(T),
    /// The viewer backed out.
    Cancelled,
    /// The host could not: no such hardware, the system permission refused,
    /// no answer.
    Unavailable,
}

/// The viewer's answer to the consent question (Kit
/// `FlareCapabilityConsent`), on the wire `{decision: "once" | "always" |
/// "deny"}`. Only `Always` is kept; `Once` lets this request through and the
/// next one asks again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConsentDecision {
    Once,
    Always,
    Deny,
}

/// The consent question (Kit `FlareCapabilityConsent`).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityConsentPrompt {
    pub plugin_id: String,
    pub plugin_version: String,
    pub capability: NativeCapability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<NativeParams>,
}

/// Frame, compress and strip one side of an identity document (Kit
/// `FlareDocumentCapture`). The file must be one of `accept` and at most
/// `maxBytes`, with no EXIF.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentCapturePrompt {
    pub plugin_id: String,
    pub side: UiDocumentSide,
    pub accept: Vec<String>,
    pub max_bytes: u64,
}

/// Pick a file: `selectImage` (photo library), `camera` (take a photo) or
/// `selectFile`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FilePickPrompt {
    pub plugin_id: String,
    pub capability: NativeCapability,
    pub accept: Vec<String>,
    pub max_bytes: u64,
}

/// Show the order (Kit `FlarePaymentConfirmation`, from the order's fields
/// only: the order as the engine recorded it), collect the viewer's payment
/// credential, and hand it back. The engine gives it to the payment
/// provider only, never to the plugin whose order it is.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentConfirmationPrompt {
    pub plugin_id: String,
    pub order: PaymentConfirmationOrder,
}

/// One position fix, confirmed by the viewer.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocationPrompt {
    pub plugin_id: String,
    pub precision: UiLocationPrecision,
}

/// A file the host captured or picked.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapturedFile {
    pub file_name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// A position fix, in degrees (WGS 84) and metres.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocationFix {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accuracy_m: Option<f64>,
}

/// What every host implements once, for every plugin.
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait PluginCapabilityHost: Send + Sync {
    /// Ask the viewer whether the plugin may use the capability: this once,
    /// always (kept), or not.
    async fn consent(&self, prompt: CapabilityConsentPrompt) -> ConsentDecision;
    async fn capture_document(
        &self,
        prompt: DocumentCapturePrompt,
    ) -> CapabilityHostAnswer<CapturedFile>;
    async fn pick_file(&self, prompt: FilePickPrompt) -> CapabilityHostAnswer<CapturedFile>;
    /// `Done` carries the proof (a JSON object, at most 4 KiB) the payment
    /// provider checks.
    async fn confirm_payment(
        &self,
        prompt: PaymentConfirmationPrompt,
    ) -> CapabilityHostAnswer<Map<String, Value>>;
    async fn locate_once(&self, prompt: LocationPrompt) -> CapabilityHostAnswer<LocationFix>;
}

// ---------- The default host: events and completions ----------

/// Host requests waiting for `plugin.capability_complete`, shared by every
/// session of a client.
#[derive(Default)]
pub struct CapabilityHostRequests {
    next_request_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
}

impl CapabilityHostRequests {
    /// Hand back the host's `result` for `request_id`; `false` when no such
    /// request is waiting (unknown, already answered, or given up).
    pub fn complete(&self, request_id: u64, result: Value) -> bool {
        let sender = self
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&request_id);
        sender.is_some_and(|sender| sender.send(result).is_ok())
    }

    /// Publish the request and wait for the host's answer; `None` when no
    /// one listens, the host does not answer in time, or too many wait.
    async fn ask(self: &Arc<Self>, bus: &EventBus, method: &str, request: Value) -> Option<Value> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            if pending.len() >= MAX_PENDING_HOST_REQUESTS {
                tracing::warn!(
                    target: "flare_sdk.plugin",
                    pending = pending.len(),
                    "plugin capability requests are piling up unanswered by the host"
                );
                return None;
            }
            pending.insert(request_id, tx);
        }
        let _guard = PendingRequest {
            requests: Arc::clone(self),
            request_id,
        };
        let published = bus.publish(SdkEvent::Plugin(PluginEvent::CapabilityRequest {
            request_id,
            method: method.to_owned(),
            request,
        }));
        if !matches!(published, PublishOutcome::Published { .. }) {
            return None;
        }
        match crate::shared::util::time::timeout(HOST_ANSWER_TIMEOUT, rx).await {
            Ok(Ok(value)) => Some(value),
            _ => None,
        }
    }
}

/// Drops a request nobody waits for any more (answered, timed out, or the
/// session ended), so a late completion is refused.
struct PendingRequest {
    requests: Arc<CapabilityHostRequests>,
    request_id: u64,
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.requests
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.request_id);
    }
}

/// The host as the bindings see it: each part a `plugin.capability_request`
/// event whose answer comes back through `plugin.capability_complete`.
///
/// | method | request | result |
/// |---|---|---|
/// | `consent` | [`CapabilityConsentPrompt`] | `{decision: "once" \| "always" \| "deny"}` |
/// | `captureDocument` | [`DocumentCapturePrompt`] | `{file: {fileName, contentType, bytes}}` |
/// | `pickFile` | [`FilePickPrompt`] | `{file: {fileName, contentType, bytes}}` |
/// | `confirmPayment` | [`PaymentConfirmationPrompt`] | `{confirmation: {...}}` |
/// | `locateOnce` | [`LocationPrompt`] | `{location: {latitude, longitude, accuracyM?}}` |
///
/// Any result may instead be `{cancelled: true}` (the viewer backed out) or
/// `{unavailable: true}` (the host could not).
pub(crate) struct EventCapabilityHost {
    requests: Arc<CapabilityHostRequests>,
    bus: EventBus,
}

impl EventCapabilityHost {
    async fn ask<T: DeserializeOwned>(
        &self,
        method: &str,
        request: impl Serialize,
        key: &str,
    ) -> CapabilityHostAnswer<T> {
        let Ok(request) = serde_json::to_value(request) else {
            return CapabilityHostAnswer::Unavailable;
        };
        read_answer(self.requests.ask(&self.bus, method, request).await, key)
    }
}

/// The host's result for a request whose answer sits under `key`.
pub(crate) fn read_answer<T: DeserializeOwned>(
    result: Option<Value>,
    key: &str,
) -> CapabilityHostAnswer<T> {
    let Some(result) = result else {
        return CapabilityHostAnswer::Unavailable;
    };
    if result.get("cancelled") == Some(&Value::Bool(true)) {
        return CapabilityHostAnswer::Cancelled;
    }
    match result
        .get(key)
        .cloned()
        .map(crate::shared::util::json::from_value::<T>)
    {
        Some(Ok(value)) => CapabilityHostAnswer::Done(value),
        _ => CapabilityHostAnswer::Unavailable,
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl PluginCapabilityHost for EventCapabilityHost {
    async fn consent(&self, prompt: CapabilityConsentPrompt) -> ConsentDecision {
        match self
            .ask::<ConsentDecision>("consent", prompt, "decision")
            .await
        {
            CapabilityHostAnswer::Done(decision) => decision,
            CapabilityHostAnswer::Cancelled | CapabilityHostAnswer::Unavailable => {
                ConsentDecision::Deny
            }
        }
    }
    async fn capture_document(
        &self,
        prompt: DocumentCapturePrompt,
    ) -> CapabilityHostAnswer<CapturedFile> {
        self.ask("captureDocument", prompt, "file").await
    }
    async fn pick_file(&self, prompt: FilePickPrompt) -> CapabilityHostAnswer<CapturedFile> {
        self.ask("pickFile", prompt, "file").await
    }
    async fn confirm_payment(
        &self,
        prompt: PaymentConfirmationPrompt,
    ) -> CapabilityHostAnswer<Map<String, Value>> {
        self.ask("confirmPayment", prompt, "confirmation").await
    }
    async fn locate_once(&self, prompt: LocationPrompt) -> CapabilityHostAnswer<LocationFix> {
        self.ask("locateOnce", prompt, "location").await
    }
}

/// Where the host's part lives across sessions (inside the client's
/// [`super::PluginHostSlot`]): the requests waiting for completions, and an
/// embedder's own host when one is installed.
#[derive(Default)]
pub(crate) struct CapabilityHostSlot {
    pub requests: Arc<CapabilityHostRequests>,
    custom: RwLock<Option<Arc<dyn PluginCapabilityHost>>>,
    /// Page action continuations waiting for their payment confirmation.
    pub payments: PaymentContinuations,
    /// Payment orders as operation results brought them through Core.
    pub orders: PaymentOrders,
}

/// The payment orders the engine received in operation results, the only
/// orders it confirms: a host or a bundle names one by id, never supplies
/// one. Kept per tenant, viewer, plugin and order id while open.
#[derive(Default)]
pub(crate) struct PaymentOrders {
    recorded: Mutex<HashMap<HeldPaymentKey, PaymentConfirmationOrder>>,
}

impl PaymentOrders {
    /// Record `order`; run-out orders are dropped, and past
    /// [`MAX_RECORDED_PAYMENT_ORDERS`] the one running out first goes.
    pub fn record(&self, key: HeldPaymentKey, order: PaymentConfirmationOrder, now_ms: i64) {
        let mut recorded = self.recorded.lock().unwrap_or_else(PoisonError::into_inner);
        recorded.retain(|_, order| order.is_open(now_ms));
        if recorded.len() >= MAX_RECORDED_PAYMENT_ORDERS && !recorded.contains_key(&key) {
            let first = recorded
                .iter()
                .min_by_key(|(_, order)| order.expires_at_ms)
                .map(|(key, _)| key.clone());
            if let Some(first) = first {
                recorded.remove(&first);
            }
        }
        recorded.insert(key, order);
    }

    /// The order recorded for `key`.
    pub fn get(&self, key: &HeldPaymentKey) -> Option<PaymentConfirmationOrder> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    /// Forget the order for `key` (paid).
    pub fn remove(&self, key: &HeldPaymentKey) {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
    }
}

/// Which payment order a held continuation belongs to: the tenant and viewer
/// it was asked for and the plugin and order id it names.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct HeldPaymentKey {
    tenant: String,
    viewer: String,
    plugin_id: String,
    order_id: String,
}

impl HeldPaymentKey {
    pub fn new(tenant: &str, viewer: &str, plugin_id: &str, order_id: &str) -> Self {
        Self {
            tenant: tenant.to_owned(),
            viewer: viewer.to_owned(),
            plugin_id: plugin_id.to_owned(),
            order_id: order_id.to_owned(),
        }
    }
}

struct HeldPaymentContinuation {
    origin: super::page::PageContinuationOrigin,
    continuation: UiPageTransition,
    /// The order's expiry plus [`HELD_CONTINUATION_GRACE_MS`]: a payment
    /// confirmed just before its order ran out still continues.
    keep_until_ms: i64,
}

/// A page action's continuation (the page it opens next, or the durable
/// action it runs next) held while the payment order its operation answered
/// with waits for the viewer. It runs after that order's transient
/// operation succeeded, never on a refusal, a cancel or a failure; a failed
/// confirmation keeps it for another try of the same order.
#[derive(Default)]
pub(crate) struct PaymentContinuations {
    held: Mutex<HashMap<HeldPaymentKey, HeldPaymentContinuation>>,
}

impl PaymentContinuations {
    /// Hold `continuation` for the order `key` names until a little after
    /// `expires_at_ms`. Run-out entries are dropped; past
    /// [`MAX_HELD_PAYMENT_CONTINUATIONS`] the one running out first goes.
    pub fn hold(
        &self,
        key: HeldPaymentKey,
        origin: super::page::PageContinuationOrigin,
        continuation: UiPageTransition,
        expires_at_ms: i64,
        now_ms: i64,
    ) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        held.retain(|_, entry| entry.keep_until_ms > now_ms);
        if held.len() >= MAX_HELD_PAYMENT_CONTINUATIONS && !held.contains_key(&key) {
            let first = held
                .iter()
                .min_by_key(|(_, entry)| entry.keep_until_ms)
                .map(|(key, _)| key.clone());
            if let Some(first) = first {
                held.remove(&first);
            }
        }
        held.insert(
            key,
            HeldPaymentContinuation {
                origin,
                continuation,
                keep_until_ms: expires_at_ms.saturating_add(HELD_CONTINUATION_GRACE_MS),
            },
        );
    }

    /// Take the continuation held for `key`, once.
    pub fn take(
        &self,
        key: &HeldPaymentKey,
        now_ms: i64,
    ) -> Option<(super::page::PageContinuationOrigin, UiPageTransition)> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = held.remove(key)?;
        held.retain(|_, entry| entry.keep_until_ms > now_ms);
        Some((entry.origin, entry.continuation))
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl std::fmt::Debug for CapabilityHostSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilityHostSlot")
            .field("custom", &self.custom().is_some())
            .finish_non_exhaustive()
    }
}

impl CapabilityHostSlot {
    pub fn set(&self, host: Option<Arc<dyn PluginCapabilityHost>>) {
        *self.custom.write().unwrap_or_else(PoisonError::into_inner) = host;
    }
    pub fn custom(&self) -> Option<Arc<dyn PluginCapabilityHost>> {
        self.custom
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

// ---------- Dispatch ----------

/// `plugin_capability_invoke` input. `capability`, `params` and `into` are
/// the native target a `plugin_ui_invoke` handed back (`{kind: "host",
/// target}`); `orderId` names the `confirmation` an operation result
/// carried.
/// For an `into`, the page form it belongs to: `page`, `arguments`, `data`
/// and `formValues` as the host shows them, like `plugin_file_upload`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityInvokeRequest {
    plugin_id: String,
    capability: NativeCapability,
    #[serde(default)]
    params: Option<NativeParams>,
    #[serde(default)]
    into: Option<String>,
    #[serde(default)]
    order_id: Option<String>,
    platform: ClientPlatform,
    #[serde(default)]
    locale: String,
    #[serde(default)]
    page: Option<PluginFileUploadPage>,
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    arguments: Option<Value>,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    form_values: Option<Value>,
    /// Stable id of the upload (retries reuse it); one is made when absent.
    #[serde(default)]
    command_id: Option<String>,
    #[serde(default)]
    user_gesture: bool,
}

fn invalid() -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, "sdk.plugin.invalid_ui_request")
}
fn unavailable() -> FlareError {
    FlareError::localized(
        ErrorCode::OperationNotSupported,
        "sdk.plugin.ui_action_unavailable",
    )
}
fn capability_unavailable() -> FlareError {
    FlareError::localized(
        ErrorCode::PermissionDenied,
        "sdk.plugin.native_capability_unavailable",
    )
}
fn order_expired() -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        "sdk.plugin.payment_order_expired",
    )
}
fn session_required() -> FlareError {
    FlareError::localized(ErrorCode::NotConnected, "sdk.capability.session_required")
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

/// A location fix truncated to `precision` ([`UiLocationValue::truncate`]):
/// a coarse fix to two decimals (about a kilometre) and an accuracy of at
/// least 1 000 m, a fine one to five decimals (about a metre). `None` for a
/// fix that is not one.
pub(crate) fn truncate_location(
    fix: &LocationFix,
    precision: UiLocationPrecision,
) -> Option<Value> {
    UiLocationValue::truncate(fix.latitude, fix.longitude, fix.accuracy_m, precision)
        .map(|value| json!(value))
}

/// Whether a JPEG carries nothing but image data before its scan: the JFIF
/// header (APP0 `JFIF\0`), an ICC profile (APP2 `ICC_PROFILE\0`), the Adobe
/// colour transform (APP14 `Adobe`), tables and the frame header, with 0xFF
/// fill bytes allowed before a marker. Any other segment (Exif or XMP in APP1,
/// MPF, IPTC, comments, ...), a truncated header or no scan at all is not a
/// stripped JPEG. A document photo must reach the plugin stripped; the kits
/// re-encode it and check it with the same rule (`flareJpegIsStripped`).
pub(crate) fn jpeg_is_stripped(bytes: &[u8]) -> bool {
    if bytes.len() < 4 || bytes[0] != 0xFF || bytes[1] != 0xD8 {
        return false;
    }
    let mut at = 2;
    loop {
        if bytes.get(at) != Some(&0xFF) {
            return false;
        }
        while bytes.get(at + 1) == Some(&0xFF) {
            at += 1;
        }
        let Some(&marker) = bytes.get(at + 1) else {
            return false;
        };
        if marker == 0xDA {
            return true;
        }
        let Some(head) = bytes.get(at + 2..at + 4) else {
            return false;
        };
        let length = usize::from(head[0]) << 8 | usize::from(head[1]);
        let Some(body) = bytes.get(at + 4..at + 2 + length).filter(|_| length >= 2) else {
            return false;
        };
        let allowed = match marker {
            0xE0 => body.starts_with(b"JFIF\0"),
            0xE2 => body.starts_with(b"ICC_PROFILE\0"),
            0xEE => body.starts_with(b"Adobe"),
            0xDB | 0xC4 | 0xCC | 0xDD => true,
            0xC0..=0xCF => !matches!(marker, 0xC4 | 0xC8 | 0xCC),
            _ => false,
        };
        if !allowed {
            return false;
        }
        at += 2 + length;
    }
}

impl CapabilityApi {
    /// Record the payment order in a successful result `data` of `plugin_id`
    /// for `user` ([`PaymentConfirmationOrder::from_result`]), the only way an
    /// order becomes one the engine confirms.
    pub(crate) fn record_payment_order(
        &self,
        user: Option<&str>,
        plugin_id: &str,
        data: &Value,
    ) -> Option<PaymentConfirmationOrder> {
        let order = PaymentConfirmationOrder::from_result(plugin_id, data)?;
        if let Some(user) = user.filter(|user| !user.is_empty()) {
            self.plugin_host.capability_host.orders.record(
                HeldPaymentKey::new(&self.default_tenant_id, user, plugin_id, &order.order_id),
                order.clone(),
                self.server_clock.now_ms(),
            );
        }
        Some(order)
    }

    /// The outcome of a plugin operation an action ran: `{kind: "operation",
    /// response}`, plus `confirmation` when the successful result asks the
    /// host to confirm a payment; the order is recorded for `user`
    /// ([`Self::record_payment_order`]) and the host passes its `orderId` to
    /// `plugin_capability_invoke`.
    pub(crate) fn operation_outcome(
        &self,
        user: Option<&str>,
        plugin_id: &str,
        response: &CapabilityDispatchResult,
    ) -> Value {
        let mut outcome = json!({"kind": "operation", "response": response});
        if response.success
            && let Some(order) = self.record_payment_order(user, plugin_id, &response.data)
        {
            outcome["confirmation"] = json!(order);
        }
        outcome
    }

    /// The host's part: an embedder's own host, else the event host on this
    /// session's bus. `None` without either.
    pub(crate) fn capability_host(&self) -> Option<Arc<dyn PluginCapabilityHost>> {
        let slot = &self.plugin_host.capability_host;
        slot.custom().or_else(|| {
            self.bus.clone().map(|bus| {
                Arc::new(EventCapabilityHost {
                    requests: Arc::clone(&slot.requests),
                    bus,
                }) as Arc<dyn PluginCapabilityHost>
            })
        })
    }

    /// `plugin_capability_invoke`: run a plugin host capability through the
    /// three allowlists (manifest, host profile, consent), the host's part
    /// and the engine's handling of its result (module docs). Called with a
    /// user gesture: the native target a `plugin_ui_invoke` handed back, or
    /// the `orderId` of the `confirmation` an operation result carried.
    ///
    /// Results: `{kind: "input", input, value}` (`{fileId}` for an upload,
    /// `{latitude, longitude, accuracyM, precision}` for a location),
    /// `{kind: "operation", response, confirmation?}`, `{kind: "host",
    /// capability}`, `{kind: "denied"}`, `{kind: "cancelled"}`, `{kind:
    /// "unavailable"}`. Errors: `sdk.plugin.invalid_ui_request` (a broken
    /// request or a host file that does not fit the input),
    /// `sdk.plugin.ui_action_unavailable` (no gesture, plugin or page not in
    /// the catalog, an order the engine did not record for this viewer and
    /// plugin, or whose operation is not one of the plugin's transient
    /// operations), `sdk.plugin.native_capability_unavailable` (not declared,
    /// not allowed by the host, not available yet, no payment provider),
    /// `sdk.plugin.payment_order_expired`, and the plugin's own failures.
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn plugin_capability_invoke(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.plugin_capability_invoke_now(request, false))
    }

    /// `plugin_capability_invoke` for a web bundle's `host.capability`: a
    /// location fix needs no page input and comes back as `{kind:
    /// "location", location}`, and a confirmed payment answers with the
    /// order's operation result, never a page action's continuation.
    #[inline(never)]
    pub(crate) fn bundle_capability_invoke(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.plugin_capability_invoke_now(request, true))
    }

    async fn plugin_capability_invoke_now(&self, request: Value, bundle: bool) -> Result<Value> {
        let request: CapabilityInvokeRequest =
            crate::shared::util::json::from_value(request).map_err(|_| invalid())?;
        if !valid_id(&request.plugin_id)
            || request.locale.len() > 64
            || request.into.as_deref().is_some_and(|id| !valid_id(id))
            || request.order_id.as_deref().is_some_and(|id| !valid_id(id))
            || request
                .command_id
                .as_deref()
                .is_some_and(|id| !valid_id(id))
            || request
                .conversation_id
                .as_deref()
                .is_some_and(|id| !valid_id(id))
        {
            return Err(invalid());
        }
        if !request.user_gesture {
            return Err(unavailable());
        }
        // The shape of the request: an order id for a payment confirmation
        // and nothing else; the declared target shape for everything else,
        // except that a bundle's location fix writes into no page input.
        let valid = match request.capability {
            NativeCapability::PaymentConfirmation => {
                request.params.is_none() && request.into.is_none() && request.order_id.is_some()
            }
            NativeCapability::LocationOnce if bundle => {
                request.into.is_none()
                    && matches!(request.params, Some(NativeParams::LocationOnce(_)))
            }
            capability => {
                request.order_id.is_none()
                    && capability.accepts(request.params.as_ref(), request.into.as_deref())
            }
        };
        if !valid {
            return Err(invalid());
        }
        // A host-held media session is the second phase (rtcSession).
        if request.capability == NativeCapability::RtcSession {
            return Err(capability_unavailable());
        }
        self.session_guard
            .run_with_user(|user| async move {
                let user = user.ok_or_else(session_required)?;
                let (_, catalog) = self.fetch_plugin_ui_catalog().await?;
                let plugin = catalog
                    .plugins
                    .iter()
                    .find(|p| {
                        p.plugin_id == request.plugin_id
                            && p.extensions.platforms.contains(&request.platform)
                    })
                    .ok_or_else(unavailable)?;
                // Allowlists 1 and 2: the signed manifest declares it, the
                // host profile allows it.
                if !plugin
                    .extensions
                    .native_capabilities
                    .contains(&request.capability)
                    || !self.plugin_client().native.contains(&request.capability)
                {
                    return Err(capability_unavailable());
                }
                // Everything that needs no viewer is checked before asking.
                let order = match &request.order_id {
                    Some(order_id) => {
                        // Only an order this viewer received from this
                        // plugin through Core, and only behind a transient
                        // operation of the same plugin.
                        let order = self
                            .plugin_host
                            .capability_host
                            .orders
                            .get(&HeldPaymentKey::new(
                                &self.default_tenant_id,
                                &user,
                                &plugin.plugin_id,
                                order_id,
                            ))
                            .ok_or_else(unavailable)?;
                        if !crate::shared::util::is_under_namespace(
                            &order.operation,
                            &plugin.plugin_id,
                        ) || !plugin.operations.contains(&order.operation)
                            || !plugin.transient_operations.contains(&order.operation)
                            || plugin.query_operations.contains(&order.operation)
                        {
                            return Err(unavailable());
                        }
                        if !order.is_open(self.server_clock.now_ms()) {
                            return Err(order_expired());
                        }
                        // The credential goes to the deployment's payment
                        // provider; without one nothing is confirmed.
                        let provider = catalog
                            .payment_provider
                            .as_ref()
                            .filter(|provider| {
                                catalog.plugins.iter().any(|p| {
                                    p.plugin_id == provider.plugin_id
                                        && p.transient_operations.contains(&provider.operation)
                                        && crate::shared::util::is_under_namespace(
                                            &provider.operation,
                                            &p.plugin_id,
                                        )
                                })
                            })
                            .cloned()
                            .ok_or_else(capability_unavailable)?;
                        Some((order, provider))
                    }
                    None => None,
                };
                let (upload, location_input) = match request.into.as_deref() {
                    Some(into) if request.capability == NativeCapability::LocationOnce => {
                        let (_, input) =
                            self.resolve_into_input(&catalog, plugin, &request, &user, into)?;
                        (None, Some(input.id))
                    }
                    Some(into) => (
                        Some(self.upload_target(&catalog, plugin, &request, &user, into)?),
                        None,
                    ),
                    None => (None, None),
                };
                let host = self.capability_host().ok_or_else(capability_unavailable)?;
                // Allowlist 3: the viewer.
                if !self
                    .ensure_consent(
                        host.as_ref(),
                        &user,
                        plugin,
                        request.capability,
                        request.params.as_ref(),
                    )
                    .await?
                {
                    return Ok(json!({"kind": "denied"}));
                }
                match request.capability {
                    NativeCapability::PaymentConfirmation => {
                        let (order, provider) = order.ok_or_else(invalid)?;
                        self.confirm_payment(
                            host.as_ref(),
                            &catalog,
                            plugin,
                            PaymentCall {
                                order,
                                provider: &provider,
                                request: &request,
                                user: &user,
                                bundle,
                            },
                        )
                        .await
                    }
                    NativeCapability::LocationOnce => {
                        let Some(NativeParams::LocationOnce(params)) = &request.params else {
                            return Err(invalid());
                        };
                        if !bundle && location_input.is_none() {
                            return Err(invalid());
                        }
                        let answer = host
                            .locate_once(LocationPrompt {
                                plugin_id: plugin.plugin_id.clone(),
                                precision: params.precision,
                            })
                            .await;
                        Ok(match answer {
                            CapabilityHostAnswer::Done(fix) => {
                                match (truncate_location(&fix, params.precision), location_input) {
                                    (Some(location), Some(input)) => {
                                        json!({"kind": "input", "input": input, "value": location})
                                    }
                                    (Some(location), None) => {
                                        json!({"kind": "location", "location": location})
                                    }
                                    (None, _) => json!({"kind": "unavailable"}),
                                }
                            }
                            CapabilityHostAnswer::Cancelled => json!({"kind": "cancelled"}),
                            CapabilityHostAnswer::Unavailable => json!({"kind": "unavailable"}),
                        })
                    }
                    capability => match upload {
                        Some(target) => {
                            let answer = match (capability, &request.params) {
                                (
                                    NativeCapability::DocumentCapture,
                                    Some(NativeParams::DocumentCapture(params)),
                                ) => {
                                    host.capture_document(DocumentCapturePrompt {
                                        plugin_id: plugin.plugin_id.clone(),
                                        side: params.side,
                                        accept: target.accept.clone(),
                                        max_bytes: target.max_bytes,
                                    })
                                    .await
                                }
                                (
                                    NativeCapability::SelectImage
                                    | NativeCapability::Camera
                                    | NativeCapability::SelectFile,
                                    _,
                                ) => {
                                    host.pick_file(FilePickPrompt {
                                        plugin_id: plugin.plugin_id.clone(),
                                        capability,
                                        accept: target.accept.clone(),
                                        max_bytes: target.max_bytes,
                                    })
                                    .await
                                }
                                _ => return Err(invalid()),
                            };
                            match answer {
                                CapabilityHostAnswer::Done(file) => {
                                    self.upload_into(&request, &target, capability, file).await
                                }
                                CapabilityHostAnswer::Cancelled => Ok(json!({"kind": "cancelled"})),
                                CapabilityHostAnswer::Unavailable => {
                                    Ok(json!({"kind": "unavailable"}))
                                }
                            }
                        }
                        // clipboardWrite, microphone, a pick without into:
                        // the host goes on with it.
                        None => Ok(json!({"kind": "host", "capability": capability})),
                    },
                }
            })
            .await
    }

    /// The page input an `into` names: of a page of the same plugin, of a
    /// kind the capability writes, the same side for a document and the same
    /// precision for a location.
    fn resolve_into_input(
        &self,
        catalog: &flare_plugin_protocol::ui::UiCatalog,
        plugin: &ClientPlugin,
        request: &CapabilityInvokeRequest,
        user: &str,
        into: &str,
    ) -> Result<(PluginFileUploadPage, UiInput)> {
        let page = request.page.as_ref().ok_or_else(invalid)?;
        if page.plugin_id != plugin.plugin_id {
            return Err(invalid());
        }
        let context = UiRenderContext {
            tenant: &self.default_tenant_id,
            platform: request.platform,
            kind: UiContributionKind::Page,
            locale: &request.locale,
            viewer: user,
            now_ms: crate::extension::capability::ui_page::now_ms(),
        };
        let (_, contribution) = catalog
            .page(&page.plugin_id, &page.page, &context)
            .map_err(|_| unavailable())?;
        let input = contribution
            .schema
            .inputs
            .iter()
            .find(|input| input.id == into)
            .ok_or_else(invalid)?;
        if !request.capability.into_kinds().contains(&input.kind) {
            return Err(invalid());
        }
        let fits = match &request.params {
            Some(NativeParams::DocumentCapture(params)) => input.side == Some(params.side),
            Some(NativeParams::LocationOnce(params)) => input.precision == Some(params.precision),
            _ => true,
        };
        if !fits {
            return Err(invalid());
        }
        Ok((page.clone(), input.clone()))
    }

    /// The page input an `into` names ([`Self::resolve_into_input`]), with a
    /// declared private upload.
    fn upload_target(
        &self,
        catalog: &flare_plugin_protocol::ui::UiCatalog,
        plugin: &ClientPlugin,
        request: &CapabilityInvokeRequest,
        user: &str,
        into: &str,
    ) -> Result<UploadTarget> {
        let (page, input) = self.resolve_into_input(catalog, plugin, request, user, into)?;
        let file = input
            .file
            .as_ref()
            .filter(|file| file.is_valid())
            .ok_or_else(invalid)?;
        Ok(UploadTarget {
            page,
            input: into.to_owned(),
            accept: file.accept.clone(),
            max_bytes: file.max_bytes,
        })
    }

    /// Upload the host's file through the input's declared private upload
    /// ([`Self::plugin_file_upload`], which checks the page form again) and
    /// hand back its reference for the input.
    async fn upload_into(
        &self,
        request: &CapabilityInvokeRequest,
        target: &UploadTarget,
        capability: NativeCapability,
        file: CapturedFile,
    ) -> Result<Value> {
        let fits = !file.file_name.trim().is_empty()
            && file.file_name.chars().count() <= 255
            && !file
                .file_name
                .chars()
                .any(|c| c.is_control() || matches!(c, '/' | '\\'))
            && target.accept.contains(&file.content_type)
            && !file.bytes.is_empty()
            && file.bytes.len() as u64 <= target.max_bytes;
        // A document photo reaches the plugin stripped of its metadata: the
        // kits re-encode it as a JPEG, so anything else is refused.
        let stripped = capability != NativeCapability::DocumentCapture
            || (file.content_type == "image/jpeg" && jpeg_is_stripped(&file.bytes));
        if !fits || !stripped {
            return Err(invalid());
        }
        let command_id = request
            .command_id
            .clone()
            .unwrap_or_else(|| format!("capability-{}", uuid::Uuid::new_v4()));
        let mut upload = json!({
            "page": target.page, "inputId": target.input, "platform": request.platform,
            "locale": request.locale, "fileName": file.file_name,
            "contentType": file.content_type, "bytes": file.bytes, "commandId": command_id,
        });
        for (key, value) in [
            (
                "conversationId",
                request.conversation_id.clone().map(Value::String),
            ),
            ("arguments", request.arguments.clone()),
            ("data", request.data.clone()),
            ("formValues", request.form_values.clone()),
        ] {
            if let Some(value) = value {
                upload[key] = value;
            }
        }
        let reference = self.plugin_file_upload(upload).await?;
        Ok(json!({"kind": "input", "input": target.input, "value": reference}))
    }

    /// Show the recorded order and take the host's proof; hand the proof to
    /// the payment provider, which answers with a one-time ticket bound to
    /// the order, the viewer and the plugin; and call the order's transient
    /// operation with `{orderId, ticket}` while the order is still open. When
    /// the order came from a page action with a continuation, that
    /// continuation runs once the transient operation succeeded, against its
    /// result, as it would have after the action's own operation (never for a
    /// bundle, which gets the operation's result).
    async fn confirm_payment(
        &self,
        host: &dyn PluginCapabilityHost,
        catalog: &flare_plugin_protocol::ui::UiCatalog,
        plugin: &ClientPlugin,
        call: PaymentCall<'_>,
    ) -> Result<Value> {
        let PaymentCall {
            order,
            provider,
            request,
            user,
            bundle,
        } = call;
        let answer = host
            .confirm_payment(PaymentConfirmationPrompt {
                plugin_id: plugin.plugin_id.clone(),
                order: order.clone(),
            })
            .await;
        let proof = match answer {
            CapabilityHostAnswer::Done(proof) => proof,
            CapabilityHostAnswer::Cancelled => return Ok(json!({"kind": "cancelled"})),
            CapabilityHostAnswer::Unavailable => return Ok(json!({"kind": "unavailable"})),
        };
        if serde_json::to_vec(&proof).map_or(true, |bytes| bytes.len() > MAX_PAYMENT_PROOF_BYTES) {
            return Err(invalid());
        }
        // The viewer may have taken longer than the order lives.
        if !order.is_open(self.server_clock.now_ms()) {
            return Err(order_expired());
        }
        let conversation = request.conversation_id.as_deref();
        let tenant = Some(self.default_tenant_id.as_str());
        // The credential reaches the provider only (`PaymentTicketRequest`).
        let ticket = match self
            .dispatch_with(
                &provider.operation,
                json!({
                    "pluginId": plugin.plugin_id, "orderId": order.order_id,
                    "amount": order.amount, "currency": order.currency, "payee": order.payee,
                    "expiresAtMs": order.expires_at_ms, "confirmation": proof,
                }),
                conversation,
                tenant,
                Some(user),
                None,
            )
            .await
        {
            // A wrong credential: the host shows the provider's answer.
            Ok(response) if !response.success => {
                return Ok(json!({"kind": "operation", "response": response}));
            }
            Ok(response) => response
                .data
                .get("ticket")
                .and_then(Value::as_str)
                .filter(|ticket| flare_plugin_protocol::ui::PaymentTicket::is_valid(ticket))
                .map(str::to_owned)
                .ok_or_else(unavailable)?,
            Err(error) => {
                self.after_plugin_error(&error, &provider.plugin_id).await;
                return Err(error);
            }
        };
        let response = match self
            .dispatch_with(
                &order.operation,
                json!({"orderId": order.order_id, "ticket": ticket}),
                conversation,
                tenant,
                Some(user),
                None,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.after_plugin_error(&error, &plugin.plugin_id).await;
                return Err(error);
            }
        };
        if response.success {
            let key = HeldPaymentKey::new(
                &self.default_tenant_id,
                user,
                &plugin.plugin_id,
                &order.order_id,
            );
            self.plugin_host.capability_host.orders.remove(&key);
            if !bundle
                && let Some((origin, continuation)) = self
                    .plugin_host
                    .capability_host
                    .payments
                    .take(&key, self.server_clock.now_ms())
            {
                return self
                    .continue_page_action(
                        catalog,
                        &origin,
                        Some(user),
                        response,
                        Some(continuation),
                    )
                    .await;
            }
        }
        Ok(self.operation_outcome(Some(user), &plugin.plugin_id, &response))
    }
}

/// A payment confirmation's recorded order and where its credential goes.
struct PaymentCall<'a> {
    order: PaymentConfirmationOrder,
    provider: &'a flare_plugin_protocol::ui::UiPaymentProvider,
    request: &'a CapabilityInvokeRequest,
    user: &'a str,
    bundle: bool,
}

/// Where a captured file goes: a page input and its upload declaration.
struct UploadTarget {
    page: PluginFileUploadPage,
    input: String,
    accept: Vec<String>,
    max_bytes: u64,
}

/// `plugin.capability_complete` input.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CapabilityCompletion {
    request_id: u64,
    #[serde(default)]
    result: Value,
}

impl IMClient {
    /// `plugin.capability_complete`: `{requestId, result}`, the host's answer
    /// to a `plugin.capability_request` event (the result shapes are on
    /// [`EventCapabilityHost`]). Refused with
    /// `sdk.plugin.capability_request_unknown` when that request is no
    /// longer waiting (answered, timed out, or its session ended).
    pub async fn complete_plugin_capability_request(&self, request: Value) -> Result<()> {
        let completion: CapabilityCompletion =
            crate::shared::util::json::from_value(request).map_err(|_| invalid())?;
        let slot = self.inner.read().await.plugin_host.clone();
        if slot
            .capability_host
            .requests
            .complete(completion.request_id, completion.result)
        {
            Ok(())
        } else {
            Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.plugin.capability_request_unknown",
            ))
        }
    }

    /// Install a Rust host for plugin capabilities instead of the
    /// `plugin.capability_request` events (`None` goes back to the events).
    /// It lasts across sessions, like the host profile.
    pub async fn set_plugin_capability_host(&self, host: Option<Arc<dyn PluginCapabilityHost>>) {
        let slot = self.inner.read().await.plugin_host.clone();
        slot.capability_host.set(host);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn a_coarse_fix_is_about_a_kilometre_and_a_fine_one_about_a_metre() {
        let fix = LocationFix {
            latitude: 31.230_416_7,
            longitude: 121.473_701_9,
            accuracy_m: Some(12.4),
        };
        assert_eq!(
            truncate_location(&fix, UiLocationPrecision::Coarse).unwrap(),
            json!({"latitude": 31.23, "longitude": 121.47, "accuracyM": 1000.0, "precision": "coarse"})
        );
        assert_eq!(
            truncate_location(&fix, UiLocationPrecision::Fine).unwrap(),
            json!({"latitude": 31.23042, "longitude": 121.4737, "accuracyM": 12.0, "precision": "fine"})
        );
        for bad in [
            LocationFix {
                latitude: 91.0,
                longitude: 0.0,
                accuracy_m: None,
            },
            LocationFix {
                latitude: f64::NAN,
                longitude: 0.0,
                accuracy_m: None,
            },
            LocationFix {
                latitude: 0.0,
                longitude: 0.0,
                accuracy_m: Some(-1.0),
            },
        ] {
            assert!(truncate_location(&bad, UiLocationPrecision::Fine).is_none());
        }
    }

    /// The kits' shared vectors (flare-im-design spec/capability-ui-vectors.json,
    /// `jpegStripped`), so the kits' check and the engine's stay one rule.
    #[test]
    fn only_a_jpeg_with_image_data_alone_is_stripped() {
        fn hex(text: &str) -> Vec<u8> {
            (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
                .collect()
        }
        let jfif = "FFE000104A46494600010100000100010000";
        let tables = "FFDB0004AAAAFFC0000500AAAAFFC4000400AA";
        let scan = "FFDA0002FFD9";
        let cases = [
            (format!("FFD8{jfif}{tables}{scan}"), true),
            // Fill bytes before a marker.
            (format!("FFD8FFFF{jfif}FFFF{tables}FFFF{scan}"), true),
            // An ICC profile and the Adobe segment are colour, not metadata.
            (
                format!("FFD8{jfif}FFE2000E4943435F50524F46494C4500{tables}{scan}"),
                true,
            ),
            (format!("FFD8FFEE000741646F6265{tables}{scan}"), true),
            // Exif, XMP, MPF, IPTC and comments are refused, before or after the tables.
            (
                format!("FFD8{jfif}FFE10008457869660000{tables}{scan}"),
                false,
            ),
            (
                format!("FFD8{jfif}{tables}FFE1000A687474703A2F2F6E73{scan}"),
                false,
            ),
            (
                format!("FFD8{jfif}FFE200084D5046000000{tables}{scan}"),
                false,
            ),
            (format!("FFD8{jfif}FFED00045043{tables}{scan}"), false),
            (format!("FFD8{jfif}FFFE0004AAAA{tables}{scan}"), false),
            // An APP0 that is not JFIF, a truncated header, no scan, not a JPEG.
            (format!("FFD8FFE000064A465858{tables}{scan}"), false),
            (format!("FFD8{jfif}FFDB0010AAAA"), false),
            (format!("FFD8{jfif}{tables}"), false),
            ("89504E470D0A1A0A".to_owned(), false),
            (String::new(), false),
        ];
        for (bytes, stripped) in cases {
            assert_eq!(jpeg_is_stripped(&hex(&bytes)), stripped, "{bytes}");
        }
    }

    #[test]
    fn host_results_read_as_done_cancelled_or_unavailable() {
        assert_eq!(
            read_answer::<ConsentDecision>(Some(json!({"decision": "once"})), "decision"),
            CapabilityHostAnswer::Done(ConsentDecision::Once)
        );
        assert_eq!(
            read_answer::<ConsentDecision>(Some(json!({"decision": "always"})), "decision"),
            CapabilityHostAnswer::Done(ConsentDecision::Always)
        );
        assert_eq!(
            read_answer::<ConsentDecision>(Some(json!({"decision": "deny"})), "decision"),
            CapabilityHostAnswer::Done(ConsentDecision::Deny)
        );
        assert_eq!(
            read_answer::<ConsentDecision>(Some(json!({"granted": true})), "decision"),
            CapabilityHostAnswer::Unavailable,
            "only a decision answers the consent question"
        );
        assert_eq!(
            read_answer::<ConsentDecision>(Some(json!({"cancelled": true})), "decision"),
            CapabilityHostAnswer::Cancelled
        );
        assert_eq!(
            read_answer::<ConsentDecision>(Some(json!({"unavailable": true})), "decision"),
            CapabilityHostAnswer::Unavailable
        );
        assert_eq!(
            read_answer::<ConsentDecision>(None, "decision"),
            CapabilityHostAnswer::Unavailable
        );
        assert_eq!(
            read_answer::<LocationFix>(
                Some(json!({"location": {"latitude": 1.0, "longitude": 2.0, "altitude": 3}})),
                "location"
            ),
            CapabilityHostAnswer::Unavailable,
            "unknown fields are refused"
        );
    }

    #[tokio::test]
    async fn a_host_answer_reaches_the_waiting_request_once() {
        let requests = Arc::new(CapabilityHostRequests::default());
        let bus = EventBus::new();
        let mut events = bus.subscribe();
        let waiting = {
            let requests = Arc::clone(&requests);
            let bus = bus.clone();
            tokio::spawn(async move {
                requests
                    .ask(&bus, "locateOnce", json!({"pluginId": "example.checkin"}))
                    .await
            })
        };
        let request_id = loop {
            if let Ok(SdkEvent::Plugin(PluginEvent::CapabilityRequest {
                request_id,
                method,
                request,
            })) = events.recv().await
            {
                assert_eq!(method, "locateOnce");
                assert_eq!(request["pluginId"], "example.checkin");
                break request_id;
            }
        };
        let answer = json!({"location": {"latitude": 1.0, "longitude": 2.0}});
        assert!(requests.complete(request_id, answer.clone()));
        assert_eq!(waiting.await.unwrap(), Some(answer));
        assert!(!requests.complete(request_id, json!({})), "answered once");
    }

    #[tokio::test]
    async fn without_a_listener_the_request_is_unavailable_at_once() {
        let requests = Arc::new(CapabilityHostRequests::default());
        assert_eq!(
            requests.ask(&EventBus::new(), "consent", json!({})).await,
            None
        );
        assert!(requests.pending.lock().unwrap().is_empty());
    }
}
