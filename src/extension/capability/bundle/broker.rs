//! The bridge broker: one [`BridgeSession`] per opened bundle, and the checks
//! every request goes through before anything runs (design 8.6), in this
//! order:
//!
//! 1. the envelope: at most 64 KiB, JSON, strict fields, a request with a
//!    valid id ([`BridgeEnvelope::parse`]);
//! 2. the bridge version: `1..=` the host's;
//! 3. `host.hello` first: nothing else is served before it (`not_ready`);
//! 4. the method and its parameters (`method_not_found`, `invalid_params`,
//!    `payload_too_large`; operation input within 16 KiB);
//! 5. the bridge permission: in `bundle.json` and approved by the tenant;
//! 6. the operation: in `bundle.json` and authorized by the catalog, a query
//!    for `plugin.query`, never a transient one ([`BridgeGrant::authorize`]);
//! 7. the rate: 20 requests a second, 4 operations at once (`rate_limited`);
//! 8. the call.
//!
//! Every failure is a fixed [`BridgeErrorCode`]; no text of Core, the plugin
//! or the engine reaches the bundle ([`bridge_error_of`]).
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Mutex, PoisonError};

use flare_plugin_protocol::bridge::{
    BRIDGE_CONCURRENT_OPERATIONS, BRIDGE_REQUESTS_PER_SECOND, BridgeEnvelope, BridgeError,
    BridgeErrorCode, BridgeEvent, BridgeGrant, BridgeMethod, BridgeRequest, HelloResult,
    LifecycleRevokedPayload, RevokeReason,
};
use serde_json::{Value, json};

use crate::shared::error::{ErrorCode, FlareError};

/// Live bundle pages: one in front and one suspended (design 8.8); opening a
/// third ends the oldest.
pub const MAX_LIVE_SESSIONS: usize = 2;
/// Pages whose `ui.saveState` is kept for their next opening.
const MAX_SAVED_STATES: usize = 16;
/// Requests handed to the host (a confirmation, a navigation) at once.
const MAX_HOST_CALLS: usize = 8;

/// What a session was opened for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionContext {
    pub tenant_id: String,
    pub viewer: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub contribution_id: String,
    pub route: String,
    pub bundle_id: String,
    pub sha256: String,
    pub conversation_id: Option<String>,
    pub message_id: Option<String>,
}

/// What [`BridgeSession::admit`] decided about one message.
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    /// Answer with this envelope now.
    Answer(BridgeEnvelope),
    /// Passed every check: run it, answer, then [`BridgeSession::finish`].
    Run { id: String, request: BridgeRequest },
}

/// One opened bundle page.
#[derive(Debug)]
pub struct BridgeSession {
    pub id: String,
    pub context: SessionContext,
    pub grant: BridgeGrant,
    hello: HelloResult,
    greeted: bool,
    topics: BTreeSet<BridgeEvent>,
    /// Admission times within the last second.
    window: VecDeque<i64>,
    /// Requests running: the id and whether it counts as an operation. A
    /// handful at most (the rate limit), so a list, not a map.
    running: Vec<(String, bool)>,
    /// Requests the host is drawing (a confirmation, a toast): the id and the
    /// method; at most [`MAX_HOST_CALLS`].
    host_calls: Vec<(String, BridgeMethod)>,
    revoked: Option<RevokeReason>,
    #[cfg(not(target_arch = "wasm32"))]
    file_lease: Option<super::provider::BundleLease>,
}

impl BridgeSession {
    pub fn new(
        id: String,
        context: SessionContext,
        grant: BridgeGrant,
        hello: HelloResult,
    ) -> Self {
        Self {
            id,
            context,
            grant,
            hello,
            greeted: false,
            topics: BTreeSet::new(),
            window: VecDeque::new(),
            running: Vec::new(),
            host_calls: Vec::new(),
            revoked: None,
            #[cfg(not(target_arch = "wasm32"))]
            file_lease: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn hold_files(&mut self, lease: super::provider::BundleLease) {
        self.file_lease = Some(lease);
    }

    pub fn hello(&self) -> &HelloResult {
        &self.hello
    }

    pub fn revoked(&self) -> Option<RevokeReason> {
        self.revoked
    }

    /// Runs the checks of the module header on one message from the bundle.
    pub fn admit(&mut self, bytes: &[u8], host_bridge: u32, now_ms: i64) -> Admission {
        let envelope = match BridgeEnvelope::parse(bytes, host_bridge) {
            Ok(envelope) => envelope,
            Err((id, code)) => return Admission::Answer(failure(id, code)),
        };
        let id = envelope.id.clone().unwrap_or_default();
        if self.revoked.is_some() {
            return Admission::Answer(failure(Some(id), BridgeErrorCode::Revoked));
        }
        if self.running.iter().any(|(running, _)| *running == id)
            || self.host_calls.iter().any(|(call, _)| *call == id)
        {
            return Admission::Answer(failure(Some(id), BridgeErrorCode::InvalidEnvelope));
        }
        if !self.greeted && envelope.method.as_deref() != Some(BridgeMethod::HostHello.name()) {
            return Admission::Answer(failure(Some(id), BridgeErrorCode::NotReady));
        }
        let request = match envelope.request_body() {
            Ok(request) => request,
            Err(code) => return Admission::Answer(failure(Some(id), code)),
        };
        if let Err(code) = self.grant.authorize(&request) {
            return Admission::Answer(failure(Some(id), code));
        }
        let method = request.method();
        while self
            .window
            .front()
            .is_some_and(|at| now_ms.saturating_sub(*at) >= 1000)
        {
            self.window.pop_front();
        }
        if self.window.len() >= BRIDGE_REQUESTS_PER_SECOND as usize
            || (method.is_operation()
                && self.running.iter().filter(|(_, op)| *op).count()
                    >= BRIDGE_CONCURRENT_OPERATIONS as usize)
        {
            return Admission::Answer(failure(Some(id), BridgeErrorCode::RateLimited));
        }
        self.window.push_back(now_ms);
        match request {
            BridgeRequest::Hello(_) => {
                self.greeted = true;
                Admission::Answer(BridgeEnvelope::success(
                    id,
                    serde_json::to_value(&self.hello).unwrap_or_else(|_| json!({})),
                ))
            }
            BridgeRequest::EventsSubscribe(params) => {
                self.topics = params.topics;
                Admission::Answer(BridgeEnvelope::success(id, json!({})))
            }
            BridgeRequest::DiagLog(params) => {
                // Only a development build keeps it; bundle text never goes upstream.
                if cfg!(debug_assertions) {
                    tracing::debug!(
                        target: "flare_sdk.plugin_bundle",
                        plugin = %self.context.plugin_id,
                        level = ?params.level,
                        message = %params.message,
                        "bundle diagnostic"
                    );
                }
                Admission::Answer(BridgeEnvelope::success(id, json!({})))
            }
            request => {
                self.running.push((id.clone(), method.is_operation()));
                Admission::Run { id, request }
            }
        }
    }

    /// The request `id` finished (answered by the engine or the host).
    pub fn finish(&mut self, id: &str) {
        self.running.retain(|(running, _)| running != id);
        self.host_calls.retain(|(call, _)| call != id);
    }

    /// Hand a running request to the host. Refused past [`MAX_HOST_CALLS`].
    pub fn hand_to_host(&mut self, id: &str, method: BridgeMethod) -> bool {
        if self.host_calls.len() >= MAX_HOST_CALLS {
            return false;
        }
        self.host_calls.retain(|(call, _)| call != id);
        self.host_calls.push((id.to_owned(), method));
        true
    }

    /// The method of a request the host is drawing.
    pub fn host_call(&self, id: &str) -> Option<BridgeMethod> {
        self.host_calls
            .iter()
            .find(|(call, _)| call == id)
            .map(|(_, method)| *method)
    }

    pub fn subscribed(&self, topic: BridgeEvent) -> bool {
        self.greeted && self.revoked.is_none() && self.topics.contains(&topic)
    }

    /// End the session for `reason`: every later request is `revoked`. The
    /// `lifecycle.revoked` event goes to the bundle whether or not it
    /// subscribed; the host closes the page.
    pub fn revoke(&mut self, reason: RevokeReason) -> Option<BridgeEnvelope> {
        if self.revoked.is_some() {
            return None;
        }
        self.revoked = Some(reason);
        self.running.clear();
        self.host_calls.clear();
        Some(BridgeEnvelope::event(
            BridgeEvent::LifecycleRevoked,
            serde_json::to_value(LifecycleRevokedPayload { reason }).unwrap_or_default(),
        ))
    }
}

/// A failed response.
pub fn failure(id: Option<String>, code: BridgeErrorCode) -> BridgeEnvelope {
    BridgeEnvelope::failure(id, BridgeError::new(code))
}

/// The fixed bridge error of an engine error: the plugin's own rejection code
/// when it is one (`operation_failed` with `pluginCode`), otherwise a code by
/// the error's kind. No message text is passed on.
pub fn bridge_error_of(error: &FlareError) -> BridgeError {
    if let Some(code) = crate::extension::capability::plugin_errors::plugin_rejection_code(error) {
        return BridgeError::operation_failed(Some(code));
    }
    if crate::domain::is_bundle_storage_quota_error(error) {
        return BridgeError::new(BridgeErrorCode::QuotaExceeded);
    }
    let code = match error.code() {
        Some(ErrorCode::PermissionDenied) => BridgeErrorCode::PermissionDenied,
        Some(ErrorCode::InvalidParameter) => BridgeErrorCode::InvalidParams,
        Some(ErrorCode::OperationTimeout) => BridgeErrorCode::Timeout,
        Some(
            ErrorCode::ServiceUnavailable
            | ErrorCode::NotConnected
            | ErrorCode::OperationNotSupported,
        ) => BridgeErrorCode::Unavailable,
        Some(ErrorCode::MessageRateLimitExceeded) => BridgeErrorCode::RateLimited,
        _ => BridgeErrorCode::Internal,
    };
    BridgeError::new(code)
}

/// The live sessions of this client, and the states pages saved for their
/// next opening.
#[derive(Default)]
pub struct BundleSessions {
    inner: Mutex<SessionsInner>,
}

#[derive(Default)]
struct SessionsInner {
    /// Sorted by id; [`MAX_LIVE_SESSIONS`] at most, so a list, not a map.
    sessions: Vec<BridgeSession>,
    /// Opening order, oldest first.
    order: VecDeque<String>,
    /// `ui.saveState` by (tenant, viewer, plugin, contribution), oldest first.
    saved: VecDeque<((String, String, String, String), Value)>,
}

fn saved_key(context: &SessionContext) -> (String, String, String, String) {
    (
        context.tenant_id.clone(),
        context.viewer.clone(),
        context.plugin_id.clone(),
        context.contribution_id.clone(),
    )
}

impl SessionsInner {
    fn position(&self, id: &str) -> Result<usize, usize> {
        self.sessions
            .binary_search_by(|session| session.id.as_str().cmp(id))
    }

    fn take(&mut self, id: &str) -> Option<BridgeSession> {
        self.position(id).ok().map(|at| self.sessions.remove(at))
    }
}

impl BundleSessions {
    fn locked(&self) -> std::sync::MutexGuard<'_, SessionsInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keep `session`; the sessions it pushes past [`MAX_LIVE_SESSIONS`],
    /// oldest first, come back to be closed.
    pub fn insert(&self, session: BridgeSession) -> Vec<BridgeSession> {
        let mut inner = self.locked();
        inner.order.push_back(session.id.clone());
        match inner.position(&session.id) {
            Ok(at) => inner.sessions[at] = session,
            Err(at) => inner.sessions.insert(at, session),
        }
        let mut evicted = Vec::new();
        while inner.order.len() > MAX_LIVE_SESSIONS {
            if let Some(oldest) = inner.order.pop_front()
                && let Some(session) = inner.take(&oldest)
            {
                evicted.push(session);
            }
        }
        evicted
    }

    pub fn with<T>(&self, id: &str, f: impl FnOnce(&mut BridgeSession) -> T) -> Option<T> {
        let mut inner = self.locked();
        let at = inner.position(id).ok()?;
        Some(f(&mut inner.sessions[at]))
    }

    pub fn remove(&self, id: &str) -> Option<BridgeSession> {
        let mut inner = self.locked();
        inner.order.retain(|open| open != id);
        inner.take(id)
    }

    /// Every session, removed (signing out).
    pub fn drain(&self) -> Vec<BridgeSession> {
        let mut inner = self.locked();
        inner.order.clear();
        inner.saved.clear();
        std::mem::take(&mut inner.sessions)
    }

    pub fn is_empty(&self) -> bool {
        self.locked().sessions.is_empty()
    }

    /// Runs `f` on every session; what it returns, by session id.
    pub fn each<T>(&self, mut f: impl FnMut(&mut BridgeSession) -> Option<T>) -> Vec<(String, T)> {
        let mut inner = self.locked();
        inner
            .sessions
            .iter_mut()
            .filter_map(|session| f(session).map(|out| (session.id.clone(), out)))
            .collect()
    }

    pub fn save_state(&self, context: &SessionContext, state: Value) {
        let key = saved_key(context);
        let mut inner = self.locked();
        inner.saved.retain(|(saved, _)| *saved != key);
        inner.saved.push_back((key, state));
        while inner.saved.len() > MAX_SAVED_STATES {
            inner.saved.pop_front();
        }
    }

    /// The state the page saved last, taken (used once).
    pub fn take_saved_state(&self, context: &SessionContext) -> Option<Value> {
        let key = saved_key(context);
        let mut inner = self.locked();
        let index = inner.saved.iter().position(|(saved, _)| *saved == key)?;
        inner.saved.remove(index).map(|(_, state)| state)
    }
}

#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;
