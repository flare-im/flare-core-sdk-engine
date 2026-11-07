//! Product-neutral OS token lifecycle. Intent is saved BEFORE a possible write;
//! logout can revoke by capability without retaining a bearer or an OS token on disk.
//!
//! The browser has no OS push token (APNs, FCM, Getui): registration needs a native host, so
//! a wasm32 build never configures, attaches or reconciles, and reports `disabled`. Those
//! paths return before any journal, URL or HTTP work, which keeps that code out of the
//! web bundle.
use crate::{
    infrastructure::transport::http::{HttpApiResponse, HttpClient, unwrap_api_response},
    shared::error::{ErrorCode, FlareError, Result},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
mod journal;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
use journal::{Intent, Journal};

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushTokenInput {
    /// OS delivery provider: fcm, apns or getui.
    pub provider: String,
    /// Native platform: android, ios or harmony.
    pub platform: String,
    /// OS-issued token. Never log or persist outside protected native storage.
    pub token: String,
}
impl std::fmt::Debug for PushTokenInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushTokenInput")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClearPushTokenRequest {
    /// Provider whose current desired OS token is invalidated.
    pub provider: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetPushPermissionRequest {
    /// Current OS permission, including explicit denial or revocation.
    pub allowed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PushLifecycleStatus {
    /// disabled, waitingForSession, waitingForToken, pending or registered.
    pub state: String,
    /// Number of OS providers configured in memory.
    pub configured_providers: u32,
    /// Providers confirmed for the current session, not vendor delivery.
    pub registered_providers: u32,
    /// Unresolved revoke intents, including writes with an unknown result.
    pub pending_revocations: u32,
    /// Fixed safe error category; no token, URL, bearer or driver detail.
    pub last_error: Option<String>,
}
#[derive(Clone)]
struct Session {
    epoch: u64,
    generation: Arc<AtomicU64>,
    device: String,
    http: HttpClient,
    /// The binding's `expiresAtMs` is checked against the server's clock.
    server_clock: crate::kernel::ServerClock,
}
impl Session {
    fn current(&self) -> bool {
        self.generation.load(Ordering::Acquire) == self.epoch
    }
}
pub(crate) struct AttachPushSession {
    pub epoch: u64,
    pub generation: Arc<AtomicU64>,
    pub user: String,
    pub app: String,
    pub device: String,
    pub http: HttpClient,
    pub token: String,
    pub root: PathBuf,
    pub server_clock: crate::kernel::ServerClock,
}
struct Attempt {
    epoch: u64,
    provider: String,
    digest: String,
    confirmed: bool,
}
#[derive(Default)]
struct State {
    permission: bool,
    desired: BTreeMap<String, PushTokenInput>,
    session: Option<Session>,
    transport: Option<HttpClient>,
    journal: Option<Journal>,
    attempts: BTreeMap<String, Attempt>,
    last_error: Option<String>,
}
#[derive(Default)]
pub(crate) struct PushLifecycle {
    state: Mutex<State>,
    io: tokio::sync::Mutex<()>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct View {
    version: String,
    current_binding_id: String,
    state: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Registered {
    success: bool,
    binding_id: String,
}
#[derive(Deserialize)]
struct Revoked {
    success: bool,
}
fn failure(code: ErrorCode) -> FlareError {
    FlareError::localized(code, "push lifecycle unavailable; reconcile pending intent")
}
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn input_digest(input: &PushTokenInput) -> String {
    digest(&format!(
        "{}:{}:{}",
        input.provider, input.platform, input.token
    ))
}
fn valid_provider(provider: &str) -> bool {
    matches!(provider, "apns" | "fcm" | "getui")
}
fn validate(input: &PushTokenInput) -> Result<()> {
    if cfg!(target_arch = "wasm32") {
        return Err(FlareError::localized(
            ErrorCode::OperationNotSupported,
            "native OS push registration requires a native host",
        ));
    }
    if !valid_provider(&input.provider)
        || !matches!(input.platform.as_str(), "ios" | "android" | "harmony")
        || input.token.is_empty()
        || input.token.len() > 4096
        || input
            .token
            .chars()
            .any(|c| c.is_control() || c.is_whitespace())
        || (input.provider == "apns"
            && (!input.token.bytes().all(|b| b.is_ascii_hexdigit()) || input.token.len() > 512))
    {
        return Err(failure(ErrorCode::InvalidParameter));
    }
    Ok(())
}
impl PushLifecycle {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn update(&self, input: PushTokenInput) -> Result<()> {
        validate(&input)?;
        let mut s = self.lock();
        s.desired.insert(input.provider.clone(), input);
        s.last_error = None;
        Ok(())
    }
    pub fn clear(&self, provider: &str) -> Result<()> {
        if !valid_provider(provider) {
            return Err(failure(ErrorCode::InvalidParameter));
        }
        self.lock().desired.remove(provider);
        Ok(())
    }
    pub fn permission(&self, allowed: bool) {
        self.lock().permission = allowed;
    }
    pub fn detach(&self) {
        self.lock().session = None;
    }
    /// Load revoke-only intent without a login. Serializes endpoint changes with IO;
    /// old capabilities are never forwarded to a newly configured deployment.
    pub async fn configure(&self, http: HttpClient, root: PathBuf, device: &str) -> Result<()> {
        let _io = self.io.lock().await;
        self.configure_locked(http, root, device)
    }
    fn configure_locked(&self, http: HttpClient, root: PathBuf, device: &str) -> Result<()> {
        if cfg!(target_arch = "wasm32") {
            return Ok(());
        }
        // 路由客户端的入口来自验签后的接入清单（开发模式来自本机配置），不再逐个校验地址；
        // 固定入口（测试替身）仍只接受 https 或本机 http。
        if !http.is_routed() {
            // 带 userinfo 的地址在解析时就被拒绝。
            let endpoint = flare_access::ParsedUrl::parse(http.base_url())
                .map_err(|_| failure(ErrorCode::ConfigurationError))?;
            let host = endpoint.host_str();
            let local = host == "localhost"
                || host.ends_with(".localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            if endpoint.query().is_some()
                || endpoint.fragment().is_some()
                || !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && local)
            {
                return Err(failure(ErrorCode::ConfigurationError));
            }
        }
        if device.is_empty()
            || device.len() > 128
            || device.chars().any(|c| c.is_control() || c.is_whitespace())
        {
            return Err(failure(ErrorCode::InvalidParameter));
        }
        let root = root
            .join("push-lifecycle")
            .join(digest(&format!("{}\0{device}", http.endpoint_identity())));
        let mut s = self.lock();
        if s.journal
            .as_ref()
            .is_none_or(|j| j.root() != root.as_path())
        {
            s.session = None;
            if s.journal.as_ref().is_some_and(|j| !j.entries.is_empty()) {
                s.last_error = Some("storage".into());
                return Err(failure(ErrorCode::DatabaseError));
            }
            // Release an empty old lease before opening another namespace.
            s.journal = None;
            s.journal = Some(Journal::open(root).inspect_err(|_| {
                s.last_error = Some("storage".into());
            })?);
            s.attempts.clear();
            s.session = None;
        }
        s.transport = Some(http.anonymous_clone());
        Ok(())
    }
    pub async fn attach(&self, request: AttachPushSession) -> Result<()> {
        if cfg!(target_arch = "wasm32") {
            return Ok(());
        }
        let AttachPushSession {
            epoch,
            generation,
            user,
            app,
            device,
            http,
            token,
            root,
            server_clock,
        } = request;
        use base64::Engine;
        let claims = token
            .split('.')
            .nth(1)
            .filter(|v| v.len() <= 16384)
            .and_then(|v| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(v)
                    .ok()
            })
            .and_then(|v| serde_json::from_slice::<serde_json::Value>(&v).ok());
        if claims.as_ref().is_none_or(|v| {
            v["sub"].as_str() != Some(user.as_str())
                || v["tenant_id"].as_str() != Some(app.as_str())
                || v["device_id"].as_str() != Some(device.as_str())
        }) || device.is_empty()
            || device.len() > 128
        {
            return Err(failure(ErrorCode::AuthenticationFailed));
        }
        let pinned = http.pinned_core_auth(token, &user, &app).await;
        let _io = self.io.lock().await;
        if generation.load(Ordering::Acquire) != epoch {
            return Err(failure(ErrorCode::NotConnected));
        }
        self.configure_locked(pinned.clone(), root, &device)?;
        let mut s = self.lock();
        if generation.load(Ordering::Acquire) != epoch {
            return Err(failure(ErrorCode::NotConnected));
        }
        s.session = Some(Session {
            epoch,
            generation,
            device,
            http: pinned,
            server_clock,
        });
        s.last_error = None;
        Ok(())
    }
    pub fn status(&self) -> PushLifecycleStatus {
        if cfg!(target_arch = "wasm32") {
            return PushLifecycleStatus {
                state: "disabled".into(),
                configured_providers: 0,
                registered_providers: 0,
                pending_revocations: 0,
                last_error: None,
            };
        }
        let s = self.lock();
        let confirmed = s
            .attempts
            .values()
            .filter(|a| {
                a.confirmed
                    && s.session
                        .as_ref()
                        .is_some_and(|v| v.current() && v.epoch == a.epoch)
                    && s.permission
                    && s.desired
                        .get(&a.provider)
                        .is_some_and(|v| input_digest(v) == a.digest)
            })
            .count();
        let pending = s
            .journal
            .as_ref()
            .map_or(0, |j| j.entries.len())
            .saturating_sub(confirmed);
        PushLifecycleStatus {
            state: if pending > 0 || s.last_error.is_some() {
                "pending"
            } else if !s.permission {
                "disabled"
            } else if s.session.as_ref().is_none_or(|v| !v.current()) {
                "waitingForSession"
            } else if s.desired.is_empty() {
                "waitingForToken"
            } else if confirmed == s.desired.len() {
                "registered"
            } else {
                "pending"
            }
            .into(),
            configured_providers: s.desired.len() as u32,
            registered_providers: confirmed as u32,
            pending_revocations: pending as u32,
            last_error: s.last_error.clone(),
        }
    }
    fn keep(&self, id: &str) -> bool {
        let s = self.lock();
        self.keep_unlocked(&s, id)
    }
    pub async fn reconcile(&self) -> PushLifecycleStatus {
        let result = crate::shared::util::timeout(Duration::from_secs(20), self.run()).await;
        if !matches!(result, Ok(Ok(()))) {
            self.lock().last_error = Some("unavailable".into());
        }
        self.status()
    }
    async fn run(&self) -> Result<()> {
        if cfg!(target_arch = "wasm32") {
            return Ok(());
        }
        let _io = self.io.lock().await;
        let (transport, entries) = {
            let s = self.lock();
            (
                s.transport.clone(),
                s.journal
                    .as_ref()
                    .map(|j| j.entries.clone())
                    .unwrap_or_default(),
            )
        };
        let Some(transport) = transport else {
            return Ok(());
        };
        let endpoint = digest(&transport.endpoint_identity());
        for intent in entries {
            if intent.endpoint != endpoint {
                return Err(failure(ErrorCode::ConfigurationError));
            }
            let id = digest(&intent.revocation_token);
            if self.keep(&id) {
                let (confirmed, session) = {
                    let s = self.lock();
                    (
                        s.attempts.get(&id).is_some_and(|a| a.confirmed),
                        s.session.clone(),
                    )
                };
                if confirmed {
                    continue;
                }
                if let Some(session) = session {
                    let view = self.inspect(&session, &intent.provider, &id).await?;
                    if view.state == "active" && view.current_binding_id == id && self.keep(&id) {
                        if let Some(a) = self.lock().attempts.get_mut(&id) {
                            a.confirmed = true;
                        }
                        continue;
                    }
                }
            }
            let response: HttpApiResponse<Revoked> = io(transport.post(
                "/api/v1/push/bindings/revoke",
                &serde_json::json!({"revocationToken":intent.revocation_token}),
            ))
            .await?;
            if !unwrap_api_response(response, "revoke push binding")?.success {
                return Err(failure(ErrorCode::InternalError));
            }
            let mut s = self.lock();
            if let Some(journal) = s.journal.as_mut() {
                journal.remove(&id)?;
            }
            s.attempts.remove(&id);
        }
        let (session, desired, permission) = {
            let s = self.lock();
            (s.session.clone(), s.desired.clone(), s.permission)
        };
        let Some(session) = session.filter(|s| permission && s.current()) else {
            self.lock().last_error = None;
            return Ok(());
        };
        for input in desired.into_values() {
            let already_registered = {
                let s = self.lock();
                s.attempts.iter().any(|(id, a)| {
                    a.provider == input.provider && a.confirmed && self.keep_unlocked(&s, id)
                })
            };
            if already_registered {
                continue;
            }
            let view = self.inspect(&session, &input.provider, "").await?;
            let secret = rand::random::<[u8; 32]>()
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            let id = digest(&secret);
            let intent = Intent {
                revocation_token: secret,
                endpoint: endpoint.clone(),
                provider: input.provider.clone(),
            };
            {
                let mut s = self.lock();
                if !s.permission
                    || !session.current()
                    || s.session.as_ref().is_none_or(|v| v.epoch != session.epoch)
                    || s.desired
                        .get(&input.provider)
                        .is_none_or(|v| input_digest(v) != input_digest(&input))
                {
                    return Ok(());
                }
                s.journal
                    .as_mut()
                    .ok_or_else(|| failure(ErrorCode::DatabaseError))?
                    .add(intent)?;
                s.attempts.insert(
                    id.clone(),
                    Attempt {
                        epoch: session.epoch,
                        provider: input.provider.clone(),
                        digest: input_digest(&input),
                        confirmed: false,
                    },
                );
            }
            // The server refuses a deadline outside its own next ten minutes: count from its clock.
            let now = session.server_clock.now_ms();
            let body = serde_json::json!({"provider":input.provider,"platform":input.platform,"token":input.token,"bindingId":id,"expectedVersion":view.version,"expiresAtMs":now+120_000});
            let response: HttpApiResponse<Registered> =
                io(session.http.put(&path(&session.device), &body)).await?;
            let response = unwrap_api_response(response, "register push binding")?;
            if !response.success || response.binding_id != id {
                return Err(failure(ErrorCode::InternalError));
            }
            if self.keep(&id)
                && let Some(a) = self.lock().attempts.get_mut(&id)
            {
                a.confirmed = true;
            }
        }
        self.lock().last_error = None;
        Ok(())
    }
    fn keep_unlocked(&self, s: &State, id: &str) -> bool {
        s.permission
            && s.attempts.get(id).is_some_and(|a| {
                s.session
                    .as_ref()
                    .is_some_and(|v| v.current() && v.epoch == a.epoch)
                    && s.desired
                        .get(&a.provider)
                        .is_some_and(|v| input_digest(v) == a.digest)
            })
    }
    async fn inspect(&self, session: &Session, provider: &str, id: &str) -> Result<View> {
        let query = std::collections::HashMap::from([
            ("provider".into(), provider.into()),
            ("bindingId".into(), id.into()),
        ]);
        let result: HttpApiResponse<View> =
            io(session.http.get(&path(&session.device), Some(&query))).await?;
        let value = unwrap_api_response(result, "inspect push binding")?;
        if (!value.version.is_empty() && !hex_id(&value.version))
            || (!value.current_binding_id.is_empty() && !hex_id(&value.current_binding_id))
            || !matches!(
                value.state.as_str(),
                "absent" | "active" | "revoked" | "notCurrent"
            )
        {
            return Err(failure(ErrorCode::InternalError));
        }
        Ok(value)
    }
}
fn path(device: &str) -> String {
    format!(
        "/api/v1/devices/{}/push-token",
        device
            .bytes()
            .map(
                |b| if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
                    (b as char).to_string()
                } else {
                    format!("%{b:02X}")
                }
            )
            .collect::<String>()
    )
}
fn hex_id(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
async fn io<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let request = std::pin::pin!(future);
    let deadline = std::pin::pin!(crate::shared::util::delay(Duration::from_secs(5)));
    match futures::future::select(request, deadline).await {
        futures::future::Either::Left((result, _)) => {
            result.map_err(|_| failure(ErrorCode::InternalError))
        }
        futures::future::Either::Right(_) => Err(failure(ErrorCode::OperationTimeout)),
    }
}
