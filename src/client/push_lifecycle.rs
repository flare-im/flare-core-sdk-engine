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
    /// iOS bundle id or Android applicationId. The server sends APNs to this topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_bundle: Option<String>,
    /// APNs only: sandbox or production, the APNs host the token belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    /// BCP 47 language tag the server renders notification text in, e.g. zh-CN.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    /// IANA time zone, e.g. Asia/Shanghai.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    /// App version string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_version: Option<String>,
    /// What the app can do with a push: dataMessages (Android FCM data messages) and
    /// serviceExtension (iOS notification service extension). Unknown values are rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
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
/// 登记的身份：令牌之外，登记附带的元数据也算在内。任何一项变了都和换了令牌一样，下一次对账时
/// 撤销旧登记、按 CAS 重新登记，服务端才会按新的包名、环境、语言去推。
fn input_digest(input: &PushTokenInput) -> String {
    let field = |value: &Option<String>| {
        value
            .as_deref()
            .map_or_else(|| "-".to_owned(), |value| format!("+{value}"))
    };
    // 能力的先后不算变化。
    let capabilities = input.capabilities.as_ref().map_or_else(
        || "-".to_owned(),
        |capabilities| {
            let mut sorted = capabilities.clone();
            sorted.sort();
            format!("+{}", sorted.join(","))
        },
    );
    digest(&format!(
        "{}:{}:{}\0{}\0{}\0{}\0{}\0{}\0{}",
        input.provider,
        input.platform,
        input.token,
        field(&input.app_bundle),
        field(&input.environment),
        field(&input.locale),
        field(&input.time_zone),
        field(&input.app_version),
        capabilities
    ))
}
/// PUT 登记的请求体：令牌与 CAS 字段之外，附带的元数据（有的才带）原样带上，键名与输入相同。
fn binding_body(
    input: &PushTokenInput,
    binding_id: &str,
    expected_version: &str,
    expires_at_ms: i64,
) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("provider".into(), input.provider.clone().into());
    body.insert("platform".into(), input.platform.clone().into());
    body.insert("token".into(), input.token.clone().into());
    body.insert("bindingId".into(), binding_id.into());
    body.insert("expectedVersion".into(), expected_version.into());
    body.insert("expiresAtMs".into(), expires_at_ms.into());
    for (key, value) in [
        ("appBundle", &input.app_bundle),
        ("environment", &input.environment),
        ("locale", &input.locale),
        ("timeZone", &input.time_zone),
        ("appVersion", &input.app_version),
    ] {
        if let Some(value) = value {
            body.insert(key.into(), value.clone().into());
        }
    }
    if let Some(capabilities) = &input.capabilities {
        body.insert("capabilities".into(), capabilities.clone().into());
    }
    serde_json::Value::Object(body)
}
fn valid_provider(provider: &str) -> bool {
    matches!(provider, "apns" | "fcm" | "getui")
}
/// 已知的推送能力：Android FCM 收数据消息、iOS 通知服务扩展。
const KNOWN_CAPABILITIES: &[&str] = &["dataMessages", "serviceExtension"];
/// 元数据只做轻校验：长度、字符集、取值在已知集合里。不认识的值直接拒绝，不静默丢弃。
fn valid_metadata(input: &PushTokenInput) -> bool {
    let plain = |value: &str, max: usize, extra: &[u8]| {
        !value.is_empty()
            && value.len() <= max
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || extra.contains(&b))
    };
    input
        .app_bundle
        .as_deref()
        .is_none_or(|value| plain(value, 255, b"._-".as_slice()))
        && match input.environment.as_deref() {
            None => true,
            // APNs 才分沙盒与生产，别的通道带了就是传错了。
            Some("sandbox" | "production") => input.provider == "apns",
            Some(_) => false,
        }
        && input
            .locale
            .as_deref()
            .is_none_or(|value| plain(value, 35, b"-_".as_slice()))
        && input
            .time_zone
            .as_deref()
            .is_none_or(|value| plain(value, 64, b"/_-+".as_slice()))
        && input.app_version.as_deref().is_none_or(|value| {
            !value.trim().is_empty() && value.len() <= 64 && !value.chars().any(char::is_control)
        })
        && input.capabilities.as_deref().is_none_or(|capabilities| {
            capabilities.iter().enumerate().all(|(index, capability)| {
                KNOWN_CAPABILITIES.contains(&capability.as_str())
                    && !capabilities[..index].contains(capability)
            })
        })
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
        || !valid_metadata(input)
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
            let body = binding_body(&input, &id, &view.version, now + 120_000);
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
