use std::time::Duration;

use flare_access::release::UpdateStatus;

use crate::FlareError;
use crate::client::token_provider::{GatewayTokenProvider, jwt_exp_secs};
use crate::kernel::SyncRunContext;
use crate::kernel::event::{
    ConnectionEvent, ConnectionEventType, EventBus, SdkEvent, SdkEventType,
};
use crate::runtime::link::{ConnectToken, LinkTokenSource};
use crate::shared::error::{ErrorCode, Result};
use crate::shared::util::delay;

use super::{IMClient, WeakIMClient, spawn_im_background};

/// 建连前令牌在这么多秒内到期就先换新（按服务端时钟判断）。
const CONNECT_TOKEN_REFRESH_LEAD_SECS: u64 = 30;
/// 宿主续期回调（[`crate::client::token_provider::ConnectTokenRefresher`]）的最长等待。
const HOST_TOKEN_RENEWAL_TIMEOUT: Duration = Duration::from_secs(20);

/// 会话给链路状态机的令牌来源：只认自己那一代会话，会话结束后什么也不给。
pub(crate) struct SessionTokens {
    client: WeakIMClient,
    generation: u64,
}

impl SessionTokens {
    pub(crate) fn new(client: &IMClient, generation: u64) -> Self {
        Self {
            client: client.downgrade(),
            generation,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl LinkTokenSource for SessionTokens {
    async fn token_for_connect(&self) -> ConnectToken {
        match self.client.upgrade() {
            Some(client) => client.token_for_connect(self.generation).await,
            None => ConnectToken::Unavailable,
        }
    }

    async fn renew_after_rejection(&self, rejected: &str) -> Option<String> {
        self.client
            .upgrade()?
            .renew_connect_token(self.generation, rejected)
            .await
    }
}

impl IMClient {
    /// 会话期间按连接状态维护状态快照（同步读路径用）。会话结束（登出、断开、重新登录、客户端销毁）
    /// 时立即收手。
    pub(super) fn spawn_state_snapshot_watcher(&self, generation: u64, bus: EventBus) {
        let client = self.downgrade();
        let session_end = self.session_end_watch(generation);
        // 只订连接状态：总线不必为本订阅者深克隆消息批等热事件。
        let mut rx =
            bus.subscribe_event_type(SdkEventType::Connection(ConnectionEventType::Status));
        spawn_im_background(async move {
            loop {
                let event = tokio::select! {
                    event = rx.recv() => match event {
                        Ok(ev) => ev,
                        Err(_) => break,
                    },
                    () = session_end.ended() => break,
                };
                let Some(client) = client.upgrade() else {
                    break;
                };
                if !client.is_generation_current(generation).await {
                    break;
                }
                if let SdkEvent::Connection(ConnectionEvent::Status(status)) = event {
                    client.store_state_snapshot(status.sdk_state());
                }
            }
            tracing::debug!(
                session_generation = generation,
                "SDK state snapshot watcher stopped"
            );
        });
    }

    /// 把更新状态转到这一代会话的事件总线上（`update://state`）。更新中心跨会话，总线不跨：
    /// 会话开始时先宣告一次当前状态，之后逐条转发，会话结束时收手，不让总线活得比会话久。
    pub(super) fn spawn_update_state_watcher(&self, generation: u64) {
        let client = self.downgrade();
        let session_end = self.session_end_watch(generation);
        spawn_im_background(async move {
            let Some(strong) = client.upgrade() else {
                return;
            };
            let Some(update) = strong.update_runtime().await else {
                return;
            };
            drop(strong);
            let mut states = update.subscribe();
            if !publish_update_state(&client, generation, update.status_now()).await {
                return;
            }
            loop {
                let status = tokio::select! {
                    status = states.recv() => match status {
                        Ok(status) => status,
                        // 只关心最新值：落后了就补发现在的。
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            update.status_now()
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    () = session_end.ended() => break,
                };
                if !publish_update_state(&client, generation, status).await {
                    break;
                }
            }
        });
    }

    pub(super) async fn sync_foreground_convergence_silent(&self) -> Result<()> {
        let sync = self
            .with_engine_async(|engine| engine.conversation_summary_sync())
            .await?
            .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未配置同步"))?;
        let user_id = self.current_user_id().await.unwrap_or_default();
        if user_id.is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        sync.sync_foreground_convergence(
            &user_id,
            SyncRunContext::silent_multidevice_private_data(),
        )
        .await
    }

    /// 建连前取令牌（链路每次尝试建连前调用）。
    ///
    /// - SDK 托管形态还没有令牌（刚登录）：先向网关签发。签发放在这里而不是登录时，离线冷启动的
    ///   链路才能照常起来、显示等网络，联网后先签发再握手。
    /// - 令牌按服务端时钟已临期（或读不出到期）就先换新——带着服务端已判过期的令牌去连只会被拒。
    ///   换新没走通时，令牌还没真过期就照常用它连；已经过期则按网络失败退避。
    /// - 宿主托管形态原样返回，续期只在网关明确拒绝之后（见 [`Self::renew_connect_token`]）。
    pub(super) async fn token_for_connect(&self, generation: u64) -> ConnectToken {
        let (token, user_id, provider, clock) = {
            let g = self.inner.read().await;
            if g.session_generation != generation {
                return ConnectToken::Unavailable;
            }
            (
                g.connect_token
                    .clone()
                    .filter(|token| !token.trim().is_empty()),
                g.current_user_id.clone(),
                Self::gateway_token_provider_from_inner(&g),
                server_clock_of(&g),
            )
        };
        let Some(token) = token else {
            let Some(user_id) = user_id else {
                return ConnectToken::Unavailable;
            };
            return match provider {
                Some(provider) => {
                    self.issue_via_gateway(generation, &user_id, &provider)
                        .await
                }
                // 联调：环境变量里的令牌。
                None => crate::client::lifecycle::resolve_connect_token(&user_id, None)
                    .map_or(ConnectToken::Unavailable, ConnectToken::Ready),
            };
        };
        if let Some(provider) = provider
            && token_needs_refresh(&token, CONNECT_TOKEN_REFRESH_LEAD_SECS, &clock)
        {
            match self.refresh_tokens_via_gateway(generation, &provider).await {
                Ok(issued) => return ConnectToken::Ready(issued.token),
                Err(err) if token_needs_refresh(&token, 0, &clock) && !is_auth_failure(&err) => {
                    tracing::warn!(%err, "the connect token expired and could not be renewed; retrying with backoff");
                    return ConnectToken::Unreachable(as_network_failure(err));
                }
                Err(err) => {
                    tracing::warn!(%err, "token refresh before connect failed; connecting with the current token");
                }
            }
        }
        ConnectToken::Ready(token)
    }

    /// SDK 托管形态登录后的第一枚令牌：向网关签发，记下接入令牌与刷新令牌，并给共享 HTTP 带上它。
    async fn issue_via_gateway(
        &self,
        generation: u64,
        user_id: &str,
        provider: &GatewayTokenProvider,
    ) -> ConnectToken {
        let issued = match provider.issue(user_id).await {
            Ok(issued) => issued,
            Err(err) if is_auth_failure(&err) => {
                tracing::warn!(%err, "the gateway refused to issue a connect token");
                return ConnectToken::Unavailable;
            }
            Err(err) => {
                tracing::warn!(%err, "issuing a connect token failed; retrying with backoff");
                return ConnectToken::Unreachable(as_network_failure(err));
            }
        };
        tracing::info!(
            user_id,
            expires_at = issued.expires_at,
            has_refresh = issued.refresh_token.is_some(),
            "access token issued by gateway"
        );
        let (context, tenant_id) = {
            let mut g = self.inner.write().await;
            if g.session_generation != generation {
                return ConnectToken::Unavailable;
            }
            g.connect_token = Some(issued.token.clone());
            // 长效刷新令牌：接入令牌过期后凭它换新，支撑 7x24 免重登。
            g.refresh_token = issued.refresh_token.clone();
            (g.http_request_context.clone(), Self::resolve_tenant_id(&g))
        };
        if let Some(context) = context {
            context.set_auth_context(issued.token.clone(), None).await;
            context.ensure_identity(user_id, &tenant_id).await;
        }
        ConnectToken::Ready(issued.token)
    }

    /// 网关以鉴权原因拒绝了 `rejected`：续期一次。SDK 托管形态走刷新令牌向网关换新；宿主托管形态
    /// 问宿主装的续期回调。拿到的令牌与 `rejected` 不同才算换到——和被拒的那枚比，而不是和写回时的比：
    /// 宿主刷新成功后往往另起任务把新令牌推进来（update_access_token），它可能抢先写入。
    pub(super) async fn renew_connect_token(
        &self,
        generation: u64,
        rejected: &str,
    ) -> Option<String> {
        let sdk_managed = Self::sdk_auth_from_inner(&*self.inner.read().await).is_some();
        if sdk_managed {
            let provider = self.gateway_token_provider().await?;
            return match self.refresh_tokens_via_gateway(generation, &provider).await {
                Ok(issued) if issued.token != rejected => Some(issued.token),
                Ok(_) => None,
                Err(err) => {
                    tracing::warn!(%err, "renewing the connect token via the gateway failed");
                    None
                }
            };
        }
        let refresher = {
            let g = self.inner.read().await;
            if g.session_generation != generation {
                return None;
            }
            g.connect_token_refresher.clone()?
        };
        // 回调跨进 C / JS / Tauri 前端，宿主可能永远不答；限时后按「没换到」处理，链路照常退避，
        // 不让一次无响应的续期把重连卡住。
        let fresh = match crate::shared::util::time::timeout(
            HOST_TOKEN_RENEWAL_TIMEOUT,
            refresher.refresh_connect_token(),
        )
        .await
        {
            Ok(fresh) => fresh?,
            Err(_) => {
                tracing::warn!(
                    timeout_secs = HOST_TOKEN_RENEWAL_TIMEOUT.as_secs(),
                    "the host did not answer the connect token renewal in time"
                );
                return None;
            }
        };
        let fresh = fresh.trim();
        if fresh.is_empty() || fresh == rejected {
            return None;
        }
        let mut g = self.inner.write().await;
        if g.session_generation != generation {
            return None;
        }
        g.connect_token = Some(fresh.to_string());
        tracing::info!(
            session_generation = generation,
            "connect rejected by the gateway; the host supplied a fresh connect token"
        );
        Some(fresh.to_string())
    }

    /// SDK 托管形态下的网关签发器；应用托管（显式 token）时为 `None`。
    pub(super) async fn gateway_token_provider(&self) -> Option<GatewayTokenProvider> {
        let g = self.inner.read().await;
        Self::gateway_token_provider_from_inner(&g)
    }

    /// SDK 托管连接令牌的配置（构建期配置叠 init 覆盖项）；`None` 表示令牌由宿主给。
    pub(super) fn sdk_auth_from_inner(
        g: &super::IMClientInner,
    ) -> Option<crate::client::config::SdkAuthConfig> {
        match (g.sdk_config.as_ref(), g.configured_config.as_ref()) {
            (Some(overlay), _) if overlay.auth.is_some() => overlay.auth.clone(),
            (_, Some(configured)) => configured.auth.clone(),
            _ => None,
        }
    }

    /// 签发与换新走接入清单的 Core API（HttpRouter，换接入点与重试照 §3.9；开发模式即
    /// `developmentEndpoints.coreApiUrl`），用部署的信任根，不带会话鉴权头（凭据由签发、换新各自给出）。
    /// 不是 SDK 托管、接入还没初始化、或签发路径写错时为 `None`。
    pub(super) fn gateway_token_provider_from_inner(
        g: &super::IMClientInner,
    ) -> Option<GatewayTokenProvider> {
        use crate::infrastructure::transport::http::router::HttpRouter;
        use crate::infrastructure::transport::http::{HttpClient, HttpRequestContext};

        let auth = Self::sdk_auth_from_inner(g)?;
        let path = match auth.token_path() {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!(%error, "SDK-managed tokens are misconfigured");
                return None;
            }
        };
        let access = g.access.clone()?;
        let router = HttpRouter::new(access, flare_access::ServiceClass::CoreApi);
        let context = std::sync::Arc::new(HttpRequestContext::new());
        #[cfg(not(target_arch = "wasm32"))]
        let http = {
            let config = crate::client::lifecycle::merge_sdk_config_onto(
                g.configured_config.clone().unwrap_or_default(),
                g.sdk_config.as_ref(),
            );
            HttpClient::routed(router, context, config.core_tls_config())
        };
        #[cfg(target_arch = "wasm32")]
        let http = HttpClient::routed(router, context);
        Some(GatewayTokenProvider::new(
            http,
            path,
            Some(Self::resolve_tenant_id(g)),
            g.sdk_config
                .as_ref()
                .and_then(|overlay| overlay.device_id.clone())
                .or_else(|| {
                    g.configured_config
                        .as_ref()
                        .and_then(|configured| configured.device_id.clone())
                }),
        ))
    }

    /// 走网关换新一对令牌：优先拿**刷新令牌**作 Bearer（接入令牌早过期也能换），
    /// 旧网关不下发刷新令牌时回退用接入令牌（服务端两者都认）。成功后回写 access + 轮换后的 refresh。
    pub(super) async fn refresh_tokens_via_gateway(
        &self,
        generation: u64,
        provider: &GatewayTokenProvider,
    ) -> Result<crate::client::token_provider::IssuedAccessToken> {
        let (bearer, tenant_id) = {
            let g = self.inner.read().await;
            if g.session_generation != generation {
                return Err(FlareError::localized(
                    ErrorCode::NotConnected,
                    "session generation changed",
                ));
            }
            // 刷新令牌优先；没有再退回接入令牌（兼容旧网关/旧会话）。
            let bearer = g
                .refresh_token
                .clone()
                .or_else(|| g.connect_token.clone())
                .ok_or_else(|| {
                    FlareError::localized(ErrorCode::NotConnected, "no token to refresh")
                })?;
            (bearer, Self::resolve_tenant_id(&g))
        };
        let issued = provider.refresh(&bearer).await?;
        {
            let mut g = self.inner.write().await;
            if g.session_generation == generation {
                g.connect_token = Some(issued.token.clone());
                // 轮换后的新刷新令牌；网关没下发则保留原来的（别把已有的清成 None）。
                if issued.refresh_token.is_some() {
                    g.refresh_token = issued.refresh_token.clone();
                }
            }
        }
        // 同步 HTTP 侧 bearer（媒体等）。
        let _ = self.sync_core_http_context(Some(&tenant_id)).await;
        Ok(issued)
    }

    /// SDK 托管 token：到期前 `refresh_lead_secs` 秒换新并 `update_access_token`。
    /// 宿主托管形态（没配 `auth`）什么都不做——刷新是宿主的事。
    ///
    /// 会话结束（登出、断开、重新登录、终止会话、客户端销毁）时立即收手：此前它睡到下一次刷新
    /// （可能是几十分钟后）才发现世代已变，一直挂着。
    pub(super) fn spawn_token_refresh_watcher(&self, generation: u64) {
        let client = self.downgrade();
        let session_end = self.session_end_watch(generation);
        spawn_im_background(async move {
            'watcher: loop {
                let Some(c) = client.upgrade() else { break };
                if !c.is_generation_current(generation).await {
                    break;
                }
                let (provider, token, lead_secs, clock) = {
                    let g = c.inner.read().await;
                    let provider = Self::gateway_token_provider_from_inner(&g);
                    let lead = Self::sdk_auth_from_inner(&g)
                        .map(|auth| auth.refresh_lead().as_secs())
                        .unwrap_or(crate::client::config::SdkAuthConfig::DEFAULT_REFRESH_LEAD_SECS);
                    (provider, g.connect_token.clone(), lead, server_clock_of(&g))
                };
                let Some(provider) = provider else {
                    break;
                };
                let Some(token) = token else {
                    // 还没签发（链路在等网络或在退避）：签发由链路建连前完成，稍后再看。
                    drop(c);
                    tokio::select! {
                        () = delay(Duration::from_secs(5)) => {}
                        () = session_end.ended() => break 'watcher,
                    }
                    continue;
                };
                let wait_secs = token_refresh_wait_secs(&token, lead_secs, &clock);
                drop(c);
                tokio::select! {
                    () = delay(Duration::from_secs(wait_secs.max(5))) => {}
                    () = session_end.ended() => break 'watcher,
                }
                let Some(c) = client.upgrade() else { break };
                if !c.is_generation_current(generation).await {
                    break;
                }
                match c.refresh_tokens_via_gateway(generation, &provider).await {
                    Ok(issued) => {
                        tracing::info!(
                            expires_at = issued.expires_at,
                            rotated_refresh = issued.refresh_token.is_some(),
                            "access token refreshed via gateway (refresh-token grant)"
                        );
                    }
                    Err(err) => {
                        // 网关暂时不可达：一分钟后再试；真正过期时 TOKEN_EXPIRED 路径还会兜底。
                        tracing::warn!(%err, "scheduled token refresh failed; retrying in 60s");
                        drop(c);
                        tokio::select! {
                            () = delay(Duration::from_secs(60)) => {}
                            () = session_end.ended() => break 'watcher,
                        }
                    }
                }
            }
            tracing::debug!(
                session_generation = generation,
                "SDK token refresh watcher stopped"
            );
        });
    }

    pub(super) async fn is_generation_current(&self, generation: u64) -> bool {
        self.load_session_generation_snapshot() == generation
    }
}

/// 发到这一代会话的引擎总线上。不留总线的克隆，客户端释放时才收得干净。会话不在了返回 `false`。
async fn publish_update_state(
    client: &WeakIMClient,
    generation: u64,
    status: UpdateStatus,
) -> bool {
    let Some(client) = client.upgrade() else {
        return false;
    };
    if !client.is_generation_current(generation).await {
        return false;
    }
    let event = SdkEvent::Connection(ConnectionEvent::UpdateState(Box::new(status)));
    client
        .with_engine_async(|engine| {
            engine.bus().publish(event);
        })
        .await
        .is_ok()
}

/// The server clock estimate of the session's engine; without an engine, an
/// empty estimate (the device clock).
pub(super) fn server_clock_of(inner: &super::IMClientInner) -> crate::kernel::ServerClock {
    inner
        .engine
        .as_ref()
        .map(|engine| engine.server_clock().clone())
        .unwrap_or_default()
}

/// Whether `token` expires within `lead_secs`. Its `exp` is the server's
/// clock, so it is compared with the server time estimate: a device clock
/// running ahead would otherwise refresh every token on arrival.
pub(super) fn token_needs_refresh(
    token: &str,
    lead_secs: u64,
    clock: &crate::kernel::ServerClock,
) -> bool {
    jwt_exp_secs(token).is_some_and(|exp| exp <= clock.now_secs() + lead_secs)
}

/// How long the refresh watcher sleeps before refreshing `token`: until
/// `lead_secs` before its `exp`, on the server clock (a device clock ahead of
/// the server by more than the token's lifetime would otherwise refresh it
/// every few seconds). A token without a readable `exp` is looked at again
/// in ten minutes (the app may have replaced it).
fn token_refresh_wait_secs(token: &str, lead_secs: u64, clock: &crate::kernel::ServerClock) -> u64 {
    match jwt_exp_secs(token) {
        Some(exp) => exp
            .saturating_sub(lead_secs)
            .saturating_sub(clock.now_secs()),
        None => 600,
    }
}

/// 网关明确拒绝（401/403、签发被拒）：重试无益，需要用户动作。
fn is_auth_failure(error: &FlareError) -> bool {
    error.code() == Some(ErrorCode::AuthenticationFailed)
}

/// 拿令牌没走通，交给链路当一次网络失败：网关根本没答复时就是 `NETWORK_UNREACHABLE`，
/// 网关答了（服务不可用等）保留原样。
fn as_network_failure(error: FlareError) -> FlareError {
    match error.code() {
        Some(ErrorCode::ServiceUnavailable | ErrorCode::NetworkUnreachable) => error,
        _ => FlareError::localized(ErrorCode::NetworkUnreachable, error.to_string()),
    }
}

#[cfg(test)]
mod token_refresh_tests {
    use super::*;
    use crate::shared::util::{CoreTokenConfig, generate_core_token};

    fn token(ttl_secs: u64) -> String {
        generate_core_token(&CoreTokenConfig {
            secret: "a-strong-shared-secret-with-more-than-32-bytes!".into(),
            issuer: "flare-im-core".into(),
            user_id: "u".into(),
            ttl_secs,
            device_id: None,
            tenant_id: None,
        })
        .unwrap()
    }

    #[test]
    fn needs_refresh_only_inside_the_lead_window() {
        let clock = crate::kernel::ServerClock::new();
        // generate_core_token 把 ttl 抬到至少 60s：60s 内到期 + 30s 提前量 = 不该刷；提前量 120s 就该刷。
        assert!(!token_needs_refresh(&token(60), 30, &clock));
        assert!(token_needs_refresh(&token(60), 120, &clock));
        assert!(
            !token_needs_refresh("garbage", 120, &clock),
            "读不出 exp 不能误判成要刷"
        );
    }

    /// A token the server issued with `exp` fifteen minutes after its own now.
    fn server_issued_token(clock: &crate::kernel::ServerClock, ttl_secs: u64) -> String {
        use base64::Engine as _;
        let payload = serde_json::json!({ "sub": "u", "exp": clock.now_secs() + ttl_secs });
        let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        format!(
            "{}.{}.sig",
            encode(br#"{"alg":"HS256","typ":"JWT"}"#),
            encode(payload.to_string().as_bytes())
        )
    }

    /// The token's `exp` is the server's clock. A device clock twenty minutes
    /// ahead of the server must not see a fresh fifteen-minute token as
    /// expired (refreshing it again every few seconds), and one twenty
    /// minutes behind must still refresh it on time.
    #[test]
    fn token_expiry_is_judged_on_the_server_clock() {
        for ahead_secs in [20 * 60_i64, -20 * 60] {
            let clock = crate::kernel::server_clock::test_support::clock_with_device_ahead_by(
                ahead_secs * 1000,
            );
            let token = server_issued_token(&clock, 15 * 60);
            assert!(
                !token_needs_refresh(&token, 30, &clock),
                "fresh token, device ahead by {ahead_secs}s"
            );
            let wait = token_refresh_wait_secs(&token, 60, &clock);
            assert!(
                (15 * 60 - 60 - 2..=15 * 60 - 60).contains(&wait),
                "refresh one lead before exp, device ahead by {ahead_secs}s: waits {wait}s"
            );
        }
    }
}
