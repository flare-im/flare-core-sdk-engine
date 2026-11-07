use super::IMClient;
use crate::{
    client::push_lifecycle::{PushLifecycleStatus, PushTokenInput},
    shared::error::Result,
};
impl IMClient {
    pub(super) async fn configure_push_transport(&self) {
        use crate::client::lifecycle::merge_sdk_config_onto;
        use crate::infrastructure::transport::http::router::HttpRouter;
        use crate::infrastructure::transport::http::{HttpClient, HttpRequestContext};
        let configured = {
            let g = self.inner.read().await;
            let base = g.configured_config.clone().unwrap_or_default();
            let config = merge_sdk_config_onto(base, g.sdk_config.as_ref());
            match (
                g.access.clone(),
                config.device_id.clone(),
                g.data_root.clone(),
            ) {
                (Some(access), Some(device), Some(root))
                    if access.config().is_some() && !device.is_empty() =>
                {
                    // 推送撤销走 Core API，与其它 REST 一样按接入点路由；不带任何会话凭据。
                    let router = HttpRouter::new(access, flare_access::ServiceClass::CoreApi);
                    let context = std::sync::Arc::new(HttpRequestContext::new());
                    #[cfg(not(target_arch = "wasm32"))]
                    let http = HttpClient::routed(router, context, config.core_tls_config());
                    #[cfg(target_arch = "wasm32")]
                    let http = HttpClient::routed(router, context);
                    Some((http, root, device))
                }
                _ => None,
            }
        };
        if let Some((http, root, device)) = configured {
            // No bearer is required or retained for pending revocation recovery.
            if self
                .push_lifecycle
                .configure(http, root, &device)
                .await
                .is_err()
            {
                tracing::warn!("push recovery storage unavailable; existing intent retained");
            }
        }
    }
    /// OS callback, not an IM credential. May arrive before login; permission defaults false.
    pub async fn update_push_token(&self, input: PushTokenInput) -> Result<PushLifecycleStatus> {
        self.push_lifecycle.update(input)?;
        self.reconcile_push().await
    }
    pub async fn clear_push_token(&self, provider: &str) -> Result<PushLifecycleStatus> {
        self.push_lifecycle.clear(provider)?;
        self.reconcile_push().await
    }
    pub async fn set_push_permission(&self, allowed: bool) -> Result<PushLifecycleStatus> {
        self.push_lifecycle.permission(allowed);
        self.reconcile_push().await
    }
    pub fn push_status(&self) -> PushLifecycleStatus {
        self.push_lifecycle.status()
    }
    /// Explicit retry/read-back on foreground/network recovery; never blindly replays PUT.
    pub async fn reconcile_push(&self) -> Result<PushLifecycleStatus> {
        self.configure_push_transport().await;
        Ok(self.push_lifecycle.reconcile().await)
    }
    pub(super) async fn attach_push_session(&self) {
        self.push_lifecycle.detach();
        let capture = {
            let g = self.inner.read().await;
            let stable_device = g
                .sdk_config
                .as_ref()
                .and_then(|v| v.device_id.as_deref())
                .or_else(|| {
                    g.configured_config
                        .as_ref()
                        .and_then(|v| v.device_id.as_deref())
                })
                .filter(|v| !v.is_empty());
            match (
                &g.presence_api,
                &g.current_user_id,
                &g.connect_token,
                &g.data_root,
            ) {
                (Some(api), Some(user), Some(token), Some(root))
                    if stable_device == Some(api.push_device_id().as_str()) =>
                {
                    Some((
                        g.session_generation,
                        user.clone(),
                        Self::resolve_tenant_id(&g),
                        api.push_device_id(),
                        api.push_http(),
                        token.clone(),
                        root.clone(),
                        g.engine
                            .as_ref()
                            .map(|engine| engine.server_clock().clone())
                            .unwrap_or_default(),
                    ))
                }
                _ => None,
            }
        };
        if let Some((epoch, user, app, device, http, token, root, server_clock)) = capture {
            if self
                .push_lifecycle
                .attach(crate::client::push_lifecycle::AttachPushSession {
                    epoch,
                    generation: self.session_generation_snapshot.clone(),
                    user,
                    app,
                    device,
                    http,
                    token,
                    root,
                    server_clock,
                })
                .await
                .is_err()
            {
                tracing::warn!(
                    "push lifecycle session or storage unavailable; no binding dispatched"
                );
                self.reconcile_push_in_background();
                return;
            }
            self.reconcile_push_in_background();
        }
    }
    pub(super) fn reconcile_push_in_background(&self) {
        let weak = self.downgrade();
        super::spawn_im_background(async move {
            if let Some(client) = weak.upgrade() {
                let _ = client.reconcile_push().await;
            }
        });
    }
}
