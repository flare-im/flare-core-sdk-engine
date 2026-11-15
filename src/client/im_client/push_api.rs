use super::{IMClient, IMClientInner};
use crate::{
    client::push_inbox::{
        HandleRemoteMessageRequest, InboxSession, NotificationTapResult, RemoteMessageOutcome,
        ResolveNotificationTapRequest,
    },
    client::push_lifecycle::{PushLifecycleStatus, PushTokenInput},
    shared::error::Result,
};
impl IMClient {
    /// 远程推送的无界面入口：平台推送包收到推送（APNs、FCM、个推）时调用，150 ms 内答复该怎么展示，
    /// 不等网络。按合并键与 seq 去重（链路上已经提示过的不再响），本地已读覆盖了的改为清除；
    /// `budgetMs` 大于零时顺带唤醒链路做一次增量同步，答复不等它。
    pub async fn handle_remote_message(
        &self,
        request: HandleRemoteMessageRequest,
    ) -> Result<RemoteMessageOutcome> {
        self.push_inbox
            .handle(request, self.is_app_foreground_snapshot())
            .await
    }

    /// 点开的通知去哪个会话、哪条消息；载荷不是当前账号的时候什么都不给。
    pub fn resolve_notification_tap(
        &self,
        request: ResolveNotificationTapRequest,
    ) -> Result<NotificationTapResult> {
        self.push_inbox.resolve_tap(&request)
    }

    /// 把这一代会话的事实交给推送入口：用户、租户、本地会话库、提示台账与链路。登录准备好、建连
    /// 开始时各调一次（用户、租户、引擎都可能刚换）；登出、断开时由 `clear_session_snapshot` 卸下。
    pub(super) fn attach_push_inbox(&self, g: &IMClientInner) {
        let (Some(user_id), Some(engine)) = (
            g.current_user_id
                .as_ref()
                .filter(|user_id| !user_id.trim().is_empty()),
            g.engine.as_ref(),
        ) else {
            self.push_inbox.detach();
            return;
        };
        let tenant_id = Self::resolve_tenant_id(g);
        // 链路确认的消息要按租户算合并键，才能和推送落在同一处。
        engine.alert_ledger().scope(&tenant_id);
        // 通知快照按这个账号写到宿主给的目录（目录跨登录保留，在客户端那份宿主事实里）。
        let notify = engine.notify_center();
        notify.adopt_host(self.notification_host.clone());
        notify.attach_session(user_id, &tenant_id);
        // 到达回执的队列在数据根下，只记、只报这个账号的。
        if let Some(root) = g.data_root.clone() {
            self.push_inbox.receipts().set_root(root);
        }
        self.push_inbox.receipts().set_owner(user_id, &tenant_id);
        self.push_inbox.attach(InboxSession {
            user_id: user_id.clone(),
            tenant_id,
            conversations: engine.stores().conversations.clone(),
            alerts: engine.alert_ledger().clone(),
            link: engine.link().clone(),
        });
    }
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
        let app_version = input.app_version.clone();
        self.push_lifecycle.update(input)?;
        // 到达回执按最近一次登记的应用版本报。
        self.push_inbox.receipts().set_app_version(app_version);
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
            // 到达回执用同一个设备报，凭据每批从会话共用的请求上下文现取（刷新、续期之后的那枚），
            // 只在这一代会话还在时用，不会借用下一个账号的。浏览器没有系统推送，也就没有回执。
            #[cfg(not(target_arch = "wasm32"))]
            {
                self.push_inbox
                    .receipts()
                    .attach(crate::client::push_receipts::ReceiptSender {
                        device_id: device.clone(),
                        http: http.clone(),
                        user_id: user.clone(),
                        tenant_id: app.clone(),
                        epoch,
                        generation: self.session_generation_snapshot.clone(),
                    });
                // 会话装上了：读进通知扩展记下的回执，把排着的报上去。
                self.sync_push_receipts_in_background();
            }
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
    /// 读进通知扩展记下的回执，再把排着的回执报上去（会话装上、回到前台时）。不挡调用方。
    pub(super) fn sync_push_receipts_in_background(&self) {
        let client = self.clone();
        super::spawn_im_background(async move {
            client.sync_push_receipts().await;
        });
    }

    /// 读进通知扩展记下的回执，再把排着的回执报上去。没有会话、没有上报通道时只读不报。
    pub(super) async fn sync_push_receipts(&self) {
        let receipts = self.push_inbox.receipts().clone();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(directory) = self.notification_host.snapshot_directory() {
            receipts.ingest(&directory);
        }
        receipts.flush().await;
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
