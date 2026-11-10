use std::sync::Arc;

use serde_json::Value;

use crate::FlareError;
use crate::application::notification::NotificationHandlerRegistry;
use crate::client::api::{
    CapabilityApi, CapabilityDispatchResult, ConversationApi, MediaApi, MessageApi,
    MessageBuildApi, PresenceApi, ViewApi,
};
use crate::extension::capability::SdkCapabilityRegistry;
use crate::infrastructure::transport::http::HttpRequestContext;
use crate::kernel::SdkState;
use crate::kernel::event::EventBus;
use crate::shared::error::{ErrorCode, Result};

use super::IMClient;

impl IMClient {
    /// 读取 SDK 当前连接状态快照（FSM 驱动）。
    ///
    /// 锁竞争或引擎暂时被连接/重连流程取出时，返回句柄级状态快照。
    pub fn state(&self) -> SdkState {
        match self.inner.try_read() {
            Ok(g) => match g.engine.as_ref() {
                Some(engine) => {
                    let state = engine.state();
                    self.store_state_snapshot(state);
                    state
                }
                None => self.load_state_snapshot(),
            },
            Err(_) => self.load_state_snapshot(),
        }
    }

    /// 共享 HTTP 鉴权上下文（媒体 / 能力 / Social Gateway 可共用）。
    pub fn http_request_context(&self) -> Option<Arc<HttpRequestContext>> {
        self.inner
            .try_read()
            .ok()
            .and_then(|g| g.http_request_context.clone())
    }

    /// 当前 IM 连接使用的 access token（与 WebSocket 鉴权一致）。
    pub async fn access_token(&self) -> Option<String> {
        let g = self.inner.read().await;
        g.connect_token.clone().filter(|t| !t.trim().is_empty())
    }

    /// Refresh Core HTTP (media/presence/capabilities) with the IM bearer.
    /// A host-owned BFF bearer has a separate purpose and must remain untouched.
    pub async fn sync_core_http_context(&self, tenant_id: Option<&str>) -> Result<()> {
        let g = self.inner.read().await;
        if !self.inner_session_active(&g)
            || !matches!(
                self.state(),
                SdkState::Connected | SdkState::Ready | SdkState::Reconnecting
            )
        {
            return Err(Self::not_connected());
        }
        let user_id = g
            .current_user_id
            .clone()
            .filter(|u| !u.is_empty())
            .ok_or_else(Self::not_connected)?;
        let token = g
            .connect_token
            .clone()
            .filter(|t| !t.trim().is_empty())
            .ok_or_else(Self::not_connected)?;
        let tenant = tenant_id
            .map(str::to_string)
            .unwrap_or_else(|| Self::resolve_tenant_id(&g));
        let tenant = crate::shared::util::normalize_tenant_id(tenant);
        let http = g.http_request_context.clone().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                "http_request_context not configured",
            )
        })?;
        drop(g);
        http.set_auth_context(token, None).await;
        http.ensure_identity(&user_id, &tenant).await;
        Ok(())
    }

    /// 安装宿主托管令牌的续期回调：重连被网关按鉴权拒绝时，核心向宿主要一枚新的连接令牌。
    ///
    /// SDK 托管形态（配了 `auth`）不需要它，核心自己会向 Core API 换。
    /// 登录时重建会话也会保留它；传 `None` 卸下。
    pub async fn set_connect_token_refresher(
        &self,
        refresher: Option<Arc<dyn crate::client::token_provider::ConnectTokenRefresher>>,
    ) {
        let http = {
            let mut g = self.inner.write().await;
            g.connect_token_refresher = refresher.clone();
            g.http_request_context.clone()
        };
        if let Some(http) = http {
            let renewal = refresher.map(|r| {
                Arc::new(crate::client::token_provider::ConnectTokenRenewal(r))
                    as Arc<dyn crate::infrastructure::transport::http::CoreBearerRenewal>
            });
            http.set_core_bearer_renewal(renewal).await;
        }
    }

    /// 更新 access token 并同步到共享 HTTP 上下文（token 刷新后调用）。
    pub async fn update_access_token(
        &self,
        access_token: impl Into<String>,
        tenant_id: Option<&str>,
    ) -> Result<()> {
        let token = access_token.into();
        if token.trim().is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "access_token must not be empty",
            ));
        }
        {
            let mut g = self.inner.write().await;
            g.connect_token = Some(token.clone());
        }
        self.sync_core_http_context(tenant_id).await?;
        self.attach_push_session().await;
        Ok(())
    }

    /// 获取消息 API 门面；未连接时返回 `NotConnected`。
    pub fn message(&self) -> Result<MessageApi> {
        Ok(self.connected_apis_sync()?.message_api)
    }

    /// 获取消息构建 API（负责组装 `IMMessage`）；未连接时返回 `NotConnected`。
    pub fn message_build(&self) -> Result<Arc<MessageBuildApi>> {
        Ok(self.connected_apis_sync()?.message_build_api)
    }

    /// 获取会话 API 门面；未连接时返回 `NotConnected`。
    pub fn conversation(&self) -> Result<ConversationApi> {
        Ok(self.connected_apis_sync()?.conversation_api)
    }

    /// 获取 core observable view API；未连接时返回 `NotConnected`。
    pub fn view(&self) -> Result<Arc<ViewApi>> {
        Ok(self.connected_apis_sync()?.view_api)
    }

    /// 获取媒体 API 门面（上传/删除）。
    pub fn media(&self) -> Result<Arc<MediaApi>> {
        Ok(self.connected_apis_sync()?.media_api)
    }

    /// 获取能力插件 API（付费模块入口，包含 RTC/SFU 能力）。
    pub fn capability(&self) -> Result<Arc<CapabilityApi>> {
        Ok(self.connected_apis_sync()?.capability_api)
    }

    /// 获取用户在线状态 API。
    pub fn presence(&self) -> Result<Arc<PresenceApi>> {
        Ok(self.connected_apis_sync()?.presence_api)
    }

    /// 获取 SDK 能力插件注册表（支持多付费插件扩展）。
    pub fn capability_registry(&self) -> Result<Arc<SdkCapabilityRegistry>> {
        Ok(self.connected_apis_sync()?.capability_registry)
    }

    /// 经注册表派发扩展能力（等价于 `capability_registry()?.invoke(...).await`）。
    pub async fn invoke_capability(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        self.capability_registry()?
            .invoke(capability_id, payload, conversation_id, tenant_id)
            .await
    }

    /// 获取 SDK 事件总线（用于原始事件订阅或桥接到宿主事件系统）。
    pub async fn bus(&self) -> Result<EventBus> {
        self.with_engine_async(|e| e.bus().clone()).await
    }

    pub async fn notification_handlers(&self) -> Result<Arc<NotificationHandlerRegistry>> {
        let g = self.read_inner_async().await?;
        g.notification_registry
            .clone()
            .ok_or_else(|| FlareError::localized(ErrorCode::InternalError, "IMClient not built"))
    }

    /// 同步获取事件总线：仅用于非 async 上下文；热路径请用 [`Self::bus`].
    pub fn bus_sync(&self) -> Result<EventBus> {
        self.with_engine(|e| e.bus().clone())
    }
}
