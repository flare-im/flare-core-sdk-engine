//! Tauri `State`：持有唯一 [`IMClient`] 与登录后的 [`ConnectedApis`] 快照。
//!
//! 热路径优先读 `session` 缓存；未安装或代际变化时回退 `IMClient::connected_apis()` 并自动刷新，
//! 避免 `sdk_login` 返回前同步事件触发 IPC 时报「未登录或会话未就绪」。

use std::sync::{Arc, Mutex};

use flare_im_core_sdk::Result;
use flare_im_core_sdk::client::api::{
    CapabilityApi, ConversationApi, MediaApi, MessageApi, MessageBuildApi, PresenceApi,
};
use flare_im_core_sdk::client::{ConnectedApis, HostTokenBroker, IMClient, SdkConfigOverlay};
use flare_im_core_sdk::prelude::StoreProvider;
use flare_im_core_sdk_bindings_runtime::{InvokeSession, SessionSlot, SessionTaskSlot};

/// 由 `tauri::Builder::manage(SdkState::new())` 注入。
pub struct SdkState {
    client: IMClient,
    session: SessionSlot,
    event_bridge: SessionTaskSlot,
    /// 前端装了连接令牌续期回调时的配对表（见 `sdk_set_connect_token_refresher`）。
    token_broker: Mutex<Option<HostTokenBroker>>,
}

impl SdkState {
    pub fn new() -> Self {
        Self {
            client: IMClient::new(),
            session: SessionSlot::default(),
            event_bridge: SessionTaskSlot::default(),
            token_broker: Mutex::new(None),
        }
    }

    pub fn set_token_broker(&self, broker: Option<HostTokenBroker>) {
        if let Ok(mut slot) = self.token_broker.lock() {
            *slot = broker;
        }
    }

    pub fn token_broker(&self) -> Option<HostTokenBroker> {
        self.token_broker.lock().ok().and_then(|slot| slot.clone())
    }

    /// O(1)：`IMClient` 为 `Arc` 浅拷贝。
    #[inline]
    pub fn client(&self) -> IMClient {
        self.client.clone()
    }

    pub async fn set_config(
        &self,
        environment: Option<String>,
        sdk_config: Option<SdkConfigOverlay>,
    ) -> Result<()> {
        self.client.init(environment, sdk_config).await
    }

    pub async fn config_snapshot(&self) -> (Option<String>, Option<SdkConfigOverlay>) {
        self.client.config_snapshot().await
    }

    pub async fn install_session(&self, apis: ConnectedApis) {
        self.session.install(&self.client, apis).await;
    }

    pub async fn clear_session(&self) {
        self.session.clear().await;
    }

    pub fn event_bridge(&self) -> SessionTaskSlot {
        self.event_bridge.clone()
    }

    pub fn clear_event_bridge(&self) {
        self.event_bridge.clear();
    }

    pub async fn logout(&self) -> Result<()> {
        self.clear_event_bridge();
        self.clear_session().await;
        self.client.logout().await
    }

    pub async fn message_api(&self) -> Result<MessageApi> {
        self.session.message_api(&self.client).await
    }

    pub async fn conversation_api(&self) -> Result<ConversationApi> {
        self.session.conversation_api(&self.client).await
    }

    pub async fn media_api(&self) -> Result<Arc<MediaApi>> {
        self.session.media_api(&self.client).await
    }

    pub async fn capability_api(&self) -> Result<Arc<CapabilityApi>> {
        self.session.capability_api(&self.client).await
    }

    pub async fn presence_api(&self) -> Result<Arc<PresenceApi>> {
        self.session.presence_api(&self.client).await
    }

    pub async fn message_build_api(&self) -> Result<Arc<MessageBuildApi>> {
        self.session.message_build_api(&self.client).await
    }

    pub async fn stores(&self) -> Result<StoreProvider> {
        self.client.stores_async().await
    }
}

impl InvokeSession for SdkState {
    fn client(&self) -> IMClient {
        self.client.clone()
    }

    async fn message_api(&self) -> Result<MessageApi> {
        SdkState::message_api(self).await
    }

    async fn message_build_api(&self) -> Result<Arc<MessageBuildApi>> {
        SdkState::message_build_api(self).await
    }

    async fn conversation_api(&self) -> Result<ConversationApi> {
        SdkState::conversation_api(self).await
    }

    async fn media_api(&self) -> Result<Arc<MediaApi>> {
        SdkState::media_api(self).await
    }

    async fn capability_api(&self) -> Result<Arc<CapabilityApi>> {
        SdkState::capability_api(self).await
    }

    async fn after_disconnect(&self) {
        SdkState::clear_session(self).await;
    }
}

impl Default for SdkState {
    fn default() -> Self {
        Self::new()
    }
}
