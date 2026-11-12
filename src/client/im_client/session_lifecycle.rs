use std::sync::Arc;
use std::time::Duration;

use flare_core::common::HeartbeatAppState;

use crate::FlareError;
use crate::client::builder::IMClientBuilder;
use crate::client::connected_apis::ConnectedApis;
use crate::client::lifecycle::{
    LoginDbKind, merge_sdk_config, resolve_connect_token, resolve_sdk_data_root,
};
use crate::kernel::SdkState;
use crate::shared::error::{ErrorCode, Result};
use crate::shared::util::{
    CoreTokenConfig, generate_core_token as util_generate_core_token, timeout,
};

use super::{IMClient, NetworkChangeEvent, spawn_im_background};

impl IMClient {
    /// 是否处于“已登录 SDK 会话”状态。
    ///
    /// 该值表示本地有当前用户上下文，不等同于底层链路一定可发包；
    /// 连接细粒度状态请使用 [`Self::state`] 或事件回调。
    pub async fn is_connected(&self) -> bool {
        self.inner.read().await.current_user_id.is_some()
    }

    /// 应用前后台状态（`lifecycle.set_app_state`）。进入后台拉长心跳、同步降配、健康簿落盘；
    /// 回到前台恢复心跳并立即验活当前连接，停在退避里的链路立即重试，同时做一次前台收敛。
    ///
    /// 进后台即托管的平台（iOS）上，进后台的这一次调用等托管交接收尾才返回（交接每一步都有时限，
    /// 合计约 3.5 秒）：平台包据此恰好持有这么久的后台执行。其它平台立即返回。
    pub async fn set_app_state(&self, state: HeartbeatAppState) -> Result<()> {
        self.store_heartbeat_app_state_snapshot(state);
        let g = self.read_inner_async().await?;
        let link = match g.engine.as_ref() {
            Some(engine) => {
                engine.set_app_state(state).await?;
                // I9：同步收敛跟随前后台降配（后台批/页减半，让电让带宽）。
                engine
                    .sync_manager()
                    .attention()
                    .set_app_in_background(matches!(state, HeartbeatAppState::Background));
                Some(engine.link().clone())
            }
            None => None,
        };
        drop(g);
        if matches!(state, HeartbeatAppState::Background)
            && let Some(link) = link.as_ref()
        {
            // 在锁外等：交接要几秒，期间登出、查询、推送入口都不能被挡住。
            link.parking_settled().await;
        }
        if matches!(state, HeartbeatAppState::Foreground) {
            self.reconcile_push_in_background();
            // 后台期间通知扩展记下的到达回执读进来报上去。
            self.sync_push_receipts_in_background();
            if let Some(update) = self.update_runtime().await {
                update.foregrounded();
            }
            let client = self.clone();
            let generation = self.load_session_generation_snapshot();
            spawn_im_background(async move {
                if client.is_generation_current(generation).await {
                    let _ = client.sync_foreground_convergence_silent().await;
                }
            });
        } else if let Some(update) = self.update_runtime().await {
            update.backgrounded();
        }
        Ok(())
    }

    /// 更新 NAT 空闲超时探测结果。传入 `None` 表示清除探测值。
    pub async fn set_heartbeat_nat_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        let g = self.read_inner_async().await?;
        if let Some(engine) = g.engine.as_ref() {
            engine.set_heartbeat_nat_timeout(timeout).await?;
        }
        Ok(())
    }

    /// 当前实际心跳间隔；未连接时返回 `None`。
    pub async fn heartbeat_effective_interval(&self) -> Result<Option<Duration>> {
        let g = self.read_inner_async().await?;
        match g.engine.as_ref() {
            Some(engine) => Ok(engine.heartbeat_effective_interval().await),
            None => Ok(None),
        }
    }

    /// 平台网络状态变化通知（`connection.notify_network_change`）。
    ///
    /// `interface` + `fingerprintHint` 决定网络指纹：健康记录按网络分开（同一接入点 Wi-Fi 下好、某运营商
    /// 蜂窝下差是常态）。不可用时链路挂起、发件箱计时冻结；恢复时对仍在的连接立即验活（旧路径上的连接
    /// 常被中间设备静默丢弃），并按新网络的健康记录决定是否换接入点。怎么恢复由链路状态机决定，
    /// 这里只转告。
    #[tracing::instrument(skip(self, event), fields(available = event.available, interface = event.interface.as_deref().unwrap_or("unknown")))]
    pub async fn notify_network_change(&self, event: NetworkChangeEvent) -> Result<()> {
        // 记下完整输入：登录前的首报（平台之后对相同状态去重）要带进之后新建的运行时与链路。
        self.inner.write().await.last_network = Some(event.clone());
        let changed = match self.access_runtime().await {
            Some(access) => access.set_network(
                event.interface.as_deref(),
                event.fingerprint_hint.as_deref(),
            ),
            None => false,
        };
        tracing::info!(
            interface = event.interface.as_deref().unwrap_or("unknown"),
            expensive = event.expensive,
            metered = event.metered,
            reason = event.reason.as_deref().unwrap_or("network_change"),
            fingerprint_changed = changed,
            "network change reported"
        );
        let link = {
            let g = self.inner.read().await;
            g.engine.as_ref().map(|engine| engine.link().clone())
        };
        if let Some(link) = link {
            link.network_changed(event.available, changed).await;
        }
        if event.available {
            self.reconcile_push_in_background();
        }
        if let Some(update) = self.update_runtime().await {
            update.network_changed(event.available, event.is_metered());
        }
        Ok(())
    }

    /// 与 [`Self::state`] / 传输层 `Ready` 不同：未登录时引擎可能仍存在，此时本方法为 `false`。
    pub fn session_active_sync(&self) -> bool {
        self.active_connected_apis_snapshot().is_some()
    }

    /// 返回当前登录用户 ID；未登录时返回 `None`。
    pub async fn current_user_id(&self) -> Option<String> {
        self.inner.read().await.current_user_id.clone()
    }

    /// 会话代际：登录/重连/登出时递增，供 Tauri `SdkState` 判断 API 快照是否过期。
    pub async fn session_generation(&self) -> u64 {
        self.load_session_generation_snapshot()
    }

    /// 同步读取当前会话代际快照，供绑定层命中缓存时避开 async 锁。
    pub fn session_generation_snapshot(&self) -> u64 {
        self.load_session_generation_snapshot()
    }

    /// 生成 Flare IM Core 接入 JWT token。
    ///
    /// 调用方必须显式传入签名 secret、issuer、user_id 与 ttl，避免默认测试密钥进入真实集成。
    pub fn generate_core_token(config: CoreTokenConfig) -> Result<String> {
        util_generate_core_token(&config)
    }

    /// 主动退出登录并清空 SDK 会话上下文。
    ///
    /// 该操作会断开连接、推进会话代际并清理 `message/conversation` API 句柄；
    /// 调用后需重新 `login` 或 `connect` 才可继续收发消息。
    pub async fn logout(&self) -> Result<()> {
        self.clear_session_snapshot();
        let lifecycle = self.extension_lifecycle_snapshot(None).await;
        let (engine, presence_api, presence_user_id, http_request_context, vault) = {
            let mut g = self.inner.write().await;
            let presence_api = g.presence_api.clone();
            let presence_user_id = g.current_user_id.clone();
            g.session_generation = g.session_generation.wrapping_add(1);
            self.store_session_generation_snapshot(g.session_generation);
            g.current_user_id = None;
            g.connect_token = None;
            // 登出即放弃这个账号的长效凭据：内存里的与宿主安全存储里的都不留。
            g.refresh_token = None;
            let vault = g.refresh_token_vault.take();
            g.message_api = None;
            g.media_api = None;
            g.capability_api = None;
            g.presence_api = None;
            g.capability_registry = None;
            g.message_build_api = None;
            g.conversation_api = None;
            let engine = g.engine.take();
            let http_request_context = g.http_request_context.clone();
            (
                engine,
                presence_api,
                presence_user_id,
                http_request_context,
                vault,
            )
        };
        self.clear_session_snapshot();
        self.store_state_snapshot(SdkState::Disconnected);
        self.reconcile_push_in_background();
        if let (Some(vault), Some(user_id)) = (vault.as_ref(), presence_user_id.as_deref()) {
            vault.clear(user_id).await;
        }
        if let Some(engine) = engine.as_ref() {
            engine.deactivate_local_session().await;
        }
        if let Some(api) = presence_api.as_ref() {
            let logout_result = if let Some(user_id) = presence_user_id
                .as_deref()
                .filter(|id| !id.trim().is_empty())
            {
                api.logout_user_device_presence(user_id).await
            } else {
                api.logout_current_device_presence().await
            };
            if let Err(err) = logout_result {
                tracing::warn!(%err, "active presence logout failed; falling back to transport disconnect");
            }
        }
        if let Some(context) = http_request_context.as_ref() {
            context.set_auth_context(String::new(), None).await;
            context.clear_gateway_context().await;
        }
        if let Some(mut e) = engine {
            e.disconnect().await?;
        }
        Self::notify_extension_disconnect_snapshot(&lifecycle).await;
        Self::notify_extension_logout_snapshot(&lifecycle).await;
        Ok(())
    }

    /// 反初始化 SDK 运行态。
    ///
    /// 与 [`Self::logout`] 的区别：`logout` 只结束当前用户会话并保留 `init` 配置；
    /// `uninit` 会在结束会话后清空环境、配置、数据根与宿主 HTTP 上下文，允许同一 client
    /// 以新的 `SdkConfigOverlay` 重新 [`Self::init`]。
    pub async fn uninit(&self) -> Result<()> {
        let logout_result = self.logout().await;
        {
            let mut g = self.inner.write().await;
            g.environment = None;
            g.sdk_config = None;
            g.data_root = None;
            g.http_request_context = None;
            g.notification_registry = None;
        }
        logout_result
    }

    /// 登录前清理旧会话：presence / disconnect 带短超时，避免 Consul 或 gRPC 阻塞 `sdk_login`。
    pub(super) async fn logout_for_login(&self) -> Result<()> {
        const PRESENCE_LOGOUT_TIMEOUT: Duration = Duration::from_secs(2);
        const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(3);

        self.clear_session_snapshot();
        let lifecycle = self.extension_lifecycle_snapshot(None).await;
        let (engine, presence_api, presence_user_id, http_request_context) = {
            let mut g = self.inner.write().await;
            let presence_api = g.presence_api.clone();
            let presence_user_id = g.current_user_id.clone();
            g.session_generation = g.session_generation.wrapping_add(1);
            self.store_session_generation_snapshot(g.session_generation);
            g.current_user_id = None;
            g.connect_token = None;
            g.message_api = None;
            g.media_api = None;
            g.capability_api = None;
            g.presence_api = None;
            g.capability_registry = None;
            g.message_build_api = None;
            g.conversation_api = None;
            let engine = g.engine.take();
            let http_request_context = g.http_request_context.clone();
            (engine, presence_api, presence_user_id, http_request_context)
        };
        self.clear_session_snapshot();
        self.store_state_snapshot(SdkState::Disconnected);
        self.reconcile_push_in_background();
        if let Some(engine) = engine.as_ref() {
            engine.deactivate_local_session().await;
        }

        if let Some(api) = presence_api.as_ref() {
            let logout_future = async {
                if let Some(user_id) = presence_user_id
                    .as_deref()
                    .filter(|id| !id.trim().is_empty())
                {
                    api.logout_user_device_presence(user_id).await
                } else {
                    api.logout_current_device_presence().await
                }
            };
            match timeout(PRESENCE_LOGOUT_TIMEOUT, logout_future).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::warn!(%err, "presence logout before login failed");
                }
                Err(_) => tracing::warn!("presence logout before login timed out"),
            }
        }
        if let Some(context) = http_request_context.as_ref() {
            context.set_auth_context(String::new(), None).await;
            context.clear_gateway_context().await;
        }
        if let Some(mut e) = engine {
            match timeout(DISCONNECT_TIMEOUT, e.disconnect()).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => tracing::warn!(%err, "disconnect before login failed"),
                Err(_) => tracing::warn!("disconnect before login timed out"),
            }
        }
        Self::notify_extension_disconnect_snapshot(&lifecycle).await;
        Self::notify_extension_logout_snapshot(&lifecycle).await;
        Ok(())
    }

    /// 登录入口：按用户初始化存储、建立连接并切换为新会话。
    ///
    /// - 会先执行一次 [`Self::logout`]，确保会话隔离；
    /// - `before_connect` 可在建连前注册事件监听；
    /// - 被踢下线 / token 过期将由内部 watcher 自动终止会话（等价登出）。
    ///
    /// 准备登录会话：打开 per-user 本地库、装配引擎，但**不连网**。
    ///
    /// 配合 [`Self::connect`] 实现「初始化前置、登录只做网络」：App 启动即可对
    /// 「上次登录用户」调用 `prepare`，把开库 / 迁移 / 建引擎 / 待发队列恢复等本地重活
    /// 移出登录关键路径；待拿到 token 再 [`Self::connect`]，登录仅剩连接 + 首次同步。
    ///
    /// 幂等：对任意 user 重复调用都会先清理旧会话、丢弃未连接引擎，再按新 user 重建本地栈。
    #[tracing::instrument(skip(self, db), fields(user_id = %user_id))]
    pub async fn prepare(&self, user_id: &str, db: LoginDbKind) -> Result<()> {
        let snap = {
            let g = self.inner.read().await;
            (
                g.environment.clone(),
                g.sdk_config.clone(),
                g.data_root.clone(),
                g.http_request_context.clone(),
                g.extension_components.clone(),
                g.access.clone(),
                g.connect_token_refresher.clone(),
                g.configured_config.clone(),
                // 上一个会话属于谁：决定发件箱是原样恢复（同一用户）还是清空（换了用户）。
                g.current_user_id
                    .clone()
                    .or_else(|| g.last_session_user.clone()),
                g.last_network.clone(),
                // 宿主注入的登录前全局存储（wasm 的 IndexedDB）：属于宿主，跨会话保留，之后的 init 仍用它。
                g.access_store.clone(),
                // 上一个会话的刷新令牌保管处：换了账号时用它删掉上一个账号的刷新令牌。
                g.refresh_token_vault.clone(),
            )
        };
        let vault = refresh_token_vault_for(&db, snap.1.as_ref());
        self.logout_for_login().await?;
        let data_root = match snap.2.clone() {
            Some(path) => path,
            None => {
                let path =
                    resolve_sdk_data_root(snap.1.as_ref().and_then(|cfg| cfg.data_url.as_deref()))?;
                #[cfg(not(target_arch = "wasm32"))]
                std::fs::create_dir_all(&path).map_err(|e| {
                    FlareError::localized(
                        ErrorCode::InvalidParameter,
                        format!("sdk data root create_dir_all failed: {}", e),
                    )
                })?;
                path
            }
        };
        #[cfg(feature = "lifecycle-sqlite")]
        let stores = match db {
            #[cfg(feature = "lifecycle-sqlite")]
            LoginDbKind::Sqlite => {
                crate::shared::util::sqlite_store::open_sqlite_store_for_user(&data_root, user_id)
                    .await?
            }
            #[cfg(feature = "lifecycle-sqlite")]
            LoginDbKind::EncryptedSqlite {
                key_store,
                key_namespace,
                tenant_id,
                key_name,
            } => {
                let namespace = key_namespace.as_deref().unwrap_or("flare-im-core-sdk");
                let tenant_id = tenant_id
                    .as_deref()
                    .or_else(|| snap.1.as_ref().and_then(|cfg| cfg.tenant_id.as_deref()))
                    .unwrap_or(crate::shared::util::DEFAULT_TENANT_ID);
                crate::shared::util::sqlite_store::open_sqlite_store_for_user_with_secure_key_store(
                    &data_root,
                    user_id,
                    namespace,
                    tenant_id,
                    key_name.as_deref(),
                    key_store.as_ref(),
                )
                .await?
            }
            LoginDbKind::IndexedDb(stores) => stores,
        };
        #[cfg(not(feature = "lifecycle-sqlite"))]
        let LoginDbKind::IndexedDb(stores) = db;
        // 以构建期配置为底叠 overlay（只拿默认值作底会把宿主在构建期配好的项在登录那一刻顶掉）。
        let config = match snap.7 {
            Some(base) => crate::client::lifecycle::merge_sdk_config_onto(base, snap.1.as_ref()),
            None => merge_sdk_config(snap.1.as_ref()),
        };
        let mut child_builder = IMClientBuilder::new().config(config).stores(stores);
        if let Some(ctx) = snap.3.clone() {
            child_builder = child_builder.http_request_context(ctx);
        }
        // 接入运行时跨会话共用：健康簿、熔断、UDP 封锁记忆属于设备与网络，不属于某次登录。
        if let Some(access) = snap.5.clone() {
            child_builder = child_builder.access_runtime(access);
        }
        // 宿主此前报过的网络状态：新链路从它起步（离线时登录直接显示等网络）。
        let network_available = snap.9.as_ref().is_none_or(|network| network.available);
        child_builder = child_builder.initial_network_available(network_available);
        child_builder = snap.4.apply_to_builder(child_builder);
        let child = child_builder.build()?;
        let mut inner = child.into_inner()?;
        inner.environment = snap.0;
        inner.sdk_config = snap.1;
        inner.data_root = Some(data_root);
        if inner.http_request_context.is_none() {
            inner.http_request_context = snap.3;
        }
        // 宿主装的续期回调跨会话保留：它属于宿主，不属于某一次登录。
        inner.connect_token_refresher = snap.6;
        inner.last_network = snap.9;
        inner.access_store = snap.10;
        inner.refresh_token_vault = vault.clone();
        inner.current_user_id = Some(user_id.to_string());
        inner.last_session_user = Some(user_id.to_string());
        let previous_user = match snap.8 {
            Some(previous) => Some(previous),
            None => match inner.data_root.as_deref() {
                Some(root) => read_last_session_user(root).await,
                None => None,
            },
        };
        // 没有任何记录（首次使用、浏览器里刷新）按同一用户处理：本库里的待发本来就属于这个用户。
        let same_user = previous_user
            .as_deref()
            .is_none_or(|previous| previous == user_id);
        // 换了账号：上一个账号存着的刷新令牌不再留。冷启动时没有上一个会话的保管处，用这一次的
        // （宿主的安全存储是同一个，按用户分项）。
        if !same_user
            && let Some(previous) = previous_user.as_deref()
            && let Some(previous_vault) = snap.11.as_ref().or(vault.as_ref())
        {
            previous_vault.clear(previous).await;
        }
        // 本地会话身份随 prepare 建立：bootstrap_startup_home / 视图等本地优先
        // 读路径在 connect 之前即可出图（热启动 T0）。
        if let Some(engine) = inner.engine.as_ref() {
            engine.adopt_local_session_identity(user_id).await;
        }
        {
            let mut current = self.inner.write().await;
            // Never recycle the child builder's generation (normally zero): binding
            // caches and old watchers must not match a newly prepared user.
            inner.session_generation = current.session_generation.wrapping_add(1);
            *current = inner;
            self.store_session_generation_snapshot(current.session_generation);
            self.store_state_snapshot(SdkState::Disconnected);
            self.store_connected_apis_snapshot(Self::connected_apis_from_inner(&current)?);
            // 准备好就能接推送：被推送冷启动、还没连上的进程也能按本地已读与台账判断。
            self.attach_push_inbox(&current);
        }
        self.repair_local_conversation_identities_on_login(user_id)
            .await?;
        tokio::task::yield_now().await;
        self.prepare_outbox(same_user).await?;
        if let Some(root) = self.data_root().await {
            write_last_session_user(&root, user_id).await;
        }
        self.notify_extension_login_best_effort(user_id).await;
        Ok(())
    }

    /// 一步登录：[`Self::prepare`] + [`Self::connect`] 的组合，供不做预热的调用方使用。
    ///
    /// `before_connect` 在 `prepare` 之后、建连之前回调，可在此注册事件监听
    /// （预热路径下等价于在 `prepare` 与 `connect` 之间订阅事件）。
    #[tracing::instrument(skip(self, explicit_token, db, before_connect), fields(user_id = %user_id))]
    pub async fn login<F>(
        &self,
        user_id: &str,
        explicit_token: Option<&str>,
        db: LoginDbKind,
        before_connect: F,
    ) -> Result<ConnectedApis>
    where
        F: FnOnce(crate::kernel::event::EventBus, Arc<dyn crate::domain::MessageStore>)
            + Send
            + 'static,
    {
        self.prepare(user_id, db).await?;
        let bus = self.bus().await?.clone();
        let msg_store = self.stores_async().await?.messages.clone();
        before_connect(bus, msg_store);
        self.connect(user_id, explicit_token).await
    }

    /// 连接已 [`Self::prepare`] / builder 装配好的引擎并完成首次同步，返回连接态 API 快照。
    ///
    /// 「预热后登录」的网络半段：先 [`Self::prepare`]，拿到 token 后调用本方法即可，
    /// 登录关键路径只剩连接握手 + 首次同步。若引擎不存在（未 prepare / 未 build）返回 `NotConnected`。
    #[tracing::instrument(skip(self, explicit_token), fields(user_id = %user_id))]
    pub async fn connect(
        &self,
        user_id: &str,
        explicit_token: Option<&str>,
    ) -> Result<ConnectedApis> {
        self.connect_internal(user_id, explicit_token, true).await?;
        self.connected_apis().await
    }

    #[tracing::instrument(skip(self, explicit_token), fields(user_id = %user_id, install_watcher))]
    pub(super) async fn connect_internal(
        &self,
        user_id: &str,
        explicit_token: Option<&str>,
        install_watcher: bool,
    ) -> Result<()> {
        let generation = self.session_generation_snapshot();
        // token 三档来源：显式传入 > SDK 托管（向网关签发）> 环境变量（联调）。没有本地签发。
        // SDK 托管的签发不在这里做：交给链路在每次建连前取令牌时完成（`token_for_connect`）。
        // 离线冷启动时签发必然失败，在这里签就会让 connect 直接报错、链路根本不起来。
        let token = match explicit_token.map(str::trim).filter(|t| !t.is_empty()) {
            Some(token) => Some(token.to_string()),
            None if Self::sdk_auth_from_inner(&*self.inner.read().await).is_some() => None,
            None => Some(resolve_connect_token(user_id, None)?),
        };
        // 宿主此前报过的前后台带进这次会话（平台常在登录前就报过，之后对相同状态去重）。被系统在后台
        // 拉起的进程若按前台建连，服务端会以为这台设备自己提示新消息，不再转给推送。
        let foreground = self.is_app_foreground_snapshot();
        // 会话的这一代从这里开始：链路状态机、令牌来源与监视器都认这一代。之后的线路切换、重连
        // 都不再改变它（切换不是重登）。
        let (link_generation, bus, link, user_store, http_request_context, tenant_id) = {
            let mut g = self.inner.write().await;
            if g.session_generation != generation {
                return Err(Self::not_connected());
            }
            let Some(engine) = g.engine.as_ref() else {
                return Err(FlareError::localized(
                    ErrorCode::NotConnected,
                    "no engine; use builder or login",
                ));
            };
            let bus = engine.bus().clone();
            let (link, user_store) = engine.link_start_parts();
            engine.adopt_app_state(foreground);
            // 后台展示方与正在看的会话同样常在登录前就报过：引擎每次登录新建，从客户端那份接过来。
            engine.adopt_notification_host(self.notification_host.clone());
            // 没经 prepare/login、直接在构建器建好的引擎上 connect 时，引擎用的是构建器那份接入运行时，
            // 而 init 另建了数据根下的一份：以链路实际用的为准，状态、诊断、固定接入点才落在同一份上。
            if g.access
                .as_ref()
                .is_none_or(|access| !Arc::ptr_eq(access, link.access()))
            {
                if let Some(network) = g.last_network.as_ref() {
                    link.access().set_network(
                        network.interface.as_deref(),
                        network.fingerprint_hint.as_deref(),
                    );
                }
                g.access = Some(link.access().clone());
            }
            g.current_user_id = Some(user_id.to_string());
            g.connect_token = token.clone();
            g.refresh_token = None;
            g.session_generation = g.session_generation.wrapping_add(1);
            self.store_session_generation_snapshot(g.session_generation);
            let link_generation = g.session_generation;
            let tenant_id = Self::resolve_tenant_id(&g);
            let apis = Self::connected_apis_from_inner(&g)?;
            self.store_connected_apis_snapshot(apis);
            // 建连前就装上：链路确认的第一条消息要按这个用户与租户记进提示台账。
            self.attach_push_inbox(&g);
            (
                link_generation,
                bus,
                link,
                user_store,
                g.http_request_context.clone(),
                tenant_id,
            )
        };
        if let Some(context) = http_request_context.as_ref() {
            // Avoid reusing a stale Social Gateway token after a fresh IM login.
            // Media access resolution can use the IM token fallback until a new
            // gateway session is explicitly applied.
            context.clear_gateway_context().await;
            // SDK 托管时令牌还没签发：先清空，签发后由令牌来源带上。
            context
                .set_auth_context(token.clone().unwrap_or_default(), None)
                .await;
            // IM login 期间 Background 社交同步仍走共享 HTTP，须保留 user/tenant。
            context.ensure_identity(user_id, &tenant_id).await;
        }
        if install_watcher {
            self.spawn_update_state_watcher(link_generation);
            self.spawn_state_snapshot_watcher(link_generation, bus);
            self.spawn_token_refresh_watcher(link_generation);
        }
        *user_store.write().await = user_id.to_string();
        self.store_state_snapshot(SdkState::Connecting);
        let tokens = Arc::new(super::session_watchers::SessionTokens::new(
            self,
            link_generation,
        ));
        link.app_state(foreground).await;
        // 不持锁等待：建连最长要一个握手时限，期间登出、查询都不能被挡住。
        link.start(user_id, tokens).await?;
        if !self.is_generation_current(link_generation).await {
            return Err(Self::not_connected());
        }
        self.notify_extension_connect_best_effort(user_id).await;
        self.attach_push_session().await;
        Ok(())
    }

    /// 主动断开连接并清理当前会话上下文（语义上等价于轻量登出）。
    pub async fn disconnect(&self) -> Result<()> {
        self.clear_session_snapshot();
        let lifecycle = self.extension_lifecycle_snapshot(None).await;
        let (engine, http_request_context) = {
            let mut g = self.inner.write().await;
            g.session_generation = g.session_generation.wrapping_add(1);
            self.store_session_generation_snapshot(g.session_generation);
            g.current_user_id = None;
            g.connect_token = None;
            g.message_api = None;
            g.media_api = None;
            g.capability_api = None;
            g.presence_api = None;
            g.capability_registry = None;
            g.message_build_api = None;
            g.conversation_api = None;
            let engine = g.engine.take();
            let http_request_context = g.http_request_context.clone();
            (engine, http_request_context)
        };
        self.clear_session_snapshot();
        self.store_state_snapshot(SdkState::Disconnected);
        self.reconcile_push_in_background();
        if let Some(context) = http_request_context.as_ref() {
            context.set_auth_context(String::new(), None).await;
            context.clear_gateway_context().await;
        }
        if let Some(mut e) = engine {
            e.disconnect().await?;
        }
        Self::notify_extension_disconnect_snapshot(&lifecycle).await;
        Ok(())
    }
}

/// 以加密本地库登录时，刷新令牌与库密钥放进同一个宿主安全存储（同一组命名空间与租户，用途不同）。
/// 其它登录方式宿主没有给安全存储，刷新令牌只在内存里。
#[cfg_attr(not(feature = "lifecycle-sqlite"), allow(unused_variables))]
fn refresh_token_vault_for(
    db: &LoginDbKind,
    overlay: Option<&crate::client::lifecycle::SdkConfigOverlay>,
) -> Option<super::refresh_vault::RefreshTokenVault> {
    match db {
        #[cfg(feature = "lifecycle-sqlite")]
        LoginDbKind::EncryptedSqlite {
            key_store,
            key_namespace,
            tenant_id,
            ..
        } => Some(super::refresh_vault::RefreshTokenVault::new(
            key_store.clone(),
            key_namespace.as_deref().unwrap_or("flare-im-core-sdk"),
            tenant_id
                .as_deref()
                .or_else(|| overlay.and_then(|cfg| cfg.tenant_id.as_deref()))
                .unwrap_or(crate::shared::util::DEFAULT_TENANT_ID),
        )),
        _ => None,
    }
}

/// 上一个会话的用户（原生端记在数据根下，冷启动也能判断「同一用户」）。
#[cfg(not(target_arch = "wasm32"))]
async fn read_last_session_user(data_root: &std::path::Path) -> Option<String> {
    tokio::fs::read_to_string(data_root.join(LAST_SESSION_USER_FILE))
        .await
        .ok()
        .map(|user| user.trim().to_owned())
        .filter(|user| !user.is_empty())
}

#[cfg(target_arch = "wasm32")]
async fn read_last_session_user(_data_root: &std::path::Path) -> Option<String> {
    None
}

#[cfg(not(target_arch = "wasm32"))]
async fn write_last_session_user(data_root: &std::path::Path, user_id: &str) {
    if let Err(error) = tokio::fs::write(data_root.join(LAST_SESSION_USER_FILE), user_id).await {
        tracing::debug!(%error, "recording the last session user failed");
    }
}

#[cfg(target_arch = "wasm32")]
async fn write_last_session_user(_data_root: &std::path::Path, _user_id: &str) {}

#[cfg(not(target_arch = "wasm32"))]
const LAST_SESSION_USER_FILE: &str = "last_session_user";
