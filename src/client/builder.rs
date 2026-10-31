use std::sync::Arc;

use tokio::sync::RwLock;

use crate::application::SyncProtocolAdapter;
use crate::application::notification::{
    NotificationHandler, NotificationHandlerRegistry, NotificationInboundPipeline,
};
use crate::application::services::EventDeduper;
use crate::application::services::MessageDeduper;
use crate::application::services::PluginStateRefetch;
use crate::application::sync_task::{
    ConversationSettingsSyncTask, ConversationsSyncTask, KeyEventsSyncTask, MessagesSyncTask,
    ReadStatesSyncTask,
};
use crate::application::usecases::{
    ConversationCommandUseCase, ConversationViewAssembler, MessageMutationUseCase,
    MessageSendUseCase, MessageViewAssembler, sync_request::SyncRequestUseCase,
};
use crate::client::api::{
    CapabilityApi, ConversationApi, MediaApi, MessageApi, MessageBuildApi, PluginCatalogNotices,
    PresenceApi, ViewApi,
};
use crate::client::config::SdkConfig;
use crate::client::im_client::{IMClient, IMClientInner};
use crate::extension::capability::{SdkCapabilityPlugin, SdkCapabilityRegistry};
use crate::extension::encryption::{
    ContentEncryptionInterceptor, ConversationEncryptionPolicyResolver,
};
use crate::extension::middleware::{EventInterceptor, MessageInterceptor, MiddlewareChain};
use crate::extension::{
    ContentCodec, ConversationProjectionSource, ExtensionLifecycle, ExtensionMigration,
    ExtensionRegistry, ExtensionRuntime, ProfileProvider, SdkExtension,
};
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::protocol::{Codec, ProtobufCodec};
use crate::infrastructure::transport::{HttpClient, HttpRequestContext, SocketTransport};
use crate::kernel::event::EventBus;
use crate::kernel::{
    ConversationSummarySync, CurrentUserIdStore, ReliableSendQueuePort, SessionSyncRunner,
    SyncDomain, SyncResponseHandler, SyncTask,
};
use crate::platform::adapters::media::{MediaService, UploadOnlyMediaService};
use crate::platform::ports::media::MediaServicePort;
use crate::platform::runtime::RuntimeComponents;
use crate::runtime::{SdkEngine, SdkEngineConfig};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::spi::metrics::{MetricsRecorder, MetricsSink};

#[derive(Clone, Default)]
pub(crate) struct IMClientExtensionComponents {
    sync_tasks: Vec<Arc<dyn SyncTask>>,
    sync_domains: Vec<Arc<dyn SyncDomain>>,
    message_interceptors: Vec<Arc<dyn MessageInterceptor>>,
    event_interceptors: Vec<Arc<dyn EventInterceptor>>,
    capability_plugins: Vec<Arc<dyn SdkCapabilityPlugin>>,
    notification_handlers: Vec<Arc<dyn NotificationHandler>>,
    extension_lifecycles: Vec<Arc<dyn ExtensionLifecycle>>,
    profile_providers: Vec<Arc<dyn ProfileProvider>>,
    conversation_projection_sources: Vec<Arc<dyn ConversationProjectionSource>>,
    content_codecs: Vec<Arc<dyn ContentCodec>>,
    extension_migrations: Vec<Arc<dyn ExtensionMigration>>,
}

impl IMClientExtensionComponents {
    pub(crate) fn apply_to_builder(&self, mut builder: IMClientBuilder) -> IMClientBuilder {
        builder.sync_tasks.extend(self.sync_tasks.iter().cloned());
        builder
            .sync_domains
            .extend(self.sync_domains.iter().cloned());
        builder
            .message_interceptors
            .extend(self.message_interceptors.iter().cloned());
        builder
            .event_interceptors
            .extend(self.event_interceptors.iter().cloned());
        builder
            .capability_plugins
            .extend(self.capability_plugins.iter().cloned());
        builder
            .notification_handlers
            .extend(self.notification_handlers.iter().cloned());
        builder
            .extension_lifecycles
            .extend(self.extension_lifecycles.iter().cloned());
        builder
            .profile_providers
            .extend(self.profile_providers.iter().cloned());
        builder
            .conversation_projection_sources
            .extend(self.conversation_projection_sources.iter().cloned());
        builder
            .content_codecs
            .extend(self.content_codecs.iter().cloned());
        builder
            .extension_migrations
            .extend(self.extension_migrations.iter().cloned());
        builder
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn derive_capability_url(config: &SdkConfig) -> String {
    if let Ok(url) = std::env::var("FLARE_CAPABILITY_GRPC_URI")
        .or_else(|_| std::env::var("FLARE_IM_CAPABILITY_GRPC_URI"))
    {
        let url = url.trim();
        if !url.is_empty() {
            return url.to_string();
        }
    }
    if let Some(url) = config
        .capability_url
        .as_ref()
        .filter(|u| !u.trim().is_empty())
    {
        return url.clone();
    }
    if let Some(http_url) = config.http_url.as_ref()
        && let Ok(mut parsed) = url::Url::parse(http_url)
    {
        let _ = parsed.set_port(Some(50110));
        parsed.set_path("");
        parsed.set_query(None);
        parsed.set_fragment(None);
        return parsed.to_string().trim_end_matches('/').to_string();
    }
    "http://localhost:50110".to_string()
}

#[cfg(target_arch = "wasm32")]
fn derive_capability_url(config: &SdkConfig) -> String {
    config
        .capability_url
        .clone()
        .unwrap_or_else(|| "http://localhost:50110".to_string())
}

/// IMClient 构建器
///
/// 核心只构建消息和会话能力，其他功能通过扩展注入：
/// - `add_sync_task`: 注册自定义同步任务（业务 SDK 扩展，如社交好友/群同步）
/// - `add_message_interceptor`: 消息拦截（端到端加密、内容过滤等）
/// - `add_event_interceptor`: 事件拦截（日志、审计等）
///
/// ```ignore
/// let client = IMClient::builder()
///     .config(SdkConfig::new("wss://im.example.com"))
///     .stores(stores)
///     .add_sync_task(MyBusinessSync::new())    // 扩展：业务同步任务
///     .add_message_interceptor(E2EEncryption::new()) // 扩展：端到端加密
///     .build()?;
/// ```
pub struct IMClientBuilder {
    config: SdkConfig,
    stores: Option<StoreProvider>,
    runtime: Option<RuntimeComponents>,
    http_request_context: Option<Arc<HttpRequestContext>>,
    codec: Option<Arc<dyn Codec>>,
    sync_tasks: Vec<Arc<dyn SyncTask>>,
    sync_domains: Vec<Arc<dyn SyncDomain>>,
    message_interceptors: Vec<Arc<dyn MessageInterceptor>>,
    event_interceptors: Vec<Arc<dyn EventInterceptor>>,
    capability_plugins: Vec<Arc<dyn SdkCapabilityPlugin>>,
    notification_handlers: Vec<Arc<dyn NotificationHandler>>,
    extension_lifecycles: Vec<Arc<dyn ExtensionLifecycle>>,
    profile_providers: Vec<Arc<dyn ProfileProvider>>,
    conversation_projection_sources: Vec<Arc<dyn ConversationProjectionSource>>,
    content_codecs: Vec<Arc<dyn ContentCodec>>,
    extension_migrations: Vec<Arc<dyn ExtensionMigration>>,
    metrics_sink: Option<Arc<dyn MetricsSink>>,
    builder_errors: Vec<FlareError>,
}

impl IMClientBuilder {
    /// 创建构建器，带默认 SDK 配置。
    pub fn new() -> Self {
        Self {
            config: SdkConfig::default(),
            stores: None,
            runtime: None,
            http_request_context: None,
            codec: None,
            sync_tasks: Vec::new(),
            sync_domains: Vec::new(),
            message_interceptors: Vec::new(),
            event_interceptors: Vec::new(),
            capability_plugins: Vec::new(),
            notification_handlers: Vec::new(),
            extension_lifecycles: Vec::new(),
            profile_providers: Vec::new(),
            conversation_projection_sources: Vec::new(),
            content_codecs: Vec::new(),
            extension_migrations: Vec::new(),
            metrics_sink: None,
            builder_errors: Vec::new(),
        }
    }

    /// 设置 SDK 基础配置（网络、超时、重连等）。
    pub fn config(mut self, config: SdkConfig) -> Self {
        self.config = config;
        self
    }

    /// 注入存储实现（必填）。
    ///
    /// 未设置时 [`Self::build`] 返回配置错误。
    pub fn stores(mut self, stores: StoreProvider) -> Self {
        self.stores = Some(stores);
        self
    }

    /// 注入已装配好的运行时组件。
    ///
    /// 这是 Web/RN/uni-app/Electron/Android/iOS/鸿蒙等平台 adapter 的主接入点。
    /// 原生测试和服务端内嵌场景可直接使用 `.stores(...)` 注入内存或 SQLite 存储。
    pub fn runtime(mut self, runtime: RuntimeComponents) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// 注入共享 HTTP 鉴权上下文（与 Social Gateway / 媒体 HTTP 共用 Bearer 与链路头）。
    pub fn http_request_context(mut self, context: Arc<HttpRequestContext>) -> Self {
        self.http_request_context = Some(context);
        self
    }

    /// 设置编解码器实现；未设置时默认使用 `ProtobufCodec`。
    pub fn codec(mut self, codec: Arc<dyn Codec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// 注册自定义同步任务（支持同步/异步完成）
    pub fn add_sync_task(mut self, task: impl SyncTask + 'static) -> Self {
        self.sync_tasks.push(Arc::new(task));
        self
    }

    /// 注册已装箱的同步任务实现。
    pub fn add_sync_task_arc(mut self, task: Arc<dyn SyncTask>) -> Self {
        self.sync_tasks.push(task);
        self
    }

    /// 注册领域无关同步领域（业务/群/好友等）。实现 [`SyncDomain`] 即接入收敛内核，
    /// 白拿冷启/热启/重连/追赶/优先级/缺口修复——core 不认识其业务规则。
    pub fn add_sync_domain(mut self, domain: impl SyncDomain + 'static) -> Self {
        self.sync_domains.push(Arc::new(domain));
        self
    }

    /// 注册已装箱的同步领域实现。
    pub fn add_sync_domain_arc(mut self, domain: Arc<dyn SyncDomain>) -> Self {
        self.sync_domains.push(domain);
        self
    }

    /// 注册消息拦截器（加密/过滤/富化）
    pub fn add_message_interceptor(
        mut self,
        interceptor: impl MessageInterceptor + 'static,
    ) -> Self {
        self.message_interceptors.push(Arc::new(interceptor));
        self
    }

    /// 注册事件拦截器
    pub fn add_event_interceptor(mut self, interceptor: impl EventInterceptor + 'static) -> Self {
        self.event_interceptors.push(Arc::new(interceptor));
        self
    }

    /// 注册自定义能力插件。
    ///
    /// 插件命名空间由 manifest 声明，注册表负责所有权冲突校验。
    ///
    /// 每个命名空间只能有一个插件所有者；冲突会使 SDK 构建失败。
    pub fn add_capability_plugin(mut self, plugin: impl SdkCapabilityPlugin + 'static) -> Self {
        self.capability_plugins.push(Arc::new(plugin));
        self
    }

    /// 注册已装箱能力插件。
    pub fn add_capability_plugin_arc(mut self, plugin: Arc<dyn SdkCapabilityPlugin>) -> Self {
        self.capability_plugins.push(plugin);
        self
    }

    pub fn add_notification_handler_arc(mut self, handler: Arc<dyn NotificationHandler>) -> Self {
        self.notification_handlers.push(handler);
        self
    }

    pub fn add_extension_lifecycle_arc(mut self, lifecycle: Arc<dyn ExtensionLifecycle>) -> Self {
        self.extension_lifecycles.push(lifecycle);
        self
    }

    pub fn add_profile_provider_arc(mut self, provider: Arc<dyn ProfileProvider>) -> Self {
        self.profile_providers.push(provider);
        self
    }

    pub fn add_conversation_projection_source_arc(
        mut self,
        source: Arc<dyn ConversationProjectionSource>,
    ) -> Self {
        self.conversation_projection_sources.push(source);
        self
    }

    pub fn add_content_codec_arc(mut self, codec: Arc<dyn ContentCodec>) -> Self {
        self.content_codecs.push(codec);
        self
    }

    /// 注册会话加密策略与内容编解码器。
    ///
    /// `Transport` / `None` 档位不改写消息内容；`E2e` 档位会在发送前使用
    /// `codec` 将明文 `MessageContent` 封装为 typed encrypted placeholder。
    pub fn conversation_encryption(
        mut self,
        resolver: Arc<dyn ConversationEncryptionPolicyResolver>,
        codec: Arc<dyn ContentCodec>,
    ) -> Self {
        self.content_codecs.push(codec.clone());
        self.message_interceptors
            .push(Arc::new(ContentEncryptionInterceptor::new(resolver, codec)));
        self
    }

    pub fn add_extension_migration_arc(mut self, migration: Arc<dyn ExtensionMigration>) -> Self {
        self.extension_migrations.push(migration);
        self
    }

    /// Inject a host metrics exporter. The exporter is used only when
    /// `SdkConfig.enable_metrics` is true; diagnostics still keep an in-memory
    /// snapshot for platform SDKs.
    pub fn metrics_sink_arc(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics_sink = Some(sink);
        self
    }

    /// 安装业务扩展。
    ///
    /// 扩展只能注册任务、拦截器和能力插件；核心 IM API 不因业务扩展而变化。
    pub fn add_extension(mut self, extension: impl SdkExtension + 'static) -> Self {
        let mut registry = ExtensionRegistry::default();
        match extension.install(&mut registry) {
            Ok(()) => {
                self.sync_tasks.extend(registry.sync_tasks);
                self.message_interceptors
                    .extend(registry.message_interceptors);
                self.event_interceptors.extend(registry.event_interceptors);
                self.capability_plugins.extend(registry.capability_plugins);
                self.notification_handlers
                    .extend(registry.notification_handlers);
                self.extension_lifecycles.extend(registry.lifecycles);
                self.profile_providers.extend(registry.profile_providers);
                self.conversation_projection_sources
                    .extend(registry.conversation_projection_sources);
                self.content_codecs.extend(registry.content_codecs);
                self.extension_migrations.extend(registry.migrations);
            }
            Err(err) => {
                tracing::error!(
                    namespace = extension.namespace(),
                    error = %err,
                    "SDK extension installation failed"
                );
                self.builder_errors.push(err);
            }
        }
        self
    }

    /// 构建 [`IMClient`] 并装配引擎、API 门面和默认同步任务。
    ///
    /// 该方法只完成组装，不会自动登录或建连。
    pub fn build(self) -> Result<IMClient> {
        // self.config 后面会被 move 进 transport，先把 ws 地址取出来。
        let configured_ws_url = self.config.ws_url.clone();
        let configured_config = self.config.clone();
        if let Some(err) = self.builder_errors.first().cloned() {
            return Err(err);
        }
        let extension_components = IMClientExtensionComponents {
            sync_tasks: self.sync_tasks.clone(),
            sync_domains: self.sync_domains.clone(),
            message_interceptors: self.message_interceptors.clone(),
            event_interceptors: self.event_interceptors.clone(),
            capability_plugins: self.capability_plugins.clone(),
            notification_handlers: self.notification_handlers.clone(),
            extension_lifecycles: self.extension_lifecycles.clone(),
            profile_providers: self.profile_providers.clone(),
            conversation_projection_sources: self.conversation_projection_sources.clone(),
            content_codecs: self.content_codecs.clone(),
            extension_migrations: self.extension_migrations.clone(),
        };
        let codec: Arc<dyn Codec> = self.codec.unwrap_or_else(|| Arc::new(ProtobufCodec));
        let (stores, transport, runtime_media_service) = match self.runtime {
            Some(RuntimeComponents {
                stores,
                transport,
                media_service,
                media_processor,
                media_uploader,
                ..
            }) => {
                let media_service = media_service.or_else(|| {
                    media_uploader.map(|uploader| match media_processor {
                        Some(processor) => {
                            Arc::new(UploadOnlyMediaService::with_processor(uploader, processor))
                                as Arc<dyn MediaServicePort>
                        }
                        None => Arc::new(UploadOnlyMediaService::new(uploader))
                            as Arc<dyn MediaServicePort>,
                    })
                });
                (stores, transport, media_service)
            }
            None => {
                let stores = self.stores.ok_or_else(|| {
                    FlareError::localized(
                        ErrorCode::ConfigurationError,
                        "StoreProvider is required",
                    )
                })?;
                let transport = SocketTransport::with_codec(self.config.clone(), codec.clone());
                (stores, transport, None)
            }
        };
        let sender = transport.sender();
        let mut chain = MiddlewareChain::new();
        for i in self.message_interceptors {
            chain.add_message_interceptor(i);
        }
        for i in self.event_interceptors {
            chain.add_event_interceptor(i);
        }
        let chain = Arc::new(chain);
        let resources = self.config.runtime_resources();
        let metrics = if self.config.enable_metrics {
            MetricsRecorder::enabled(self.metrics_sink)
        } else {
            MetricsRecorder::disabled()
        };
        let bus = EventBus::with_middleware_capacity_and_metrics(
            chain.clone(),
            resources.event_bus_capacity,
            metrics.clone(),
        );
        let event_deduper = EventDeduper::new(Some(resources.event_dedupe_capacity));
        let message_deduper = MessageDeduper::new(Some(resources.message_dedupe_capacity));
        let extension_runtime = Arc::new(ExtensionRuntime::new(
            self.extension_lifecycles,
            self.profile_providers,
            self.conversation_projection_sources,
            self.content_codecs,
            self.extension_migrations,
        ));
        if !extension_runtime.is_empty() {
            tracing::debug!(
                target = "flare_sdk.extension",
                lifecycles = extension_runtime.lifecycle_count(),
                profile_providers = extension_runtime.profile_provider_count(),
                conversation_projection_sources =
                    extension_runtime.conversation_projection_source_count(),
                content_codecs = extension_runtime.content_codec_count(),
                migrations = extension_runtime.migration_count(),
                "SDK extension runtime installed"
            );
        }
        let notification_registry = Arc::new(NotificationHandlerRegistry::with_handlers(
            self.notification_handlers,
        ));
        let notification_pipeline = NotificationInboundPipeline::new(
            notification_registry.clone(),
            message_deduper.clone(),
            bus.clone(),
        );
        let init_msg_concurrency = resources.init_message_sync_concurrency as usize;
        let sync_handler: Arc<SyncProtocolAdapter> = Arc::new(SyncProtocolAdapter::new(
            sender.clone(),
            stores.clone(),
            bus.clone(),
            event_deduper.clone(),
            notification_pipeline.clone(),
            self.config.effective_device_id(),
            metrics.clone(),
        ));
        // 远端游标合并冲刷 worker：热路径入队即返回，去抖批量推送（advisory 数据不占 catch-up 延迟）。
        sync_handler.start_remote_cursor_flush_worker();

        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new(String::new()));
        let engine = SdkEngine::new(SdkEngineConfig {
            stores,
            chain: chain.clone(),
            transport,
            current_user_id: current_user_id.clone(),
            codec,
            bus,
            sync_response_handler: Some(sync_handler.clone() as Arc<dyn SyncResponseHandler>),
            session_sync: Some(sync_handler.clone() as Arc<dyn SessionSyncRunner>),
            conversation_summary_sync: Some(
                sync_handler.clone() as Arc<dyn ConversationSummarySync>
            ),
            event_deduper,
            message_deduper,
            notification_pipeline,
            ack_timeout_secs: self.config.ack_timeout_secs,
            ack_max_retries: self.config.ack_max_retries,
            ack_max_in_flight: self.config.ack_max_in_flight,
            metrics: metrics.clone(),
        });

        // 注入应用层同步任务（构造时传入 SyncProtocolAdapter，execute 内自行调用，与用户扩展一致）
        engine
            .sync_manager()
            .register_task_arc(Arc::new(ConversationsSyncTask::new(sync_handler.clone())));
        engine.sync_manager().register_task_arc(Arc::new(
            MessagesSyncTask::with_max_sync_concurrency(
                sync_handler.clone(),
                init_msg_concurrency,
                engine.sync_manager().attention(),
            ),
        ));
        engine
            .sync_manager()
            .register_task_arc(Arc::new(KeyEventsSyncTask::new(sync_handler.clone())));
        engine
            .sync_manager()
            .register_task_arc(Arc::new(ReadStatesSyncTask::new(sync_handler.clone())));
        engine
            .sync_manager()
            .register_task_arc(Arc::new(ConversationSettingsSyncTask::new(
                sync_handler.clone(),
            )));
        for task in self.sync_tasks {
            engine.sync_manager().register_task_arc(task);
        }
        for domain in self.sync_domains {
            engine.sync_manager().register_domain(domain);
        }

        let sender = engine.sender().clone();
        let store_ref = engine.stores().clone();
        let chain_ref = engine.middleware_chain();
        let profile_reader = store_ref.user_profiles_or_memory();
        let reliable_queue: Option<Arc<dyn ReliableSendQueuePort>> = engine
            .reliable_queue()
            .map(|queue| -> Arc<dyn ReliableSendQueuePort> { queue });
        sync_handler.set_reliable_queue(reliable_queue.clone());
        let bus = engine.bus().clone();

        let message_build_api = MessageBuildApi::new(
            current_user_id.clone(),
            store_ref.conversations.clone(),
            profile_reader.clone(),
        );
        let media_base_url = self
            .config
            .http_url
            .clone()
            .unwrap_or_else(|| "http://localhost:50050".to_string());
        let capability_base_url = derive_capability_url(&self.config);
        let tenant_id = self
            .config
            .tenant_id
            .clone()
            .or_else(|| std::env::var("FLARE_IM_TENANT_ID").ok())
            .map(crate::shared::util::normalize_tenant_id)
            .unwrap_or_else(|| "0".to_string());
        let http_request_context = self
            .http_request_context
            .unwrap_or_else(|| Arc::new(HttpRequestContext::new()));
        #[cfg(not(target_arch = "wasm32"))]
        let media_http_client = HttpClient::with_context_and_tls(
            media_base_url.clone(),
            http_request_context.clone(),
            self.config.core_tls_config(),
        )
        .with_core_bearer();
        #[cfg(target_arch = "wasm32")]
        let media_http_client =
            HttpClient::with_context(media_base_url.clone(), http_request_context.clone())
                .with_core_bearer()
                .with_direct_url_rewrite(
                    self.config.media_storage_proxy_prefix.clone(),
                    self.config.media_storage_proxy_targets.clone(),
                );
        // Presence talks to the same Core API gateway with the same trust and bearer.
        let presence_http_client = media_http_client.clone();
        let capability_http_client = media_http_client.clone();
        let default_media_service: Arc<dyn MediaServicePort> = Arc::new(MediaService::new(
            media_http_client,
            current_user_id.clone(),
            store_ref.upload_manifest_store.clone(),
            store_ref.media_cache_store.clone(),
            store_ref.media_cache_admin.clone(),
            store_ref.user_file_download_store.clone(),
        ));
        let media_service = runtime_media_service.unwrap_or(default_media_service);
        let message_send_use_case = Arc::new(MessageSendUseCase::new(
            sender.clone(),
            store_ref.messages.clone(),
            store_ref.conversations.clone(),
            chain_ref,
            current_user_id.clone(),
            reliable_queue,
            media_service.clone(),
            bus.clone(),
        ));
        let plugin_state_lookup = Arc::new(SyncRequestUseCase::new(
            sender.clone(),
            self.config.effective_device_id(),
        ));
        let message_mutation_use_case = Arc::new(MessageMutationUseCase::new(
            sender,
            store_ref.messages.clone(),
            store_ref.conversations.clone(),
            current_user_id.clone(),
            self.config.effective_device_id(),
            Some(bus.clone()),
        ));
        let read_summaries = crate::application::services::ReadSummaryService::new(
            Arc::new(
                crate::infrastructure::transport::http::read_receipt_api::HttpReadReceiptTransport::new(
                    capability_http_client.clone(),
                ),
            ),
            bus.clone(),
            current_user_id.clone(),
        );
        let message_view_assembler = Arc::new(
            MessageViewAssembler::new(store_ref.messages.clone(), profile_reader.clone())
                .with_read_summaries(read_summaries.clone())
                .with_member_names(store_ref.conversations.clone()),
        );
        let media_api = Arc::new(MediaApi::from_session_handler(
            media_service,
            current_user_id.clone(),
        ));
        let capability_api = Arc::new(
            CapabilityApi::new(
                capability_base_url,
                current_user_id.clone(),
                tenant_id.clone(),
                http_request_context.clone(),
            )
            .with_http_transport(capability_http_client)
            .with_server_clock(engine.server_clock().clone())
            .with_plugin_command_store(store_ref.plugin_commands.clone())
            .with_message_state(store_ref.messages.clone(), bus.clone())
            .with_plugin_state_refetch(Arc::new(PluginStateRefetch::new(
                plugin_state_lookup,
                store_ref.messages.clone(),
                bus.clone(),
                current_user_id.clone(),
            )))
            // Last: the viewer-layer driver dispatches through a clone of the API built so far.
            .with_viewer_state(current_user_id.clone()),
        );
        let presence_api = Arc::new(PresenceApi::new(
            presence_http_client,
            current_user_id.clone(),
            self.config.effective_device_id(),
            tenant_id,
            bus.clone(),
        ));
        let capability_registry = Arc::new(SdkCapabilityRegistry::new_session_bound(
            current_user_id.clone(),
        ));
        // Forward records take a plugin card only when its contribution declares it forwardable.
        let message_build_api =
            Arc::new(message_build_api.with_plugin_cards(capability_api.clone()));
        engine.set_plugin_catalog_notices(Arc::new(PluginCatalogNotices::new(
            capability_api.as_ref().clone(),
            bus.clone(),
        )));
        for plugin in self.capability_plugins {
            let plugin_id = plugin.plugin_id();
            let register_result = capability_registry.register(plugin.clone());
            if let Err(err) = register_result {
                tracing::warn!(
                    target = "flare_sdk.capability",
                    plugin_id = plugin_id,
                    error = %err,
                    "capability plugin registration rejected"
                );
                return Err(err);
            }
        }
        let conversation_command_use_case = Arc::new(ConversationCommandUseCase::new(
            store_ref.conversations.clone(),
            current_user_id.clone(),
            Some(sync_handler.clone()),
            Some(store_ref.cursors.clone()),
        ));
        let conversation_view_assembler = Arc::new(
            ConversationViewAssembler::new(store_ref.conversations.clone(), profile_reader)
                .with_current_user(current_user_id.clone()),
        );

        let conversation_api = Arc::new(ConversationApi::new(
            conversation_command_use_case,
            conversation_view_assembler,
            message_view_assembler.clone(),
            bus.clone(),
            current_user_id.clone(),
        ));
        let view_api = Arc::new(ViewApi::new(
            conversation_api.as_ref().clone(),
            Some(sync_handler.clone() as Arc<dyn SessionSyncRunner>),
            bus.clone(),
            engine.sync_manager().attention(),
        ));

        Ok(IMClient::from_inner(IMClientInner {
            // 把构建期的 ws 地址留下来：登录时重建子客户端只看快照里的 overlay，
            // 不留这一份的话父配置就在那一刻丢了（见字段上的说明）。
            configured_ws_url: configured_ws_url.clone(),
            configured_config: Some(configured_config),
            engine: Some(engine),
            message_api: Some(
                MessageApi::new(
                    message_send_use_case,
                    message_mutation_use_case,
                    message_view_assembler,
                    current_user_id,
                )
                .with_plugin_cards(capability_api.clone())
                .with_read_receipts(read_summaries),
            ),
            media_api: Some(media_api),
            capability_api: Some(capability_api),
            presence_api: Some(presence_api),
            capability_registry: Some(capability_registry),
            message_build_api: Some(message_build_api),
            conversation_api: Some(conversation_api),
            view_api: Some(view_api),
            http_request_context: Some(http_request_context),
            notification_registry: Some(notification_registry),
            extension_runtime: Some(extension_runtime),
            extension_components,
            metrics,
            ..Default::default()
        }))
    }
}

impl Default for IMClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use super::*;
    use crate::extension::ExtensionLifecycleContext;
    use crate::infrastructure::persistence::in_memory_empty_im_provider;
    use crate::kernel::{SyncContext, SyncResult, SyncTaskResult};

    struct ReplayTask;

    impl SyncTask for ReplayTask {
        fn id(&self) -> &'static str {
            "test.replay"
        }

        fn execute(
            &self,
            _ctx: SyncContext,
        ) -> Pin<Box<dyn Future<Output = SyncResult<SyncTaskResult>> + Send>> {
            Box::pin(async { Ok(SyncTaskResult::ok()) })
        }
    }

    struct ReplayLifecycle;

    #[async_trait::async_trait]
    impl ExtensionLifecycle for ReplayLifecycle {
        fn namespace(&self) -> &str {
            "test.lifecycle"
        }

        async fn on_login(&self, _context: &ExtensionLifecycleContext) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn extension_components_replay_into_child_builder() {
        let parent = IMClientBuilder::new()
            .stores(in_memory_empty_im_provider())
            .add_sync_task_arc(Arc::new(ReplayTask))
            .add_extension_lifecycle_arc(Arc::new(ReplayLifecycle))
            .build()
            .unwrap();
        let components = parent
            .inner
            .try_read()
            .unwrap()
            .extension_components
            .clone();

        let child = components
            .apply_to_builder(IMClientBuilder::new().stores(in_memory_empty_im_provider()))
            .build()
            .unwrap();
        let inner = child.inner.try_read().unwrap();
        let extension_task_count = inner
            .engine
            .as_ref()
            .unwrap()
            .sync_manager()
            .registered_tasks()
            .into_iter()
            .filter(|task| task.id() == "test.replay")
            .count();

        assert_eq!(extension_task_count, 1);
        assert_eq!(
            inner.extension_runtime.as_ref().unwrap().lifecycle_count(),
            1
        );
    }
}
