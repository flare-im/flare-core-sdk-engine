//! 客户端壳层：配置与生命周期、[`IMClient`]、消息/会话 Facade（[`api`]）、引擎组装（[`builder`]）、事件订阅（[`events`]）。
//!
//! 与「消息/会话领域逻辑」的分界：业务命令在 [`api`] → `application`；此处只做句柄、连接与 `EventBus` 订阅入口。

pub mod api;
pub mod builder;
pub mod config;
pub mod connected_apis;
pub mod events;
pub mod im_client;
pub mod lifecycle;
pub mod profile_center;
pub mod push_lifecycle;
pub mod token_broker;
pub mod token_provider;

pub use crate::extension::{
    ExtensionLifecycle, ExtensionLifecycleContext, ExtensionRegistry, SdkExtension,
};
pub use crate::model::{
    MediaAccessUrl, MediaCacheEntryVo, MediaCacheStatsVo, MediaResolvedAccess, UploadOptions,
    UploadedMedia,
};
pub use crate::platform::ports::media::{
    MediaMetadata, MediaServicePort, MediaSourceDescriptor, MediaSourceKind,
};
pub use crate::platform::runtime::{NativeRuntimeAssembler, RuntimeAssembler, RuntimeComponents};
pub use crate::shared::util::CoreTokenConfig;
pub use api::{
    CapabilityApi, CapabilityDescriptorDto, CapabilityDispatchResult, ConversationApi,
    CreateLocationRequest, CreateRichDocRequest, CreateStickerRequest, EditRichDocRequest,
    FileDownloadProgress, FileDownloadProgressCallback, MediaApi, MessageApi, MessageBuildApi,
    UploadPhase, UploadProgress, UploadProgressCallback, UserCapabilityGrantDto,
};
pub use builder::IMClientBuilder;
pub use config::{
    SdkAppConfig, SdkConfig, SdkConfigBuilder, SdkResourceProfile, SdkRuntimeResources,
};
pub use connected_apis::ConnectedApis;
pub use flare_core::common::{HeartbeatAppState, TlsConfig};
pub use im_client::{
    IMClient, NetworkChangeEvent, PinnedAccessPoint, RuntimeHealthSnapshot, ServerClockEstimate,
};
pub use lifecycle::{
    LoginDbKind, SdkConfigOverlay, default_sdk_data_root, dev_data_dir_relative_to_cwd,
    merge_sdk_config, parse_data_url_to_path, resolve_connect_token, resolve_sdk_data_root,
    resolve_user_db_path, sanitize_user_id_for_dir,
};
pub use token_broker::HostTokenBroker;
