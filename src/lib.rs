//! Flare IM Core SDK.
//!
//! This crate is the client-side source of truth for IM behavior: reliable
//! send, local pending state, sync convergence, message projections, protocol
//! mapping, and extension dispatch. Platform bindings adapt this API instead
//! of duplicating delivery or synchronization rules.

#![cfg_attr(
    not(test),
    deny(clippy::expect_used, clippy::panic, clippy::unwrap_used)
)]

// Internal modules stay crate-private; public facades below own the SDK surface.
#[allow(dead_code, unused_imports)]
pub(crate) mod application;
pub mod client;
pub mod content;
#[allow(dead_code, unused_imports)]
pub(crate) mod core;
#[allow(dead_code, unused_imports)]
pub(crate) mod domain;
#[allow(dead_code, unused_imports)]
pub(crate) mod extension;
#[allow(dead_code, unused_imports)]
pub(crate) mod infrastructure;
#[allow(dead_code, unused_imports)]
pub(crate) mod kernel;
pub mod model;
#[allow(dead_code, unused_imports)]
pub(crate) mod platform;
#[allow(dead_code, unused_imports)]
pub(crate) mod runtime;
#[allow(dead_code, unused_imports)]
pub(crate) mod shared;
pub mod spi;

/// Re-export serialization crates for platform binding runtimes that must share
/// the exact same serde/serde_json crate instances as SDK model types.
pub use schemars;
pub use serde;
pub use serde_json;

/// Event subscription and SDK event payload facade.
/// 客户端链路：接入配置、接入运行时与诊断数据、HTTP 路由（社交 SDK 等经它按服务路由与带幂等键重试）。
pub mod link {
    pub use crate::infrastructure::transport::http::fetch::PlatformHttpFetch;
    pub use crate::infrastructure::transport::http::router::{
        HttpRouter, IDEMPOTENCY_KEY_HEADER, new_idempotency_key,
    };
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub use crate::runtime::link::{AccessLab, DialRewrite};
    pub use crate::runtime::link::{
        AccessPointDto, AccessRuntime, AccessStatusDto, AccessStoreSource, AccessUnavailable,
        ConnectAckSummary, RecoveringStatus, RecoveryImportDto, RefreshAttemptDto,
        RefreshReportDto, RefreshStatus,
    };
    pub use flare_access;
}

pub mod event {
    pub use crate::domain::SendFailure;
    pub use crate::kernel::event::*;
}

/// SDK capability plugin facade.
///
/// Plugin crates and code generators should depend on this facade instead of
/// reaching into SDK internals.
pub mod plugin {
    pub use crate::extension::capability::{
        SdkCapabilityPlugin, SdkCapabilityRegistry, SdkPluginEventManifest, SdkPluginManifest,
        SdkPluginOperationManifest, SdkPluginPermissionManifest, SdkPluginUiKitManifest,
    };
    /// Shared platform Manifest, permission/grant and lifecycle contract. Client
    /// composition manifests below remain build-time SDK package declarations.
    pub use flare_plugin_protocol as platform;
}

/// Storage adapter facade for core-owned persistence implementations.
///
/// Product/business extensions should prefer `spi::ExtensionStore`; this
/// module is for SDK storage adapters such as SQLite or IndexedDB.
pub mod storage {
    pub use crate::domain::{
        ConversationReader, ConversationStore, ConversationWriter, MediaCacheAdmin,
        MediaCacheEntryVo, MediaCacheStatsVo, MediaCacheStore, MessageReader, MessageStore,
        MessageWriter, OperationApplyResult, PendingSendReader, PendingSendVo, PendingSendWriter,
        SeqRange, SyncCursorReader, SyncCursorVo, SyncCursorWriter, TimelineCoverage,
        TimelineCoverageStore, UserFileDownloadStore, UserReader, UserWriter,
        set_local_cleared_through_seq,
    };
    pub use crate::domain::{MemberKey, ParticipantDisplaySnapshot, stored_name_is_an_id};
    pub use crate::domain::{
        PluginCommand, PluginCommandRecord, PluginCommandScope, PluginCommandStore,
    };
    pub use crate::infrastructure::persistence::memory_im::{
        MemoryConversationStore, MemoryMessageStore,
    };
    #[cfg(feature = "storage-sqlite")]
    pub use crate::infrastructure::persistence::sqlite::SqlitePluginCommandRepo;
    pub use crate::infrastructure::persistence::{
        CursorBackedTimelineCoverageStore, MemoryPendingSendStore, MemorySyncCursorStore,
        MemoryTimelineCoverageStore, MemoryUserProfileStore, StoreProvider,
        in_memory_empty_im_provider, in_memory_im_provider,
    };
    pub use crate::platform::ports::storage::{
        LOCAL_DATABASE_KEY_BYTES, SecureKeyDescriptor, SecureKeyStore, SecureSecret,
        VolatileSecureKeyStore, load_or_create_local_database_key, validate_local_database_key,
    };
}

pub use client::*;
pub use core::SdkState;
pub use core::event::{
    CustomEventDefinition, CustomEventSelector, EventBus, EventFilter, EventReceiver,
    FilteredEventReceiver, PublishOutcome, RawSdkEvent, SdkEvent, SdkEventKind, SdkEventType,
    SharedEventReceiver,
};
/// Connect failures the link recovers from by itself: `login` / `connect` succeed and the session stays.
pub use shared::error::link_keeps_session;
pub use shared::error::{ErrorCode, FlareError, Result};
/// An error naming the messages an operation refused (`refusedMessageIds` in the binding error).
pub use shared::error::{
    MAX_REFUSED_MESSAGE_ID_BYTES, MAX_REFUSED_MESSAGE_IDS, REFUSED_MESSAGE_IDS,
    refused_message_ids, refusing_messages,
};
/// An overload error carrying the server's retry wait (`retryAfterMs` in the binding error).
pub use shared::error::{MAX_RETRY_AFTER_MS, RETRY_AFTER_MS, retry_after_ms, retrying_after};
/// 跨平台异步延时（wasm 用 JS 定时器，native 用 tokio）。供 SDK 上层做有界重试等。
pub use shared::util::time::delay;

/// App-facing imports for the production SDK surface.
pub mod prelude {
    pub use crate::application::{
        FileDownloadProgress, FileDownloadProgressCallback, InboundNotificationView,
        NotificationDispatchReport, NotificationHandleResult, NotificationHandler,
        NotificationHandlerRegistry, UploadPhase, UploadProgress, UploadProgressCallback,
        UserFileDownloadRequest,
    };
    pub use crate::client::{
        CapabilityApi, CapabilityDescriptorDto, CapabilityDispatchResult, ConnectedApis,
        ConversationApi, CoreTokenConfig, CreateLocationRequest, CreateRichDocRequest,
        CreateStickerRequest, EditRichDocRequest, IMClient, IMClientBuilder, LoginDbKind, MediaApi,
        MessageApi, MessageBuildApi, NetworkChangeEvent, PinnedAccessPoint, RuntimeHealthSnapshot,
        SdkAppConfig, SdkConfig, SdkConfigBuilder, SdkConfigOverlay, SdkResourceProfile,
        SdkRuntimeResources, ServerClockEstimate, TlsConfig, UserCapabilityGrantDto,
    };
    pub use crate::domain::{
        ConversationStore, DEFAULT_MEDIA_CACHE_MAX_BYTES, MediaCacheEntryVo, MediaCacheStatsVo,
        MessageStore, PendingSendVo, SyncCursorVo, UserDownloadDirectoryVo,
        UserFileDownloadResultVo,
    };
    /// E2EE 参考 codec（需 `e2ee` feature）。
    #[cfg(feature = "e2ee")]
    pub use crate::extension::e2ee_codec::{
        E2EE_CODEC_CONTENT_TYPE, E2EE_CODEC_NAMESPACE, X25519AeadCodec, shared_secret_codec,
    };
    pub use crate::extension::encryption::{
        ContentEncryptionInterceptor, ConversationEncryptionPolicy,
        ConversationEncryptionPolicyResolver, E2EE_CONTENT_TYPE, E2EE_FALLBACK_TEXT,
        E2EE_PLACEHOLDER_REASON, E2eeIdentityKey, E2eeKeyManager, E2eePreKeyBundle,
        E2eeSessionDescriptor, EncryptedContentEnvelope, EncryptionTier,
        KeyManagedConversationEncryptionPolicyResolver, PLAINTEXT_CONTENT_TYPE,
        StaticConversationEncryptionPolicyResolver, VolatileE2eeKeyManager,
        encrypted_content_envelope, encrypted_content_envelope_from_bytes,
    };
    pub use crate::extension::{
        ContentCodec, ConversationProjectionReport, ConversationProjectionSource, ExtensionContent,
        ExtensionContext, ExtensionLifecycle, ExtensionLifecycleContext, ExtensionMigration,
        ExtensionRegistry, ExtensionRuntime, ExtensionStore, ProfileProvider, SdkExtension,
    };
    #[cfg(feature = "storage-sqlite")]
    pub use crate::infrastructure::persistence::sqlite_init_schema;
    pub use crate::infrastructure::persistence::{
        StoreProvider, in_memory_empty_im_provider, in_memory_im_provider,
    };
    pub use crate::infrastructure::protocol::{
        Codec, DownlinkPayload, PacketSender, ProtobufCodec,
    };
    pub use crate::infrastructure::transport::{
        HttpApiResponse, HttpClient, HttpRequestContext, unwrap_api_response,
        unwrap_optional_api_response, unwrap_void_api_response,
    };
    pub use crate::kernel::event::{
        AccessChanged, AccessSwitched, ActionRequiredReason, ConnectionPhase, ConnectionStatus,
        LinkQuality, SwitchReason,
    };
    pub use crate::kernel::event::{
        ConnectionEvent, ConnectionEventType, ConversationEvent, ConversationEventType,
        CustomEventDefinition, CustomEventSelector, EventBus, EventFilter, EventReceiver,
        ExtensionEvent, ExtensionEventType, FilteredEventReceiver, MessageEvent, MessageEventType,
        NotificationEvent, NotificationEventType, PublishOutcome, RawSdkEvent, SdkEvent,
        SdkEventKind, SdkEventType, SharedEventReceiver, Subscription, SyncEventType, SyncNotify,
        SyncPhase,
    };
    pub use crate::kernel::{SdkState, SyncRunContext, SyncScope, SyncTrigger, SyncVisibility};
    pub use crate::model::{
        Conversation, ConversationSummary, IMMessage, MediaDestinationDescriptor,
        MediaDestinationKind, MessageStatus, MessageType, RenderableMedia, RenderableMediaKind,
        SendAck, UploadedMedia,
    };
    pub use crate::platform::ports::media::{
        MediaByteStream, MediaDeliverMeta, MediaDeliveryResult, MediaHost, MediaHttp,
        MediaHttpRequest, MediaHttpResponse, MediaMetadata, MediaProfile, MediaServicePort,
        MediaSink, MediaSinkCapabilities, MediaSourceDescriptor, MediaSourceKind,
        MediaSourceReader, MediaTranscoder, TranscodedMedia,
    };
    pub use crate::platform::ports::storage::{
        LOCAL_DATABASE_KEY_BYTES, SecureKeyDescriptor, SecureKeyStore, SecureSecret,
        VolatileSecureKeyStore, load_or_create_local_database_key, validate_local_database_key,
    };
    pub use crate::platform::{
        MediaRuntimeConfig, MediaRuntimeKind, NativeRuntimeAssembler, PlatformKind,
        RuntimeAssembler, RuntimeComponents, RuntimeConfig, StorageConfig, StorageEncryptionConfig,
        StorageKind,
    };
    pub use crate::plugin::{
        SdkCapabilityPlugin, SdkCapabilityRegistry, SdkPluginEventManifest, SdkPluginManifest,
        SdkPluginOperationManifest, SdkPluginPermissionManifest, SdkPluginUiKitManifest,
    };
    pub use crate::shared::error::{ErrorCode, FlareError, Result};
    #[cfg(feature = "lifecycle-sqlite")]
    pub use crate::shared::util::sqlite_store::ensure_core_sqlite_schema_registered;
    pub use crate::shared::util::{generate_core_token, resolve_user_db_path};
    #[cfg(feature = "lifecycle-sqlite")]
    pub use crate::shared::util::{
        open_sqlite_store_for_user_with_secure_key_store,
        sqlite_security_config_from_secure_key_store,
    };
    pub use crate::spi::metrics::{
        HistogramSnapshot, InMemoryMetricsSink, MetricLabel, MetricsRecorder, MetricsSink,
        MetricsSnapshot,
    };
    pub use flare_access::{AccessConfig, DevelopmentAccessPoint, DevelopmentEndpoints, TrustMode};
}
