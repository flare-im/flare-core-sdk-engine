//! 持久化（Repository + Store）
//!
//! - **repository**：仓储 trait 定义（Message/Conversation/PendingSend/UserProfile Query+Command）。
//! - **Domain Port**：`domain::repository` 定义 Message/Conversation/SyncCursor 统一端口，StoreProvider 直接持有。
//! - **memory**：纯内存实现，用于 fallback（如未配置 SQLite 时的 user_profiles）。
//! - **sqlite**（feature `storage-sqlite`）：SQLite 仓储实现，并实现 MessageStore/ConversationStore 适配。
//! - **layered**：仅 PendingSend 的内存缓存 + 后端分层。

pub mod db;

pub mod empty_stores;
pub mod memory;
pub mod memory_im;
pub mod minimal_provider;
pub mod timeline_coverage;

/// 待发送队列：内存 + 后端分层（推荐与 sqlite 组合使用）
pub mod layered;

/// IndexedDB 便捷接入：实现 domain Reader/Writer 或 MessageStorageBackend + MessageBackendAdapter
pub mod indexeddb_adapter;

#[cfg(feature = "storage-sqlite")]
pub mod sqlite;

pub use db::StoreProvider;
pub use empty_stores::MemorySyncCursorStore;
pub use indexeddb_adapter::{MessageBackendAdapter, MessageStorageBackend};
pub use layered::LayeredPendingSendStore;
pub use memory::{MemoryPendingSendStore, MemoryUserProfileStore};
pub use memory_im::in_memory_im_provider;
pub use minimal_provider::in_memory_empty_im_provider;
pub use timeline_coverage::{
    CursorBackedTimelineCoverageStore, MemoryTimelineCoverageStore, timeline_coverage_cursor_key,
};

#[cfg(feature = "storage-sqlite")]
pub use sqlite::{
    SqliteConversationParticipantRepo, SqliteConversationRepo, SqliteMediaCacheRepo,
    SqliteMessageRepo, SqlitePendingSendRepo, SqliteSyncCursorRepo, SqliteTimelineCoverageRepo,
    SqliteUploadManifestRepo, SqliteUserFileDownloadRepo, SqliteUserProfileRepo,
    init_schema as sqlite_init_schema,
};

pub(crate) mod search;
