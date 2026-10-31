//! 领域层：消息、会话、用户展示模型
//!
//! 所有模型均包含展示用**显示名称（display_name）**与**头像（avatar_url）**，
//! 由本地 UserProfile 缓存或同步填充，供 UI 列表与详情展示。

pub mod conversation;
pub mod display_name;
mod download_manifest;
mod media_cache;
mod media_progressive;
mod message;
pub mod message_actions;
mod pending_send;
mod repository;
mod sync;
mod sync_cursor;
mod upload_manifest;
mod user_profile;

pub use conversation::*;
pub use display_name::{
    ConversationTitleInput, DisplayName, MAX_TITLE_MEMBER_NAMES, NameEntity, NameSource,
    ParticipantDisplaySnapshot, PersonNameCandidates, business_name, conversation_shows_person,
    group_is_titled_by_members, id_tail, member_nicknames_apply, real_name,
    resolve_conversation_title, resolve_person_name, stored_name_is_an_id,
};
pub use download_manifest::{DownloadManifestState, MediaDownloadManifestVo, MediaDownloadPartVo};
pub use media_cache::{
    DEFAULT_MEDIA_CACHE_MAX_BYTES, MediaCacheEntryVo, MediaCacheStatsVo, UserDownloadDirectoryVo,
    UserFileDownloadResultVo,
};
pub use media_progressive::{MediaProgressiveEvent, MediaProgressiveStage, MediaProgressiveState};
pub use message::*;
pub use pending_send::PendingSendVo;
pub(crate) use repository::plugin_command_repository::{command_error, valid_key};
pub use repository::*;
pub use sync::*;
pub use sync_cursor::SyncCursorVo;
pub use upload_manifest::{
    DirectUploadTransportKindVo, MediaUploadManifestVo, MediaUploadPartVo, UploadManifestState,
    UploadSourceKind,
};
pub use user_profile::UserProfile;
