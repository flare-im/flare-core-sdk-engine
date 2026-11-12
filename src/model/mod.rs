//! 协议层模型 — 与 flare_proto 对齐；内容构建与解码实现位于 `crate::content`

pub mod conversation;
pub mod conversation_user_settings;
pub mod event;
pub mod media;
pub mod message;
pub mod notification;
pub mod receipt;
pub mod search;
pub mod sync;
pub mod timeline;

pub use crate::content::{
    BuiltContent, ContentBuilder, DEFAULT_STICKER_DISPLAY_SIDE, DecodedContent, Elem,
    MessageBuilder, MessagePreviewElem, NormalizeOutput, PreviewStoragePayload, RichDocDerived,
    RichDocV2Error, decode_content, decode_content_bytes, decoded_content_to_elem,
    derive_from_json_str, derive_from_value, normalize_from_doc_json, normalize_from_html,
    normalize_from_markdown, validate_doc_json,
};
pub use crate::domain::{
    MediaCacheEntryVo, MediaCacheStatsVo, total_unread, unread_counted_in_total,
};
pub use conversation::{Conversation, ConversationParticipant, ConversationSummary};
pub use conversation_user_settings::{
    EXT_NOTIFY_ALL, EXT_SETTINGS_DIRTY, EXT_USER_SETTINGS_VERSION, apply_remote_settings_version,
    is_settings_dirty, mark_settings_dirty, notify_all, set_notify_all, set_notify_all_in,
    user_settings_version,
};
pub use event::{Event, EventType};
pub use media::{
    MediaAccessUrl, MediaDestinationDescriptor, MediaDestinationKind, MediaResolvedAccess,
    RenderableMedia, RenderableMediaKind, UploadOptions, UploadedMedia,
};
pub use message::{
    ConversationType, DeleteScope, DeleteType, IMMessage, MarkType, Message, MessageSource,
    MessageStatus, MessageType, ReactionAction, SendAck, SendAckDurability,
};
pub use receipt::{MessageReadSummary, MessageReceiptStatus, MessageReceiptView};
pub use search::{ConversationListQuery, MessageSearchKind, MessageSearchQuery};
pub use sync::{
    ConversationHistoryBackfillResponse, ConversationVersion, StartupHomeSyncRequest,
    StartupHomeSyncResponse, SyncConversationSummariesRequest, SyncConversationSummariesResponse,
};
pub use timeline::{
    BootstrapHomeTimelineRequest, CloseViewRequest, CloseViewResponse,
    ConversationTimelineSnapshot, HomeTimelineSnapshot, LoadNewerTimelineViewRequest,
    LoadOlderTimelineViewRequest, OpenConversationListViewRequest, OpenConversationTimelineRequest,
    OpenTimelineViewRequest, TimelineAnchor, TimelineSyncState, TimelineViewPageResponse,
    TimelineViewRequest, TimelineViewSnapshot, ViewDelta, ViewDeltaKind, ViewDeltaOp,
    ViewOpenResponse, ViewSnapshot, ViewUpdate, ViewUpdateKind, normalized_conversation_limit,
    normalized_message_limit,
};
