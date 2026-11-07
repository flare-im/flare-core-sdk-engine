//! Application layer.
//!
//! `commands` and `queries` express the CQRS boundary; `usecases` orchestrate
//! core IM flows; `projections` maintain local read models; `services` contains
//! application-level dedupe, convergence, and message construction helpers.

mod adapters;
pub mod callbacks;
pub mod commands;
pub mod lifecycle;
pub mod notification;
pub mod projections;
pub mod queries;
pub mod services;
pub mod sync_task;
pub(crate) mod usecases;

pub use adapters::SyncProtocolAdapter;
pub use callbacks::{
    FileDownloadProgress, FileDownloadProgressCallback, UploadPhase, UploadProgress,
    UploadProgressCallback, UserFileDownloadRequest,
};
pub use commands::{RecallMessageCommand, SendMessageCommand};
pub use lifecycle::{
    ConversationLocalLifecycle, LocalConversationClearResult, LocalConversationVisibility,
};
pub use notification::{
    InboundDelivery, InboundNotificationView, NotificationDispatchReport, NotificationHandleResult,
    NotificationHandler, NotificationHandlerRegistry,
};
pub use projections::{
    ConversationDisplayProjectionApplier, ConversationDisplaySnapshot,
    ParticipantDisplayProjectionApplier, UserProfileProjectionApplier, resolve_display_name,
};
pub use queries::{
    GetConversationQuery, GetConversationsQuery, GetMessagesQuery, SearchMessagesQuery,
};
pub use services::{
    BuildCardRequest, BuildLinkCardRequest, BuildLocationRequest, BuildMiniProgramRequest,
    BuildRichDocRequest, BuildScheduleRequest, BuildStickerRequest, MessageBuilderService,
};
pub use sync_task::{
    ConversationSettingsSyncTask, ConversationsSyncTask, KeyEventsSyncTask, MessagesSyncTask,
    ReadStatesSyncTask,
};
