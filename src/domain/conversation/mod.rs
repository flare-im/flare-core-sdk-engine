pub mod id;
mod identity;
mod local_clear;
mod message_projection;
mod models;
mod read;
mod read_position;
mod summary_merge;
mod unread_total;

pub use id::*;
pub use identity::{
    ConversationIdRewrite, ConversationIdentityService, USER_INBOX_PREFIX, is_user_inbox,
    user_inbox_id, user_inbox_owner,
};
pub use local_clear::{
    EXT_LOCAL_CLEARED_THROUGH_SEQ, filter_messages_after_clear, local_cleared_through_seq,
    message_visible_after_clear, set_local_cleared_through_seq, sync_visibility_floor,
};
pub use message_projection::{
    EXT_MESSAGE_PROJECTION, MessageProjectionReceipt, copy_message_projection,
    message_projection_coverage, message_projection_matches, message_projection_receipt,
    preserve_message_projection, projected_business_type,
};
pub use models::ConversationReadDecision;
pub use read::ConversationReadService;
pub(crate) use read::{pending_read_seq, set_read_intent};
pub use read_position::ReadPosition;
pub use summary_merge::{
    merge_incoming_conversation_summary, preserve_local_remark, preserve_local_single_chat_channel,
};
pub use unread_total::{total_unread, unread_counted_in_total};
