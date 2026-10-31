//! 会话摘要与本地投影字段合并。
//!
//! 服务端会话摘要是远端投影，不允许回滚 SDK 本地已经确认的读位、最新消息、
//! 本地设置和单聊身份修复结果。所有 store 在保存摘要前都应走这里。

use crate::model::conversation::ConversationType;
use crate::model::{Conversation, EXT_SETTINGS_DIRTY, EXT_USER_SETTINGS_VERSION};

use super::{ReadPosition, local_cleared_through_seq};

/// 摘要同步时保留本地 remark（服务端摘要不含通讯录备注）。
#[must_use]
pub fn preserve_local_remark(incoming: Option<&str>, local: Option<&str>) -> Option<String> {
    if incoming.map(str::trim).is_some_and(|v| !v.is_empty()) {
        return incoming.map(str::to_string);
    }
    local
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// 单聊 channel_id 以本地已修复的 peer 为准。
#[must_use]
pub fn preserve_local_single_chat_channel(
    conversation_type: ConversationType,
    incoming_channel: &str,
    local_channel: &str,
) -> String {
    if !conversation_type.is_single_chat_conversation() {
        return incoming_channel.to_string();
    }
    let local = local_channel.trim();
    let incoming = incoming_channel.trim();
    if !local.is_empty() && (incoming.is_empty() || incoming != local) {
        local.to_string()
    } else {
        incoming.to_string()
    }
}

/// 将 incoming 会话摘要合并到本地会话投影。
#[must_use]
pub fn merge_incoming_conversation_summary(
    local: Option<&Conversation>,
    incoming: &Conversation,
) -> Conversation {
    let mut merged = incoming.clone();
    let merged_read = ReadPosition::merge_with_incoming_summary(
        local
            .map(ReadPosition::from_conversation)
            .unwrap_or_default(),
        ReadPosition::from_conversation(incoming),
    );
    merged.last_read_seq = merged_read.last_read_seq;
    merged.unread_count = merged_read.unread_count;
    merged.max_seq = merged.max_seq.max(merged_read.max_seq);

    if let Some(local) = local {
        merge_user_settings(local, &mut merged);
        preserve_local_latest_message(local, incoming, &mut merged);

        merged.remark = preserve_local_remark(merged.remark.as_deref(), local.remark.as_deref());
        merged.channel_id = preserve_local_single_chat_channel(
            local.conversation_type,
            &merged.channel_id,
            &local.channel_id,
        );
    }

    // 名字列只存业务名：等于自己 id（对端、群或会话 id）的值不是名字；备注另存在 `remark`，
    // 不复制进名字列。
    merged.display_name = crate::domain::business_name(
        &merged.display_name,
        &[&merged.channel_id, &merged.conversation_id],
    );

    let local_cleared_floor = crate::domain::sync_visibility_floor(&merged);
    merged.visible_after_seq = local_cleared_floor;
    if local_cleared_floor > 0 {
        merged.last_read_seq = merged.last_read_seq.max(local_cleared_floor);
        merged.max_seq = merged.max_seq.max(local_cleared_floor);
        if incoming.max_seq <= local_cleared_floor {
            merged.last_message_id = None;
            merged.last_sender_id = None;
            merged.last_message_at = None;
            merged.last_message_preview = None;
            merged.last_message = None;
            merged.unread_count = 0;
        }
    }

    merged.unread_count = merged
        .unread_count
        .min(ReadPosition::from_conversation(&merged).unread_upper_bound());
    merged
}

fn merge_user_settings(local: &Conversation, merged: &mut Conversation) {
    let local_version = settings_version(&local.ext);
    let incoming_version = settings_version(&merged.ext);
    let local_dirty = settings_dirty(&local.ext);

    if local_dirty && incoming_version <= local_version {
        merged.is_pinned = local.is_pinned;
        merged.is_muted = local.is_muted;
        merged.is_archived = local.is_archived;
        merged.draft = local.draft.clone();
        merged.ext = local.ext.clone();
    } else if incoming_version >= local_version {
        for (key, value) in &local.ext {
            merged
                .ext
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
        merged
            .ext
            .insert(EXT_SETTINGS_DIRTY.to_string(), "0".to_string());
    } else {
        merged.is_pinned = local.is_pinned;
        merged.is_muted = local.is_muted;
        merged.is_archived = local.is_archived;
        merged.draft = local.draft.clone();
        merged.ext = local.ext.clone();
    }
}

fn settings_version(ext: &std::collections::HashMap<String, String>) -> u64 {
    ext.get(EXT_USER_SETTINGS_VERSION)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

fn settings_dirty(ext: &std::collections::HashMap<String, String>) -> bool {
    ext.get(EXT_SETTINGS_DIRTY)
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn preserve_local_latest_message(
    local: &Conversation,
    incoming: &Conversation,
    merged: &mut Conversation,
) {
    let local_has_last_message = has_last_message(local);
    let incoming_has_last_message = has_last_message(incoming);
    let incoming_preview_is_empty = incoming
        .last_message_preview
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty);

    if local_has_last_message
        && (incoming.max_seq <= local.max_seq
            || !incoming_has_last_message
            || incoming_preview_is_empty)
    {
        merged.last_message_id = local.last_message_id.clone();
        merged.last_sender_id = local.last_sender_id.clone();
        merged.last_message_at = local.last_message_at;
        merged.last_message_preview = local.last_message_preview.clone();
        merged.last_message = local.last_message.clone();
        merged.last_sender_nickname = local.last_sender_nickname.clone();
        merged.last_sender_avatar_url = local.last_sender_avatar_url.clone();
    }
}

fn has_last_message(conversation: &Conversation) -> bool {
    conversation
        .last_message_id
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
        || conversation
            .last_message_preview
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
        || conversation.last_message_at.unwrap_or_default() > 0
}

#[cfg(test)]
mod tests {
    use super::{
        merge_incoming_conversation_summary, preserve_local_remark,
        preserve_local_single_chat_channel,
    };
    use crate::model::Conversation;
    use crate::model::conversation::ConversationType;

    #[test]
    fn the_merged_name_column_holds_neither_a_remark_nor_an_id() {
        let local = Conversation {
            conversation_id: "1A_cid".to_string(),
            conversation_type: crate::model::conversation::ConversationType::Single,
            channel_id: "peer".to_string(),
            remark: Some("好友备注".to_string()),
            ..Default::default()
        };
        let incoming = Conversation {
            conversation_id: "1A_cid".to_string(),
            conversation_type: crate::model::conversation::ConversationType::Single,
            channel_id: "peer".to_string(),
            display_name: "peer".to_string(),
            ..Default::default()
        };
        let merged = merge_incoming_conversation_summary(Some(&local), &incoming);
        assert_eq!(merged.remark.as_deref(), Some("好友备注"));
        assert_eq!(
            merged.display_name, "",
            "neither the remark nor the peer id"
        );

        let named = Conversation {
            display_name: " Peer Shop ".to_string(),
            ..incoming.clone()
        };
        let merged = merge_incoming_conversation_summary(Some(&local), &named);
        assert_eq!(merged.display_name, "Peer Shop");
        let by_id = Conversation {
            display_name: "1A_cid".to_string(),
            ..incoming
        };
        assert_eq!(
            merge_incoming_conversation_summary(None, &by_id).display_name,
            ""
        );
    }

    #[test]
    fn preserve_remark_when_incoming_empty() {
        assert_eq!(
            preserve_local_remark(None, Some(" 备注 ")),
            Some("备注".to_string())
        );
    }

    #[test]
    fn preserve_incoming_remark_when_present() {
        assert_eq!(
            preserve_local_remark(Some("服务端"), Some("本地")),
            Some("服务端".to_string())
        );
    }

    #[test]
    fn preserve_local_single_chat_channel_when_incoming_is_display_name() {
        let channel =
            preserve_local_single_chat_channel(ConversationType::Single, "张三", "user_123");
        assert_eq!(channel, "user_123");
    }

    #[test]
    fn incoming_summary_does_not_roll_back_local_read_position() {
        let local = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 310,
            last_read_seq: 310,
            unread_count: 0,
            ..Default::default()
        };
        let incoming = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 310,
            last_read_seq: 20,
            unread_count: 17,
            ..Default::default()
        };

        let merged = merge_incoming_conversation_summary(Some(&local), &incoming);

        assert_eq!(merged.max_seq, 310);
        assert_eq!(merged.last_read_seq, 310);
        assert_eq!(merged.unread_count, 0);
    }

    #[test]
    fn incoming_empty_preview_does_not_clear_local_latest_message() {
        let local = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 100,
            last_message_id: Some("msg-100".to_string()),
            last_sender_id: Some("u2".to_string()),
            last_message_at: Some(12_000),
            last_message_preview: Some("latest".to_string()),
            ..Default::default()
        };
        let incoming = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 100,
            last_read_seq: 90,
            unread_count: 10,
            ..Default::default()
        };

        let merged = merge_incoming_conversation_summary(Some(&local), &incoming);

        assert_eq!(merged.last_message_id.as_deref(), Some("msg-100"));
        assert_eq!(merged.last_message_preview.as_deref(), Some("latest"));
        assert_eq!(merged.last_message_at, Some(12_000));
    }
}
