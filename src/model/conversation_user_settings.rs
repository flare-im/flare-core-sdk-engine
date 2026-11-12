//! 用户级会话偏好 ext 键与辅助函数（多端 LWW 同步）。

use super::Conversation;

pub const EXT_USER_SETTINGS_VERSION: &str = "user_settings_version";
pub const EXT_SETTINGS_DIRTY: &str = "settings_dirty";
/// 大群里当前用户显式开启了「全部消息通知」（`ConversationUserSettings.notify_all`）。
pub const EXT_NOTIFY_ALL: &str = "notify_all";

pub fn user_settings_version(conversation: &Conversation) -> u64 {
    conversation
        .ext
        .get(EXT_USER_SETTINGS_VERSION)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

pub fn is_settings_dirty(conversation: &Conversation) -> bool {
    conversation
        .ext
        .get(EXT_SETTINGS_DIRTY)
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

pub fn mark_settings_dirty(conversation: &mut Conversation) {
    conversation
        .ext
        .insert(EXT_SETTINGS_DIRTY.to_string(), "1".to_string());
}

pub fn apply_remote_settings_version(conversation: &mut Conversation, version: u64) {
    conversation
        .ext
        .insert(EXT_USER_SETTINGS_VERSION.to_string(), version.to_string());
    conversation
        .ext
        .insert(EXT_SETTINGS_DIRTY.to_string(), "0".to_string());
}

/// 大群里是否显式开启了「全部消息通知」；没有记录时为否。
pub fn notify_all(conversation: &Conversation) -> bool {
    notify_all_in(&conversation.ext).unwrap_or(false)
}

/// ext 里记录的「全部消息通知」；没有记录时为 `None`。
pub fn notify_all_in(ext: &std::collections::HashMap<String, String>) -> Option<bool> {
    ext.get(EXT_NOTIFY_ALL)
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

pub fn set_notify_all(conversation: &mut Conversation, notify_all: bool) {
    set_notify_all_in(&mut conversation.ext, notify_all);
}

/// 把「全部消息通知」记进 ext。
pub fn set_notify_all_in(ext: &mut std::collections::HashMap<String, String>, notify_all: bool) {
    ext.insert(
        EXT_NOTIFY_ALL.to_string(),
        if notify_all { "1" } else { "0" }.to_string(),
    );
}
