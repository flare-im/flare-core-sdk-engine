//! 通知相关的对外数据：要展示的一条通知、用户级通知设置及其读写请求。
//!
//! 通知本身（[`NotificationPresentation`]）由链路在线时的 NotifyCenter 与远程推送入口共同产出，
//! 形状一致，宿主只认一种；用户级设置与协议里的 `UserNotificationSettings` 一一对应，
//! 由核心存储、经同步通道读写。

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One notification to show (or to replace in place).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationPresentation {
    /// Notification identity: one per conversation. Used as the iOS notification identifier and
    /// the Android notification tag, so a later notification for the same conversation replaces it.
    pub collapse_id: String,
    /// Grouping key in the notification centre.
    pub thread_id: String,
    /// Conversation the notification belongs to; empty for the digest notification.
    pub conversation_id: String,
    /// Server id of the newest message it stands for; empty for a catch-up without a loaded
    /// message and for the digest notification.
    pub message_id: String,
    /// Conversation seq of the newest message it stands for; 0 for the digest notification.
    pub seq: u64,
    /// Title, already rendered in the app's language.
    pub title: String,
    /// Body, already rendered in the app's language.
    pub body: String,
    /// Unread messages the notification stands for, at least 1.
    pub count: u32,
    /// Whether to play a sound; a silent replacement never sounds.
    pub sound: bool,
    /// messages, quiet, mentions or urgent (Android channel flare.<channel>).
    pub channel: String,
    /// system (an OS notification) or inApp (a banner inside the running app).
    pub surface: String,
}

impl std::fmt::Debug for NotificationPresentation {
    // 标题与正文是消息预览，不进日志。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationPresentation")
            .field("collapse_id", &self.collapse_id)
            .field("conversation_id", &self.conversation_id)
            .field("seq", &self.seq)
            .field("count", &self.count)
            .field("sound", &self.sound)
            .field("channel", &self.channel)
            .field("surface", &self.surface)
            .finish_non_exhaustive()
    }
}

/// Payload of notification.clear: remove these notifications of the app.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationClearEvent {
    /// Collapse ids of the notifications to remove.
    pub collapse_ids: Vec<String>,
    /// Remove every notification of the app (the session ended); collapseIds is then empty.
    pub all: bool,
}

/// Payload of notification.badge: the app badge as an absolute value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationBadgeEvent {
    /// Unread total, counted like the conversation list (archived conversations do not count,
    /// muted ones only with mentions).
    pub count: u32,
}

/// How much of a message a notification shows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NotificationPreview {
    /// Sender and content.
    #[default]
    Full,
    /// Only who sent it; the body says that a message arrived.
    SenderOnly,
    /// Neither sender nor content: a generic title and body.
    Hidden,
}

/// What a phone does with an alert while another device of the same user is in use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MultiDeviceAlertPolicy {
    /// Wait about 15 seconds; reading the conversation elsewhere in that time cancels the alert.
    #[default]
    Delay,
    /// Alert at once, as if no other device were in use.
    Immediate,
    /// Do not alert; only update the badge.
    BadgeOnly,
}

/// Do-not-disturb hours, in the device's local time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuietHours {
    /// Start, minute of the day (0..1439), inclusive.
    pub start_minute: u32,
    /// End, minute of the day (0..1439), exclusive. Equal to the start: all day. Smaller than the
    /// start: the window crosses midnight.
    pub end_minute: u32,
    /// Days the window starts on: bit0 Monday .. bit6 Sunday; 0 means every day.
    pub weekdays: u32,
    /// Whether messages that mention the user still ring inside the window.
    pub allow_mentions: bool,
}

/// The user's notification settings, shared by all of the user's devices.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserNotificationSettings {
    /// Do-not-disturb hours; absent when the user has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_hours: Option<QuietHours>,
    /// How much a notification shows.
    #[serde(default)]
    pub preview: NotificationPreview,
    /// What a phone does while another device of the user is in use.
    #[serde(default)]
    pub multi_device: MultiDeviceAlertPolicy,
    /// Version of the settings; 0 when the user never changed them. Pass it back as
    /// baseSettingsVersion so a concurrent change elsewhere is not overwritten.
    #[serde(default)]
    pub settings_version: u64,
}

/// A partial update of the user's notification settings: items left out keep their value; at
/// least one item must be given.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateUserNotificationSettingsRequest {
    /// New do-not-disturb hours, replacing the old ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_hours: Option<QuietHours>,
    /// Remove the do-not-disturb hours. Rejected together with quietHours.
    #[serde(default)]
    pub clear_quiet_hours: bool,
    /// New preview mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<NotificationPreview>,
    /// New multi-device policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multi_device: Option<MultiDeviceAlertPolicy>,
    /// The settingsVersion the change is based on; 0 skips the check. When the stored settings
    /// changed in the meantime the whole update is rejected: read them again and retry.
    #[serde(default)]
    pub base_settings_version: u64,
}

/// The conversation the user is looking at.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetNotificationFocusRequest {
    /// Conversation on screen; absent when no conversation is open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
}

/// Whether the platform presents alerts itself while the app is in the background.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetBackgroundPresenterRequest {
    /// The platform push module posts local notifications for the engine while the app is in
    /// the background.
    pub attached: bool,
}

/// Where the engine keeps the notification snapshot an iOS notification service extension reads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetNotificationSnapshotDirectoryRequest {
    /// Directory shared with the extension (the App Group container on iOS). The engine writes
    /// snapshot.json there and reads the extension's receipts.log from it. Absent: stop writing;
    /// files already there are left alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
}

/// One background refresh the platform granted the app (iOS BGAppRefreshTask).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackgroundRefreshRequest {
    /// Time the platform grants, in milliseconds; the engine uses at most 25000 of it.
    pub budget_ms: u32,
}

/// How a background refresh ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundRefreshResult {
    /// The link came up and the incremental sync finished within the budget. False when the
    /// budget ran out or no session could connect.
    pub completed: bool,
}

impl UserNotificationSettings {
    /// 由协议里的设置转换。未指明的预览按完整、未指明的多端策略按延迟（与服务端一致）；
    /// 不认识的取值按最保守的一档：预览隐藏、多端延迟。
    pub(crate) fn from_proto(settings: &flare_proto::common::UserNotificationSettings) -> Self {
        use flare_proto::common::{
            MultiDeviceAlertPolicy as WirePolicy, NotificationPreview as WirePreview,
        };
        let preview = match WirePreview::try_from(settings.preview) {
            Ok(WirePreview::Unspecified | WirePreview::Full) => NotificationPreview::Full,
            Ok(WirePreview::SenderOnly) => NotificationPreview::SenderOnly,
            Ok(WirePreview::Hidden) | Err(_) => NotificationPreview::Hidden,
        };
        let multi_device = match WirePolicy::try_from(settings.multi_device) {
            Ok(WirePolicy::Immediate) => MultiDeviceAlertPolicy::Immediate,
            Ok(WirePolicy::BadgeOnly) => MultiDeviceAlertPolicy::BadgeOnly,
            Ok(WirePolicy::Unspecified | WirePolicy::Delay) | Err(_) => {
                MultiDeviceAlertPolicy::Delay
            }
        };
        Self {
            quiet_hours: settings.quiet_hours.as_ref().map(|quiet| QuietHours {
                start_minute: quiet.start_minute,
                end_minute: quiet.end_minute,
                weekdays: quiet.weekdays,
                allow_mentions: quiet.allow_mentions,
            }),
            preview,
            multi_device,
            settings_version: settings.settings_version,
        }
    }
}

impl NotificationPreview {
    pub(crate) fn to_proto(self) -> flare_proto::common::NotificationPreview {
        use flare_proto::common::NotificationPreview as Wire;
        match self {
            Self::Full => Wire::Full,
            Self::SenderOnly => Wire::SenderOnly,
            Self::Hidden => Wire::Hidden,
        }
    }
}

impl MultiDeviceAlertPolicy {
    pub(crate) fn to_proto(self) -> flare_proto::common::MultiDeviceAlertPolicy {
        use flare_proto::common::MultiDeviceAlertPolicy as Wire;
        match self {
            Self::Delay => Wire::Delay,
            Self::Immediate => Wire::Immediate,
            Self::BadgeOnly => Wire::BadgeOnly,
        }
    }
}

impl QuietHours {
    /// 分钟在一天之内、星期只用低 7 位。服务端同样校验，这里先挡住明显写错的请求。
    pub(crate) fn is_valid(&self) -> bool {
        self.start_minute < 1_440 && self.end_minute < 1_440 && self.weekdays <= 0x7f
    }

    pub(crate) fn to_proto(self) -> flare_proto::common::QuietHours {
        flare_proto::common::QuietHours {
            start_minute: self.start_minute,
            end_minute: self.end_minute,
            weekdays: self.weekdays,
            allow_mentions: self.allow_mentions,
        }
    }
}

impl UpdateUserNotificationSettingsRequest {
    /// 至少改了一项。
    pub(crate) fn changes_something(&self) -> bool {
        self.quiet_hours.is_some()
            || self.clear_quiet_hours
            || self.preview.is_some()
            || self.multi_device.is_some()
    }

    pub(crate) fn to_proto(&self) -> flare_proto::common::UserNotificationSettingsSync {
        flare_proto::common::UserNotificationSettingsSync {
            quiet_hours: self.quiet_hours.map(QuietHours::to_proto),
            clear_quiet_hours: self.clear_quiet_hours,
            preview: self.preview.map(|preview| preview.to_proto() as i32),
            multi_device: self.multi_device.map(|policy| policy.to_proto() as i32),
            base_settings_version: self.base_settings_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 线上 JSON 是驼峰；没设置的可选项不出现。
    #[test]
    fn settings_use_camel_case_on_the_wire() {
        let settings = UserNotificationSettings {
            quiet_hours: Some(QuietHours {
                start_minute: 1_320,
                end_minute: 420,
                weekdays: 0,
                allow_mentions: true,
            }),
            preview: NotificationPreview::SenderOnly,
            multi_device: MultiDeviceAlertPolicy::BadgeOnly,
            settings_version: 3,
        };
        assert_eq!(
            serde_json::to_value(&settings).unwrap(),
            serde_json::json!({
                "quietHours": {"startMinute": 1320, "endMinute": 420, "weekdays": 0, "allowMentions": true},
                "preview": "senderOnly",
                "multiDevice": "badgeOnly",
                "settingsVersion": 3
            })
        );
        assert_eq!(
            serde_json::to_value(UserNotificationSettings::default()).unwrap(),
            serde_json::json!({"preview": "full", "multiDevice": "delay", "settingsVersion": 0})
        );
    }

    /// 撤掉与角标两个事件的载荷是驼峰的固定形状。
    #[test]
    fn event_payloads_use_camel_case_on_the_wire() {
        let clear = NotificationClearEvent {
            collapse_ids: vec!["c:1".into()],
            all: false,
        };
        assert_eq!(
            serde_json::to_value(&clear).unwrap(),
            serde_json::json!({"collapseIds": ["c:1"], "all": false})
        );
        assert_eq!(
            serde_json::to_value(NotificationBadgeEvent { count: 3 }).unwrap(),
            serde_json::json!({"count": 3})
        );
    }

    /// 快照目录与后台刷新：驼峰键，没给目录时键不出现，认不得的键拒绝。
    #[test]
    fn snapshot_directory_and_background_refresh_use_camel_case() {
        let request: SetNotificationSnapshotDirectoryRequest =
            serde_json::from_value(serde_json::json!({"directory": "/group/flare-notify"}))
                .unwrap();
        assert_eq!(request.directory.as_deref(), Some("/group/flare-notify"));
        assert_eq!(
            serde_json::to_value(SetNotificationSnapshotDirectoryRequest::default()).unwrap(),
            serde_json::json!({})
        );
        let refresh: BackgroundRefreshRequest =
            serde_json::from_value(serde_json::json!({"budgetMs": 25000})).unwrap();
        assert_eq!(refresh.budget_ms, 25_000);
        assert!(
            serde_json::from_value::<BackgroundRefreshRequest>(serde_json::json!({"budget": 1}))
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(BackgroundRefreshResult { completed: true }).unwrap(),
            serde_json::json!({"completed": true})
        );
    }

    /// 协议里未指明的取值按缺省，不认识的按最保守的一档。
    #[test]
    fn wire_settings_map_onto_the_dto() {
        let wire = flare_proto::common::UserNotificationSettings {
            quiet_hours: None,
            preview: 0,
            multi_device: 0,
            settings_version: 0,
        };
        assert_eq!(
            UserNotificationSettings::from_proto(&wire),
            UserNotificationSettings::default()
        );
        let odd = flare_proto::common::UserNotificationSettings {
            preview: 99,
            multi_device: 99,
            ..wire
        };
        let settings = UserNotificationSettings::from_proto(&odd);
        assert_eq!(settings.preview, NotificationPreview::Hidden);
        assert_eq!(settings.multi_device, MultiDeviceAlertPolicy::Delay);
    }

    /// 部分更新：没带的项在协议里也不带，清除勿扰时段单独一个开关。
    #[test]
    fn an_update_carries_only_what_changes() {
        let request: UpdateUserNotificationSettingsRequest = serde_json::from_value(
            serde_json::json!({"preview": "hidden", "baseSettingsVersion": 4}),
        )
        .unwrap();
        assert!(request.changes_something());
        let wire = request.to_proto();
        assert_eq!(
            wire.preview,
            Some(flare_proto::common::NotificationPreview::Hidden as i32)
        );
        assert_eq!(wire.multi_device, None);
        assert_eq!(wire.quiet_hours, None);
        assert!(!wire.clear_quiet_hours);
        assert_eq!(wire.base_settings_version, 4);
        assert!(!UpdateUserNotificationSettingsRequest::default().changes_something());
        assert!(
            serde_json::from_value::<UpdateUserNotificationSettingsRequest>(
                serde_json::json!({"previewMode": "hidden"})
            )
            .is_err(),
            "认不得的键拒绝"
        );
    }
}
