//! NotifyCenter 手上的用户级通知设置：上线时读、本机改、别的设备改了收到信号再读；会话级的
//! 「全部消息通知」随信号写进本地会话。

use prost::Message as _;

use super::*;

impl NotifyCenter {
    // ---------------------------------------------------------------------------------------
    // 用户级通知设置
    // ---------------------------------------------------------------------------------------

    /// 当前的用户级通知设置：手上有就用手上的（上线时读过、变更信号会刷新），没有就去读。
    pub(crate) async fn settings(&self) -> Result<UserNotificationSettings> {
        if let Some(settings) = self.cached_settings() {
            return Ok(settings);
        }
        self.refresh_settings().await
    }

    /// 向服务端读一次。手上的版本更新时（读的同时本机刚改过）保留手上的。
    pub(crate) async fn refresh_settings(&self) -> Result<UserNotificationSettings> {
        let response = self.sync.request_user_notification_settings().await?;
        let settings = match response.payload {
            Some(SyncResPayload::GetUserNotificationSettings(res)) => {
                settings_of(res.settings.as_ref())
            }
            _ => return Err(unexpected_response()),
        };
        Ok(self.adopt_settings(settings, false))
    }

    /// 部分更新，返回写入后的完整设置。
    pub(crate) async fn update_settings(
        &self,
        request: UpdateUserNotificationSettingsRequest,
    ) -> Result<UserNotificationSettings> {
        if !request.changes_something() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "the update changes nothing",
            ));
        }
        if request.quiet_hours.is_some() && request.clear_quiet_hours {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "quietHours and clearQuietHours cannot be combined",
            ));
        }
        if request
            .quiet_hours
            .is_some_and(|quiet_hours| !quiet_hours.is_valid())
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "quiet hours minutes must be below 1440 and weekdays within 0x7F",
            ));
        }
        let response: SyncRes = self
            .sync
            .update_user_notification_settings(request.to_proto())
            .await?;
        let settings = match response.payload {
            Some(SyncResPayload::UserNotificationSettings(res)) => {
                settings_of(res.settings.as_ref())
            }
            _ => return Err(unexpected_response()),
        };
        Ok(self.adopt_settings(settings, true))
    }

    pub(super) fn adopt_settings(
        &self,
        settings: UserNotificationSettings,
        written: bool,
    ) -> UserNotificationSettings {
        let preview_changed = {
            let mut cached = self
                .settings
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(current) = cached.as_ref()
                && !written
                && current.settings_version > settings.settings_version
            {
                return current.clone();
            }
            let previous = cached.as_ref().map(|current| current.preview);
            *cached = Some(settings.clone());
            previous != Some(settings.preview)
        };
        // 通知快照里写着预览设置。
        if preview_changed {
            self.snapshot_changed();
        }
        settings
    }

    /// 别的设备改了设置（`sync:<用户>` 收件箱上的信号）：用户级的去重新读，会话级的「全部消息通知」
    /// 写进本地会话。信号只是提示，内容以读取结果为准。
    pub(crate) fn on_user_settings_changed(&self, payload: &[u8]) {
        let changed = match UserSettingsChanged::decode(payload) {
            Ok(changed) => changed,
            Err(error) => {
                tracing::debug!(%error, "malformed user settings change signal");
                return;
            }
        };
        let center = self.me.clone();
        let conversation_id = changed.conversation_id.trim().to_owned();
        if conversation_id.is_empty() {
            let known = self
                .cached_settings()
                .map(|settings| settings.settings_version);
            if known.is_some_and(|version| version >= changed.settings_version) {
                return;
            }
            spawn_background(async move {
                if let Some(center) = center.upgrade()
                    && let Err(error) = center.refresh_settings().await
                {
                    tracing::debug!(%error, "re-reading user notification settings failed");
                }
            });
        } else {
            let value = changed.notify_all;
            spawn_background(async move {
                if let Some(center) = center.upgrade() {
                    center.apply_notify_all(&conversation_id, value).await;
                }
            });
        }
    }

    pub(super) async fn apply_notify_all(&self, conversation_id: &str, value: bool) {
        let conversation = match self.stores.conversations.get(conversation_id).await {
            Ok(Some(conversation)) => conversation,
            Ok(None) => return,
            Err(error) => {
                tracing::debug!(%error, "reading the conversation for notify_all failed");
                return;
            }
        };
        if notify_all(&conversation) == value {
            return;
        }
        if let Err(error) = self
            .stores
            .conversations
            .set_notify_all(conversation_id, value)
            .await
        {
            tracing::warn!(%error, "saving notify_all failed");
            return;
        }
        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Updated {
                conversation_id: conversation_id.to_owned(),
            }));
    }
}

fn unexpected_response() -> FlareError {
    FlareError::localized(
        ErrorCode::GeneralError,
        "unexpected user notification settings sync response",
    )
}

fn settings_of(
    settings: Option<&flare_proto::common::UserNotificationSettings>,
) -> UserNotificationSettings {
    settings.map_or_else(
        UserNotificationSettings::default,
        UserNotificationSettings::from_proto,
    )
}
