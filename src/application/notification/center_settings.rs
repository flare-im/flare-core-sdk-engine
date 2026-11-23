//! NotifyCenter 手上的用户级通知设置：上线时读、本机改、别的设备改了收到信号再读；会话级的
//! 「全部消息通知」随信号写进本地会话。

use prost::Message as _;

use super::*;

impl NotifyCenter {
    // ---------------------------------------------------------------------------------------
    // 用户级通知设置
    // ---------------------------------------------------------------------------------------

    /// The notification settings projection belongs to one authenticated account.
    /// Store its versioned value alongside the other durable sync projections.
    pub(crate) fn bind_settings_scope(&self, tenant: &str, user: &str) {
        let key = format!("notification_settings:{}:{tenant}:{user}", tenant.len());
        if self.settings_scope.set(key).is_ok() {
            let center = self.me.clone();
            spawn_background(async move {
                if let Some(center) = center.upgrade() {
                    center.restore_settings().await;
                }
            });
        }
    }

    pub(super) async fn restore_settings(&self) -> Option<UserNotificationSettings> {
        if let Some(settings) = self.cached_settings() {
            return Some(settings);
        }
        let key = self.settings_scope.get()?;
        let _io = self.settings_io.lock().await;
        if let Some(settings) = self.cached_settings() {
            return Some(settings);
        }
        let raw = match self.stores.cursors.get_raw(key).await {
            Ok(value) => value?,
            Err(error) => {
                tracing::debug!(%error, "cannot restore user notification settings");
                return None;
            }
        };
        let settings = serde_json::from_str(&raw).ok()?;
        Some(self.adopt_settings(settings, false))
    }

    pub(super) async fn commit_settings(
        &self,
        settings: UserNotificationSettings,
        written: bool,
    ) -> UserNotificationSettings {
        let _io = self.settings_io.lock().await;
        let settings = self.adopt_settings(settings, written);
        if !self.ended()
            && let Some(key) = self.settings_scope.get()
            && let Ok(payload) = serde_json::to_string(&settings)
            && let Err(error) = self.stores.cursors.save_raw(key, &payload).await
        {
            tracing::debug!(%error, "cannot store user notification settings");
        }
        settings
    }

    /// 从内存或本地库读上次确认的设置。上线和变更信号会刷新它。
    pub(crate) async fn settings(&self) -> Result<UserNotificationSettings> {
        if let Some(settings) = self.restore_settings().await {
            return Ok(settings);
        }
        self.refresh_settings().await
    }

    /// 向服务端读一次。手上的版本更新时（读的同时本机刚改过）保留手上的。
    pub(crate) async fn refresh_settings(&self) -> Result<UserNotificationSettings> {
        self.restore_settings().await;
        let response = self.sync.request_user_notification_settings().await?;
        let settings = match response.payload {
            Some(SyncResPayload::GetUserNotificationSettings(res)) => {
                settings_of(res.settings.as_ref())
            }
            _ => return Err(unexpected_response()),
        };
        Ok(self.commit_settings(settings, false).await)
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
        Ok(self.commit_settings(settings, true).await)
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
