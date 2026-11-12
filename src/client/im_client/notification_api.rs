//! 通知模块的对外入口：正在看的会话、用户级通知设置的读写、后台展示方、与通知扩展共用的目录、
//! 后台刷新。
//!
//! 焦点、后台展示方与共用目录不依赖登录：宿主常在登录前就报，记在客户端这一份，建连时交给引擎。
//! 设置的读写走同步通道，要求已登录且连着。

#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use super::IMClient;
use crate::model::notification::{
    BackgroundRefreshResult, UpdateUserNotificationSettingsRequest, UserNotificationSettings,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

impl IMClient {
    /// 宿主当前在看的会话（`None`：没有打开任何会话）。前台在看的会话来消息不提示；打开一个会话时它的
    /// 应用内横幅从头计数。不等锁、不等网络。
    pub fn set_notification_focus(&self, conversation_id: Option<String>) {
        self.notification_host.set_focus(conversation_id);
        let focus = self.notification_host.focus();
        // 拿不到锁（正在登录、登出）时只记在客户端：引擎读的就是这一份，横幅计数下次再从头。
        if let Ok(g) = self.read_inner()
            && let Some(engine) = g.engine.as_ref()
        {
            engine.notify_center().focus_changed(focus.as_deref());
        }
    }

    /// 用户级通知设置（勿扰时段、预览、多端策略）。上线时读过、别的设备改了会再读；手上没有时去服务端读。
    pub async fn notification_settings(&self) -> Result<UserNotificationSettings> {
        let center = self
            .with_engine_async(|engine| engine.notify_center().clone())
            .await?;
        center.settings().await
    }

    /// 部分更新用户级通知设置，返回写入后的完整设置；其余在线设备随后收到变更信号。
    pub async fn update_notification_settings(
        &self,
        request: UpdateUserNotificationSettingsRequest,
    ) -> Result<UserNotificationSettings> {
        let center = self
            .with_engine_async(|engine| engine.notify_center().clone())
            .await?;
        center.update_settings(request).await
    }

    /// 平台推送包在后台替引擎出本地通知（接上）或不再出（卸下）。接上后手机在后台由引擎确认并提示从
    /// 链路收到的消息；没接上时提示交给服务端推送。登录前报的记在客户端，建连时带上。
    pub async fn set_background_presenter(&self, attached: bool) -> Result<()> {
        self.notification_host.set_background_presenter(attached);
        let g = self.read_inner_async().await?;
        if let Some(engine) = g.engine.as_ref() {
            engine.set_background_presenter(attached).await;
        }
        Ok(())
    }

    /// 与 iOS 通知扩展共用的目录（App Group 容器）。会话装上期间引擎在这里写通知快照
    /// （`snapshot.json`），并从这里读扩展记下的到达回执（`receipts.log`）；登出时删掉快照。`None`
    /// 不再写，已有的文件不动。登录前就能报，记在客户端，跨登录保留。浏览器里没有通知扩展。
    pub async fn set_notification_snapshot_directory(
        &self,
        directory: Option<String>,
    ) -> Result<()> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = directory;
            Err(native_only("the notification snapshot needs a native host"))
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let directory = match directory
                .as_deref()
                .map(str::trim)
                .filter(|directory| !directory.is_empty())
            {
                Some(directory) => {
                    let path = std::path::PathBuf::from(directory);
                    if !path.is_absolute() {
                        return Err(FlareError::localized(
                            ErrorCode::InvalidParameter,
                            "the snapshot directory must be an absolute path",
                        ));
                    }
                    Some(path)
                }
                None => None,
            };
            let changed = self.notification_host.snapshot_directory() != directory;
            self.notification_host.set_snapshot_directory(directory);
            if changed {
                let g = self.read_inner_async().await?;
                if let Some(engine) = g.engine.as_ref() {
                    engine.notify_center().snapshot_directory_changed();
                }
            }
            Ok(())
        }
    }

    /// 平台给的一次后台刷新（iOS BGAppRefreshTask）：把链路拉起来做一次增量同步，再回到原来的做法
    /// （iOS 重新托管、关掉链路）。预算最多 25 秒；预算用完、没有会话、连不上时 `completed` 为假。
    /// 收尾时读进通知扩展记下的到达回执并报上去，没写出的通知快照写出去：进程随时会被挂起。
    pub async fn background_refresh(&self, budget_ms: u32) -> Result<BackgroundRefreshResult> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = budget_ms;
            Err(native_only("background refresh needs a native host"))
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            use crate::runtime::link::WakeSource;
            use crate::shared::util::{now_millis, timeout};

            let budget = Duration::from_millis(u64::from(budget_ms.min(BACKGROUND_REFRESH_CAP_MS)));
            let session = {
                let g = self.read_inner_async().await?;
                match (g.current_user_id.as_deref(), g.engine.as_ref()) {
                    (Some(user_id), Some(engine)) if !user_id.trim().is_empty() => {
                        Some((engine.link().clone(), engine.notify_center().clone()))
                    }
                    _ => None,
                }
            };
            let Some((link, notify)) = session else {
                return Ok(BackgroundRefreshResult { completed: false });
            };
            if budget.is_zero() {
                return Ok(BackgroundRefreshResult { completed: false });
            }
            let started = now_millis();
            let completed = link
                .wake_until_ready(WakeSource::BackgroundRefresh, budget)
                .await;
            // 剩下的时间收尾：回执与快照都是给系统挂起之后用的。
            let spent = Duration::from_millis(now_millis().saturating_sub(started));
            let remaining = budget.saturating_sub(spent);
            let _ = timeout(remaining, async {
                self.sync_push_receipts().await;
                notify.flush_snapshot().await;
            })
            .await;
            // 回到原来的做法：iOS 把链路交回推送通道再关掉，保留链路的平台不动。
            link.end_wake().await;
            link.parking_settled().await;
            Ok(BackgroundRefreshResult { completed })
        }
    }
}

/// 后台刷新最多用多久：iOS 给 BGAppRefreshTask 的时间约 30 秒，留出交回推送通道的几秒。
#[cfg(not(target_arch = "wasm32"))]
const BACKGROUND_REFRESH_CAP_MS: u32 = 25_000;

#[cfg(target_arch = "wasm32")]
fn native_only(message: &'static str) -> FlareError {
    FlareError::localized(ErrorCode::OperationNotSupported, message)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// 焦点与后台展示方在登录前就能报，记在客户端；设置要等登录。
    #[tokio::test]
    async fn focus_and_presenter_work_before_login() {
        let client = IMClient::new();
        client.set_notification_focus(Some(" conv-1 ".to_string()));
        assert_eq!(client.notification_host.focus().as_deref(), Some("conv-1"));
        client.set_notification_focus(Some("   ".to_string()));
        assert_eq!(client.notification_host.focus(), None);

        client.set_background_presenter(true).await.unwrap();
        assert!(client.notification_host.background_presenter());

        assert!(client.notification_settings().await.is_err());
        assert!(
            client
                .update_notification_settings(UpdateUserNotificationSettingsRequest::default())
                .await
                .is_err()
        );
    }

    /// 共用目录登录前就能报，只收绝对路径，`None` 清掉；没有会话时后台刷新照常答复、没有完成。
    #[tokio::test]
    async fn the_snapshot_directory_and_background_refresh_work_without_a_session() {
        let client = IMClient::new();
        let directory = std::env::temp_dir().join("flare-notify");
        client
            .set_notification_snapshot_directory(Some(directory.display().to_string()))
            .await
            .unwrap();
        assert_eq!(
            client.notification_host.snapshot_directory().as_deref(),
            Some(directory.as_path())
        );
        assert!(
            client
                .set_notification_snapshot_directory(Some("relative/dir".into()))
                .await
                .is_err()
        );
        client
            .set_notification_snapshot_directory(Some("  ".into()))
            .await
            .unwrap();
        assert_eq!(client.notification_host.snapshot_directory(), None);

        let refreshed = client.background_refresh(25_000).await.unwrap();
        assert!(!refreshed.completed);
    }
}
