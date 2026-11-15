//! 会话用户偏好上行同步（Background）：补偿 `settings_dirty` 的 pending 写入。

use std::pin::Pin;
use std::sync::Arc;

use tracing::debug;

use super::super::SyncProtocolAdapter;
use crate::kernel::{
    SyncContext, SyncFailurePolicy, SyncMode, SyncResult, SyncTask, SyncTaskResult,
};
use crate::model::{is_settings_dirty, user_settings_version};

pub struct ConversationSettingsSyncTask(pub(crate) Arc<SyncProtocolAdapter>);

impl ConversationSettingsSyncTask {
    pub fn new(handler: Arc<SyncProtocolAdapter>) -> Self {
        Self(handler)
    }
}

impl SyncTask for ConversationSettingsSyncTask {
    fn id(&self) -> &'static str {
        "conversation_user_settings"
    }
    fn mode(&self) -> SyncMode {
        SyncMode::Background
    }
    fn weight(&self) -> u32 {
        4
    }
    fn failure_policy(&self) -> SyncFailurePolicy {
        SyncFailurePolicy::Continue
    }
    fn execute(
        &self,
        ctx: SyncContext,
    ) -> Pin<Box<dyn std::future::Future<Output = SyncResult<SyncTaskResult>> + Send>> {
        let handler = self.0.clone();
        Box::pin(async move {
            debug!(task = "conversation_user_settings", "sync phase start");
            ctx.report_progress("syncing conversation settings");
            // 共享快照：与同 phase 其他任务复用一次 list 查询。
            let list = ctx.conversations_snapshot().await?;
            let mut pushed = 0usize;
            for conversation in list.iter() {
                if !is_settings_dirty(conversation) {
                    continue;
                }
                let base = user_settings_version(conversation);
                handler
                    .push_conversation_user_settings_from_local(
                        &conversation.conversation_id,
                        base,
                        conversation,
                    )
                    .await?;
                pushed = pushed.saturating_add(1);
            }
            debug!(
                task = "conversation_user_settings",
                pushed, "sync phase done"
            );
            Ok(SyncTaskResult::ok())
        })
    }
}

/// 单次设置 patch（optional 表示是否修改该字段）
#[derive(Debug, Clone, Default)]
pub struct ConversationUserSettingsPatch {
    pub is_pinned: Option<bool>,
    pub is_muted: Option<bool>,
    pub is_archived: Option<bool>,
    pub draft: Option<String>,
    pub clear_history_through_seq: Option<u64>,
    /// 大群里显式开启「全部消息通知」。
    pub notify_all: Option<bool>,
}
