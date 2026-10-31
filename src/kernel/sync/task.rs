//! 同步任务抽象：可注册到 [super::SyncManager] 的自定义任务。
//!
//! [SyncMode] 区分 Init（阻塞 UI 就绪）与 Background；[SyncRunContext] 描述同步触发源、范围和可见性。
//! 会话列表同步由同步引擎在连接后自动执行（ConversationsSyncTask），不暴露给上层，降低系统风险。
//! [SessionSyncRunner] 暴露单会话消息同步、历史回补与已读上报，供 IMClient / timeline view 使用。

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::infrastructure::persistence::StoreProvider;

use super::checkpoint::{CheckpointStore, SyncCheckpoint};
use super::domain::DomainId;
use super::error::SyncResult;
use super::progress::SyncProgressReporter;

/// 单会话消息同步与已读上报（由 application sync protocol adapter 实现，供 IMClient 使用）。
/// 会话列表全量同步由同步引擎内部触发，不通过本 trait 暴露。
pub trait SessionSyncRunner: Send + Sync {
    fn request_message_sync(
        &self,
        conversation_id: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = crate::shared::error::Result<()>> + Send + '_>>;
    fn request_message_sync_from_seq(
        &self,
        conversation_id: &str,
        last_seq: u64,
        limit: i32,
    ) -> Pin<Box<dyn std::future::Future<Output = crate::shared::error::Result<()>> + Send + '_>>;
    fn request_message_backfill_before_seq(
        &self,
        _conversation_id: &str,
        _before_seq: u64,
        _limit: i32,
    ) -> Pin<Box<dyn std::future::Future<Output = crate::shared::error::Result<bool>> + Send + '_>>
    {
        Box::pin(async { Ok(false) })
    }
    /// 取 `after_seq` 之后的一页并写入它证明的区间，不推进同步游标（围绕某条消息打开时间线用）。
    /// 返回服务端是否还有更新的。
    fn request_message_page_after_seq_detached(
        &self,
        _conversation_id: &str,
        _after_seq: u64,
        _limit: i32,
    ) -> Pin<Box<dyn std::future::Future<Output = crate::shared::error::Result<bool>> + Send + '_>>
    {
        Box::pin(async { Ok(false) })
    }
    fn send_read_ack(
        &self,
        conversation_id: &str,
        read_seq: u64,
    ) -> Pin<Box<dyn std::future::Future<Output = crate::shared::error::Result<()>> + Send + '_>>;
    fn request_participants_sync(
        &self,
        conversation_id: &str,
        limit: i32,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = crate::shared::error::Result<
                        Vec<crate::model::ConversationParticipant>,
                    >,
                > + Send
                + '_,
        >,
    >;
}

static NEXT_SYNC_RUN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    Init,
    Background,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncFailurePolicy {
    /// 必要同步门禁：任务失败后本阶段失败，不再发布阶段完成事件。
    FailRun,
    /// 后台补偿/可选任务：记录失败并继续，不影响用户可见同步状态。
    Continue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncTrigger {
    InitialLogin,
    WarmStartupCalibration,
    Reconnect,
    Manual,
    SeqGapRepair,
    CriticalEventReplay,
    ReadStateUpload,
    BackgroundMaintenance,
    RecoveryHint,
}

impl SyncTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InitialLogin => "InitialLogin",
            Self::WarmStartupCalibration => "WarmStartupCalibration",
            Self::Reconnect => "Reconnect",
            Self::Manual => "Manual",
            Self::SeqGapRepair => "SeqGapRepair",
            Self::CriticalEventReplay => "CriticalEventReplay",
            Self::ReadStateUpload => "ReadStateUpload",
            Self::BackgroundMaintenance => "BackgroundMaintenance",
            Self::RecoveryHint => "RecoveryHint",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncScope {
    Global,
    Conversations,
    SingleConversation,
    CriticalEvents,
    ReadStates,
    Domain(DomainId),
}

impl SyncScope {
    pub fn domain(id: impl Into<String>) -> Self {
        Self::Domain(DomainId::new(id))
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Global => "Global",
            Self::Conversations => "Conversations",
            Self::SingleConversation => "SingleConversation",
            Self::CriticalEvents => "CriticalEvents",
            Self::ReadStates => "ReadStates",
            Self::Domain(id) => id.as_str(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncVisibility {
    /// 用户可见同步门禁：通常可展示 loading / progress。
    UserVisible,
    /// 非阻塞同步：本地可先出图，但同步状态、进度与诊断仍应发布。
    NonBlocking,
    /// 完全静默补偿：不打扰 UI，也不发布常规同步状态事件。
    Silent,
}

impl SyncVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserVisible => "UserVisible",
            Self::NonBlocking => "NonBlocking",
            Self::Silent => "Silent",
        }
    }

    pub fn is_user_visible(self) -> bool {
        matches!(self, Self::UserVisible)
    }

    pub fn emits_status_events(self) -> bool {
        matches!(self, Self::UserVisible | Self::NonBlocking)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncReason {
    Startup,
    WarmStartupCalibration,
    ReconnectCatchUp,
    UserRequested,
    SequenceGap,
    CriticalEventCompensation,
    ReadStateFlush,
    BackgroundCatchUp,
    ServerRecoveryHint,
}

impl SyncReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "Startup",
            Self::WarmStartupCalibration => "WarmStartupCalibration",
            Self::ReconnectCatchUp => "ReconnectCatchUp",
            Self::UserRequested => "UserRequested",
            Self::SequenceGap => "SequenceGap",
            Self::CriticalEventCompensation => "CriticalEventCompensation",
            Self::ReadStateFlush => "ReadStateFlush",
            Self::BackgroundCatchUp => "BackgroundCatchUp",
            Self::ServerRecoveryHint => "ServerRecoveryHint",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncRunContext {
    pub run_id: String,
    pub trigger: SyncTrigger,
    pub scope: SyncScope,
    pub visibility: SyncVisibility,
    pub reason: SyncReason,
    /// 这一轮拉到的页是否推进同步游标。只有围绕某条消息取的页为 false：
    /// 它只写消息与已证实区间，游标留在原处。
    pub advances_cursor: bool,
}

impl SyncRunContext {
    pub fn new(
        trigger: SyncTrigger,
        scope: SyncScope,
        visibility: SyncVisibility,
        reason: SyncReason,
    ) -> Self {
        let id = NEXT_SYNC_RUN_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            run_id: format!("sync-{id}"),
            trigger,
            scope,
            visibility,
            reason,
            advances_cursor: true,
        }
    }

    /// 不推进同步游标的单页（静默）：落库与写区间照常，游标不动。
    pub fn detached_page() -> Self {
        Self {
            advances_cursor: false,
            ..Self::silent_gap_repair()
        }
    }

    pub fn initial_login() -> Self {
        Self::new(
            SyncTrigger::InitialLogin,
            SyncScope::Global,
            SyncVisibility::UserVisible,
            SyncReason::Startup,
        )
    }

    /// 热启动同步：本地已有该用户数据 → 静默补齐，不阻塞首屏（本地优先出图）。
    pub fn warm_start() -> Self {
        Self::new(
            SyncTrigger::WarmStartupCalibration,
            SyncScope::Global,
            SyncVisibility::NonBlocking,
            SyncReason::WarmStartupCalibration,
        )
    }

    /// 重连追赶：本地数据始终存在 → 静默 CatchingUp，不弹同步 loading（连接态仍经连接 FSM 可见）。
    pub fn reconnect() -> Self {
        Self::new(
            SyncTrigger::Reconnect,
            SyncScope::Global,
            SyncVisibility::Silent,
            SyncReason::ReconnectCatchUp,
        )
    }

    pub fn manual_single_conversation() -> Self {
        Self::new(
            SyncTrigger::Manual,
            SyncScope::SingleConversation,
            SyncVisibility::UserVisible,
            SyncReason::UserRequested,
        )
    }

    pub fn silent_gap_repair() -> Self {
        Self::new(
            SyncTrigger::SeqGapRepair,
            SyncScope::SingleConversation,
            SyncVisibility::Silent,
            SyncReason::SequenceGap,
        )
    }

    pub fn silent_background(scope: SyncScope, reason: SyncReason) -> Self {
        Self::new(
            SyncTrigger::BackgroundMaintenance,
            scope,
            SyncVisibility::Silent,
            reason,
        )
    }

    pub fn for_background_phase(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            trigger: SyncTrigger::BackgroundMaintenance,
            scope: self.scope.clone(),
            visibility: match self.visibility {
                SyncVisibility::NonBlocking => SyncVisibility::NonBlocking,
                SyncVisibility::UserVisible | SyncVisibility::Silent => SyncVisibility::Silent,
            },
            reason: match self.reason {
                SyncReason::WarmStartupCalibration => SyncReason::WarmStartupCalibration,
                _ => SyncReason::BackgroundCatchUp,
            },
            advances_cursor: self.advances_cursor,
        }
    }

    pub fn needs_startup_catch_up(&self) -> bool {
        matches!(
            self.trigger,
            SyncTrigger::InitialLogin
                | SyncTrigger::WarmStartupCalibration
                | SyncTrigger::Reconnect
        )
    }

    /// 多端私有数据补偿（如通讯录备注）：静默、不阻塞 UI。
    pub fn silent_multidevice_private_data() -> Self {
        Self::silent_background(SyncScope::Global, SyncReason::CriticalEventCompensation)
    }
}

/// 启动分类：区分冷启动（本地空，首屏需网络 gate）与热启动（本地有数据，本地优先出图 + 静默补齐）。
/// 产品中立行为，沉 core；[connect] 据此选择启动同步上下文，UI 只据 visibility 决定是否展示 loading。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupClass {
    /// 本地无该用户数据：首屏 gate 拉取（可接受略慢）。
    Cold,
    /// 本地已有该用户数据：本地优先出图，网络静默补齐（不阻塞首屏）。
    Warm,
}

impl StartupClass {
    /// `has_local_data`：本地是否已有该用户可展示数据（会话/游标）。
    pub fn classify(has_local_data: bool) -> Self {
        if has_local_data {
            Self::Warm
        } else {
            Self::Cold
        }
    }

    /// 启动同步上下文：冷启动用户可见 gate；热启动静默（不阻塞首屏）。
    pub fn startup_sync_run(self) -> SyncRunContext {
        match self {
            Self::Cold => SyncRunContext::initial_login(),
            Self::Warm => SyncRunContext::warm_start(),
        }
    }

    pub fn is_cold(self) -> bool {
        matches!(self, Self::Cold)
    }
}

#[derive(Clone, Debug)]
pub enum SyncPhase {
    Start,
    Progress,
    Done,
}

#[derive(Clone, Debug, Default)]
pub struct SyncTaskResult {
    pub success: bool,
    pub message: Option<String>,
    pub cursor: Option<String>,
}

impl SyncTaskResult {
    pub fn ok() -> Self {
        Self {
            success: true,
            message: None,
            cursor: None,
        }
    }
    pub fn ok_with_cursor(cursor: impl Into<String>) -> Self {
        Self {
            success: true,
            message: None,
            cursor: Some(cursor.into()),
        }
    }
    pub fn fail(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(msg.into()),
            cursor: None,
        }
    }
}

/// changed-only 跳过判据（I4）：服务端水位（本轮刚与服务端对齐的摘要 max_seq）未超过
/// 已检查/已同步位点 ⇒ 位点之后可证明无新内容，跳过该会话的拉取。
/// 水位未知(0)保守照拉——绝不欠拉。消息任务与关键事件回放共用同一规则。
pub fn watermark_provably_clean(watermark: u64, checked_through: u64) -> bool {
    watermark > 0 && watermark <= checked_through
}

/// 同一 sync phase 内共享的会话列表快照：Background 阶段 messages/read_states/settings/key_events
/// 四任务并行、各自 `conversations.list()`（带 latest-visible-message JOIN 的重查询）——共享后 ×4→×1。
/// 惰性初始化；同 phase 各任务看到同一份一致快照（并行下反而比各查各的更一致）。
#[derive(Clone, Default)]
pub struct SharedConversationsSnapshot {
    cell: Arc<tokio::sync::OnceCell<Arc<Vec<crate::model::Conversation>>>>,
}

impl SharedConversationsSnapshot {
    pub async fn load(
        &self,
        store: &StoreProvider,
    ) -> crate::shared::error::Result<Arc<Vec<crate::model::Conversation>>> {
        self.cell
            .get_or_try_init(|| async { store.conversations.list().await.map(Arc::new) })
            .await
            .cloned()
    }
}

/// 执行时由引擎注入：store、进度上报、检查点（不包含协议能力，任务自行持有处理器并调用）。
#[derive(Clone)]
pub struct SyncContext {
    pub user_id: String,
    pub task_id: String,
    pub run: SyncRunContext,
    pub store: StoreProvider,
    pub progress: Option<Arc<dyn SyncProgressReporter>>,
    pub checkpoint_store: Option<Arc<CheckpointStore>>,
    /// 同 phase 共享的会话列表快照（惰性，一次查询多任务复用）。
    pub conversations: SharedConversationsSnapshot,
}

impl SyncContext {
    pub fn report_progress(&self, detail: impl Into<String>) {
        if let Some(ref p) = self.progress {
            p.report_current(detail.into());
        }
    }

    /// 取本 phase 共享的会话列表快照（首个调用者触发一次 `conversations.list()`）。
    pub async fn conversations_snapshot(
        &self,
    ) -> crate::shared::error::Result<Arc<Vec<crate::model::Conversation>>> {
        self.conversations.load(&self.store).await
    }

    pub async fn load_checkpoint(&self) -> crate::shared::error::Result<Option<SyncCheckpoint>> {
        let Some(ref store) = self.checkpoint_store else {
            return Ok(None);
        };
        let cp = store.load(&self.task_id).await?;
        Ok(if cp.cursor.is_some() { Some(cp) } else { None })
    }

    pub async fn save_checkpoint(
        &self,
        cursor: Option<impl AsRef<str>>,
    ) -> crate::shared::error::Result<()> {
        let Some(ref store) = self.checkpoint_store else {
            return Ok(());
        };
        let cp = SyncCheckpoint::new(self.task_id.clone(), cursor.map(|c| c.as_ref().to_string()));
        store.save(&cp).await
    }
}

pub trait SyncTask: Send + Sync {
    fn id(&self) -> &'static str;
    fn mode(&self) -> SyncMode {
        SyncMode::Background
    }
    fn failure_policy(&self) -> SyncFailurePolicy {
        match self.mode() {
            SyncMode::Init => SyncFailurePolicy::FailRun,
            SyncMode::Background => SyncFailurePolicy::Continue,
        }
    }
    fn weight(&self) -> u32 {
        1
    }
    fn execute(
        &self,
        ctx: SyncContext,
    ) -> Pin<Box<dyn std::future::Future<Output = SyncResult<SyncTaskResult>> + Send>>;
}

#[cfg(test)]
mod tests {
    use super::{SyncReason, SyncRunContext, SyncScope, SyncTrigger, SyncVisibility};

    #[test]
    fn initial_login_sync_is_user_visible_global_startup() {
        let run = SyncRunContext::initial_login();

        assert_eq!(run.trigger, SyncTrigger::InitialLogin);
        assert_eq!(run.scope, SyncScope::Global);
        assert_eq!(run.visibility, SyncVisibility::UserVisible);
        assert_eq!(run.reason, SyncReason::Startup);
        assert!(run.visibility.is_user_visible());
    }

    #[test]
    fn reconnect_sync_is_silent_catch_up() {
        let run = SyncRunContext::reconnect();

        assert_eq!(run.trigger, SyncTrigger::Reconnect);
        // 重连不弹同步 loading：静默 CatchingUp，连接态经连接 FSM 单独可见。
        assert_eq!(run.visibility, SyncVisibility::Silent);
        assert_eq!(run.reason, SyncReason::ReconnectCatchUp);
    }

    #[test]
    fn seq_gap_repair_is_silent_single_conversation_sync() {
        let run = SyncRunContext::silent_gap_repair();

        assert_eq!(run.trigger, SyncTrigger::SeqGapRepair);
        assert_eq!(run.scope, SyncScope::SingleConversation);
        assert_eq!(run.visibility, SyncVisibility::Silent);
        assert_eq!(run.reason, SyncReason::SequenceGap);
    }

    #[test]
    fn startup_classifier_maps_local_data_to_warm_silent_and_empty_to_cold_visible() {
        use super::{StartupClass, SyncReason, SyncScope, SyncVisibility};

        let cold = StartupClass::classify(false);
        let warm = StartupClass::classify(true);
        assert_eq!(cold, StartupClass::Cold);
        assert_eq!(warm, StartupClass::Warm);
        assert!(cold.is_cold());

        // 冷启动：用户可见 gate（首屏可接受略慢）。
        let cold_run = cold.startup_sync_run();
        assert_eq!(cold_run.visibility, SyncVisibility::UserVisible);
        // 热启动：非阻塞校准补齐；本地先出图，但同步状态仍应可观测。
        let warm_run = warm.startup_sync_run();
        assert_eq!(warm_run.visibility, SyncVisibility::NonBlocking);
        assert_eq!(warm_run.trigger, SyncTrigger::WarmStartupCalibration);
        assert_eq!(warm_run.reason, SyncReason::WarmStartupCalibration);
        assert!(!warm_run.visibility.is_user_visible());
        assert!(warm_run.visibility.emits_status_events());

        // 两者都属启动全量域，但热启动在报告里必须可与冷启动区分。
        assert_eq!(cold_run.reason, SyncReason::Startup);
        for run in [cold_run, warm_run] {
            assert_eq!(run.scope, SyncScope::Global);
        }
    }

    #[test]
    fn background_phase_keeps_run_id_but_becomes_silent() {
        let run = SyncRunContext::initial_login();
        let background = run.for_background_phase();

        assert_eq!(background.run_id, run.run_id);
        assert_eq!(background.trigger, SyncTrigger::BackgroundMaintenance);
        assert_eq!(background.visibility, SyncVisibility::Silent);
        assert_eq!(background.reason, SyncReason::BackgroundCatchUp);
    }

    #[test]
    fn warm_start_background_phase_keeps_calibration_status_visible() {
        let run = SyncRunContext::warm_start();
        let background = run.for_background_phase();

        assert_eq!(background.run_id, run.run_id);
        assert_eq!(background.visibility, SyncVisibility::NonBlocking);
        assert_eq!(background.reason, SyncReason::WarmStartupCalibration);
        assert!(background.visibility.emits_status_events());
        assert!(!background.visibility.is_user_visible());
    }

    #[test]
    fn sync_scope_accepts_custom_domain_id() {
        let scope = SyncScope::domain("social.friends");
        assert_eq!(scope.as_str(), "social.friends");

        let run = SyncRunContext::silent_background(
            SyncScope::domain("biz.orders"),
            SyncReason::BackgroundCatchUp,
        );
        assert_eq!(run.scope.as_str(), "biz.orders");
        assert_eq!(run.visibility, SyncVisibility::Silent);
    }

    #[test]
    fn startup_wait_timing_reports_cold_and_warm_readiness_durations() {
        use crate::kernel::event::ReadinessStage;
        use crate::kernel::sync::StartupSyncTiming;

        let cold_run = SyncRunContext::initial_login();
        let mut cold = StartupSyncTiming::new(cold_run.clone(), 1_000);
        cold.record_readiness(&cold_run, ReadinessStage::LocalReady, 1_020);
        cold.record_readiness(&cold_run, ReadinessStage::ForegroundFresh, 1_260);
        cold.record_readiness(
            &cold_run.for_background_phase(),
            ReadinessStage::Converged,
            1_640,
        );

        let cold_report = cold.report().expect("cold startup report");
        assert_eq!(cold_report.local_ready_wait_ms, Some(20));
        assert_eq!(cold_report.foreground_fresh_wait_ms, Some(260));
        assert_eq!(cold_report.converged_wait_ms, Some(640));
        assert_eq!(cold_report.hot_calibration_wait_ms, None);

        let warm_run = SyncRunContext::warm_start();
        let mut warm = StartupSyncTiming::new(warm_run.clone(), 2_000);
        warm.record_readiness(&warm_run, ReadinessStage::LocalReady, 2_005);
        warm.record_readiness(&warm_run, ReadinessStage::ForegroundFresh, 2_070);
        warm.record_readiness(
            &warm_run.for_background_phase(),
            ReadinessStage::Converged,
            2_180,
        );

        let warm_report = warm.report().expect("warm startup report");
        assert_eq!(warm_report.local_ready_wait_ms, Some(5));
        assert_eq!(warm_report.foreground_fresh_wait_ms, Some(70));
        assert_eq!(warm_report.converged_wait_ms, Some(180));
        assert_eq!(warm_report.hot_calibration_wait_ms, Some(175));
    }
}
