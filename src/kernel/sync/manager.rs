//! 同步引擎：仅任务注册与编排；协议由应用层任务在构造时注入处理器并自行调用。

use std::sync::{Arc, Mutex};

use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::event::EventBus;
use crate::shared::util::BackgroundTask;

use super::orchestrator::Orchestrator;
use super::{
    AttentionRegistry, DomainId, SyncDomain, SyncDomainRegistry, SyncMode, SyncRunContext, SyncTask,
};

pub struct SyncManager {
    tasks: Mutex<Vec<Arc<dyn SyncTask>>>,
    /// 领域无关同步注册表：业务/群/好友经 `IMClientBuilder::add_sync_domain` 注册，
    /// 由（未来的）全局流 `DomainStreamRouter` 路由。builder 期注册、运行期只读。
    domains: Mutex<SyncDomainRegistry>,
    /// 运行期共享注意力：客户端经视口 API 更新，收敛任务快照读取以做前台优先。
    attention: AttentionRegistry,
    running: Mutex<Option<BackgroundTask>>,
    background: Mutex<Vec<BackgroundTask>>,
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
            domains: Mutex::new(SyncDomainRegistry::new()),
            attention: AttentionRegistry::new(),
            running: Mutex::new(None),
            background: Mutex::new(Vec::new()),
        }
    }

    /// 共享注意力句柄（引擎/任务/客户端 API 共用一份）。
    pub fn attention(&self) -> AttentionRegistry {
        self.attention.clone()
    }

    /// 注册一个同步领域（builder 期）。
    pub fn register_domain(&self, domain: Arc<dyn SyncDomain>) {
        lock_recovered(&self.domains).register(domain);
    }

    /// 已注册领域 id（诊断 / 测试 / 未来路由枚举）。
    pub fn registered_domain_ids(&self) -> Vec<DomainId> {
        lock_recovered(&self.domains).ids()
    }

    pub fn register_task_arc(&self, task: Arc<dyn SyncTask>) {
        lock_recovered(&self.tasks).push(task);
    }

    /// 导出已注册任务（供 `IMClient::login` 合并子 engine 时保留社交 Background 任务）。
    pub fn registered_tasks(&self) -> Vec<Arc<dyn SyncTask>> {
        lock_recovered(&self.tasks).clone()
    }

    pub fn run_sync(&self, user_id: &str, store: StoreProvider, bus: EventBus) {
        self.run_with_context(user_id, SyncRunContext::initial_login(), store, bus);
    }

    /// 执行全部已注册任务（任务内自行调用注入的 SyncProtocolAdapter 等）。
    pub fn run_with_context(
        &self,
        user_id: &str,
        run: SyncRunContext,
        store: StoreProvider,
        bus: EventBus,
    ) {
        self.stop_sync();
        let tasks = self.registered_tasks();
        let handle = Orchestrator::new(store, bus).run(user_id, run, tasks);
        *lock_recovered(&self.running) = Some(handle);
    }

    /// 启动不抢占当前编排的非阻塞 catch-up。
    ///
    /// 用于热重连/网络切换后仍保持 transport 可用的校准补齐：状态事件可见，但不 `stop_sync()`，
    /// 避免高频前台/网络变化打断正在进行的启动或后台收敛。
    pub fn run_nonblocking_with_context(
        &self,
        user_id: &str,
        run: SyncRunContext,
        store: StoreProvider,
        bus: EventBus,
    ) {
        let tasks = self.registered_tasks();
        let handle = Orchestrator::new(store, bus).run(user_id, run, tasks);
        let mut background = lock_recovered(&self.background);
        background.retain(|task| !task.is_finished());
        background.push(handle);
    }

    /// 立即停止当前同步编排（用于登出/被踢/断连后的会话硬重置）。
    pub fn stop_sync(&self) {
        if let Some(handle) = lock_recovered(&self.running).take() {
            handle.abort();
        }
        self.abort_background_tasks();
    }

    /// 按 task id 静默触发 Background 同步（多端私有数据补偿等）。
    pub fn spawn_background_tasks_by_ids(
        &self,
        user_id: &str,
        task_ids: &[&str],
        store: StoreProvider,
        bus: EventBus,
    ) {
        let selected: Vec<Arc<dyn SyncTask>> = self
            .registered_tasks()
            .into_iter()
            .filter(|task| {
                task.mode() == SyncMode::Background && task_ids.iter().any(|id| *id == task.id())
            })
            .collect();
        if selected.is_empty() {
            return;
        }
        let run = SyncRunContext::silent_multidevice_private_data();
        let handle = Orchestrator::new(store, bus).spawn_background_tasks(
            user_id.to_string(),
            run,
            selected,
        );
        let mut background = lock_recovered(&self.background);
        background.retain(|task| !task.is_finished());
        background.push(handle);
    }

    fn abort_background_tasks(&self) {
        let handles = lock_recovered(&self.background)
            .drain(..)
            .collect::<Vec<_>>();
        for handle in handles {
            handle.abort();
        }
    }
}

/// 毒锁恢复：持锁线程 panic 时接管内层数据继续服务——长驻管理器不因单任务 panic 拒绝服务。
fn lock_recovered<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        tracing::warn!("sync manager lock poisoned, recovering");
        poisoned.into_inner()
    })
}

impl Default for SyncManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SyncManager {
    fn drop(&mut self) {
        if let Some(handle) = self.running.get_mut().ok().and_then(|h| h.take()) {
            handle.abort();
        }
        if let Ok(handles) = self.background.get_mut() {
            for handle in handles.drain(..) {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::SyncManager;
    use crate::infrastructure::persistence::in_memory_empty_im_provider;
    use crate::kernel::event::{EventBus, ReadinessStage, SdkEvent, SyncNotify, SyncPhase};
    use crate::kernel::{SyncContext, SyncMode, SyncResult, SyncTask, SyncTaskResult, SyncTrigger};

    struct DropSignal {
        tx: Option<oneshot::Sender<()>>,
    }

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.tx.take() {
                let _ = tx.send(());
            }
        }
    }

    struct BlockingBackgroundTask {
        started_tx: Mutex<Option<oneshot::Sender<()>>>,
        dropped_tx: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl SyncTask for BlockingBackgroundTask {
        fn id(&self) -> &'static str {
            "blocking-bg"
        }

        fn mode(&self) -> SyncMode {
            SyncMode::Background
        }

        fn execute(
            &self,
            _ctx: SyncContext,
        ) -> Pin<Box<dyn future::Future<Output = SyncResult<SyncTaskResult>> + Send>> {
            let started_tx = self.started_tx.lock().ok().and_then(|mut tx| tx.take());
            let dropped_tx = self.dropped_tx.lock().ok().and_then(|mut tx| tx.take());
            Box::pin(async move {
                if let Some(tx) = started_tx {
                    let _ = tx.send(());
                }
                let _drop_signal = DropSignal { tx: dropped_tx };
                future::pending::<()>().await;
                Ok(SyncTaskResult::ok())
            })
        }
    }

    struct CompletingTask {
        id: &'static str,
        mode: SyncMode,
        hits: Arc<AtomicUsize>,
    }

    impl SyncTask for CompletingTask {
        fn id(&self) -> &'static str {
            self.id
        }

        fn mode(&self) -> SyncMode {
            self.mode
        }

        fn execute(
            &self,
            _ctx: SyncContext,
        ) -> Pin<Box<dyn future::Future<Output = SyncResult<SyncTaskResult>> + Send>> {
            let hits = self.hits.clone();
            Box::pin(async move {
                hits.fetch_add(1, Ordering::AcqRel);
                Ok(SyncTaskResult::ok())
            })
        }
    }

    #[test]
    fn register_domain_exposes_it_for_routing() {
        use crate::kernel::{
            ApplyOutcome, ConvergencePriority, DomainCursor, DomainDelta, DomainId, DomainItem,
            DomainPhase, LaneSpec, SyncDomain, SyncTrigger, SyncVisibility,
        };
        use std::pin::Pin;

        struct FriendsDomain;
        impl SyncDomain for FriendsDomain {
            fn id(&self) -> DomainId {
                DomainId::new("example.items")
            }
            fn lane(&self) -> LaneSpec {
                LaneSpec {
                    phase: DomainPhase::Ambient,
                    priority: ConvergencePriority::P3Ambient,
                    visibility: SyncVisibility::Silent,
                    trigger: SyncTrigger::BackgroundMaintenance,
                }
            }
            fn pull(
                &self,
                _context: crate::kernel::sync::SyncDomainContext,
                _since: DomainCursor,
            ) -> Pin<Box<dyn future::Future<Output = crate::Result<DomainDelta>> + Send + '_>>
            {
                Box::pin(async { Ok(DomainDelta::default()) })
            }
            fn apply(
                &self,
                _context: crate::kernel::sync::SyncDomainContext,
                _item: &DomainItem,
            ) -> Pin<Box<dyn future::Future<Output = crate::Result<ApplyOutcome>> + Send + '_>>
            {
                Box::pin(async { Ok(ApplyOutcome::Applied) })
            }
        }

        let manager = SyncManager::new();
        assert!(manager.registered_domain_ids().is_empty());
        manager.register_domain(Arc::new(FriendsDomain));
        assert_eq!(
            manager.registered_domain_ids(),
            vec![DomainId::new("example.items")]
        );
    }

    #[tokio::test]
    async fn stop_sync_aborts_silent_background_tasks() {
        let manager = SyncManager::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        manager.register_task_arc(Arc::new(BlockingBackgroundTask {
            started_tx: Mutex::new(Some(started_tx)),
            dropped_tx: Mutex::new(Some(dropped_tx)),
        }));

        manager.spawn_background_tasks_by_ids(
            "u1",
            &["blocking-bg"],
            in_memory_empty_im_provider(),
            EventBus::new(),
        );

        timeout(Duration::from_millis(200), started_rx)
            .await
            .expect("background sync task should start")
            .expect("started signal should be sent");
        assert_eq!(manager.background.lock().unwrap().len(), 1);

        manager.stop_sync();

        timeout(Duration::from_millis(200), dropped_rx)
            .await
            .expect("background sync task should be aborted")
            .expect("drop signal should be sent");
        assert!(manager.background.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn warm_start_calibration_runs_init_and_background_with_status_events() {
        let manager = SyncManager::new();
        let init_hits = Arc::new(AtomicUsize::new(0));
        let background_hits = Arc::new(AtomicUsize::new(0));
        manager.register_task_arc(Arc::new(CompletingTask {
            id: "init-calibration",
            mode: SyncMode::Init,
            hits: init_hits.clone(),
        }));
        manager.register_task_arc(Arc::new(CompletingTask {
            id: "background-calibration",
            mode: SyncMode::Background,
            hits: background_hits.clone(),
        }));

        let bus = EventBus::new();
        let mut rx = bus.subscribe_raw();
        let run = crate::kernel::SyncRunContext::warm_start();

        manager.run_with_context("u1", run.clone(), in_memory_empty_im_provider(), bus);

        let mut events = Vec::new();
        loop {
            let event = timeout(Duration::from_millis(500), rx.recv())
                .await
                .expect("warm-start calibration should publish convergence events")
                .expect("event bus should stay open");
            let is_converged = matches!(
                &event,
                SdkEvent::Sync(SyncNotify::Readiness {
                    run: event_run,
                    stage: ReadinessStage::Converged,
                }) if event_run.run_id == run.run_id
            );
            events.push(event);
            if is_converged {
                break;
            }
        }

        assert_eq!(init_hits.load(Ordering::Acquire), 1);
        assert_eq!(background_hits.load(Ordering::Acquire), 1);
        assert!(events.iter().any(|event| matches!(
            event,
            SdkEvent::Sync(SyncNotify::Started { run: started })
                if started.trigger == SyncTrigger::WarmStartupCalibration
                    && started.run_id == run.run_id
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SdkEvent::Sync(SyncNotify::Finished {
                run: finished,
                phase: SyncPhase::Init,
            }) if finished.run_id == run.run_id
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SdkEvent::Sync(SyncNotify::Finished {
                run: finished,
                phase: SyncPhase::Background,
            }) if finished.run_id == run.run_id
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SdkEvent::Sync(SyncNotify::Readiness {
                run: readiness_run,
                stage: ReadinessStage::Converged,
            }) if readiness_run.run_id == run.run_id
        )));
    }

    #[tokio::test]
    async fn nonblocking_catch_up_does_not_abort_current_sync_run() {
        let manager = SyncManager::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        manager.register_task_arc(Arc::new(BlockingBackgroundTask {
            started_tx: Mutex::new(Some(started_tx)),
            dropped_tx: Mutex::new(Some(dropped_tx)),
        }));

        manager.run_with_context(
            "u1",
            crate::kernel::SyncRunContext::initial_login(),
            in_memory_empty_im_provider(),
            EventBus::new(),
        );

        timeout(Duration::from_millis(200), started_rx)
            .await
            .expect("current sync should start")
            .expect("started signal should be sent");

        manager.run_nonblocking_with_context(
            "u1",
            crate::kernel::SyncRunContext::reconnect(),
            in_memory_empty_im_provider(),
            EventBus::new(),
        );

        assert!(
            timeout(Duration::from_millis(50), dropped_rx)
                .await
                .is_err(),
            "nonblocking catch-up must not abort the active sync run"
        );

        manager.stop_sync();
    }
}
