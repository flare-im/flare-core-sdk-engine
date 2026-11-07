//! 消息同步任务（Background）：按会话列表 diff 出变化会话，按注意力排序后批量拉取消息。
//! 构造时注入 [SyncProtocolAdapter]，在 execute 内直接调用。

use std::pin::Pin;
use std::sync::Arc;

use tracing::debug;

use super::super::SyncProtocolAdapter;
use crate::kernel::{
    AttentionRegistry, AttentionState, ConvergenceDriver, ConvergencePriority,
    ConvergenceScheduler, ConvergenceTarget, SyncContext, SyncFailurePolicy, SyncMode, SyncResult,
    SyncTask, SyncTaskResult,
};

pub struct MessagesSyncTask {
    pub(crate) handler: Arc<SyncProtocolAdapter>,
    max_sync_concurrency: usize,
    attention: AttentionRegistry,
}
const DEFAULT_SYNC_CONCURRENCY: usize = 4;
const MULTI_CONVERSATION_BATCH_MULTIPLIER: usize = 16;
// ExecuteSync accepts at most 100 conversation IDs per MultiConversation request.
// Concurrency is a local scheduling budget, never permission to exceed that wire bound.
const MAX_MULTI_CONVERSATION_BATCH_SIZE: usize = 100;

impl MessagesSyncTask {
    pub fn new(handler: Arc<SyncProtocolAdapter>, attention: AttentionRegistry) -> Self {
        Self::with_max_sync_concurrency(handler, DEFAULT_SYNC_CONCURRENCY, attention)
    }

    pub fn with_max_sync_concurrency(
        handler: Arc<SyncProtocolAdapter>,
        max_sync_concurrency: usize,
        attention: AttentionRegistry,
    ) -> Self {
        Self {
            handler,
            max_sync_concurrency: Self::normalize_max_sync_concurrency(max_sync_concurrency),
            attention,
        }
    }

    pub(crate) fn normalize_max_sync_concurrency(value: usize) -> usize {
        value.max(1)
    }
}

fn multi_conversation_batch_size(max_sync_concurrency: usize) -> usize {
    // Keep default/mobile budgets, but high custom budgets must not generate rejected
    // batches. Saturation handles usize::MAX without overflow before enforcing the cap.
    max_sync_concurrency
        .saturating_mul(MULTI_CONVERSATION_BATCH_MULTIPLIER)
        .clamp(1, MAX_MULTI_CONVERSATION_BATCH_SIZE)
}

/// I9 自适应预算：后台/锁屏时批大小与每会话页大小减半（让电/让带宽），前台全速。
fn adaptive_sync_budget(
    app_in_background: bool,
    batch_size: usize,
    page_limit: i32,
) -> (usize, i32) {
    if app_in_background {
        ((batch_size / 2).max(1), (page_limit / 2).max(1))
    } else {
        (batch_size, page_limit)
    }
}

/// I8 批间重排 + P0 抢占：每批开始前按**最新**注意力重排剩余目标——用户批中途切会话，
/// 新前台下一批即优先且**单飞**（最小 RTT 到屏）；其余按有界批大小取。
fn next_batch_plan(
    remaining: &mut Vec<String>,
    attention: &AttentionState,
    batch_size: usize,
) -> Vec<String> {
    if remaining.is_empty() {
        return Vec::new();
    }
    remaining.sort_by_cached_key(|id| attention.priority_for_conversation(id));
    let head_is_foreground =
        attention.priority_for_conversation(&remaining[0]) == ConvergencePriority::P0Foreground;
    let take = if head_is_foreground {
        1
    } else {
        batch_size.max(1).min(remaining.len())
    };
    remaining.drain(..take).collect()
}

impl SyncTask for MessagesSyncTask {
    fn id(&self) -> &'static str {
        "messages"
    }
    fn mode(&self) -> SyncMode {
        // 会话快照先完成，消息再做分页补齐，避免 Init 阶段并行导致空会话列表。
        SyncMode::Background
    }
    fn weight(&self) -> u32 {
        20
    }
    fn failure_policy(&self) -> SyncFailurePolicy {
        SyncFailurePolicy::Continue
    }
    fn execute(
        &self,
        ctx: SyncContext,
    ) -> Pin<Box<dyn std::future::Future<Output = SyncResult<SyncTaskResult>> + Send>> {
        let handler = self.handler.clone();
        let max_sync_concurrency = self.max_sync_concurrency;
        let attention_registry = self.attention.clone();
        let attention_snapshot = self.attention.snapshot();
        Box::pin(async move {
            debug!(task = "messages", "sync phase: messages start");
            ctx.report_progress("syncing messages");
            let user_id = ctx.user_id.clone();
            let run = ctx.run.clone();
            // 共享快照：与同 phase 的 read_states/settings/key_events 复用一次 list 查询。
            let list = ctx.conversations_snapshot().await?;
            let total = list.len();

            // 只补齐"变化"的会话：仅在 server 最新 seq(max_seq) 明确 ≤ 本地已同步游标 时跳过
            // （可证明已最新）；其余（含 max_seq 未知=0）一律收敛——保守，绝不欠拉。
            // 用引擎共享注意力快照播种：用户正看/可见的会话优先补齐（前台优先）。
            let mut scheduler =
                ConvergenceScheduler::with_attention(total.max(1), attention_snapshot);

            // 近期活跃(按 last_message_at)标为 P2：即便无 app 注意力信号，最近的会话也先补齐（秒展示）。
            const RECENCY_WINDOW: usize = 20;
            let mut by_recency: Vec<&crate::model::Conversation> = list.iter().collect();
            by_recency.sort_by(|a, b| {
                b.last_message_at
                    .unwrap_or(0)
                    .cmp(&a.last_message_at.unwrap_or(0))
            });
            let recent_ids: Vec<String> = by_recency
                .into_iter()
                .take(RECENCY_WINDOW)
                .map(|conversation| conversation.conversation_id.clone())
                .collect();
            // 本轮 recency 标记：播种调度器 + 每批快照复用（两处同一定义）。
            let seed_recency = |attention: &mut AttentionState| {
                for conversation_id in &recent_ids {
                    attention.mark_recent(conversation_id.clone());
                }
            };
            seed_recency(scheduler.attention_mut());

            // 一次批量读全部游标（I7 同款批量优于 N+1；WASM/IndexedDB 上逐会话读=每会话一次桥往返）。
            // 批量失败 → 空表 → 全部视为待收敛（保守，绝不欠拉）。
            let conversation_ids: Vec<String> = list
                .iter()
                .map(|conversation| conversation.conversation_id.clone())
                .collect();
            let cursor_map = ctx
                .store
                .cursors
                .get_conversation_cursors(&user_id, &conversation_ids)
                .await
                .unwrap_or_default();

            let mut skipped = 0usize;
            for conversation in list.iter() {
                let cursor_seq = cursor_map
                    .get(&conversation.conversation_id)
                    .map(|cursor| cursor.last_seq)
                    .unwrap_or(0);
                if crate::kernel::watermark_provably_clean(conversation.max_seq, cursor_seq) {
                    skipped += 1;
                    continue;
                }
                scheduler.enqueue(ConvergenceTarget::Conversation(
                    conversation.conversation_id.clone(),
                ));
            }

            let enqueued = scheduler.pending_len();
            let mut remaining: Vec<String> =
                ConvergenceDriver::drain_ordered_targets(&mut scheduler)
                    .into_iter()
                    .filter_map(|target| match target {
                        ConvergenceTarget::Conversation(id) => Some(id),
                        ConvergenceTarget::Global => None,
                    })
                    .collect();
            let batch_size = multi_conversation_batch_size(max_sync_concurrency);
            let mut converged = 0usize;
            let mut failed = 0usize;
            let mut pages = 0usize;
            let mut decoded_items = 0usize;
            while !remaining.is_empty() {
                // I8：每批开始前重读共享注意力（合并本轮 recency 标记）——
                // 用户批中途打开新会话，下一批立即优先该会话（P0 单飞抢占）。
                let mut attention = attention_registry.snapshot();
                seed_recency(&mut attention);
                // I9：后台态降配（批/页减半）；随注意力快照逐批生效，回前台下一批即恢复全速。
                let (effective_batch_size, page_limit) = adaptive_sync_budget(
                    attention.app_in_background(),
                    batch_size,
                    crate::domain::DEFAULT_SYNC_LIMIT,
                );
                let batch = next_batch_plan(&mut remaining, &attention, effective_batch_size);
                if batch.is_empty() {
                    break;
                }
                match handler
                    .sync_multi_conversations_with_context(&batch, run.clone(), page_limit)
                    .await
                {
                    Ok(report) => {
                        converged = converged.saturating_add(report.applied_conversations);
                        failed = failed.saturating_add(report.failed_conversations);
                        pages = pages.saturating_add(report.pages);
                        decoded_items = decoded_items.saturating_add(report.decoded_items);
                    }
                    Err(error) => {
                        failed = failed.saturating_add(batch.len());
                        tracing::warn!(
                            conversations = batch.len(),
                            error = %error,
                            "批量消息收敛失败，等待后续同步重试"
                        );
                    }
                }
            }
            debug!(
                task = "messages",
                total,
                skipped,
                enqueued,
                converged,
                failed,
                pages,
                decoded_items,
                "sync phase: messages result (changed-only, attention-ordered, batched)"
            );
            debug!(task = "messages", "sync phase: messages done");
            Ok(SyncTaskResult::ok())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SYNC_CONCURRENCY, MAX_MULTI_CONVERSATION_BATCH_SIZE, MessagesSyncTask,
        adaptive_sync_budget, multi_conversation_batch_size, next_batch_plan,
    };
    use crate::kernel::AttentionState;

    #[test]
    fn max_sync_concurrency_is_never_zero() {
        assert_eq!(MessagesSyncTask::normalize_max_sync_concurrency(0), 1);
    }

    #[test]
    fn max_sync_concurrency_preserves_mobile_budget() {
        assert_eq!(MessagesSyncTask::normalize_max_sync_concurrency(2), 2);
    }

    #[test]
    fn multi_conversation_batch_preserves_default_and_mobile_budgets() {
        assert_eq!(multi_conversation_batch_size(DEFAULT_SYNC_CONCURRENCY), 64);
        assert_eq!(multi_conversation_batch_size(2), 32);
        assert_eq!(
            multi_conversation_batch_size(MessagesSyncTask::normalize_max_sync_concurrency(0)),
            16,
        );
    }

    #[test]
    fn multi_conversation_batch_caps_custom_and_overflow_budgets() {
        assert_eq!(multi_conversation_batch_size(6), 96);
        for concurrency in [7, 8, 100, usize::MAX / 16, usize::MAX] {
            assert_eq!(
                multi_conversation_batch_size(concurrency),
                MAX_MULTI_CONVERSATION_BATCH_SIZE,
                "custom concurrency {concurrency} must obey the server request cap",
            );
        }
    }

    #[test]
    fn capped_batch_planner_drains_every_target_without_exceeding_the_wire_budget() {
        let targets: Vec<String> = (0..237).map(|index| format!("owned-{index}")).collect();
        for in_background in [false, true] {
            let mut remaining = targets.clone();
            let (effective_batch_size, _) = adaptive_sync_budget(
                in_background,
                multi_conversation_batch_size(usize::MAX),
                100,
            );
            assert_eq!(effective_batch_size, if in_background { 50 } else { 100 });
            let attention = AttentionState::default();
            let mut drained = Vec::new();
            while !remaining.is_empty() {
                let batch = next_batch_plan(&mut remaining, &attention, effective_batch_size);
                assert!(!batch.is_empty());
                assert!(batch.len() <= effective_batch_size);
                assert!(batch.len() <= MAX_MULTI_CONVERSATION_BATCH_SIZE);
                drained.extend(batch);
            }
            assert_eq!(
                drained, targets,
                "no target can be lost or duplicated by clamping"
            );
        }
    }

    #[test]
    fn background_budget_halves_batch_and_page() {
        // I9：后台降配（批/页减半、下限 1）；前台全速。
        assert_eq!(adaptive_sync_budget(false, 64, 100), (64, 100));
        assert_eq!(adaptive_sync_budget(true, 64, 100), (32, 50));
        assert_eq!(adaptive_sync_budget(true, 1, 1), (1, 1));
    }

    #[test]
    fn foreground_conversation_preempts_next_batch_alone() {
        // I8：P0 前台单飞抢占；其余按可见/近期/长尾排序有界成批。
        let mut attention = AttentionState::default();
        attention.open_timeline("fg");
        attention.set_visible(["visible-1", "visible-2"]);

        let mut remaining = vec![
            "ambient".to_string(),
            "visible-1".to_string(),
            "fg".to_string(),
            "visible-2".to_string(),
        ];

        let first = next_batch_plan(&mut remaining, &attention, 2);
        assert_eq!(first, vec!["fg".to_string()], "P0 must go alone first");

        let second = next_batch_plan(&mut remaining, &attention, 2);
        assert_eq!(
            second,
            vec!["visible-1".to_string(), "visible-2".to_string()]
        );

        let third = next_batch_plan(&mut remaining, &attention, 2);
        assert_eq!(third, vec!["ambient".to_string()]);
        assert!(remaining.is_empty());
    }

    #[test]
    fn mid_run_timeline_open_reorders_remaining_batches() {
        // I8：批中途 open_timeline → 下一批用最新注意力重排，新前台立即优先。
        let mut remaining = vec![
            "cold-1".to_string(),
            "cold-2".to_string(),
            "cold-3".to_string(),
        ];
        let idle = AttentionState::default();
        let first = next_batch_plan(&mut remaining, &idle, 2);
        assert_eq!(first.len(), 2);

        // 用户此刻打开 cold-3 → 新注意力快照下 cold-3 抢占下一批并单飞。
        let mut focused = AttentionState::default();
        focused.open_timeline("cold-3");
        let preempted = next_batch_plan(&mut remaining, &focused, 2);
        assert_eq!(preempted, vec!["cold-3".to_string()]);
    }
}
