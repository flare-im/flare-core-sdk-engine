//! 同步协议处理：会话列表、单会话消息、已读上报及响应落库。
//! 与 flare-proto 对齐：上行 Ack(ReadAck)、Sync；下行 SyncRes。

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use flare_proto::common::{
    Ack, ConversationParticipantsSync, ConversationSyncSlice, ConversationUserSettingsSync,
    EnsureConversationSync, GetSyncCursorSync, MultiDeviceCursor, QueryEventsSync, ReadAck,
    SingleConversationSyncRes, SyncRes, UpdateSyncCursorSync, ack::Payload as AckPayload,
    sync_res::Payload as SyncResPayload,
};
use tokio::sync::Mutex as AsyncMutex;

use crate::application::notification::NotificationInboundPipeline;
use crate::application::services::EventDeduper;
use crate::application::usecases::sync_request::SyncRequestUseCase;
use crate::application::usecases::{SyncApplyUseCase, local_message_sync_start_seq};
use crate::domain::{
    CONVERSATION_CURSOR_KEY, CRITICAL_EVENT_CURSOR_KEY, DEFAULT_SYNC_LIMIT, ReadPosition,
    SyncPolicy,
};
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::protocol::PacketSender;
use crate::kernel::ConversationSummarySync;
use crate::kernel::event::SyncNotify;
use crate::kernel::event::{EventBus, SdkEvent};
use crate::kernel::{
    ReliableSendQueuePort, SessionSyncRunner, SyncResponseHandler, SyncRunContext,
    watermark_provably_clean,
};
use crate::kernel::{SyncFsm, SyncState, SyncTransition};
use crate::model::apply_remote_settings_version;
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::now_millis;
use crate::spi::metrics::{MetricLabel, MetricsRecorder};

#[derive(Debug, Clone, Default)]
struct QueryEventsReqV1 {
    conversation_id: String,
    after_seq: u64,
    before_seq: u64,
    limit: i32,
    event_types: Vec<i32>,
    include_deleted: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MultiConversationCatchUpReport {
    pub requested_conversations: usize,
    pub applied_conversations: usize,
    pub failed_conversations: usize,
    pub pages: usize,
    pub decoded_items: usize,
}

/// I2 gap-too-large：服务端明示增量不可回放（快照重拉/会话重同步）时，客户端必须放弃
/// 增量回放切快照，否则要么欠拉（静默丢更新）要么无界重放。
fn snapshot_cutover_required(
    stale: Option<&flare_proto::common::SyncStaleContext>,
    hints: Option<&flare_proto::common::SyncSessionHints>,
) -> bool {
    const REFETCH: i32 = flare_proto::common::SyncRecoveryHint::RefetchSnapshot as i32;
    const RESYNC: i32 = flare_proto::common::SyncRecoveryHint::ResyncConversation as i32;
    stale
        .map(|context| {
            context.recommended_action == REFETCH || context.recommended_action == RESYNC
        })
        .unwrap_or(false)
        || hints
            .map(|hints| hints.recovery_hint == REFETCH || hints.recovery_hint == RESYNC)
            .unwrap_or(false)
}

/// 服务端快照/列表游标形如 `millis|conversation_id`（分页）或纯 `millis`（水位回显）。
/// 客户端持久化的是 u64 毫秒水位——取 `|` 前的 ms 部分。
/// 旧实现 `parse::<u64>()` 对 `ms|cid` 必然得 0 → 列表游标永不前进 → 每次热启/重连全量拉列表。
fn snapshot_cursor_watermark_ms(cursor: &str) -> u64 {
    let ms_part = cursor.split_once('|').map(|(ms, _)| ms).unwrap_or(cursor);
    ms_part.trim().parse::<u64>().unwrap_or_default()
}

/// slice 按值转换（items 含整页消息 payload，move 而非 clone——批量 catch-up 热路径）。
fn single_response_from_multi_slice(slice: ConversationSyncSlice) -> SingleConversationSyncRes {
    SingleConversationSyncRes {
        conversation_id: slice.conversation_id,
        items: slice.items,
        max_conversation_seq: slice.max_conversation_seq,
        next_cursor: slice.next_cursor,
        has_more: slice.has_more,
        hints: None,
        stale: None,
    }
}

fn build_read_ack(device_id: &str, conversation_id: &str, read_seq: u64) -> Ack {
    let ack_id = format!("read:{}:{}", conversation_id, read_seq);
    Ack {
        ack_id: Some(ack_id.clone()),
        ack_at: Some(now_millis() as i64),
        payload: Some(AckPayload::Read(ReadAck {
            conversation_id: conversation_id.to_string(),
            read_seq,
            device_id: non_empty(device_id),
            ack_id: Some(ack_id),
        })),
    }
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

pub struct SyncProtocolAdapter {
    sender: Arc<PacketSender>,
    stores: StoreProvider,
    bus: EventBus,
    sync_state: Mutex<SyncState>,
    active_user_id: AsyncMutex<String>,
    sync_apply_use_case: SyncApplyUseCase,
    sync_request_use_case: SyncRequestUseCase,
    /// 远端游标合并冲刷：热路径只进 map（按会话取 max 合并），后台去抖批量推送。
    remote_cursor_flush: Arc<RemoteCursorFlushState>,
    /// 冲刷 worker 只启动一次（builder 装载；未装载时热路径退化为内联推送，语义等价）。
    remote_cursor_worker: OnceLock<()>,
    /// Arc 自引用（builder Arc 化后随冲刷 worker 一并装载）：冷启 bundle 余页后台续拉用。
    self_weak: OnceLock<std::sync::Weak<Self>>,
    /// 冷启 bundle 续拉单飞：避免续拉未完成期间的再次冷入口重复整套后台拉取。
    cold_bundle_continuation_in_flight: std::sync::atomic::AtomicBool,
    metrics: MetricsRecorder,
}

/// 远端游标推送是 advisory（多端/新设备的引导提示，服务端单调合并）——
/// 无需在 catch-up 热路径逐会话同步等待。合并=同会话取最大 seq；去抖=批量窗口一次推。
#[derive(Default)]
struct RemoteCursorFlushState {
    pending: AsyncMutex<HashMap<String, u64>>,
    notify: tokio::sync::Notify,
}

impl SyncProtocolAdapter {
    async fn get_remote_cursor_seq(
        &self,
        _user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<u64>> {
        let resp = self
            .sync_request_use_case
            .request_get_cursor(GetSyncCursorSync {
                device_id: self.sync_request_use_case.device_id().to_string(),
                conversation_id: conversation_id.to_string(),
                ..Default::default()
            })
            .await?;
        let Some(SyncResPayload::GetSyncCursor(res)) = resp.payload else {
            return Ok(None);
        };
        // 编排器 GetSyncCursor 在缓存未命中时返回 cursor=None。若当成 0 再与本地取 max，
        // 会误用本地 SQLite 里陈旧的毫秒游标做增量过滤，导致会话列表永远为空（conversation 侧 bootstrap 实际有数据）。
        let Some(parsed) = res.cursor else {
            return Ok(None);
        };
        Ok(Some(parsed.last_conversation_seq))
    }

    /// 推送远端游标。**不做读-比较-写**：服务端 `update_sync_cursor` 已单调合并
    /// （merge_cursor_monotonic + read/message/sync_at 取 max，有测试守护）——
    /// 客户端预读 GetSyncCursor 每次推进多一个 RPC 且缓存 miss 时触发服务端全量 bootstrap。
    async fn update_remote_cursor_seq(&self, conversation_id: &str, last_seq: u64) -> Result<()> {
        let local_read_seq = self
            .stores
            .conversations
            .get(conversation_id)
            .await
            .ok()
            .flatten()
            .and_then(|conversation| ReadPosition::from_conversation(&conversation).ack_read_seq())
            .unwrap_or(0);
        self.push_remote_cursor(conversation_id, last_seq, local_read_seq)
            .await
    }

    async fn push_remote_cursor(
        &self,
        conversation_id: &str,
        last_seq: u64,
        local_read_seq: u64,
    ) -> Result<()> {
        let resp = self
            .sync_request_use_case
            .request_update_cursor(UpdateSyncCursorSync {
                cursor: Some(MultiDeviceCursor {
                    device_id: self.sync_request_use_case.device_id().to_string(),
                    conversation_id: conversation_id.to_string(),
                    last_conversation_seq: last_seq,
                    last_sync_at: now_millis() as i64,
                    last_read_seq: local_read_seq,
                    last_message_seq: 0,
                }),
            })
            .await?;
        let Some(SyncResPayload::UpdateSyncCursor(res)) = resp.payload else {
            return Ok(());
        };
        let _ = res.cursor;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sender: Arc<PacketSender>,
        stores: StoreProvider,
        bus: EventBus,
        event_deduper: EventDeduper,
        notification_pipeline: NotificationInboundPipeline,
        device_id: String,
        metrics: MetricsRecorder,
    ) -> Self {
        let sync_apply_use_case = SyncApplyUseCase::new(
            stores.clone(),
            bus.clone(),
            event_deduper,
            notification_pipeline,
        );
        let sync_request_use_case = SyncRequestUseCase::new(sender.clone(), device_id);
        Self {
            sender,
            stores,
            bus,
            sync_state: Mutex::new(SyncState::Idle),
            active_user_id: AsyncMutex::new(String::new()),
            sync_apply_use_case,
            sync_request_use_case,
            remote_cursor_flush: Arc::new(RemoteCursorFlushState::default()),
            remote_cursor_worker: OnceLock::new(),
            self_weak: OnceLock::new(),
            cold_bundle_continuation_in_flight: std::sync::atomic::AtomicBool::new(false),
            metrics,
        }
    }

    pub fn set_reliable_queue(&self, reliable_queue: Option<Arc<dyn ReliableSendQueuePort>>) {
        self.sync_apply_use_case.set_reliable_queue(reliable_queue);
    }

    async fn current_user_id(&self) -> String {
        self.active_user_id.lock().await.clone()
    }

    async fn save_cursor_with_remote(
        &self,
        user_id: &str,
        conversation_id: &str,
        last_seq: u64,
        proven_absent_seqs: &[u64],
    ) -> Result<()> {
        self.sync_apply_use_case
            .save_cursor_with_remote(
                user_id,
                conversation_id,
                last_seq,
                proven_absent_seqs,
                |_, conversation_id, last_seq| async move {
                    self.queue_remote_cursor(&conversation_id, last_seq).await
                },
            )
            .await
    }

    /// 远端游标推送入队（合并同会话取 max）。worker 已装载 → 去抖批量推；
    /// 未装载（测试 harness/裸构造）→ 内联推送，语义等价。
    async fn queue_remote_cursor(&self, conversation_id: &str, last_seq: u64) -> Result<()> {
        if self.remote_cursor_worker.get().is_none() {
            return self
                .update_remote_cursor_seq(conversation_id, last_seq)
                .await;
        }
        {
            let mut pending = self.remote_cursor_flush.pending.lock().await;
            let entry = pending.entry(conversation_id.to_string()).or_insert(0);
            *entry = (*entry).max(last_seq);
        }
        self.remote_cursor_flush.notify.notify_one();
        Ok(())
    }

    /// 水位/快照建立型游标保存（不做连续性钳制，见 `save_watermark_with_remote`）+ 远端合并推送。
    async fn save_watermark_cursor(
        &self,
        user_id: &str,
        cursor_key: &str,
        last_seq: u64,
    ) -> Result<()> {
        self.sync_apply_use_case
            .save_watermark_with_remote(user_id, cursor_key, last_seq, |_, key, seq| async move {
                self.queue_remote_cursor(&key, seq).await
            })
            .await
    }

    /// 关键事件检查点保存：无远端推送（检查点仅本设备语义），失败仅告警（幂等，下轮重查自愈）。
    async fn save_critical_checkpoint(&self, user_id: &str, cursor_key: &str, seq: u64) {
        if let Err(error) = self
            .sync_apply_use_case
            .save_watermark_with_remote(user_id, cursor_key, seq, |_, _, _| async { Ok(()) })
            .await
        {
            tracing::warn!(
                cursor_key = %cursor_key,
                seq,
                error = %error,
                "关键事件检查点保存失败，下轮重查（幂等）"
            );
        }
    }

    /// 启动远端游标冲刷 worker（builder 在 Arc 化后调用一次，幂等）。
    /// worker 持 Weak：adapter 释放 → worker 退出；推送失败仅 debug（advisory，下轮同步重推）。
    pub(crate) fn start_remote_cursor_flush_worker(self: &Arc<Self>) {
        let _ = self.self_weak.set(Arc::downgrade(self));
        if self.remote_cursor_worker.set(()).is_err() {
            return;
        }
        const FLUSH_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(200);
        let weak = Arc::downgrade(self);
        let state = self.remote_cursor_flush.clone();
        crate::shared::util::spawn_background(async move {
            loop {
                state.notify.notified().await;
                crate::shared::util::delay(FLUSH_DEBOUNCE).await;
                let batch: Vec<(String, u64)> = {
                    let mut pending = state.pending.lock().await;
                    pending.drain().collect()
                };
                if batch.is_empty() {
                    continue;
                }
                let Some(adapter) = weak.upgrade() else {
                    break;
                };
                // read_seq 批量预取（冷启后批次可达 N，逐条点查在 IndexedDB 上放大）。
                let ids: Vec<String> = batch.iter().map(|(cid, _)| cid.clone()).collect();
                let read_seqs: HashMap<String, u64> = adapter
                    .stores
                    .conversations
                    .get_many(&ids)
                    .await
                    .map(|conversations| {
                        conversations
                            .into_iter()
                            .filter_map(|conversation| {
                                ReadPosition::from_conversation(&conversation)
                                    .ack_read_seq()
                                    .map(|seq| (conversation.conversation_id, seq))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                for (conversation_id, last_seq) in batch {
                    let read_seq = read_seqs.get(&conversation_id).copied().unwrap_or(0);
                    if let Err(error) = adapter
                        .push_remote_cursor(&conversation_id, last_seq, read_seq)
                        .await
                    {
                        tracing::debug!(
                            conversation_id = %conversation_id,
                            last_seq,
                            error = %error,
                            "远端游标推送失败（advisory，下轮同步重推）"
                        );
                    }
                }
                drop(adapter);
            }
        });
    }

    /// I2 gap-too-large 快照切换：一次 `SyncSnapshotSync` 拉该会话最新页 + 权威已读/未读，
    /// 落库成功后把游标直接置到快照水位（守 I1：先数据后游标）；更旧历史留给按需 backfill。
    /// 返回切换后的游标水位（0 表示服务端未返回该会话行，游标不动）。
    async fn resync_conversation_from_snapshot(&self, conversation_id: &str) -> Result<u64> {
        let user_id = self.current_user_id().await;
        let res = self
            .sync_request_use_case
            .request_sync_snapshot(flare_proto::common::SyncSnapshotSync {
                conversation_ids: vec![conversation_id.to_string()],
                messages_per_conversation: DEFAULT_SYNC_LIMIT,
                include_deleted: false,
                include_conversations: false,
                snapshot_cursor: String::new(),
                newest_first: false,
                conversation_page_limit: 1,
            })
            .await?;
        let mut cutover_seq = 0u64;
        for row in res.conversations {
            if row.conversation_id != conversation_id {
                continue;
            }
            let last_seq = self
                .sync_apply_use_case
                .apply_snapshot_row(&user_id, row)
                .await?;
            if last_seq > 0 && !user_id.is_empty() {
                self.establish_snapshot_cursors(&user_id, conversation_id, last_seq)
                    .await?;
            }
            cutover_seq = last_seq;
        }
        tracing::info!(
            conversation_id = %conversation_id,
            cutover_seq,
            "gap-too-large 快照切换完成，游标已置服务端水位"
        );
        Ok(cutover_seq)
    }

    /// 快照建立型双位点：消息游标（无钳制水位）+ 关键事件位点一并置到快照水位。
    /// 快照行是服务端「当前态物化」（撤回/编辑/反应已反映在行内），≤last_seq 的事件
    /// 要么已体现在返回消息、要么指向不在本地的历史；不建立关键位点则冷启后首轮
    /// critical 扫描会对全部会话各发一个 QueryEvents（N 个空响应 RPC）。
    async fn establish_snapshot_cursors(
        &self,
        user_id: &str,
        conversation_id: &str,
        last_seq: u64,
    ) -> Result<()> {
        self.save_watermark_cursor(user_id, conversation_id, last_seq)
            .await?;
        self.save_critical_checkpoint(
            user_id,
            &critical_event_cursor_key(conversation_id),
            last_seq,
        )
        .await;
        Ok(())
    }

    fn transition_sync(&self, run: &SyncRunContext, event: SyncTransition) {
        let mut guard = match self.sync_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("sync state lock poisoned, recovering poisoned state");
                poisoned.into_inner()
            }
        };
        if let Ok(next) = SyncFsm::transition(*guard, &event) {
            *guard = next;
            drop(guard);
            self.bus.publish(SdkEvent::Sync(SyncNotify::StateChanged {
                run: run.clone(),
                state: next,
            }));
        } else {
            tracing::debug!("sync transition ignored: invalid state transition");
        }
    }

    /// 已读上报（ack.proto Ack.payload.read = ReadAck）。
    pub async fn send_read_ack(&self, conversation_id: &str, read_seq: u64) -> Result<()> {
        self.send_read_ack_impl(conversation_id, read_seq).await
    }

    /// 已读位点批量上报（登录前强推与后台 read_states 任务共用；**恒 delta**，I4）：
    /// 只上报"本地已读位点 > 上次已上报位点"的会话，O(会话数)→O(前进数)。
    /// 位点在 `send?` 成功后才记录（失败下轮重推）；live 读路径不记录位点，周期任务必然重推。
    /// 残余丢失窗口=ack 写入濒死 socket 且服务端未收：影响仅为他端未读徽标暂滞，下次阅读自愈——
    /// 换来重连/热启不再每次全量 O(N) ack 风暴（历史教训：ACK 116s）。
    pub async fn push_local_read_states(
        &self,
        conversations: &[crate::model::Conversation],
    ) -> Result<usize> {
        // 一次批量读全部已上报位点（逐会话 get_raw 在重连/热启是 N 次点查）。
        // 批量失败 → 空表 → 全部视为前进（保守重推，不欠推）。
        let ack_keys: Vec<String> = conversations
            .iter()
            .map(|conversation| Self::read_ack_cursor_key(&conversation.conversation_id))
            .collect();
        let acked = self
            .stores
            .cursors
            .get_raws(&ack_keys)
            .await
            .unwrap_or_default();

        let mut ack_count = 0usize;
        let mut skipped = 0usize;
        for (conversation, ack_key) in conversations.iter().zip(&ack_keys) {
            let Some(read_seq) = ReadPosition::from_conversation(conversation).ack_read_seq()
            else {
                continue;
            };
            let advanced = acked
                .get(ack_key)
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(|last| read_seq > last)
                // 未知/解析失败视为前进 → 上报（保守）。
                .unwrap_or(true);
            if !advanced {
                skipped = skipped.saturating_add(1);
                continue;
            }
            self.send_read_ack_impl(&conversation.conversation_id, read_seq)
                .await?;
            self.remember_read_ack(&conversation.conversation_id, read_seq)
                .await;
            ack_count = ack_count.saturating_add(1);
        }
        if ack_count > 0 || skipped > 0 {
            tracing::debug!(ack_count, skipped, "pushed local read states to server");
        }
        Ok(ack_count)
    }

    fn read_ack_cursor_key(conversation_id: &str) -> String {
        format!("read_ack:{conversation_id}")
    }

    async fn remember_read_ack(&self, conversation_id: &str, read_seq: u64) {
        let key = Self::read_ack_cursor_key(conversation_id);
        if let Err(error) = self
            .stores
            .cursors
            .save_raw(&key, &read_seq.to_string())
            .await
        {
            tracing::warn!(conversation_id, error = %error, "记录已读上报位点失败，下次将重复上报");
        }
    }

    /// 冷启动/重连拉摘要前，先把本地**前进过的**已读位点推给服务端（delta），
    /// 使 participants 表读位与客户端一致、摘要 `last_read_seq` 不依赖反推。
    /// 冷启本地为空=天然 no-op；重连未前进=零 ack（I4 重连轻量化）。
    async fn push_local_read_states_before_summary_sync(&self) -> Result<usize> {
        let list = self.stores.conversations.list().await?;
        self.push_local_read_states(&list).await
    }

    async fn send_read_ack_impl(&self, conversation_id: &str, read_seq: u64) -> Result<()> {
        let ack = build_read_ack(
            self.sync_request_use_case.device_id(),
            conversation_id,
            read_seq,
        );
        self.sender.send_ack(&ack).await
    }

    /// 显式建群：把整张成员表一次性交服务端建群（之后消息不再携带成员表）。返回服务端是否建群成功；
    /// 失败（返回 false 或网络错误）时调用方回退到"首条消息携带成员表"的兜底建群。
    pub async fn ensure_conversation(
        &self,
        conversation_id: &str,
        conversation_type: i32,
        business_type: &str,
        channel_id: &str,
        member_ids: Vec<String>,
    ) -> Result<bool> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            ));
        }
        let resp = self
            .sync_request_use_case
            .request_ensure_conversation(EnsureConversationSync {
                conversation_id: conversation_id.to_string(),
                conversation_type,
                business_type: business_type.to_string(),
                channel_id: channel_id.to_string(),
                member_ids,
            })
            .await?;
        match resp.payload {
            Some(SyncResPayload::EnsureConversation(res)) => Ok(res.ok),
            _ => Err(FlareError::localized(
                ErrorCode::GeneralError,
                "unexpected ensure conversation sync response",
            )),
        }
    }

    /// 上行同步当前用户的会话偏好（置顶/免打扰/归档/草稿）。
    pub async fn push_conversation_user_settings(
        &self,
        conversation_id: &str,
        base_settings_version: u64,
        patch: crate::application::sync_task::ConversationUserSettingsPatch,
    ) -> Result<()> {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "conversation_id is required",
            ));
        }
        let resp = self
            .sync_request_use_case
            .request_conversation_user_settings(ConversationUserSettingsSync {
                conversation_id: conversation_id.to_string(),
                is_pinned: patch.is_pinned,
                is_muted: patch.is_muted,
                is_archived: patch.is_archived,
                draft: patch.draft,
                base_settings_version,
            })
            .await?;
        let Some(SyncResPayload::ConversationUserSettings(res)) = resp.payload else {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                "unexpected conversation user settings sync response",
            ));
        };
        let Some(settings) = res.settings else {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                "conversation user settings sync response missing settings",
            ));
        };
        if let Some(mut conversation) = self.stores.conversations.get(conversation_id).await? {
            conversation.is_pinned = settings.is_pinned;
            conversation.is_muted = settings.is_muted;
            conversation.is_archived = settings.is_archived;
            conversation.draft = non_empty(&settings.draft);
            apply_remote_settings_version(&mut conversation, settings.settings_version);
            self.stores
                .conversations
                .save_batch(&[conversation])
                .await?;
        }
        Ok(())
    }

    pub async fn push_conversation_user_settings_from_local(
        &self,
        conversation_id: &str,
        base_settings_version: u64,
        conversation: &crate::model::Conversation,
    ) -> Result<()> {
        self.push_conversation_user_settings(
            conversation_id,
            base_settings_version,
            crate::application::sync_task::ConversationUserSettingsPatch {
                is_pinned: Some(conversation.is_pinned),
                is_muted: Some(conversation.is_muted),
                is_archived: Some(conversation.is_archived),
                draft: conversation.draft.clone(),
            },
        )
        .await
    }

    /// 将 SyncRes.payload.single_conversation 转为事件列表并落库、发布、更新游标
    async fn apply_sync_res_single(
        &self,
        run: &SyncRunContext,
        conversation_id: &str,
        resp: &SyncRes,
        requested_after_seq: u64,
        requested_cursor: &str,
    ) -> Result<(u64, bool, String)> {
        let sc = match &resp.payload {
            Some(SyncResPayload::SingleConversation(s)) => s,
            _ => {
                tracing::warn!(conversation_id = %conversation_id, "同步响应payload为空或类型不匹配");
                self.transition_sync(run, SyncTransition::SyncDone);
                return Ok((0, false, String::new()));
            }
        };

        tracing::debug!(
            conversation_id = %conversation_id,
            items_count = sc.items.len(),
            max_seq = sc.max_conversation_seq,
            has_more = sc.has_more,
            "收到消息同步响应"
        );

        // I2：服务端明示增量窗口已失效 → 放弃本页增量回放，切快照重建（游标置服务端水位）。
        // 不推进游标的页不做重建：重建会把游标挪到服务端水位。
        if run.advances_cursor && snapshot_cutover_required(sc.stale.as_ref(), sc.hints.as_ref()) {
            tracing::warn!(
                conversation_id = %conversation_id,
                stale = ?sc.stale,
                "服务端建议快照重建，切换 gap-too-large 快照路径"
            );
            let cutover_seq = self
                .resync_conversation_from_snapshot(conversation_id)
                .await?;
            self.transition_sync(run, SyncTransition::SyncDone);
            return Ok((cutover_seq, false, String::new()));
        }

        let user_id = self.current_user_id().await;
        let cursor_seq = if user_id.is_empty() {
            0
        } else {
            self.stores
                .cursors
                .get_conversation_cursor(&user_id, conversation_id)
                .await?
                .map(|c| c.last_seq)
                .unwrap_or(0)
        };
        let cleared_floor = self
            .stores
            .conversations
            .get(conversation_id)
            .await?
            .map(|c| crate::domain::sync_visibility_floor(&c))
            .unwrap_or(0);
        // 不推进游标的页从请求位点起算：它可能在游标之上，也可能远在游标之下。
        let decode_after_seq = if run.advances_cursor {
            cursor_seq.min(requested_after_seq)
        } else {
            requested_after_seq
        }
        .max(cleared_floor);

        tracing::debug!(
            conversation_id = %conversation_id,
            cursor_seq = cursor_seq,
            requested_after_seq = requested_after_seq,
            cleared_floor = cleared_floor,
            decode_after_seq = decode_after_seq,
            "本地已知消息seq"
        );

        let applied = self
            .sync_apply_use_case
            .apply_single_conversation_page(conversation_id, &user_id, decode_after_seq, sc)
            .await?;
        if applied.has_decoded_items {
            self.transition_sync(run, SyncTransition::DataReceived);
        }
        // 冷启 tail 页是快照语义（最新一段 + 服务端水位，旧史按需回溯）：
        // 按增量页要求「从 0 连续」永远不成立，游标会卡死在 0，实时消息也进不了时间线。
        if is_cold_start_tail_snapshot(
            requested_after_seq,
            requested_cursor,
            applied.has_decoded_items,
            applied.max_seq,
            applied.remote_max_seq,
        ) {
            tracing::info!(
                conversation_id = %conversation_id,
                remote_max_seq = applied.remote_max_seq,
                item_count = sc.items.len(),
                "冷启 tail 页按快照采信：游标推到服务端水位，旧史留给按需回溯"
            );
            // 尾页证明从页内最小 seq 到水位，不从 1 起：更旧的历史留给往旧翻页时回填。
            self.sync_apply_use_case
                .record_coverage(
                    conversation_id,
                    crate::domain::timeline::tail_page_range(
                        applied.min_item_seq,
                        applied.remote_max_seq.max(applied.max_item_seq),
                    ),
                )
                .await;
            if !user_id.is_empty() && run.advances_cursor {
                self.save_watermark_cursor(&user_id, conversation_id, applied.remote_max_seq)
                    .await?;
            }
            self.transition_sync(run, SyncTransition::SyncDone);
            return Ok((applied.remote_max_seq, false, String::new()));
        }

        if applied.has_seq_gap {
            tracing::warn!(
                conversation_id = %conversation_id,
                cursor_seq,
                requested_after_seq,
                decode_after_seq,
                safe_max_seq = applied.max_seq,
                remote_max_seq = applied.remote_max_seq,
                "消息同步未到达远端水位，保留 cursor 在连续位点并等待后续补偿"
            );
        }
        if applied.max_seq > cursor_seq && !user_id.is_empty() && run.advances_cursor {
            self.save_cursor_with_remote(
                &user_id,
                conversation_id,
                applied.max_seq,
                &applied.absent_seqs,
            )
            .await?;
        }
        if applied.has_more {
            self.transition_sync(run, SyncTransition::BatchDone);
        } else {
            self.transition_sync(run, SyncTransition::SyncDone);
        }
        Ok((applied.max_seq, applied.has_more, applied.next_cursor))
    }

    async fn request_single_page(
        &self,
        run: &SyncRunContext,
        conversation_id: &str,
        last_seq: u64,
        limit: i32,
        cursor: String,
    ) -> Result<(u64, bool, String)> {
        tracing::debug!(
            conversation_id = %conversation_id,
            last_seq = last_seq,
            limit = limit,
            cursor = %cursor,
            "请求消息同步页面"
        );
        let mut retries = 0u8;
        loop {
            match self
                .sync_request_use_case
                .request_single_page(conversation_id, last_seq, limit, cursor.clone())
                .await
            {
                Ok(resp) => {
                    return self
                        .apply_sync_res_single(run, conversation_id, &resp, last_seq, &cursor)
                        .await;
                }
                Err(e) => {
                    retries += 1;
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        retry = retries,
                        error = %e,
                        "消息同步请求失败，准备重试"
                    );
                    if retries >= 3 {
                        tracing::error!(
                            conversation_id = %conversation_id,
                            "消息同步请求重试次数超过上限(3次)"
                        );
                        self.transition_sync(run, SyncTransition::SyncFailed);
                        return Err(FlareError::general_error(format!(
                            "sdk.sync.single_conversation.timeout_or_failed conversation_id={conversation_id}"
                        )));
                    }
                }
            }
        }
    }

    async fn request_query_events(&self, req: QueryEventsSync) -> Result<SyncRes> {
        self.sync_request_use_case.request_query_events(req).await
    }

    /// 关键事件回放补偿。`conversations` 由调用方提供（sync phase 内传共享快照，×N list 查询→×1）。
    pub async fn sync_critical_events(
        &self,
        conversations: &[crate::model::Conversation],
    ) -> Result<()> {
        let user_id = self.current_user_id().await;
        if user_id.is_empty() || conversations.is_empty() {
            return Ok(());
        }

        let total = conversations.len();
        let mut skipped = 0usize;
        for conversation in conversations {
            let summary_max_seq = conversation.max_seq;
            let conversation_id = conversation.conversation_id.clone();
            if conversation_id.trim().is_empty() {
                continue;
            }
            let cursor_key = critical_event_cursor_key(&conversation_id);
            let mut after_seq = self
                .stores
                .cursors
                .get_conversation_cursor(&user_id, &cursor_key)
                .await?
                .map(|c| c.last_seq as i64)
                .unwrap_or(0);

            if watermark_provably_clean(summary_max_seq, after_seq.max(0) as u64) {
                skipped = skipped.saturating_add(1);
                continue;
            }
            let mut fully_caught_up = false;

            loop {
                let req = QueryEventsReqV1 {
                    conversation_id: conversation_id.clone(),
                    after_seq: after_seq.max(0) as u64,
                    before_seq: 0,
                    limit: 200,
                    event_types: SyncPolicy::critical_event_query_plan().event_types,
                    include_deleted: true,
                };
                let resp = match self
                    .request_query_events(QueryEventsSync {
                        conversation_id: req.conversation_id,
                        after_conversation_seq: req.after_seq,
                        before_conversation_seq: req.before_seq,
                        limit: req.limit,
                        event_types: req.event_types,
                        include_deleted: req.include_deleted,
                        replay_preset: 0,
                        client_last_applied_conversation_seq: after_seq.max(0) as u64,
                    })
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        tracing::warn!(
                            conversation_id = %conversation_id,
                            error = %e,
                            after_seq,
                            "关键事件回放请求失败，跳过该会话本轮后台补偿"
                        );
                        break;
                    }
                };
                let Some(SyncResPayload::QueryEvents(query_res)) = resp.payload else {
                    break;
                };
                // I2：事件流缺口不可回放（服务端返回 stale/resync 建议）→ 快照重建该会话，
                // 事件游标跳到快照水位（快照已物化事件最终态，≤水位的事件被快照覆盖）。
                if snapshot_cutover_required(query_res.stale.as_ref(), query_res.hints.as_ref()) {
                    // 快照切换在 resync 内一并建立消息游标 + 关键事件位点（双位点同源）。
                    if let Err(e) = self
                        .resync_conversation_from_snapshot(&conversation_id)
                        .await
                    {
                        tracing::warn!(
                            conversation_id = %conversation_id,
                            error = %e,
                            "关键事件缺口快照切换失败，保留游标下轮重试"
                        );
                    }
                    break;
                }
                let envelope = query_res.envelope.unwrap_or_default();
                let applied_event_seqs = self
                    .sync_apply_use_case
                    .apply_critical_events(&user_id, &envelope.events)
                    .await;
                let safe_event_seq = max_applied_event_prefix_seq(
                    after_seq.max(0) as u64,
                    &envelope.events,
                    &applied_event_seqs,
                );
                if safe_event_seq > after_seq.max(0) as u64 {
                    after_seq = safe_event_seq as i64;
                    self.save_critical_checkpoint(&user_id, &cursor_key, safe_event_seq)
                        .await;
                }
                if envelope.max_conversation_seq > safe_event_seq {
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        after_seq,
                        safe_event_seq,
                        remote_max_seq = envelope.max_conversation_seq,
                        "关键事件回放未连续完成，保留 cursor 等待后续重放"
                    );
                    break;
                }
                if !envelope.has_more || envelope.max_conversation_seq == 0 {
                    fully_caught_up = true;
                    break;
                }
            }

            // I4：本会话事件已拉到服务端水位 → 把检查位点抬到摘要水位，
            // 使下一轮（重连/前台高频触发）可证明跳过，QueryEvents 从 O(会话数) 降为 O(变化)。
            if fully_caught_up && summary_max_seq > after_seq.max(0) as u64 {
                self.save_critical_checkpoint(&user_id, &cursor_key, summary_max_seq)
                    .await;
            }
        }
        tracing::debug!(total, skipped, "critical events sweep done (changed-only)");
        Ok(())
    }
}

impl SessionSyncRunner for SyncProtocolAdapter {
    fn request_message_sync(
        &self,
        conversation_id: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let id = conversation_id.to_string();
        Box::pin(async move { self.sync_conversation(&id).await })
    }

    fn request_message_sync_from_seq(
        &self,
        conversation_id: &str,
        last_seq: u64,
        limit: i32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let id = conversation_id.to_string();
        Box::pin(async move { self.sync_conversation_from_seq(&id, last_seq, limit).await })
    }

    fn request_message_backfill_before_seq(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: i32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + '_>> {
        let id = conversation_id.to_string();
        Box::pin(async move {
            self.sync_conversation_backfill_before_seq(&id, before_seq, limit)
                .await
        })
    }

    fn request_message_page_after_seq_detached(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: i32,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<bool>> + Send + '_>> {
        let id = conversation_id.to_string();
        Box::pin(async move {
            self.sync_conversation_page_after_seq_detached(&id, after_seq, limit)
                .await
                .map(|(_, has_more)| has_more)
        })
    }

    fn send_read_ack(
        &self,
        conversation_id: &str,
        read_seq: u64,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let id = conversation_id.to_string();
        Box::pin(async move { self.send_read_ack_impl(&id, read_seq).await })
    }

    fn request_participants_sync(
        &self,
        conversation_id: &str,
        limit: i32,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<crate::model::ConversationParticipant>>>
                + Send
                + '_,
        >,
    > {
        let id = conversation_id.to_string();
        Box::pin(async move { self.sync_conversation_participants(&id, limit).await })
    }
}

impl ConversationSummarySync for SyncProtocolAdapter {
    fn sync_conversation_summaries(
        &self,
        user_id: &str,
        run: SyncRunContext,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let user_id = user_id.to_string();
        Box::pin(async move { self.sync_conversations_with_context(&user_id, run).await })
    }

    fn sync_foreground_convergence(
        &self,
        user_id: &str,
        run: SyncRunContext,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let user_id = user_id.to_string();
        Box::pin(async move {
            self.sync_conversations_with_context(&user_id, run).await?;
            let conversations = self.stores.conversations.list().await?;
            self.sync_critical_events(&conversations).await
        })
    }
}

impl SyncResponseHandler for SyncProtocolAdapter {
    fn handle_sync_response(
        &self,
        resp: SyncRes,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if self
                .sync_request_use_case
                .handle_response(resp.clone())
                .await
            {
                return;
            }
            if resp.payload.is_none() {
                tracing::warn!("sync response has no payload, ignored");
            }
        })
    }
}

fn critical_event_cursor_key(conversation_id: &str) -> String {
    format!("{CRITICAL_EVENT_CURSOR_KEY}:{conversation_id}")
}

fn max_applied_event_prefix_seq(
    known_seq: u64,
    events: &[flare_proto::common::Event],
    applied_seqs: &[u64],
) -> u64 {
    if events.is_empty() {
        return known_seq;
    }

    let applied = applied_seqs
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let mut cursor = known_seq;
    for event in events {
        let seq = event.conversation_seq;
        if seq <= cursor {
            continue;
        }
        if applied.contains(&seq) {
            cursor = seq;
            continue;
        }
        break;
    }
    cursor
}

impl SyncProtocolAdapter {
    /// 拉取会话列表（conversation.proto SyncConversationsRequest，经 DATA 发送）
    pub async fn sync_conversations_impl(&self, user_id: &str) -> Result<()> {
        self.sync_conversations_with_context(user_id, SyncRunContext::initial_login())
            .await
    }

    pub async fn sync_conversations_with_context(
        &self,
        user_id: &str,
        run: SyncRunContext,
    ) -> Result<()> {
        tracing::debug!(user_id = %user_id, "开始同步会话列表");

        {
            let mut user = self.active_user_id.lock().await;
            *user = user_id.to_string();
        }
        self.transition_sync(&run, SyncTransition::SyncRequested);
        if run.needs_startup_catch_up()
            && let Err(error) = self.push_local_read_states_before_summary_sync().await
        {
            tracing::warn!(
                error = %error,
                "push local read states before summary sync failed"
            );
        }
        let prior_cursor = self
            .stores
            .cursors
            .get_conversation_cursor(user_id, CONVERSATION_CURSOR_KEY)
            .await?;
        let local_cursor_ms = prior_cursor.as_ref().and_then(|cursor| {
            if cursor.last_seq > 0 {
                Some(cursor.last_seq)
            } else if cursor.synced_at > 0 {
                Some(cursor.synced_at)
            } else {
                None
            }
        });
        let remote_cursor_ms = match self
            .get_remote_cursor_seq(user_id, CONVERSATION_CURSOR_KEY)
            .await
        {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "远端会话游标拉取失败，跳过远端游标继续会话列表同步"
                );
                None
            }
        };

        let local_conv_count = self.stores.conversations.list().await?.len();
        let cursor_selection = SyncPolicy::select_conversation_cursor_ms(
            local_cursor_ms,
            remote_cursor_ms,
            local_conv_count,
        );
        if cursor_selection.drop_local_incremental_cursor {
            tracing::debug!(
                "服务端未返回 __conversations__ 游标（常见于同步编排实例冷启动/缓存未命中）；放弃本地时间游标，全量拉会话列表"
            );
        }
        let mut cursor = cursor_selection
            .selected_cursor_ms
            .map(|value| value.to_string())
            .unwrap_or_default();

        tracing::debug!(
            local_cursor = ?local_cursor_ms,
            remote_cursor = ?remote_cursor_ms,
            using_cursor = %cursor,
            local_conv_count,
            "会话同步游标信息"
        );

        // I6 冷启 bootstrap bundle：无列表游标（真冷启/游标丢失）时，一次 RPC/页 同时拿
        // top-N 摘要 + 每会话首页消息（活跃度降序，首屏先到），替代 列表→逐会话消息 的串行瀑布。
        if cursor.is_empty() {
            return self.cold_start_bundle_sync(user_id, &run).await;
        }

        let mut total_synced = 0usize;
        loop {
            tracing::debug!(
                cursor = %cursor,
                "发送会话同步请求"
            );
            let resp = match self
                .sync_request_use_case
                .request_conversations(cursor.clone(), 100)
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(error = %error, "会话同步响应接收失败");
                    self.transition_sync(&run, SyncTransition::SyncFailed);
                    return Err(error);
                }
            };

            tracing::debug!(
                conversations_count = resp.conversations.len(),
                has_more = resp.has_more,
                "收到会话同步响应"
            );

            let applied = self
                .sync_apply_use_case
                .apply_conversations(user_id, &resp)
                .await?;
            total_synced += resp.conversations.len();
            let server_cursor_ms = snapshot_cursor_watermark_ms(&resp.next_cursor);
            if server_cursor_ms > 0 {
                // 列表水位是伪 key（无本地消息行），必须走无钳制保存。
                self.save_watermark_cursor(user_id, CONVERSATION_CURSOR_KEY, server_cursor_ms)
                    .await?;
            }
            if run.needs_startup_catch_up() {
                self.sync_conversation_messages_bounded(
                    &applied.message_sync_conversation_ids,
                    run.clone(),
                )
                .await?;
            } else {
                for conversation_id in &applied.message_sync_conversation_ids {
                    if let Err(error) = self
                        .sync_conversation_with_context(conversation_id, run.clone())
                        .await
                    {
                        tracing::warn!(
                            conversation_id = %conversation_id,
                            error = %error,
                            "会话摘要已更新，但补拉会话消息失败"
                        );
                        if run.visibility.is_user_visible() {
                            return Err(error);
                        }
                    }
                }
            }
            if !applied.has_more {
                self.transition_sync(&run, SyncTransition::SyncDone);
                tracing::debug!(total_synced, "会话列表同步完成");
                break;
            }
            cursor = resp.next_cursor;
        }
        Ok(())
    }

    /// I6 冷启 bundle：SyncSnapshot 分页循环（活跃度降序）。每页=摘要+首页消息一次到位；
    /// 页内数据落库后才推进各会话游标（I1）；**列表水位仅在整个 bundle 完成后保存**——
    /// 降序分页中途崩溃时下次仍走 bundle（幂等），避免"水位已到顶、旧会话永不拉取"的缺口。
    async fn cold_start_bundle_sync(&self, user_id: &str, run: &SyncRunContext) -> Result<()> {
        // 首屏放行：首页（最活跃 50 会话 + 首页消息）内联拉取并应用后即返回，
        // 余页交给后台续拉——大账号首屏从 O(页数) 个串行 RPC 压到 1 个。
        // 数据纪律不变：每页应用时建立自身消息/关键位点；列表水位只在全部页完成后保存
        //（中断 → 下次启动列表同步全量兜底，宁可多拉不丢）。
        let first = self.request_cold_bundle_page(String::new()).await?;
        let next_cursor =
            (first.has_more && !first.next_cursor.is_empty()).then(|| first.next_cursor.clone());
        let (watermark_ms, total) = self.apply_cold_bundle_page(user_id, run, first).await?;

        let Some(cursor) = next_cursor else {
            return self
                .finish_cold_bundle(user_id, run, watermark_ms, total)
                .await;
        };

        let continuation_available = self
            .self_weak
            .get()
            .and_then(std::sync::Weak::upgrade)
            .filter(|_| {
                !self
                    .cold_bundle_continuation_in_flight
                    .swap(true, std::sync::atomic::Ordering::AcqRel)
            });
        match continuation_available {
            Some(adapter) => {
                let user_id = user_id.to_string();
                let run = run.clone();
                crate::shared::util::spawn_background(async move {
                    adapter
                        .continue_cold_bundle(&user_id, &run, cursor, watermark_ms, total)
                        .await;
                    adapter
                        .cold_bundle_continuation_in_flight
                        .store(false, std::sync::atomic::Ordering::Release);
                });
                Ok(())
            }
            // 未 Arc 装配（测试/裸构造）或续拉已在飞：内联拉完，行为与旧实现一致。
            None => {
                let (watermark_ms, total) = self
                    .drain_cold_bundle_pages(user_id, run, cursor, watermark_ms, total)
                    .await?;
                self.finish_cold_bundle(user_id, run, watermark_ms, total)
                    .await
            }
        }
    }

    /// 后台续拉余页（首页已放行首屏）。错误不上抛：告警后放弃本轮，
    /// 列表水位未保存 → 下次启动列表同步全量兜底。
    async fn continue_cold_bundle(
        &self,
        user_id: &str,
        run: &SyncRunContext,
        cursor: String,
        watermark_ms: u64,
        total: usize,
    ) {
        match self
            .drain_cold_bundle_pages(user_id, run, cursor, watermark_ms, total)
            .await
        {
            Ok((watermark_ms, total)) => {
                if let Err(error) = self
                    .finish_cold_bundle(user_id, run, watermark_ms, total)
                    .await
                {
                    tracing::warn!(error = %error, "冷启 bundle 收尾失败（下次启动全量兜底）");
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "冷启 bundle 后台续拉中断（下次启动全量兜底）");
            }
        }
    }

    /// 页间流水线排空：应用第 N 页的同时并发请求第 N+1 页（网络等待与本地落库重叠；
    /// 服务端 BootstrapPageCache 使续拉页命中快照缓存，预取不放大服务端成本）。
    /// 每页前校验会话未切换（登出/换号即中止，写入幂等）。
    async fn drain_cold_bundle_pages(
        &self,
        user_id: &str,
        run: &SyncRunContext,
        first_cursor: String,
        mut watermark_ms: u64,
        mut total: usize,
    ) -> Result<(u64, usize)> {
        let mut current = self.request_cold_bundle_page(first_cursor).await?;
        loop {
            if self.current_user_id().await != user_id {
                tracing::info!("冷启 bundle 续拉中止：会话已切换");
                return Ok((watermark_ms, total));
            }
            let next_cursor = (current.has_more && !current.next_cursor.is_empty())
                .then(|| current.next_cursor.clone());
            let (applied, fetched) = match next_cursor {
                Some(cursor) => {
                    let (applied, fetched) = futures::join!(
                        self.apply_cold_bundle_page(user_id, run, current),
                        self.request_cold_bundle_page(cursor),
                    );
                    (applied, Some(fetched))
                }
                None => (
                    self.apply_cold_bundle_page(user_id, run, current).await,
                    None,
                ),
            };
            let (page_watermark, page_total) = applied?;
            watermark_ms = watermark_ms.max(page_watermark);
            total += page_total;
            match fetched {
                Some(next) => current = next?,
                None => return Ok((watermark_ms, total)),
            }
        }
    }

    async fn finish_cold_bundle(
        &self,
        user_id: &str,
        run: &SyncRunContext,
        watermark_ms: u64,
        total: usize,
    ) -> Result<()> {
        if watermark_ms > 0 {
            self.save_watermark_cursor(user_id, CONVERSATION_CURSOR_KEY, watermark_ms)
                .await?;
        }
        self.transition_sync(run, SyncTransition::SyncDone);
        tracing::info!(
            total,
            watermark_ms,
            "冷启 bundle 同步完成（摘要+首页消息一次到位）"
        );
        Ok(())
    }

    async fn request_cold_bundle_page(
        &self,
        snapshot_cursor: String,
    ) -> Result<flare_proto::common::SyncSnapshotSyncRes> {
        const COLD_BUNDLE_PAGE_CONVERSATIONS: i32 = 50;
        const COLD_BUNDLE_MESSAGES_PER_CONVERSATION: i32 = 30;
        self.sync_request_use_case
            .request_sync_snapshot(flare_proto::common::SyncSnapshotSync {
                conversation_ids: Vec::new(),
                messages_per_conversation: COLD_BUNDLE_MESSAGES_PER_CONVERSATION,
                include_deleted: false,
                include_conversations: true,
                snapshot_cursor,
                newest_first: true,
                conversation_page_limit: COLD_BUNDLE_PAGE_CONVERSATIONS,
            })
            .await
    }

    /// 应用一页冷启 bundle：摘要 + 首页消息 + 快照双位点。按值消费整页（免逐行深拷贝）。
    /// 返回（页内最大水位, 行数）。
    async fn apply_cold_bundle_page(
        &self,
        user_id: &str,
        run: &SyncRunContext,
        res: flare_proto::common::SyncSnapshotSyncRes,
    ) -> Result<(u64, usize)> {
        // WASM 上 yield_for_frame 是 setTimeout(0) 宏任务（嵌套 >5 层被钳到 ~4ms），
        // 逐行让帧一页 50 行最多白等 ~200ms；按块让帧兼顾流畅与冷启速度。
        const YIELD_EVERY_ROWS: usize = 8;
        if !res.conversations.is_empty() {
            self.transition_sync(run, SyncTransition::DataReceived);
        }

        let mut rows = res.conversations;
        let summaries: Vec<_> = rows
            .iter_mut()
            .filter_map(|row| row.summary.take())
            .collect();
        if !summaries.is_empty() {
            let synthesized = flare_proto::common::ConversationsSyncRes {
                conversations: summaries,
                next_cursor: String::new(),
                has_more: false,
                hints: None,
            };
            self.sync_apply_use_case
                .apply_conversations(user_id, &synthesized)
                .await?;
        }
        let mut watermark_ms: u64 = 0;
        let mut total = 0usize;
        // 页内先落数据、页末一批建双位点（I1 仍守"先数据后游标"，只是更晚）：
        // 每行 2 次串行 upsert（消息游标+关键位点）→ 每页 1 个事务。
        let mut cursor_entries: Vec<(String, u64)> = Vec::with_capacity(rows.len() * 2);
        let mut remote_pushes: Vec<(String, u64)> = Vec::with_capacity(rows.len());
        for row in rows {
            total += 1;
            watermark_ms = watermark_ms.max(row.last_message_at.max(0) as u64);
            let conversation_id = row.conversation_id.clone();
            let last_seq = self
                .sync_apply_use_case
                .apply_snapshot_row(user_id, row)
                .await?;
            if last_seq > 0 {
                cursor_entries.push((conversation_id.clone(), last_seq));
                cursor_entries.push((critical_event_cursor_key(&conversation_id), last_seq));
                remote_pushes.push((conversation_id, last_seq));
            }
            if total.is_multiple_of(YIELD_EVERY_ROWS) {
                crate::shared::util::yield_for_frame().await;
            }
        }
        self.sync_apply_use_case
            .save_watermarks(user_id, &cursor_entries)
            .await?;
        for (conversation_id, last_seq) in remote_pushes {
            // 去抖合并队列（K4），页末统一入队。
            self.queue_remote_cursor(&conversation_id, last_seq).await?;
        }
        Ok((watermark_ms, total))
    }

    /// I7：批量起始位点——3 次批量读替代每会话 3 次串行往返（批量优于 N+1；
    /// WASM/IndexedDB 桥上尤其显著，呼应 timeline-open N+1 教训）。
    async fn multi_conversation_start_positions(
        &self,
        user_id: &str,
        conversation_ids: &[String],
    ) -> Result<HashMap<String, u64>> {
        let cursors = self
            .stores
            .cursors
            .get_conversation_cursors(user_id, conversation_ids)
            .await?;
        let cleared_floors: HashMap<String, u64> = self
            .stores
            .conversations
            .get_many(conversation_ids)
            .await?
            .into_iter()
            .map(|c| {
                let floor = crate::domain::sync_visibility_floor(&c);
                (c.conversation_id, floor)
            })
            .collect();
        let local_max_seqs = self
            .stores
            .conversations
            .get_local_max_seqs(conversation_ids)
            .await?;

        let mut positions = HashMap::with_capacity(conversation_ids.len());
        for conversation_id in conversation_ids {
            let cursor_last_seq = cursors
                .get(conversation_id)
                .map(|cursor| cursor.last_seq)
                .unwrap_or(0);
            let cleared_floor = cleared_floors.get(conversation_id).copied().unwrap_or(0);
            let local_max_seq = local_max_seqs.get(conversation_id).copied().unwrap_or(0);
            positions.insert(
                conversation_id.clone(),
                local_message_sync_start_seq(cursor_last_seq, local_max_seq, cleared_floor),
            );
        }
        Ok(positions)
    }

    /// 批量补齐多个会话消息：一次 `MultiConversationSync` 请求承载多个 changed conversation，
    /// 每个 slice 仍复用单会话 page 应用逻辑，保留现有 LWW、pending 合并、缺口游标推进语义。
    pub(crate) async fn sync_multi_conversations_with_context(
        &self,
        conversation_ids: &[String],
        run: SyncRunContext,
        limit_per_conversation: i32,
    ) -> Result<MultiConversationCatchUpReport> {
        let mut seen = HashSet::new();
        let mut active: Vec<String> = conversation_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .filter_map(|id| {
                if seen.insert(id.to_string()) {
                    Some(id.to_string())
                } else {
                    None
                }
            })
            .collect();
        if active.is_empty() {
            return Ok(MultiConversationCatchUpReport::default());
        }

        self.transition_sync(&run, SyncTransition::SyncRequested);
        let user_id = self.current_user_id().await;
        let mut positions = self
            .multi_conversation_start_positions(&user_id, &active)
            .await?;
        let page_limit = limit_per_conversation.max(1);
        let mut report = MultiConversationCatchUpReport {
            requested_conversations: active.len(),
            ..Default::default()
        };
        let mut applied_conversation_ids = HashSet::new();

        while !active.is_empty() {
            report.pages = report.pages.saturating_add(1);
            tracing::debug!(
                conversations = active.len(),
                page = report.pages,
                limit_per_conversation = page_limit,
                "请求批量会话消息同步页面"
            );
            let active_positions = active
                .iter()
                .filter_map(|id| positions.get(id).map(|seq| (id.clone(), *seq)))
                .collect::<HashMap<_, _>>();
            let resp = self
                .sync_request_use_case
                .request_multi_conversation(active.clone(), active_positions, page_limit)
                .await?;
            let Some(SyncResPayload::MultiConversation(multi)) = resp.payload else {
                self.transition_sync(&run, SyncTransition::SyncFailed);
                return Err(FlareError::general_error(
                    "unexpected multi conversation sync response".to_string(),
                ));
            };

            let mut returned = HashSet::new();
            let mut next_active = Vec::new();
            let has_more_pages = multi.has_more;
            for slice in multi.slices {
                if slice.conversation_id.trim().is_empty() {
                    continue;
                }
                let conversation_id = slice.conversation_id.clone();
                returned.insert(conversation_id.clone());
                let requested_after_seq =
                    positions.get(&conversation_id).copied().unwrap_or_default();
                let single = single_response_from_multi_slice(slice);
                report.decoded_items = report.decoded_items.saturating_add(single.items.len());
                let single_resp = SyncRes {
                    payload: Some(SyncResPayload::SingleConversation(single)),
                };
                match self
                    .apply_sync_res_single(
                        &run,
                        &conversation_id,
                        &single_resp,
                        requested_after_seq,
                        // 批量切片走服务端 contiguous 构造（缺口补 tombstone），
                        // 不会是 tail 快照；空游标即可。
                        "",
                    )
                    .await
                {
                    Ok((next_seq, has_more, _next_cursor)) => {
                        applied_conversation_ids.insert(conversation_id.clone());
                        report.applied_conversations = applied_conversation_ids.len();
                        if has_more {
                            if next_seq > requested_after_seq {
                                positions.insert(conversation_id.clone(), next_seq);
                                next_active.push(conversation_id);
                            } else {
                                report.failed_conversations =
                                    report.failed_conversations.saturating_add(1);
                                tracing::warn!(
                                    conversation_id = %conversation_id,
                                    requested_after_seq,
                                    next_seq,
                                    "批量消息同步未推进，等待后续缺口修复"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        report.failed_conversations = report.failed_conversations.saturating_add(1);
                        tracing::warn!(
                            conversation_id = %conversation_id,
                            error = %error,
                            "批量消息 slice 应用失败"
                        );
                    }
                }
            }

            for conversation_id in &active {
                if !returned.contains(conversation_id) {
                    report.failed_conversations = report.failed_conversations.saturating_add(1);
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        "批量消息同步响应缺少会话 slice"
                    );
                }
            }
            // I9：每应用完一页 slices 让帧，catch-up 中不饿死渲染/事件循环（WASM 尤其）。
            crate::shared::util::yield_for_frame().await;

            if !has_more_pages || next_active.is_empty() {
                break;
            }
            active = next_active;
        }

        Ok(report)
    }

    async fn sync_conversation_messages_bounded(
        &self,
        conversation_ids: &[String],
        run: SyncRunContext,
    ) -> Result<()> {
        if conversation_ids.is_empty() {
            return Ok(());
        }
        let report = self
            .sync_multi_conversations_with_context(
                conversation_ids,
                run.clone(),
                DEFAULT_SYNC_LIMIT,
            )
            .await?;
        tracing::debug!(
            requested = report.requested_conversations,
            applied = report.applied_conversations,
            failed = report.failed_conversations,
            pages = report.pages,
            decoded_items = report.decoded_items,
            "会话摘要更新后的批量消息补拉完成"
        );
        Ok(())
    }

    /// 单会话消息同步（sync.proto Sync.single_conversation，经 DATA 发送）
    pub async fn sync_conversation(&self, conversation_id: &str) -> Result<()> {
        self.sync_conversation_with_context(
            conversation_id,
            SyncRunContext::manual_single_conversation(),
        )
        .await
    }

    #[tracing::instrument(skip(self, run), fields(conversation_id = %conversation_id, trigger = ?run.trigger))]
    pub async fn sync_conversation_with_context(
        &self,
        conversation_id: &str,
        run: SyncRunContext,
    ) -> Result<()> {
        tracing::debug!(conversation_id = %conversation_id, "开始同步会话消息");
        let started_at = now_millis();

        self.transition_sync(&run, SyncTransition::SyncRequested);
        let user_id = self.current_user_id().await;
        let cursor_last_seq = self
            .stores
            .cursors
            .get_conversation_cursor(&user_id, conversation_id)
            .await?
            .map(|c| c.last_seq)
            .unwrap_or(0);
        let conversation = self.stores.conversations.get(conversation_id).await?;
        let cleared_floor = conversation
            .as_ref()
            .map(crate::domain::sync_visibility_floor)
            .unwrap_or(0);
        let local_max_seq = self
            .stores
            .conversations
            .get_local_max_seq(conversation_id)
            .await?;
        let remote_cursor_seq = if user_id.is_empty() {
            0
        } else {
            self.get_remote_cursor_seq(&user_id, conversation_id)
                .await
                .ok()
                .flatten()
                .unwrap_or(0)
        };
        let last_seq = local_message_sync_start_seq(cursor_last_seq, local_max_seq, cleared_floor);

        tracing::debug!(
            conversation_id = %conversation_id,
            user_id = %user_id,
            last_seq = last_seq,
            cursor_last_seq = cursor_last_seq,
            local_max_seq = local_max_seq,
            cleared_floor = cleared_floor,
            remote_cursor_seq = remote_cursor_seq,
            "会话消息同步起始位置"
        );

        let (page_count, total_messages) = self
            .sync_conversation_loop(&run, conversation_id, last_seq, DEFAULT_SYNC_LIMIT)
            .await?;
        tracing::debug!(
            conversation_id = %conversation_id,
            total_pages = page_count,
            total_messages = total_messages,
            "会话消息同步完成"
        );
        self.metrics.histogram_with_labels(
            "sync.conversation_duration_ms",
            &[MetricLabel::new("trigger", format!("{:?}", run.trigger))],
            now_millis().saturating_sub(started_at) as f64,
        );
        self.metrics
            .counter("sync.messages_total", total_messages as u64);
        self.metrics
            .gauge("sync.last_finished_at_ms", now_millis() as i64);
        // 会话级关键事件兜底回放容易在会话数较多时放大 QueryEvents 请求量，
        // 且与全局 key_events 任务存在重复。这里不阻塞消息同步链路，优先保证消息实时性。
        Ok(())
    }

    /// 按需同步非单聊会话成员。会话列表只负责摘要；成员在打开会话、发起群通话或空闲任务中调用。
    pub async fn sync_conversation_participants(
        &self,
        conversation_id: &str,
        page_limit: i32,
    ) -> Result<Vec<crate::model::ConversationParticipant>> {
        let Some(store) = &self.stores.conversation_participants else {
            return Ok(Vec::new());
        };
        let known_version = store.version(conversation_id).await?;
        let mut cursor = String::new();
        let mut all = Vec::new();
        let mut first_page = true;
        loop {
            let resp = self
                .sync_request_use_case
                .request_conversation_participants(ConversationParticipantsSync {
                    conversation_id: conversation_id.to_string(),
                    known_participant_version: known_version,
                    cursor: cursor.clone(),
                    limit: page_limit.max(1),
                    include_removed: false,
                })
                .await?;
            let Some(SyncResPayload::ConversationParticipants(page)) = resp.payload else {
                return Err(FlareError::general_error(
                    "unexpected conversation participants response".to_string(),
                ));
            };
            let participants = page
                .participants
                .into_iter()
                .map(crate::model::ConversationParticipant::from)
                .collect::<Vec<_>>();
            if !participants.is_empty() || first_page {
                store
                    .save_page(
                        conversation_id,
                        &participants,
                        page.participant_version,
                        first_page && cursor.is_empty(),
                    )
                    .await?;
            }
            all.extend(participants);
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
            first_page = false;
        }
        Ok(all)
    }

    /// 单会话消息同步（显式指定 last_seq 与 limit，供业务层对接 storage sync 契约）
    pub async fn sync_conversation_from_seq(
        &self,
        conversation_id: &str,
        last_seq: u64,
        limit: i32,
    ) -> Result<()> {
        self.sync_conversation_from_seq_with_context(
            conversation_id,
            last_seq,
            limit,
            SyncRunContext::silent_gap_repair(),
        )
        .await
    }

    pub async fn sync_conversation_backfill_before_seq(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: i32,
    ) -> Result<bool> {
        if before_seq == 0 {
            return Ok(false);
        }
        let run = SyncRunContext::silent_gap_repair();
        self.transition_sync(&run, SyncTransition::SyncRequested);
        let page_limit = if limit > 0 { limit } else { DEFAULT_SYNC_LIMIT };
        let resp = self
            .sync_request_use_case
            .request_backfill_page(conversation_id, before_seq, page_limit)
            .await?;
        let Some(SyncResPayload::SingleConversation(sc)) = resp.payload else {
            self.transition_sync(&run, SyncTransition::SyncDone);
            return Ok(false);
        };
        let has_more = sc.has_more;
        let user_id = self.current_user_id().await;
        let applied = self
            .sync_apply_use_case
            .apply_single_conversation_page(conversation_id, &user_id, 0, &sc)
            .await?;
        if applied.has_decoded_items {
            self.transition_sync(&run, SyncTransition::DataReceived);
        }
        // 落库之后才写它证明的区间：还有更早则 [页内最小 seq, before - 1]，否则 [1, before - 1]。
        self.sync_apply_use_case
            .record_backfill_coverage(conversation_id, before_seq, &sc)
            .await;
        self.transition_sync(&run, SyncTransition::SyncDone);
        Ok(has_more)
    }

    /// 取 `after_seq` 之后的一页：消息落库、写入它证明的区间，**不推进同步游标**。
    ///
    /// 围绕某条消息打开时间线时向新的方向取页用它：那一页可能远在游标之下，推进游标会把它
    /// 挪到孤岛上，之后的实时消息和缺口补拉都会错位。返回（页内连续到的 seq, 服务端是否还有更新的）。
    pub async fn sync_conversation_page_after_seq_detached(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: i32,
    ) -> Result<(u64, bool)> {
        let run = SyncRunContext::detached_page();
        self.transition_sync(&run, SyncTransition::SyncRequested);
        let page_limit = if limit > 0 { limit } else { DEFAULT_SYNC_LIMIT };
        let (max_seq, has_more, _next_cursor) = self
            .request_single_page(&run, conversation_id, after_seq, page_limit, String::new())
            .await?;
        Ok((max_seq, has_more))
    }

    pub async fn sync_conversation_from_seq_with_context(
        &self,
        conversation_id: &str,
        last_seq: u64,
        limit: i32,
        run: SyncRunContext,
    ) -> Result<()> {
        self.transition_sync(&run, SyncTransition::SyncRequested);
        let page_limit = if limit > 0 { limit } else { DEFAULT_SYNC_LIMIT };
        let _ = self
            .sync_conversation_loop(&run, conversation_id, last_seq, page_limit)
            .await?;
        Ok(())
    }

    async fn sync_conversation_loop(
        &self,
        run: &SyncRunContext,
        conversation_id: &str,
        start_seq: u64,
        page_limit: i32,
    ) -> Result<(usize, usize)> {
        let mut from_seq = start_seq;
        let mut cursor = String::new();
        let mut total_messages = 0usize;
        let mut page_count = 0usize;

        loop {
            page_count += 1;
            let (next_seq, has_more, next_cursor) = self
                .request_single_page(run, conversation_id, from_seq, page_limit, cursor)
                .await?;

            let messages_in_page = if next_seq > from_seq {
                (next_seq - from_seq) as usize
            } else {
                0
            };
            total_messages += messages_in_page;

            tracing::debug!(
                conversation_id = %conversation_id,
                page = page_count,
                from_seq = from_seq,
                next_seq = next_seq,
                has_more = has_more,
                messages_in_page = messages_in_page,
                total_messages = total_messages,
                "消息同步页面完成"
            );

            if next_seq <= from_seq {
                if should_repair_operation_only_gap(start_seq, from_seq, next_seq) {
                    tracing::debug!(
                        conversation_id = %conversation_id,
                        start_seq,
                        from_seq,
                        next_seq,
                        "消息增量未推进，执行 operation-only 窗口修复"
                    );
                    let _ = self
                        .request_single_page(run, conversation_id, 0, page_limit, String::new())
                        .await?;
                }
                if has_more {
                    tracing::warn!(
                        conversation_id = %conversation_id,
                        from_seq,
                        next_seq,
                        "消息同步页面未推进但仍存在远端水位，停止本轮同步并等待后续补偿"
                    );
                    self.transition_sync(run, SyncTransition::SyncFailed);
                }
                break;
            }
            if !has_more {
                break;
            }
            from_seq = next_seq;
            cursor = next_cursor;
        }

        Ok((page_count, total_messages))
    }
}

fn should_repair_operation_only_gap(start_seq: u64, from_seq: u64, next_seq: u64) -> bool {
    start_seq > 0 && from_seq >= start_seq && next_seq <= from_seq
}

/// 冷启 tail 页识别：`after_seq=0 且无游标` 时服务端回的是**快照**语义 ——
/// 只有最新一段消息 + 服务端水位，旧史留给按需回溯。这种页从 0 起算连续性恒为 0，
/// 当增量页处理会让游标永远推不动：每轮同步拉回同一页，实时消息也进不了时间线。
///
/// 判据必须同时成立，才不会把「真的从 seq 1 开始、只是本地还没落库」的增量页误判成快照：
/// 页里确实解出了东西（不是空页），但连续位点仍是 0（说明服务端跳过了头部），
/// 且服务端给出了非零水位。
fn is_cold_start_tail_snapshot(
    requested_after_seq: u64,
    cursor: &str,
    has_decoded_items: bool,
    safe_max_seq: u64,
    remote_max_seq: u64,
) -> bool {
    requested_after_seq == 0
        && cursor.trim().is_empty()
        && has_decoded_items
        && safe_max_seq == 0
        && remote_max_seq > 0
}

#[cfg(test)]
mod tests {
    use super::{
        build_read_ack, is_cold_start_tail_snapshot, max_applied_event_prefix_seq,
        should_repair_operation_only_gap, single_response_from_multi_slice,
    };
    use flare_proto::common::ack::Payload as AckPayload;
    use flare_proto::common::{ConversationSyncSlice, SyncSliceItem};

    fn make_event(seq: u64) -> flare_proto::common::Event {
        flare_proto::common::Event {
            conversation_seq: seq,
            ..Default::default()
        }
    }

    #[test]
    fn event_prefix_progresses_with_sparse_seq_when_prefix_applied() {
        let events = vec![make_event(1184), make_event(1191), make_event(1202)];
        let safe = max_applied_event_prefix_seq(0, &events, &[1184, 1191, 1202]);
        assert_eq!(safe, 1202);
    }

    #[test]
    fn event_prefix_stops_on_first_not_applied_event() {
        let events = vec![make_event(1184), make_event(1191), make_event(1202)];
        let safe = max_applied_event_prefix_seq(0, &events, &[1184, 1202]);
        assert_eq!(safe, 1184);
    }

    #[test]
    fn event_prefix_keeps_known_seq_when_no_events() {
        let safe = max_applied_event_prefix_seq(77, &[], &[]);
        assert_eq!(safe, 77);
    }

    #[test]
    fn cold_start_tail_page_is_recognised_as_snapshot() {
        // 线上现场：本地库空、远端游标 42，于是从 0 拉；服务端回 tail（首条 seq=3）。
        // 从 0 起算连续位点必然是 0 —— 必须认成快照，否则游标永远停在 0。
        assert!(is_cold_start_tail_snapshot(0, "", true, 0, 42));
    }

    #[test]
    fn ordinary_incremental_pages_are_not_mistaken_for_snapshots() {
        // 有游标 → 是续拉，不是冷启。
        assert!(!is_cold_start_tail_snapshot(0, "seq:42", true, 0, 42));
        // 从非 0 位点续拉。
        assert!(!is_cold_start_tail_snapshot(12, "", true, 0, 42));
        // 空页：没东西可采信，不能据此把游标抬到水位。
        assert!(!is_cold_start_tail_snapshot(0, "", false, 0, 42));
        // 页面真的从 seq 1 开始且连续 → 走正常增量路径。
        assert!(!is_cold_start_tail_snapshot(0, "", true, 6, 42));
        // 服务端没有水位可采信。
        assert!(!is_cold_start_tail_snapshot(0, "", true, 0, 0));
    }

    #[test]
    fn build_read_ack_uses_typed_payload() {
        let ack = build_read_ack("device-1", "conv-1", 42);

        match ack.payload {
            Some(AckPayload::Read(read)) => {
                assert_eq!(read.conversation_id, "conv-1");
                assert_eq!(read.read_seq, 42);
                assert_eq!(read.device_id.as_deref(), Some("device-1"));
                assert_eq!(read.ack_id, ack.ack_id);
            }
            other => panic!("expected AckPayload::Read, got {other:?}"),
        }
    }

    #[test]
    fn operation_only_repair_runs_for_non_zero_cursor_without_progress() {
        assert!(should_repair_operation_only_gap(3, 3, 3));
        assert!(!should_repair_operation_only_gap(0, 0, 0));
        assert!(!should_repair_operation_only_gap(3, 3, 4));
    }

    #[test]
    fn snapshot_cursor_watermark_extracts_ms_from_paging_and_plain_forms() {
        use super::snapshot_cursor_watermark_ms;
        // 分页游标 "ms|cid" → 取 ms（旧 parse::<u64> 得 0，列表游标从不前进）。
        assert_eq!(snapshot_cursor_watermark_ms("2000|conv-9"), 2000);
        // 纯 ms 回显 → 原样。
        assert_eq!(snapshot_cursor_watermark_ms("1735"), 1735);
        // 空/非法 → 0（调用方跳过保存，不倒退游标）。
        assert_eq!(snapshot_cursor_watermark_ms(""), 0);
        assert_eq!(snapshot_cursor_watermark_ms("abc|c1"), 0);
    }

    #[test]
    fn critical_events_skip_requires_known_watermark_at_or_below_checkpoint() {
        use crate::kernel::watermark_provably_clean;
        // 摘要水位 ≤ 已检查位点 → 可证明无新事件，跳过。
        assert!(watermark_provably_clean(10, 10));
        assert!(watermark_provably_clean(10, 12));
        // 水位领先位点 → 必须查。
        assert!(!watermark_provably_clean(13, 12));
        // 水位未知(0) → 保守照拉。
        assert!(!watermark_provably_clean(0, 12));
    }

    #[test]
    fn snapshot_cutover_triggers_on_stale_or_hint_recovery() {
        use super::snapshot_cutover_required;
        use flare_proto::common::{SyncRecoveryHint, SyncSessionHints, SyncStaleContext};

        assert!(!snapshot_cutover_required(None, None));

        let refetch_stale = SyncStaleContext {
            recommended_action: SyncRecoveryHint::RefetchSnapshot as i32,
            ..Default::default()
        };
        assert!(snapshot_cutover_required(Some(&refetch_stale), None));

        let resync_stale = SyncStaleContext {
            recommended_action: SyncRecoveryHint::ResyncConversation as i32,
            ..Default::default()
        };
        assert!(snapshot_cutover_required(Some(&resync_stale), None));

        let replay_ok = SyncStaleContext {
            recommended_action: SyncRecoveryHint::ReplayEvents as i32,
            ..Default::default()
        };
        assert!(!snapshot_cutover_required(Some(&replay_ok), None));

        let hint_resync = SyncSessionHints {
            recovery_hint: SyncRecoveryHint::ResyncConversation as i32,
            ..Default::default()
        };
        assert!(snapshot_cutover_required(None, Some(&hint_resync)));

        let hint_none = SyncSessionHints::default();
        assert!(!snapshot_cutover_required(None, Some(&hint_none)));
    }

    #[test]
    fn multi_conversation_slice_reuses_single_page_shape() {
        let slice = ConversationSyncSlice {
            conversation_id: "conv-1".to_string(),
            items: vec![SyncSliceItem {
                conversation_seq: 7,
                ..Default::default()
            }],
            max_conversation_seq: 9,
            next_cursor: "next".to_string(),
            has_more: true,
        };

        let single = single_response_from_multi_slice(slice);

        assert_eq!(single.conversation_id, "conv-1");
        assert_eq!(single.items.len(), 1);
        assert_eq!(single.items[0].conversation_seq, 7);
        assert_eq!(single.max_conversation_seq, 9);
        assert_eq!(single.next_cursor, "next");
        assert!(single.has_more);
    }
}

#[cfg(test)]
#[path = "sync_protocol_adapter_coverage_tests.rs"]
mod coverage_tests;
