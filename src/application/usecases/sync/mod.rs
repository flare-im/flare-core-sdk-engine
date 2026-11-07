mod coverage;
mod decoding;
mod display;
mod event_applier;
mod models;

use crate::application::projections::ConversationProjectionApplier;
use crate::application::services::EventDeduper;
use crate::application::services::IncomingMessageConverger;
use decoding::decode_single_conversation_items;
use event_applier::SyncEventApplier;
use models::{AppliedConversationIncremental, AppliedSingleConversationPage, ReplayMode};

use crate::application::notification::{
    InboundDelivery, NotificationInboundPipeline, partition_notification_durability,
};
use crate::domain::timeline::{contiguous_page_range, cursor_advance_range, snapshot_rows_range};
use crate::domain::{
    ConversationIdRewrite, ConversationIdentityService, SeqRange, SyncCursorVo,
    filter_messages_after_clear, local_cleared_through_seq,
};
use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::ReliableSendQueuePort;
use crate::kernel::event::{ConversationEvent, EventBus, SdkEvent};
use crate::model::IMMessage;
use crate::shared::error::Result;
use flare_proto::common::{ConversationsSyncRes, SingleConversationSyncRes};

pub struct SyncApplyUseCase {
    stores: StoreProvider,
    bus: EventBus,
    event_applier: SyncEventApplier,
    notification_pipeline: NotificationInboundPipeline,
    incoming_message_converger: IncomingMessageConverger,
    conversation_projection_applier: ConversationProjectionApplier,
}

impl SyncApplyUseCase {
    pub fn new(
        stores: StoreProvider,
        bus: EventBus,
        event_deduper: EventDeduper,
        notification_pipeline: NotificationInboundPipeline,
    ) -> Self {
        let event_applier = SyncEventApplier::new(stores.clone(), bus.clone(), event_deduper);
        let incoming_message_converger = IncomingMessageConverger::new(
            stores.messages.clone(),
            stores.conversations.clone(),
            bus.clone(),
            None,
        );
        let conversation_projection_applier =
            ConversationProjectionApplier::new(stores.clone(), bus.clone());
        Self {
            stores,
            bus,
            event_applier,
            notification_pipeline,
            incoming_message_converger,
            conversation_projection_applier,
        }
    }

    pub fn set_reliable_queue(
        &self,
        reliable_queue: Option<std::sync::Arc<dyn ReliableSendQueuePort>>,
    ) {
        self.incoming_message_converger
            .set_reliable_queue(reliable_queue);
    }

    async fn local_message_sync_start_seq(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<u64> {
        let cursor_seq = self
            .stores
            .cursors
            .get_conversation_cursor(user_id, conversation_id)
            .await?
            .map(|cursor| cursor.last_seq)
            .unwrap_or_default();
        let local_max_seq = self
            .stores
            .conversations
            .get_local_max_seq(conversation_id)
            .await?;
        let cleared_floor = self
            .stores
            .conversations
            .get(conversation_id)
            .await?
            .map(|conversation| crate::domain::sync_visibility_floor(&conversation))
            .unwrap_or_default();
        Ok(local_message_sync_start_seq(
            cursor_seq,
            local_max_seq,
            cleared_floor,
        ))
    }

    pub async fn apply_single_conversation_page(
        &self,
        conversation_id: &str,
        user_id: &str,
        known_seq: u64,
        response: &SingleConversationSyncRes,
        delivery: InboundDelivery,
    ) -> Result<AppliedSingleConversationPage> {
        let decoded = decode_single_conversation_items(response, known_seq);

        // I1 顺序契约：游标只计入「落库有保证」的 seq——消息/skip/tombstone（save_batch 失败整页出错）
        // + 应用**成功**的事件。失败事件留在游标之外形成缺口，由既有 has_seq_gap 补拉重放；
        // 若在此处计入解码 seq，游标会越过失败事件造成永久丢失。
        let event_outcomes = self
            .event_applier
            .apply_events(user_id, &decoded.events, ReplayMode::SingleConversation)
            .await;
        let mut applied_item_seqs = decoded.covered_item_seqs.clone();
        applied_item_seqs.extend(
            decoded
                .event_item_seqs
                .iter()
                .zip(&event_outcomes)
                .filter(|(seq, applied)| **seq > 0 && **applied)
                .map(|(seq, _)| *seq),
        );

        self.persist_converged_messages(user_id, conversation_id, decoded.messages, delivery)
            .await?;

        let safe_max_seq = max_contiguous_seq(known_seq, &applied_item_seqs);
        // 连续同步页证明 (known_seq, safe_max_seq]：其中每个 seq 都已落库成消息、应用成事件，
        // 或由 skip/tombstone 证明没有消息。
        self.record_coverage(
            conversation_id,
            contiguous_page_range(known_seq, safe_max_seq),
        )
        .await;
        let (min_item_seq, max_item_seq) = seq_bounds(&applied_item_seqs);
        let has_seq_gap = response.max_conversation_seq > safe_max_seq;
        if has_seq_gap {
            tracing::warn!(
                conversation_id = %conversation_id,
                known_seq,
                safe_max_seq,
                remote_max_seq = response.max_conversation_seq,
                item_count = response.items.len(),
                "消息同步响应存在非连续 seq，游标只推进到已落库连续位点"
            );
        }

        Ok(AppliedSingleConversationPage {
            has_decoded_items: decoded.has_decoded_items,
            max_seq: safe_max_seq,
            remote_max_seq: response.max_conversation_seq,
            has_more: response.has_more || has_seq_gap,
            next_cursor: response.next_cursor.clone(),
            has_seq_gap,
            absent_seqs: decoded.absent_item_seqs,
            min_item_seq,
            max_item_seq,
        })
    }

    /// I2 gap-too-large 快照切换：应用一行会话快照（最新页消息 + 服务端权威已读/未读），
    /// 返回快照水位 `last_conversation_seq` 供调用方在**落库成功后**推进游标（守 I1 顺序契约）。
    /// 更旧历史不在此拉取——由既有 backfill 按需补。
    pub async fn apply_snapshot_row(
        &self,
        user_id: &str,
        row: flare_proto::common::SnapshotConversationRow,
    ) -> Result<u64> {
        // 快照行是会话最新的若干条消息，证明它们之间没有别的消息（不延到水位）。
        let proven =
            snapshot_rows_range(row.messages.iter().map(|message| message.conversation_seq));
        // 按值消费：快照行的消息 move 进收敛管线（冷启 bundle 每页 50×30 条，免整页深拷贝）。
        let messages: Vec<IMMessage> = row.messages.into_iter().map(IMMessage::new).collect();
        self.persist_converged_messages(
            user_id,
            &row.conversation_id,
            messages,
            InboundDelivery::HistoryReplay,
        )
        .await?;
        // 快照行携带服务端权威已读/未读，直接覆盖本地摘要（LWW：快照即最新状态）。
        if let Err(error) = self
            .stores
            .conversations
            .update_unread(
                &row.conversation_id,
                row.unread_count.max(0) as u32,
                row.last_read_seq,
            )
            .await
        {
            tracing::warn!(
                conversation_id = %row.conversation_id,
                error = %error,
                "快照切换：覆盖本地未读/已读失败（消息已落库，游标仍可推进）"
            );
        }
        self.record_coverage(&row.conversation_id, proven).await;
        Ok(row.last_conversation_seq)
    }

    /// 消息收敛落库管线（页应用/快照行应用共用）：清空水位过滤 → 身份规范化+pending 合并 →
    /// durable 落库 + 会话投影 → 视图/通知投递。`save_batch` 失败向上传播（游标因此不动，守 I1）。
    async fn persist_converged_messages(
        &self,
        user_id: &str,
        conversation_id: &str,
        messages: Vec<IMMessage>,
        delivery: InboundDelivery,
    ) -> Result<()> {
        let cleared_floor = self
            .stores
            .conversations
            .get(conversation_id)
            .await?
            .map(|c| crate::domain::sync_visibility_floor(&c))
            .unwrap_or(0);
        let fresh_messages = filter_messages_after_clear(
            self.incoming_message_converger
                .converge_messages(user_id, messages)
                .await?,
            cleared_floor,
        );
        if fresh_messages.is_empty() {
            return Ok(());
        }
        let (durable_messages, ephemeral_messages): (Vec<IMMessage>, Vec<IMMessage>) =
            partition_notification_durability(fresh_messages);
        if !durable_messages.is_empty() {
            self.stores.messages.save_batch(&durable_messages).await?;
            self.conversation_projection_applier
                .apply_synced_messages(&durable_messages, user_id)
                .await?;
        }
        let mut inbound = durable_messages;
        inbound.extend(ephemeral_messages);
        self.notification_pipeline
            .finish_batch_with_delivery(inbound, delivery)
            .await;
        Ok(())
    }

    pub async fn apply_critical_events(
        &self,
        user_id: &str,
        events: &[flare_proto::common::Event],
    ) -> Vec<u64> {
        let outcomes = self
            .event_applier
            .apply_events(user_id, events, ReplayMode::CriticalEvents)
            .await;
        events
            .iter()
            .zip(outcomes)
            .filter(|(_, applied)| *applied)
            .map(|(event, _)| event.conversation_seq)
            .collect()
    }

    pub async fn apply_conversations(
        &self,
        user_id: &str,
        response: &ConversationsSyncRes,
    ) -> Result<AppliedConversationIncremental> {
        let mut conversation_ids = Vec::new();
        let mut message_sync_conversation_ids = Vec::new();
        let mut conversations = Vec::new();

        for summary in response
            .conversations
            .iter()
            .filter(|summary| !is_hidden_internal_conversation(&summary.conversation_id))
            .cloned()
        {
            let mut conversation = crate::model::Conversation::from(summary);
            if let Some(rewrite) =
                ConversationIdentityService::canonicalize_single_chat_conversation(
                    &mut conversation,
                    user_id,
                )
            {
                self.apply_conversation_identity_rewrite(&rewrite).await?;
            }
            if is_hidden_internal_conversation(&conversation.conversation_id) {
                tracing::warn!(
                    conversation_id = %conversation.conversation_id,
                    "drop invalid or hidden conversation summary before local save"
                );
                continue;
            }
            conversation_ids.push(conversation.conversation_id.clone());
            conversations.push(conversation);
        }

        if !conversations.is_empty() {
            for conversation in &conversations {
                if conversation.conversation_id.trim().is_empty() {
                    continue;
                }
                let remote_target_seq = remote_conversation_target_seq(conversation);
                if remote_target_seq == 0 {
                    continue;
                }
                let local_message_seq = self
                    .local_message_sync_start_seq(user_id, &conversation.conversation_id)
                    .await?;
                if remote_target_seq > local_message_seq {
                    message_sync_conversation_ids.push(conversation.conversation_id.clone());
                }
            }
            let mut before_save = std::collections::HashMap::new();
            for conversation in &conversations {
                if let Ok(Some(prev)) = self
                    .stores
                    .conversations
                    .get(&conversation.conversation_id)
                    .await
                {
                    before_save.insert(conversation.conversation_id.clone(), prev);
                }
            }
            for conversation in &conversations {
                let previous_unread = before_save
                    .get(&conversation.conversation_id)
                    .map(|c| c.unread_count)
                    .unwrap_or_default();
                if should_sync_messages_for_unread_delta(previous_unread, conversation.unread_count)
                    && !message_sync_conversation_ids
                        .iter()
                        .any(|id| id == &conversation.conversation_id)
                {
                    message_sync_conversation_ids.push(conversation.conversation_id.clone());
                }
            }
            // Core summaries may omit the business SDK's group profile. An
            // unread/read synchronization must not erase its projected name.
            for conversation in &mut conversations {
                display::retain_projected_group_name(
                    conversation,
                    before_save.get(&conversation.conversation_id),
                );
            }
            if let Err(error) = self.stores.conversations.save_batch(&conversations).await {
                tracing::error!(%error, count = conversations.len(), "保存会话失败");
            } else {
                for conversation in &conversations {
                    let previous_unread = before_save
                        .get(&conversation.conversation_id)
                        .map(|c| c.unread_count)
                        .unwrap_or_default();
                    if let Ok(Some(updated)) = self
                        .stores
                        .conversations
                        .get(&conversation.conversation_id)
                        .await
                        && updated.unread_count != previous_unread
                    {
                        self.bus.publish(SdkEvent::Conversation(
                            ConversationEvent::UnreadCountChanged {
                                conversation_id: conversation.conversation_id.clone(),
                                unread_count: updated.unread_count,
                            },
                        ));
                    }
                    if let Err(error) = self
                        .stores
                        .messages
                        .reconcile_outgoing_read_by_peer_seq(
                            &conversation.conversation_id,
                            user_id,
                            conversation.peer_read_seq,
                        )
                        .await
                    {
                        tracing::warn!(
                            conversation_id = %conversation.conversation_id,
                            peer_read_seq = conversation.peer_read_seq,
                            error = %error,
                            "同步会话时修正对端已读位点失败"
                        );
                    }
                }
            }
        } else {
            tracing::debug!(
                has_more = response.has_more,
                server_cursor = %response.next_cursor,
                "会话增量同步无变更"
            );
        }

        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Synced {
                conversation_ids: conversation_ids.clone(),
            }));

        Ok(AppliedConversationIncremental {
            has_more: response.has_more,
            message_sync_conversation_ids,
            synced_conversation_ids: conversation_ids,
        })
    }

    async fn apply_conversation_identity_rewrite(
        &self,
        rewrite: &ConversationIdRewrite,
    ) -> Result<()> {
        if rewrite.from == rewrite.to {
            return Ok(());
        }
        let moved = self
            .stores
            .messages
            .rewrite_conversation_id(&rewrite.from, &rewrite.to)
            .await?;
        self.stores
            .conversations
            .merge_conversation_identity(&rewrite.from, &rewrite.to)
            .await?;
        tracing::info!(
            from = %rewrite.from,
            to = %rewrite.to,
            moved,
            "canonicalized conversation summary identity"
        );
        Ok(())
    }

    /// 增量页应用后的游标保存：**钳制到本地连续物化位点**（I1——游标不越过未落库消息）。
    /// 仅适用于"逐页增量"的真实会话消息游标；水位/检查点/快照建立型游标用
    /// [`Self::save_watermark_with_remote`]（钳制会把它们清零导致静默不保存）。
    pub async fn save_cursor_with_remote<F, Fut>(
        &self,
        user_id: &str,
        conversation_id: &str,
        last_seq: u64,
        proven_absent_seqs: &[u64],
        update_remote: F,
    ) -> Result<()>
    where
        F: FnOnce(String, String, u64) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        // 连续性证明的下界（floor）：只需证明 (floor, last_seq] 窗口——
        // ① 旧游标保存时其下连续性已被证明（归纳）；② 清空水位/可见边界之下的消息本地**有意缺失**，
        //   从 0 证明会让清空过历史的会话游标永久卡死。
        // 同时把每页保存的扫描成本从 O(全历史) 降到 O(本页窗口)。
        let prior_cursor_seq = self
            .stores
            .cursors
            .get_conversation_cursor(user_id, conversation_id)
            .await?
            .map(|cursor| cursor.last_seq)
            .unwrap_or(0);
        let cleared_floor = self
            .stores
            .conversations
            .get(conversation_id)
            .await?
            .map(|c| crate::domain::sync_visibility_floor(&c))
            .unwrap_or(0);
        let floor_seq = prior_cursor_seq.max(cleared_floor);
        if last_seq <= floor_seq {
            // 不倒退：目标位点已被旧游标/清空水位覆盖。
            return Ok(());
        }

        let safe_last_seq = self
            .local_materialized_contiguous_seq(
                conversation_id,
                last_seq,
                floor_seq,
                proven_absent_seqs,
            )
            .await?;
        if safe_last_seq < last_seq {
            tracing::warn!(
                user_id = %user_id,
                conversation_id = %conversation_id,
                requested_last_seq = last_seq,
                materialized_last_seq = safe_last_seq,
                floor_seq,
                "clamping conversation cursor to local materialized contiguous seq"
            );
        }
        if safe_last_seq <= floor_seq {
            return Ok(());
        }
        self.persist_cursor_and_push_remote(user_id, conversation_id, safe_last_seq, update_remote)
            .await?;
        // 游标从 floor 推进到 safe_last_seq：其间每个 seq 刚刚逐个证明过。
        self.record_coverage(
            conversation_id,
            cursor_advance_range(floor_seq, safe_last_seq),
        )
        .await;
        Ok(())
    }

    /// 水位/检查点型游标保存（**不做连续性钳制**）：
    /// - 伪 key（`__conversations__` 列表水位、`critical_event:*` 检查点）没有本地消息行，
    ///   连续性钳制恒为 0 → 会静默跳过保存；
    /// - 快照建立型游标（gap-too-large 切换、冷启 bundle）语义即"覆盖到水位、旧史 lazy backfill"，
    ///   本地只有最新一页，contiguous-from-0 必为 0。
    ///
    /// 语义正确性由调用方保证（快照/水位本身就是服务端权威）。
    pub async fn save_watermark_with_remote<F, Fut>(
        &self,
        user_id: &str,
        cursor_key: &str,
        last_seq: u64,
        update_remote: F,
    ) -> Result<()>
    where
        F: FnOnce(String, String, u64) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        if last_seq == 0 {
            return Ok(());
        }
        self.persist_cursor_and_push_remote(user_id, cursor_key, last_seq, update_remote)
            .await
    }

    /// 批量水位保存（本地半段）：跳过 seq==0，一次批量写（SQLite=单事务）。
    /// 远端推送由调用方按需处理（消息游标走去抖队列，关键位点无远端）。
    pub async fn save_watermarks(&self, user_id: &str, entries: &[(String, u64)]) -> Result<()> {
        let now = now_ms();
        let cursors: Vec<SyncCursorVo> = entries
            .iter()
            .filter(|(_, seq)| *seq > 0)
            .map(|(cursor_key, last_seq)| SyncCursorVo {
                user_id: user_id.to_string(),
                conversation_id: cursor_key.clone(),
                last_seq: *last_seq,
                synced_at: now,
            })
            .collect();
        if cursors.is_empty() {
            return Ok(());
        }
        self.stores
            .cursors
            .save_conversation_cursors(&cursors)
            .await
    }

    async fn persist_cursor_and_push_remote<F, Fut>(
        &self,
        user_id: &str,
        cursor_key: &str,
        last_seq: u64,
        update_remote: F,
    ) -> Result<()>
    where
        F: FnOnce(String, String, u64) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.stores
            .cursors
            .save_conversation_cursor(&SyncCursorVo {
                user_id: user_id.to_string(),
                conversation_id: cursor_key.to_string(),
                last_seq,
                synced_at: now_ms(),
            })
            .await?;
        if let Err(error) =
            update_remote(user_id.to_string(), cursor_key.to_string(), last_seq).await
        {
            tracing::warn!(
                user_id = %user_id,
                cursor_key = %cursor_key,
                error = %error,
                "update remote cursor failed"
            );
        }
        Ok(())
    }

    /// 本地物化连续位点：从 `target_seq` 向下扫描到 `floor_seq`（floor 之下的连续性由
    /// 上一次保存/清空语义保证，无需重证）。返回从 floor 起算的最大连续位点。
    /// `proven_absent_seqs`：服务端以 skip/tombstone 证明其上没有消息行的位点。
    /// 本地永远不会有对应消息，因此它们与真实消息一样能构成连续性 —— 不算进来，
    /// 游标会永远停在第一个这类 seq 前面。
    async fn local_materialized_contiguous_seq(
        &self,
        conversation_id: &str,
        target_seq: u64,
        floor_seq: u64,
        proven_absent_seqs: &[u64],
    ) -> Result<u64> {
        if target_seq == 0 {
            return Ok(0);
        }

        const PAGE_LIMIT: u32 = 512;
        let mut before_seq = target_seq.saturating_add(1);
        let mut seqs = Vec::new();
        loop {
            let page = self
                .stores
                .messages
                .get_by_conversation(conversation_id, before_seq, PAGE_LIMIT)
                .await?;
            if page.is_empty() {
                break;
            }
            let min_seq = page
                .iter()
                .map(|message| message.conversation_seq)
                .filter(|seq| *seq > 0)
                .min()
                .unwrap_or(0);
            seqs.extend(page.into_iter().map(|message| message.conversation_seq));
            if min_seq <= floor_seq.saturating_add(1) || seqs.len() < PAGE_LIMIT as usize {
                break;
            }
            before_seq = min_seq;
        }

        seqs.extend(
            proven_absent_seqs
                .iter()
                .copied()
                .filter(|seq| *seq > floor_seq && *seq <= target_seq),
        );

        Ok(max_contiguous_seq(floor_seq, &seqs).min(target_seq))
    }

    /// 写入一段已证实区间。失败只告警：宁可漏标，漏标的代价是多一次回填。
    pub(crate) async fn record_coverage(&self, conversation_id: &str, range: Option<SeqRange>) {
        coverage::record_range(
            self.stores.timeline_coverage.as_ref(),
            conversation_id,
            range,
        )
        .await;
    }

    /// 回填页（`before = before_seq`）落库之后写入它证明的区间。
    pub(crate) async fn record_backfill_coverage(
        &self,
        conversation_id: &str,
        before_seq: u64,
        page: &SingleConversationSyncRes,
    ) {
        coverage::record_backfill_page(
            self.stores.timeline_coverage.as_ref(),
            conversation_id,
            before_seq,
            page,
        )
        .await;
    }
}

/// 页内已落库 item 的最小、最大 seq；没有时为 (0, 0)。
fn seq_bounds(seqs: &[u64]) -> (u64, u64) {
    let positive = || seqs.iter().copied().filter(|seq| *seq > 0);
    (positive().min().unwrap_or(0), positive().max().unwrap_or(0))
}

fn max_contiguous_seq(known_seq: u64, seqs: &[u64]) -> u64 {
    let mut sorted = seqs
        .iter()
        .copied()
        .filter(|seq| *seq > known_seq)
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    sorted.dedup();

    let mut cursor = known_seq;
    for seq in sorted {
        if seq == cursor + 1 {
            cursor = seq;
        } else if seq > cursor + 1 {
            break;
        }
    }
    cursor
}

pub(crate) fn local_message_sync_start_seq(
    cursor_last_seq: u64,
    local_max_seq: u64,
    cleared_floor: u64,
) -> u64 {
    let materialized_seq = if cursor_last_seq == 0 || local_max_seq == 0 {
        0
    } else {
        cursor_last_seq.min(local_max_seq)
    };
    materialized_seq.max(cleared_floor)
}

fn remote_conversation_target_seq(conversation: &crate::model::Conversation) -> u64 {
    let unread_tail_seq = conversation
        .last_read_seq
        .saturating_add(conversation.unread_count as u64);
    conversation
        .max_seq
        .max(unread_tail_seq)
        .max(conversation.visible_after_seq)
}

fn should_sync_messages_for_unread_delta(previous_unread: u32, next_unread: u32) -> bool {
    next_unread > previous_unread
}

/// Social SyncSignal 内部路由会话；不得进入本地会话列表。
fn is_hidden_internal_conversation(conversation_id: &str) -> bool {
    let conversation_id = conversation_id.trim();
    conversation_id.is_empty() || conversation_id.starts_with("sync:")
}

fn now_ms() -> u64 {
    crate::shared::util::now_millis()
}

#[cfg(test)]
mod tests {
    use super::{
        is_hidden_internal_conversation, local_message_sync_start_seq,
        remote_conversation_target_seq, should_sync_messages_for_unread_delta,
    };
    use crate::model::Conversation;

    #[test]
    fn local_sync_start_ignores_polluted_cursor_ahead_of_local_messages() {
        assert_eq!(local_message_sync_start_seq(100, 0, 0), 0);
        assert_eq!(local_message_sync_start_seq(100, 12, 0), 12);
    }

    #[test]
    fn local_sync_start_uses_persisted_cursor_when_materialized() {
        assert_eq!(local_message_sync_start_seq(80, 100, 0), 80);
    }

    #[test]
    fn local_sync_start_respects_local_clear_floor() {
        assert_eq!(local_message_sync_start_seq(0, 0, 50), 50);
        assert_eq!(local_message_sync_start_seq(80, 100, 90), 90);
    }

    #[test]
    fn conversation_sync_target_infers_tail_from_unread_state() {
        let stale_summary = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 1,
            last_read_seq: 0,
            unread_count: 2,
            ..Default::default()
        };
        assert_eq!(remote_conversation_target_seq(&stale_summary), 2);

        let read_based_tail = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 1,
            last_read_seq: 10,
            unread_count: 2,
            ..Default::default()
        };
        assert_eq!(remote_conversation_target_seq(&read_based_tail), 12);

        let normal_summary = Conversation {
            conversation_id: "conv-1".to_string(),
            max_seq: 20,
            last_read_seq: 10,
            unread_count: 0,
            ..Default::default()
        };
        assert_eq!(remote_conversation_target_seq(&normal_summary), 20);
    }

    #[test]
    fn unread_increase_forces_message_sync() {
        assert!(should_sync_messages_for_unread_delta(0, 1));
        assert!(should_sync_messages_for_unread_delta(1, 2));
        assert!(!should_sync_messages_for_unread_delta(2, 2));
        assert!(!should_sync_messages_for_unread_delta(2, 1));
    }

    #[test]
    fn hidden_conversation_filter_rejects_blank_and_internal_ids() {
        assert!(is_hidden_internal_conversation(""));
        assert!(is_hidden_internal_conversation("   "));
        assert!(is_hidden_internal_conversation("sync:conversation"));
        assert!(!is_hidden_internal_conversation("conv-1"));
    }
}
