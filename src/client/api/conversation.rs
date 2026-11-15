//! 会话 Facade — 直接编排会话读写 usecase，并在关键写操作后发布会话域事件。
//!
//! **边界**：会话列表、置顶、免打扰、归档、已读/未读、草稿均属 IM core 域；
//! Social SDK 仅负责好友/群 REST 与 IM 资料投影，不得在此重复实现会话逻辑。

use std::sync::Arc;

use crate::application::usecases::{
    ConversationCommandUseCase, ConversationViewAssembler, MessageViewAssembler,
};
use crate::domain::{TimelineCoverage, TimelineCoverageStore};
use crate::kernel::CurrentUserIdStore;
use crate::kernel::event::{ConversationEvent, EventBus, SdkEvent};
use crate::model::conversation::ConversationType;
use crate::model::{
    BootstrapHomeTimelineRequest, Conversation, ConversationListQuery,
    ConversationTimelineSnapshot, HomeTimelineSnapshot, IMMessage, OpenConversationTimelineRequest,
    TimelineSyncState, normalized_conversation_limit, normalized_message_limit,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// 会话命令与查询入口（直接委托 application usecases；部分写操作经 `EventBus` 推事件）。
#[derive(Clone)]
pub struct ConversationApi {
    command_use_case: Arc<ConversationCommandUseCase>,
    view_assembler: Arc<ConversationViewAssembler>,
    message_view_assembler: Arc<MessageViewAssembler>,
    timeline_coverage: Arc<dyn TimelineCoverageStore>,
    bus: EventBus,
    current_user_id: CurrentUserIdStore,
}

impl ConversationApi {
    pub fn new(
        command_use_case: Arc<ConversationCommandUseCase>,
        view_assembler: Arc<ConversationViewAssembler>,
        message_view_assembler: Arc<MessageViewAssembler>,
        timeline_coverage: Arc<dyn TimelineCoverageStore>,
        bus: EventBus,
        current_user_id: CurrentUserIdStore,
    ) -> Self {
        Self {
            command_use_case,
            view_assembler,
            message_view_assembler,
            timeline_coverage,
            bus,
            current_user_id,
        }
    }

    async fn ensure_session_active(&self) -> Result<()> {
        if self.current_user_id.read().await.trim().is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        Ok(())
    }

    pub async fn current_user_id(&self) -> Result<String> {
        self.command_use_case.current_user_id().await
    }

    /// 获取或创建单个会话：有则返回，无则创建并落库后返回，保证 list() 能立即看到新会话。
    /// 新建时 `channel_id` 恒为入参 `source_id`（与类型无关）。
    pub async fn get_one(
        &self,
        source_id: &str,
        conversation_type: &ConversationType,
    ) -> Result<Conversation> {
        self.ensure_session_active().await?;
        let conversation = self
            .command_use_case
            .get_one(source_id, conversation_type, true)
            .await?;
        Ok(self.view_assembler.hydrate_conversation(conversation).await)
    }

    /// 通过用户 ID 列表获取或创建群聊。本机会话会保存成员 ID，发送群消息时用于服务端补齐参与者。
    pub async fn get_group_by_user_ids(
        &self,
        user_ids: &[String],
        display_name: Option<&str>,
    ) -> Result<Conversation> {
        self.ensure_session_active().await?;
        let conversation = self
            .command_use_case
            .get_group_by_user_ids(user_ids, display_name)
            .await?;
        Ok(self.view_assembler.hydrate_conversation(conversation).await)
    }

    pub async fn list(&self) -> Result<Vec<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler.list(false).await
    }

    /// 飞书式会话筛选：未读、@我、单聊/群聊、置顶、免打扰、归档、草稿、标记消息所在会话等。
    pub async fn list_by_query(&self, query: ConversationListQuery) -> Result<Vec<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler.list_by_query(&query).await
    }

    /// 含已归档会话的完整列表（飞书「已完成」视图等）
    pub async fn list_including_archived(&self) -> Result<Vec<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler.list(true).await
    }

    pub async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler.get(conversation_id).await
    }

    /// 批量获取会话（按 id 列表，不存在的跳过）
    pub async fn get_multiple(&self, conversation_ids: &[String]) -> Result<Vec<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler.get_multiple(conversation_ids).await
    }

    /// 分页会话列表（cursor 为会话 id 表示从此之后开始，limit 为条数）
    pub async fn list_paginated(
        &self,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler
            .list_paginated(cursor, limit, false)
            .await
    }

    pub async fn list_raw(&self) -> Result<Vec<Conversation>> {
        self.ensure_session_active().await?;
        self.view_assembler.list_raw().await
    }

    /// 未读总数（Tab 角标）：全部会话按 [`crate::domain::total_unread`] 的口径求和——已归档不计，
    /// 免打扰只计「@我」。宿主的角标只读这个数，不再自己对会话列表求和。
    pub async fn total_unread(&self) -> Result<u64> {
        self.ensure_session_active().await?;
        Ok(crate::domain::total_unread(
            &self.view_assembler.list_raw().await?,
        ))
    }

    /// SDK 统一首页快照：会话查询会在仓储层修复本地消息投影，业务端无需自行回写 lastMessage。
    ///
    /// `total_unread` 按全部会话计（[`crate::domain::total_unread`]），先求和再截断首屏，不随首屏条数变化。
    pub async fn bootstrap_home(
        &self,
        request: BootstrapHomeTimelineRequest,
    ) -> Result<HomeTimelineSnapshot> {
        self.ensure_session_active().await?;
        let mut conversations = self.view_assembler.list(false).await?;
        let total_unread = crate::domain::total_unread(&conversations);
        conversations.truncate(normalized_conversation_limit(request.conversation_limit) as usize);
        Ok(HomeTimelineSnapshot {
            conversations,
            total_unread,
            sync_state: TimelineSyncState::LocalReady,
        })
    }

    /// SDK 统一会话快照：读取最近消息窗口，并依赖 conversation.get 的投影修复返回一致会话摘要。
    ///
    /// 窗口由新到旧取：比最高已证实 seq 新的行（刚由实时推送写入的头部未证实行）与待发送行照收，
    /// 其余的行必须落在接着最新的那一段已证实区间里，碰到区间外的行就停，不把隔着缺口的旧行
    /// 接到窗口下面。`has_more`（视图的 hasOlder）只由区间与窗口之下的本地行决定，不再按
    /// 「本地页装满没有」或「最旧一条是不是 seq 1」去猜。
    pub async fn open_timeline(
        &self,
        request: OpenConversationTimelineRequest,
    ) -> Result<ConversationTimelineSnapshot> {
        self.ensure_session_active().await?;
        let conversation_id = request.conversation_id.trim();
        let limit = normalized_message_limit(request.message_limit) as usize;
        if conversation_id.is_empty() {
            return Ok(ConversationTimelineSnapshot {
                conversation: None,
                messages: Vec::new(),
                has_more: false,
            });
        }
        // 多读一行：判断窗口之下本地还有没有行。
        let rows = self
            .message_view_assembler
            .list(conversation_id, 0, limit as u32 + 1)
            .await?;
        let conversation = self.view_assembler.get(conversation_id).await?;
        let coverage = self
            .coverage_with_floor(conversation_id, conversation.as_ref())
            .await?;
        let highest = coverage.highest_proven_seq();
        let latest = coverage.latest_range();
        let mut messages = Vec::with_capacity(limit.min(rows.len()));
        let mut rows_below = false;
        for row in rows {
            if messages.len() == limit {
                rows_below = true;
                break;
            }
            let seq = row.conversation_seq;
            let joins_latest =
                seq == 0 || seq > highest || latest.is_some_and(|range| range.contains(seq));
            if !joins_latest {
                rows_below = true;
                break;
            }
            messages.push(row);
        }
        messages.sort_by(IMMessage::compare_for_timeline_asc);
        let has_more = window_has_older(&messages, rows_below, &coverage, conversation.as_ref());
        Ok(ConversationTimelineSnapshot {
            conversation,
            messages,
            has_more,
        })
    }

    /// 已证实区间，并入会话的可见下限：本地清空水位与服务端历史可见起点之下没有可见消息。
    async fn coverage_with_floor(
        &self,
        conversation_id: &str,
        conversation: Option<&Conversation>,
    ) -> Result<TimelineCoverage> {
        let floor = conversation
            .map(crate::domain::sync_visibility_floor)
            .unwrap_or(0);
        Ok(self
            .timeline_coverage
            .coverage(conversation_id)
            .await?
            .with_visibility_floor(floor))
    }

    /// 往旧翻页用：会话摘要（本地投影）与并入可见下限后的已证实区间。
    pub(crate) async fn timeline_paging_state(
        &self,
        conversation_id: &str,
    ) -> Result<(Option<Conversation>, TimelineCoverage)> {
        self.ensure_session_active().await?;
        let conversation = self.view_assembler.get(conversation_id).await?;
        let coverage = self
            .coverage_with_floor(conversation_id, conversation.as_ref())
            .await?;
        Ok((conversation, coverage))
    }

    pub(crate) async fn hydrate_timeline_messages(
        &self,
        messages: &mut [crate::model::IMMessage],
    ) -> Result<()> {
        self.message_view_assembler
            .hydrate_messages_for_view(messages)
            .await
    }

    /// 区间内读本地：seq 落在 `[from_seq, before_seq)` 的行，由新到旧最多 `limit` 行。
    /// 第二项说明这一段里本地是否还有更旧的行。区间之外的行一律不读——不越过缺口。
    pub(crate) async fn timeline_rows_in_range(
        &self,
        conversation_id: &str,
        from_seq: u64,
        before_seq: u64,
        limit: usize,
    ) -> Result<(Vec<IMMessage>, bool)> {
        self.ensure_session_active().await?;
        let conversation_id = conversation_id.trim();
        let from_seq = from_seq.max(1);
        if conversation_id.is_empty() || before_seq <= from_seq || limit == 0 {
            return Ok((Vec::new(), false));
        }
        let rows = self
            .message_view_assembler
            .list(conversation_id, before_seq, limit as u32 + 1)
            .await?;
        let mut in_range = rows
            .into_iter()
            .filter(|message| (from_seq..before_seq).contains(&message.conversation_seq))
            .collect::<Vec<_>>();
        let more = in_range.len() > limit;
        in_range.truncate(limit);
        Ok((in_range, more))
    }

    /// 在窗口的 seq 范围内重读（事件触发的刷新）：seq 不小于 `oldest_seq` 的行与待发送行，
    /// 由新到旧最多 `limit` 行。窗口之下的行不读进来，所以重读不会改变 `has_more`，
    /// 除非窗口从旧的一端被截掉。
    pub(crate) async fn reload_timeline_window(
        &self,
        conversation_id: &str,
        oldest_seq: u64,
        limit: usize,
    ) -> Result<ConversationTimelineSnapshot> {
        self.ensure_session_active().await?;
        let conversation_id = conversation_id.trim();
        let limit = limit.max(1);
        let rows = self
            .message_view_assembler
            .list(conversation_id, 0, limit.saturating_add(1) as u32)
            .await?;
        let conversation = self.view_assembler.get(conversation_id).await?;
        let coverage = self
            .coverage_with_floor(conversation_id, conversation.as_ref())
            .await?;
        let mut messages = Vec::with_capacity(limit.min(rows.len()));
        let mut rows_below = false;
        for row in rows {
            let seq = row.conversation_seq;
            if (seq != 0 && seq < oldest_seq) || messages.len() == limit {
                rows_below = true;
                break;
            }
            messages.push(row);
        }
        messages.sort_by(IMMessage::compare_for_timeline_asc);
        let has_more = window_has_older(&messages, rows_below, &coverage, conversation.as_ref());
        Ok(ConversationTimelineSnapshot {
            conversation,
            messages,
            has_more,
        })
    }

    /// 往新的方向在区间内读本地：seq 落在 `(after_seq, to_seq]` 的行，由旧到新最多 `limit` 行。
    /// 第二项说明这一段里本地是否还有更新的行。区间之外的行一律不读——不越过缺口。
    pub(crate) async fn timeline_rows_after(
        &self,
        conversation_id: &str,
        after_seq: u64,
        to_seq: u64,
        limit: usize,
    ) -> Result<(Vec<IMMessage>, bool)> {
        self.ensure_session_active().await?;
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() || to_seq <= after_seq || limit == 0 {
            return Ok((Vec::new(), false));
        }
        let rows = self
            .message_view_assembler
            .list_after(
                conversation_id,
                after_seq,
                u32::try_from(limit.saturating_add(1)).unwrap_or(u32::MAX),
            )
            .await?;
        let mut in_range = rows
            .into_iter()
            .filter(|message| {
                message.conversation_seq > after_seq && message.conversation_seq <= to_seq
            })
            .collect::<Vec<_>>();
        let more = in_range.len() > limit;
        in_range.truncate(limit);
        Ok((in_range, more))
    }

    /// 按 id 批量重读视图里的几行：有客户端 id 的按客户端 id 读（行键就是它，发送确认前后不变），
    /// 其余按服务端 id 读。投影与时间线读法相同；找不到的行不返回。
    pub(crate) async fn timeline_rows_by_id(
        &self,
        client_msg_ids: &[String],
        server_ids: &[String],
    ) -> Result<Vec<IMMessage>> {
        self.ensure_session_active().await?;
        self.message_view_assembler
            .get_many(client_msg_ids, server_ids)
            .await
    }

    /// 定位用：按服务端 id 或客户端 id 找本地的一条消息。
    pub(crate) async fn find_message(&self, message_id: &str) -> Result<Option<IMMessage>> {
        self.ensure_session_active().await?;
        let message_id = message_id.trim();
        if message_id.is_empty() {
            return Ok(None);
        }
        self.message_view_assembler.get_raw(message_id).await
    }

    pub async fn mark_read(&self, conversation_id: &str, read_seq: u64) -> Result<()> {
        self.ensure_session_active().await?;
        let unread_count = self
            .command_use_case
            .mark_read(conversation_id, read_seq)
            .await?;
        self.bus.publish(SdkEvent::Conversation(
            ConversationEvent::UnreadCountChanged {
                conversation_id: conversation_id.to_string(),
                unread_count,
            },
        ));
        Ok(())
    }

    pub async fn delete(&self, conversation_id: &str) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case.delete(conversation_id).await?;
        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Deleted {
                conversation_id: conversation_id.to_string(),
            }));
        Ok(())
    }

    pub async fn set_pinned(&self, conversation_id: &str, pinned: bool) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case
            .set_pinned(conversation_id, pinned)
            .await?;
        self.publish_updated(conversation_id);
        Ok(())
    }

    pub async fn set_muted(&self, conversation_id: &str, muted: bool) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case
            .set_muted(conversation_id, muted)
            .await?;
        self.publish_updated(conversation_id);
        Ok(())
    }

    /// In a large group, turns notifications for every message on or off for the current user.
    /// The choice shows at once; when the server does not take it, it goes back and the error is
    /// returned. Either way the conversation is announced as updated.
    pub async fn set_notify_all(&self, conversation_id: &str, enabled: bool) -> Result<()> {
        self.ensure_session_active().await?;
        let result = self
            .command_use_case
            .set_notify_all(conversation_id, enabled)
            .await;
        self.publish_updated(conversation_id);
        result
    }

    pub async fn set_archived(&self, conversation_id: &str, archived: bool) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case
            .set_archived(conversation_id, archived)
            .await?;
        self.publish_updated(conversation_id);
        Ok(())
    }

    pub async fn mark_unread(&self, conversation_id: &str) -> Result<u32> {
        self.ensure_session_active().await?;
        let unread_count = self.command_use_case.mark_unread(conversation_id).await?;
        self.bus.publish(SdkEvent::Conversation(
            ConversationEvent::UnreadCountChanged {
                conversation_id: conversation_id.to_string(),
                unread_count,
            },
        ));
        Ok(unread_count)
    }

    /// 设置会话草稿（本地）
    pub async fn update_draft(&self, conversation_id: &str, draft: Option<&str>) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case
            .update_draft(conversation_id, draft)
            .await?;
        self.publish_updated(conversation_id);
        Ok(())
    }

    /// 清空本地聊天记录并更新会话摘要。
    pub async fn clear_local_chat_history(&self, conversation_id: &str) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case
            .clear_local_chat_history(conversation_id)
            .await?;
        self.publish_updated(conversation_id);
        Ok(())
    }

    /// Clear conversation history for the current account across devices.
    pub async fn clear_chat_history_for_self(&self, conversation_id: &str) -> Result<()> {
        self.ensure_session_active().await?;
        self.command_use_case
            .clear_chat_history_for_self(conversation_id)
            .await?;
        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Synced {
                conversation_ids: vec![conversation_id.to_string()],
            }));
        Ok(())
    }

    fn publish_updated(&self, conversation_id: &str) {
        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Updated {
                conversation_id: conversation_id.to_string(),
            }));
    }
}

/// 窗口之下还有没有可见消息（视图的 hasOlder）：只由已证实区间与窗口之下的本地行决定。
/// 空窗口按会话已知的最新 seq（摘要 `max_seq` 与最高已证实 seq 取大）之上一位判断，
/// 所以会话 `max_seq` 不超过可见下限时为 false。
pub(crate) fn window_has_older(
    messages: &[IMMessage],
    rows_below: bool,
    coverage: &TimelineCoverage,
    conversation: Option<&Conversation>,
) -> bool {
    match messages
        .iter()
        .map(|message| message.conversation_seq)
        .filter(|seq| *seq > 0)
        .min()
    {
        Some(oldest) => coverage.has_older(oldest, rows_below),
        None => {
            let newest_known = conversation
                .map(|conversation| conversation.max_seq)
                .unwrap_or(0)
                .max(coverage.highest_proven_seq());
            coverage.has_older(newest_known.saturating_add(1), rows_below)
        }
    }
}

#[cfg(test)]
mod open_timeline_tests {
    use super::*;
    use crate::domain::{ConversationWriter, MessageWriter, SeqRange};
    use crate::infrastructure::persistence::{MemoryTimelineCoverageStore, MemoryUserProfileStore};
    use crate::storage::{MemoryConversationStore, MemoryMessageStore};
    use flare_proto::common::Message as ProtoMessage;

    struct Fixture {
        api: ConversationApi,
        messages: Arc<MemoryMessageStore>,
        conversations: Arc<MemoryConversationStore>,
        coverage: Arc<MemoryTimelineCoverageStore>,
    }

    fn fixture() -> Fixture {
        let messages = Arc::new(MemoryMessageStore::new());
        let conversations = Arc::new(MemoryConversationStore::new());
        let coverage = Arc::new(MemoryTimelineCoverageStore::new());
        let user: CurrentUserIdStore = Arc::new(tokio::sync::RwLock::new("me".to_string()));
        let profiles = Arc::new(MemoryUserProfileStore::new());
        let api = ConversationApi::new(
            Arc::new(ConversationCommandUseCase::new(
                conversations.clone(),
                user.clone(),
                None,
                None,
            )),
            Arc::new(ConversationViewAssembler::new(
                conversations.clone(),
                profiles.clone(),
            )),
            Arc::new(MessageViewAssembler::new(messages.clone(), profiles)),
            coverage.clone(),
            EventBus::new(),
            user,
        );
        Fixture {
            api,
            messages,
            conversations,
            coverage,
        }
    }

    impl Fixture {
        async fn conversation(&self, max_seq: u64, visible_after_seq: u64) {
            self.conversations
                .save_one(&Conversation {
                    conversation_id: "c1".into(),
                    channel_id: "c1".into(),
                    conversation_type: ConversationType::Group,
                    max_seq,
                    visible_after_seq,
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        async fn rows(&self, seqs: impl IntoIterator<Item = u64>) {
            let rows = seqs
                .into_iter()
                .map(|seq| {
                    IMMessage::new(ProtoMessage {
                        conversation_id: "c1".into(),
                        server_id: format!("s{seq}"),
                        client_msg_id: format!("m{seq}"),
                        conversation_seq: seq,
                        ..Default::default()
                    })
                })
                .collect::<Vec<_>>();
            self.messages.save_batch(&rows).await.unwrap();
        }

        async fn covered(&self, from: u64, to: u64) {
            self.coverage
                .add("c1", SeqRange::new(from, to).unwrap())
                .await
                .unwrap();
        }

        async fn open(&self, limit: u32) -> (Vec<u64>, bool) {
            let snapshot = self
                .api
                .open_timeline(OpenConversationTimelineRequest {
                    conversation_id: "c1".into(),
                    message_limit: limit,
                })
                .await
                .unwrap();
            (
                snapshot
                    .messages
                    .iter()
                    .map(|message| message.conversation_seq)
                    .collect(),
                snapshot.has_more,
            )
        }
    }

    /// 新设备本地只同步了最新一小段：没有区间证明到起点，绝不能报「没有更多」。
    #[tokio::test]
    async fn a_partial_local_window_without_proof_still_has_older() {
        let f = fixture();
        f.conversation(1_000_000, 0).await;
        f.rows(999_965..=1_000_000).await;
        let (window, has_older) = f.open(40).await;
        assert_eq!(window.len(), 36);
        assert!(has_older);
    }

    /// 会话第一条消息是 seq 3（1、2 是已读回执）：区间证明到 1，就没有更早的了。
    #[tokio::test]
    async fn the_start_is_proven_by_the_ranges_not_by_seq_one() {
        let f = fixture();
        f.conversation(12, 0).await;
        f.rows(3..=12).await;
        f.covered(1, 12).await;
        assert_eq!(f.open(40).await, ((3..=12).collect(), false));
        assert_eq!(
            f.open(4).await,
            ((9..=12).collect(), true),
            "older local rows below the window are older messages"
        );
    }

    #[tokio::test]
    async fn an_empty_window_has_older_only_above_the_visibility_floor() {
        let f = fixture();
        assert_eq!(f.open(40).await, (vec![], false), "unknown conversation");
        f.conversation(30, 30).await;
        assert_eq!(f.open(40).await, (vec![], false), "cleared through max_seq");
        f.conversation(31, 30).await;
        assert_eq!(
            f.open(40).await,
            (vec![], true),
            "one message above the floor"
        );
    }

    /// 冷启动只拿到尾页 [980, 1000]，本地还有上次留下的 [1, 36]：窗口只含 980 之后。
    #[tokio::test]
    async fn the_window_stops_at_the_gap_below_the_latest_range() {
        let f = fixture();
        f.conversation(1000, 0).await;
        f.rows((1..=36).chain(980..=1000)).await;
        f.covered(1, 36).await;
        f.covered(980, 1000).await;
        assert_eq!(f.open(40).await, ((980..=1000).collect(), true));
    }

    /// 实时消息 1001 先于补拉到达：它比最高已证实 seq 新，照常进窗口，下面接着已证实的一段。
    #[tokio::test]
    async fn rows_above_the_highest_proven_seq_are_kept() {
        let f = fixture();
        f.conversation(1001, 0).await;
        f.rows((961..=990).chain([1001])).await;
        f.covered(961, 990).await;
        let (window, has_older) = f.open(40).await;
        assert_eq!(window, (961..=990).chain([1001]).collect::<Vec<_>>());
        assert!(has_older, "the range does not reach the start");
    }

    #[tokio::test]
    async fn the_cleared_floor_proves_the_start() {
        let f = fixture();
        f.conversation(14, 10).await;
        f.rows(11..=14).await;
        f.covered(11, 14).await;
        assert_eq!(f.open(40).await, ((11..=14).collect(), false));
    }
}

#[cfg(test)]
mod total_unread_tests {
    use super::*;
    use crate::domain::ConversationWriter;
    use crate::infrastructure::persistence::MemoryUserProfileStore;
    use crate::storage::{MemoryConversationStore, MemoryMessageStore};

    fn conversation(id: &str, unread: u32, last_message_at: u64) -> Conversation {
        Conversation {
            conversation_id: id.to_string(),
            channel_id: id.to_string(),
            conversation_type: ConversationType::Single,
            unread_count: unread,
            max_seq: u64::from(unread),
            last_message_at: Some(last_message_at),
            updated_at: last_message_at,
            ..Default::default()
        }
    }

    async fn api_with(
        conversations: &[Conversation],
    ) -> (ConversationApi, Arc<MemoryConversationStore>) {
        let store = Arc::new(MemoryConversationStore::new());
        store.save_batch(conversations).await.unwrap();
        let user: CurrentUserIdStore = Arc::new(tokio::sync::RwLock::new("me".to_string()));
        let profiles = Arc::new(MemoryUserProfileStore::new());
        let api = ConversationApi::new(
            Arc::new(ConversationCommandUseCase::new(
                store.clone(),
                user.clone(),
                None,
                None,
            )),
            Arc::new(ConversationViewAssembler::new(
                store.clone(),
                profiles.clone(),
            )),
            Arc::new(MessageViewAssembler::new(
                Arc::new(MemoryMessageStore::new()),
                profiles,
            )),
            Arc::new(crate::infrastructure::persistence::MemoryTimelineCoverageStore::new()),
            EventBus::new(),
            user,
        );
        (api, store)
    }

    /// The home snapshot shows the first conversations only; its total still
    /// counts all of them, and is the same number `total_unread` gives.
    #[tokio::test]
    async fn the_total_does_not_depend_on_how_many_conversations_the_home_shows() {
        let conversations: Vec<_> = (0..5)
            .map(|i| conversation(&format!("c{i}"), 2, 1_000 + i))
            .collect();
        let (api, _) = api_with(&conversations).await;
        for limit in [1, 3, 100] {
            let home = api
                .bootstrap_home(BootstrapHomeTimelineRequest {
                    conversation_limit: limit,
                })
                .await
                .unwrap();
            assert_eq!(home.conversations.len(), (limit as usize).min(5));
            assert_eq!(home.total_unread, 10, "home showing {limit} conversations");
        }
        assert_eq!(api.total_unread().await.unwrap(), 10);
    }

    /// Muting a conversation takes its unread out of the total (it keeps its
    /// own count in the list); its mentions of me still count.
    #[tokio::test]
    async fn muted_conversations_count_only_their_mentions() {
        let mut mentioned = conversation("mentioned", 6, 2_000);
        mentioned.is_muted = true;
        mentioned.mention_count = 1;
        mentioned.mention_me = true;
        let (api, _) = api_with(&[conversation("plain", 3, 3_000), mentioned]).await;
        assert_eq!(api.total_unread().await.unwrap(), 3 + 1);

        api.set_muted("plain", true).await.unwrap();
        assert_eq!(api.total_unread().await.unwrap(), 1);
        let home = api
            .bootstrap_home(BootstrapHomeTimelineRequest::default())
            .await
            .unwrap();
        assert_eq!(home.total_unread, 1);
        assert_eq!(
            home.conversations
                .iter()
                .map(|c| c.unread_count)
                .sum::<u32>(),
            9,
            "rows keep their own unread counts"
        );

        api.set_muted("plain", false).await.unwrap();
        assert_eq!(api.total_unread().await.unwrap(), 4);
    }

    /// The total follows conversations coming and going and being read.
    #[tokio::test]
    async fn the_total_follows_new_deleted_and_read_conversations() {
        let (api, store) = api_with(&[conversation("a", 2, 1_000)]).await;
        assert_eq!(api.total_unread().await.unwrap(), 2);

        store
            .save_batch(&[conversation("b", 5, 2_000)])
            .await
            .unwrap();
        assert_eq!(api.total_unread().await.unwrap(), 7);

        api.mark_read("b", 5).await.unwrap();
        assert_eq!(api.total_unread().await.unwrap(), 2);

        api.delete("a").await.unwrap();
        assert_eq!(api.total_unread().await.unwrap(), 0);

        api.set_archived("b", true).await.unwrap();
        api.mark_unread("b").await.unwrap();
        assert_eq!(
            api.total_unread().await.unwrap(),
            0,
            "archived conversations stay out of the total"
        );
    }
}
