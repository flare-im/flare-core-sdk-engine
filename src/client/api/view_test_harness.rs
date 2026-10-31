//! 时间线视图测试的公共装置：脚本化的服务端、数存储读取次数的消息仓储、视图测试台，
//! 以及一个按 `view.updated` 应用规则维护宿主侧窗口的参考应用器。
//!
//! 服务端按真实语义回页：回填取 `before` 之前最新的一页可见消息、多取一条判断 `has_more`，
//! 落库后按同步适配器的规则写区间；向新取页取 `after` 之后的一页，写 `[after + 1, M]`，
//! 不碰同步游标。测试台每做完一步，都把这一步推出的全部更新应用到宿主侧窗口上，并断言它与
//! 视图持有的窗口逐行相同、版本号连续——核心产出的每个增量都要能照做。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use flare_proto::common::{
    Message as ProtoMessage, MessageDeleteEvent, MessageRecallEvent, SendAck,
    SingleConversationSyncRes, SyncSliceItem, sync_slice_item::Payload as ItemPayload,
};
use tokio::sync::RwLock;

use super::{ViewApi, view_refresh_plan};
use crate::application::notification::{NotificationHandlerRegistry, NotificationInboundPipeline};
use crate::application::services::{EventDeduper, MessageDeduper};
use crate::application::usecases::{
    ConversationCommandUseCase, ConversationViewAssembler, MessageViewAssembler, SyncApplyUseCase,
};
use crate::client::api::ConversationApi;
use crate::domain::{MessageReader, MessageStore, MessageWriter, SeqRange};
use crate::infrastructure::persistence::{
    MemoryUserProfileStore, StoreProvider, in_memory_im_provider,
};
use crate::kernel::event::{
    EventBus, EventReceiver, MessageEvent, SdkEvent, SyncNotify, SyncPhase,
};
use crate::kernel::{
    AttentionRegistry, CurrentUserIdStore, SessionSyncRunner, SyncReason, SyncRunContext,
    SyncScope, SyncTrigger, SyncVisibility,
};
use crate::model::conversation::ConversationType;
use crate::model::message::ReactionEntry;
use crate::model::{
    Conversation, ConversationParticipant, IMMessage, LoadNewerTimelineViewRequest,
    LoadOlderTimelineViewRequest, MessageSearchQuery, OpenTimelineViewRequest, TimelineAnchor,
    TimelineViewPageResponse, TimelineViewRequest, TimelineViewSnapshot, ViewDelta, ViewSnapshot,
    ViewUpdate, ViewUpdateKind,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

pub(super) const CONVERSATION: &str = "c1";

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

pub(super) fn proto(seq: u64) -> ProtoMessage {
    ProtoMessage {
        conversation_id: CONVERSATION.into(),
        server_id: format!("s{seq}"),
        client_msg_id: format!("m{seq}"),
        sender_id: "u2".into(),
        conversation_seq: seq,
        ..Default::default()
    }
}

pub(super) fn row(seq: u64) -> IMMessage {
    IMMessage::new(proto(seq))
}

pub(super) fn expand(ranges: &[[u64; 2]]) -> Vec<u64> {
    ranges.iter().flat_map(|[from, to]| *from..=*to).collect()
}

pub(super) fn seqs(from: u64, to: u64) -> Vec<u64> {
    (from..=to).collect()
}

/// 服务端收到的一次请求。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Req {
    /// 回填：`before` 之前的一页。
    Before(u64, usize),
    /// 向新取页：`after` 之后的一页，不推进游标。
    After(u64, usize),
}

/// 模拟服务端：持有可见消息的 seq 段，按真实回填语义回页。
pub(super) struct ScriptedServer {
    visible: Vec<(u64, u64)>,
    max_seq: u64,
    pub offline: AtomicBool,
    /// 回页时说还有更早、却一条也不给（坏掉的服务端）。
    pub empty_pages_with_more: AtomicBool,
    requests: StdMutex<Vec<Req>>,
    /// 推进游标的同步请求次数（视图不该发）。
    pub cursor_syncs: AtomicUsize,
    stores: StoreProvider,
    sync_apply: SyncApplyUseCase,
}

impl ScriptedServer {
    /// `before` 之前最新的 `limit` 条可见消息（由新到旧）与是否还有更早的。
    fn page_before(&self, before: u64, limit: usize) -> (Vec<u64>, bool) {
        let mut ranges = self.visible.clone();
        ranges.sort_by_key(|range| std::cmp::Reverse(range.1));
        let mut seqs = Vec::with_capacity(limit + 1);
        'ranges: for (from, to) in ranges {
            let top = to.min(before.saturating_sub(1));
            if top < from {
                continue;
            }
            for seq in (from..=top).rev() {
                seqs.push(seq);
                if seqs.len() > limit {
                    break 'ranges;
                }
            }
        }
        let has_more = seqs.len() > limit;
        seqs.truncate(limit);
        (seqs, has_more)
    }

    /// `after` 之后最旧的 `limit` 条可见消息（由旧到新）与是否还有更新的。
    fn page_after(&self, after: u64, limit: usize) -> (Vec<u64>, bool) {
        let mut seqs = (after.saturating_add(1)..=self.max_seq)
            .filter(|seq| self.is_visible(*seq))
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = seqs.len() > limit;
        seqs.truncate(limit);
        (seqs, has_more)
    }

    fn is_visible(&self, seq: u64) -> bool {
        self.visible.iter().any(|(a, b)| *a <= seq && seq <= *b)
    }

    pub fn visible_in(&self, from: u64, to: u64) -> Vec<u64> {
        (from..=to).filter(|seq| self.is_visible(*seq)).collect()
    }

    pub fn take_requests(&self) -> Vec<Req> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }

    fn check_online(&self) -> Result<()> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(FlareError::localized(ErrorCode::NetworkError, "offline"));
        }
        Ok(())
    }
}

impl SessionSyncRunner for ScriptedServer {
    fn request_message_sync(&self, _conversation_id: &str) -> BoxFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request_message_sync_from_seq(
        &self,
        _conversation_id: &str,
        _last_seq: u64,
        _limit: i32,
    ) -> BoxFuture<'_, ()> {
        self.cursor_syncs.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }

    fn request_message_backfill_before_seq(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: i32,
    ) -> BoxFuture<'_, bool> {
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let limit = limit.max(1) as usize;
            self.requests
                .lock()
                .unwrap()
                .push(Req::Before(before_seq, limit));
            self.check_online()?;
            let (seqs, has_more) = if self.empty_pages_with_more.load(Ordering::SeqCst) {
                (Vec::new(), true)
            } else {
                self.page_before(before_seq, limit)
            };
            let rows = seqs.iter().copied().map(row).collect::<Vec<_>>();
            if !rows.is_empty() {
                self.stores.messages.save_batch(&rows).await?;
            }
            let page = SingleConversationSyncRes {
                conversation_id: conversation_id.clone(),
                items: seqs
                    .iter()
                    .map(|seq| SyncSliceItem {
                        conversation_seq: *seq,
                        created_at: 0,
                        payload: Some(ItemPayload::Message(proto(*seq))),
                    })
                    .collect(),
                has_more,
                ..Default::default()
            };
            self.sync_apply
                .record_backfill_coverage(&conversation_id, before_seq, &page)
                .await;
            Ok(has_more)
        })
    }

    fn request_message_page_after_seq_detached(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: i32,
    ) -> BoxFuture<'_, bool> {
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let limit = limit.max(1) as usize;
            self.requests
                .lock()
                .unwrap()
                .push(Req::After(after_seq, limit));
            self.check_online()?;
            let (seqs, has_more) = self.page_after(after_seq, limit);
            let rows = seqs.iter().copied().map(row).collect::<Vec<_>>();
            if !rows.is_empty() {
                self.stores.messages.save_batch(&rows).await?;
            }
            // 连续同步页：服务端逐个 seq 补了 tombstone，证明到页内最后一条；没有更新的时到水位。
            let contiguous_to = if has_more {
                seqs.last().copied().unwrap_or(after_seq)
            } else {
                self.max_seq.max(after_seq)
            };
            if let Some(range) = SeqRange::new(after_seq.saturating_add(1), contiguous_to) {
                self.stores
                    .timeline_coverage
                    .add(&conversation_id, range)
                    .await?;
            }
            Ok(has_more)
        })
    }

    fn send_read_ack(&self, _conversation_id: &str, _read_seq: u64) -> BoxFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request_participants_sync(
        &self,
        _conversation_id: &str,
        _limit: i32,
    ) -> BoxFuture<'_, Vec<ConversationParticipant>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// 视图读消息经过的仓储：转发给内存仓储，并数读了几次、读了哪些 id。
#[derive(Default)]
pub(super) struct ReadCounts {
    /// `get_by_conversation`（最新窗口、往旧翻页、范围重读）的次数。
    pub pages: AtomicUsize,
    /// `get_by_conversation_after`（往新）的次数。
    pub newer_pages: AtomicUsize,
    /// 按 id 读的次数与 id 总数。
    pub by_id_calls: AtomicUsize,
    pub by_id_ids: AtomicUsize,
    pub by_id_rows: AtomicUsize,
}

impl ReadCounts {
    pub fn reset(&self) {
        for counter in [
            &self.pages,
            &self.newer_pages,
            &self.by_id_calls,
            &self.by_id_ids,
            &self.by_id_rows,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
    }

    pub fn get(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }
}

pub(super) struct CountingMessageStore {
    inner: Arc<dyn MessageStore>,
    pub counts: ReadCounts,
}

impl CountingMessageStore {
    fn by_id(&self, ids: usize, rows: usize) {
        self.counts.by_id_calls.fetch_add(1, Ordering::SeqCst);
        self.counts.by_id_ids.fetch_add(ids, Ordering::SeqCst);
        self.counts.by_id_rows.fetch_add(rows, Ordering::SeqCst);
    }
}

#[async_trait]
impl MessageReader for CountingMessageStore {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        let found = self.inner.get(message_id).await?;
        self.by_id(1, usize::from(found.is_some()));
        Ok(found)
    }

    async fn get_by_message_ids(&self, message_ids: &[String]) -> Result<Vec<IMMessage>> {
        let rows = self.inner.get_by_message_ids(message_ids).await?;
        self.by_id(message_ids.len(), rows.len());
        Ok(rows)
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        let found = self.inner.get_by_client_msg_id(client_msg_id).await?;
        self.by_id(1, usize::from(found.is_some()));
        Ok(found)
    }

    async fn get_by_client_msg_ids(&self, client_msg_ids: &[String]) -> Result<Vec<IMMessage>> {
        let rows = self.inner.get_by_client_msg_ids(client_msg_ids).await?;
        self.by_id(client_msg_ids.len(), rows.len());
        Ok(rows)
    }

    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.counts.pages.fetch_add(1, Ordering::SeqCst);
        self.inner
            .get_by_conversation(conversation_id, before_seq, limit)
            .await
    }

    async fn get_by_conversation_after(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.counts.newer_pages.fetch_add(1, Ordering::SeqCst);
        self.inner
            .get_by_conversation_after(conversation_id, after_seq, limit)
            .await
    }

    async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.inner.search(keyword, limit).await
    }

    async fn search_by_query(&self, query: &MessageSearchQuery) -> Result<Vec<IMMessage>> {
        self.inner.search_by_query(query).await
    }

    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.inner
            .search_in_conversation(conversation_id, keyword, limit)
            .await
    }
}

#[async_trait]
impl MessageWriter for CountingMessageStore {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        self.inner.save_batch(messages).await
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        self.inner.save_one(message).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        self.inner.update_status(message_id, status).await
    }

    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool> {
        self.inner.update_content(message_id, new_content).await
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        self.inner.delete(message_id).await
    }

    async fn rewrite_conversation_id(
        &self,
        old_conversation_id: &str,
        new_conversation_id: &str,
    ) -> Result<u64> {
        self.inner
            .rewrite_conversation_id(old_conversation_id, new_conversation_id)
            .await
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        self.inner.update_after_ack(client_msg_id, message).await
    }
}

#[async_trait]
impl MessageStore for CountingMessageStore {
    async fn list_reactions(
        &self,
        message_ids: &[String],
    ) -> Result<HashMap<String, Vec<ReactionEntry>>> {
        self.inner.list_reactions(message_ids).await
    }
}

pub(super) struct Setup<'a> {
    pub page_size: u32,
    pub max_seq: u64,
    pub visible_after_seq: u64,
    pub server: &'a [[u64; 2]],
    pub local: &'a [[u64; 2]],
    pub coverage: &'a [[u64; 2]],
}

/// 宿主侧窗口：只按 `view.updated` 的应用规则维护（社交包应用器的参考实现）。
#[derive(Clone, Debug, Default)]
pub(super) struct HostTimeline {
    pub revision: u64,
    pub rows: Vec<IMMessage>,
    pub has_older: bool,
    pub has_newer: bool,
    pub attached_to_latest: bool,
    pub conversation: Option<Conversation>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ApplyOutcome {
    Applied,
    /// 版本号跳了：不应用，取快照整体替换。
    SnapshotRequired,
    /// 比已应用的版本旧（快照已经包含它）：丢弃。
    Stale,
}

impl HostTimeline {
    pub fn from_snapshot(revision: u64, snapshot: &TimelineViewSnapshot) -> Self {
        Self {
            revision,
            rows: snapshot.messages.clone(),
            has_older: snapshot.has_older,
            has_newer: snapshot.has_newer,
            attached_to_latest: snapshot.attached_to_latest,
            conversation: snapshot.conversation.clone(),
        }
    }

    /// 应用规则：版本号不连续时不应用；删 `remove`；原位替换 `upsert`；`prepend` 放最前、
    /// `append` 放最后；`reorder` 非空时按它重排；最后更新三个状态字段与 `conversation`。
    /// `kind = snapshot` 时整体替换。
    pub fn apply(&mut self, update: &ViewUpdate) -> ApplyOutcome {
        if update.revision <= self.revision {
            return ApplyOutcome::Stale;
        }
        if update.revision != self.revision + 1 {
            return ApplyOutcome::SnapshotRequired;
        }
        match (&update.kind, &update.snapshot, &update.delta) {
            (ViewUpdateKind::Snapshot, Some(ViewSnapshot::Timeline(snapshot)), _) => {
                *self = Self::from_snapshot(update.revision, snapshot);
                return ApplyOutcome::Applied;
            }
            (
                ViewUpdateKind::Delta,
                _,
                Some(ViewDelta::Timeline {
                    prepend,
                    append,
                    upsert,
                    remove,
                    reorder,
                    has_older,
                    has_newer,
                    attached_to_latest,
                    conversation,
                }),
            ) => {
                self.rows.retain(|row| !remove.contains(&row.row_key()));
                for fresh in upsert {
                    let key = fresh.row_key();
                    if let Some(row) = self.rows.iter_mut().find(|row| row.row_key() == key) {
                        *row = fresh.clone();
                    }
                }
                let mut rows = prepend.clone();
                rows.append(&mut self.rows);
                rows.extend(append.iter().cloned());
                if let Some(order) = reorder {
                    let mut by_key = rows
                        .into_iter()
                        .map(|row| (row.row_key(), row))
                        .collect::<HashMap<_, _>>();
                    rows = order.iter().filter_map(|key| by_key.remove(key)).collect();
                }
                self.rows = rows;
                self.has_older = *has_older;
                self.has_newer = *has_newer;
                self.attached_to_latest = *attached_to_latest;
                if conversation.is_some() {
                    self.conversation = conversation.clone();
                }
            }
            other => panic!("not a timeline update: {other:?}"),
        }
        self.revision = update.revision;
        ApplyOutcome::Applied
    }
}

/// 视图当前窗口的可读形式。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct WindowState {
    /// 已发送行的 seq（时间线顺序）。
    pub seqs: Vec<u64>,
    /// 待发送行的行键（尾部）。
    pub pending: Vec<String>,
    pub has_older: bool,
    pub has_newer: bool,
    pub attached_to_latest: bool,
    pub anchor_key: Option<String>,
    pub revision: u64,
}

pub(super) struct Harness {
    pub view: ViewApi,
    pub stores: StoreProvider,
    pub server: Arc<ScriptedServer>,
    pub reads: Arc<CountingMessageStore>,
    view_id: StdMutex<String>,
    pub page_size: u32,
    events: tokio::sync::Mutex<EventReceiver>,
    pub host: StdMutex<HostTimeline>,
    /// 本步推出的更新（每次 `settle` 清空）。
    pub updates: StdMutex<Vec<ViewUpdate>>,
}

impl Harness {
    pub async fn new(setup: Setup<'_>) -> Self {
        let stores = in_memory_im_provider();
        stores
            .conversations
            .save_one(&Conversation {
                conversation_id: CONVERSATION.into(),
                channel_id: CONVERSATION.into(),
                conversation_type: ConversationType::Group,
                max_seq: setup.max_seq,
                visible_after_seq: setup.visible_after_seq,
                ..Default::default()
            })
            .await
            .unwrap();
        let rows = expand(setup.local).into_iter().map(row).collect::<Vec<_>>();
        if !rows.is_empty() {
            stores.messages.save_batch(&rows).await.unwrap();
        }
        for [from, to] in setup.coverage {
            stores
                .timeline_coverage
                .add(CONVERSATION, SeqRange::new(*from, *to).unwrap())
                .await
                .unwrap();
        }

        let bus = EventBus::new();
        let user: CurrentUserIdStore = Arc::new(RwLock::new("me".to_string()));
        let profiles = Arc::new(MemoryUserProfileStore::new());
        let reads = Arc::new(CountingMessageStore {
            inner: stores.messages.clone(),
            counts: ReadCounts::default(),
        });
        let conversation_api = ConversationApi::new(
            Arc::new(
                ConversationCommandUseCase::new(
                    stores.conversations.clone(),
                    user.clone(),
                    None,
                    Some(stores.cursors.clone()),
                )
                .with_timeline_coverage(stores.timeline_coverage.clone()),
            ),
            Arc::new(ConversationViewAssembler::new(
                stores.conversations.clone(),
                profiles.clone(),
            )),
            Arc::new(MessageViewAssembler::new(reads.clone(), profiles)),
            stores.timeline_coverage.clone(),
            bus.clone(),
            user,
        );
        let sync_apply = SyncApplyUseCase::new(
            stores.clone(),
            bus.clone(),
            EventDeduper::new(Some(64)),
            NotificationInboundPipeline::new(
                Arc::new(NotificationHandlerRegistry::new()),
                MessageDeduper::new(Some(64)),
                bus.clone(),
            ),
        );
        let server = Arc::new(ScriptedServer {
            visible: setup.server.iter().map(|[from, to]| (*from, *to)).collect(),
            max_seq: setup.max_seq,
            offline: AtomicBool::new(false),
            empty_pages_with_more: AtomicBool::new(false),
            requests: StdMutex::new(Vec::new()),
            cursor_syncs: AtomicUsize::new(0),
            stores: stores.clone(),
            sync_apply,
        });
        let events = bus.subscribe_raw();
        let view = ViewApi::new(
            conversation_api,
            Some(server.clone() as Arc<dyn SessionSyncRunner>),
            bus,
            AttentionRegistry::new(),
        );
        Self {
            view,
            stores,
            server,
            reads,
            view_id: StdMutex::new(String::new()),
            page_size: setup.page_size,
            events: tokio::sync::Mutex::new(events),
            host: StdMutex::new(HostTimeline::default()),
            updates: StdMutex::new(Vec::new()),
        }
    }

    pub fn view_id(&self) -> String {
        self.view_id.lock().unwrap().clone()
    }

    async fn opened(&self, request: OpenTimelineViewRequest) -> Result<()> {
        let response = self.view.open_timeline(request).await?;
        let ViewSnapshot::Timeline(snapshot) = &response.snapshot else {
            panic!("a timeline view opens with a timeline snapshot");
        };
        *self.host.lock().unwrap() = HostTimeline::from_snapshot(response.revision, snapshot);
        *self.view_id.lock().unwrap() = response.view_id;
        self.settle().await;
        Ok(())
    }

    pub async fn open(&self) {
        self.opened(OpenTimelineViewRequest {
            conversation_id: CONVERSATION.into(),
            message_limit: self.page_size,
            anchor: None,
        })
        .await
        .unwrap();
    }

    pub async fn open_around(&self, message_id: Option<&str>, seq: Option<u64>) -> Result<()> {
        self.opened(OpenTimelineViewRequest {
            conversation_id: CONVERSATION.into(),
            message_limit: self.page_size,
            anchor: Some(TimelineAnchor {
                message_id: message_id.map(str::to_string),
                seq,
            }),
        })
        .await
    }

    pub async fn load_older_page(&self) -> Result<TimelineViewPageResponse> {
        let outcome = self
            .view
            .load_older_timeline(LoadOlderTimelineViewRequest {
                view_id: self.view_id(),
                message_limit: self.page_size,
            })
            .await;
        self.settle().await;
        outcome
    }

    pub async fn load_older(&self) -> Result<u32> {
        self.load_older_page()
            .await
            .map(|response| response.loaded_count)
    }

    pub async fn load_newer(&self) -> Result<u32> {
        let outcome = self
            .view
            .load_newer_timeline(LoadNewerTimelineViewRequest {
                view_id: self.view_id(),
                message_limit: self.page_size,
            })
            .await;
        self.settle().await;
        outcome.map(|response| response.loaded_count)
    }

    pub async fn jump_to_latest(&self) -> TimelineViewPageResponse {
        let response = self
            .view
            .jump_to_latest_timeline(TimelineViewRequest {
                view_id: self.view_id(),
            })
            .await
            .unwrap();
        self.settle().await;
        response
    }

    /// 视图当前持有的窗口（seq 升序）与 hasOlder。
    pub async fn window(&self) -> (Vec<u64>, bool) {
        let state = self.state().await;
        (state.seqs, state.has_older)
    }

    pub async fn state(&self) -> WindowState {
        let view_id = self.view_id();
        let views = self.view.inner.views.read().await;
        let view = views.timelines.get(&view_id).expect("view is open");
        WindowState {
            seqs: view
                .window
                .messages
                .iter()
                .map(|message| message.conversation_seq)
                .filter(|seq| *seq > 0)
                .collect(),
            pending: view
                .window
                .messages
                .iter()
                .filter(|message| message.conversation_seq == 0)
                .map(IMMessage::row_key)
                .collect(),
            has_older: view.window.has_older,
            has_newer: view.window.has_newer(),
            attached_to_latest: view.window.attached_to_latest,
            anchor_key: view.window.anchor_key.clone(),
            revision: view.revision,
        }
    }

    pub async fn window_rows(&self) -> Vec<IMMessage> {
        let view_id = self.view_id();
        let views = self.view.inner.views.read().await;
        views
            .timelines
            .get(&view_id)
            .expect("view is open")
            .window
            .messages
            .clone()
    }

    pub async fn coverage(&self) -> Vec<[u64; 2]> {
        self.stores
            .timeline_coverage
            .coverage(CONVERSATION)
            .await
            .unwrap()
            .ranges()
            .iter()
            .map(|range| [range.from, range.to])
            .collect()
    }

    /// 取走这一步推出的更新，逐条应用到宿主侧窗口，并断言结果与视图的窗口相同。
    pub async fn settle(&self) {
        let view_id = self.view_id();
        let mut drained = Vec::new();
        {
            let mut events = self.events.lock().await;
            while let Ok(event) = events.try_recv() {
                if let SdkEvent::View(update) = event
                    && update.view_id == view_id
                {
                    drained.push(update);
                }
            }
        }
        for update in &drained {
            let applied = self.host.lock().unwrap().apply(update);
            assert_eq!(
                applied,
                ApplyOutcome::Applied,
                "revision {} must follow the applied one",
                update.revision
            );
        }
        self.updates.lock().unwrap().extend(drained);
        if view_id.is_empty() {
            return;
        }
        let views = self.view.inner.views.read().await;
        let Some(view) = views.timelines.get(&view_id) else {
            return;
        };
        let host = self.host.lock().unwrap();
        assert_eq!(host.revision, view.revision, "host revision");
        assert_eq!(
            serde_json::to_value(&host.rows).unwrap(),
            serde_json::to_value(&view.window.messages).unwrap(),
            "the host rows built from the updates equal the view window"
        );
        assert_eq!(host.has_older, view.window.has_older, "host hasOlder");
        assert_eq!(host.has_newer, view.window.has_newer(), "host hasNewer");
        assert_eq!(
            host.attached_to_latest, view.window.attached_to_latest,
            "host attachedToLatest"
        );
    }

    pub fn take_updates(&self) -> Vec<ViewUpdate> {
        std::mem::take(&mut *self.updates.lock().unwrap())
    }

    pub async fn refresh(&self, event: SdkEvent) {
        let plan = view_refresh_plan(&event).expect("the event refreshes views");
        self.view.refresh_open_views(plan).await;
        self.settle().await;
    }

    /// 一条实时推送：只落库，不写区间，经实时合并进窗口。
    pub async fn live_message(&self, seq: u64) {
        let message = row(seq);
        self.stores
            .messages
            .save_batch(std::slice::from_ref(&message))
            .await
            .unwrap();
        self.refresh(SdkEvent::Message(MessageEvent::Received {
            message: Box::new(message),
        }))
        .await;
    }

    /// 本机发出一条消息：发送队列先落库乐观行，再推 `ReceivedBatch`。
    pub async fn send(&self, client_msg_id: &str) {
        let mut message = IMMessage::new(ProtoMessage {
            conversation_id: CONVERSATION.into(),
            client_msg_id: client_msg_id.into(),
            sender_id: "me".into(),
            ..Default::default()
        });
        message.local_state.is_local = true;
        message.local_state.sending = true;
        message.local_state.sort_ts = 1;
        self.stores
            .messages
            .save_batch(std::slice::from_ref(&message))
            .await
            .unwrap();
        self.refresh(SdkEvent::Message(MessageEvent::ReceivedBatch {
            messages: vec![message],
        }))
        .await;
    }

    /// 发送确认：同一行拿到服务端 id 与 seq，再推 `SendAck`。
    pub async fn ack(&self, client_msg_id: &str, seq: u64) {
        let acked = IMMessage::new(ProtoMessage {
            conversation_id: CONVERSATION.into(),
            client_msg_id: client_msg_id.into(),
            server_id: format!("s{seq}"),
            sender_id: "me".into(),
            conversation_seq: seq,
            created_at: seq as i64,
            ..Default::default()
        });
        self.stores
            .messages
            .update_after_ack(client_msg_id, &acked)
            .await
            .unwrap();
        self.refresh(SdkEvent::Message(MessageEvent::SendAck {
            ack: Box::new(SendAck {
                conversation_id: CONVERSATION.into(),
                client_msg_id: client_msg_id.into(),
                ..Default::default()
            }),
        }))
        .await;
    }

    /// 一次同步证明 `[from, to]`：服务端可见的消息落库、写区间，同步完成后视图重读。
    pub async fn gap_filled(&self, from: u64, to: u64) {
        let rows = self
            .server
            .visible_in(from, to)
            .into_iter()
            .map(row)
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            self.stores.messages.save_batch(&rows).await.unwrap();
        }
        self.stores
            .timeline_coverage
            .add(CONVERSATION, SeqRange::new(from, to).unwrap())
            .await
            .unwrap();
        self.refresh(sync_finished()).await;
    }

    pub async fn event(&self, kind: &str) {
        let event = match kind {
            "reaction" => SdkEvent::Message(MessageEvent::ReactionChanged {
                conversation_id: CONVERSATION.into(),
                server_msg_id: "s10".into(),
                user_id: "u2".into(),
                emoji: "+1".into(),
                action: 1,
            }),
            "recall" => SdkEvent::Message(MessageEvent::Recalled {
                conversation_id: CONVERSATION.into(),
                event: MessageRecallEvent::default(),
            }),
            "delete" => SdkEvent::Message(MessageEvent::Deleted {
                conversation_id: CONVERSATION.into(),
                event: MessageDeleteEvent::default(),
            }),
            "syncFinished" => sync_finished(),
            other => panic!("unknown event kind {other}"),
        };
        self.refresh(event).await;
    }
}

pub(super) fn sync_finished() -> SdkEvent {
    SdkEvent::Sync(SyncNotify::Finished {
        run: SyncRunContext::new(
            SyncTrigger::Manual,
            SyncScope::Global,
            SyncVisibility::Silent,
            SyncReason::UserRequested,
        ),
        phase: SyncPhase::Background,
    })
}
