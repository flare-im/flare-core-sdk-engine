use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc::error::TryRecvError};

use crate::client::api::ConversationApi;
use crate::kernel::event::{ConversationEvent, EventBus, MessageEvent, SdkEvent, SyncNotify};
use crate::kernel::{AttentionRegistry, SessionSyncRunner};
use crate::model::{
    BootstrapHomeTimelineRequest, CloseViewRequest, CloseViewResponse, Conversation,
    ConversationTimelineSnapshot, HomeTimelineSnapshot, IMMessage, LoadOlderTimelineViewRequest,
    OpenConversationListViewRequest, OpenConversationTimelineRequest, OpenTimelineViewRequest,
    ViewDelta, ViewDeltaKind, ViewDeltaOp, ViewLoadOlderResponse, ViewOpenResponse, ViewSnapshot,
    ViewUpdate, ViewUpdateKind, normalized_conversation_limit, normalized_message_limit,
};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::{BackgroundTask, delay, spawn_background, spawn_background_task};

const VIEW_REFRESH_DEBOUNCE: Duration = Duration::from_millis(40);
const MAX_TIMELINE_VIEWS: usize = 8;
const MAX_CONVERSATION_LIST_VIEWS: usize = 2;
const MAX_TIMELINE_WINDOW_MESSAGES: usize = 10_000;

#[derive(Clone)]
pub struct ViewApi {
    inner: Arc<ViewApiInner>,
}

struct ViewApiInner {
    conversation_api: ConversationApi,
    session_sync: Option<Arc<dyn SessionSyncRunner>>,
    bus: EventBus,
    /// 共享注意力：打开时间线即把该会话标为前台，收敛（重连/后台补齐）优先该会话——秒展示。
    attention: AttentionRegistry,
    /// I5：前台定向补拉的 single-flight（同会话在途去重）。
    foreground_pull: ForegroundPullGuard,
    views: RwLock<ViewRegistrations>,
    next_id: AtomicU64,
    refresh_worker: Mutex<Option<BackgroundTask>>,
}

/// I5：前台定向补拉守卫——同会话在途只允许一个补拉，完成后释放。
/// 与批量收敛的重叠由应用侧幂等（LWW + converger 去重）兜底；本守卫消灭的是重复网络往返。
#[derive(Default)]
struct ForegroundPullGuard {
    in_flight: Mutex<HashSet<String>>,
}

impl ForegroundPullGuard {
    /// 占用该会话的在途名额；已在途返回 false（调用方跳过本次补拉）。
    fn try_begin(&self, conversation_id: &str) -> bool {
        self.lock().insert(conversation_id.to_string())
    }

    fn finish(&self, conversation_id: &str) {
        self.lock().remove(conversation_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for ViewApiInner {
    fn drop(&mut self) {
        if let Ok(worker) = self.refresh_worker.get_mut()
            && let Some(worker) = worker.take()
        {
            worker.abort();
        }
    }
}

#[derive(Default)]
struct ViewRegistrations {
    timelines: HashMap<String, TimelineViewRegistration>,
    conversation_lists: HashMap<String, ConversationListViewRegistration>,
}

struct TimelineViewRegistration {
    request: OpenTimelineViewRequest,
    order: u64,
    last_snapshot: ConversationTimelineSnapshot,
}

struct ConversationListViewRegistration {
    request: OpenConversationListViewRequest,
    order: u64,
    last_snapshot: HomeTimelineSnapshot,
}

impl ViewRegistrations {
    fn insert_timeline(
        &mut self,
        view_id: String,
        request: OpenTimelineViewRequest,
        order: u64,
        last_snapshot: ConversationTimelineSnapshot,
    ) {
        let conversation_id = request.conversation_id.clone();
        self.timelines
            .retain(|_, view| view.request.conversation_id != conversation_id);
        self.timelines.insert(
            view_id,
            TimelineViewRegistration {
                request,
                order,
                last_snapshot,
            },
        );
        evict_oldest(&mut self.timelines, MAX_TIMELINE_VIEWS);
    }

    fn insert_conversation_list(
        &mut self,
        view_id: String,
        request: OpenConversationListViewRequest,
        order: u64,
        last_snapshot: HomeTimelineSnapshot,
    ) {
        self.conversation_lists.insert(
            view_id,
            ConversationListViewRegistration {
                request,
                order,
                last_snapshot,
            },
        );
        evict_oldest(&mut self.conversation_lists, MAX_CONVERSATION_LIST_VIEWS);
    }
}

trait OrderedViewRegistration {
    fn order(&self) -> u64;
}

impl OrderedViewRegistration for TimelineViewRegistration {
    fn order(&self) -> u64 {
        self.order
    }
}

impl OrderedViewRegistration for ConversationListViewRegistration {
    fn order(&self) -> u64 {
        self.order
    }
}

fn evict_oldest<T: OrderedViewRegistration>(views: &mut HashMap<String, T>, limit: usize) {
    while views.len() > limit {
        let Some(oldest_id) = views
            .iter()
            .min_by_key(|(_, view)| view.order())
            .map(|(view_id, _)| view_id.clone())
        else {
            return;
        };
        views.remove(&oldest_id);
    }
}

impl ViewApi {
    pub fn new(
        conversation_api: ConversationApi,
        session_sync: Option<Arc<dyn SessionSyncRunner>>,
        bus: EventBus,
        attention: AttentionRegistry,
    ) -> Self {
        let api = Self {
            inner: Arc::new(ViewApiInner {
                conversation_api,
                session_sync,
                bus,
                attention,
                foreground_pull: ForegroundPullGuard::default(),
                views: RwLock::new(ViewRegistrations::default()),
                next_id: AtomicU64::new(1),
                refresh_worker: Mutex::new(None),
            }),
        };
        let worker = api.spawn_refresh_worker();
        if let Ok(mut slot) = api.inner.refresh_worker.lock() {
            *slot = Some(worker);
        }
        api
    }

    pub async fn open_timeline(
        &self,
        request: OpenTimelineViewRequest,
    ) -> Result<ViewOpenResponse> {
        let request = OpenTimelineViewRequest {
            conversation_id: request.conversation_id.trim().to_string(),
            message_limit: normalized_message_limit(request.message_limit),
        };
        // 注意力信号：打开的会话升为前台，后续收敛优先补齐它（秒展示）。app 打开会话即自动触发。
        // I5：同时立即定向补拉该会话（P0 中断，不等下一轮收敛）——本地快照仍即时返回不被网络阻塞。
        if !request.conversation_id.is_empty() {
            self.inner.attention.open_timeline(&request.conversation_id);
            self.spawn_foreground_pull(&request.conversation_id);
        }
        let snapshot = self.timeline_snapshot(&request).await?;
        let (view_id, order) = self.next_view_id("timeline");
        self.inner.views.write().await.insert_timeline(
            view_id.clone(),
            request,
            order,
            snapshot.clone(),
        );
        Ok(ViewOpenResponse {
            view_id,
            snapshot: ViewSnapshot::Timeline(snapshot),
        })
    }

    /// I5：打开会话触发一次该会话定向补拉（fire-and-forget）。best-effort：
    /// 未接线/未连接时静默降级为纯 attention hint（等下一轮收敛）；single-flight 在途去重。
    fn spawn_foreground_pull(&self, conversation_id: &str) {
        let Some(session_sync) = self.inner.session_sync.clone() else {
            return;
        };
        if !self.inner.foreground_pull.try_begin(conversation_id) {
            return;
        }
        let conversation_id = conversation_id.to_string();
        let inner = Arc::downgrade(&self.inner);
        spawn_background(async move {
            if let Err(error) = session_sync.request_message_sync(&conversation_id).await {
                tracing::debug!(
                    conversation_id = %conversation_id,
                    error = %error,
                    "前台定向补拉失败（best-effort，等常规收敛补齐）"
                );
            }
            if let Some(inner) = inner.upgrade() {
                inner.foreground_pull.finish(&conversation_id);
            }
        });
    }

    /// 往旧翻一页。窗口最旧一行为 `O`、页大小 `L`：
    ///
    /// 1. `O-1` 落在某段区间 `[r, ..]` 里：读本地 seq 在 `[r, O-1]` 内、由新到旧的 `L` 行；
    ///    不够 `L` 行而 `r = 1` 就是到了起点；`r > 1` 则以 `before = r` 回填缺的行数，区间扩展后
    ///    在新的区间内接着读。
    /// 2. `O-1` 不在任何区间里（窗口紧挨着一个洞）：以 `before = O` 回填，写入区间后再读本地。
    /// 3. 回填失败（离线、超时、服务端错误）：窗口不变，返回错误。不越过缺口去读更旧的本地行。
    /// 4. 窗口里没有已发送的行：以会话已知最新 seq 之上一位为 `O`。
    ///
    /// `has_more`（hasOlder）只由区间与窗口之下的本地行决定。
    pub async fn load_older_timeline(
        &self,
        request: LoadOlderTimelineViewRequest,
    ) -> Result<ViewLoadOlderResponse> {
        let view_id = request.view_id.trim().to_string();
        if view_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "viewId is required",
            ));
        }
        let requested_limit = normalized_message_limit(request.message_limit) as usize;
        let (conversation_id, oldest_seq, page_limit) = {
            let views = self.inner.views.read().await;
            // view 已关闭（用户切走会话 / silent_refresh 替换了 view）是无害竞态：
            // 加载更多历史对已关闭的 timeline 无意义，静默返回"无更多"而非报错打断 UI。
            let Some(registration) = views.timelines.get(&view_id) else {
                return Ok(ViewLoadOlderResponse {
                    view_id,
                    loaded_count: 0,
                    has_more: false,
                    update: None,
                });
            };
            let current_len = registration.last_snapshot.messages.len();
            if current_len >= MAX_TIMELINE_WINDOW_MESSAGES {
                return Ok(ViewLoadOlderResponse {
                    view_id,
                    loaded_count: 0,
                    has_more: false,
                    update: None,
                });
            }
            (
                registration.request.conversation_id.clone(),
                oldest_positive_seq(&registration.last_snapshot),
                requested_limit.min(MAX_TIMELINE_WINDOW_MESSAGES - current_len),
            )
        };

        if page_limit == 0 {
            return Ok(ViewLoadOlderResponse {
                view_id,
                loaded_count: 0,
                has_more: true,
                update: None,
            });
        }

        let page = self
            .read_older_page(&conversation_id, oldest_seq, page_limit)
            .await?;

        let (loaded_count, has_more, update) = {
            let mut views = self.inner.views.write().await;
            // 网络往返期间 view 被关闭同属无害竞态（拉到的历史丢弃即可，用户已切走）。
            let Some(registration) = views.timelines.get_mut(&view_id) else {
                return Ok(ViewLoadOlderResponse {
                    view_id,
                    loaded_count: 0,
                    has_more: false,
                    update: None,
                });
            };
            let old = registration.last_snapshot.clone();
            let (mut new, loaded_count) = expand_timeline_snapshot(
                &old,
                &page.messages,
                page.has_older,
                MAX_TIMELINE_WINDOW_MESSAGES,
            );
            if new.messages.len() >= MAX_TIMELINE_WINDOW_MESSAGES {
                new.has_more = false;
            }
            let has_more = new.has_more;
            let update = timeline_view_update(&view_id, &old, &new);
            registration.request.message_limit = registration
                .request
                .message_limit
                .max(new.messages.len() as u32);
            registration.last_snapshot = new;
            (loaded_count, has_more, update)
        };

        Ok(ViewLoadOlderResponse {
            view_id,
            loaded_count,
            has_more,
            update,
        })
    }

    /// 读窗口之下的一页（规则见 [`Self::load_older_timeline`]）。只读区间内的本地行；
    /// 碰到区间边界就回填，回填失败直接返回错误，已读到的行也不并入。
    async fn read_older_page(
        &self,
        conversation_id: &str,
        oldest_seq: Option<u64>,
        limit: usize,
    ) -> Result<OlderPage> {
        let api = &self.inner.conversation_api;
        let (conversation, mut coverage) = api.timeline_paging_state(conversation_id).await?;
        let window_oldest = match oldest_seq {
            Some(seq) => seq,
            None => conversation
                .as_ref()
                .map(|conversation| conversation.max_seq)
                .unwrap_or(0)
                .max(coverage.highest_proven_seq())
                .saturating_add(1),
        };
        let mut messages: Vec<IMMessage> = Vec::with_capacity(limit);
        let mut backfills = 0;
        loop {
            let lowest = messages
                .last()
                .map(|message| message.conversation_seq)
                .unwrap_or(window_oldest);
            if lowest <= 1 {
                return Ok(OlderPage {
                    messages,
                    has_older: false,
                });
            }
            let before = match coverage.range_containing(lowest - 1) {
                Some(range) => {
                    let (rows, more) = api
                        .timeline_rows_in_range(
                            conversation_id,
                            range.from,
                            lowest,
                            limit - messages.len(),
                        )
                        .await?;
                    messages.extend(rows);
                    if messages.len() >= limit || range.from == 1 {
                        let oldest = messages
                            .last()
                            .map(|message| message.conversation_seq)
                            .unwrap_or(lowest);
                        return Ok(OlderPage {
                            messages,
                            has_older: coverage.has_older(oldest, more),
                        });
                    }
                    range.from
                }
                None => lowest,
            };
            if backfills == MAX_BACKFILLS_PER_PAGE {
                return Ok(OlderPage {
                    messages,
                    has_older: true,
                });
            }
            backfills += 1;
            self.backfill_before(conversation_id, before, limit - messages.len())
                .await?;
            let (_, extended) = api.timeline_paging_state(conversation_id).await?;
            let progressed = extended.range_containing(before - 1).is_some();
            coverage = extended;
            if !progressed {
                // 服务端没有证明任何东西（不该发生）：不再重复请求，留给下一次往旧翻页。
                return Ok(OlderPage {
                    messages,
                    has_older: true,
                });
            }
        }
    }

    async fn backfill_before(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: usize,
    ) -> Result<()> {
        let Some(sync) = &self.inner.session_sync else {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未配置同步"));
        };
        sync.request_message_backfill_before_seq(conversation_id, before_seq, limit.max(1) as i32)
            .await
            .map(|_| ())
    }

    pub async fn open_conversation_list(
        &self,
        request: OpenConversationListViewRequest,
    ) -> Result<ViewOpenResponse> {
        let request = OpenConversationListViewRequest {
            conversation_limit: normalized_conversation_limit(request.conversation_limit),
        };
        let snapshot = self.conversation_list_snapshot(&request).await?;
        let (view_id, order) = self.next_view_id("conversation_list");
        self.inner.views.write().await.insert_conversation_list(
            view_id.clone(),
            request,
            order,
            snapshot.clone(),
        );
        Ok(ViewOpenResponse {
            view_id,
            snapshot: ViewSnapshot::ConversationList(snapshot),
        })
    }

    pub async fn close(&self, request: CloseViewRequest) -> Result<CloseViewResponse> {
        let view_id = request.view_id.trim();
        if view_id.is_empty() {
            return Ok(CloseViewResponse { closed: false });
        }
        let (closed_timeline, closed_list) = {
            let mut views = self.inner.views.write().await;
            (
                views.timelines.remove(view_id),
                views.conversation_lists.remove(view_id).is_some(),
            )
        };
        // 关时间线即清注意力前台——否则已关会话永远保持 P0，持续错误抢占收敛优先级。
        if let Some(registration) = &closed_timeline {
            self.inner
                .attention
                .close_timeline(&registration.request.conversation_id);
        }
        Ok(CloseViewResponse {
            closed: closed_timeline.is_some() || closed_list,
        })
    }

    async fn timeline_snapshot(
        &self,
        request: &OpenTimelineViewRequest,
    ) -> Result<crate::model::ConversationTimelineSnapshot> {
        self.inner
            .conversation_api
            .open_timeline(OpenConversationTimelineRequest {
                conversation_id: request.conversation_id.clone(),
                message_limit: request.message_limit,
            })
            .await
    }

    /// 事件触发的重读：在窗口的 seq 范围内重读，不再用打开时的「最新窗口」去整窗替换，
    /// 所以窗口不会缩回、`has_more` 也不会因为重读回跳。窗口里还没有已发送的行时按打开处理。
    async fn reload_timeline(
        &self,
        request: &OpenTimelineViewRequest,
        oldest_seq: Option<u64>,
    ) -> Result<ConversationTimelineSnapshot> {
        let Some(oldest_seq) = oldest_seq else {
            return self.timeline_snapshot(request).await;
        };
        self.inner
            .conversation_api
            .reload_timeline_window(
                &request.conversation_id,
                oldest_seq,
                normalized_timeline_window_limit(request.message_limit),
            )
            .await
    }

    async fn conversation_list_snapshot(
        &self,
        request: &OpenConversationListViewRequest,
    ) -> Result<crate::model::HomeTimelineSnapshot> {
        self.inner
            .conversation_api
            .bootstrap_home(BootstrapHomeTimelineRequest {
                conversation_limit: request.conversation_limit,
            })
            .await
    }

    fn next_view_id(&self, prefix: &str) -> (String, u64) {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        (format!("{prefix}:{id}"), id)
    }

    fn spawn_refresh_worker(&self) -> BackgroundTask {
        let inner = Arc::downgrade(&self.inner);
        let bus = self.inner.bus.clone();
        spawn_background_task(async move {
            let mut rx = bus.subscribe_raw();
            loop {
                let mut plan = match rx.recv().await {
                    Ok(event) => {
                        let Some(plan) = view_refresh_plan(&event) else {
                            continue;
                        };
                        plan
                    }
                    Err(_) => break,
                };
                delay(VIEW_REFRESH_DEBOUNCE).await;

                let mut closed = false;
                loop {
                    match rx.try_recv() {
                        Ok(event) => {
                            if let Some(next_plan) = view_refresh_plan(&event) {
                                plan.merge(next_plan);
                            }
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            closed = true;
                            break;
                        }
                    }
                }

                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let api = ViewApi { inner };
                api.refresh_open_views(plan).await;
                if closed {
                    break;
                }
            }
        })
    }

    async fn refresh_open_views(&self, plan: ViewRefreshPlan) {
        if !plan.hot_messages.is_empty() {
            self.publish_hot_timeline_messages(&plan.hot_messages, plan.timeline_query.as_ref())
                .await;
        }

        let (timelines, conversation_lists) = {
            let views = self.inner.views.read().await;
            (
                plan.timeline_query
                    .as_ref()
                    .map(|target| {
                        views
                            .timelines
                            .iter()
                            .filter(|(_, view)| {
                                target.matches_timeline(&view.request.conversation_id)
                            })
                            .map(|(view_id, view)| {
                                (
                                    view_id.clone(),
                                    view.request.clone(),
                                    oldest_positive_seq(&view.last_snapshot),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                if plan.refresh_conversation_lists {
                    views
                        .conversation_lists
                        .iter()
                        .map(|(view_id, view)| (view_id.clone(), view.request.clone()))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                },
            )
        };

        for (view_id, request, oldest_seq) in timelines {
            match self.reload_timeline(&request, oldest_seq).await {
                Ok(snapshot) => {
                    self.publish_timeline_refresh(view_id, snapshot, oldest_seq)
                        .await
                }
                Err(error) => tracing::warn!(
                    target = "flare_sdk.view",
                    view_id,
                    error = %error,
                    "refresh timeline view failed"
                ),
            }
        }

        for (view_id, request) in conversation_lists {
            match self.conversation_list_snapshot(&request).await {
                Ok(snapshot) => {
                    self.publish_conversation_list_refresh(view_id, snapshot)
                        .await
                }
                Err(error) => tracing::warn!(
                    target = "flare_sdk.view",
                    view_id,
                    error = %error,
                    "refresh conversation list view failed"
                ),
            }
        }
    }

    async fn publish_hot_timeline_messages(
        &self,
        messages: &[IMMessage],
        query_target: Option<&ViewRefreshTarget>,
    ) {
        let active_conversation_ids = {
            let views = self.inner.views.read().await;
            views
                .timelines
                .values()
                .filter(|view| {
                    !query_target
                        .map(|target| target.matches_timeline(&view.request.conversation_id))
                        .unwrap_or(false)
                })
                .map(|view| view.request.conversation_id.clone())
                .collect::<HashSet<_>>()
        };
        if active_conversation_ids.is_empty() {
            return;
        }

        let mut hot_messages = messages
            .iter()
            .filter(|message| active_conversation_ids.contains(message.conversation_id.trim()))
            .cloned()
            .collect::<Vec<_>>();
        if hot_messages.is_empty() {
            return;
        }

        let mut latest_conversations = HashMap::new();
        for conversation_id in &active_conversation_ids {
            if !hot_messages
                .iter()
                .any(|message| message.conversation_id.trim() == conversation_id)
            {
                continue;
            }
            match self.inner.conversation_api.get(conversation_id).await {
                Ok(Some(conversation)) => {
                    latest_conversations.insert(conversation_id.clone(), conversation);
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    target = "flare_sdk.view",
                    conversation_id,
                    error = %error,
                    "load hot conversation projection failed"
                ),
            }
        }

        if let Err(error) = self
            .inner
            .conversation_api
            .hydrate_timeline_messages(&mut hot_messages)
            .await
        {
            tracing::warn!(
                target = "flare_sdk.view",
                error = %error,
                "hydrate hot timeline messages failed"
            );
            return;
        }

        let updates = {
            let mut views = self.inner.views.write().await;
            let mut updates = Vec::new();
            for (view_id, registration) in views.timelines.iter_mut() {
                if query_target
                    .map(|target| target.matches_timeline(&registration.request.conversation_id))
                    .unwrap_or(false)
                {
                    continue;
                }
                let old = registration.last_snapshot.clone();
                let Some(new) = hot_timeline_snapshot(
                    &old,
                    &hot_messages,
                    registration.request.message_limit,
                    latest_conversations.get(&registration.request.conversation_id),
                ) else {
                    continue;
                };
                let update = timeline_view_update(view_id, &old, &new);
                registration.last_snapshot = new;
                if let Some(update) = update {
                    updates.push(update);
                }
            }
            updates
        };

        for update in updates {
            self.inner.bus.publish(SdkEvent::View(update));
        }
    }

    async fn publish_timeline_refresh(
        &self,
        view_id: String,
        mut snapshot: ConversationTimelineSnapshot,
        reloaded_from: Option<u64>,
    ) {
        let update = {
            let mut views = self.inner.views.write().await;
            let Some(registration) = views.timelines.get_mut(&view_id) else {
                return;
            };
            // 重读期间往旧翻页并入了更旧的行：这些行与它们算出的 has_more 保留。
            if let Some(reloaded_from) = reloaded_from
                && oldest_positive_seq(&registration.last_snapshot)
                    .is_some_and(|oldest| oldest < reloaded_from)
            {
                snapshot.messages.extend(
                    registration
                        .last_snapshot
                        .messages
                        .iter()
                        .filter(|message| {
                            message.conversation_seq > 0 && message.conversation_seq < reloaded_from
                        })
                        .cloned(),
                );
                snapshot
                    .messages
                    .sort_by(IMMessage::compare_for_timeline_asc);
                snapshot.has_more = registration.last_snapshot.has_more;
            }
            let update = timeline_view_update(&view_id, &registration.last_snapshot, &snapshot);
            registration.last_snapshot = snapshot;
            update
        };
        if let Some(update) = update {
            self.inner.bus.publish(SdkEvent::View(update));
        }
    }

    async fn publish_conversation_list_refresh(
        &self,
        view_id: String,
        snapshot: HomeTimelineSnapshot,
    ) {
        let update = {
            let mut views = self.inner.views.write().await;
            let Some(registration) = views.conversation_lists.get_mut(&view_id) else {
                return;
            };
            let update =
                conversation_list_view_update(&view_id, &registration.last_snapshot, &snapshot);
            registration.last_snapshot = snapshot;
            update
        };
        if let Some(update) = update {
            self.inner.bus.publish(SdkEvent::View(update));
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ViewRefreshTarget {
    All,
    Conversations(HashSet<String>),
}

impl ViewRefreshTarget {
    fn matches_timeline(&self, conversation_id: &str) -> bool {
        match self {
            Self::All => true,
            Self::Conversations(ids) => ids.contains(conversation_id),
        }
    }

    fn merge(&mut self, other: ViewRefreshTarget) {
        if matches!(self, Self::All) {
            return;
        }
        match other {
            Self::All => *self = Self::All,
            Self::Conversations(other_ids) => {
                if let Self::Conversations(ids) = self {
                    ids.extend(other_ids);
                }
            }
        }
    }
}

struct ViewRefreshPlan {
    timeline_query: Option<ViewRefreshTarget>,
    hot_messages: Vec<IMMessage>,
    refresh_conversation_lists: bool,
}

impl ViewRefreshPlan {
    fn query(target: ViewRefreshTarget) -> Self {
        Self {
            timeline_query: Some(target),
            hot_messages: Vec::new(),
            refresh_conversation_lists: true,
        }
    }

    fn hot_messages(messages: Vec<IMMessage>) -> Option<Self> {
        (!messages.is_empty()).then_some(Self {
            timeline_query: None,
            hot_messages: messages,
            refresh_conversation_lists: true,
        })
    }

    fn merge(&mut self, other: ViewRefreshPlan) {
        match (&mut self.timeline_query, other.timeline_query) {
            (Some(target), Some(other_target)) => target.merge(other_target),
            (None, Some(other_target)) => self.timeline_query = Some(other_target),
            _ => {}
        }
        self.hot_messages.extend(other.hot_messages);
        self.refresh_conversation_lists |= other.refresh_conversation_lists;
    }
}

fn view_refresh_plan(event: &SdkEvent) -> Option<ViewRefreshPlan> {
    match event {
        SdkEvent::Message(MessageEvent::Received { message }) => {
            ViewRefreshPlan::hot_messages(vec![message.as_ref().clone()])
        }
        SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) => {
            ViewRefreshPlan::hot_messages(messages.clone())
        }
        _ => view_refresh_target(event).map(ViewRefreshPlan::query),
    }
}

fn view_refresh_target(event: &SdkEvent) -> Option<ViewRefreshTarget> {
    match event {
        SdkEvent::Message(MessageEvent::Received { .. } | MessageEvent::ReceivedBatch { .. }) => {
            None
        }
        SdkEvent::Message(event) => Some(message_refresh_target(event)),
        SdkEvent::Conversation(event) => Some(conversation_refresh_target(event)),
        SdkEvent::Sync(SyncNotify::Finished { .. } | SyncNotify::ResyncNeeded { .. }) => {
            Some(ViewRefreshTarget::All)
        }
        _ => None,
    }
}

fn message_refresh_target(event: &MessageEvent) -> ViewRefreshTarget {
    let mut ids = HashSet::new();
    match event {
        MessageEvent::Received { message } => insert_non_empty(&mut ids, &message.conversation_id),
        MessageEvent::ReceivedBatch { messages } => {
            for message in messages {
                insert_non_empty(&mut ids, &message.conversation_id);
            }
        }
        MessageEvent::SendAck { ack } => insert_non_empty(&mut ids, &ack.conversation_id),
        MessageEvent::Recalled {
            conversation_id, ..
        }
        | MessageEvent::Typing {
            conversation_id, ..
        }
        | MessageEvent::TypingAggregate {
            conversation_id, ..
        }
        | MessageEvent::Edited {
            conversation_id, ..
        }
        | MessageEvent::ReactionChanged {
            conversation_id, ..
        }
        | MessageEvent::Deleted {
            conversation_id, ..
        }
        | MessageEvent::ReadReceipt {
            conversation_id, ..
        }
        | MessageEvent::RetentionScheduled {
            conversation_id, ..
        }
        | MessageEvent::RetentionExpired {
            conversation_id, ..
        }
        | MessageEvent::RetentionPurged {
            conversation_id, ..
        }
        | MessageEvent::Pinned {
            conversation_id, ..
        }
        | MessageEvent::Unpinned {
            conversation_id, ..
        }
        | MessageEvent::Marked {
            conversation_id, ..
        }
        | MessageEvent::Unmarked {
            conversation_id, ..
        }
        | MessageEvent::PresenceChanged {
            conversation_id, ..
        }
        | MessageEvent::Custom {
            conversation_id, ..
        }
        | MessageEvent::PluginStateChanged {
            conversation_id, ..
        }
        | MessageEvent::ReadSummariesChanged {
            conversation_id, ..
        } => insert_non_empty(&mut ids, conversation_id),
        MessageEvent::SendFailed { .. } => return ViewRefreshTarget::All,
    }
    if ids.is_empty() {
        ViewRefreshTarget::All
    } else {
        ViewRefreshTarget::Conversations(ids)
    }
}

fn conversation_refresh_target(event: &ConversationEvent) -> ViewRefreshTarget {
    let mut ids = HashSet::new();
    match event {
        ConversationEvent::Synced { conversation_ids } => {
            for conversation_id in conversation_ids {
                insert_non_empty(&mut ids, conversation_id);
            }
        }
        ConversationEvent::Created { conversation_id }
        | ConversationEvent::Updated { conversation_id }
        | ConversationEvent::UnreadCountChanged {
            conversation_id, ..
        }
        | ConversationEvent::Deleted { conversation_id } => {
            insert_non_empty(&mut ids, conversation_id)
        }
    }
    if ids.is_empty() {
        ViewRefreshTarget::All
    } else {
        ViewRefreshTarget::Conversations(ids)
    }
}

fn insert_non_empty(ids: &mut HashSet<String>, value: &str) {
    let value = value.trim();
    if !value.is_empty() {
        ids.insert(value.to_string());
    }
}

fn hot_timeline_snapshot(
    snapshot: &ConversationTimelineSnapshot,
    incoming: &[IMMessage],
    limit: u32,
    latest_conversation: Option<&Conversation>,
) -> Option<ConversationTimelineSnapshot> {
    let conversation_id = snapshot
        .conversation
        .as_ref()
        .map(|conversation| conversation.conversation_id.trim())
        .filter(|id| !id.is_empty())
        .or_else(|| {
            snapshot
                .messages
                .first()
                .map(|message| message.conversation_id.trim())
                .filter(|id| !id.is_empty())
        })?;
    let mut messages = snapshot.messages.clone();
    let original_len = messages.len();
    // key → index 一次构建：旧实现对每条 incoming 线性扫描且逐元素重算 timeline_key，
    // 窗口放大到 1 万条后每条新消息是 O(K×W) 次 String 构造。
    let mut index_by_key: HashMap<String, usize> = messages
        .iter()
        .enumerate()
        .map(|(index, message)| (message.timeline_key(), index))
        .collect();
    let mut changed = false;
    // 就地更新可能改变排序字段（如 ts 补齐）→ 走全量重排兜底；纯追加走有序合并快路径。
    let mut order_dirty = false;

    for message in incoming
        .iter()
        .filter(|message| message.conversation_id.trim() == conversation_id)
    {
        let key = message.timeline_key();
        if let Some(&index) = index_by_key.get(&key) {
            if json_changed(&messages[index], message) {
                messages[index] = message.clone();
                changed = true;
                if index < original_len {
                    order_dirty = true;
                }
            }
        } else {
            index_by_key.insert(key, messages.len());
            messages.push(message.clone());
            changed = true;
        }
    }

    if !changed {
        return None;
    }

    let limit = normalized_timeline_window_limit(limit);
    let sorted_prefix_intact = !order_dirty
        && messages[..original_len]
            .windows(2)
            .all(|pair| IMMessage::compare_for_timeline_asc(&pair[0], &pair[1]).is_le());
    if sorted_prefix_intact {
        // 常态路径（新消息追加）：现有窗口按快照不变式已 ASC 有序，只排 K 条新消息后
        // 两路有序合并——W log W 全排 → O(W+K)。
        let appended = messages.split_off(original_len);
        messages = merge_sorted_timeline(messages, appended);
    } else {
        // 兜底（就地更新/前缀失序）：全量 ASC 重排。
        messages.sort_by(IMMessage::compare_for_timeline_asc);
    }
    // 窗口截断 = 保留 ASC 尾部 limit 条（旧实现"desc 截断 + 回排 asc"的等价形式：
    // `compare_for_latest_window_desc` 是 asc 的严格反序）。
    let truncated = messages.len() > limit;
    if truncated {
        let overflow = messages.len() - limit;
        messages.drain(..overflow);
    }

    Some(ConversationTimelineSnapshot {
        conversation: latest_conversation
            .cloned()
            .or_else(|| snapshot.conversation.clone()),
        messages,
        has_more: snapshot.has_more || truncated,
    })
}

/// 两路有序合并（ASC；tie 时 existing 在前，与稳定全排序对 [existing..., appended...] 的结果一致）。
fn merge_sorted_timeline(existing: Vec<IMMessage>, mut appended: Vec<IMMessage>) -> Vec<IMMessage> {
    if appended.is_empty() {
        return existing;
    }
    appended.sort_by(IMMessage::compare_for_timeline_asc);
    let mut merged = Vec::with_capacity(existing.len() + appended.len());
    let mut left = existing.into_iter().peekable();
    let mut right = appended.into_iter().peekable();
    loop {
        match (left.peek(), right.peek()) {
            // 用 if let 取值而非 expect：本 crate 策略禁止生产路径出现
            // expect/unwrap/panic（见 lib.rs 顶部 deny）。peek 后 next() 必为
            // Some，但把这个「必然」写成不会 panic 的形式，代价为零。
            (Some(l), Some(r)) => {
                let take_left = IMMessage::compare_for_timeline_asc(l, r).is_le();
                let taken = if take_left { left.next() } else { right.next() };
                if let Some(v) = taken {
                    merged.push(v);
                }
            }
            (Some(_), None) => {
                if let Some(v) = left.next() {
                    merged.push(v);
                }
            }
            (None, Some(_)) => {
                if let Some(v) = right.next() {
                    merged.push(v);
                }
            }
            (None, None) => break,
        }
    }
    merged
}

fn normalized_timeline_window_limit(limit: u32) -> usize {
    (limit as usize).clamp(1, MAX_TIMELINE_WINDOW_MESSAGES)
}

fn oldest_positive_seq(snapshot: &ConversationTimelineSnapshot) -> Option<u64> {
    snapshot
        .messages
        .iter()
        .filter_map(|message| (message.conversation_seq > 0).then_some(message.conversation_seq))
        .min()
}

/// 往旧翻到的一页（由新到旧）与它之下还有没有可见消息。
struct OlderPage {
    messages: Vec<IMMessage>,
    has_older: bool,
}

/// 一次往旧翻页最多回填几次：正常一次就够（服务端回满一页或到达起点），多的只是兜底。
const MAX_BACKFILLS_PER_PAGE: usize = 3;

fn expand_timeline_snapshot(
    snapshot: &ConversationTimelineSnapshot,
    older: &[IMMessage],
    has_older: bool,
    max_window: usize,
) -> (ConversationTimelineSnapshot, u32) {
    let mut messages = snapshot.messages.clone();
    let mut keys = messages
        .iter()
        .map(|message| message.timeline_key())
        .collect::<HashSet<_>>();
    let mut loaded_count = 0_u32;
    for message in older {
        if keys.insert(message.timeline_key()) {
            messages.push(message.clone());
            loaded_count += 1;
        }
    }
    messages.sort_by(IMMessage::compare_for_latest_window_desc);
    let truncated = messages.len() > max_window;
    messages.truncate(max_window);
    messages.sort_by(IMMessage::compare_for_timeline_asc);
    (
        ConversationTimelineSnapshot {
            conversation: snapshot.conversation.clone(),
            messages,
            has_more: has_older || truncated,
        },
        loaded_count,
    )
}

fn timeline_view_update(
    view_id: &str,
    old: &ConversationTimelineSnapshot,
    new: &ConversationTimelineSnapshot,
) -> Option<ViewUpdate> {
    let Some(ops) = keyed_delta_ops(&old.messages, &new.messages, |message| {
        message.timeline_key().trim().to_string()
    }) else {
        return Some(snapshot_update(
            view_id,
            ViewSnapshot::Timeline(new.clone()),
        ));
    };
    let side_changed =
        json_changed(&old.conversation, &new.conversation) || old.has_more != new.has_more;
    if ops.is_empty() && !side_changed {
        return None;
    }
    Some(ViewUpdate {
        view_id: view_id.to_string(),
        kind: ViewUpdateKind::Delta,
        snapshot: None,
        delta: Some(ViewDelta::Timeline {
            ops,
            conversation: new.conversation.clone(),
            has_more: new.has_more,
        }),
    })
}

fn conversation_list_view_update(
    view_id: &str,
    old: &HomeTimelineSnapshot,
    new: &HomeTimelineSnapshot,
) -> Option<ViewUpdate> {
    let Some(ops) = keyed_delta_ops(&old.conversations, &new.conversations, |conversation| {
        conversation.conversation_id.trim().to_string()
    }) else {
        return Some(snapshot_update(
            view_id,
            ViewSnapshot::ConversationList(new.clone()),
        ));
    };
    let side_changed =
        old.total_unread != new.total_unread || json_changed(&old.sync_state, &new.sync_state);
    if ops.is_empty() && !side_changed {
        return None;
    }
    Some(ViewUpdate {
        view_id: view_id.to_string(),
        kind: ViewUpdateKind::Delta,
        snapshot: None,
        delta: Some(ViewDelta::ConversationList {
            ops,
            total_unread: new.total_unread,
            sync_state: new.sync_state.clone(),
        }),
    })
}

fn snapshot_update(view_id: &str, snapshot: ViewSnapshot) -> ViewUpdate {
    ViewUpdate {
        view_id: view_id.to_string(),
        kind: ViewUpdateKind::Snapshot,
        snapshot: Some(snapshot),
        delta: None,
    }
}

fn keyed_delta_ops<T, K>(old: &[T], new: &[T], key: K) -> Option<Vec<ViewDeltaOp>>
where
    T: Serialize,
    K: Fn(&T) -> String,
{
    let mut old_by_key: HashMap<String, (usize, Value)> = HashMap::with_capacity(old.len());
    for (index, item) in old.iter().enumerate() {
        let key = key(item);
        if key.is_empty() {
            return None;
        }
        let value = serde_json::to_value(item).ok()?;
        old_by_key.insert(key, (index, value));
    }

    let mut new_by_key = HashSet::with_capacity(new.len());
    let mut ops = Vec::new();
    for (index, item) in new.iter().enumerate() {
        let key = key(item);
        if key.is_empty() {
            return None;
        }
        let value = serde_json::to_value(item).ok()?;
        new_by_key.insert(key.clone());
        match old_by_key.get(&key) {
            None => ops.push(ViewDeltaOp {
                op: ViewDeltaKind::Insert,
                key,
                index: index as u32,
                from_index: None,
                item: Some(value),
            }),
            Some((old_index, old_value)) => {
                if *old_index != index {
                    ops.push(ViewDeltaOp {
                        op: ViewDeltaKind::Move,
                        key: key.clone(),
                        index: index as u32,
                        from_index: Some(*old_index as u32),
                        item: None,
                    });
                }
                if old_value != &value {
                    ops.push(ViewDeltaOp {
                        op: ViewDeltaKind::Update,
                        key,
                        index: index as u32,
                        from_index: None,
                        item: Some(value),
                    });
                }
            }
        }
    }

    for (key, (index, _)) in old_by_key {
        if !new_by_key.contains(&key) {
            ops.push(ViewDeltaOp {
                op: ViewDeltaKind::Remove,
                key,
                index: index as u32,
                from_index: None,
                item: None,
            });
        }
    }

    Some(ops)
}

fn json_changed<T: Serialize>(left: &T, right: &T) -> bool {
    serde_json::to_value(left).ok() != serde_json::to_value(right).ok()
}

#[cfg(test)]
mod tests {
    use flare_proto::common::{Message as ProtoMessage, SendAck};

    use super::{
        ForegroundPullGuard, ViewRefreshTarget, ViewRegistrations, expand_timeline_snapshot,
        hot_timeline_snapshot, oldest_positive_seq, timeline_view_update, view_refresh_plan,
        view_refresh_target,
    };
    use crate::kernel::event::{MessageEvent, SdkEvent, SyncNotify, SyncPhase};
    use crate::kernel::{SyncReason, SyncRunContext, SyncScope, SyncTrigger, SyncVisibility};
    use crate::model::{
        Conversation, ConversationTimelineSnapshot, IMMessage, OpenTimelineViewRequest, ViewDelta,
        ViewDeltaKind, ViewUpdateKind,
    };

    #[test]
    fn foreground_pull_guard_dedups_in_flight_and_releases_on_finish() {
        // I5：同会话在途去重；完成后释放可再次补拉；不同会话互不影响。
        let guard = ForegroundPullGuard::default();
        assert!(guard.try_begin("c1"));
        assert!(!guard.try_begin("c1"), "in-flight pull must dedupe");
        assert!(guard.try_begin("c2"), "other conversations are independent");
        guard.finish("c1");
        assert!(guard.try_begin("c1"), "finished pull frees the slot");
    }

    #[test]
    fn message_batch_refresh_plan_uses_hot_messages_without_timeline_query() {
        let event = SdkEvent::Message(MessageEvent::ReceivedBatch {
            messages: vec![
                IMMessage::new(ProtoMessage {
                    conversation_id: "c1".into(),
                    ..Default::default()
                }),
                IMMessage::new(ProtoMessage {
                    conversation_id: "c2".into(),
                    ..Default::default()
                }),
                IMMessage::new(ProtoMessage {
                    conversation_id: "c1".into(),
                    ..Default::default()
                }),
            ],
        });
        let plan = view_refresh_plan(&event).expect("message event refreshes views");
        assert!(plan.timeline_query.is_none());
        assert!(plan.refresh_conversation_lists);
        assert_eq!(plan.hot_messages.len(), 3);
    }

    #[test]
    fn send_ack_refresh_uses_ack_conversation_id() {
        let event = SdkEvent::Message(MessageEvent::SendAck {
            ack: Box::new(SendAck {
                conversation_id: "c1".into(),
                ..Default::default()
            }),
        });
        let target = view_refresh_target(&event).expect("ack refreshes views");
        assert!(target.matches_timeline("c1"));
        assert!(!target.matches_timeline("c2"));
    }

    #[test]
    fn sync_finish_refreshes_all_views() {
        let event = SdkEvent::Sync(SyncNotify::Finished {
            run: SyncRunContext::new(
                SyncTrigger::Manual,
                SyncScope::Global,
                SyncVisibility::Silent,
                SyncReason::UserRequested,
            ),
            phase: SyncPhase::Background,
        });
        assert_eq!(view_refresh_target(&event), Some(ViewRefreshTarget::All));
    }

    #[test]
    fn refresh_target_merge_unions_conversation_targets() {
        let mut target = ViewRefreshTarget::Conversations(["c1".to_string()].into_iter().collect());

        target.merge(ViewRefreshTarget::Conversations(
            ["c1".to_string(), "c2".to_string()].into_iter().collect(),
        ));

        assert!(target.matches_timeline("c1"));
        assert!(target.matches_timeline("c2"));
        assert!(!target.matches_timeline("c3"));
    }

    #[test]
    fn refresh_target_merge_all_dominates() {
        let mut target = ViewRefreshTarget::Conversations(["c1".to_string()].into_iter().collect());
        target.merge(ViewRefreshTarget::All);
        assert_eq!(target, ViewRefreshTarget::All);

        target.merge(ViewRefreshTarget::Conversations(
            ["c2".to_string()].into_iter().collect(),
        ));
        assert_eq!(target, ViewRefreshTarget::All);
    }

    #[test]
    fn timeline_registry_replaces_same_conversation_and_caps_oldest() {
        let mut views = ViewRegistrations::default();
        for index in 0..9 {
            views.insert_timeline(
                format!("timeline:{index}"),
                OpenTimelineViewRequest {
                    conversation_id: format!("c{index}"),
                    message_limit: 50,
                },
                index,
                empty_timeline_snapshot(),
            );
        }

        assert_eq!(views.timelines.len(), 8);
        assert!(!views.timelines.contains_key("timeline:0"));

        views.insert_timeline(
            "timeline:replacement".to_string(),
            OpenTimelineViewRequest {
                conversation_id: "c8".to_string(),
                message_limit: 50,
            },
            10,
            empty_timeline_snapshot(),
        );

        assert_eq!(views.timelines.len(), 8);
        assert!(!views.timelines.contains_key("timeline:8"));
        assert!(views.timelines.contains_key("timeline:replacement"));
    }

    #[test]
    fn timeline_delta_inserts_only_new_message() {
        let old = ConversationTimelineSnapshot {
            conversation: None,
            messages: vec![message("c1", "m1", 1)],
            has_more: false,
        };
        let new = ConversationTimelineSnapshot {
            conversation: None,
            messages: vec![message("c1", "m1", 1), message("c1", "m2", 2)],
            has_more: false,
        };

        let Some(update) = timeline_view_update("timeline:1", &old, &new) else {
            panic!("new message should produce a view update");
        };
        assert_eq!(update.kind, ViewUpdateKind::Delta);
        assert!(update.snapshot.is_none());
        let Some(ViewDelta::Timeline { ops, .. }) = update.delta else {
            panic!("timeline delta expected");
        };
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].op, ViewDeltaKind::Insert);
        assert_eq!(ops[0].key, "client:m2");
        assert_eq!(ops[0].index, 1);
        assert!(ops[0].item.is_some());
    }

    #[test]
    fn plugin_state_change_reloads_its_timeline_and_updates_the_card_row() {
        let event = SdkEvent::Message(MessageEvent::PluginStateChanged {
            conversation_id: "c1".into(),
            message_id: "m1".into(),
            version: 2,
        });
        let plan = view_refresh_plan(&event).expect("plugin state refreshes views");
        let target = plan.timeline_query.expect("timeline is reloaded");
        assert!(target.matches_timeline("c1"));
        assert!(!target.matches_timeline("c2"));

        let card = message("c1", "m1", 1);
        let mut with_state = card.clone();
        with_state
            .attributes
            .insert("pluginStateVersion".into(), "2".into());
        let snapshot = |messages| ConversationTimelineSnapshot {
            conversation: None,
            messages,
            has_more: false,
        };
        let update = timeline_view_update(
            "timeline:1",
            &snapshot(vec![card, message("c1", "m2", 2)]),
            &snapshot(vec![with_state, message("c1", "m2", 2)]),
        )
        .expect("an attribute-only change still updates the row");
        let Some(ViewDelta::Timeline { ops, .. }) = update.delta else {
            panic!("timeline delta expected");
        };
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].op, ViewDeltaKind::Update);
        assert_eq!(ops[0].key, "client:m1");
    }

    #[test]
    fn hot_timeline_snapshot_keeps_latest_window_order_and_limit() {
        let old = ConversationTimelineSnapshot {
            conversation: None,
            messages: vec![message("c1", "m2", 2), message("c1", "m1", 1)],
            has_more: false,
        };

        let new = hot_timeline_snapshot(&old, &[message("c1", "m3", 3)], 2, None)
            .expect("new message should update hot timeline");

        assert_eq!(new.messages.len(), 2);
        assert_eq!(new.messages[0].timeline_key(), "client:m2");
        assert_eq!(new.messages[1].timeline_key(), "client:m3");
        assert!(new.has_more);
    }

    #[test]
    fn hot_timeline_snapshot_merges_out_of_order_incoming_into_sorted_window() {
        // 常态合并路径：已 ASC 有序的窗口 + 乱序 incoming（含中间插入）→ 与全量重排等价。
        let old = ConversationTimelineSnapshot {
            conversation: None,
            messages: vec![message("c1", "m1", 1), message("c1", "m3", 3)],
            has_more: false,
        };

        let new = hot_timeline_snapshot(
            &old,
            &[message("c1", "m4", 4), message("c1", "m2", 2)],
            10,
            None,
        )
        .expect("incoming should update hot timeline");

        let keys: Vec<String> = new
            .messages
            .iter()
            .map(|message| message.timeline_key())
            .collect();
        assert_eq!(
            keys,
            vec!["client:m1", "client:m2", "client:m3", "client:m4"]
        );
        assert!(!new.has_more, "window not truncated at limit 10");
    }

    #[test]
    fn hot_timeline_snapshot_carries_latest_conversation_projection() {
        let old = ConversationTimelineSnapshot {
            conversation: Some(Conversation {
                conversation_id: "c1".to_string(),
                last_message_preview: None,
                ..Default::default()
            }),
            messages: vec![message("c1", "m1", 1)],
            has_more: false,
        };
        let latest = Conversation {
            conversation_id: "c1".to_string(),
            last_message_preview: Some("hello".to_string()),
            ..Default::default()
        };

        let new = hot_timeline_snapshot(&old, &[message("c1", "m2", 2)], 50, Some(&latest))
            .expect("new message should update hot timeline");

        assert_eq!(
            new.conversation
                .as_ref()
                .and_then(|conversation| conversation.last_message_preview.as_deref()),
            Some("hello")
        );
    }

    #[test]
    fn hot_timeline_snapshot_preserves_expanded_history_window() {
        let old = ConversationTimelineSnapshot {
            conversation: None,
            messages: (1..=120)
                .map(|seq| message("c1", &format!("m{seq}"), seq))
                .collect(),
            has_more: true,
        };

        let new = hot_timeline_snapshot(&old, &[message("c1", "m121", 121)], 120, None)
            .expect("new message should update hot timeline");

        assert_eq!(new.messages.len(), 120);
        assert_eq!(new.messages[0].timeline_key(), "client:m2");
        assert_eq!(new.messages[119].timeline_key(), "client:m121");
        assert!(new.has_more);
    }

    #[test]
    fn expand_timeline_snapshot_appends_older_page_without_duplicates() {
        let old = ConversationTimelineSnapshot {
            conversation: None,
            messages: vec![message("c1", "m3", 3), message("c1", "m2", 2)],
            has_more: true,
        };

        let (new, loaded_count) = expand_timeline_snapshot(
            &old,
            &[message("c1", "m2", 2), message("c1", "m1", 1)],
            false,
            500,
        );

        assert_eq!(loaded_count, 1);
        assert_eq!(
            new.messages
                .iter()
                .map(IMMessage::timeline_key)
                .collect::<Vec<_>>(),
            vec!["client:m1", "client:m2", "client:m3"]
        );
        assert!(!new.has_more);
    }

    #[test]
    fn oldest_positive_seq_ignores_local_pending_messages() {
        let snapshot = ConversationTimelineSnapshot {
            conversation: None,
            messages: vec![message("c1", "pending", 0), message("c1", "m4", 4)],
            has_more: true,
        };

        assert_eq!(oldest_positive_seq(&snapshot), Some(4));
    }

    fn empty_timeline_snapshot() -> ConversationTimelineSnapshot {
        ConversationTimelineSnapshot {
            conversation: None,
            messages: Vec::new(),
            has_more: false,
        }
    }

    fn message(conversation_id: &str, client_msg_id: &str, seq: u64) -> IMMessage {
        IMMessage::new(ProtoMessage {
            conversation_id: conversation_id.into(),
            client_msg_id: client_msg_id.into(),
            conversation_seq: seq,
            ..Default::default()
        })
    }
}

#[cfg(test)]
#[path = "view_paging_tests.rs"]
mod paging_tests;
