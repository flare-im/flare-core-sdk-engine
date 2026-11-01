//! 核心可观察视图：时间线与会话列表。
//!
//! 时间线视图持有窗口（哪些行、顺序、往旧往新还有没有、是否接在最新），决定何时读本地、何时
//! 向服务端回填、何时跨不过缺口，并推出可以照做的增量。所有更新只经 `view.updated` 事件发出，
//! 每次更新视图的版本号加 1；op 的返回值只说「包含这一页的是版本几」。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc::error::TryRecvError};

use crate::client::api::ConversationApi;
use crate::kernel::event::{
    ConversationEvent, EventBus, MessageEvent, NameChange, SdkEvent, SyncNotify,
};
use crate::kernel::{AttentionRegistry, SessionSyncRunner};
use crate::model::{
    BootstrapHomeTimelineRequest, CloseViewRequest, CloseViewResponse, Conversation,
    HomeTimelineSnapshot, IMMessage, LoadNewerTimelineViewRequest, LoadOlderTimelineViewRequest,
    OpenConversationListViewRequest, OpenConversationTimelineRequest, OpenTimelineViewRequest,
    TimelineAnchor, TimelineViewPageResponse, TimelineViewRequest, ViewDelta, ViewDeltaKind,
    ViewDeltaOp, ViewOpenResponse, ViewSnapshot, ViewUpdate, ViewUpdateKind,
    normalized_conversation_limit, normalized_message_limit,
};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::{BackgroundTask, delay, spawn_background, spawn_background_task};

#[path = "view_window.rs"]
mod window;

use window::{
    CompareRows, MAX_TIMELINE_WINDOW_MESSAGES, TimelineChange, TimelineWindow, append_newer,
    apply_reread_rows, json_changed, merge_live, prepend_older, timeline_change,
};

const VIEW_REFRESH_DEBOUNCE: Duration = Duration::from_millis(40);
const MAX_TIMELINE_VIEWS: usize = 8;
const MAX_CONVERSATION_LIST_VIEWS: usize = 2;

/// 一次往旧或往新翻页最多向服务端要几次：正常一次就够（服务端回满一页或到达端点），多的只是兜底。
const MAX_FETCHES_PER_PAGE: usize = 3;

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
    /// 页大小（打开时的 `messageLimit`，往旧往新翻页没给时也用它）。
    page_size: u32,
    order: u64,
    revision: u64,
    window: TimelineWindow,
}

struct ConversationListViewRegistration {
    request: OpenConversationListViewRequest,
    order: u64,
    revision: u64,
    last_snapshot: HomeTimelineSnapshot,
}

impl ViewRegistrations {
    /// 同一会话打开两个视图时，后打开的替换先打开的。
    fn insert_timeline(&mut self, view_id: String, registration: TimelineViewRegistration) {
        let conversation_id = registration.window.conversation_id.clone();
        self.timelines
            .retain(|_, view| view.window.conversation_id != conversation_id);
        self.timelines.insert(view_id, registration);
        evict_oldest(&mut self.timelines, MAX_TIMELINE_VIEWS);
    }

    fn insert_conversation_list(
        &mut self,
        view_id: String,
        registration: ConversationListViewRegistration,
    ) {
        self.conversation_lists.insert(view_id, registration);
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

/// 往旧或往新翻到的一页，与翻页途中回填 / 向新取页失败时的错误。
///
/// 失败前已经读到的行都在连续区间内（区间证明了它们与窗口之间没有别的消息），先并入窗口，
/// 再在缺口处把错误交给宿主（Kit 显示 `olderError` / `newerError` 与重试）。
struct PageRead {
    messages: Vec<IMMessage>,
    /// 往旧：窗口之下还有没有；往新不用。
    has_older: bool,
    /// 往新：接上了最新（`messages` 之后再并入 `head` 里的头部未证实行与待发送行）。
    reached_latest: bool,
    head: Vec<IMMessage>,
    error: Option<FlareError>,
}

impl PageRead {
    fn older(messages: Vec<IMMessage>, has_older: bool) -> Self {
        Self {
            messages,
            has_older,
            reached_latest: false,
            head: Vec::new(),
            error: None,
        }
    }

    fn newer(messages: Vec<IMMessage>) -> Self {
        Self {
            messages,
            has_older: true,
            reached_latest: false,
            head: Vec::new(),
            error: None,
        }
    }

    fn failed(mut self, error: FlareError) -> Self {
        self.error = Some(error);
        self
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

    /// 打开时间线视图。不带 `anchor` 时是最新窗口；带 `anchor` 时围绕目标消息打开
    /// （目标之前、之后各取半页），快照的 `anchorKey` 是目标行的行键。
    pub async fn open_timeline(
        &self,
        request: OpenTimelineViewRequest,
    ) -> Result<ViewOpenResponse> {
        let conversation_id = request.conversation_id.trim().to_string();
        let page_size = normalized_message_limit(request.message_limit);
        // 注意力信号：打开的会话升为前台，后续收敛优先补齐它（秒展示）。app 打开会话即自动触发。
        // I5：同时立即定向补拉该会话（P0 中断，不等下一轮收敛）——本地快照仍即时返回不被网络阻塞。
        if !conversation_id.is_empty() {
            self.inner.attention.open_timeline(&conversation_id);
            self.spawn_foreground_pull(&conversation_id);
        }
        let window = match request.anchor.filter(anchor_is_set) {
            Some(anchor) => {
                self.window_around(&conversation_id, page_size, &anchor)
                    .await?
            }
            None => self.latest_window(&conversation_id, page_size).await?,
        };
        let snapshot = window.snapshot();
        let (view_id, order) = self.next_view_id("timeline");
        self.inner.views.write().await.insert_timeline(
            view_id.clone(),
            TimelineViewRegistration {
                page_size,
                order,
                revision: 1,
                window,
            },
        );
        Ok(ViewOpenResponse {
            view_id,
            revision: 1,
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

    /// 最新窗口：比最高已证实 seq 新的行与待发送行照收，其余的行必须落在接着最新的那段区间里。
    async fn latest_window(&self, conversation_id: &str, page_size: u32) -> Result<TimelineWindow> {
        let snapshot = self
            .inner
            .conversation_api
            .open_timeline(OpenConversationTimelineRequest {
                conversation_id: conversation_id.to_string(),
                message_limit: page_size,
            })
            .await?;
        Ok(TimelineWindow {
            conversation_id: conversation_id.to_string(),
            conversation: snapshot.conversation,
            messages: snapshot.messages,
            has_older: snapshot.has_more,
            attached_to_latest: true,
            anchor_key: None,
        })
    }

    /// 围绕目标消息的窗口。
    ///
    /// - 目标 seq：`messageId` 在本地找得到用本地的 seq，否则用调用方给的 `seq`；两者都没有时
    ///   `sdk.view.anchor_not_found`。目标是待发送行时它就在最新窗口里。
    /// - 本地 `[S - 半页, S + 半页]` 已被区间覆盖：直接读本地，不请求。
    /// - 否则并发发两个请求：以 `before = S + 1` 回填半页（证明 `[m, S]`），以 `after = S` 取
    ///   半页向新的同步页（证明 `[S + 1, M]`，不推进同步游标）。
    /// - 窗口新的一端接上最高已证实 seq 时接在最新，否则 `hasNewer`。
    async fn window_around(
        &self,
        conversation_id: &str,
        page_size: u32,
        anchor: &TimelineAnchor,
    ) -> Result<TimelineWindow> {
        let api = &self.inner.conversation_api;
        let found = match anchor
            .message_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            Some(message_id) => api
                .find_message(message_id)
                .await?
                .filter(|message| message.conversation_id.trim() == conversation_id),
            None => None,
        };
        let target_seq = match &found {
            Some(message) if message.conversation_seq > 0 => message.conversation_seq,
            Some(pending) => {
                let mut window = self.latest_window(conversation_id, page_size).await?;
                let key = pending.row_key();
                window.anchor_key = window
                    .messages
                    .iter()
                    .any(|message| message.row_key() == key)
                    .then_some(key);
                return Ok(window);
            }
            None => match anchor.seq.filter(|seq| *seq > 0) {
                Some(seq) => seq,
                None => {
                    return Err(FlareError::localized(
                        ErrorCode::MessageNotFound,
                        "sdk.view.anchor_not_found",
                    ));
                }
            },
        };

        let half = (page_size as u64 / 2).max(1);
        let (_, coverage) = api.timeline_paging_state(conversation_id).await?;
        let covered = coverage.range_containing(target_seq).is_some_and(|range| {
            range.from <= target_seq.saturating_sub(half).max(1)
                && (range.to >= target_seq.saturating_add(half)
                    || range.to >= coverage.highest_proven_seq())
        });
        if !covered {
            let Some(sync) = self.inner.session_sync.as_ref() else {
                return Err(FlareError::localized(ErrorCode::NotConnected, "未配置同步"));
            };
            let limit = i32::try_from(half).unwrap_or(i32::MAX);
            let (older, newer) = futures::future::join(
                sync.request_message_backfill_before_seq(
                    conversation_id,
                    target_seq.saturating_add(1),
                    limit,
                ),
                sync.request_message_page_after_seq_detached(conversation_id, target_seq, limit),
            )
            .await;
            older?;
            newer?;
        }

        let (conversation, coverage) = api.timeline_paging_state(conversation_id).await?;
        let half = half as usize;
        let (older, rows_below) = match coverage.range_containing(target_seq) {
            Some(range) => {
                api.timeline_rows_in_range(
                    conversation_id,
                    range.from,
                    target_seq.saturating_add(1),
                    half,
                )
                .await?
            }
            // 服务端没有证明目标所在的那一段：只放目标本身，两头都按还有。
            None => {
                let (rows, _) = api
                    .timeline_rows_in_range(
                        conversation_id,
                        target_seq,
                        target_seq.saturating_add(1),
                        1,
                    )
                    .await?;
                (rows, true)
            }
        };
        let highest = coverage.highest_proven_seq();
        let (newer, rows_above, reaches_top) = match coverage.range_containing(target_seq + 1) {
            Some(range) => {
                let (rows, more) = api
                    .timeline_rows_after(conversation_id, target_seq, range.to, half)
                    .await?;
                (rows, more, range.to >= highest)
            }
            None => (Vec::new(), false, target_seq >= highest),
        };
        let attached = !rows_above && reaches_top;
        let mut messages = older;
        messages.reverse();
        messages.extend(newer);
        let mut window = TimelineWindow {
            conversation_id: conversation_id.to_string(),
            conversation,
            messages,
            has_older: true,
            attached_to_latest: false,
            anchor_key: None,
        };
        let oldest = window.oldest_seq().unwrap_or(target_seq.saturating_add(1));
        window.has_older = coverage.has_older(oldest, rows_below);
        if attached {
            let top = window.newest_seq().unwrap_or(target_seq);
            let head = self.head_rows(conversation_id, top).await?;
            append_newer(&mut window, &head);
            window.attached_to_latest = true;
        }
        window.anchor_key = window
            .messages
            .iter()
            .find(|message| match &found {
                Some(target) => message.row_key() == target.row_key(),
                None => message.conversation_seq == target_seq,
            })
            .map(IMMessage::row_key);
        window.trim_older_end(MAX_TIMELINE_WINDOW_MESSAGES);
        Ok(window)
    }

    /// 接上最新时窗口新的一端还要并入的行：`top` 之上的头部未证实行与待发送行。
    async fn head_rows(&self, conversation_id: &str, top: u64) -> Result<Vec<IMMessage>> {
        Ok(self
            .inner
            .conversation_api
            .reload_timeline_window(
                conversation_id,
                top.saturating_add(1),
                MAX_TIMELINE_WINDOW_MESSAGES,
            )
            .await?
            .messages)
    }

    /// 往旧翻一页。窗口最旧一行为 `O`、页大小 `L`：
    ///
    /// 1. `O-1` 落在某段区间 `[r, ..]` 里：读本地 seq 在 `[r, O-1]` 内、由新到旧的 `L` 行；
    ///    不够 `L` 行而 `r = 1` 就是到了起点；`r > 1` 则以 `before = r` 回填缺的行数，区间扩展后
    ///    在新的区间内接着读。
    /// 2. `O-1` 不在任何区间里（窗口紧挨着一个洞）：以 `before = O` 回填，写入区间后再读本地。
    /// 3. 回填失败（离线、超时、服务端错误）：先把区间内已经读到的连续行并入窗口，再返回错误。
    ///    这些行由区间证明与窗口连续，不算越过缺口；缺口之下的本地行不读。
    /// 4. 窗口里没有已发送的行：以会话已知最新 seq 之上一位为 `O`。
    ///
    /// 并入的行只经 `view.updated` 推出；返回值只有并入了几行与包含这一页的版本号。
    /// 往旧翻超过窗口上限时从新的一端裁掉，窗口随之脱离最新。
    pub async fn load_older_timeline(
        &self,
        request: LoadOlderTimelineViewRequest,
    ) -> Result<TimelineViewPageResponse> {
        let view_id = required_view_id(&request.view_id)?;
        let page_limit = normalized_message_limit(request.message_limit) as usize;
        // view 已关闭（用户切走会话 / silent_refresh 替换了 view）是无害竞态：
        // 加载更多历史对已关闭的 timeline 无意义，静默返回而非报错打断 UI。
        let Some((conversation_id, oldest_seq)) = self
            .with_timeline(&view_id, |view| {
                (
                    view.window.conversation_id.clone(),
                    view.window.oldest_seq(),
                )
            })
            .await
        else {
            return Ok(closed_page(view_id));
        };

        let page = self
            .read_older_page(&conversation_id, oldest_seq, page_limit)
            .await?;

        let (loaded_count, revision, update) = {
            let mut views = self.inner.views.write().await;
            // 网络往返期间 view 被关闭同属无害竞态（拉到的历史丢弃即可，用户已切走）。
            let Some(registration) = views.timelines.get_mut(&view_id) else {
                return Ok(closed_page(view_id));
            };
            // 窗口在往返期间被整体换掉（回到最新、清空记录）：这一页接不到新窗口下面。
            if registration.window.oldest_seq() != oldest_seq {
                (0, registration.revision, None)
            } else {
                let mut next = registration.window.clone();
                let loaded = prepend_older(&mut next, &page.messages);
                next.has_older = page.has_older;
                next.trim_newer_end(MAX_TIMELINE_WINDOW_MESSAGES);
                let update = commit_timeline(&view_id, registration, next, CompareRows::None);
                (loaded, registration.revision, update)
            }
        };
        self.publish(update);
        if let Some(error) = page.error {
            return Err(error);
        }
        Ok(TimelineViewPageResponse {
            view_id,
            loaded_count,
            revision,
        })
    }

    /// 读窗口之下的一页（规则见 [`Self::load_older_timeline`]）。只读区间内的本地行；
    /// 碰到区间边界就回填；回填失败时带上已经读到的行与错误返回。
    async fn read_older_page(
        &self,
        conversation_id: &str,
        oldest_seq: Option<u64>,
        limit: usize,
    ) -> Result<PageRead> {
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
                return Ok(PageRead::older(messages, false));
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
                        return Ok(PageRead::older(messages, coverage.has_older(oldest, more)));
                    }
                    range.from
                }
                None => lowest,
            };
            if backfills == MAX_FETCHES_PER_PAGE {
                return Ok(PageRead::older(messages, true));
            }
            backfills += 1;
            if let Err(error) = self
                .backfill_before(conversation_id, before, limit - messages.len())
                .await
            {
                return Ok(PageRead::older(messages, true).failed(error));
            }
            let (_, extended) = api.timeline_paging_state(conversation_id).await?;
            let progressed = extended.range_containing(before - 1).is_some();
            coverage = extended;
            if !progressed {
                // 服务端没有证明任何东西（不该发生）：不再重复请求，留给下一次往旧翻页。
                return Ok(PageRead::older(messages, true));
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

    /// 往新翻一页，与 [`Self::load_older_timeline`] 对称：在覆盖 `N+1` 的区间内读本地更新的行，
    /// 碰到区间边界就取一页向新的同步页（不推进同步游标）；窗口新的一端接上最高已证实 seq 时
    /// 接在最新，此后实时消息追加进来。取页失败时先并入已经读到的连续行，再返回错误。
    /// 已经接在最新时什么也不做。往新翻超过窗口上限时从旧的一端裁掉。
    pub async fn load_newer_timeline(
        &self,
        request: LoadNewerTimelineViewRequest,
    ) -> Result<TimelineViewPageResponse> {
        let view_id = required_view_id(&request.view_id)?;
        let page_limit = normalized_message_limit(request.message_limit) as usize;
        let Some((conversation_id, newest_seq, attached, revision, page_size)) = self
            .with_timeline(&view_id, |view| {
                (
                    view.window.conversation_id.clone(),
                    view.window.newest_seq(),
                    view.window.attached_to_latest,
                    view.revision,
                    view.page_size,
                )
            })
            .await
        else {
            return Ok(closed_page(view_id));
        };
        if attached {
            return Ok(TimelineViewPageResponse {
                view_id,
                loaded_count: 0,
                revision,
            });
        }
        let Some(newest_seq) = newest_seq else {
            // 脱离最新却没有已发送的行（不该出现）：直接回到最新。
            return self
                .jump_to_latest_inner(view_id, &conversation_id, page_size)
                .await;
        };

        let page = self
            .read_newer_page(&conversation_id, newest_seq, page_limit)
            .await?;

        let (loaded_count, revision, update) = {
            let mut views = self.inner.views.write().await;
            let Some(registration) = views.timelines.get_mut(&view_id) else {
                return Ok(closed_page(view_id));
            };
            if registration.window.attached_to_latest
                || registration.window.newest_seq() != Some(newest_seq)
            {
                (0, registration.revision, None)
            } else {
                let mut next = registration.window.clone();
                let mut loaded = append_newer(&mut next, &page.messages);
                if page.reached_latest {
                    loaded += append_newer(&mut next, &page.head);
                    next.attached_to_latest = true;
                }
                next.trim_older_end(MAX_TIMELINE_WINDOW_MESSAGES);
                let update = commit_timeline(&view_id, registration, next, CompareRows::None);
                (loaded, registration.revision, update)
            }
        };
        self.publish(update);
        if let Some(error) = page.error {
            return Err(error);
        }
        Ok(TimelineViewPageResponse {
            view_id,
            loaded_count,
            revision,
        })
    }

    /// 读窗口之上的一页（规则见 [`Self::load_newer_timeline`]）。
    async fn read_newer_page(
        &self,
        conversation_id: &str,
        newest_seq: u64,
        limit: usize,
    ) -> Result<PageRead> {
        let api = &self.inner.conversation_api;
        let (_, mut coverage) = api.timeline_paging_state(conversation_id).await?;
        let mut messages: Vec<IMMessage> = Vec::with_capacity(limit);
        let mut fetches = 0;
        loop {
            let top = messages
                .last()
                .map(|message| message.conversation_seq)
                .unwrap_or(newest_seq);
            let highest = coverage.highest_proven_seq();
            let after = match coverage.range_containing(top.saturating_add(1)) {
                Some(range) => {
                    let (rows, more) = api
                        .timeline_rows_after(conversation_id, top, range.to, limit - messages.len())
                        .await?;
                    messages.extend(rows);
                    if !more && range.to >= highest {
                        return self.reach_latest(conversation_id, messages).await;
                    }
                    if messages.len() >= limit {
                        return Ok(PageRead::newer(messages));
                    }
                    range.to
                }
                // 已在最高已证实 seq 处：之上只有头部未证实行。
                None if top >= highest => {
                    return self.reach_latest(conversation_id, messages).await;
                }
                None => top,
            };
            if fetches == MAX_FETCHES_PER_PAGE {
                return Ok(PageRead::newer(messages));
            }
            fetches += 1;
            if let Err(error) = self
                .fetch_after(conversation_id, after, limit - messages.len())
                .await
            {
                return Ok(PageRead::newer(messages).failed(error));
            }
            let (_, extended) = api.timeline_paging_state(conversation_id).await?;
            let progressed = extended.range_containing(after.saturating_add(1)).is_some();
            coverage = extended;
            if !progressed {
                return Ok(PageRead::newer(messages));
            }
        }
    }

    async fn reach_latest(
        &self,
        conversation_id: &str,
        messages: Vec<IMMessage>,
    ) -> Result<PageRead> {
        let top = messages
            .iter()
            .map(|message| message.conversation_seq)
            .max()
            .unwrap_or(0);
        let mut page = PageRead::newer(messages);
        page.head = self.head_rows(conversation_id, top).await?;
        page.reached_latest = true;
        Ok(page)
    }

    async fn fetch_after(&self, conversation_id: &str, after_seq: u64, limit: usize) -> Result<()> {
        let Some(sync) = &self.inner.session_sync else {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未配置同步"));
        };
        sync.request_message_page_after_seq_detached(
            conversation_id,
            after_seq,
            limit.max(1) as i32,
        )
        .await
        .map(|_| ())
    }

    /// 窗口直接换成最新窗口（与不带锚点的打开相同），版本号递增并以快照推出。
    pub async fn jump_to_latest_timeline(
        &self,
        request: TimelineViewRequest,
    ) -> Result<TimelineViewPageResponse> {
        let view_id = required_view_id(&request.view_id)?;
        let Some((conversation_id, page_size)) = self
            .with_timeline(&view_id, |view| {
                (view.window.conversation_id.clone(), view.page_size)
            })
            .await
        else {
            return Ok(closed_page(view_id));
        };
        self.jump_to_latest_inner(view_id, &conversation_id, page_size)
            .await
    }

    async fn jump_to_latest_inner(
        &self,
        view_id: String,
        conversation_id: &str,
        page_size: u32,
    ) -> Result<TimelineViewPageResponse> {
        let latest = self.latest_window(conversation_id, page_size).await?;
        let loaded_count = latest.messages.len() as u32;
        let (revision, update) = {
            let mut views = self.inner.views.write().await;
            let Some(registration) = views.timelines.get_mut(&view_id) else {
                return Ok(closed_page(view_id));
            };
            let update = replace_timeline(&view_id, registration, latest);
            (registration.revision, Some(update))
        };
        self.publish(update);
        Ok(TimelineViewPageResponse {
            view_id,
            loaded_count,
            revision,
        })
    }

    /// 视图当前的快照与版本号（宿主发现版本号不连续时整体替换用）。
    pub async fn timeline_snapshot(
        &self,
        request: TimelineViewRequest,
    ) -> Result<ViewOpenResponse> {
        let view_id = required_view_id(&request.view_id)?;
        self.with_timeline(&view_id, |view| ViewOpenResponse {
            view_id: view_id.clone(),
            revision: view.revision,
            snapshot: ViewSnapshot::Timeline(view.window.snapshot()),
        })
        .await
        .ok_or_else(|| FlareError::localized(ErrorCode::InvalidParameter, "sdk.view.not_open"))
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
            ConversationListViewRegistration {
                request,
                order,
                revision: 1,
                last_snapshot: snapshot.clone(),
            },
        );
        Ok(ViewOpenResponse {
            view_id,
            revision: 1,
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
                .close_timeline(&registration.window.conversation_id);
        }
        Ok(CloseViewResponse {
            closed: closed_timeline.is_some() || closed_list,
        })
    }

    async fn with_timeline<T>(
        &self,
        view_id: &str,
        read: impl FnOnce(&TimelineViewRegistration) -> T,
    ) -> Option<T> {
        self.inner
            .views
            .read()
            .await
            .timelines
            .get(view_id)
            .map(read)
    }

    fn publish(&self, update: Option<ViewUpdate>) {
        if let Some(update) = update {
            self.inner.bus.publish(SdkEvent::View(update));
        }
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
        // 先订阅再起任务：构造之后、任务第一次运行之前写入的名字也不会漏掉。
        let name_changes = bus.subscribe_name_changes();
        spawn_background_task(async move {
            let mut rx = bus.subscribe_raw();
            loop {
                let mut plan = tokio::select! {
                    event = rx.recv() => match event {
                        Ok(event) => {
                            let Some(plan) = view_refresh_plan(&event) else {
                                continue;
                            };
                            plan
                        }
                        Err(_) => break,
                    },
                    change = name_changes.recv() => ViewRefreshPlan::names(change),
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
                if let Some(change) = name_changes.try_recv() {
                    plan.names.merge(change);
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

    /// 事件驱动的刷新，按实时合并规则：
    ///
    /// | 窗口 | 新消息 | 其他消息事件 | 同步完成 / 需要重同步 |
    /// | --- | --- | --- | --- |
    /// | 接在最新 | 有序追加，超上限从旧的一端裁掉 | 只按 id 重读涉及的行，`upsert` 或 `remove` | 在窗口的 seq 范围内重读 |
    /// | 脱离最新 | 不追加，只更新已在窗口里的行 | 同上 | 同上 |
    ///
    /// 在脱离最新的会话里发送消息时先回到最新，待发送行随最新窗口一起推出。事件不带消息 id 时
    /// 退化为在窗口的 seq 范围内重读。会话事件只换会话摘要，可见下限升高（清空记录）时换成最新窗口。
    ///
    /// 名字投影的输入变了（身份缓存的资料名与别名、会话内成员昵称）：时间线只按 id 重读窗口里
    /// 这些人发出的行（成员昵称只看所在会话），会话摘要以他们命名时一并换；会话列表只重读以他们
    /// 命名的会话。本轮已经在窗口范围内重读、或会整体重读列表的，不再重读一遍。
    async fn refresh_open_views(&self, plan: ViewRefreshPlan) {
        if !plan.live.is_empty() {
            self.refresh_live(&plan.live).await;
        }
        if !plan.rows.is_empty() {
            self.refresh_rows(&plan.rows).await;
        }
        if let Some(target) = &plan.reread {
            self.reread_windows(target).await;
        }
        if let Some(target) = &plan.headers {
            self.refresh_headers(target, plan.reread.as_ref()).await;
        }
        if !plan.names.is_empty() {
            self.refresh_timeline_names(&plan.names, plan.reread.as_ref(), plan.headers.as_ref())
                .await;
            if !plan.refresh_conversation_lists {
                self.refresh_conversation_list_names(&plan.names).await;
            }
        }
        if plan.refresh_conversation_lists {
            self.refresh_conversation_lists().await;
        }
    }

    /// 打开的时间线视图：视图 id、会话 id，以及读这一刻的窗口状态。
    async fn timeline_views<T>(
        &self,
        mut select: impl FnMut(&str, &TimelineViewRegistration) -> Option<T>,
    ) -> Vec<(String, T)> {
        let views = self.inner.views.read().await;
        views
            .timelines
            .iter()
            .filter_map(|(view_id, view)| {
                select(view_id, view).map(|value| (view_id.clone(), value))
            })
            .collect()
    }

    async fn refresh_live(&self, messages: &[IMMessage]) {
        let mut by_conversation: HashMap<String, Vec<IMMessage>> = HashMap::new();
        for message in messages {
            let conversation_id = message.conversation_id.trim();
            if !conversation_id.is_empty() {
                by_conversation
                    .entry(conversation_id.to_string())
                    .or_default()
                    .push(message.clone());
            }
        }
        // 脱离最新的视图里出现了本机刚发出的待发送行，或接在最新的视图还没有已发送的行：
        // 换成最新窗口（待发送行已先落库，随最新窗口一起推出）。其余视图做有序合并。
        let targets = self
            .timeline_views(|_, view| {
                let incoming = by_conversation.get(&view.window.conversation_id)?;
                let own_send = incoming.iter().any(is_local_pending_send);
                let reset = (!view.window.attached_to_latest && own_send)
                    || (view.window.attached_to_latest && view.window.oldest_seq().is_none());
                Some((view.window.conversation_id.clone(), view.page_size, reset))
            })
            .await;
        if targets.is_empty() {
            return;
        }

        let mut hydrated: HashMap<String, (Vec<IMMessage>, Option<Conversation>)> = HashMap::new();
        for (view_id, (conversation_id, page_size, reset)) in targets {
            if reset {
                match self.latest_window(&conversation_id, page_size).await {
                    Ok(latest) => self.replace_window(&view_id, latest).await,
                    Err(error) => warn_refresh(&view_id, &error),
                }
                continue;
            }
            if !hydrated.contains_key(&conversation_id) {
                let mut rows = by_conversation
                    .get(&conversation_id)
                    .cloned()
                    .unwrap_or_default();
                if let Err(error) = self
                    .inner
                    .conversation_api
                    .hydrate_timeline_messages(&mut rows)
                    .await
                {
                    warn_refresh(&view_id, &error);
                    continue;
                }
                let header = self
                    .inner
                    .conversation_api
                    .get(&conversation_id)
                    .await
                    .unwrap_or_default();
                hydrated.insert(conversation_id.clone(), (rows, header));
            }
            let Some((rows, header)) = hydrated.get(&conversation_id) else {
                continue;
            };
            let touched = rows.iter().map(IMMessage::row_key).collect::<HashSet<_>>();
            let update = {
                let mut views = self.inner.views.write().await;
                let Some(registration) = views.timelines.get_mut(&view_id) else {
                    continue;
                };
                let mut next = registration.window.clone();
                merge_live(&mut next, rows);
                if header.is_some() {
                    next.conversation = header.clone();
                }
                next.trim_older_end(MAX_TIMELINE_WINDOW_MESSAGES);
                commit_timeline(&view_id, registration, next, CompareRows::Keys(&touched))
            };
            self.publish(update);
        }
    }

    /// 按 id 只重读事件涉及、且在窗口里的那几行。不在窗口里的 id 不读。
    async fn refresh_rows(&self, ids: &[MessageRef]) {
        let targets = self
            .timeline_views(|_, view| {
                let rows = view
                    .window
                    .messages
                    .iter()
                    .filter(|message| {
                        ids.iter().any(|reference| {
                            reference
                                .conversation_id
                                .as_deref()
                                .is_none_or(|id| id == view.window.conversation_id)
                                && reference.matches(message)
                        })
                    })
                    .map(WindowRow::of)
                    .collect::<Vec<_>>();
                (!rows.is_empty()).then(|| (rows, visibility_floor(&view.window)))
            })
            .await;
        for (view_id, (rows, floor)) in targets {
            let (keys, fresh) = match self.reread_rows(&rows).await {
                Ok(read) => read,
                Err(error) => {
                    warn_refresh(&view_id, &error);
                    continue;
                }
            };
            let update = {
                let mut views = self.inner.views.write().await;
                let Some(registration) = views.timelines.get_mut(&view_id) else {
                    continue;
                };
                let mut next = registration.window.clone();
                apply_reread_rows(&mut next, &keys, &fresh, floor);
                commit_timeline(&view_id, registration, next, CompareRows::Keys(&keys))
            };
            self.publish(update);
        }
    }

    /// 按 id 重读窗口里的这几行：有客户端 id 的按客户端 id 读（行键就是它），其余按服务端 id 读，
    /// 各一次批量查询。返回这几行的行键与读到的行（读不到的行已删除或不再可见）。
    async fn reread_rows(&self, rows: &[WindowRow]) -> Result<(HashSet<String>, Vec<IMMessage>)> {
        let mut client_ids = Vec::new();
        let mut server_ids = Vec::new();
        for row in rows {
            if !row.client_msg_id.is_empty() {
                client_ids.push(row.client_msg_id.clone());
            } else {
                server_ids.push(row.server_id.clone());
            }
        }
        let fresh = self
            .inner
            .conversation_api
            .timeline_rows_by_id(&client_ids, &server_ids)
            .await?;
        Ok((rows.iter().map(|row| row.key.clone()).collect(), fresh))
    }

    /// 名字变了的人：每个打开的时间线只按 id 重读窗口里他们发出的行（成员昵称只看所在会话），
    /// 会话摘要以他们命名时（[`shows_renamed_people`]）一并重读摘要，一次提交、版本号加 1；重新
    /// 投影后内容没变的行不推。本轮已在窗口范围内重读的视图（同步完成等）跳过，已换过摘要的不再读摘要。
    async fn refresh_timeline_names(
        &self,
        change: &NameChange,
        reread: Option<&ViewRefreshTarget>,
        headers: Option<&ViewRefreshTarget>,
    ) {
        let targets = self
            .timeline_views(|_, view| {
                let conversation_id = view.window.conversation_id.as_str();
                if reread.is_some_and(|target| target.matches_timeline(conversation_id)) {
                    return None;
                }
                let rows = view
                    .window
                    .messages
                    .iter()
                    .filter(|message| change.renames_sender(conversation_id, message.sender_id()))
                    .map(WindowRow::of)
                    .collect::<Vec<_>>();
                let header = !headers
                    .is_some_and(|target| target.matches_timeline(conversation_id))
                    && view
                        .window
                        .conversation
                        .as_ref()
                        .is_some_and(|conversation| shows_renamed_people(change, conversation));
                (!rows.is_empty() || header).then(|| {
                    (
                        conversation_id.to_string(),
                        rows,
                        header,
                        visibility_floor(&view.window),
                    )
                })
            })
            .await;
        for (view_id, (conversation_id, rows, header, floor)) in targets {
            let (keys, fresh) = if rows.is_empty() {
                (HashSet::new(), Vec::new())
            } else {
                match self.reread_rows(&rows).await {
                    Ok(read) => read,
                    Err(error) => {
                        warn_refresh(&view_id, &error);
                        continue;
                    }
                }
            };
            let conversation = if header {
                match self.inner.conversation_api.get(&conversation_id).await {
                    Ok(conversation) => conversation,
                    Err(error) => {
                        warn_refresh(&view_id, &error);
                        None
                    }
                }
            } else {
                None
            };
            let update = {
                let mut views = self.inner.views.write().await;
                let Some(registration) = views.timelines.get_mut(&view_id) else {
                    continue;
                };
                let mut next = registration.window.clone();
                if !keys.is_empty() {
                    apply_reread_rows(&mut next, &keys, &fresh, floor);
                }
                if conversation.is_some() {
                    next.conversation = conversation;
                }
                commit_timeline(&view_id, registration, next, CompareRows::Keys(&keys))
            };
            self.publish(update);
        }
    }

    /// 名字变了的人：每个打开的会话列表只重读以他们命名的会话（[`shows_renamed_people`]），原位
    /// 换掉，内容变了的推 `update`，版本号加 1。列表里没有受影响的会话时什么也不读。
    async fn refresh_conversation_list_names(&self, change: &NameChange) {
        let targets = {
            let views = self.inner.views.read().await;
            views
                .conversation_lists
                .iter()
                .filter_map(|(view_id, view)| {
                    let ids = view
                        .last_snapshot
                        .conversations
                        .iter()
                        .filter(|conversation| shows_renamed_people(change, conversation))
                        .map(|conversation| conversation.conversation_id.clone())
                        .collect::<Vec<_>>();
                    (!ids.is_empty()).then(|| (view_id.clone(), ids))
                })
                .collect::<Vec<_>>()
        };
        if targets.is_empty() {
            return;
        }
        let mut ids = targets
            .iter()
            .flat_map(|(_, ids)| ids.iter().cloned())
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        let fresh = match self.inner.conversation_api.get_multiple(&ids).await {
            Ok(fresh) => fresh
                .into_iter()
                .map(|conversation| (conversation.conversation_id.clone(), conversation))
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                tracing::warn!(
                    target = "flare_sdk.view",
                    error = %error,
                    "refresh conversation list names failed"
                );
                return;
            }
        };
        for (view_id, ids) in targets {
            let update = {
                let mut views = self.inner.views.write().await;
                let Some(registration) = views.conversation_lists.get_mut(&view_id) else {
                    continue;
                };
                let mut next = registration.last_snapshot.clone();
                for conversation in next.conversations.iter_mut() {
                    if ids.contains(&conversation.conversation_id)
                        && let Some(fresh) = fresh.get(&conversation.conversation_id)
                    {
                        *conversation = fresh.clone();
                    }
                }
                let update = conversation_list_view_update(
                    &view_id,
                    registration.revision + 1,
                    &registration.last_snapshot,
                    &next,
                );
                if update.is_some() {
                    registration.revision += 1;
                }
                registration.last_snapshot = next;
                update
            };
            self.publish(update);
        }
    }

    /// 在窗口的 seq 范围内重读：接在最新时读窗口最旧一行之上的全部行（含头部未证实行与
    /// 待发送行），脱离最新时只读窗口两端之间。`hasOlder` 由区间与窗口之下的本地行回答，
    /// 所以重读不会让它回跳。
    async fn reread_windows(&self, target: &ViewRefreshTarget) {
        let targets = self
            .timeline_views(|_, view| {
                target
                    .matches_timeline(&view.window.conversation_id)
                    .then(|| {
                        (
                            view.window.conversation_id.clone(),
                            view.page_size,
                            view.window.attached_to_latest,
                            view.window.oldest_seq(),
                            view.window.newest_seq(),
                        )
                    })
            })
            .await;
        for (view_id, (conversation_id, page_size, attached, oldest, newest)) in targets {
            let (Some(oldest), Some(newest)) = (oldest, newest) else {
                if attached {
                    match self.latest_window(&conversation_id, page_size).await {
                        Ok(latest) => self.commit_window(&view_id, latest, None).await,
                        Err(error) => warn_refresh(&view_id, &error),
                    }
                }
                continue;
            };
            let reread = if attached {
                self.inner
                    .conversation_api
                    .reload_timeline_window(&conversation_id, oldest, MAX_TIMELINE_WINDOW_MESSAGES)
                    .await
                    .map(|snapshot| (snapshot.conversation, snapshot.messages, snapshot.has_more))
            } else {
                self.reread_detached(&conversation_id, oldest, newest).await
            };
            match reread {
                Ok((conversation, messages, has_older)) => {
                    let window = TimelineWindow {
                        conversation_id: conversation_id.clone(),
                        conversation,
                        messages,
                        has_older,
                        attached_to_latest: attached,
                        anchor_key: None,
                    };
                    self.commit_window(
                        &view_id,
                        window,
                        Some((oldest, (!attached).then_some(newest))),
                    )
                    .await
                }
                Err(error) => warn_refresh(&view_id, &error),
            }
        }
    }

    async fn reread_detached(
        &self,
        conversation_id: &str,
        oldest: u64,
        newest: u64,
    ) -> Result<(Option<Conversation>, Vec<IMMessage>, bool)> {
        let api = &self.inner.conversation_api;
        let (conversation, coverage) = api.timeline_paging_state(conversation_id).await?;
        let (messages, _) = api
            .timeline_rows_after(
                conversation_id,
                oldest.saturating_sub(1),
                newest,
                MAX_TIMELINE_WINDOW_MESSAGES,
            )
            .await?;
        let (below, _) = api
            .timeline_rows_in_range(conversation_id, 1, oldest, 1)
            .await?;
        let oldest_now = messages
            .first()
            .map(|message| message.conversation_seq)
            .unwrap_or(oldest);
        Ok((
            conversation,
            messages,
            coverage.has_older(oldest_now, !below.is_empty()),
        ))
    }

    /// 用重读得到的窗口替换当前窗口。`reread_range` 是重读覆盖的 seq 范围（`newest = None`
    /// 表示一直到最新）：重读期间往旧、往新翻页并入的范围外的行保留，hasOlder 随之保留。
    async fn commit_window(
        &self,
        view_id: &str,
        mut fresh: TimelineWindow,
        reread_range: Option<(u64, Option<u64>)>,
    ) {
        let update = {
            let mut views = self.inner.views.write().await;
            let Some(registration) = views.timelines.get_mut(view_id) else {
                return;
            };
            let current = &registration.window;
            fresh.anchor_key = current.anchor_key.clone();
            if let Some((from, to)) = reread_range {
                if current.attached_to_latest != fresh.attached_to_latest {
                    // 重读期间窗口换了方向（回到最新）：这份重读已经过时。
                    return;
                }
                let outside = current
                    .messages
                    .iter()
                    .filter(|message| {
                        let seq = message.conversation_seq;
                        seq > 0 && (seq < from || to.is_some_and(|to| seq > to))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if current.oldest_seq().is_some_and(|oldest| oldest < from) {
                    fresh.has_older = current.has_older;
                }
                if !outside.is_empty() {
                    fresh.messages.extend(outside);
                    fresh.messages.sort_by(IMMessage::compare_for_timeline_asc);
                }
            }
            if fresh.attached_to_latest {
                fresh.trim_older_end(MAX_TIMELINE_WINDOW_MESSAGES);
            } else {
                fresh.trim_newer_end(MAX_TIMELINE_WINDOW_MESSAGES);
            }
            commit_timeline(view_id, registration, fresh, CompareRows::All)
        };
        self.publish(update);
    }

    async fn replace_window(&self, view_id: &str, latest: TimelineWindow) {
        let update = {
            let mut views = self.inner.views.write().await;
            let Some(registration) = views.timelines.get_mut(view_id) else {
                return;
            };
            replace_timeline(view_id, registration, latest)
        };
        self.publish(Some(update));
    }

    /// 会话摘要变了：只换摘要；可见下限升高（本地清空记录、服务端历史可见起点）时，
    /// 下限之下的行不再可见，换成最新窗口。已在本轮重读过的视图跳过。
    async fn refresh_headers(
        &self,
        target: &ViewRefreshTarget,
        reread: Option<&ViewRefreshTarget>,
    ) {
        let targets = self
            .timeline_views(|_, view| {
                let conversation_id = &view.window.conversation_id;
                (target.matches_timeline(conversation_id)
                    && !reread.is_some_and(|reread| reread.matches_timeline(conversation_id)))
                .then(|| (conversation_id.clone(), view.page_size))
            })
            .await;
        for (view_id, (conversation_id, page_size)) in targets {
            let header = match self.inner.conversation_api.get(&conversation_id).await {
                Ok(header) => header,
                Err(error) => {
                    warn_refresh(&view_id, &error);
                    continue;
                }
            };
            let floor = header
                .as_ref()
                .map(crate::domain::sync_visibility_floor)
                .unwrap_or(0);
            let reset = self
                .with_timeline(&view_id, |view| {
                    let old_floor = view
                        .window
                        .conversation
                        .as_ref()
                        .map(crate::domain::sync_visibility_floor)
                        .unwrap_or(0);
                    floor > old_floor
                        && view.window.messages.iter().any(|message| {
                            message.conversation_seq > 0 && message.conversation_seq <= floor
                        })
                })
                .await
                .unwrap_or(false);
            if reset {
                match self.latest_window(&conversation_id, page_size).await {
                    Ok(latest) => self.replace_window(&view_id, latest).await,
                    Err(error) => warn_refresh(&view_id, &error),
                }
                continue;
            }
            let update = {
                let mut views = self.inner.views.write().await;
                let Some(registration) = views.timelines.get_mut(&view_id) else {
                    continue;
                };
                let mut next = registration.window.clone();
                next.conversation = header;
                commit_timeline(&view_id, registration, next, CompareRows::None)
            };
            self.publish(update);
        }
    }

    async fn refresh_conversation_lists(&self) {
        let lists = {
            let views = self.inner.views.read().await;
            views
                .conversation_lists
                .iter()
                .map(|(view_id, view)| (view_id.clone(), view.request.clone()))
                .collect::<Vec<_>>()
        };
        for (view_id, request) in lists {
            match self.conversation_list_snapshot(&request).await {
                Ok(snapshot) => {
                    let update = {
                        let mut views = self.inner.views.write().await;
                        let Some(registration) = views.conversation_lists.get_mut(&view_id) else {
                            continue;
                        };
                        let update = conversation_list_view_update(
                            &view_id,
                            registration.revision + 1,
                            &registration.last_snapshot,
                            &snapshot,
                        );
                        if update.is_some() {
                            registration.revision += 1;
                        }
                        registration.last_snapshot = snapshot;
                        update
                    };
                    self.publish(update);
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
}

fn anchor_is_set(anchor: &TimelineAnchor) -> bool {
    anchor
        .message_id
        .as_deref()
        .is_some_and(|id| !id.trim().is_empty())
        || anchor.seq.is_some_and(|seq| seq > 0)
}

fn required_view_id(view_id: &str) -> Result<String> {
    let view_id = view_id.trim();
    if view_id.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "viewId is required",
        ));
    }
    Ok(view_id.to_string())
}

fn closed_page(view_id: String) -> TimelineViewPageResponse {
    TimelineViewPageResponse {
        view_id,
        loaded_count: 0,
        revision: 0,
    }
}

/// 窗口里要按 id 重读的一行。
struct WindowRow {
    key: String,
    client_msg_id: String,
    server_id: String,
}

impl WindowRow {
    fn of(message: &IMMessage) -> Self {
        Self {
            key: message.row_key(),
            client_msg_id: message.client_msg_id.clone(),
            server_id: message.server_id.clone(),
        }
    }
}

/// 窗口所在会话的可见下限（本地清空记录、服务端历史可见起点）：重读到的行在它之下就移出。
fn visibility_floor(window: &TimelineWindow) -> u64 {
    window
        .conversation
        .as_ref()
        .map(crate::domain::sync_visibility_floor)
        .unwrap_or(0)
}

/// 会话摘要里有取自这次名字变了的人的名字：
///
/// - 标题或末条发送者：身份缓存写入时算受影响会话的同一条规则
///   （[`crate::domain::conversation_shows_person`]），这里直接调用它；
/// - 摘要带着的成员（成员预览、成员列表）里有这个人：成员的 `name` 也在读取时投影。有名字的群
///   不以成员命名，却照样带着成员预览；只按标题与末条发送者推，宿主把增量应用到上一版后，
///   成员的名字仍是旧的，与新快照不一致。
///
/// 第二条只决定打开中的视图重读哪些会话；身份缓存写入时发 `ConversationUpdated` 的范围不变。
fn shows_renamed_people(change: &NameChange, conversation: &Conversation) -> bool {
    change
        .people_in(conversation.conversation_id.trim())
        .any(|user_id| {
            crate::domain::conversation_shows_person(conversation, user_id)
                || conversation
                    .member_preview
                    .iter()
                    .chain(conversation.participants.iter())
                    .any(|member| member.user_id.trim() == user_id)
        })
}

/// 本机刚发出、还没有服务端 seq 的行（发送队列的乐观行，含上传中）。
fn is_local_pending_send(message: &IMMessage) -> bool {
    message.conversation_seq == 0 && message.local_state.is_local
}

fn warn_refresh(view_id: &str, error: &FlareError) {
    tracing::warn!(
        target = "flare_sdk.view",
        view_id,
        error = %error,
        "refresh timeline view failed"
    );
}

/// 换上新窗口；有变化时版本号加 1 并给出对应的更新。
fn commit_timeline(
    view_id: &str,
    registration: &mut TimelineViewRegistration,
    next: TimelineWindow,
    compare: CompareRows<'_>,
) -> Option<ViewUpdate> {
    let change = timeline_change(&registration.window, &next, compare);
    registration.window = next;
    match change {
        TimelineChange::None => None,
        TimelineChange::Delta(delta) => {
            registration.revision += 1;
            Some(ViewUpdate {
                view_id: view_id.to_string(),
                revision: registration.revision,
                kind: ViewUpdateKind::Delta,
                snapshot: None,
                delta: Some(*delta),
            })
        }
        TimelineChange::Snapshot => {
            registration.revision += 1;
            Some(timeline_snapshot_update(view_id, registration))
        }
    }
}

/// 整体换成新窗口（回到最新、清空记录），版本号加 1，以快照推出。
fn replace_timeline(
    view_id: &str,
    registration: &mut TimelineViewRegistration,
    next: TimelineWindow,
) -> ViewUpdate {
    registration.window = next;
    registration.revision += 1;
    timeline_snapshot_update(view_id, registration)
}

fn timeline_snapshot_update(view_id: &str, registration: &TimelineViewRegistration) -> ViewUpdate {
    ViewUpdate {
        view_id: view_id.to_string(),
        revision: registration.revision,
        kind: ViewUpdateKind::Snapshot,
        snapshot: Some(ViewSnapshot::Timeline(registration.window.snapshot())),
        delta: None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ViewRefreshTarget {
    All,
    Conversations(HashSet<String>),
}

impl ViewRefreshTarget {
    fn conversation(conversation_id: &str) -> Self {
        let conversation_id = conversation_id.trim();
        if conversation_id.is_empty() {
            return Self::All;
        }
        Self::Conversations([conversation_id.to_string()].into_iter().collect())
    }

    fn conversations<'a>(ids: impl IntoIterator<Item = &'a String>) -> Self {
        let ids = ids
            .into_iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .collect::<HashSet<_>>();
        if ids.is_empty() {
            Self::All
        } else {
            Self::Conversations(ids)
        }
    }

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

fn merge_target(slot: &mut Option<ViewRefreshTarget>, other: Option<ViewRefreshTarget>) {
    match (slot.as_mut(), other) {
        (Some(target), Some(other)) => target.merge(other),
        (None, Some(other)) => *slot = Some(other),
        _ => {}
    }
}

/// 事件涉及的一条消息：服务端 id 或客户端 id；`conversation_id` 为空时（发送失败）匹配任何视图。
#[derive(Clone, Debug, PartialEq, Eq)]
struct MessageRef {
    conversation_id: Option<String>,
    message_id: String,
}

impl MessageRef {
    fn matches(&self, message: &IMMessage) -> bool {
        message.server_id == self.message_id || message.client_msg_id == self.message_id
    }
}

#[derive(Default)]
struct ViewRefreshPlan {
    /// 新消息（推送、同步、本机发送的乐观行）。
    live: Vec<IMMessage>,
    /// 事件涉及的消息：只重读窗口里的这几行。
    rows: Vec<MessageRef>,
    /// 在窗口的 seq 范围内重读。
    reread: Option<ViewRefreshTarget>,
    /// 只换会话摘要。
    headers: Option<ViewRefreshTarget>,
    /// 名字变了的人：只重读他们发出的行与以他们命名的会话。
    names: NameChange,
    refresh_conversation_lists: bool,
}

impl ViewRefreshPlan {
    fn merge(&mut self, other: ViewRefreshPlan) {
        self.live.extend(other.live);
        for reference in other.rows {
            if !self.rows.contains(&reference) {
                self.rows.push(reference);
            }
        }
        merge_target(&mut self.reread, other.reread);
        merge_target(&mut self.headers, other.headers);
        self.names.merge(other.names);
        self.refresh_conversation_lists |= other.refresh_conversation_lists;
    }

    fn names(change: NameChange) -> Self {
        Self {
            names: change,
            ..Self::default()
        }
    }

    fn lists_only() -> Self {
        Self {
            refresh_conversation_lists: true,
            ..Self::default()
        }
    }

    fn reread(target: ViewRefreshTarget) -> Self {
        Self {
            reread: Some(target),
            refresh_conversation_lists: true,
            ..Self::default()
        }
    }

    /// 只重读这些消息；没有可用的 id 时退化为在窗口的 seq 范围内重读该会话。
    fn rows(conversation_id: Option<&str>, message_ids: &[&str]) -> Self {
        let rows = message_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(|message_id| MessageRef {
                conversation_id: conversation_id
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(str::to_string),
                message_id: message_id.to_string(),
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Self::reread(
                conversation_id
                    .map(ViewRefreshTarget::conversation)
                    .unwrap_or(ViewRefreshTarget::All),
            );
        }
        Self {
            rows,
            refresh_conversation_lists: true,
            ..Self::default()
        }
    }
}

fn view_refresh_plan(event: &SdkEvent) -> Option<ViewRefreshPlan> {
    match event {
        SdkEvent::Message(event) => Some(message_refresh_plan(event)),
        SdkEvent::Conversation(event) => Some(conversation_refresh_plan(event)),
        SdkEvent::Sync(SyncNotify::Finished { .. } | SyncNotify::ResyncNeeded { .. }) => {
            Some(ViewRefreshPlan::reread(ViewRefreshTarget::All))
        }
        _ => None,
    }
}

fn message_refresh_plan(event: &MessageEvent) -> ViewRefreshPlan {
    match event {
        MessageEvent::Received { message } => ViewRefreshPlan {
            live: vec![message.as_ref().clone()],
            refresh_conversation_lists: true,
            ..ViewRefreshPlan::default()
        },
        MessageEvent::ReceivedBatch { messages } => ViewRefreshPlan {
            live: messages.clone(),
            refresh_conversation_lists: true,
            ..ViewRefreshPlan::default()
        },
        MessageEvent::SendAck { ack } => {
            ViewRefreshPlan::rows(Some(&ack.conversation_id), &[&ack.client_msg_id])
        }
        MessageEvent::SendFailed { client_msg_id, .. } => {
            ViewRefreshPlan::rows(None, &[client_msg_id])
        }
        MessageEvent::Recalled {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::Edited {
            conversation_id,
            server_msg_id,
        }
        | MessageEvent::ReactionChanged {
            conversation_id,
            server_msg_id,
            ..
        } => ViewRefreshPlan::rows(Some(conversation_id), &[server_msg_id]),
        MessageEvent::Deleted {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::RetentionScheduled {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::RetentionExpired {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::RetentionPurged {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::Pinned {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::Unpinned {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::Marked {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::Unmarked {
            conversation_id,
            event,
        } => ViewRefreshPlan::rows(Some(conversation_id), &[&event.server_msg_id]),
        MessageEvent::PluginStateChanged {
            conversation_id,
            message_id,
            ..
        } => ViewRefreshPlan::rows(Some(conversation_id), &[message_id]),
        MessageEvent::ReadSummariesChanged {
            conversation_id,
            message_ids,
        } => ViewRefreshPlan::rows(
            Some(conversation_id),
            &message_ids.iter().map(String::as_str).collect::<Vec<_>>(),
        ),
        // 已读回执改的是本人消息的回执投影，涉及的行不止一条，也不带全 id：在窗口范围内重读。
        MessageEvent::ReadReceipt {
            conversation_id, ..
        }
        | MessageEvent::Custom {
            conversation_id, ..
        } => ViewRefreshPlan::reread(ViewRefreshTarget::conversation(conversation_id)),
        // 正在输入、在线状态不改时间线的行。
        MessageEvent::Typing { .. }
        | MessageEvent::TypingAggregate { .. }
        | MessageEvent::PresenceChanged { .. } => ViewRefreshPlan::lists_only(),
    }
}

fn conversation_refresh_plan(event: &ConversationEvent) -> ViewRefreshPlan {
    match event {
        ConversationEvent::Synced { conversation_ids } => {
            ViewRefreshPlan::reread(ViewRefreshTarget::conversations(conversation_ids))
        }
        ConversationEvent::Deleted { conversation_id } => {
            ViewRefreshPlan::reread(ViewRefreshTarget::conversation(conversation_id))
        }
        ConversationEvent::Created { conversation_id }
        | ConversationEvent::Updated { conversation_id }
        | ConversationEvent::UnreadCountChanged {
            conversation_id, ..
        } => ViewRefreshPlan {
            headers: Some(ViewRefreshTarget::conversation(conversation_id)),
            refresh_conversation_lists: true,
            ..ViewRefreshPlan::default()
        },
    }
}

fn conversation_list_view_update(
    view_id: &str,
    revision: u64,
    old: &HomeTimelineSnapshot,
    new: &HomeTimelineSnapshot,
) -> Option<ViewUpdate> {
    let Some(ops) = keyed_delta_ops(&old.conversations, &new.conversations, |conversation| {
        conversation.conversation_id.trim().to_string()
    }) else {
        return Some(ViewUpdate {
            view_id: view_id.to_string(),
            revision,
            kind: ViewUpdateKind::Snapshot,
            snapshot: Some(ViewSnapshot::ConversationList(new.clone())),
            delta: None,
        });
    };
    let side_changed =
        old.total_unread != new.total_unread || json_changed(&old.sync_state, &new.sync_state);
    if ops.is_empty() && !side_changed {
        return None;
    }
    Some(ViewUpdate {
        view_id: view_id.to_string(),
        revision,
        kind: ViewUpdateKind::Delta,
        snapshot: None,
        delta: Some(ViewDelta::ConversationList {
            ops,
            total_unread: new.total_unread,
            sync_state: new.sync_state.clone(),
        }),
    })
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

#[cfg(test)]
mod tests {
    use flare_proto::common::{Message as ProtoMessage, SendAck};

    use super::{
        ForegroundPullGuard, MessageRef, TimelineViewRegistration, TimelineWindow,
        ViewRefreshTarget, ViewRegistrations, view_refresh_plan,
    };
    use crate::kernel::event::{MessageEvent, SdkEvent, SyncNotify, SyncPhase};
    use crate::kernel::{SyncReason, SyncRunContext, SyncScope, SyncTrigger, SyncVisibility};
    use crate::model::IMMessage;

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
    fn message_batch_refresh_plan_carries_live_messages_only() {
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
            ],
        });
        let plan = view_refresh_plan(&event).expect("message event refreshes views");
        assert!(plan.reread.is_none());
        assert!(plan.rows.is_empty());
        assert!(plan.refresh_conversation_lists);
        assert_eq!(plan.live.len(), 2);
    }

    #[test]
    fn send_ack_rereads_the_acked_row_by_its_client_id() {
        let event = SdkEvent::Message(MessageEvent::SendAck {
            ack: Box::new(SendAck {
                conversation_id: "c1".into(),
                client_msg_id: "cm-1".into(),
                ..Default::default()
            }),
        });
        let plan = view_refresh_plan(&event).expect("ack refreshes views");
        assert_eq!(
            plan.rows,
            vec![MessageRef {
                conversation_id: Some("c1".into()),
                message_id: "cm-1".into(),
            }]
        );
        assert!(plan.reread.is_none());
    }

    #[test]
    fn an_event_without_a_message_id_rereads_the_window_range() {
        let event = SdkEvent::Message(MessageEvent::Recalled {
            conversation_id: "c1".into(),
            event: Default::default(),
        });
        let plan = view_refresh_plan(&event).expect("recall refreshes views");
        assert!(plan.rows.is_empty());
        let target = plan.reread.expect("window range reread");
        assert!(target.matches_timeline("c1"));
        assert!(!target.matches_timeline("c2"));
    }

    #[test]
    fn typing_does_not_touch_timelines() {
        let event = SdkEvent::Message(MessageEvent::Typing {
            conversation_id: "c1".into(),
            event: Default::default(),
        });
        let plan = view_refresh_plan(&event).expect("typing refreshes lists");
        assert!(plan.live.is_empty() && plan.rows.is_empty());
        assert!(plan.reread.is_none() && plan.headers.is_none());
    }

    #[test]
    fn sync_finish_rereads_all_views() {
        let event = SdkEvent::Sync(SyncNotify::Finished {
            run: SyncRunContext::new(
                SyncTrigger::Manual,
                SyncScope::Global,
                SyncVisibility::Silent,
                SyncReason::UserRequested,
            ),
            phase: SyncPhase::Background,
        });
        let plan = view_refresh_plan(&event).expect("sync finish refreshes views");
        assert_eq!(plan.reread, Some(ViewRefreshTarget::All));
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
                registration(&format!("c{index}"), index),
            );
        }

        assert_eq!(views.timelines.len(), 8);
        assert!(!views.timelines.contains_key("timeline:0"));

        views.insert_timeline("timeline:replacement".to_string(), registration("c8", 10));

        assert_eq!(views.timelines.len(), 8);
        assert!(!views.timelines.contains_key("timeline:8"));
        assert!(views.timelines.contains_key("timeline:replacement"));
    }

    fn registration(conversation_id: &str, order: u64) -> TimelineViewRegistration {
        TimelineViewRegistration {
            page_size: 50,
            order,
            revision: 1,
            window: TimelineWindow {
                conversation_id: conversation_id.to_string(),
                conversation: None,
                messages: Vec::new(),
                has_older: false,
                attached_to_latest: true,
                anchor_key: None,
            },
        }
    }
}

#[cfg(test)]
#[path = "view_test_harness.rs"]
mod harness;

#[cfg(test)]
#[path = "view_paging_tests.rs"]
mod paging_tests;

#[cfg(test)]
#[path = "view_window_tests.rs"]
mod window_tests;

#[cfg(test)]
#[path = "view_name_tests.rs"]
mod name_tests;
