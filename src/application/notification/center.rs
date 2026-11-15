//! NotifyCenter：链路在线时唯一的提示决策点。
//!
//! 实时到达的每条消息（同步拉回的不算）在这里决定提示与否、怎么提示：
//!
//! 1. 本人发的、前台正在看的那个会话里的，不提示；
//! 2. 按规则 crate 决策：提示等级、是否点名、会话偏好（免打扰、仅 @我、大群）、用户设置（勿扰时段）；
//! 3. 前台在别的页面：应用内横幅，不过闸门，但按会话合并（同一会话一条，带条数）；
//!    后台且本进程能自己提示（桌面、Web、接上了后台展示方的手机）：系统通知，过闸门（一个会话一条
//!    通知、响铃阶梯、设备预算、摘要）；后台而提示归推送（手机没有后台展示方、iOS 已托管）：不提示，
//!    服务端推送。
//!
//! 结论以 `notification.present` 交给宿主；之后修正已经发出的通知：会话读了撤掉（`notification.clear`），
//! 展示着的消息被撤回就静默替换成「撤回了一条消息」，回到前台时撤掉已读会话的系统通知；未读总数变化时
//! 报角标（`notification.badge`）。
//!
//! 推送通道在服务端跑同一套规则，两边用同一个合并键；本进程提示过的都记进提示台账，同一条的推送再到达
//! 时不再响。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use flare_notify_rules::text::Lang;
use flare_notify_rules::{
    AlertClass, ConversationKind, ConversationPrefs, ConversationTag, DecideInput, DeviceState,
    DueAction, Gate, GateInput, Mentioned, MultiDevicePolicy, PreviewMode,
    QuietHours as QuietWindow, SlotAction, SlotState, Tier, UserPrefs, decide,
};
use flare_proto::common::sync_res::Payload as SyncResPayload;
use flare_proto::common::{SyncRes, UserSettingsChanged};

use super::AlertLedger;
use super::local_time::local_minute_of_week;
use super::render::{AlertCopy, MessageFacts};
use crate::application::projections::names::{NameBook, NameRequest};
use crate::application::usecases::sync_request::SyncRequestUseCase;
use crate::content::message_elem::Elem;
use crate::domain::{DisplayName, member_nicknames_apply, resolve_person_name};
use crate::extension::encryption::E2EE_PLACEHOLDER_REASON;
use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::event::{
    ConversationEvent, EventBus, EventFilter, MessageEvent, MessageEventType, NotificationEvent,
    SdkEvent, SdkEventKind, SdkEventType, SyncNotify,
};
use crate::model::conversation::ConversationType;
use crate::model::notification::{
    MultiDeviceAlertPolicy, NotificationPresentation, NotificationPreview,
    UpdateUserNotificationSettingsRequest, UserNotificationSettings,
};
use crate::model::{Conversation, IMMessage, notify_all, total_unread};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::{
    BackgroundTask, delay, now_millis, spawn_background, spawn_background_task, timeout,
};

/// 服务端经 `sync:<用户>` 收件箱下发的「用户设置变了」信号（`RealtimeControlPacket.custom.type`）。
pub(crate) const USER_SETTINGS_CHANGED_TYPE: &str = "flare.user_settings_changed";
/// 摘要通知的合并键：每台设备一条，与会话的合并键（`c:` 开头）不会撞。
pub(crate) const DIGEST_COLLAPSE_ID: &str = "d:digest";
/// 超过这么多成员的群默认只提示 @我，显式开启「全部消息通知」的除外（与服务端的默认成员上限一致）。
const LARGE_GROUP_MEMBERS: u32 = 1_000;
/// 应用内横幅两次响铃之间至少隔多久（与闸门的设备最小响铃间隔一致）。
const IN_APP_RING_GAP_MS: i64 = 1_000;
/// 未读总数变化后等多久再报角标：一批消息落库引起的一串变化只报一次。
const BADGE_DEBOUNCE: Duration = Duration::from_millis(300);
/// 读会话列表（角标、回前台对账）的时限。
const LIST_CAP: Duration = Duration::from_secs(2);
/// 内存里最多跟踪多少个会话的通知状态；再多就丢最久没碰过、手上没有通知的那个。
const MAX_SLOTS: usize = 512;

/// 一条提示展示在哪里。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlertSurface {
    /// 系统通知（应用在后台，本进程能自己提示）。
    System,
    /// 应用内横幅（应用在前台）。
    InApp,
}

impl AlertSurface {
    fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::InApp => "inApp",
        }
    }
}

/// 本进程此刻能不能自己提示、在哪里提示：由链路的可达状态回答。
pub(crate) trait AlertReach: Send + Sync {
    /// `None`：此刻提示归推送通道（手机在后台而没有后台展示方，或链路已托管）。
    fn alert_surface(&self) -> Option<AlertSurface>;
}

/// 宿主告诉核心的、跨登录保留的事实：正在看哪个会话，后台有没有展示方，与通知扩展共用哪个目录。
#[derive(Default)]
pub struct NotificationHost {
    focus: Mutex<Option<String>>,
    background_presenter: AtomicBool,
    /// 与 iOS 通知扩展共用的目录（App Group 容器）：通知快照写在这里，扩展的回执日志也在这里。
    #[cfg(not(target_arch = "wasm32"))]
    snapshot_directory: Mutex<Option<std::path::PathBuf>>,
}

impl NotificationHost {
    fn focus_slot(&self) -> MutexGuard<'_, Option<String>> {
        self.focus
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 正在看的会话；空白当作没有。
    pub fn set_focus(&self, conversation_id: Option<String>) {
        *self.focus_slot() = conversation_id
            .map(|id| id.trim().to_owned())
            .filter(|id| !id.is_empty());
    }

    pub fn focus(&self) -> Option<String> {
        self.focus_slot().clone()
    }

    fn is_focused(&self, conversation_id: &str) -> bool {
        self.focus_slot().as_deref() == Some(conversation_id)
    }

    pub fn set_background_presenter(&self, attached: bool) {
        self.background_presenter.store(attached, Ordering::Release);
    }

    pub fn background_presenter(&self) -> bool {
        self.background_presenter.load(Ordering::Acquire)
    }

    /// 与通知扩展共用的目录；`None` 不再写快照（已有的文件不动）。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_snapshot_directory(&self, directory: Option<std::path::PathBuf>) {
        *self
            .snapshot_directory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = directory;
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_directory(&self) -> Option<std::path::PathBuf> {
        self.snapshot_directory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// 建 NotifyCenter 要的东西。
pub(crate) struct NotifyCenterParts {
    pub bus: EventBus,
    pub stores: StoreProvider,
    pub reach: Arc<dyn AlertReach>,
    pub alerts: Arc<AlertLedger>,
    pub sync: SyncRequestUseCase,
    /// 应用的界面语言（BCP 47）；没给时用中文。
    pub locale: Option<String>,
}

/// 一条进了决策、要提示的消息。
#[derive(Clone)]
struct Candidate {
    message: IMMessage,
    tier: Tier,
    /// 点名到了接收者（含 @所有人）。
    mentioned: bool,
    urgent: bool,
    kind: ConversationKind,
}

/// 宿主眼下展示着的那条通知。
#[derive(Debug, Clone)]
struct Shown {
    seq: u64,
    message_id: String,
    /// 不带条数的标题；通用标题时为 `None`。撤回替换沿用它。
    title: Option<String>,
    count: u32,
    surface: AlertSurface,
}

/// 一个会话在这台设备上的通知状态。
struct Slot {
    collapse_id: String,
    kind: ConversationKind,
    tag: ConversationTag,
    gate: SlotState,
    /// 本中心在这个会话里见过的最大 seq：已读游标越过它，会话就算读完了。
    max_seq: u64,
    /// 闸门吞并、还没展示的最新一条：尾部刷新用它出文案。
    latest: Option<Box<Candidate>>,
    /// 已登记的尾部刷新时刻。
    tail_due_ms: Option<i64>,
    shown: Option<Shown>,
    /// 应用内横幅打开以来代表的消息数。
    in_app_count: u32,
    touched_ms: i64,
}

impl Slot {
    fn idle(&self) -> bool {
        self.shown.is_none()
            && self.tail_due_ms.is_none()
            && self.in_app_count == 0
            && !self.gate.is_open()
    }
}

struct CenterState {
    device: DeviceState,
    slots: HashMap<String, Slot>,
    last_in_app_ring_ms: i64,
    /// 当前用户：名字投影排除自己时用（尾部刷新时手上没有别的来源）。
    user_id: String,
    /// 摘要通知展示着：回到前台时撤掉。
    digest_shown: bool,
}

impl Default for CenterState {
    fn default() -> Self {
        Self {
            device: DeviceState::default(),
            slots: HashMap::new(),
            last_in_app_ring_ms: i64::MIN,
            user_id: String::new(),
            digest_shown: false,
        }
    }
}

#[derive(Default)]
struct BadgeState {
    scheduled: bool,
    force: bool,
    last: Option<u32>,
}

/// 决策之后、出文案之前的一次展示。
enum Order {
    Message {
        candidate: Box<Candidate>,
        collapse_id: String,
        count: u32,
        sound: bool,
        surface: AlertSurface,
        /// 替换展示着的那条（尾部刷新）：台账里相同的 seq 也放行。
        replace: bool,
    },
    Digest {
        conversations: u32,
        messages: u32,
    },
}

/// 链路在线时的提示决策点，属于一个登录会话（与提示台账同生命周期）。
pub(crate) struct NotifyCenter {
    me: Weak<NotifyCenter>,
    bus: EventBus,
    stores: StoreProvider,
    reach: Arc<dyn AlertReach>,
    alerts: Arc<AlertLedger>,
    sync: SyncRequestUseCase,
    lang: Lang,
    gate: Gate,
    host: ArcSwap<NotificationHost>,
    state: Mutex<CenterState>,
    settings: Mutex<Option<UserNotificationSettings>>,
    badge: Mutex<BadgeState>,
    watcher: Mutex<Option<BackgroundTask>>,
    /// 会话已结束（登出、断开）：撤掉全部通知只做一次。
    ended: AtomicBool,
    /// 给 iOS 通知扩展的通知快照（浏览器里没有）。
    #[cfg(not(target_arch = "wasm32"))]
    snapshot: snapshot::SnapshotSlot,
}

impl Drop for NotifyCenter {
    fn drop(&mut self) {
        if let Some(watcher) = self
            .watcher
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            watcher.abort();
        }
    }
}

impl NotifyCenter {
    /// 建好并开始盯着会话与撤回事件（已读修正、撤回替换、角标）。
    pub(crate) fn start(parts: NotifyCenterParts) -> Arc<Self> {
        let NotifyCenterParts {
            bus,
            stores,
            reach,
            alerts,
            sync,
            locale,
        } = parts;
        let center = Arc::new_cyclic(|me| Self {
            me: me.clone(),
            bus,
            stores,
            reach,
            alerts,
            sync,
            lang: Lang::from_locale(locale.as_deref().unwrap_or_default()),
            gate: Gate::default(),
            host: ArcSwap::from_pointee(NotificationHost::default()),
            state: Mutex::new(CenterState::default()),
            settings: Mutex::new(None),
            badge: Mutex::new(BadgeState::default()),
            watcher: Mutex::new(None),
            ended: AtomicBool::new(false),
            #[cfg(not(target_arch = "wasm32"))]
            snapshot: snapshot::SnapshotSlot::new(locale.as_deref()),
        });
        center.start_watcher();
        #[cfg(not(target_arch = "wasm32"))]
        center.start_snapshot_watcher();
        center
    }

    /// 这一个登录会话属于谁（准备好、建连开始时各报一次）：通知快照按它写。
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn attach_session(&self, _user_id: &str, _tenant_id: &str) {}

    /// 通知快照：浏览器里没有通知扩展，什么都不做。
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn snapshot_changed(&self) {}

    /// 用客户端那份宿主事实（正在看的会话、后台展示方）：它跨登录保留，引擎每次登录新建。
    pub(crate) fn adopt_host(&self, host: Arc<NotificationHost>) {
        self.host.store(host);
    }

    fn lock_state(&self) -> MutexGuard<'_, CenterState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 会话已结束（登出、断开）：之后到期的定时、晚到的事件一律不再出通知、不再报角标。
    fn ended(&self) -> bool {
        self.ended.load(Ordering::Acquire)
    }

    fn cached_settings(&self) -> Option<UserNotificationSettings> {
        self.settings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    // ---------------------------------------------------------------------------------------
    // 实时到达的消息
    // ---------------------------------------------------------------------------------------

    /// 一批实时到达的消息里可能要提示的那些（同步不阻塞，供时间线先拿走消息）。此刻提示归推送时
    /// 一条不剩。
    pub(crate) fn live_candidates(&self, messages: &[IMMessage], user_id: &str) -> Vec<IMMessage> {
        let Some(surface) = self.reach.alert_surface() else {
            return Vec::new();
        };
        let host = self.host.load();
        messages
            .iter()
            .filter(|message| alertable(message, user_id))
            .filter(|message| {
                surface != AlertSurface::InApp || !host.is_focused(&message.conversation_id)
            })
            .cloned()
            .collect()
    }

    /// 决策、过闸门（或按会话合并）、出文案、交给宿主。在送达确认之前做完：确认即承诺由本进程提示。
    pub(crate) async fn present_live(&self, candidates: Vec<IMMessage>, user_id: &str) {
        if candidates.is_empty() || self.ended() {
            return;
        }
        let Some(surface) = self.reach.alert_surface() else {
            return;
        };
        let now = now_millis() as i64;
        let summaries = self.summaries(&candidates).await;
        let settings = self.cached_settings();
        let user = user_prefs(settings.as_ref());
        let local_time = Some(local_minute_of_week(now));

        let mut admitted = Vec::with_capacity(candidates.len());
        for message in candidates {
            let summary = summaries.get(&message.conversation_id);
            let class = alert_class(&message);
            let mention = mentioned(&message, user_id);
            let decision = decide(&DecideInput {
                class,
                mentioned: mention,
                from_self: false,
                conversation: conversation_prefs(summary),
                user,
                local_time,
                // 链路在线时看不到别的设备是否在用；多端延迟只在服务端做。
                other_device_attentive: false,
            });
            let Some(tier) = decision.without_delay().tier() else {
                continue;
            };
            let kind = conversation_kind(conversation_type_of(summary, &message));
            admitted.push(Candidate {
                message,
                tier,
                mentioned: mention == Mentioned::Yes,
                urgent: class == AlertClass::Urgent,
                kind,
            });
        }
        if admitted.is_empty() {
            return;
        }
        let (orders, tails) = {
            let mut state = self.lock_state();
            if state.user_id != user_id {
                state.user_id = user_id.to_owned();
            }
            match surface {
                AlertSurface::InApp => {
                    (self.coalesce_in_app(&mut state, admitted, now), Vec::new())
                }
                AlertSurface::System => self.admit_system(&mut state, admitted, now),
            }
        };
        for (conversation_id, due_ms) in tails {
            self.schedule_tail(conversation_id, due_ms);
        }
        self.present(orders, &summaries, user_id).await;
    }

    /// 前台：同一会话一条横幅，条数累加，不过闸门；一批里最多响一次，两次响铃至少隔 1 秒。
    fn coalesce_in_app(
        &self,
        state: &mut CenterState,
        admitted: Vec<Candidate>,
        now: i64,
    ) -> Vec<Order> {
        // 按会话归拢，会话的先后取它最后一条的位置。
        let mut grouped: Vec<(String, Vec<Candidate>)> = Vec::new();
        for candidate in admitted {
            let conversation_id = candidate.message.conversation_id.clone();
            match grouped.iter().position(|(id, _)| *id == conversation_id) {
                Some(index) => {
                    let mut entry = grouped.remove(index);
                    entry.1.push(candidate);
                    grouped.push(entry);
                }
                None => grouped.push((conversation_id, vec![candidate])),
            }
        }
        let CenterState {
            slots,
            last_in_app_ring_ms,
            ..
        } = state;
        let mut orders = Vec::with_capacity(grouped.len());
        let mut rang = false;
        for (conversation_id, mut group) in grouped {
            let Some(last) = group.pop() else {
                continue;
            };
            let messages = group.len() as u32 + 1;
            let tier = group
                .iter()
                .map(|candidate| candidate.tier)
                .fold(last.tier, louder);
            let urgent = last.urgent || group.iter().any(|candidate| candidate.urgent);
            let max_seq = group
                .iter()
                .map(|candidate| candidate.message.conversation_seq)
                .fold(last.message.conversation_seq, std::cmp::max);
            let Some(slot) = self.slot(slots, &conversation_id, last.kind, now) else {
                continue;
            };
            slot.max_seq = slot.max_seq.max(max_seq);
            slot.in_app_count = slot.in_app_count.saturating_add(messages);
            let sound = tier != Tier::Quiet
                && !rang
                && now.saturating_sub(*last_in_app_ring_ms) >= IN_APP_RING_GAP_MS;
            if sound {
                rang = true;
                *last_in_app_ring_ms = now;
            }
            orders.push(Order::Message {
                collapse_id: slot.collapse_id.clone(),
                count: slot.in_app_count,
                candidate: Box::new(Candidate { urgent, ..last }),
                sound,
                surface: AlertSurface::InApp,
                replace: false,
            });
        }
        orders
    }

    /// 后台：逐条过闸门。立即发的出文案；吞并的记下最新一条，到间隔结束补一次尾部刷新。
    fn admit_system(
        &self,
        state: &mut CenterState,
        admitted: Vec<Candidate>,
        now: i64,
    ) -> (Vec<Order>, Vec<(String, i64)>) {
        let CenterState { device, slots, .. } = state;
        let mut orders = Vec::new();
        let mut tails = Vec::new();
        for candidate in admitted {
            let conversation_id = candidate.message.conversation_id.clone();
            let Some(slot) = self.slot(slots, &conversation_id, candidate.kind, now) else {
                continue;
            };
            slot.max_seq = slot.max_seq.max(candidate.message.conversation_seq);
            let input = GateInput {
                kind: slot.kind,
                tier: candidate.tier,
                tag: slot.tag,
            };
            let admitted = self.gate.admit(now, input, &mut slot.gate, device);
            match admitted.slot {
                SlotAction::Loud { count } | SlotAction::Quiet { count } => {
                    slot.latest = None;
                    orders.push(Order::Message {
                        collapse_id: slot.collapse_id.clone(),
                        count,
                        sound: matches!(admitted.slot, SlotAction::Loud { .. }),
                        candidate: Box::new(candidate),
                        surface: AlertSurface::System,
                        replace: false,
                    });
                }
                SlotAction::Defer { due_ms, .. } => {
                    slot.latest = Some(Box::new(candidate));
                    slot.tail_due_ms = Some(due_ms);
                    tails.push((conversation_id, due_ms));
                }
                SlotAction::CountOnly { .. } => {
                    slot.latest = Some(Box::new(candidate));
                }
            }
            if let Some(digest) = admitted.digest {
                orders.push(Order::Digest {
                    conversations: u32::from(digest.conversations),
                    messages: digest.messages,
                });
            }
        }
        (orders, tails)
    }

    /// 取（必要时新建）一个会话的通知状态。还没定下租户、算不出合并键时为 `None`。
    fn slot<'a>(
        &self,
        slots: &'a mut HashMap<String, Slot>,
        conversation_id: &str,
        kind: ConversationKind,
        now: i64,
    ) -> Option<&'a mut Slot> {
        if !slots.contains_key(conversation_id) {
            let collapse_id = self.alerts.collapse_id(conversation_id)?;
            if slots.len() >= MAX_SLOTS {
                let oldest = slots
                    .iter()
                    .filter(|(_, slot)| slot.shown.is_none() && slot.tail_due_ms.is_none())
                    .min_by_key(|(_, slot)| slot.touched_ms)
                    .map(|(id, _)| id.clone());
                if let Some(oldest) = oldest {
                    slots.remove(&oldest);
                }
            }
            slots.insert(
                conversation_id.to_owned(),
                Slot {
                    collapse_id,
                    kind,
                    tag: ConversationTag::of(conversation_id.as_bytes()),
                    gate: SlotState::default(),
                    max_seq: 0,
                    latest: None,
                    tail_due_ms: None,
                    shown: None,
                    in_app_count: 0,
                    touched_ms: now,
                },
            );
        }
        let slot = slots.get_mut(conversation_id)?;
        slot.kind = kind;
        slot.touched_ms = now;
        Some(slot)
    }

    /// 本地会话摘要（偏好、标题、类型）。读不到的会话按默认偏好放行、用通用标题。
    async fn summaries(&self, messages: &[IMMessage]) -> HashMap<String, Conversation> {
        let mut ids: Vec<String> = Vec::new();
        for message in messages {
            if !ids.contains(&message.conversation_id) {
                ids.push(message.conversation_id.clone());
            }
        }
        match self.stores.conversations.get_many(&ids).await {
            Ok(rows) => rows
                .into_iter()
                .map(|row| (row.conversation_id.clone(), row))
                .collect(),
            Err(error) => {
                tracing::warn!(%error, "reading conversations for notifications failed; using defaults");
                HashMap::new()
            }
        }
    }

    /// 出文案、记进提示台账、交给宿主。
    async fn present(
        &self,
        orders: Vec<Order>,
        summaries: &HashMap<String, Conversation>,
        user_id: &str,
    ) {
        if orders.is_empty() {
            return;
        }
        let preview = self
            .cached_settings()
            .map(|settings| settings.preview)
            .unwrap_or_default();
        let book = self.names(&orders, summaries, user_id).await;
        for order in orders {
            match order {
                Order::Message {
                    candidate,
                    collapse_id,
                    count,
                    sound,
                    surface,
                    replace,
                } => {
                    let message = &candidate.message;
                    let summary = summaries.get(&message.conversation_id);
                    let conversation_type = conversation_type_of(summary, message);
                    let title = conversation_title(&book, summary, message, user_id, self.lang);
                    let sender = member_nicknames_apply(conversation_type)
                        .then(|| sender_name(&book, summary, message, conversation_type))
                        .flatten();
                    let copy = AlertCopy::message(
                        message,
                        &MessageFacts {
                            title: title.as_deref(),
                            sender: sender.as_deref(),
                            mentioned: candidate.mentioned,
                            count,
                            preview,
                        },
                    );
                    let seq = message.conversation_seq;
                    if let Err(skip) = self.alerts.claim_alert(&collapse_id, seq, replace) {
                        tracing::debug!(?skip, seq, "notification already shown elsewhere");
                        continue;
                    }
                    let text = copy.render(self.lang);
                    let channel_name = channel(&candidate, sound);
                    self.note_shown(
                        &message.conversation_id,
                        Shown {
                            seq,
                            message_id: message.server_id.clone(),
                            title: copy.plain_title().map(str::to_owned),
                            count,
                            surface,
                        },
                    );
                    self.publish_presentation(NotificationPresentation {
                        thread_id: collapse_id.clone(),
                        collapse_id,
                        conversation_id: message.conversation_id.clone(),
                        message_id: message.server_id.clone(),
                        seq,
                        title: text.title,
                        body: text.body,
                        count: count.max(1),
                        sound,
                        channel: channel_name.to_owned(),
                        surface: surface.as_str().to_owned(),
                    });
                }
                Order::Digest {
                    conversations,
                    messages,
                } => {
                    let text = AlertCopy::digest(conversations, messages).render(self.lang);
                    self.lock_state().digest_shown = true;
                    self.publish_presentation(NotificationPresentation {
                        collapse_id: DIGEST_COLLAPSE_ID.to_owned(),
                        thread_id: DIGEST_COLLAPSE_ID.to_owned(),
                        conversation_id: String::new(),
                        message_id: String::new(),
                        seq: 0,
                        title: text.title,
                        body: text.body,
                        count: messages.max(1),
                        sound: true,
                        channel: "messages".to_owned(),
                        surface: AlertSurface::System.as_str().to_owned(),
                    });
                }
            }
        }
    }

    /// 文案要用的名字：会话标题、发送者，一次读齐。
    async fn names(
        &self,
        orders: &[Order],
        summaries: &HashMap<String, Conversation>,
        user_id: &str,
    ) -> NameBook {
        let mut request = NameRequest::default();
        for order in orders {
            let Order::Message { candidate, .. } = order else {
                continue;
            };
            let message = &candidate.message;
            let summary = summaries.get(&message.conversation_id);
            if let Some(summary) = summary {
                request.conversation(summary, user_id);
            }
            request.person(
                &message.conversation_id,
                conversation_type_of(summary, message),
                &message.sender_id,
            );
        }
        let profiles = self.stores.user_profiles_or_memory();
        request
            .load(profiles.as_ref(), Some(self.stores.conversations.as_ref()))
            .await
    }

    fn note_shown(&self, conversation_id: &str, shown: Shown) {
        if let Some(slot) = self.lock_state().slots.get_mut(conversation_id) {
            slot.shown = Some(shown);
        }
    }

    fn publish_presentation(&self, presentation: NotificationPresentation) {
        self.bus
            .publish(SdkEvent::Notification(NotificationEvent::Present(
                Box::new(presentation),
            )));
    }

    fn publish_clear(&self, collapse_ids: Vec<String>, all: bool) {
        if collapse_ids.is_empty() && !all {
            return;
        }
        self.bus
            .publish(SdkEvent::Notification(NotificationEvent::Clear {
                collapse_ids,
                all,
            }));
    }

    // ---------------------------------------------------------------------------------------
    // 尾部刷新
    // ---------------------------------------------------------------------------------------

    fn schedule_tail(&self, conversation_id: String, due_ms: i64) {
        let wait = due_ms.saturating_sub(now_millis() as i64).max(0) as u64;
        let center = self.me.clone();
        spawn_background(async move {
            delay(Duration::from_millis(wait)).await;
            if let Some(center) = center.upgrade() {
                center.on_tail_due(conversation_id, due_ms).await;
            }
        });
    }

    /// 尾部刷新到期：静默替换成最新一条与正确计数。被读、被取消、换了登记的到期不算数。
    async fn on_tail_due(&self, conversation_id: String, due_ms: i64) {
        if self.ended() {
            return;
        }
        // 定时器可能比墙上时钟早醒几毫秒：按登记的时刻算，不会因此再推一轮。
        let now = (now_millis() as i64).max(due_ms);
        let mut reschedule = None;
        let due = {
            let mut state = self.lock_state();
            let CenterState { device, slots, .. } = &mut *state;
            let Some(slot) = slots.get_mut(&conversation_id) else {
                return;
            };
            if slot.tail_due_ms != Some(due_ms) {
                return;
            }
            slot.tail_due_ms = None;
            match self.gate.on_due(now, &mut slot.gate, device) {
                DueAction::Tail { count } => slot
                    .latest
                    .take()
                    .map(|candidate| (candidate, count, slot.collapse_id.clone())),
                DueAction::Defer { due_ms } => {
                    slot.tail_due_ms = Some(due_ms);
                    reschedule = Some(due_ms);
                    None
                }
                DueAction::Nothing => None,
            }
        };
        if let Some(due_ms) = reschedule {
            self.schedule_tail(conversation_id, due_ms);
            return;
        }
        let Some((candidate, count, collapse_id)) = due else {
            return;
        };
        // 用户回到了前台（系统通知已对过账），或提示已交给推送：不再补发系统通知。
        if self.reach.alert_surface() != Some(AlertSurface::System) {
            return;
        }
        let summaries = self
            .summaries(std::slice::from_ref(&candidate.message))
            .await;
        let user_id = self.lock_state().user_id.clone();
        let order = Order::Message {
            candidate,
            collapse_id,
            count,
            sound: false,
            surface: AlertSurface::System,
            replace: true,
        };
        self.present(vec![order], &summaries, &user_id).await;
    }
}

/// 一条实时到达的消息值不值得进决策：别人发的、指向一个会话与一个 seq、没被撤回或删除。
fn alertable(message: &IMMessage, user_id: &str) -> bool {
    !user_id.is_empty()
        && !message.sender_id.is_empty()
        && message.sender_id != user_id
        && !message.conversation_id.trim().is_empty()
        && message.conversation_seq > 0
        && !message.is_recalled
        && message.status != flare_proto::common::MessageStatus::Deleted as i32
}

/// 提示等级，与服务端分类器相同：通知类内容要响铃的是普通消息，只计角标或只进列表的是静默更新，
/// 三者皆无不提示；其余内容一律普通。
fn alert_class(message: &IMMessage) -> AlertClass {
    match message.content.as_ref() {
        Some(Elem::Notification(notice)) => {
            if notice.play_sound {
                AlertClass::Normal
            } else if !(notice.title.trim().is_empty()
                && notice.body.trim().is_empty()
                && !notice.show_in_list
                && !notice.show_badge)
            {
                AlertClass::Silent
            } else {
                AlertClass::None
            }
        }
        _ => AlertClass::Normal,
    }
}

/// 是否点名到这个用户。内容还没解开（E2EE 密文、解码失败）时说不清。
fn mentioned(message: &IMMessage, user_id: &str) -> Mentioned {
    if message.mention_all || message.mention_users.iter().any(|id| id == user_id) {
        return Mentioned::Yes;
    }
    match message.content.as_ref() {
        None => Mentioned::Unknown,
        Some(Elem::Placeholder(placeholder)) if placeholder.reason == E2EE_PLACEHOLDER_REASON => {
            Mentioned::Unknown
        }
        Some(_) => Mentioned::No,
    }
}

/// 会话偏好：免打扰、仅 @我；大群没显式开启「全部消息通知」时也只提示 @我。本地没有这个会话时放行。
fn conversation_prefs(summary: Option<&Conversation>) -> ConversationPrefs {
    let Some(conversation) = summary else {
        return ConversationPrefs::default();
    };
    let large_group = conversation.conversation_type == ConversationType::Group
        && conversation.members_count > LARGE_GROUP_MEMBERS
        && !notify_all(conversation);
    ConversationPrefs {
        muted: conversation.is_muted,
        mention_only: conversation.is_mention_only.unwrap_or(false) || large_group,
    }
}

/// 用户级设置转成规则的输入。没读到设置时用默认值（放行）；越界的勿扰时段当作没有。
fn user_prefs(settings: Option<&UserNotificationSettings>) -> UserPrefs {
    let Some(settings) = settings else {
        return UserPrefs::default();
    };
    let quiet_hours = settings
        .quiet_hours
        .filter(|quiet_hours| quiet_hours.is_valid())
        .map(|quiet_hours| QuietWindow {
            start_minute: quiet_hours.start_minute as u16,
            end_minute: quiet_hours.end_minute as u16,
            weekdays: quiet_hours.weekdays as u8,
            allow_mentions: quiet_hours.allow_mentions,
        });
    UserPrefs {
        quiet_hours,
        preview: match settings.preview {
            NotificationPreview::Full => PreviewMode::Full,
            NotificationPreview::SenderOnly => PreviewMode::SenderOnly,
            NotificationPreview::Hidden => PreviewMode::Hidden,
        },
        multi_device: match settings.multi_device {
            MultiDeviceAlertPolicy::Delay => MultiDevicePolicy::Delay,
            MultiDeviceAlertPolicy::Immediate => MultiDevicePolicy::Immediate,
            MultiDeviceAlertPolicy::BadgeOnly => MultiDevicePolicy::BadgeOnly,
        },
    }
}

fn conversation_type_of(summary: Option<&Conversation>, message: &IMMessage) -> ConversationType {
    summary
        .map(|conversation| conversation.conversation_type)
        .filter(|kind| *kind != ConversationType::Unspecified)
        .unwrap_or_else(|| ConversationType::from_proto_int(message.conversation_type))
}

/// 用哪条响铃阶梯：多人会话（群、频道、广播）用群的，其余按单聊。
fn conversation_kind(conversation_type: ConversationType) -> ConversationKind {
    match conversation_type {
        ConversationType::Group | ConversationType::Channel | ConversationType::Broadcast => {
            ConversationKind::Group
        }
        _ => ConversationKind::Direct,
    }
}

/// 两个档位里更响的那个。
fn louder(a: Tier, b: Tier) -> Tier {
    let rank = |tier: Tier| match tier {
        Tier::Quiet => 0,
        Tier::Normal => 1,
        Tier::Priority => 2,
    };
    if rank(b) > rank(a) { b } else { a }
}

/// 安卓通知渠道：紧急、@我各有一个；不响的用安静的渠道；其余是普通消息。
fn channel(candidate: &Candidate, sound: bool) -> &'static str {
    if candidate.urgent {
        "urgent"
    } else if candidate.mentioned {
        "mentions"
    } else if !sound {
        "quiet"
    } else {
        "messages"
    }
}

/// 会话标题：本地投影出的标题；本地还没有这个会话时，单聊用发送者的名字。
fn conversation_title(
    book: &NameBook,
    summary: Option<&Conversation>,
    message: &IMMessage,
    user_id: &str,
    lang: Lang,
) -> Option<String> {
    match summary {
        Some(summary) => {
            let mut conversation = summary.clone();
            book.project_conversation(&mut conversation, user_id);
            display_text(&conversation.title, lang)
        }
        None if ConversationType::from_proto_int(message.conversation_type)
            == ConversationType::Single =>
        {
            let candidates = book.candidates(
                &message.conversation_id,
                &message.sender_id,
                "",
                &message.sender_name,
            );
            display_text(
                &resolve_person_name(&message.sender_id, &candidates, None),
                lang,
            )
        }
        None => None,
    }
}

/// 群里的发送者显示名（会话内昵称、备注、资料名、消息快照）；都没有时为 `None`。
fn sender_name(
    book: &NameBook,
    summary: Option<&Conversation>,
    message: &IMMessage,
    conversation_type: ConversationType,
) -> Option<String> {
    let row_nickname = summary
        .and_then(|conversation| {
            conversation
                .member_preview
                .iter()
                .chain(conversation.participants.iter())
                .find(|member| member.user_id == message.sender_id)
        })
        .map(|member| member.nickname.as_str())
        .unwrap_or_default();
    book.person(
        &message.conversation_id,
        conversation_type,
        &message.sender_id,
        row_nickname,
        &message.sender_name,
    )
    .text()
    .map(str::to_owned)
}

/// 名字的文字：有名字用名字，无名群用成员名拼接，其余没有文字。
fn display_text(name: &DisplayName, lang: Lang) -> Option<String> {
    match name {
        DisplayName::Named { text, .. } => Some(text.clone()),
        DisplayName::Members { names, .. } if !names.is_empty() => Some(names.join(match lang {
            Lang::Zh => "、",
            Lang::En => ", ",
        })),
        _ => None,
    }
}

fn badge_of(conversations: &[Conversation]) -> u32 {
    u32::try_from(total_unread(conversations)).unwrap_or(u32::MAX)
}

#[path = "center_corrections.rs"]
mod corrections;
#[path = "center_settings.rs"]
mod settings;
#[cfg(not(target_arch = "wasm32"))]
#[path = "center_snapshot.rs"]
mod snapshot;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "center_tests.rs"]
mod tests;
