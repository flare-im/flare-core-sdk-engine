//! 远程推送的无界面入口：`handleRemoteMessage` 与 `resolveNotificationTap`。
//!
//! 平台推送包收到一条推送（APNs、FCM、个推）就把载荷交给这里，引擎在 150 ms 内答复该怎么展示，
//! 不等网络。链路在线时提示由引擎负责（谁确认谁负责提示），链路不在时由推送通道负责；两条路用同一个
//! 合并键，所以这里按合并键和 seq 去重：链路上已经提示过的、重复或乱序到达的推送不再响，本地已读
//! 覆盖了的改为清除。平台给了后台执行时间时顺带唤醒链路做一次增量同步，答复不等它。
//!
//! 属于当前账号的推送都记一条到达回执（路径 `app`，见 [`crate::client::push_receipts`]）；唤醒补完同步
//! 之后把排着的回执报上去。
//!
//! 浏览器没有系统推送：wasm 构建一律返回 OperationNotSupported，不做任何判断。
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::application::notification::{AlertLedger, AlertSkip};
use crate::client::push_receipts::PushReceipts;
use crate::domain::ConversationStore;
pub use crate::model::notification::NotificationPresentation;
use crate::runtime::link::{LinkHandle, WakeSource};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::{normalize_tenant_id, spawn_background, timeout};

/// 读本地已读进度的时限。整个调用要在 150 ms 内答复：读不完就当没读到，照常展示。
const READ_STATE_CAP: Duration = Duration::from_millis(100);
/// 合并键、分组键的长度上限。
const MAX_KEY_LEN: usize = 128;

/// One remote push the platform push module received.
#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandleRemoteMessageRequest {
    /// Push channel that delivered it: apns, fcm or getui.
    pub provider: String,
    /// The push payload flattened to strings (snake_case keys): uid, tenant_id, conversation_id,
    /// message_id, seq, collapse_id, thread_id, disposition, alert_class, count, badge, title,
    /// body, sound. Keys the engine does not know are ignored.
    pub payload: BTreeMap<String, String>,
    /// Background execution time the platform holds for this push, in milliseconds. Above zero
    /// the engine also wakes the link for one incremental sync within it; the answer never waits
    /// for that.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_ms: Option<u32>,
}

impl std::fmt::Debug for HandleRemoteMessageRequest {
    // 载荷里有消息预览：只打印键。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandleRemoteMessageRequest")
            .field("provider", &self.provider)
            .field("payload_keys", &self.payload.keys().collect::<Vec<_>>())
            .field("budget_ms", &self.budget_ms)
            .finish()
    }
}

/// How the platform should show one remote push.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemoteMessageOutcome {
    /// present (a new notification, may sound), update (silently replace the notification with
    /// the same collapse id), clear (remove the notifications in clearCollapseIds), badgeOnly
    /// (only set the badge) or ignore.
    pub action: String,
    /// What to show, for present and update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<NotificationPresentation>,
    /// Collapse ids of the notifications to remove, for clear.
    #[serde(default)]
    pub clear_collapse_ids: Vec<String>,
    /// Absolute app badge, when the payload carried one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub badge: Option<u32>,
    /// Why the push is ignored (diagnostic): otherAccount, notLoggedIn, stale, alreadyAlerted or
    /// malformed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// The payload of a notification the user tapped.
#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveNotificationTapRequest {
    /// The push payload flattened to strings, as in handleRemoteMessage.
    pub payload: BTreeMap<String, String>,
}

impl std::fmt::Debug for ResolveNotificationTapRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolveNotificationTapRequest")
            .field("payload_keys", &self.payload.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Where a tapped notification leads; every field is absent when the payload is not for the
/// current account.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationTapResult {
    /// Conversation to open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    /// Message to scroll to, when the payload named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Conversation seq of that message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
}

/// 推送任务的处置（载荷 `disposition`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Loud,
    Quiet,
    Clear,
    BadgeOnly,
    RecallReplace,
    Digest,
}

impl Disposition {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "loud" => Self::Loud,
            "quiet" => Self::Quiet,
            "clear" => Self::Clear,
            "badge_only" => Self::BadgeOnly,
            "recall_replace" => Self::RecallReplace,
            "digest" => Self::Digest,
            _ => return None,
        })
    }

    /// 要展示一条通知（新通知或原地替换）。
    fn shows(self) -> bool {
        matches!(
            self,
            Self::Loud | Self::Quiet | Self::RecallReplace | Self::Digest
        )
    }

    /// 不指向某个会话里的某条消息：只改角标的，以及跨会话的摘要（设备级合并键，不带会话与 seq，
    /// 不同会话的 seq 不可比）。
    fn spans_conversations(self) -> bool {
        matches!(self, Self::BadgeOnly | Self::Digest)
    }
}

/// 提示等级（载荷 `alert_class`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlertClass {
    None,
    Silent,
    Normal,
    Mention,
    Urgent,
}

impl AlertClass {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "none" => Self::None,
            "silent" => Self::Silent,
            "normal" => Self::Normal,
            "mention" => Self::Mention,
            "urgent" => Self::Urgent,
            _ => return None,
        })
    }
}

/// 载荷里认得的字段。
struct RemotePayload<'a> {
    uid: &'a str,
    tenant_id: String,
    conversation_id: &'a str,
    message_id: &'a str,
    seq: u64,
    collapse_id: &'a str,
    thread_id: &'a str,
    disposition: Disposition,
    alert_class: AlertClass,
    count: u32,
    badge: Option<u32>,
    title: &'a str,
    body: &'a str,
    sound: bool,
}

impl<'a> RemotePayload<'a> {
    /// 取出认得的字段。缺了必需的键，或某个值认不得（未知的处置或提示等级、数字解析不了）时为
    /// `None`，按 malformed 忽略：服务端总是带齐这些键，认不得说明载荷不是这一版的。
    fn parse(payload: &'a BTreeMap<String, String>) -> Option<Self> {
        let text = move |key: &str| {
            payload
                .get(key)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
        };
        let uid = text("uid")?;
        let tenant_id = normalize_tenant_id(text("tenant_id")?);
        let collapse_id = text("collapse_id").filter(|value| plain_key(value))?;
        let disposition = Disposition::parse(text("disposition")?)?;
        // 只改角标的、跨会话的摘要不指向哪个会话的哪条消息；其余处置都要有会话与 seq 才能去重、
        // 才能判断已读。
        let conversation_id = match text("conversation_id") {
            Some(value) => value,
            None if disposition.spans_conversations() => "",
            None => return None,
        };
        let seq = match text("seq") {
            Some(value) => value.parse::<u64>().ok()?,
            None => 0,
        };
        if seq == 0 && !disposition.spans_conversations() {
            return None;
        }
        let alert_class = match text("alert_class") {
            Some(value) => AlertClass::parse(value)?,
            None => AlertClass::Normal,
        };
        let count = match text("count") {
            Some(value) => value.parse::<u32>().ok()?.max(1),
            None => 1,
        };
        let badge = match text("badge") {
            Some(value) => Some(value.parse::<u32>().ok()?),
            None => None,
        };
        let sound = match text("sound") {
            Some("1") => true,
            Some("0") | None => false,
            Some(_) => return None,
        };
        let thread_id = match text("thread_id") {
            Some(value) if plain_key(value) => value,
            Some(_) => return None,
            None => collapse_id,
        };
        Some(Self {
            uid,
            tenant_id,
            conversation_id,
            message_id: text("message_id").unwrap_or(""),
            seq,
            collapse_id,
            thread_id,
            disposition,
            alert_class,
            count,
            badge,
            title: payload.get("title").map_or("", String::as_str),
            body: payload.get("body").map_or("", String::as_str),
            sound,
        })
    }

    /// 载荷是不是发给这个账号（用户与租户）的。
    fn owned_by(&self, user_id: &str, tenant_id: &str) -> bool {
        self.uid == user_id && self.tenant_id == tenant_id
    }
}

/// 合并键、分组键：不长、没有空白与控制字符。
fn plain_key(value: &str) -> bool {
    value.len() <= MAX_KEY_LEN && !value.chars().any(|c| c.is_control() || c.is_whitespace())
}

fn not_supported() -> FlareError {
    FlareError::localized(
        ErrorCode::OperationNotSupported,
        "remote push handling requires a native host",
    )
}

fn outcome(action: &str, badge: Option<u32>) -> RemoteMessageOutcome {
    RemoteMessageOutcome {
        action: action.to_owned(),
        presentation: None,
        clear_collapse_ids: Vec::new(),
        badge,
        reason: None,
    }
}

fn ignored(reason: &str, badge: Option<u32>) -> RemoteMessageOutcome {
    RemoteMessageOutcome {
        reason: Some(reason.to_owned()),
        ..outcome("ignore", badge)
    }
}

fn cleared(collapse_id: &str, badge: Option<u32>) -> RemoteMessageOutcome {
    RemoteMessageOutcome {
        clear_collapse_ids: vec![collapse_id.to_owned()],
        ..outcome("clear", badge)
    }
}

fn skipped(skip: AlertSkip, badge: Option<u32>) -> RemoteMessageOutcome {
    ignored(
        match skip {
            AlertSkip::AlreadyAlerted => "alreadyAlerted",
            AlertSkip::Stale => "stale",
        },
        badge,
    )
}

/// 安卓通知渠道：@我、紧急各有一个；静默更新用安静的渠道；其余都是普通消息。
fn channel(payload: &RemotePayload<'_>) -> &'static str {
    match payload.alert_class {
        AlertClass::Mention => "mentions",
        AlertClass::Urgent => "urgent",
        _ if payload.disposition == Disposition::Quiet => "quiet",
        _ => "messages",
    }
}

/// 一条（已确认属于当前账号的）推送该怎么展示。`read_seq` 是本地已读进度，读不到为 `None`。
fn decide(
    payload: &RemotePayload<'_>,
    alerts: &AlertLedger,
    read_seq: Option<u64>,
    foreground: bool,
) -> RemoteMessageOutcome {
    let badge = payload.badge;
    let collapse_id = payload.collapse_id;
    match payload.disposition {
        Disposition::BadgeOnly => outcome("badgeOnly", badge),
        Disposition::Clear => match alerts.claim_clear(collapse_id, payload.seq) {
            Ok(()) => cleared(collapse_id, badge),
            Err(skip) => skipped(skip, badge),
        },
        // 摘要跨会话、不带 seq：不进按会话 seq 记账的台账，也没有本地已读可比。设备级合并键让新的
        // 一条覆盖旧的，同一条重投也只是原地覆盖。
        Disposition::Digest => presented(payload, true, foreground, badge),
        disposition => {
            // 本地已读覆盖了这条：不该再提示，同一合并键上的通知也该撤掉。展示着的若已是已读之后的
            // 新消息就不撤（台账判定为过时）。
            if let Some(read_seq) = read_seq.filter(|read_seq| *read_seq >= payload.seq) {
                return match alerts.claim_clear(collapse_id, read_seq) {
                    Ok(()) => cleared(collapse_id, badge),
                    Err(skip) => skipped(skip, badge),
                };
            }
            let replace = disposition == Disposition::RecallReplace;
            if let Err(skip) = alerts.claim_alert(collapse_id, payload.seq, replace) {
                return skipped(skip, badge);
            }
            presented(payload, disposition == Disposition::Loud, foreground, badge)
        }
    }
}

/// 展示意图：`present` 为真时是要响的新通知，否则是原地静默替换。
fn presented(
    payload: &RemotePayload<'_>,
    present: bool,
    foreground: bool,
    badge: Option<u32>,
) -> RemoteMessageOutcome {
    RemoteMessageOutcome {
        presentation: Some(NotificationPresentation {
            collapse_id: payload.collapse_id.to_owned(),
            thread_id: payload.thread_id.to_owned(),
            conversation_id: payload.conversation_id.to_owned(),
            message_id: payload.message_id.to_owned(),
            seq: payload.seq,
            title: payload.title.to_owned(),
            body: payload.body.to_owned(),
            count: payload.count,
            sound: present && payload.sound,
            channel: channel(payload).to_owned(),
            surface: if foreground { "inApp" } else { "system" }.to_owned(),
        }),
        ..outcome(if present { "present" } else { "update" }, badge)
    }
}

/// 本地已读进度（会话的 last_read_seq）。限时读：读不完、读失败、本地没有这个会话都当没有。
async fn local_read_seq(
    conversations: &dyn ConversationStore,
    conversation_id: &str,
) -> Option<u64> {
    match timeout(READ_STATE_CAP, conversations.get(conversation_id)).await {
        Ok(Ok(conversation)) => conversation.map(|conversation| conversation.last_read_seq),
        Ok(Err(error)) => {
            tracing::debug!(%error, "reading the read state for a push failed; presenting without it");
            None
        }
        Err(_) => {
            tracing::debug!("reading the read state for a push timed out; presenting without it");
            None
        }
    }
}

/// 点开的通知去哪个会话、哪条消息。不是当前账号的一律什么都不给。
fn resolve_tap(
    account: Option<(&str, &str)>,
    payload: &BTreeMap<String, String>,
) -> NotificationTapResult {
    let text = move |key: &str| {
        payload
            .get(key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    };
    let (Some((user_id, tenant_id)), Some(uid), Some(tenant), Some(conversation_id)) = (
        account,
        text("uid"),
        text("tenant_id"),
        text("conversation_id"),
    ) else {
        return NotificationTapResult::default();
    };
    if uid != user_id || normalize_tenant_id(tenant) != tenant_id {
        return NotificationTapResult::default();
    }
    NotificationTapResult {
        conversation_id: Some(conversation_id.to_owned()),
        message_id: text("message_id").map(ToOwned::to_owned),
        seq: text("seq")
            .and_then(|seq| seq.parse::<u64>().ok())
            .filter(|seq| *seq > 0),
    }
}

/// 收推送那一刻要用到的会话事实。
#[derive(Clone)]
pub(crate) struct InboxSession {
    pub user_id: String,
    /// 规整过的租户（与载荷里的比较前同样规整）。
    pub tenant_id: String,
    /// 本地已读进度从这里读。
    pub conversations: Arc<dyn ConversationStore>,
    /// 这一个登录会话的提示台账：链路确认了的也记在里面。
    pub alerts: Arc<AlertLedger>,
    /// 推送唤醒交给链路状态机。
    pub link: LinkHandle,
}

/// 推送入口持有的会话事实：登录准备好、建连开始时装上，登出、断开时卸下。读它不拿客户端的锁，
/// 推送入口因此不会被登录、建连挡住。
#[derive(Default)]
pub(crate) struct PushInbox {
    session: Mutex<Option<InboxSession>>,
    /// 到达回执：跨登录保留（队列在数据根下）。
    receipts: Arc<PushReceipts>,
}

impl PushInbox {
    fn lock(&self) -> MutexGuard<'_, Option<InboxSession>> {
        self.session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn attach(&self, session: InboxSession) {
        *self.lock() = Some(session);
    }

    pub fn detach(&self) {
        *self.lock() = None;
    }

    fn session(&self) -> Option<InboxSession> {
        self.lock().clone()
    }

    /// 到达回执的队列与上报。
    pub fn receipts(&self) -> &Arc<PushReceipts> {
        &self.receipts
    }

    /// `handleRemoteMessage`：150 ms 内答复，不等网络。`foreground` 是宿主最近报的前后台，决定
    /// 展示在系统通知还是应用内。
    pub async fn handle(
        &self,
        request: HandleRemoteMessageRequest,
        foreground: bool,
    ) -> Result<RemoteMessageOutcome> {
        if cfg!(target_arch = "wasm32") {
            return Err(not_supported());
        }
        if !matches!(request.provider.as_str(), "apns" | "fcm" | "getui") {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "unknown push provider",
            ));
        }
        let Some(payload) = RemotePayload::parse(&request.payload) else {
            return Ok(ignored("malformed", None));
        };
        let Some(session) = self.session() else {
            return Ok(ignored("notLoggedIn", None));
        };
        if !payload.owned_by(&session.user_id, &session.tenant_id) {
            return Ok(ignored("otherAccount", None));
        }
        // 推送到了：记一条到达回执（重复、乱序到达的也算到达，按合并键与 seq 去重）。
        self.receipts
            .record_app(&request.provider, &request.payload);
        if let Some(budget_ms) = request.budget_ms.filter(|budget_ms| *budget_ms > 0) {
            // 唤醒只交给链路状态机，不等建连；展示意图不依赖它。补完同步之后顺带把回执报上去。
            let link = session.link.clone();
            let receipts = self.receipts.clone();
            spawn_background(async move {
                let budget = Duration::from_millis(u64::from(budget_ms));
                if link.wake_until_ready(WakeSource::Push, budget).await {
                    receipts.flush().await;
                }
            });
        }
        let read_seq = if payload.disposition.shows() && !payload.conversation_id.is_empty() {
            local_read_seq(&*session.conversations, payload.conversation_id).await
        } else {
            None
        };
        Ok(decide(&payload, &session.alerts, read_seq, foreground))
    }

    /// `resolveNotificationTap`：点开的通知去哪个会话、哪条消息；不是当前账号的什么都不给。
    pub fn resolve_tap(
        &self,
        request: &ResolveNotificationTapRequest,
    ) -> Result<NotificationTapResult> {
        if cfg!(target_arch = "wasm32") {
            return Err(not_supported());
        }
        let session = self.session();
        Ok(resolve_tap(
            session
                .as_ref()
                .map(|session| (session.user_id.as_str(), session.tenant_id.as_str())),
            &request.payload,
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::application::notification::conversation_collapse_id;
    use crate::domain::ConversationWriter;
    use crate::infrastructure::persistence::memory_im::MemoryConversationStore;
    use crate::model::Conversation;

    fn payload(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    /// 一条普通的新消息推送：alice、租户 0、会话 c1、seq 5。
    fn loud(seq: &str) -> BTreeMap<String, String> {
        payload(&[
            ("uid", "alice"),
            ("tenant_id", "0"),
            ("conversation_id", "c1"),
            ("message_id", "m5"),
            ("seq", seq),
            ("collapse_id", "c:0123456789abcdef0123456789abcdef"),
            ("thread_id", "c:0123456789abcdef0123456789abcdef"),
            ("disposition", "loud"),
            ("alert_class", "normal"),
            ("count", "2"),
            ("badge", "7"),
            ("title", "Bob"),
            ("body", "hello"),
            ("sound", "1"),
        ])
    }

    fn with(
        mut payload: BTreeMap<String, String>,
        key: &str,
        value: &str,
    ) -> BTreeMap<String, String> {
        payload.insert(key.to_owned(), value.to_owned());
        payload
    }

    fn decided(
        payload: &BTreeMap<String, String>,
        alerts: &AlertLedger,
        read_seq: Option<u64>,
        foreground: bool,
    ) -> RemoteMessageOutcome {
        decide(
            &RemotePayload::parse(payload).expect("well-formed payload"),
            alerts,
            read_seq,
            foreground,
        )
    }

    /// 新消息：展示系统通知并响铃，字段原样带出；同一条再来一次不再响。
    #[test]
    fn a_loud_push_is_presented_once() {
        let alerts = AlertLedger::new();
        let first = decided(&loud("5"), &alerts, None, false);
        assert_eq!(first.action, "present");
        assert_eq!(first.badge, Some(7));
        assert!(first.clear_collapse_ids.is_empty());
        let shown = first.presentation.expect("a presentation");
        assert_eq!(shown.collapse_id, "c:0123456789abcdef0123456789abcdef");
        assert_eq!(shown.thread_id, shown.collapse_id);
        assert_eq!(shown.conversation_id, "c1");
        assert_eq!(shown.message_id, "m5");
        assert_eq!(shown.seq, 5);
        assert_eq!(
            (shown.title.as_str(), shown.body.as_str()),
            ("Bob", "hello")
        );
        assert_eq!(shown.count, 2);
        assert!(shown.sound);
        assert_eq!(shown.channel, "messages");
        assert_eq!(shown.surface, "system");

        let again = decided(&loud("5"), &alerts, None, false);
        assert_eq!(again.action, "ignore");
        assert_eq!(again.reason.as_deref(), Some("alreadyAlerted"));
        assert_eq!(again.badge, Some(7), "角标照样带出");
        assert_eq!(
            decided(&loud("4"), &alerts, None, false).reason.as_deref(),
            Some("alreadyAlerted"),
            "乱序到达的旧推送不再响"
        );
        assert_eq!(decided(&loud("6"), &alerts, None, true).action, "present");
    }

    /// 链路已经确认（由本进程提示过）的那条，推送再来不响。
    #[test]
    fn an_alert_the_link_confirmed_is_not_repeated() {
        let alerts = AlertLedger::new();
        alerts.scope("0");
        alerts.note_confirmed(&[("c1".to_owned(), 5)]);
        let payload = with(
            loud("5"),
            "collapse_id",
            &conversation_collapse_id("0", "c1"),
        );
        let outcome = decided(&payload, &alerts, None, true);
        assert_eq!(outcome.action, "ignore");
        assert_eq!(outcome.reason.as_deref(), Some("alreadyAlerted"));
    }

    /// 处置到动作：loud / digest 展示，quiet / recall_replace 静默替换，clear 清除，badge_only 只改角标。
    /// 静默替换不响；前台展示在应用内。
    #[test]
    fn dispositions_map_onto_actions() {
        for (disposition, action) in [
            ("loud", "present"),
            ("digest", "present"),
            ("quiet", "update"),
            ("recall_replace", "update"),
            ("clear", "clear"),
            ("badge_only", "badgeOnly"),
        ] {
            let alerts = AlertLedger::new();
            let outcome = decided(
                &with(loud("5"), "disposition", disposition),
                &alerts,
                None,
                true,
            );
            assert_eq!(outcome.action, action, "{disposition}");
            match action {
                "present" | "update" => {
                    let shown = outcome.presentation.expect("a presentation");
                    assert_eq!(shown.sound, action == "present", "{disposition}");
                    assert_eq!(shown.surface, "inApp", "{disposition}");
                }
                "clear" => {
                    assert!(outcome.presentation.is_none());
                    assert_eq!(
                        outcome.clear_collapse_ids,
                        vec!["c:0123456789abcdef0123456789abcdef".to_owned()]
                    );
                }
                _ => {
                    assert!(outcome.presentation.is_none());
                    assert!(outcome.clear_collapse_ids.is_empty());
                    assert_eq!(outcome.badge, Some(7));
                }
            }
        }
    }

    /// 服务端的摘要：设备级合并键，不带会话与 seq。照样展示并响；再来一条同键的覆盖旧的，不被台账
    /// 当成重复或过时；没有会话可去，点开只回到应用。
    #[test]
    fn a_server_digest_without_conversation_or_seq_is_presented() {
        let digest = payload(&[
            ("uid", "alice"),
            ("tenant_id", "0"),
            ("message_id", "m9"),
            ("collapse_id", "d:0123456789abcdef0123456789abcdef"),
            ("thread_id", "d:0123456789abcdef0123456789abcdef"),
            ("disposition", "digest"),
            ("alert_class", "normal"),
            ("count", "12"),
            ("badge", "30"),
            ("title", "Flare"),
            ("body", "3 conversations, 12 new messages"),
            ("sound", "1"),
        ]);
        let alerts = AlertLedger::new();
        let first = decided(&digest, &alerts, None, false);
        assert_eq!(first.action, "present");
        assert_eq!(first.badge, Some(30));
        let shown = first.presentation.expect("a presentation");
        assert_eq!(shown.conversation_id, "");
        assert_eq!(shown.seq, 0);
        assert_eq!(shown.count, 12);
        assert!(shown.sound);
        assert_eq!(shown.surface, "system");

        let next = decided(&with(digest.clone(), "count", "15"), &alerts, None, false);
        assert_eq!(
            next.action, "present",
            "a later digest replaces the shown one"
        );
        assert_eq!(next.presentation.expect("a presentation").count, 15);

        let tap = resolve_tap(Some(("alice", "0")), &digest);
        assert!(
            tap.conversation_id.is_none(),
            "a digest has no conversation to open"
        );
    }

    /// 只有摘要和只改角标的可以不带会话；其余处置缺会话仍按 malformed 忽略。
    #[test]
    fn only_conversation_spanning_dispositions_may_omit_the_conversation() {
        let mut without = loud("5");
        without.remove("conversation_id");
        assert!(RemotePayload::parse(&without).is_none());
        assert!(
            RemotePayload::parse(&with(without.clone(), "disposition", "badge_only")).is_some()
        );
        let mut digest = with(without, "disposition", "digest");
        digest.remove("seq");
        assert!(RemotePayload::parse(&digest).is_some());
    }

    /// 渠道：@我与紧急各自的渠道优先；静默更新走安静渠道；其余是普通消息。
    #[test]
    fn alert_classes_map_onto_channels() {
        for (class, disposition, channel) in [
            ("mention", "loud", "mentions"),
            ("mention", "quiet", "mentions"),
            ("urgent", "loud", "urgent"),
            ("normal", "quiet", "quiet"),
            ("silent", "quiet", "quiet"),
            ("normal", "loud", "messages"),
            ("none", "digest", "messages"),
            ("normal", "recall_replace", "messages"),
        ] {
            let alerts = AlertLedger::new();
            let payload = with(
                with(loud("5"), "alert_class", class),
                "disposition",
                disposition,
            );
            let shown = decided(&payload, &alerts, None, false)
                .presentation
                .expect("a presentation");
            assert_eq!(shown.channel, channel, "{class} {disposition}");
        }
    }

    /// 本地已读覆盖了 seq：改为清除。展示着的已是已读之后的新消息时不清（过时）。
    #[test]
    fn local_read_state_turns_an_alert_into_a_clear() {
        let alerts = AlertLedger::new();
        let outcome = decided(&loud("5"), &alerts, Some(5), false);
        assert_eq!(outcome.action, "clear");
        assert_eq!(
            outcome.clear_collapse_ids,
            vec!["c:0123456789abcdef0123456789abcdef".to_owned()]
        );
        assert_eq!(
            decided(&loud("6"), &alerts, Some(5), false).action,
            "present"
        );

        let alerts = AlertLedger::new();
        decided(&loud("9"), &alerts, None, false);
        let outcome = decided(&loud("5"), &alerts, Some(6), false);
        assert_eq!(outcome.action, "ignore");
        assert_eq!(outcome.reason.as_deref(), Some("stale"));
    }

    /// 清除不撤更新的通知；撤回替换换的就是展示着的那条。
    #[test]
    fn clears_and_recalls_respect_newer_alerts() {
        let alerts = AlertLedger::new();
        decided(&loud("9"), &alerts, None, false);
        let clear = decided(
            &with(loud("8"), "disposition", "clear"),
            &alerts,
            None,
            false,
        );
        assert_eq!(clear.reason.as_deref(), Some("stale"));
        let recall = decided(
            &with(loud("9"), "disposition", "recall_replace"),
            &alerts,
            None,
            false,
        );
        assert_eq!(recall.action, "update");
        assert_eq!(
            decided(
                &with(loud("9"), "disposition", "clear"),
                &alerts,
                None,
                false
            )
            .action,
            "clear"
        );
        assert_eq!(
            decided(&loud("9"), &alerts, None, false).reason.as_deref(),
            Some("stale"),
            "清过的一段不再提示"
        );
    }

    /// 缺必需的键、值认不得的载荷按 malformed 忽略；只改角标的可以不带 seq；认不得的键不管。
    #[test]
    fn malformed_payloads_are_rejected() {
        for key in [
            "uid",
            "tenant_id",
            "conversation_id",
            "collapse_id",
            "disposition",
            "seq",
        ] {
            let mut broken = loud("5");
            broken.remove(key);
            assert!(RemotePayload::parse(&broken).is_none(), "without {key}");
        }
        for (key, value) in [
            ("seq", "x"),
            ("seq", "0"),
            ("disposition", "shout"),
            ("alert_class", "loudest"),
            ("count", "-1"),
            ("badge", "many"),
            ("sound", "yes"),
            ("collapse_id", "c: 1"),
        ] {
            assert!(
                RemotePayload::parse(&with(loud("5"), key, value)).is_none(),
                "{key}={value}"
            );
        }
        let mut badge_only = with(loud("5"), "disposition", "badge_only");
        badge_only.remove("seq");
        assert!(RemotePayload::parse(&badge_only).is_some());

        let extra = with(loud("5"), "aps", "{}");
        let parsed = RemotePayload::parse(&extra).expect("extra keys");
        assert_eq!(parsed.count, 2);
        let mut minimal = loud("5");
        for key in [
            "message_id",
            "thread_id",
            "alert_class",
            "count",
            "badge",
            "title",
            "body",
            "sound",
        ] {
            minimal.remove(key);
        }
        let parsed = RemotePayload::parse(&minimal).expect("optional keys");
        assert_eq!(parsed.count, 1);
        assert_eq!(parsed.thread_id, parsed.collapse_id);
        assert_eq!(parsed.alert_class, AlertClass::Normal);
        assert!(!parsed.sound);
        assert_eq!(parsed.badge, None);
        assert_eq!(
            RemotePayload::parse(&with(loud("5"), "count", "0"))
                .expect("count")
                .count,
            1,
            "条数至少 1"
        );
    }

    /// 账号：用户与租户都要对上；租户按同样的规则规整（空与 default 即 0）。
    #[test]
    fn payloads_are_matched_to_the_account() {
        let parsed = |payload: &BTreeMap<String, String>| {
            RemotePayload::parse(payload)
                .expect("well-formed")
                .owned_by("alice", "0")
        };
        assert!(parsed(&loud("5")));
        assert!(parsed(&with(loud("5"), "tenant_id", "default")));
        assert!(!parsed(&with(loud("5"), "uid", "bob")));
        assert!(!parsed(&with(loud("5"), "tenant_id", "t2")));
    }

    /// 点开通知：当前账号的给出会话、消息与 seq；别的账号、没有登录的什么都不给。
    #[test]
    fn a_tap_resolves_only_for_the_current_account() {
        let tapped = resolve_tap(Some(("alice", "0")), &loud("5"));
        assert_eq!(
            tapped,
            NotificationTapResult {
                conversation_id: Some("c1".into()),
                message_id: Some("m5".into()),
                seq: Some(5),
            }
        );
        let catch_up = resolve_tap(
            Some(("alice", "0")),
            &with(with(loud("5"), "message_id", ""), "seq", "0"),
        );
        assert_eq!(catch_up.conversation_id.as_deref(), Some("c1"));
        assert_eq!(catch_up.message_id, None);
        assert_eq!(catch_up.seq, None);
        assert_eq!(
            resolve_tap(Some(("bob", "0")), &loud("5")),
            NotificationTapResult::default()
        );
        assert_eq!(
            resolve_tap(None, &loud("5")),
            NotificationTapResult::default()
        );
        assert_eq!(
            serde_json::to_value(NotificationTapResult::default()).unwrap(),
            serde_json::json!({})
        );
    }

    /// 已读进度从本地会话库读；本地没有这个会话就当没有。
    #[tokio::test]
    async fn the_read_state_comes_from_the_local_store() {
        let store = MemoryConversationStore::new();
        let conversation = Conversation {
            conversation_id: "c1".into(),
            last_read_seq: 8,
            ..Conversation::default()
        };
        store.save_one(&conversation).await.unwrap();
        assert_eq!(local_read_seq(&store, "c1").await, Some(8));
        assert_eq!(local_read_seq(&store, "c2").await, None);
    }

    /// 没有登录、载荷认不得、通道不对：不碰台账，直接答复。
    #[tokio::test]
    async fn the_inbox_needs_a_session_and_a_known_provider() {
        let inbox = PushInbox::default();
        let request =
            |provider: &str, payload: BTreeMap<String, String>| HandleRemoteMessageRequest {
                provider: provider.into(),
                payload,
                budget_ms: Some(8_000),
            };
        let outcome = inbox
            .handle(request("fcm", loud("5")), false)
            .await
            .unwrap();
        assert_eq!(outcome, ignored("notLoggedIn", None));
        let mut broken = loud("5");
        broken.remove("uid");
        let outcome = inbox.handle(request("apns", broken), false).await.unwrap();
        assert_eq!(outcome, ignored("malformed", None));
        assert!(
            inbox
                .handle(request("webpush", loud("5")), false)
                .await
                .is_err()
        );
        assert_eq!(
            inbox
                .resolve_tap(&ResolveNotificationTapRequest { payload: loud("5") })
                .unwrap(),
            NotificationTapResult::default()
        );
    }

    /// 线上的形状：驼峰键，空的可选项不出现，clearCollapseIds 总在。
    #[test]
    fn outcomes_serialize_in_camel_case() {
        let value = serde_json::to_value(ignored("stale", Some(3))).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"action": "ignore", "clearCollapseIds": [], "badge": 3, "reason": "stale"})
        );
        let alerts = AlertLedger::new();
        let value = serde_json::to_value(decided(&loud("5"), &alerts, None, false)).unwrap();
        let shown = &value["presentation"];
        for key in [
            "collapseId",
            "threadId",
            "conversationId",
            "messageId",
            "seq",
            "title",
            "body",
            "count",
            "sound",
            "channel",
            "surface",
        ] {
            assert!(shown.get(key).is_some(), "{key}");
        }
        let request: HandleRemoteMessageRequest = serde_json::from_value(serde_json::json!({
            "provider": "fcm", "payload": {"uid": "alice"}, "budgetMs": 8000
        }))
        .unwrap();
        assert_eq!(request.budget_ms, Some(8_000));
        assert!(
            serde_json::from_value::<HandleRemoteMessageRequest>(serde_json::json!({
                "provider": "fcm", "payload": {}, "budget": 1
            }))
            .is_err()
        );
    }
}
