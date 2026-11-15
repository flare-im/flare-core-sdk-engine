//! 插件卡片本人层（message-content 贡献的 `viewerState`）的调度状态机：只决定「现在读、稍后读、
//! 不读」，不做任何 I/O。驱动它的是 `client::api` 里的 viewer 驱动（读存储、发请求、写本人层）。
//!
//! 规则（协议 `UiViewerState`）：
//! - 只给呈现过的卡片读：`present` 触发来自宿主呈现，`sharedVersion` 触发只对 [`RECENT_MS`] 内呈现过的
//!   卡片生效；
//! - 需要读：没有本人层，或本人层的 `basedOnVersion` 小于当前公开层版本；
//! - 同一张卡片在同一个（公开层版本, 本人层版本）上成功读过一次就不再读，即使结果仍然过期（例如插件
//!   回答的 `basedOnVersion` 落后），等公开层或本人层前进再说，避免「写入→重新呈现→再读」的自激回路；
//! - 每张卡片同时只有一个请求，在途期间到来的触发记成 `dirty`，结束后重新判定一次；
//! - 两次开始之间至少隔 `minIntervalMs`，太早到来的触发排一次延迟重判（每张卡片最多一个）；
//! - 失败按 `minIntervalMs × 2^失败次数` 退避，上限 [`MAX_BACKOFF_MS`]；
//! - 换了登录用户就清空，旧会话排着的重判和结果一律作废。
//!
//! 记账最多 [`MAX_CARDS`] 张卡片，满了淘汰最早呈现且不在途的那张。所有呈现过的卡片都记（目录变了时
//! 据此让宿主重新呈现，[`ViewerSchedule::recent`]），只有可交互且声明了 `viewerState` 的才会读。
use std::collections::{HashMap, VecDeque};

use flare_plugin_protocol::manifest::UiContribution;
use flare_plugin_protocol::ui::{
    PluginMessage, PluginMessageState, UiViewerRefresh, UiViewerState,
};

/// `sharedVersion` 触发、重判与重新宣告只对这段时间内呈现过的卡片生效。
pub(crate) const RECENT_MS: u64 = 5 * 60 * 1000;
/// 失败退避的上限。
pub(crate) const MAX_BACKOFF_MS: u64 = 60_000;
/// 记账的卡片数。
pub(crate) const MAX_CARDS: usize = 256;

/// 一张呈现过的卡片。
#[derive(Clone)]
pub(crate) struct ViewerCard {
    /// 卡片消息的 server id（本地存储的键，也是发给 Core 的 `messageId`）。
    pub message_id: String,
    pub conversation_id: String,
    pub message: PluginMessage,
    /// 呈现时可交互的卡片所属的贡献；不可用或只读副本没有，也就从不读本人层。
    pub contribution: Option<UiContribution>,
}
impl ViewerCard {
    pub fn viewer(&self) -> Option<&UiViewerState> {
        self.contribution.as_ref()?.viewer_state.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewerTrigger {
    /// 宿主呈现了这张卡片。
    Present,
    /// 这张卡片的公开层（或本人层）变了。
    SharedVersion,
    /// 延迟重判到点，或在途期间有新的触发。
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ViewerDecision {
    /// 现在读；调用方读完必须调 [`ViewerSchedule::finished`]。
    Read,
    /// 到这个时刻（毫秒）再用 [`ViewerTrigger::Retry`] 判一次。
    RetryAt(u64),
    Skip,
}

struct Entry {
    card: ViewerCard,
    presented_at_ms: u64,
    in_flight: bool,
    dirty: bool,
    started_at_ms: u64,
    next_allowed_ms: u64,
    failures: u32,
    /// 最近一次成功读开始时的（公开层版本, 本人层版本）。
    read_at: Option<(u64, u64)>,
    /// 读开始时的（公开层版本, 本人层版本），结束时记成 `read_at`。
    reading_at: (u64, u64),
    retry_armed: bool,
    /// 最近一次结果带回的 `refreshOn`（只收窄声明，例如大群的投票只在呈现时读）。
    narrowed: Option<Vec<UiViewerRefresh>>,
}

#[derive(Default)]
pub(crate) struct ViewerSchedule {
    user: String,
    entries: HashMap<String, Entry>,
    order: VecDeque<String>,
}

impl ViewerSchedule {
    /// 换了用户就清空并返回 `true`。
    fn session(&mut self, user: &str) -> bool {
        if self.user == user {
            return false;
        }
        self.user = user.to_owned();
        self.entries.clear();
        self.order.clear();
        true
    }

    /// 记下一次呈现。
    pub fn presented(&mut self, user: &str, card: ViewerCard, now_ms: u64) {
        if user.is_empty() {
            return;
        }
        self.session(user);
        let key = card.message_id.clone();
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.card = card;
            entry.presented_at_ms = now_ms;
            self.order.retain(|k| k != &key);
            self.order.push_back(key);
            return;
        }
        while self.entries.len() >= MAX_CARDS {
            let Some(position) = self
                .order
                .iter()
                .position(|k| self.entries.get(k).is_none_or(|e| !e.in_flight))
            else {
                return;
            };
            if let Some(old) = self.order.remove(position) {
                self.entries.remove(&old);
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            Entry {
                card,
                presented_at_ms: now_ms,
                in_flight: false,
                dirty: false,
                started_at_ms: 0,
                next_allowed_ms: 0,
                failures: 0,
                read_at: None,
                reading_at: (0, 0),
                retry_armed: false,
                narrowed: None,
            },
        );
    }

    /// 记下一次成功读取的结果对 `refreshOn` 的收窄（`None`：结果没说，按声明）。
    pub fn narrow(&mut self, user: &str, message_id: &str, narrowed: Option<Vec<UiViewerRefresh>>) {
        if self.user != user {
            return;
        }
        if let Some(entry) = self.entries.get_mut(message_id) {
            entry.narrowed = narrowed;
        }
    }

    /// 是否记着这张卡片（任何会话）：事件回调里先用它挡掉无关消息。
    pub fn tracks(&self, message_id: &str) -> bool {
        self.entries.contains_key(message_id)
    }

    pub fn card(&self, user: &str, message_id: &str) -> Option<ViewerCard> {
        (self.user == user)
            .then(|| self.entries.get(message_id).map(|e| e.card.clone()))
            .flatten()
    }

    /// 判定 `message_id` 这张卡片现在要不要读。`local` 是存储里的本人层，`shared_version` 是对这张
    /// 卡片生效的公开层版本（没有时 0）。
    pub fn decide(
        &mut self,
        user: &str,
        message_id: &str,
        trigger: ViewerTrigger,
        local: Option<&PluginMessageState>,
        shared_version: u64,
        now_ms: u64,
    ) -> ViewerDecision {
        if user.is_empty() || self.user != user {
            return ViewerDecision::Skip;
        }
        let Some(entry) = self.entries.get_mut(message_id) else {
            return ViewerDecision::Skip;
        };
        if trigger == ViewerTrigger::Retry {
            entry.retry_armed = false;
        }
        let Some(viewer) = entry.card.viewer() else {
            return ViewerDecision::Skip;
        };
        let recent = now_ms.saturating_sub(entry.presented_at_ms) <= RECENT_MS;
        let narrowed = entry.narrowed.as_deref();
        let wanted = match trigger {
            ViewerTrigger::Present => viewer.refreshes_on_with(UiViewerRefresh::Present, narrowed),
            ViewerTrigger::SharedVersion => {
                recent && viewer.refreshes_on_with(UiViewerRefresh::SharedVersion, narrowed)
            }
            ViewerTrigger::Retry => recent,
        };
        let at = (shared_version, local.map_or(0, |l| l.version));
        if !wanted || !UiViewerState::needs_read(local, shared_version) || entry.read_at == Some(at)
        {
            return ViewerDecision::Skip;
        }
        if entry.in_flight {
            entry.dirty = true;
            return ViewerDecision::Skip;
        }
        if now_ms < entry.next_allowed_ms {
            if entry.retry_armed {
                return ViewerDecision::Skip;
            }
            entry.retry_armed = true;
            return ViewerDecision::RetryAt(entry.next_allowed_ms);
        }
        entry.in_flight = true;
        entry.started_at_ms = now_ms;
        entry.reading_at = at;
        ViewerDecision::Read
    }

    /// 一次读结束（`ok`：拿到了结果，不论写没写进本人层）。返回 `true` 表示在途期间有新的触发，调用方
    /// 应当用 [`ViewerTrigger::Retry`] 重判一次。
    pub fn finished(&mut self, user: &str, message_id: &str, ok: bool, now_ms: u64) -> bool {
        if self.user != user {
            return false;
        }
        let Some(entry) = self.entries.get_mut(message_id) else {
            return false;
        };
        let interval = entry.card.viewer().map_or(
            u64::from(flare_plugin_protocol::ui::MIN_VIEWER_STATE_INTERVAL_MS),
            |v| u64::from(v.min_interval_ms),
        );
        entry.in_flight = false;
        if ok {
            entry.failures = 0;
            entry.read_at = Some(entry.reading_at);
            entry.next_allowed_ms = entry.started_at_ms.saturating_add(interval);
        } else {
            entry.failures = entry.failures.saturating_add(1);
            let backoff = interval
                .checked_shl(entry.failures.min(16))
                .unwrap_or(MAX_BACKOFF_MS)
                .min(MAX_BACKOFF_MS);
            entry.next_allowed_ms = now_ms.saturating_add(backoff);
        }
        std::mem::take(&mut entry.dirty)
    }

    /// 不再读这张卡片（消息没了）。
    pub fn forget(&mut self, message_id: &str) {
        if self.entries.remove(message_id).is_some() {
            self.order.retain(|k| k != message_id);
        }
    }

    /// 最近呈现过的卡片（`plugin_id` 给出时只要这个插件的），最新的在后：目录作废后重新宣告用。
    pub fn recent(&self, user: &str, plugin_id: Option<&str>, now_ms: u64) -> Vec<ViewerCard> {
        if self.user != user {
            return Vec::new();
        }
        self.order
            .iter()
            .filter_map(|key| self.entries.get(key))
            .filter(|entry| {
                now_ms.saturating_sub(entry.presented_at_ms) <= RECENT_MS
                    && plugin_id.is_none_or(|id| entry.card.message.plugin_id == id)
            })
            .map(|entry| entry.card.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn card(message_id: &str, refresh_on: serde_json::Value, interval: u32) -> ViewerCard {
        let contribution: UiContribution = crate::shared::util::json::from_value(json!({
            "kind": "message-content", "id": "example.poll.card", "schemaVersion": 1,
            "requiredPermissions": [], "entityPath": "/pollId", "schema": {"title": "t"},
            "viewerState": {"operation": "example.poll.view", "input": {"pollId": "/pollId"},
                            "refreshOn": refresh_on, "minIntervalMs": interval}
        }))
        .unwrap();
        contribution
            .viewer_state
            .as_ref()
            .unwrap()
            .validate()
            .unwrap();
        ViewerCard {
            message_id: message_id.into(),
            conversation_id: "group-1".into(),
            message: crate::shared::util::json::from_value(json!({
                "pluginId": "example.poll", "contributionId": "example.poll.card",
                "schemaVersion": 1, "summary": "s", "payload": {"pollId": "p-1"}
            }))
            .unwrap(),
            contribution: Some(contribution),
        }
    }
    fn layer(version: u64, based_on: Option<u64>) -> PluginMessageState {
        PluginMessageState {
            version,
            based_on_version: based_on,
            values: Default::default(),
        }
    }
    #[allow(non_snake_case)]
    fn BOTH() -> serde_json::Value {
        json!(["present", "sharedVersion"])
    }

    #[test]
    fn a_presented_card_without_a_fresh_layer_is_read_once_per_version() {
        let mut s = ViewerSchedule::default();
        s.presented("u1", card("m1", BOTH(), 2000), 0);
        use ViewerDecision::*;
        use ViewerTrigger::*;
        assert_eq!(s.decide("u1", "m1", Present, None, 0, 0), Read);
        // Presented again while in flight: one request at a time, re-judged after.
        assert_eq!(s.decide("u1", "m1", Present, None, 0, 10), Skip);
        assert!(s.finished("u1", "m1", true, 100));
        // The read left no layer (an empty result) at the same versions: not read again.
        assert_eq!(s.decide("u1", "m1", Retry, None, 0, 100), Skip);
        assert_eq!(s.decide("u1", "m1", Present, None, 0, 5000), Skip);
        // A fresh layer needs nothing; a newer shared version does.
        let fresh = layer(0, Some(0));
        assert_eq!(s.decide("u1", "m1", Present, Some(&fresh), 0, 5000), Skip);
        assert_eq!(
            s.decide("u1", "m1", SharedVersion, Some(&fresh), 3, 5000),
            Read
        );
        assert!(!s.finished("u1", "m1", true, 5100));
        // The plugin answered with an older basedOnVersion: stale, but already read at v3.
        let lagging = layer(0, Some(2));
        assert_eq!(s.decide("u1", "m1", Present, Some(&lagging), 3, 9000), Skip);
        // An action wrote a new local layer (version 4): read again.
        let acted = layer(4, None);
        assert_eq!(s.decide("u1", "m1", Present, Some(&acted), 3, 9000), Read);
    }

    #[test]
    fn reads_are_throttled_with_one_trailing_retry() {
        let mut s = ViewerSchedule::default();
        use ViewerDecision::*;
        use ViewerTrigger::*;
        s.presented("u1", card("m1", BOTH(), 2000), 0);
        assert_eq!(s.decide("u1", "m1", Present, None, 0, 1000), Read);
        s.finished("u1", "m1", true, 1200);
        let fresh = layer(0, Some(0));
        // A newer shared version 500 ms after the read started: wait for the interval.
        assert_eq!(
            s.decide("u1", "m1", SharedVersion, Some(&fresh), 1, 1500),
            RetryAt(3000)
        );
        assert_eq!(
            s.decide("u1", "m1", SharedVersion, Some(&fresh), 2, 1600),
            Skip,
            "one retry armed per card"
        );
        assert_eq!(s.decide("u1", "m1", Retry, Some(&fresh), 2, 3000), Read);
    }

    #[test]
    fn failures_back_off_exponentially_up_to_a_minute() {
        let mut s = ViewerSchedule::default();
        use ViewerDecision::*;
        use ViewerTrigger::*;
        s.presented("u1", card("m1", BOTH(), 2000), 0);
        let mut now = 0;
        let mut waits = Vec::new();
        for _ in 0..7 {
            assert_eq!(s.decide("u1", "m1", Retry, None, 0, now), Read);
            s.finished("u1", "m1", false, now);
            match s.decide("u1", "m1", Present, None, 0, now) {
                RetryAt(at) => {
                    waits.push(at - now);
                    now = at;
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(waits, [4000, 8000, 16000, 32000, 60000, 60000, 60000]);
        // Success resets the backoff.
        assert_eq!(s.decide("u1", "m1", Retry, None, 0, now), Read);
        s.finished("u1", "m1", true, now);
    }

    #[test]
    fn shared_version_triggers_need_the_trigger_and_a_recent_presentation() {
        let mut s = ViewerSchedule::default();
        use ViewerDecision::*;
        use ViewerTrigger::*;
        s.presented("u1", card("present-only", json!(["present"]), 1000), 0);
        s.presented("u1", card("both", BOTH(), 1000), 0);
        s.presented("u1", card("shared-only", json!(["sharedVersion"]), 1000), 0);
        let stale = layer(1, Some(1));
        assert_eq!(
            s.decide("u1", "present-only", SharedVersion, Some(&stale), 2, 10),
            Skip
        );
        assert_eq!(
            s.decide("u1", "shared-only", Present, Some(&stale), 2, 10),
            Skip
        );
        assert_eq!(
            s.decide("u1", "shared-only", SharedVersion, Some(&stale), 2, 10),
            Read
        );
        assert_eq!(
            s.decide("u1", "both", SharedVersion, Some(&stale), 2, RECENT_MS + 1),
            Skip,
            "not presented recently"
        );
        assert_eq!(
            s.decide("u1", "never-presented", Present, None, 0, 10),
            Skip
        );
    }

    #[test]
    fn a_session_change_drops_everything() {
        let mut s = ViewerSchedule::default();
        use ViewerDecision::*;
        use ViewerTrigger::*;
        s.presented("u1", card("m1", BOTH(), 1000), 0);
        assert_eq!(s.decide("u1", "m1", Present, None, 0, 0), Read);
        assert_eq!(s.decide("u2", "m1", Present, None, 0, 0), Skip);
        s.presented("u2", card("m2", BOTH(), 1000), 0);
        assert!(s.card("u1", "m1").is_none());
        assert!(
            !s.finished("u1", "m1", true, 10),
            "the old session's result is ignored"
        );
        assert_eq!(s.decide("u2", "m1", Present, None, 0, 10), Skip);
        assert_eq!(s.decide("u2", "m2", Present, None, 0, 10), Read);
        assert!(s.recent("u1", None, 10).is_empty());
        assert_eq!(s.recent("u2", Some("example.poll"), 10).len(), 1);
        assert!(s.recent("u2", Some("other"), 10).is_empty());
    }

    #[test]
    fn a_result_can_narrow_the_triggers_of_its_card() {
        let mut s = ViewerSchedule::default();
        use ViewerDecision::*;
        use ViewerTrigger::*;
        s.presented("u1", card("m1", BOTH(), 1000), 0);
        assert_eq!(s.decide("u1", "m1", Present, None, 0, 0), Read);
        s.finished("u1", "m1", true, 10);
        // A large group: the plugin keeps only presentation reads.
        s.narrow("u1", "m1", Some(vec![UiViewerRefresh::Present]));
        let stale = layer(0, Some(0));
        assert_eq!(
            s.decide("u1", "m1", SharedVersion, Some(&stale), 1, 5000),
            Skip
        );
        assert_eq!(s.decide("u1", "m1", Present, Some(&stale), 1, 5000), Read);
        s.finished("u1", "m1", true, 5010);
        // The next result says nothing: the declaration applies again.
        s.narrow("u1", "m1", None);
        assert_eq!(
            s.decide("u1", "m1", SharedVersion, Some(&stale), 2, 9000),
            Read
        );
    }

    #[test]
    fn an_inert_card_is_remembered_but_never_read() {
        let mut s = ViewerSchedule::default();
        let mut inert = card("m1", BOTH(), 1000);
        inert.contribution = None;
        s.presented("u1", inert, 0);
        assert_eq!(
            s.decide("u1", "m1", ViewerTrigger::Present, None, 0, 0),
            ViewerDecision::Skip
        );
        assert_eq!(
            s.recent("u1", None, 10).len(),
            1,
            "announced when the catalog changes"
        );
        // Presented again once interactive: read.
        s.presented("u1", card("m1", BOTH(), 1000), 20);
        assert_eq!(
            s.decide("u1", "m1", ViewerTrigger::Present, None, 0, 20),
            ViewerDecision::Read
        );
    }

    #[test]
    fn tracking_is_bounded_and_keeps_cards_in_flight() {
        let mut s = ViewerSchedule::default();
        s.presented("u1", card("first", BOTH(), 1000), 0);
        assert_eq!(
            s.decide("u1", "first", ViewerTrigger::Present, None, 0, 0),
            ViewerDecision::Read
        );
        for n in 0..MAX_CARDS + 10 {
            s.presented("u1", card(&format!("m{n}"), BOTH(), 1000), 1);
        }
        assert_eq!(s.entries.len(), MAX_CARDS);
        assert!(s.card("u1", "first").is_some(), "in flight, never evicted");
        assert!(s.card("u1", "m0").is_none());
    }
}
