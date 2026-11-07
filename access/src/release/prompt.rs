//! 更新提示频率（§7.8 第 4 条）：纯函数。
//!
//! - 可选：角标；每个版本最多主动弹 1 次底部弹层。
//! - 推荐：对话框，每天最多 1 次，「稍后」推迟 24 小时。
//! - 强制：每次启动与回前台都弹；`blockUi` 时全屏。
//! - 同一会话内不重复弹。
//!
//! 提示弹出之后还要撑住整个更新流程：下载、校验、安装、失败时提示不能消失，所以状态机在这些阶段
//! 用 [`held_form`] 给形态，流程结束才回到上面的频率规则。

use serde::{Deserialize, Serialize};

use super::evaluate::Urgency;
use crate::time::DAY_MS;

/// 持久记录里最多留几个版本；清单本身也只有最近 3 个。
const MAX_REMEMBERED_BUILDS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UpdatePrompt {
    None,
    Badge,
    Sheet,
    Modal,
    Fullscreen,
}

impl UpdatePrompt {
    pub fn as_str(self) -> &'static str {
        match self {
            UpdatePrompt::None => "none",
            UpdatePrompt::Badge => "badge",
            UpdatePrompt::Sheet => "sheet",
            UpdatePrompt::Modal => "modal",
            UpdatePrompt::Fullscreen => "fullscreen",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(UpdatePrompt::None),
            "badge" => Some(UpdatePrompt::Badge),
            "sheet" => Some(UpdatePrompt::Sheet),
            "modal" => Some(UpdatePrompt::Modal),
            "fullscreen" => Some(UpdatePrompt::Fullscreen),
            _ => None,
        }
    }
}

/// 跨重启的提示记录，存在 AccessStore。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PromptHistory {
    /// 已弹过底部弹层的可选版本。
    pub sheet_shown_builds: Vec<u64>,
    /// 最近一次弹推荐对话框的时刻。
    pub last_modal_at_ms: Option<i64>,
    /// 「稍后」的到期时刻。
    pub deferred_until_ms: Option<i64>,
    /// 用户跳过的版本。
    pub skipped_builds: Vec<u64>,
}

/// 只在内存里的提示记录：同一会话不重复弹。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromptSession {
    /// 这次运行里已弹过的版本。
    pub shown_builds: Vec<u64>,
    /// 这次启动或回前台之后已弹过强制提示。
    pub required_shown: bool,
}

impl PromptSession {
    /// 回到前台：强制提示重新算一次。
    pub fn foregrounded(&mut self) {
        self.required_shown = false;
    }
}

fn remember(builds: &mut Vec<u64>, build: u64) {
    if !builds.contains(&build) {
        builds.push(build);
    }
    if builds.len() > MAX_REMEMBERED_BUILDS {
        builds.remove(0);
    }
}

impl PromptHistory {
    pub fn defer(&mut self, now_ms: i64) {
        self.deferred_until_ms = Some(now_ms + DAY_MS);
    }

    pub fn skip(&mut self, build: u64) {
        remember(&mut self.skipped_builds, build);
    }

    /// UI 真的弹出了提示。
    pub fn record(
        &mut self,
        session: &mut PromptSession,
        urgency: Urgency,
        build: Option<u64>,
        form: UpdatePrompt,
        now_ms: i64,
    ) {
        if urgency.is_required() {
            session.required_shown = true;
            return;
        }
        if let Some(build) = build {
            remember(&mut session.shown_builds, build);
        }
        match (form, build) {
            (UpdatePrompt::Sheet, Some(build)) => remember(&mut self.sheet_shown_builds, build),
            (UpdatePrompt::Modal, _) => self.last_modal_at_ms = Some(now_ms),
            _ => {}
        }
    }
}

/// 更新流程进行中的形态：本会话弹过的那个，没弹过（例如从「关于」页发起）就按版本类别推导。
/// 强制版本没有底部弹层，也不能把阻断改成可关闭的形态。
pub fn held_form(urgency: Urgency, shown: Option<UpdatePrompt>) -> UpdatePrompt {
    match (urgency, shown) {
        (Urgency::Required { block_ui: true }, _) => UpdatePrompt::Fullscreen,
        (Urgency::Required { block_ui: false }, _) => UpdatePrompt::Modal,
        (_, Some(form)) => form,
        (Urgency::Recommended, None) => UpdatePrompt::Modal,
        (Urgency::Optional, None) => UpdatePrompt::Sheet,
    }
}

/// 现在该以什么形态提示。`build` 是目标版本，强制更新没有可用版本时为空。
pub fn decide_prompt(
    urgency: Urgency,
    build: Option<u64>,
    now_ms: i64,
    history: &PromptHistory,
    session: &PromptSession,
) -> UpdatePrompt {
    if let Urgency::Required { block_ui } = urgency {
        return if block_ui {
            UpdatePrompt::Fullscreen
        } else if session.required_shown {
            UpdatePrompt::Badge
        } else {
            UpdatePrompt::Modal
        };
    }
    let Some(build) = build else {
        return UpdatePrompt::None;
    };
    if history.skipped_builds.contains(&build) {
        return UpdatePrompt::None;
    }
    let deferred = history
        .deferred_until_ms
        .is_some_and(|until| until > now_ms);
    if deferred || session.shown_builds.contains(&build) {
        return UpdatePrompt::Badge;
    }
    match urgency {
        Urgency::Recommended => {
            let shown_today = history
                .last_modal_at_ms
                .is_some_and(|last| now_ms - last < DAY_MS && last <= now_ms);
            if shown_today {
                UpdatePrompt::Badge
            } else {
                UpdatePrompt::Modal
            }
        }
        _ => {
            if history.sheet_shown_builds.contains(&build) {
                UpdatePrompt::Badge
            } else {
                UpdatePrompt::Sheet
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_793_779_200_000;

    fn decide(urgency: Urgency, history: &PromptHistory, session: &PromptSession) -> UpdatePrompt {
        decide_prompt(urgency, Some(200), NOW, history, session)
    }

    #[test]
    fn an_optional_release_pops_one_sheet_per_version() {
        let mut history = PromptHistory::default();
        let mut session = PromptSession::default();
        assert_eq!(
            decide(Urgency::Optional, &history, &session),
            UpdatePrompt::Sheet
        );
        history.record(
            &mut session,
            Urgency::Optional,
            Some(200),
            UpdatePrompt::Sheet,
            NOW,
        );
        assert_eq!(
            decide(Urgency::Optional, &history, &session),
            UpdatePrompt::Badge
        );
        // 重启之后同一版本不再弹，新版本还能弹。
        let restarted = PromptSession::default();
        assert_eq!(
            decide(Urgency::Optional, &history, &restarted),
            UpdatePrompt::Badge
        );
        assert_eq!(
            decide_prompt(Urgency::Optional, Some(300), NOW, &history, &restarted),
            UpdatePrompt::Sheet
        );
    }

    #[test]
    fn a_recommended_release_pops_once_a_day_and_defers_for_a_day() {
        let mut history = PromptHistory::default();
        let mut session = PromptSession::default();
        assert_eq!(
            decide(Urgency::Recommended, &history, &session),
            UpdatePrompt::Modal
        );
        history.record(
            &mut session,
            Urgency::Recommended,
            Some(200),
            UpdatePrompt::Modal,
            NOW,
        );
        let restarted = PromptSession::default();
        assert_eq!(
            decide_prompt(
                Urgency::Recommended,
                Some(200),
                NOW + DAY_MS - 1,
                &history,
                &restarted
            ),
            UpdatePrompt::Badge
        );
        assert_eq!(
            decide_prompt(
                Urgency::Recommended,
                Some(200),
                NOW + DAY_MS,
                &history,
                &restarted
            ),
            UpdatePrompt::Modal
        );

        let mut deferred = PromptHistory::default();
        deferred.defer(NOW);
        assert_eq!(
            decide_prompt(
                Urgency::Recommended,
                Some(200),
                NOW + DAY_MS - 1,
                &deferred,
                &restarted
            ),
            UpdatePrompt::Badge
        );
        assert_eq!(
            decide_prompt(
                Urgency::Recommended,
                Some(200),
                NOW + DAY_MS,
                &deferred,
                &restarted
            ),
            UpdatePrompt::Modal
        );
    }

    #[test]
    fn a_required_release_returns_on_every_foreground() {
        let mut history = PromptHistory::default();
        let mut session = PromptSession::default();
        let required = Urgency::Required { block_ui: false };
        assert_eq!(decide(required, &history, &session), UpdatePrompt::Modal);
        history.record(&mut session, required, Some(200), UpdatePrompt::Modal, NOW);
        assert_eq!(decide(required, &history, &session), UpdatePrompt::Badge);
        session.foregrounded();
        assert_eq!(decide(required, &history, &session), UpdatePrompt::Modal);
        // 没有可用版本也要提示。
        assert_eq!(
            decide_prompt(required, None, NOW, &history, &session),
            UpdatePrompt::Modal
        );
    }

    #[test]
    fn a_blocking_release_stays_fullscreen() {
        let mut history = PromptHistory::default();
        let mut session = PromptSession::default();
        let blocking = Urgency::Required { block_ui: true };
        history.record(
            &mut session,
            blocking,
            Some(200),
            UpdatePrompt::Fullscreen,
            NOW,
        );
        assert_eq!(
            decide(blocking, &history, &session),
            UpdatePrompt::Fullscreen
        );
    }

    #[test]
    fn the_flow_keeps_the_form_that_was_shown_or_the_one_its_class_implies() {
        assert_eq!(held_form(Urgency::Optional, None), UpdatePrompt::Sheet);
        assert_eq!(
            held_form(Urgency::Optional, Some(UpdatePrompt::Modal)),
            UpdatePrompt::Modal
        );
        assert_eq!(held_form(Urgency::Recommended, None), UpdatePrompt::Modal);
        let required = Urgency::Required { block_ui: false };
        assert_eq!(held_form(required, None), UpdatePrompt::Modal);
        assert_eq!(
            held_form(required, Some(UpdatePrompt::Sheet)),
            UpdatePrompt::Modal
        );
        assert_eq!(
            held_form(
                Urgency::Required { block_ui: true },
                Some(UpdatePrompt::Modal)
            ),
            UpdatePrompt::Fullscreen
        );
    }

    #[test]
    fn a_skipped_release_stays_quiet_and_the_memory_is_bounded() {
        let mut history = PromptHistory::default();
        history.skip(200);
        let session = PromptSession::default();
        assert_eq!(
            decide(Urgency::Optional, &history, &session),
            UpdatePrompt::None
        );
        for build in 0..100 {
            history.skip(build);
        }
        assert_eq!(history.skipped_builds.len(), MAX_REMEMBERED_BUILDS);
    }
}
