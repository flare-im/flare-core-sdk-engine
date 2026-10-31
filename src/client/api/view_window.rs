//! 时间线视图窗口的纯逻辑：窗口状态、两版窗口之间的增量、实时合并与滑动裁剪。
//!
//! 窗口是按时间线顺序（[`IMMessage::compare_for_timeline_asc`]）排列的一段消息行。相邻两行
//! 之间的 seq 由已证实连续区间覆盖；唯一的例外是接在最新时尾部的头部未证实行与待发送行。
//! 增量按行键（[`IMMessage::row_key`]）表达，不带下标：`prepend` / `append` / `upsert` /
//! `remove`，顺序变化前四步表达不了时附 `reorder`（窗口的全部键）。

use std::collections::{HashMap, HashSet};

use crate::model::{Conversation, IMMessage, TimelineViewSnapshot, ViewDelta};

/// 滑动窗口上限：往旧翻超过时从新的一端裁掉（窗口随之脱离最新），往新翻或实时追加超过时
/// 从旧的一端裁掉。
pub(super) const MAX_TIMELINE_WINDOW_MESSAGES: usize = 2_000;

#[derive(Clone, Debug)]
pub(super) struct TimelineWindow {
    pub conversation_id: String,
    pub conversation: Option<Conversation>,
    /// 时间线顺序（旧在前）。
    pub messages: Vec<IMMessage>,
    pub has_older: bool,
    pub attached_to_latest: bool,
    pub anchor_key: Option<String>,
}

impl TimelineWindow {
    pub fn has_newer(&self) -> bool {
        !self.attached_to_latest
    }

    pub fn snapshot(&self) -> TimelineViewSnapshot {
        TimelineViewSnapshot {
            conversation: self.conversation.clone(),
            messages: self.messages.clone(),
            has_older: self.has_older,
            has_newer: self.has_newer(),
            attached_to_latest: self.attached_to_latest,
            anchor_key: self.anchor_key.clone(),
        }
    }

    /// 窗口里最小的已分配 seq（待发送行不算）。
    pub fn oldest_seq(&self) -> Option<u64> {
        self.messages
            .iter()
            .map(|message| message.conversation_seq)
            .filter(|seq| *seq > 0)
            .min()
    }

    /// 窗口里最大的已分配 seq（待发送行不算）。
    pub fn newest_seq(&self) -> Option<u64> {
        self.messages
            .iter()
            .map(|message| message.conversation_seq)
            .filter(|seq| *seq > 0)
            .max()
    }

    pub fn keys(&self) -> HashSet<String> {
        self.messages.iter().map(IMMessage::row_key).collect()
    }

    /// 超过上限时从旧的一端裁掉；裁掉了就还有更早的。
    pub fn trim_older_end(&mut self, max: usize) {
        if self.messages.len() > max {
            let overflow = self.messages.len() - max;
            self.messages.drain(..overflow);
            self.has_older = true;
        }
    }

    /// 超过上限时从新的一端裁掉；裁掉了窗口就脱离最新（待发送行一并留在最新那头）。
    pub fn trim_newer_end(&mut self, max: usize) {
        if self.messages.len() > max {
            self.messages.truncate(max);
            self.attached_to_latest = false;
            self.messages.retain(|message| message.conversation_seq > 0);
        }
    }

    fn sort(&mut self) {
        self.messages.sort_by(IMMessage::compare_for_timeline_asc);
    }

    fn is_sorted(&self) -> bool {
        self.messages
            .windows(2)
            .all(|pair| IMMessage::compare_for_timeline_asc(&pair[0], &pair[1]).is_le())
    }
}

/// 两版窗口之间的变化。
#[derive(Debug)]
pub(super) enum TimelineChange {
    /// 没有任何变化：不推更新，版本号不动。
    None,
    Delta(Box<ViewDelta>),
    /// 增量不划算或无法表达（行全部换掉、行键重复），整体替换。
    Snapshot,
}

/// 哪些保留下来的行要比较内容。窗口只在一头增减行时（往旧、往新、实时追加）保留的行没有变，
/// 不必逐行序列化比较；按 id 重读与范围重读才需要。
pub(super) enum CompareRows<'a> {
    None,
    Keys(&'a HashSet<String>),
    All,
}

pub(super) fn timeline_change(
    old: &TimelineWindow,
    new: &TimelineWindow,
    compare: CompareRows<'_>,
) -> TimelineChange {
    let old_keys = old
        .messages
        .iter()
        .map(IMMessage::row_key)
        .collect::<Vec<_>>();
    let new_keys = new
        .messages
        .iter()
        .map(IMMessage::row_key)
        .collect::<Vec<_>>();
    let old_index = old_keys
        .iter()
        .enumerate()
        .map(|(index, key)| (key.as_str(), index))
        .collect::<HashMap<_, _>>();
    let new_set = new_keys.iter().map(String::as_str).collect::<HashSet<_>>();
    if old_index.len() != old_keys.len() || new_set.len() != new_keys.len() {
        return TimelineChange::Snapshot;
    }

    let remove = old_keys
        .iter()
        .filter(|key| !new_set.contains(key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let retained_old_order = old_keys
        .iter()
        .filter(|key| new_set.contains(key.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    let is_kept = |index: usize| old_index.contains_key(new_keys[index].as_str());

    let (prepend, append, reorder) = match (
        (0..new_keys.len()).position(is_kept),
        (0..new_keys.len()).rposition(is_kept),
    ) {
        (Some(first), Some(last)) => {
            let prepend = new.messages[..first].to_vec();
            let middle_added = (first..=last)
                .filter(|index| !is_kept(*index))
                .map(|index| new.messages[index].clone())
                .collect::<Vec<_>>();
            let middle_kept = (first..=last)
                .filter(|index| is_kept(*index))
                .map(|index| new_keys[index].as_str())
                .collect::<Vec<_>>();
            let tail = new.messages[last + 1..].to_vec();
            if middle_added.is_empty() && middle_kept == retained_old_order {
                (prepend, tail, None)
            } else {
                let mut append = middle_added;
                append.extend(tail);
                (prepend, append, Some(new_keys.clone()))
            }
        }
        _ if old.messages.is_empty() => (Vec::new(), new.messages.clone(), None),
        _ if new.messages.is_empty() => (Vec::new(), Vec::new(), None),
        // 一行都没留下：整体替换比逐行删加便宜。
        _ => return TimelineChange::Snapshot,
    };

    let upsert = new
        .messages
        .iter()
        .filter(|message| {
            let key = message.row_key();
            let Some(index) = old_index.get(key.as_str()) else {
                return false;
            };
            let should_compare = match &compare {
                CompareRows::None => false,
                CompareRows::Keys(keys) => keys.contains(&key),
                CompareRows::All => true,
            };
            should_compare && row_changed(&old.messages[*index], message)
        })
        .cloned()
        .collect::<Vec<_>>();

    let conversation_changed = json_changed(&old.conversation, &new.conversation);
    let state_changed =
        old.has_older != new.has_older || old.attached_to_latest != new.attached_to_latest;
    if prepend.is_empty()
        && append.is_empty()
        && upsert.is_empty()
        && remove.is_empty()
        && reorder.is_none()
        && !state_changed
        && !conversation_changed
    {
        return TimelineChange::None;
    }
    TimelineChange::Delta(Box::new(ViewDelta::Timeline {
        prepend,
        append,
        upsert,
        remove,
        reorder,
        has_older: new.has_older,
        has_newer: new.has_newer(),
        attached_to_latest: new.attached_to_latest,
        conversation: conversation_changed
            .then(|| new.conversation.clone())
            .flatten(),
    }))
}

/// 并入窗口之下的一页（由新到旧）。已在窗口里的行跳过；返回并入了几行。
pub(super) fn prepend_older(window: &mut TimelineWindow, older: &[IMMessage]) -> u32 {
    let keys = window.keys();
    let mut rows = older
        .iter()
        .filter(|message| !keys.contains(&message.row_key()))
        .cloned()
        .collect::<Vec<_>>();
    let loaded = rows.len() as u32;
    rows.sort_by(IMMessage::compare_for_timeline_asc);
    rows.append(&mut window.messages);
    window.messages = rows;
    if !window.is_sorted() {
        window.sort();
    }
    loaded
}

/// 并入窗口之上的一页（由旧到新）。已在窗口里的行跳过；返回并入了几行。
pub(super) fn append_newer(window: &mut TimelineWindow, newer: &[IMMessage]) -> u32 {
    let keys = window.keys();
    let mut loaded = 0;
    for message in newer {
        if !keys.contains(&message.row_key()) {
            window.messages.push(message.clone());
            loaded += 1;
        }
    }
    if !window.is_sorted() {
        window.sort();
    }
    loaded
}

/// 实时消息并入窗口：已在窗口里的行原位更新；接在最新时，不比窗口最旧一行更旧的新行
/// （含待发送行）按时间线顺序并入。脱离最新时只更新已在窗口里的行，新行一律不进。
/// 返回窗口是否变化。
pub(super) fn merge_live(window: &mut TimelineWindow, incoming: &[IMMessage]) -> bool {
    let mut index_by_key = window
        .messages
        .iter()
        .enumerate()
        .map(|(index, message)| (message.row_key(), index))
        .collect::<HashMap<_, _>>();
    let oldest = window.oldest_seq();
    let mut changed = false;
    for message in incoming
        .iter()
        .filter(|message| message.conversation_id.trim() == window.conversation_id)
    {
        let key = message.row_key();
        if let Some(&index) = index_by_key.get(&key) {
            if row_changed(&window.messages[index], message) {
                window.messages[index] = message.clone();
                changed = true;
            }
            continue;
        }
        if !window.attached_to_latest {
            continue;
        }
        let seq = message.conversation_seq;
        // 窗口之下的行（同步或回填顺带推来的历史）不接到窗口下面：那里可能隔着缺口，
        // 往旧翻页会按区间把它们读进来。
        if seq > 0 && oldest.is_some_and(|oldest| seq < oldest) {
            continue;
        }
        index_by_key.insert(key, window.messages.len());
        window.messages.push(message.clone());
        changed = true;
    }
    if changed && !window.is_sorted() {
        window.sort();
    }
    changed
}

/// 用按 id 重读到的行更新窗口：重读到的行原位替换，`targets` 里没读到的行（已删除或不再可见）
/// 移出窗口。行键之外的排序字段可能变了（发送确认分配了 seq），必要时重排。
pub(super) fn apply_reread_rows(
    window: &mut TimelineWindow,
    targets: &HashSet<String>,
    rows: &[IMMessage],
    visibility_floor: u64,
) {
    let fresh = rows
        .iter()
        .filter(|message| {
            message.conversation_id.trim() == window.conversation_id
                && (message.conversation_seq == 0 || message.conversation_seq > visibility_floor)
        })
        .map(|message| (message.row_key(), message))
        .collect::<HashMap<_, _>>();
    window.messages.retain_mut(|message| {
        let key = message.row_key();
        if !targets.contains(&key) {
            return true;
        }
        match fresh.get(&key) {
            Some(row) => {
                *message = (*row).clone();
                true
            }
            None => false,
        }
    });
    if !window.is_sorted() {
        window.sort();
    }
}

fn row_changed(left: &IMMessage, right: &IMMessage) -> bool {
    json_changed(left, right)
}

pub(super) fn json_changed<T: serde::Serialize>(left: &T, right: &T) -> bool {
    serde_json::to_value(left).ok() != serde_json::to_value(right).ok()
}

#[cfg(test)]
mod tests {
    use flare_proto::common::Message as ProtoMessage;

    use super::*;

    fn row(key: &str, seq: u64) -> IMMessage {
        IMMessage::new(ProtoMessage {
            conversation_id: "c1".into(),
            client_msg_id: key.into(),
            server_id: format!("s-{key}"),
            conversation_seq: seq,
            ..Default::default()
        })
    }

    fn window(rows: Vec<IMMessage>) -> TimelineWindow {
        TimelineWindow {
            conversation_id: "c1".into(),
            conversation: None,
            messages: rows,
            has_older: true,
            attached_to_latest: true,
            anchor_key: None,
        }
    }

    fn delta(change: TimelineChange) -> ViewDelta {
        match change {
            TimelineChange::Delta(delta) => *delta,
            other => panic!("expected a delta, got {other:?}"),
        }
    }

    fn keys(rows: &[IMMessage]) -> Vec<String> {
        rows.iter().map(IMMessage::row_key).collect()
    }

    #[test]
    fn rows_added_at_both_ends_are_prepend_and_append() {
        let old = window(vec![row("b", 2), row("c", 3)]);
        let new = window(vec![row("a", 1), row("b", 2), row("c", 3), row("d", 4)]);
        let ViewDelta::Timeline {
            prepend,
            append,
            upsert,
            remove,
            reorder,
            ..
        } = delta(timeline_change(&old, &new, CompareRows::None))
        else {
            panic!("timeline delta");
        };
        assert_eq!(keys(&prepend), vec!["a"]);
        assert_eq!(keys(&append), vec!["d"]);
        assert!(upsert.is_empty() && remove.is_empty() && reorder.is_none());
    }

    #[test]
    fn a_row_added_in_the_middle_comes_with_a_reorder() {
        let old = window(vec![row("a", 1), row("c", 3)]);
        let new = window(vec![row("a", 1), row("b", 2), row("c", 3)]);
        let ViewDelta::Timeline {
            append, reorder, ..
        } = delta(timeline_change(&old, &new, CompareRows::None))
        else {
            panic!("timeline delta");
        };
        assert_eq!(keys(&append), vec!["b"]);
        assert_eq!(reorder, Some(vec!["a".into(), "b".into(), "c".into()]));
    }

    #[test]
    fn a_changed_row_is_an_upsert_only_when_compared() {
        let old = window(vec![row("a", 1)]);
        let mut edited = row("a", 1);
        edited.is_edited = true;
        let new = window(vec![edited]);
        assert!(matches!(
            timeline_change(&old, &new, CompareRows::None),
            TimelineChange::None
        ));
        let ViewDelta::Timeline { upsert, .. } =
            delta(timeline_change(&old, &new, CompareRows::All))
        else {
            panic!("timeline delta");
        };
        assert_eq!(keys(&upsert), vec!["a"]);
    }

    #[test]
    fn replacing_every_row_is_a_snapshot_and_duplicate_keys_too() {
        let old = window(vec![row("a", 1)]);
        let new = window(vec![row("b", 2)]);
        assert!(matches!(
            timeline_change(&old, &new, CompareRows::None),
            TimelineChange::Snapshot
        ));
        let duplicated = window(vec![row("a", 1), row("a", 2)]);
        assert!(matches!(
            timeline_change(&old, &duplicated, CompareRows::None),
            TimelineChange::Snapshot
        ));
    }

    #[test]
    fn a_detached_window_takes_no_new_rows_from_live_messages() {
        let mut detached = window(vec![row("a", 1)]);
        detached.attached_to_latest = false;
        assert!(!merge_live(&mut detached, &[row("b", 2)]));
        assert_eq!(keys(&detached.messages), vec!["a"]);

        let mut attached = window(vec![row("a", 5)]);
        assert!(merge_live(&mut attached, &[row("old", 1), row("b", 6)]));
        assert_eq!(keys(&attached.messages), vec!["a", "b"]);
    }

    #[test]
    fn trimming_the_newer_end_detaches_the_window() {
        let mut full = window(vec![row("a", 1), row("b", 2), row("c", 3)]);
        full.trim_newer_end(2);
        assert_eq!(keys(&full.messages), vec!["a", "b"]);
        assert!(!full.attached_to_latest);
        let mut tail = window(vec![row("a", 1), row("b", 2), row("c", 3)]);
        tail.has_older = false;
        tail.trim_older_end(2);
        assert_eq!(keys(&tail.messages), vec!["b", "c"]);
        assert!(tail.has_older);
    }
}
