//! 已证实连续区间。
//!
//! 每个会话一组互不重叠、互不相邻的闭区间 `[from, to]`（seq）。区间内每个 seq 要么本地有
//! 对应的消息行，要么已被服务端证明没有可见消息（非消息事件、tombstone、不可见历史）。
//! 往旧翻页只在区间内读本地，碰到区间边界才向服务端要；有没有更早的消息也只由区间回答，
//! 不再按「本地页装满没有」或「最旧一条是不是 seq 1」去猜。
//!
//! 区间只从服务端的证明写入：连续同步页、游标推进、冷启动尾页与快照行、回填页、本地清空记录。
//! 单条实时推送不写；本地删除、撤回、过期也不改区间（被删的消息是有意不显示的，那里仍然完整）。
//! 宁可漏标：漏标的代价是多一次回填，误标会藏住缺口。

use serde::{Deserialize, Serialize};

/// 闭区间 `[from, to]`，`1 <= from <= to`。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeqRange {
    pub from: u64,
    pub to: u64,
}

impl SeqRange {
    /// seq 从 1 起，`from = 0` 按 1 处理；空区间返回 `None`。
    pub fn new(from: u64, to: u64) -> Option<Self> {
        let from = from.max(1);
        (to >= from).then_some(Self { from, to })
    }

    pub fn contains(&self, seq: u64) -> bool {
        self.from <= seq && seq <= self.to
    }

    /// 重叠或首尾相接：并起来仍是一段。
    fn touches(&self, other: &Self) -> bool {
        self.from <= other.to.saturating_add(1) && other.from <= self.to.saturating_add(1)
    }
}

/// 一个会话的全部已证实区间：按 `from` 升序、互不重叠、互不相邻。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimelineCoverage {
    ranges: Vec<SeqRange>,
}

impl TimelineCoverage {
    pub fn new() -> Self {
        Self::default()
    }

    /// 任意顺序、可重叠的区间合并成规范形式（存储层读出的行不保证已合并）。
    pub fn from_ranges(ranges: impl IntoIterator<Item = SeqRange>) -> Self {
        let mut coverage = Self::default();
        for range in ranges {
            coverage.insert(range);
        }
        coverage
    }

    pub fn ranges(&self) -> &[SeqRange] {
        &self.ranges
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// 并入一段，与相邻、重叠的区间合并；返回区间是否因此变化。
    pub fn insert(&mut self, range: SeqRange) -> bool {
        if self
            .ranges
            .iter()
            .any(|existing| existing.from <= range.from && range.to <= existing.to)
        {
            return false;
        }
        let mut merged = range;
        self.ranges.retain(|existing| {
            if existing.touches(&merged) {
                merged = SeqRange {
                    from: existing.from.min(merged.from),
                    to: existing.to.max(merged.to),
                };
                false
            } else {
                true
            }
        });
        let at = self
            .ranges
            .partition_point(|existing| existing.from < merged.from);
        self.ranges.insert(at, merged);
        true
    }

    pub fn range_containing(&self, seq: u64) -> Option<SeqRange> {
        let at = self.ranges.partition_point(|range| range.to < seq);
        self.ranges
            .get(at)
            .filter(|range| range.contains(seq))
            .copied()
    }

    /// 最高已证实 seq；没有区间时为 0。比它新的本地行是「头部未证实行」。
    pub fn highest_proven_seq(&self) -> u64 {
        self.ranges.last().map(|range| range.to).unwrap_or(0)
    }

    /// 最高已证实 seq 所在的那一段，即接着最新消息的那一段。
    pub fn latest_range(&self) -> Option<SeqRange> {
        self.ranges.last().copied()
    }

    /// `[1, seq]` 是否全部已证实；`seq = 0` 恒为真。
    pub fn proven_from_start(&self, seq: u64) -> bool {
        seq == 0
            || self
                .ranges
                .first()
                .is_some_and(|range| range.from == 1 && range.to >= seq)
    }

    /// 并入可见下限 `[1, floor]`：本地清空记录与服务端的历史可见起点之下没有可见消息。
    pub fn with_visibility_floor(mut self, floor: u64) -> Self {
        if let Some(range) = cleared_history_range(floor) {
            self.insert(range);
        }
        self
    }

    /// 窗口最旧一行（seq 为 `oldest_seq`）之下还有没有可见消息：本地在它之下还有行，
    /// 或者 `[1, oldest_seq - 1]` 没有全部证实，都算还有。空窗口传会话 `max_seq + 1`。
    pub fn has_older(&self, oldest_seq: u64, local_rows_below: bool) -> bool {
        local_rows_below || !self.proven_from_start(oldest_seq.saturating_sub(1))
    }
}

/// 连续同步页：`after_seq` 之后逐个 seq 给出了消息、事件或 tombstone，连续到 `contiguous_to`。
/// 证明 `[after_seq + 1, contiguous_to]`；`after_seq = 0` 时从 1 起。
pub fn contiguous_page_range(after_seq: u64, contiguous_to: u64) -> Option<SeqRange> {
    SeqRange::new(after_seq.saturating_add(1), contiguous_to)
}

/// 同步游标从 `from` 推进到 `to`：推进前已逐个 seq 证明本地有行或服务端证明那里没有消息。
pub fn cursor_advance_range(from: u64, to: u64) -> Option<SeqRange> {
    SeqRange::new(from.saturating_add(1), to)
}

/// 冷启动尾页：服务端只回最新一段与水位 `max_seq`，证明从页内最小 seq 到水位；**不**从 1 起。
pub fn tail_page_range(min_item_seq: u64, max_seq: u64) -> Option<SeqRange> {
    if min_item_seq == 0 {
        return None;
    }
    SeqRange::new(min_item_seq, max_seq)
}

/// 快照行（冷启 bundle、gap-too-large 重建）：会话最新的若干条消息，由新到旧连着取出，
/// 证明它们的最小到最大 seq 之间没有别的消息。只到行内最大 seq，不延到水位。
pub fn snapshot_rows_range(seqs: impl IntoIterator<Item = u64>) -> Option<SeqRange> {
    let (mut min, mut max) = (u64::MAX, 0);
    for seq in seqs.into_iter().filter(|seq| *seq > 0) {
        min = min.min(seq);
        max = max.max(seq);
    }
    SeqRange::new(min, max)
}

/// 回填页（`before = before_seq`）：服务端取 `before_seq` 之前最新的一页可见消息，多取一条判断
/// `has_more`。
/// - 还有更早：页内最小 seq `m` 到 `before_seq - 1` 之间没有别的可见消息，证明 `[m, before_seq - 1]`；
/// - 没有更早：`before_seq` 之前的可见消息就是这一页（可以为空），证明 `[1, before_seq - 1]`。
///   新成员的历史可见下限由服务端执行，回填到下限时同样回 `has_more = false`，客户端不必知道下限。
pub fn backfill_page_range(
    before_seq: u64,
    item_seqs: impl IntoIterator<Item = u64>,
    has_more: bool,
) -> Option<SeqRange> {
    let to = before_seq.checked_sub(1)?;
    if !has_more {
        return SeqRange::new(1, to);
    }
    let min = item_seqs
        .into_iter()
        .filter(|seq| *seq > 0 && *seq < before_seq)
        .min()?;
    SeqRange::new(min, to)
}

/// 本地清空记录（可见下限 `floor`）：下限及以下没有可见消息，证明 `[1, floor]`。
pub fn cleared_history_range(floor: u64) -> Option<SeqRange> {
    SeqRange::new(1, floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(from: u64, to: u64) -> SeqRange {
        SeqRange::new(from, to).expect("non-empty range")
    }

    fn pairs(coverage: &TimelineCoverage) -> Vec<(u64, u64)> {
        coverage
            .ranges()
            .iter()
            .map(|range| (range.from, range.to))
            .collect()
    }

    #[test]
    fn empty_and_reversed_ranges_are_rejected_and_zero_starts_at_one() {
        assert_eq!(SeqRange::new(5, 4), None);
        assert_eq!(SeqRange::new(0, 0), None);
        assert_eq!(SeqRange::new(0, 3), Some(range(1, 3)));
    }

    #[test]
    fn insert_merges_overlapping_and_adjacent_ranges() {
        let mut coverage = TimelineCoverage::new();
        assert!(coverage.insert(range(10, 20)));
        assert!(coverage.insert(range(30, 40)));
        assert_eq!(pairs(&coverage), vec![(10, 20), (30, 40)]);

        assert!(coverage.insert(range(21, 25)), "adjacent below is joined");
        assert_eq!(pairs(&coverage), vec![(10, 25), (30, 40)]);

        assert!(coverage.insert(range(26, 29)), "filling the gap joins both");
        assert_eq!(pairs(&coverage), vec![(10, 40)]);

        assert!(
            coverage.insert(range(1, 12)),
            "overlap below extends the start"
        );
        assert_eq!(pairs(&coverage), vec![(1, 40)]);

        assert!(
            !coverage.insert(range(5, 39)),
            "a contained range changes nothing"
        );
        assert_eq!(pairs(&coverage), vec![(1, 40)]);
    }

    #[test]
    fn from_ranges_normalises_unordered_overlapping_rows() {
        let coverage =
            TimelineCoverage::from_ranges([range(50, 60), range(1, 3), range(55, 70), range(4, 4)]);
        assert_eq!(pairs(&coverage), vec![(1, 4), (50, 70)]);
    }

    #[test]
    fn lookups_answer_from_the_merged_ranges() {
        let coverage = TimelineCoverage::from_ranges([range(1, 36), range(980, 1000)]);
        assert_eq!(coverage.range_containing(36), Some(range(1, 36)));
        assert_eq!(coverage.range_containing(37), None);
        assert_eq!(coverage.range_containing(980), Some(range(980, 1000)));
        assert_eq!(coverage.range_containing(1001), None);
        assert_eq!(coverage.highest_proven_seq(), 1000);
        assert_eq!(coverage.latest_range(), Some(range(980, 1000)));
        assert!(coverage.proven_from_start(36));
        assert!(!coverage.proven_from_start(37));
        assert!(coverage.proven_from_start(0));
        assert!(!TimelineCoverage::new().proven_from_start(1));
    }

    #[test]
    fn has_older_is_decided_by_the_ranges_and_the_rows_below() {
        let coverage = TimelineCoverage::from_ranges([range(1, 12)]);
        assert!(
            !coverage.has_older(3, false),
            "1 and 2 are proven to hold no message"
        );
        assert!(
            coverage.has_older(3, true),
            "rows below the window are older messages"
        );
        let tail = TimelineCoverage::from_ranges([range(980, 1000)]);
        assert!(
            tail.has_older(980, false),
            "a tail range does not reach the start"
        );
        assert!(
            !TimelineCoverage::new().has_older(1, false),
            "nothing is below seq 1"
        );
        assert!(TimelineCoverage::new().has_older(2, false));
    }

    #[test]
    fn the_visibility_floor_is_proven_from_the_start() {
        let coverage = TimelineCoverage::from_ranges([range(11, 14)]).with_visibility_floor(10);
        assert_eq!(pairs(&coverage), vec![(1, 14)]);
        assert!(!coverage.has_older(11, false));
        assert_eq!(
            pairs(&TimelineCoverage::new().with_visibility_floor(0)),
            vec![]
        );
    }

    #[test]
    fn a_contiguous_page_proves_from_after_plus_one() {
        assert_eq!(contiguous_page_range(0, 12), Some(range(1, 12)));
        assert_eq!(contiguous_page_range(40, 52), Some(range(41, 52)));
        assert_eq!(
            contiguous_page_range(40, 40),
            None,
            "no progress proves nothing"
        );
        assert_eq!(cursor_advance_range(990, 1000), Some(range(991, 1000)));
        assert_eq!(cursor_advance_range(990, 990), None);
    }

    #[test]
    fn a_cold_start_tail_does_not_start_at_one() {
        assert_eq!(tail_page_range(980, 1000), Some(range(980, 1000)));
        assert_eq!(tail_page_range(0, 1000), None);
        assert_eq!(
            snapshot_rows_range([1000, 980, 0, 990]),
            Some(range(980, 1000))
        );
        assert_eq!(snapshot_rows_range([]), None);
    }

    #[test]
    fn a_backfill_page_proves_down_to_its_oldest_row_or_to_the_start() {
        assert_eq!(
            backfill_page_range(980, [940, 979, 960], true),
            Some(range(940, 979))
        );
        assert_eq!(
            backfill_page_range(5, [3, 4], false),
            Some(range(1, 4)),
            "no more history: the start is reached"
        );
        assert_eq!(
            backfill_page_range(5000, [], false),
            Some(range(1, 4999)),
            "an empty last page proves everything below"
        );
        assert_eq!(backfill_page_range(5000, [], true), None);
        assert_eq!(
            backfill_page_range(10, [12, 7], true),
            Some(range(7, 9)),
            "rows at or above the anchor are ignored"
        );
        assert_eq!(backfill_page_range(1, [], false), None);
        assert_eq!(cleared_history_range(10), Some(range(1, 10)));
        assert_eq!(cleared_history_range(0), None);
    }
}
