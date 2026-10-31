//! 时间线领域规则：哪些 seq 段已被服务端证明完整（已证实连续区间）。

mod coverage;

pub use coverage::{
    SeqRange, TimelineCoverage, backfill_page_range, cleared_history_range, contiguous_page_range,
    cursor_advance_range, snapshot_rows_range, tail_page_range,
};
