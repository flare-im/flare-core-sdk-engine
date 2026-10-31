use async_trait::async_trait;

use crate::domain::timeline::{SeqRange, TimelineCoverage};
use crate::shared::error::Result;

/// 时间线已证实连续区间的存储（`timeline_coverage(conversation_id, from_seq, to_seq)`）。
#[async_trait]
pub trait TimelineCoverageStore: Send + Sync {
    /// 该会话的全部区间，已合并成规范形式。
    async fn coverage(&self, conversation_id: &str) -> Result<TimelineCoverage>;

    /// 写入一段区间，与相邻、重叠的区间合并。
    async fn add(&self, conversation_id: &str, range: SeqRange) -> Result<()>;

    /// 删掉该会话的全部区间：会话连同本地消息一起删除时调用，否则区间会证明一段已经没有行的历史。
    async fn clear(&self, conversation_id: &str) -> Result<()>;
}
