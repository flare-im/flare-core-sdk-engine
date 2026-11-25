//! 会话读位与未读一致性策略（单一真相）。
//!
//! 覆盖：摘要合并、冷启动 recompute 门禁、实时消息未读、已读上报 seq。

use crate::model::Conversation;

/// 用户在会话内的读位快照（`max_seq` / `last_read_seq` / `unread_count`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPosition {
    pub max_seq: u64,
    pub last_read_seq: u64,
    pub unread_count: u32,
}

impl Default for ReadPosition {
    fn default() -> Self {
        Self::from_parts(0, 0, 0)
    }
}

impl ReadPosition {
    #[must_use]
    pub fn from_conversation(conversation: &Conversation) -> Self {
        Self::from_parts(
            conversation.max_seq,
            conversation.last_read_seq,
            conversation.unread_count,
        )
    }

    #[must_use]
    pub const fn from_parts(max_seq: u64, last_read_seq: u64, unread_count: u32) -> Self {
        Self {
            max_seq,
            last_read_seq,
            unread_count,
        }
    }

    /// 修正本地脏数据（只更新了 unread 或已读但未写 last_read）。
    #[must_use]
    pub fn normalize_local(self) -> Self {
        let mut pos = self;
        if pos.max_seq > 0
            && pos.last_read_seq == 0
            && pos.unread_count > 0
            && (pos.unread_count as u64) < pos.max_seq
        {
            pos.last_read_seq = pos.max_seq.saturating_sub(pos.unread_count as u64);
        }
        if pos.max_seq > 0 && pos.unread_count == 0 && pos.last_read_seq < pos.max_seq {
            pos.last_read_seq = pos.max_seq;
        }
        pos
    }

    /// 仅用于在线未读增量计算的保守修正。
    ///
    /// 不根据 `unread_count == 0` 把 `last_read_seq` 推到 `max_seq`，因为服务端或本地历史
    /// 数据可能已经把 `max_seq` 推进但未读仍错误为 0；此时实时新消息仍应能恢复未读角标。
    #[must_use]
    pub fn normalize_for_unread_delta(self) -> Self {
        let mut pos = self;
        if pos.max_seq > 0
            && pos.last_read_seq == 0
            && pos.unread_count > 0
            && (pos.unread_count as u64) < pos.max_seq
        {
            pos.last_read_seq = pos.max_seq.saturating_sub(pos.unread_count as u64);
        }
        pos
    }

    /// 读位已稳定（用户明确读过会话尾部或仅差 1 条）。
    #[must_use]
    pub fn is_established(self) -> bool {
        self.max_seq > 0 && self.last_read_seq >= self.max_seq.saturating_sub(1)
    }

    #[must_use]
    pub fn unread_upper_bound(self) -> u32 {
        self.max_seq.saturating_sub(self.last_read_seq) as u32
    }

    /// 未读与读位是否自洽（`last_read + unread ≈ max`）。
    #[must_use]
    pub fn read_covers_tail(self) -> bool {
        self.last_read_seq >= self.max_seq.saturating_sub(self.unread_count as u64)
    }

    /// 上报服务端的已读 seq；`None` 表示无可信读位可推。
    #[must_use]
    pub fn ack_read_seq(self) -> Option<u64> {
        // An unread count excludes own messages and non-message events. With any
        // unread remaining, only the stored cursor proves a contiguous read prefix.
        let seq = if self.unread_count == 0 {
            self.max_seq.max(self.last_read_seq)
        } else {
            self.last_read_seq
        };
        (seq > 0).then_some(seq)
    }

    /// 服务端摘要落库：合并本地与服务端读位/未读。
    #[must_use]
    pub fn merge_with_incoming_summary(local: Self, incoming: Self) -> Self {
        // Unread counts can be sparse; only an explicit read position proves a read prefix.
        let merged_max = incoming.max_seq.max(local.max_seq);
        let last_read = incoming.last_read_seq.max(local.last_read_seq);
        let upper = merged_max.saturating_sub(last_read) as u32;
        let unread = if incoming.max_seq <= local.max_seq {
            local.unread_count.min(upper)
        } else {
            incoming.unread_count.min(upper)
        };
        if local.max_seq > 0 && local.last_read_seq >= merged_max && merged_max > 0 {
            return Self {
                max_seq: merged_max,
                last_read_seq: local.last_read_seq.max(last_read),
                unread_count: 0,
            };
        }
        Self {
            max_seq: merged_max,
            last_read_seq: last_read,
            unread_count: unread,
        }
    }

    /// 摘要同步后是否跳过 `recompute_unread`（避免冷启动按历史消息重算）。
    #[must_use]
    pub fn skip_recompute_after_summary_sync(self, incoming_max_seq: u64) -> bool {
        let local = self.normalize_local();
        if local.max_seq == 0 || incoming_max_seq > local.max_seq {
            return false;
        }
        local.unread_count <= 1 || local.read_covers_tail() || local.is_established()
    }

    /// 消息投影后是否应重算未读。
    #[must_use]
    pub fn should_recompute_unread_after_message(
        self,
        peer_message_seq: u64,
        is_from_peer: bool,
    ) -> bool {
        if !is_from_peer {
            return false;
        }
        peer_message_seq > self.max_seq
            || (self.is_established() && peer_message_seq > self.last_read_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::ReadPosition;

    #[test]
    fn normalize_heals_unread_without_last_read() {
        let pos = ReadPosition::from_parts(100, 0, 3).normalize_local();
        assert_eq!(pos.last_read_seq, 97);
    }

    #[test]
    fn merge_prefers_local_unread_when_max_unchanged() {
        let local = ReadPosition::from_parts(100, 99, 1);
        let incoming = ReadPosition::from_parts(100, 99, 34);
        let merged = ReadPosition::merge_with_incoming_summary(local, incoming);
        assert_eq!(merged.last_read_seq, 99);
        assert_eq!(merged.unread_count, 1);
    }

    #[test]
    fn merge_does_not_promote_zero_unread_summary_to_read_tail() {
        let local = ReadPosition::from_parts(0, 0, 0);
        let incoming = ReadPosition::from_parts(100, 0, 0);
        let merged = ReadPosition::merge_with_incoming_summary(local, incoming);
        assert_eq!(merged.max_seq, 100);
        assert_eq!(merged.last_read_seq, 0);
        assert_eq!(merged.unread_count, 0);
    }

    #[test]
    fn skip_recompute_when_local_established() {
        let local = ReadPosition::from_parts(100, 100, 0);
        assert!(local.skip_recompute_after_summary_sync(100));
    }

    #[test]
    fn message_recompute_when_seq_advances_max() {
        let local = ReadPosition::from_parts(150, 149, 1);
        assert!(local.should_recompute_unread_after_message(151, true));
    }

    #[test]
    fn message_skip_replay_when_read_not_established() {
        let local = ReadPosition::from_parts(100, 0, 0);
        assert!(!local.should_recompute_unread_after_message(100, true));
    }

    #[test]
    fn ack_read_seq_does_not_infer_a_prefix_from_sparse_unread() {
        assert_eq!(ReadPosition::from_parts(2, 0, 1).ack_read_seq(), None);
        assert_eq!(ReadPosition::from_parts(10, 3, 1).ack_read_seq(), Some(3));
    }

    #[test]
    fn ack_read_seq_promotes_max_when_fully_read() {
        let pos = ReadPosition::from_parts(100, 80, 0);
        assert_eq!(pos.ack_read_seq(), Some(100));
    }
}
