use crate::domain::ConversationReadDecision;
use crate::model::Conversation;

pub struct ConversationReadService;

const READ_INTENT: &str = "_local.pendingReadSeq";

pub(crate) fn pending_read_seq(row: &Conversation) -> Option<u64> {
    row.ext.get(READ_INTENT)?.parse().ok().filter(|n| *n > 0)
}

pub(crate) fn set_read_intent(row: &mut Conversation, seq: Option<u64>) {
    if let Some(seq) = seq {
        row.ext.insert(READ_INTENT.into(), seq.to_string());
    } else {
        row.ext.remove(READ_INTENT);
    }
}

impl ConversationReadService {
    pub fn plan_mark_read(
        &self,
        current: &Conversation,
        local_max_seq: u64,
        requested_read_seq: u64,
    ) -> ConversationReadDecision {
        // 会话摘要 max_seq 可能滞后于本地消息表；已读位点应对齐两者中的较大值。
        let effective_max_seq = local_max_seq
            .max(current.max_seq)
            .max(current.last_read_seq);
        let target_read_seq = requested_read_seq.min(effective_max_seq);
        let next_read_seq = target_read_seq
            .max(current.last_read_seq)
            .min(effective_max_seq);
        let unread_count = if next_read_seq >= effective_max_seq {
            0
        } else {
            current.unread_count
        };
        ConversationReadDecision {
            unread_count,
            next_read_seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConversationReadService;
    use crate::model::Conversation;

    #[test]
    fn mark_read_zero_keeps_current_read_position() {
        let service = ConversationReadService;
        let conversation = Conversation {
            max_seq: 50,
            last_read_seq: 10,
            unread_count: 5,
            ..Default::default()
        };

        let decision = service.plan_mark_read(&conversation, 80, 0);

        assert_eq!(decision.next_read_seq, 10);
        assert_eq!(decision.unread_count, 5);
    }

    #[test]
    fn mark_read_non_zero_is_clamped_by_effective_max_seq() {
        let service = ConversationReadService;
        let conversation = Conversation {
            max_seq: 50,
            last_read_seq: 10,
            unread_count: 5,
            ..Default::default()
        };

        let decision = service.plan_mark_read(&conversation, 80, 999);

        assert_eq!(decision.next_read_seq, 80);
        assert_eq!(decision.unread_count, 0);
    }
}
