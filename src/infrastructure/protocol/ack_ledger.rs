//! 最近发出的已读 / 送达回执：换了新连接后补发一次。
//!
//! 回执写进一条正在死去的连接时，本端以为发出去了、服务端其实没收到：他端未读数会停在旧值，
//! 直到用户下一次阅读才自愈。回执是取最大值语义、可重复发送的，所以新连接就绪后把断链前一段时间里
//! 发过的回执（每个会话只留最大值）再发一次即可，不必知道哪些真的丢了。
//!
//! 有界：最多记 [`MAX_ENTRIES`] 个会话的最新回执，超出时淘汰最久没发过的。

use std::collections::HashMap;

use flare_proto::common::Ack;
use flare_proto::common::ack::Payload as AckPayload;

/// 记录上限（会话数 × 回执种类）。
pub const MAX_ENTRIES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AckKind {
    Read,
    Delivered,
}

#[derive(Debug, Clone)]
struct Entry {
    ack: Ack,
    seq: u64,
    sent_at_ms: u64,
}

#[derive(Debug, Default)]
pub struct AckLedger {
    entries: HashMap<(AckKind, String), Entry>,
}

impl AckLedger {
    /// 记下一个已发出的回执；同一会话只保留序号最大的那个。其它种类的回执不记（不是取最大值语义）。
    pub fn record(&mut self, ack: &Ack, sent_at_ms: u64) {
        let (kind, conversation_id, seq) = match ack.payload.as_ref() {
            Some(AckPayload::Read(read)) => (AckKind::Read, &read.conversation_id, read.read_seq),
            Some(AckPayload::Conversation(delivered)) => (
                AckKind::Delivered,
                &delivered.conversation_id,
                delivered.last_delivered_seq,
            ),
            _ => return,
        };
        if conversation_id.is_empty() {
            return;
        }
        let key = (kind, conversation_id.clone());
        match self.entries.get_mut(&key) {
            Some(entry) if entry.seq > seq => {}
            Some(entry) => {
                entry.ack = ack.clone();
                entry.seq = seq;
                entry.sent_at_ms = sent_at_ms;
            }
            None => {
                if self.entries.len() >= MAX_ENTRIES
                    && let Some(oldest) = self
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.sent_at_ms)
                        .map(|(key, _)| key.clone())
                {
                    self.entries.remove(&oldest);
                }
                self.entries.insert(
                    key,
                    Entry {
                        ack: ack.clone(),
                        seq,
                        sent_at_ms,
                    },
                );
            }
        }
    }

    /// `since_ms` 之后发过的回执（每个会话的最大值），按发出先后排序。
    pub fn since(&self, since_ms: u64) -> Vec<Ack> {
        let mut due: Vec<&Entry> = self
            .entries
            .values()
            .filter(|entry| entry.sent_at_ms >= since_ms)
            .collect();
        due.sort_by_key(|entry| entry.sent_at_ms);
        due.into_iter().map(|entry| entry.ack.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{ConversationAck, PushAck, ReadAck};

    fn read(conversation: &str, seq: u64) -> Ack {
        Ack {
            ack_id: None,
            ack_at: None,
            payload: Some(AckPayload::Read(ReadAck {
                conversation_id: conversation.into(),
                read_seq: seq,
                device_id: None,
                ack_id: None,
            })),
        }
    }

    fn seq_of(ack: &Ack) -> u64 {
        match ack.payload.as_ref() {
            Some(AckPayload::Read(read)) => read.read_seq,
            Some(AckPayload::Conversation(delivered)) => delivered.last_delivered_seq,
            _ => 0,
        }
    }

    /// 每个会话只留最大值；窗口外的不补发；不是取最大值语义的回执不记。
    #[test]
    fn keeps_the_latest_per_conversation_within_the_window() {
        let mut ledger = AckLedger::default();
        ledger.record(&read("c1", 5), 1_000);
        ledger.record(&read("c1", 3), 1_100);
        ledger.record(&read("c1", 7), 1_200);
        ledger.record(&read("c2", 2), 500);
        ledger.record(
            &Ack {
                ack_id: None,
                ack_at: None,
                payload: Some(AckPayload::Conversation(ConversationAck {
                    conversation_id: "c1".into(),
                    server_msg_ids: vec![],
                    last_delivered_seq: 9,
                    attributes: Default::default(),
                })),
            },
            1_300,
        );
        ledger.record(
            &Ack {
                ack_id: None,
                ack_at: None,
                payload: Some(AckPayload::Push(PushAck::default())),
            },
            1_400,
        );
        let due = ledger.since(900);
        assert_eq!(due.iter().map(seq_of).collect::<Vec<_>>(), vec![7, 9]);
        assert_eq!(ledger.len(), 3);
    }

    #[test]
    fn is_bounded() {
        let mut ledger = AckLedger::default();
        for i in 0..(MAX_ENTRIES as u64 + 10) {
            ledger.record(&read(&format!("c{i}"), 1), i);
        }
        assert_eq!(ledger.len(), MAX_ENTRIES);
        assert!(
            ledger.since(0).iter().all(|ack| seq_of(ack) == 1),
            "最旧的被淘汰"
        );
    }
}
