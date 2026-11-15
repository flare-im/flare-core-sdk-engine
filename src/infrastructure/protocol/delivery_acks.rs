//! 送达确认的合并：一小段时间内各会话处理到的最大 seq 合成一个批确认发出。
//!
//! 消息常常成串到达，逐条确认就是每条消息多一个上行帧。确认是取最大值语义，所以只留每个会话的最大
//! seq，攒 [`COALESCE_WINDOW`] 发一次；这个窗口远小于服务端等确认的期限（后台 1.5 秒、前台 3 秒）。
//!
//! 这里只管合并与发送。一条消息该不该确认（谁确认谁负责提示）由调用方在消息到达时判定。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use flare_proto::common::ConversationAck;

use super::PacketSender;
use crate::shared::util::{delay, spawn_background};

/// 攒多久发一次。
pub(crate) const COALESCE_WINDOW: Duration = Duration::from_millis(200);

#[derive(Default)]
struct Pending {
    /// 会话 → 待确认的最大 seq。
    highest: HashMap<String, u64>,
    /// 已经有一次定时发送在路上。
    flush_scheduled: bool,
}

pub(crate) struct DeliveryAcks {
    sender: Arc<PacketSender>,
    pending: Mutex<Pending>,
}

impl DeliveryAcks {
    pub fn new(sender: Arc<PacketSender>) -> Self {
        Self {
            sender,
            pending: Mutex::default(),
        }
    }

    fn pending(&self) -> MutexGuard<'_, Pending> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 记下一批已处理完的消息（会话、seq）。窗口里的第一批负责定时，到点一起发出。
    pub fn confirm(self: &Arc<Self>, delivered: Vec<(String, u64)>) {
        if !self.note(delivered) {
            return;
        }
        let acks = Arc::downgrade(self);
        spawn_background(async move {
            delay(COALESCE_WINDOW).await;
            if let Some(acks) = acks.upgrade() {
                acks.flush().await;
            }
        });
    }

    /// 并入待确认集合；返回是否要为它安排一次定时发送。
    fn note(&self, delivered: Vec<(String, u64)>) -> bool {
        let mut pending = self.pending();
        for (conversation_id, seq) in delivered {
            if conversation_id.is_empty() || seq == 0 {
                continue;
            }
            let highest = pending.highest.entry(conversation_id).or_default();
            *highest = (*highest).max(seq);
        }
        if pending.highest.is_empty() || pending.flush_scheduled {
            return false;
        }
        pending.flush_scheduled = true;
        true
    }

    /// 取走待确认集合（按会话排序，输出稳定）。
    fn drain(&self) -> Vec<ConversationAck> {
        let mut pending = self.pending();
        pending.flush_scheduled = false;
        let mut highest: Vec<(String, u64)> = pending.highest.drain().collect();
        drop(pending);
        highest.sort();
        highest
            .into_iter()
            .map(|(conversation_id, last_delivered_seq)| ConversationAck {
                conversation_id,
                last_delivered_seq,
                ..Default::default()
            })
            .collect()
    }

    /// 攒着还没发的确认（会话、seq），按会话排序。
    #[cfg(test)]
    pub(crate) fn pending_confirmations(&self) -> Vec<(String, u64)> {
        let mut pending: Vec<(String, u64)> = self
            .pending()
            .highest
            .iter()
            .map(|(conversation_id, seq)| (conversation_id.clone(), *seq))
            .collect();
        pending.sort();
        pending
    }

    /// 立即把攒着的确认发出去（定时到点，或托管前冲刷）。发不出去就丢：确认只对写出它们的那条
    /// 连接有意义，服务端等不到会把消息转给推送，提示不会漏。
    pub async fn flush(&self) {
        let acks = self.drain();
        if acks.is_empty() {
            return;
        }
        let count = acks.len();
        if let Err(error) = self.sender.send_delivery_acks(acks).await {
            tracing::debug!(%error, count, "delivery confirmations were not sent");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::protocol::ProtobufCodec;

    fn acks() -> DeliveryAcks {
        DeliveryAcks::new(Arc::new(PacketSender::new(
            Arc::new(tokio::sync::Mutex::new(None)),
            Arc::new(ProtobufCodec),
        )))
    }

    fn pairs(acks: &[ConversationAck]) -> Vec<(&str, u64)> {
        acks.iter()
            .map(|ack| (ack.conversation_id.as_str(), ack.last_delivered_seq))
            .collect()
    }

    /// 窗口内每个会话只留最大值，只有第一批安排定时；取走后下一批重新安排。
    #[test]
    fn a_window_keeps_the_highest_seq_per_conversation() {
        let acks = acks();
        assert!(acks.note(vec![("c2".into(), 4), ("c1".into(), 7)]));
        assert!(!acks.note(vec![("c1".into(), 5), ("c2".into(), 9)]));
        assert!(!acks.note(vec![("c3".into(), 1)]));
        assert_eq!(pairs(&acks.drain()), vec![("c1", 7), ("c2", 9), ("c3", 1)]);
        assert!(acks.drain().is_empty());
        assert!(acks.note(vec![("c1".into(), 8)]), "新的窗口重新定时");
    }

    /// 没有会话或没有 seq 的条目确认不了，也不为它们定时。
    #[test]
    fn entries_without_a_conversation_or_seq_are_skipped() {
        let acks = acks();
        assert!(!acks.note(vec![(String::new(), 3), ("c1".into(), 0)]));
        assert!(!acks.note(Vec::new()));
        assert!(acks.drain().is_empty());
    }

    /// 没有连接时冲刷不报错，攒着的确认随之丢弃（它们只对写出消息的那条连接有意义）。
    #[tokio::test]
    async fn flushing_without_a_connection_drops_the_window() {
        let acks = acks();
        assert!(acks.note(vec![("c1".into(), 3)]));
        acks.flush().await;
        assert!(acks.drain().is_empty());
    }
}
