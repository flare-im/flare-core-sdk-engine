//! 送达确认：从链路实时到达的消息处理完之后，告诉服务端「这些由我提示」。
//!
//! 服务端对每条需要提示的下行帧都等一个确认，等不到就转给推送通道。确认按会话回，值是该会话处理到
//! 的最大 seq。只有实时到达的消息在这里确认：同步拉回来的不是服务端在等的那些帧。
//!
//! 该不该确认在帧到达、开始处理的那一刻判定（谁确认谁负责提示，见 [`DeviceReach`]）；确认本身等到
//! 消息已交给持久化、提示决策已做出之后才发。

use std::collections::HashMap;
use std::sync::Arc;

use flare_proto::common::Event;
use flare_proto::common::event::Payload as EventPayload;

use super::Dispatcher;
use crate::infrastructure::protocol::DeliveryAcks;
use crate::runtime::link::DeviceReach;

/// 分发器的送达确认出口。
pub(super) struct DeliveryConfirmation {
    reach: Arc<DeviceReach>,
    acks: Arc<DeliveryAcks>,
}

impl DeliveryConfirmation {
    pub fn new(reach: Arc<DeviceReach>, acks: Arc<DeliveryAcks>) -> Self {
        Self { reach, acks }
    }
}

/// 各会话的最大 seq。没有会话或没有 seq 的条目确认不了，略过。
pub(super) fn highest_seqs<'a>(
    arrived: impl IntoIterator<Item = (&'a str, u64)>,
) -> Vec<(String, u64)> {
    let mut highest: HashMap<&str, u64> = HashMap::new();
    for (conversation_id, seq) in arrived {
        if conversation_id.is_empty() || seq == 0 {
            continue;
        }
        let known = highest.entry(conversation_id).or_default();
        *known = (*known).max(seq);
    }
    let mut delivered: Vec<(String, u64)> = highest
        .into_iter()
        .map(|(conversation_id, seq)| (conversation_id.to_owned(), seq))
        .collect();
    delivered.sort();
    delivered
}

/// 消息事件里那条消息的会话与 seq。别的事件（已读、撤回、编辑）服务端不等确认。
pub(super) fn arrived_message(event: &Event) -> Option<(&str, u64)> {
    match &event.payload {
        Some(EventPayload::Message(message)) => {
            Some((message.conversation_id.as_str(), message.conversation_seq))
        }
        _ => None,
    }
}

impl Dispatcher {
    /// 一帧到达时定下它的送达确认：该由本进程确认时是帧里各会话的最大 seq，否则为空。
    ///
    /// 必须在处理这一帧的任何等待之前调用。判定属于到达那一刻：托管应答之前到的帧，落库晚于应答
    /// 也仍然确认；应答之后才到的不确认，由推送提示。
    pub(super) fn delivery_due<'a>(
        &self,
        arrived: impl IntoIterator<Item = (&'a str, u64)>,
    ) -> Vec<(String, u64)> {
        match &self.delivery {
            Some(delivery) if delivery.reach.confirms_deliveries() => highest_seqs(arrived),
            _ => Vec::new(),
        }
    }

    /// 这一帧的消息已交给持久化、提示决策已做出（或它们是早已处理过的重复帧）：把到达时定下的确认
    /// 交出去。处理失败的帧不走到这里，服务端等不到确认会转给推送。
    pub(super) fn confirm_delivered(&self, due: Vec<(String, u64)>) {
        if let Some(delivery) = &self.delivery
            && !due.is_empty()
        {
            // 确认了就由本进程提示：记进提示台账，确认在路上丢了、推送照样到达时不再响第二次。
            if let Some(alerts) = &self.alerts {
                alerts.note_confirmed(&due);
            }
            delivery.acks.confirm(due);
        }
    }
}

#[cfg(test)]
mod tests {
    use flare_proto::common::{DeviceAppState, Message, MessagePush};
    use tokio::sync::RwLock;

    use super::*;
    use crate::application::notification::{
        NotificationHandlerRegistry, NotificationInboundPipeline,
    };
    use crate::application::services::{EventDeduper, MessageDeduper};
    use crate::infrastructure::protocol::{DownlinkPayload, PacketSender, ProtobufCodec};
    use crate::kernel::event::EventBus;
    use crate::runtime::link::ReachProfile;
    use crate::spi::metrics::MetricsRecorder;

    /// 一个装了送达确认的分发器（没有连接：确认只攒着，发不出去）。
    fn wired(platform: &str) -> (Dispatcher, Arc<DeviceReach>, Arc<DeliveryAcks>) {
        let bus = EventBus::new();
        let reach = Arc::new(DeviceReach::new(ReachProfile::for_platform(platform)));
        let acks = Arc::new(DeliveryAcks::new(Arc::new(PacketSender::new(
            Arc::new(tokio::sync::Mutex::new(None)),
            Arc::new(ProtobufCodec),
        ))));
        let dispatcher = Dispatcher::new(
            bus.clone(),
            None,
            None,
            None,
            None,
            Arc::new(RwLock::new("me".to_string())),
            EventDeduper::new(Some(64)),
            NotificationInboundPipeline::new(
                Arc::new(NotificationHandlerRegistry::new()),
                MessageDeduper::new(Some(64)),
                bus,
            ),
            MetricsRecorder::disabled(),
        )
        .with_delivery_confirmation(reach.clone(), acks.clone());
        (dispatcher, reach, acks)
    }

    fn push(arrived: &[(&str, u64)]) -> DownlinkPayload {
        let messages = arrived
            .iter()
            .map(|(conversation, seq)| Message {
                server_id: format!("{conversation}-{seq}"),
                conversation_id: (*conversation).to_owned(),
                conversation_seq: *seq,
                sender_id: "peer".into(),
                ..Default::default()
            })
            .collect();
        DownlinkPayload::MessagePush(MessagePush {
            messages,
            notifications: Vec::new(),
        })
    }

    /// 实时到达的一批消息处理完后，各会话的最大 seq 进入送达确认。
    #[tokio::test]
    async fn a_live_push_is_confirmed_per_conversation() {
        let (dispatcher, _reach, acks) = wired("android");
        dispatcher
            .dispatch(push(&[("c1", 5), ("c1", 6), ("c2", 2)]))
            .await
            .unwrap();
        let confirmed = acks.pending_confirmations();
        assert_eq!(confirmed, vec![("c1".to_owned(), 6), ("c2".to_owned(), 2)]);
    }

    /// 谁确认谁负责提示：手机进了后台、没有后台展示方时，到达的消息照常处理但不确认；接上展示方
    /// 之后才确认。
    #[tokio::test]
    async fn a_backgrounded_phone_does_not_confirm_without_a_presenter() {
        let (dispatcher, reach, acks) = wired("ios");
        reach.set_foreground(false);
        dispatcher.dispatch(push(&[("c1", 7)])).await.unwrap();
        assert!(acks.pending_confirmations().is_empty());

        reach.set_background_presenter(true);
        dispatcher.dispatch(push(&[("c1", 8)])).await.unwrap();
        assert_eq!(acks.pending_confirmations(), vec![("c1".to_owned(), 8)]);
    }

    /// 确认了的消息记进提示台账（按会话的合并键）：同一条的推送再到达时不再响。没确认的（后台没有
    /// 展示方）不记，推送照常提示。
    #[tokio::test]
    async fn confirmed_deliveries_are_recorded_as_alerts() {
        use crate::application::notification::{AlertLedger, AlertSkip, conversation_collapse_id};

        let (dispatcher, reach, _acks) = wired("android");
        let ledger = Arc::new(AlertLedger::new());
        ledger.scope("0");
        let dispatcher = dispatcher.with_alert_ledger(ledger.clone());
        dispatcher.dispatch(push(&[("c1", 5)])).await.unwrap();
        assert_eq!(
            ledger.claim_alert(&conversation_collapse_id("0", "c1"), 5, false),
            Err(AlertSkip::AlreadyAlerted)
        );

        reach.set_foreground(false);
        dispatcher.dispatch(push(&[("c2", 3)])).await.unwrap();
        assert_eq!(
            ledger.claim_alert(&conversation_collapse_id("0", "c2"), 3, false),
            Ok(()),
            "没确认的由推送提示"
        );
    }

    /// 托管应答之后才到的帧不确认，哪怕在前台时到达的帧会被确认。
    #[tokio::test]
    async fn nothing_is_confirmed_after_the_parking_ack() {
        let (dispatcher, reach, acks) = wired("ios");
        reach.begin_parking();
        reach.on_state_ack(DeviceAppState::Parking);
        dispatcher.dispatch(push(&[("c1", 9)])).await.unwrap();
        assert!(acks.pending_confirmations().is_empty());
    }

    #[test]
    fn the_highest_seq_per_conversation_is_confirmed() {
        let delivered = highest_seqs([("c1", 9), ("c2", 4), ("c1", 7), ("", 11), ("c3", 0)]);
        assert_eq!(delivered, vec![("c1".to_owned(), 9), ("c2".to_owned(), 4)]);
        assert!(highest_seqs(std::iter::empty()).is_empty());
    }

    #[test]
    fn only_message_events_name_a_delivery() {
        let message = Event {
            payload: Some(EventPayload::Message(flare_proto::common::Message {
                conversation_id: "c1".into(),
                conversation_seq: 12,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(arrived_message(&message), Some(("c1", 12)));

        let read = Event {
            conversation_id: "c1".into(),
            conversation_seq: 13,
            payload: Some(EventPayload::Read(Default::default())),
            ..Default::default()
        };
        assert_eq!(arrived_message(&read), None);
        assert_eq!(arrived_message(&Event::default()), None);
    }
}
