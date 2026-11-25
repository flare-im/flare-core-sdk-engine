use std::sync::Arc;

use crate::Result;
use crate::application::InboundSource;
use crate::application::notification::partition_notification_durability;
use crate::application::projections::ConversationProjectionApplier;
use crate::application::services::IncomingMessageConverger;
use crate::domain::MessageStore;
use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::{ReliableSendQueuePort, event::EventBus};
use crate::model::IMMessage;

pub(crate) struct InboundBatch {
    pub(crate) durable: Vec<IMMessage>,
    ephemeral: Vec<IMMessage>,
}

impl InboundBatch {
    pub(crate) fn from_messages(messages: Vec<IMMessage>) -> Self {
        let (durable, ephemeral) = partition_notification_durability(messages);
        Self { durable, ephemeral }
    }

    pub(crate) fn into_notifications(mut self) -> Vec<IMMessage> {
        self.durable.append(&mut self.ephemeral);
        self.durable
    }
}

/// Both socket delivery and replay stage the canonical rows and latest previews, then
/// commit unread changes with their projection receipt. Cursor evidence follows this call.
pub(crate) struct ApplyInbound {
    bus: EventBus,
    messages: Arc<dyn MessageStore>,
    converger: IncomingMessageConverger,
    projection: ConversationProjectionApplier,
}

impl ApplyInbound {
    pub(crate) fn new(
        stores: &StoreProvider,
        bus: &EventBus,
        queue: Option<Arc<dyn ReliableSendQueuePort>>,
    ) -> Self {
        Self {
            bus: bus.clone(),
            messages: stores.messages.clone(),
            converger: IncomingMessageConverger::new(
                stores.messages.clone(),
                stores.conversations.clone(),
                bus.clone(),
                queue,
            ),
            projection: ConversationProjectionApplier::new(stores.clone(), bus.clone()),
        }
    }

    pub(crate) fn set_reliable_queue(&self, queue: Option<Arc<dyn ReliableSendQueuePort>>) {
        self.converger.set_reliable_queue(queue);
    }

    // Live views may be delivered before the bounded persistence worker runs.
    pub(crate) async fn prepare(
        &self,
        user_id: &str,
        messages: Vec<IMMessage>,
        source: InboundSource,
    ) -> Result<InboundBatch> {
        let rows = self
            .converger
            .converge_messages(user_id, messages, source)
            .await?;
        Ok(InboundBatch::from_messages(rows))
    }

    pub(crate) async fn persist(
        &self,
        messages: &[IMMessage],
        user_id: &str,
        mode: InboundSource,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        self.messages.save_batch(messages).await?;
        self.projection
            .apply_messages_with_mode(messages, user_id, mode)
            .await?;
        self.bus.notify_committed(messages);
        Ok(())
    }
}

/// Core-only receive: no profile provider, no Social, no plugins. Single chat, group,
/// quote and forward land through the one inbound path with the same unread rules.
#[cfg(test)]
mod core_only_tests {
    use super::*;
    use crate::extension::{generate_group_conversation_id, generate_single_chat_conversation_id};
    use crate::infrastructure::persistence::in_memory_im_provider;
    use crate::model::MessageKey;
    use flare_proto::common::message_content::Content;
    use flare_proto::common::{
        ConversationType as ProtoConversationType, ForwardContent, ForwardItem,
        Message as ProtoMessage, MessageContent, QuoteContent, TextContent,
    };

    const ME: &str = "me";

    fn inbound(
        cid: &str,
        kind: ProtoConversationType,
        sender: &str,
        seq: u64,
        content: Content,
    ) -> IMMessage {
        IMMessage::new(ProtoMessage {
            conversation_id: cid.into(),
            conversation_type: kind as i32,
            server_id: format!("{cid}-s{seq}"),
            client_msg_id: format!("{sender}-c{seq}"),
            sender_id: sender.into(),
            conversation_seq: seq,
            content: Some(MessageContent {
                content: Some(content),
            }),
            ..Default::default()
        })
    }

    fn text(value: &str) -> Content {
        Content::Text(TextContent {
            text: value.into(),
            ..Default::default()
        })
    }

    async fn apply(stores: &StoreProvider, messages: Vec<IMMessage>) {
        let inbound = ApplyInbound::new(stores, &EventBus::new(), None);
        let batch = inbound
            .prepare(ME, messages, InboundSource::RealtimePush)
            .await
            .unwrap();
        inbound
            .persist(&batch.durable, ME, InboundSource::RealtimePush)
            .await
            .unwrap();
    }

    async fn stored(stores: &StoreProvider, cid: &str, sender: &str, seq: u64) -> IMMessage {
        let key = MessageKey {
            conversation_id: cid.into(),
            sender_id: sender.into(),
            client_msg_id: format!("{sender}-c{seq}"),
        };
        stores
            .messages
            .get_by_message_key(&key)
            .await
            .unwrap()
            .expect("stored row")
    }

    #[tokio::test]
    async fn single_chat_text_and_quote_without_profile_provider() {
        let stores = in_memory_im_provider();
        let cid = generate_single_chat_conversation_id(ME, "peer");
        let single = ProtoConversationType::Single;
        let quote = Content::Quote(Box::new(QuoteContent {
            quoted_message_id: format!("{cid}-s1"),
            quoted_sender_id: "peer".into(),
            quoted_text_preview: "hello".into(),
            current_content: Some(Box::new(MessageContent {
                content: Some(text("reply")),
            })),
            ..Default::default()
        }));
        apply(
            &stores,
            vec![
                inbound(&cid, single, "peer", 1, text("hello")),
                inbound(&cid, single, "peer", 2, quote),
            ],
        )
        .await;

        let reply = stored(&stores, &cid, "peer", 2).await;
        assert_eq!(
            reply.reply_to.as_deref(),
            Some(format!("{cid}-s1").as_str())
        );
        let conversation = stores
            .conversations
            .get(&cid)
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(conversation.unread_count, 2);
    }

    #[tokio::test]
    async fn group_text_and_forward_count_unread_once_per_message() {
        let stores = in_memory_im_provider();
        let cid = generate_group_conversation_id("g1");
        let group = ProtoConversationType::Group;
        let forward = Content::Forward(ForwardContent {
            items: vec![ForwardItem {
                content: Some(MessageContent {
                    content: Some(text("forwarded")),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        let batch = vec![
            inbound(&cid, group, "a", 1, text("hi")),
            inbound(&cid, group, "b", 2, forward),
        ];
        apply(&stores, batch.clone()).await;
        // A replay of the same batch must not count again.
        apply(&stores, batch).await;

        let forwarded = stored(&stores, &cid, "b", 2).await;
        assert!(matches!(
            forwarded.content,
            Some(crate::content::message_elem::Elem::Forward(_))
        ));
        let conversation = stores
            .conversations
            .get(&cid)
            .await
            .unwrap()
            .expect("conversation");
        assert_eq!(conversation.unread_count, 2);
    }
}
