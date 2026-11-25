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
            .await
    }
}
