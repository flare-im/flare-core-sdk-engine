use async_trait::async_trait;

use crate::model::message::{IMMessage, SendAck};
use crate::shared::error::Result;

/// Kernel port for reliable outbound delivery.
///
/// Application use cases depend on this port; the runtime owns the concrete
/// actor implementation.
#[async_trait]
pub trait ReliableSendQueuePort: Send + Sync {
    async fn enqueue(&self, message: IMMessage) -> Result<()>;

    async fn on_ack(&self, ack: SendAck) -> Result<()>;

    async fn reset_pending_on_login(&self) -> Result<Vec<String>>;
}
