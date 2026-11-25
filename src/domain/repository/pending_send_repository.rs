use crate::domain::PendingSendVo;
use crate::shared::error::Result;
use async_trait::async_trait;

/// 待发送队列查询（只读）
#[async_trait]
pub trait PendingSendReader: Send + Sync {
    async fn get(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>>;
    async fn list(&self) -> Result<Vec<PendingSendVo>>;

    /// 按入队时间取最老的候选项，同时排除已在途 client_msg_id。
    ///
    /// 可靠队列使用它做有界流水线填充，避免每次都全量拉取/解码 pending 表。
    async fn list_oldest_excluding(
        &self,
        excluded_client_msg_ids: &[String],
        limit: usize,
    ) -> Result<Vec<PendingSendVo>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let excluded = excluded_client_msg_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let mut list = self.list().await?;
        list.retain(|entry| !excluded.contains(entry.client_msg_id.as_str()));
        list.sort_by(|a, b| {
            a.enqueued_at_ms
                .cmp(&b.enqueued_at_ms)
                .then_with(|| a.client_msg_id.cmp(&b.client_msg_id))
        });
        list.truncate(limit);
        Ok(list)
    }

    /// 取队列中 enqueued_at_ms 最小的一条（用于可靠队列按序发送，避免全表扫描）
    async fn take_oldest(&self) -> Result<Option<PendingSendVo>> {
        Ok(self.list_oldest_excluding(&[], 1).await?.into_iter().next())
    }
}

/// 待发送队列写操作
#[async_trait]
pub trait PendingSendWriter: Send + Sync {
    async fn push(&self, entry: PendingSendVo) -> Result<()>;
    async fn pop(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>>;
}

impl PendingSendVo {
    pub fn validate_message(&self, message: &crate::model::IMMessage) -> Result<()> {
        if self.client_msg_id.trim().is_empty()
            || self.conversation_id.trim().is_empty()
            || self.client_msg_id != message.client_msg_id
            || self.client_msg_id != self.message.client_msg_id
            || self.conversation_id != message.conversation_id
            || self.conversation_id != self.message.conversation_id
            || self.message.sender_id != message.sender_id
        {
            return Err(crate::shared::error::FlareError::localized(
                crate::shared::error::ErrorCode::InvalidParameter,
                "outbox and message identity differ",
            ));
        }
        Ok(())
    }
}

/// One account's message, conversation summary and outbox share a single commit.
/// A returned error leaves all three in their previous state.
#[async_trait]
pub trait OutgoingSendStore: Send + Sync {
    /// Whether a successful commit survives closing and reopening this account's storage.
    fn survives_restart(&self) -> bool;

    async fn enqueue_send(
        &self,
        message: &crate::model::IMMessage,
        entry: &PendingSendVo,
    ) -> Result<()>;
    /// Complete a successful durable ACK, or store a terminal local failure.
    /// Returns false when an already confirmed row supersedes this outcome.
    async fn complete_send(&self, message: &crate::model::IMMessage) -> Result<bool>;
}
