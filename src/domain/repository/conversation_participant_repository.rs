use async_trait::async_trait;

use crate::model::ConversationParticipant;
use crate::shared::error::Result;

#[async_trait]
pub trait ConversationParticipantStore: Send + Sync {
    async fn save_page(
        &self,
        conversation_id: &str,
        participants: &[ConversationParticipant],
        participant_version: u64,
        replace_all: bool,
    ) -> Result<()>;

    async fn list(
        &self,
        conversation_id: &str,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<ConversationParticipant>>;

    async fn version(&self, conversation_id: &str) -> Result<u64>;

    /// 用户资料变更后，批量刷新本地参与者快照中的头像。成员昵称是这个人在会话里的名字
    /// （群昵称），不随资料名变化，这里不碰。
    async fn patch_user_avatar(&self, user_id: &str, avatar_url: &str) -> Result<()> {
        let _ = (user_id, avatar_url);
        Ok(())
    }
}
