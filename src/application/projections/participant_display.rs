//! Member nickname projection: business SDKs tell the core what an account is called
//! inside a conversation (a group nickname); the core keeps it apart from member sync
//! and uses it when it projects names.

use crate::domain::ParticipantDisplaySnapshot;
use crate::infrastructure::persistence::StoreProvider;
use crate::shared::error::Result;

pub struct ParticipantDisplayProjectionApplier;

impl ParticipantDisplayProjectionApplier {
    /// Stores the nicknames (an empty one clears it) and returns the conversations
    /// whose stored nicknames changed, so their titles and sender names are re-read.
    pub async fn apply_batch(
        stores: &StoreProvider,
        snapshots: &[ParticipantDisplaySnapshot],
    ) -> Result<Vec<String>> {
        if snapshots.is_empty() {
            return Ok(Vec::new());
        }
        stores.conversations.save_member_nicknames(snapshots).await
    }
}
