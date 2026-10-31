use crate::domain::{
    ConversationParticipantStore, ConversationStore, MediaCacheAdmin, MediaCacheStore,
    MessageStore, PendingSendReader, PendingSendWriter, SyncCursorStore, UploadManifestStore,
    UserFileDownloadStore, UserProfile, UserReader, UserWriter,
};
use std::sync::Arc;

/// 存储提供者 — 统一持有各 Store；待发/用户资料使用 domain Reader/Writer 分开持有
pub struct StoreProvider {
    pub plugin_commands: Option<Arc<dyn crate::domain::PluginCommandStore>>,
    pub messages: Arc<dyn MessageStore>,
    pub conversations: Arc<dyn ConversationStore>,
    pub conversation_participants: Option<Arc<dyn ConversationParticipantStore>>,
    pub cursors: Arc<dyn SyncCursorStore>,
    pub pending_send_reader: Option<Arc<dyn PendingSendReader>>,
    pub pending_send_writer: Option<Arc<dyn PendingSendWriter>>,
    pub upload_manifest_store: Option<Arc<dyn UploadManifestStore>>,
    pub media_cache_store: Option<Arc<dyn MediaCacheStore>>,
    pub media_cache_admin: Option<Arc<dyn MediaCacheAdmin>>,
    pub user_file_download_store: Option<Arc<dyn UserFileDownloadStore>>,
    pub user_profiles_reader: Option<Arc<dyn UserReader>>,
    pub user_profiles_writer: Option<Arc<dyn UserWriter>>,
}

impl StoreProvider {
    /// 返回已配置的待发读端，用于可靠队列（与 pending_send_writer 配对使用）
    pub fn pending_sends(
        &self,
    ) -> Option<(Arc<dyn PendingSendReader>, Arc<dyn PendingSendWriter>)> {
        match (&self.pending_send_reader, &self.pending_send_writer) {
            (Some(r), Some(w)) => Some((r.clone(), w.clone())),
            _ => None,
        }
    }

    /// 返回已配置的用户资料读端，或默认内存实现（保证不为空）
    pub fn user_profiles_or_memory(&self) -> Arc<dyn UserReader> {
        self.user_profiles_reader.clone().unwrap_or_else(|| {
            Arc::new(crate::infrastructure::persistence::MemoryUserProfileStore::new())
        })
    }

    pub fn user_profiles_writer_or_memory(&self) -> Arc<dyn UserWriter> {
        self.user_profiles_writer.clone().unwrap_or_else(|| {
            Arc::new(crate::infrastructure::persistence::MemoryUserProfileStore::new())
        })
    }

    pub async fn save_user_profiles(&self, profiles: &[UserProfile]) -> crate::Result<()> {
        self.user_profiles_writer_or_memory()
            .save_batch(profiles)
            .await
    }

    /// 写入会话内成员昵称（社交业务里的群昵称）；返回名字可能随之变化的会话。
    pub async fn apply_participant_display(
        &self,
        snapshots: &[crate::domain::ParticipantDisplaySnapshot],
    ) -> crate::Result<Vec<String>> {
        crate::application::ParticipantDisplayProjectionApplier::apply_batch(self, snapshots).await
    }

    pub async fn apply_user_profile(&self, profile: &UserProfile) -> crate::Result<Vec<String>> {
        self.save_user_profiles(std::slice::from_ref(profile))
            .await?;
        self.apply_user_profile_local_views(profile).await
    }

    pub async fn apply_user_profile_local_views(
        &self,
        profile: &UserProfile,
    ) -> crate::Result<Vec<String>> {
        if let Some(participants) = &self.conversation_participants {
            participants
                .patch_user_avatar(&profile.user_id, &profile.avatar_url)
                .await?;
        }

        self.conversations
            .apply_user_profile_snapshot(&profile.user_id, &profile.nickname, &profile.avatar_url)
            .await
    }
}

impl Clone for StoreProvider {
    fn clone(&self) -> Self {
        Self {
            plugin_commands: self.plugin_commands.clone(),
            messages: self.messages.clone(),
            conversations: self.conversations.clone(),
            conversation_participants: self.conversation_participants.clone(),
            cursors: self.cursors.clone(),
            pending_send_reader: self.pending_send_reader.clone(),
            pending_send_writer: self.pending_send_writer.clone(),
            upload_manifest_store: self.upload_manifest_store.clone(),
            media_cache_store: self.media_cache_store.clone(),
            media_cache_admin: self.media_cache_admin.clone(),
            user_file_download_store: self.user_file_download_store.clone(),
            user_profiles_reader: self.user_profiles_reader.clone(),
            user_profiles_writer: self.user_profiles_writer.clone(),
        }
    }
}
