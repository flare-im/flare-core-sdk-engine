use crate::domain::{
    ConversationParticipantStore, ConversationStore, MediaCacheAdmin, MediaCacheStore,
    MessageStore, PendingSendReader, PendingSendWriter, SyncCursorStore, TimelineCoverageStore,
    UploadManifestStore, UserFileDownloadStore, UserProfile, UserReader, UserWriter,
};
use std::sync::Arc;

/// 存储提供者 — 统一持有各 Store；待发/用户资料使用 domain Reader/Writer 分开持有
pub struct StoreProvider {
    pub plugin_commands: Option<Arc<dyn crate::domain::PluginCommandStore>>,
    /// 插件能力同意记录（用户库）；没有时进程内保存，重启后重新询问。
    pub plugin_consents: Option<Arc<dyn crate::domain::PluginConsentStore>>,
    /// 插件 Web 包经桥存的数据（用户库独立表）；没有时进程内保存。
    pub plugin_bundle_storage: Option<Arc<dyn crate::domain::PluginBundleStorage>>,
    pub messages: Arc<dyn MessageStore>,
    pub conversations: Arc<dyn ConversationStore>,
    pub conversation_participants: Option<Arc<dyn ConversationParticipantStore>>,
    pub cursors: Arc<dyn SyncCursorStore>,
    /// 时间线已证实连续区间：有没有更早的消息、往旧翻页何时回填，只由它回答。
    pub timeline_coverage: Arc<dyn TimelineCoverageStore>,
    /// Account-scoped atomic commits for a provider with a reliable outbox.
    pub outgoing: Option<Arc<dyn crate::domain::OutgoingSendStore>>,
    pub pending_send_reader: Option<Arc<dyn PendingSendReader>>,
    pub pending_send_writer: Option<Arc<dyn PendingSendWriter>>,
    pub upload_manifest_store: Option<Arc<dyn UploadManifestStore>>,
    pub media_cache_store: Option<Arc<dyn MediaCacheStore>>,
    pub media_cache_admin: Option<Arc<dyn MediaCacheAdmin>>,
    pub user_file_download_store: Option<Arc<dyn UserFileDownloadStore>>,
    pub user_profiles_reader: Option<Arc<dyn UserReader>>,
    pub user_profiles_writer: Option<Arc<dyn UserWriter>>,
    /// Record backend for partitions extensions declare, in the account's own storage.
    pub extension_records: Option<Arc<dyn crate::extension::records::RecordBackend>>,
}

impl StoreProvider {
    pub fn require_reliable_outbox(&self) -> crate::Result<()> {
        if self.pending_sends().is_none() || self.outgoing.is_none() {
            return Err(crate::FlareError::localized(
                crate::ErrorCode::ConfigurationError,
                "account backend requires an atomic outgoing store and both outbox ports",
            ));
        }
        Ok(())
    }

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
            plugin_consents: self.plugin_consents.clone(),
            plugin_bundle_storage: self.plugin_bundle_storage.clone(),
            messages: self.messages.clone(),
            conversations: self.conversations.clone(),
            conversation_participants: self.conversation_participants.clone(),
            cursors: self.cursors.clone(),
            timeline_coverage: self.timeline_coverage.clone(),
            outgoing: self.outgoing.clone(),
            pending_send_reader: self.pending_send_reader.clone(),
            pending_send_writer: self.pending_send_writer.clone(),
            upload_manifest_store: self.upload_manifest_store.clone(),
            media_cache_store: self.media_cache_store.clone(),
            media_cache_admin: self.media_cache_admin.clone(),
            user_file_download_store: self.user_file_download_store.clone(),
            user_profiles_reader: self.user_profiles_reader.clone(),
            user_profiles_writer: self.user_profiles_writer.clone(),
            extension_records: self.extension_records.clone(),
        }
    }
}
