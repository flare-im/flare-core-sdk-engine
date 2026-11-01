//! SDK extension installation.
//!
//! Business SDKs should integrate through this
//! boundary: register sync tasks, interceptors, capability plugins, profile
//! providers, or notification handlers without changing the IM core API.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::application::notification::NotificationHandler;
use crate::application::{
    ConversationLocalLifecycle, LocalConversationClearResult, LocalConversationVisibility,
};
use crate::domain::{ConversationIdentityService, UserProfile};
use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::SyncTask;
use crate::kernel::event::{ConversationEvent, EventBus, NameChange, SdkEvent};
use crate::model::{Conversation, IMMessage};
use crate::shared::error::Result;

pub mod capability;
pub mod encryption;

/// E2EE 的参考 codec（XChaCha20-Poly1305 + X25519）。
/// 需要 `e2ee` feature —— 不用 E2EE 的接入方不该背上密码学依赖。
#[cfg(feature = "e2ee")]
pub mod e2ee_codec;
pub mod middleware;

use capability::SdkCapabilityPlugin;
use middleware::{EventInterceptor, MessageInterceptor};

#[derive(Default)]
pub struct ExtensionRegistry {
    pub sync_tasks: Vec<Arc<dyn SyncTask>>,
    pub message_interceptors: Vec<Arc<dyn MessageInterceptor>>,
    pub event_interceptors: Vec<Arc<dyn EventInterceptor>>,
    pub capability_plugins: Vec<Arc<dyn SdkCapabilityPlugin>>,
    pub notification_handlers: Vec<Arc<dyn NotificationHandler>>,
    pub lifecycles: Vec<Arc<dyn ExtensionLifecycle>>,
    pub profile_providers: Vec<Arc<dyn ProfileProvider>>,
    pub conversation_projection_sources: Vec<Arc<dyn ConversationProjectionSource>>,
    pub content_codecs: Vec<Arc<dyn ContentCodec>>,
    pub migrations: Vec<Arc<dyn ExtensionMigration>>,
}

impl ExtensionRegistry {
    pub fn add_sync_task(&mut self, task: Arc<dyn SyncTask>) {
        self.sync_tasks.push(task);
    }

    pub fn add_message_interceptor(&mut self, interceptor: Arc<dyn MessageInterceptor>) {
        self.message_interceptors.push(interceptor);
    }

    pub fn add_event_interceptor(&mut self, interceptor: Arc<dyn EventInterceptor>) {
        self.event_interceptors.push(interceptor);
    }

    pub fn add_capability_plugin(&mut self, plugin: Arc<dyn SdkCapabilityPlugin>) {
        self.capability_plugins.push(plugin);
    }

    pub fn add_notification_handler(&mut self, handler: Arc<dyn NotificationHandler>) {
        self.notification_handlers.push(handler);
    }

    pub fn add_lifecycle(&mut self, lifecycle: Arc<dyn ExtensionLifecycle>) {
        self.lifecycles.push(lifecycle);
    }

    pub fn add_profile_provider(&mut self, provider: Arc<dyn ProfileProvider>) {
        self.profile_providers.push(provider);
    }

    pub fn add_conversation_projection_source(
        &mut self,
        source: Arc<dyn ConversationProjectionSource>,
    ) {
        self.conversation_projection_sources.push(source);
    }

    pub fn add_content_codec(&mut self, codec: Arc<dyn ContentCodec>) {
        self.content_codecs.push(codec);
    }

    pub fn add_migration(&mut self, migration: Arc<dyn ExtensionMigration>) {
        self.migrations.push(migration);
    }
}

pub trait SdkExtension: Send + Sync {
    fn namespace(&self) -> &str;
    fn install(&self, registry: &mut ExtensionRegistry) -> Result<()>;
}

pub fn generate_single_chat_conversation_id(current_user_id: &str, peer_user_id: &str) -> String {
    crate::domain::conversation::id::generate_single_chat_conversation_id(
        current_user_id,
        peer_user_id,
    )
}

pub fn generate_group_conversation_id(group_id: &str) -> String {
    crate::domain::conversation::id::generate_group_conversation_id(group_id)
}

pub fn repair_single_chat_channel(
    conversation: &mut Conversation,
    current_user_id: &str,
    peer_hint: Option<&str>,
) -> bool {
    ConversationIdentityService::repair_single_chat_channel(
        conversation,
        current_user_id,
        peer_hint,
    )
}

pub fn merge_or_create_conversation(
    existing: Option<Conversation>,
    conversation_id: String,
    current_user_id: &str,
    source_id: &str,
    conversation_type: &crate::model::conversation::ConversationType,
) -> (Conversation, bool) {
    let service = ConversationIdentityService;
    service.merge_or_create(
        existing,
        conversation_id,
        current_user_id,
        source_id,
        conversation_type,
    )
}

#[derive(Clone, Default)]
pub struct ExtensionRuntime {
    lifecycles: Vec<Arc<dyn ExtensionLifecycle>>,
    profile_providers: Vec<Arc<dyn ProfileProvider>>,
    conversation_projection_sources: Vec<Arc<dyn ConversationProjectionSource>>,
    content_codecs: Vec<Arc<dyn ContentCodec>>,
    migrations: Vec<Arc<dyn ExtensionMigration>>,
}

impl ExtensionRuntime {
    pub fn new(
        lifecycles: Vec<Arc<dyn ExtensionLifecycle>>,
        profile_providers: Vec<Arc<dyn ProfileProvider>>,
        conversation_projection_sources: Vec<Arc<dyn ConversationProjectionSource>>,
        content_codecs: Vec<Arc<dyn ContentCodec>>,
        migrations: Vec<Arc<dyn ExtensionMigration>>,
    ) -> Self {
        Self {
            lifecycles,
            profile_providers,
            conversation_projection_sources,
            content_codecs,
            migrations,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.lifecycles.is_empty()
            && self.profile_providers.is_empty()
            && self.conversation_projection_sources.is_empty()
            && self.content_codecs.is_empty()
            && self.migrations.is_empty()
    }

    pub fn lifecycle_count(&self) -> usize {
        self.lifecycles.len()
    }

    pub fn profile_provider_count(&self) -> usize {
        self.profile_providers.len()
    }

    pub fn conversation_projection_source_count(&self) -> usize {
        self.conversation_projection_sources.len()
    }

    pub fn content_codec_count(&self) -> usize {
        self.content_codecs.len()
    }

    pub fn migration_count(&self) -> usize {
        self.migrations.len()
    }

    pub fn lifecycles(&self) -> &[Arc<dyn ExtensionLifecycle>] {
        &self.lifecycles
    }

    pub fn profile_providers(&self) -> &[Arc<dyn ProfileProvider>] {
        &self.profile_providers
    }

    pub fn conversation_projection_sources(&self) -> &[Arc<dyn ConversationProjectionSource>] {
        &self.conversation_projection_sources
    }

    pub fn content_codecs(&self) -> &[Arc<dyn ContentCodec>] {
        &self.content_codecs
    }

    pub fn migrations(&self) -> &[Arc<dyn ExtensionMigration>] {
        &self.migrations
    }

    pub async fn notify_login(&self, context: &ExtensionLifecycleContext) -> Result<()> {
        for lifecycle in &self.lifecycles {
            lifecycle.on_login(context).await?;
        }
        Ok(())
    }

    pub async fn notify_connect(&self, context: &ExtensionLifecycleContext) -> Result<()> {
        for lifecycle in &self.lifecycles {
            lifecycle.on_connect(context).await?;
        }
        Ok(())
    }

    pub async fn notify_disconnect(&self, context: &ExtensionLifecycleContext) -> Result<()> {
        for lifecycle in &self.lifecycles {
            lifecycle.on_disconnect(context).await?;
        }
        Ok(())
    }

    pub async fn notify_logout(&self, context: &ExtensionLifecycleContext) -> Result<()> {
        for lifecycle in &self.lifecycles {
            lifecycle.on_logout(context).await?;
        }
        Ok(())
    }
}

/// Read/write view of core local state exposed to extensions.
///
/// This is intentionally narrower than `StoreProvider`: extensions can read
/// and project core model state, while core keeps repository ownership and
/// lifecycle invariants inside the SDK.
#[async_trait]
pub trait ExtensionStore: Send + Sync {
    async fn get_conversation(&self, conversation_id: &str) -> Result<Option<Conversation>>;

    async fn list_conversations(&self) -> Result<Vec<Conversation>>;

    async fn save_conversation(&self, conversation: &Conversation) -> Result<()>;

    async fn save_conversations(&self, conversations: &[Conversation]) -> Result<()>;

    async fn delete_conversation(&self, conversation_id: &str) -> Result<()>;

    async fn messages_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>>;

    async fn save_messages(&self, messages: &[IMMessage]) -> Result<()>;

    async fn clear_history_boundary(
        &self,
        current_user_id: &str,
        conversation_id: &str,
        visibility: LocalConversationVisibility,
    ) -> Result<Option<LocalConversationClearResult>>;

    async fn save_user_profiles(&self, profiles: &[UserProfile]) -> Result<()>;

    async fn apply_user_profile(&self, profile: &UserProfile) -> Result<Vec<String>>;

    /// The identity cache's profiles of [user_ids], by user id; a user it does not hold is left out.
    async fn get_user_profiles(&self, user_ids: &[String]) -> Result<HashMap<String, UserProfile>>;

    /// Stores what accounts are called inside conversations (group nicknames); an empty
    /// nickname clears one. Returns the conversations whose names may have changed, for
    /// the caller to publish as updated.
    async fn save_participant_display(
        &self,
        snapshots: &[crate::domain::ParticipantDisplaySnapshot],
    ) -> Result<Vec<String>> {
        let _ = snapshots;
        Ok(Vec::new())
    }
}

#[derive(Clone)]
pub struct ExtensionContext {
    store: Arc<dyn ExtensionStore>,
    bus: EventBus,
}

impl ExtensionContext {
    pub fn new(store: Arc<dyn ExtensionStore>, bus: EventBus) -> Self {
        Self { store, bus }
    }

    pub(crate) fn from_core(stores: StoreProvider, bus: EventBus) -> Self {
        Self::new(
            Arc::new(CoreExtensionStore {
                stores,
                bus: bus.clone(),
            }),
            bus,
        )
    }

    pub fn store(&self) -> Arc<dyn ExtensionStore> {
        self.store.clone()
    }

    pub fn bus(&self) -> EventBus {
        self.bus.clone()
    }

    pub fn publish_conversation_updated(&self, conversation_id: impl Into<String>) {
        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Updated {
                conversation_id: conversation_id.into(),
            }));
    }

    pub fn publish_conversation_deleted(&self, conversation_id: impl Into<String>) {
        self.bus
            .publish(SdkEvent::Conversation(ConversationEvent::Deleted {
                conversation_id: conversation_id.into(),
            }));
    }
}

#[derive(Clone)]
pub struct ExtensionLifecycleContext {
    extension_context: ExtensionContext,
    current_user_id: Option<String>,
}

impl ExtensionLifecycleContext {
    pub fn new(extension_context: ExtensionContext, current_user_id: Option<String>) -> Self {
        Self {
            extension_context,
            current_user_id,
        }
    }

    pub fn extension_context(&self) -> &ExtensionContext {
        &self.extension_context
    }

    pub fn store(&self) -> Arc<dyn ExtensionStore> {
        self.extension_context.store()
    }

    pub fn bus(&self) -> EventBus {
        self.extension_context.bus()
    }

    pub fn current_user_id(&self) -> Option<&str> {
        self.current_user_id.as_deref()
    }
}

#[async_trait]
pub trait ExtensionLifecycle: Send + Sync {
    fn namespace(&self) -> &str;

    async fn on_login(&self, _context: &ExtensionLifecycleContext) -> Result<()> {
        Ok(())
    }

    async fn on_connect(&self, _context: &ExtensionLifecycleContext) -> Result<()> {
        Ok(())
    }

    async fn on_disconnect(&self, _context: &ExtensionLifecycleContext) -> Result<()> {
        Ok(())
    }

    async fn on_logout(&self, _context: &ExtensionLifecycleContext) -> Result<()> {
        Ok(())
    }
}

struct CoreExtensionStore {
    stores: StoreProvider,
    /// 身份缓存与成员昵称写入后，告诉打开中的视图重读受影响的行。
    bus: EventBus,
}

#[async_trait]
impl ExtensionStore for CoreExtensionStore {
    async fn get_conversation(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        self.stores.conversations.get(conversation_id).await
    }

    async fn list_conversations(&self) -> Result<Vec<Conversation>> {
        self.stores.conversations.list().await
    }

    async fn save_conversation(&self, conversation: &Conversation) -> Result<()> {
        self.stores.conversations.save_one(conversation).await
    }

    async fn save_conversations(&self, conversations: &[Conversation]) -> Result<()> {
        self.stores.conversations.save_batch(conversations).await
    }

    async fn delete_conversation(&self, conversation_id: &str) -> Result<()> {
        self.stores.conversations.delete(conversation_id).await
    }

    async fn messages_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.stores
            .messages
            .get_by_conversation(conversation_id, before_seq, limit)
            .await
    }

    async fn save_messages(&self, messages: &[IMMessage]) -> Result<()> {
        self.stores.messages.save_batch(messages).await
    }

    async fn clear_history_boundary(
        &self,
        current_user_id: &str,
        conversation_id: &str,
        visibility: LocalConversationVisibility,
    ) -> Result<Option<LocalConversationClearResult>> {
        ConversationLocalLifecycle::clear_history_boundary(
            &self.stores,
            current_user_id,
            conversation_id,
            visibility,
        )
        .await
    }

    async fn save_user_profiles(&self, profiles: &[UserProfile]) -> Result<()> {
        self.stores.save_user_profiles(profiles).await?;
        self.bus.publish_name_change(NameChange::users(
            profiles.iter().map(|profile| profile.user_id.as_str()),
        ));
        Ok(())
    }

    async fn apply_user_profile(&self, profile: &UserProfile) -> Result<Vec<String>> {
        let affected = self.stores.apply_user_profile(profile).await?;
        self.bus
            .publish_name_change(NameChange::users([profile.user_id.as_str()]));
        Ok(affected)
    }

    async fn get_user_profiles(&self, user_ids: &[String]) -> Result<HashMap<String, UserProfile>> {
        self.stores
            .user_profiles_or_memory()
            .get_many(user_ids)
            .await
    }

    async fn save_participant_display(
        &self,
        snapshots: &[crate::domain::ParticipantDisplaySnapshot],
    ) -> Result<Vec<String>> {
        let changed = self.stores.apply_participant_display(snapshots).await?;
        self.bus
            .publish_name_change(NameChange::written_member_nicknames(snapshots, &changed));
        Ok(changed)
    }
}

#[async_trait]
pub trait ProfileProvider: Send + Sync {
    fn namespace(&self) -> &str;

    async fn get_profile(&self, user_id: &str) -> Result<Option<UserProfile>>;

    async fn get_profiles(&self, user_ids: &[String]) -> Result<Vec<UserProfile>> {
        let mut profiles = Vec::new();
        for user_id in user_ids {
            if let Some(profile) = self.get_profile(user_id).await? {
                profiles.push(profile);
            }
        }
        Ok(profiles)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionContent {
    pub content_type: String,
    pub payload: Vec<u8>,
    pub attributes: std::collections::HashMap<String, String>,
}

pub trait ContentCodec: Send + Sync {
    fn namespace(&self) -> &str;
    fn content_type(&self) -> &str;
    fn encode(&self, content: &ExtensionContent) -> Result<Vec<u8>>;
    fn decode(&self, payload: &[u8]) -> Result<ExtensionContent>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConversationProjectionReport {
    pub upserted: usize,
    pub deleted: usize,
}

#[async_trait]
pub trait ConversationProjectionSource: Send + Sync {
    fn namespace(&self) -> &str;

    async fn project(&self, context: &ExtensionContext) -> Result<ConversationProjectionReport>;
}

#[async_trait]
pub trait ExtensionMigration: Send + Sync {
    fn namespace(&self) -> &str;
    fn version(&self) -> u32;

    async fn migrate(&self, context: &ExtensionContext) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::infrastructure::persistence::in_memory_empty_im_provider;

    struct CountingLifecycle {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ExtensionLifecycle for CountingLifecycle {
        fn namespace(&self) -> &str {
            "test.lifecycle"
        }

        async fn on_login(&self, context: &ExtensionLifecycleContext) -> Result<()> {
            assert_eq!(context.current_user_id(), Some("u1"));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_connect(&self, context: &ExtensionLifecycleContext) -> Result<()> {
            assert_eq!(context.current_user_id(), Some("u1"));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_disconnect(&self, context: &ExtensionLifecycleContext) -> Result<()> {
            assert_eq!(context.current_user_id(), Some("u1"));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_logout(&self, context: &ExtensionLifecycleContext) -> Result<()> {
            assert_eq!(context.current_user_id(), Some("u1"));
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn extension_runtime_dispatches_lifecycle_hooks() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = ExtensionRuntime::new(
            vec![Arc::new(CountingLifecycle {
                calls: calls.clone(),
            })],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let context = ExtensionLifecycleContext::new(
            ExtensionContext::from_core(in_memory_empty_im_provider(), EventBus::new()),
            Some("u1".to_string()),
        );

        runtime.notify_login(&context).await.unwrap();
        runtime.notify_connect(&context).await.unwrap();
        runtime.notify_disconnect(&context).await.unwrap();
        runtime.notify_logout(&context).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }
}
