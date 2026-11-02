use std::sync::Arc;

use crate::application::projections::names::{NameBook, NameRequest};
use crate::domain::{ConversationStore, UserReader, real_name};
use crate::kernel::CurrentUserIdStore;
use crate::model::{Conversation, ConversationListQuery};
use crate::shared::error::Result;
use tracing::warn;

pub struct ConversationViewAssembler {
    store: Arc<dyn ConversationStore>,
    profile_reader: Arc<dyn UserReader>,
    current_user_id: Option<CurrentUserIdStore>,
}

impl ConversationViewAssembler {
    pub fn new(store: Arc<dyn ConversationStore>, profile_reader: Arc<dyn UserReader>) -> Self {
        Self {
            store,
            profile_reader,
            current_user_id: None,
        }
    }

    /// The signed-in user, so unnamed groups are titled by the other members and a
    /// single chat finds its peer in the member preview.
    pub fn with_current_user(mut self, current_user_id: CurrentUserIdStore) -> Self {
        self.current_user_id = Some(current_user_id);
        self
    }

    async fn current_user_id(&self) -> String {
        match &self.current_user_id {
            Some(store) => store.read().await.trim().to_owned(),
            None => String::new(),
        }
    }

    pub async fn hydrate_conversation(&self, conversation: Conversation) -> Conversation {
        self.hydrate_conversations(vec![conversation])
            .await
            .pop()
            .unwrap_or_default()
    }

    /// Projects names onto `conversations`: the profiles of every peer, last sender and
    /// previewed member are read with one `get_many`, the member nicknames with one
    /// `member_nicknames`, whatever the number of conversations.
    pub async fn hydrate_conversations(
        &self,
        mut conversations: Vec<Conversation>,
    ) -> Vec<Conversation> {
        for conversation in conversations.iter_mut() {
            if conversation.normalize_channel_id_for_wire()
                && let Err(error) = self.store.save_one(conversation).await
            {
                warn!(
                    conversation_id = %conversation.conversation_id,
                    error = %error,
                    "failed to persist repaired conversation channel_id"
                );
            }
        }
        let me = self.current_user_id().await;
        let mut request = NameRequest::default();
        for conversation in &conversations {
            request.conversation(conversation, &me);
        }
        let book = request
            .load(self.profile_reader.as_ref(), Some(self.store.as_ref()))
            .await;
        for conversation in conversations.iter_mut() {
            book.project_conversation(conversation, &me);
            project_legacy_last_sender(conversation, &book);
        }
        conversations
    }

    pub async fn list(&self, include_archived: bool) -> Result<Vec<Conversation>> {
        self.list_by_query(&ConversationListQuery {
            include_archived,
            ..ConversationListQuery::default()
        })
        .await
    }

    pub async fn list_by_query(&self, query: &ConversationListQuery) -> Result<Vec<Conversation>> {
        let mut list = self.store.list_by_query(query).await?;
        if let Some(cursor) = query
            .cursor
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let skip = list
                .iter()
                .position(|conversation| conversation.conversation_id == cursor)
                .map(|index| index + 1)
                .unwrap_or(0);
            list = list.into_iter().skip(skip).collect();
        }
        if let Some(limit) = query.normalized_limit() {
            list.truncate(limit as usize);
        }
        Ok(self.hydrate_conversations(list).await)
    }

    pub async fn get(&self, conversation_id: &str) -> Result<Option<Conversation>> {
        let conversation = self.store.get(conversation_id).await?;
        let Some(conversation) = conversation else {
            return Ok(None);
        };
        Ok(Some(self.hydrate_conversation(conversation).await))
    }

    pub async fn get_multiple(&self, conversation_ids: &[String]) -> Result<Vec<Conversation>> {
        let mut out = Vec::with_capacity(conversation_ids.len());
        for conversation_id in conversation_ids {
            if let Some(conversation) = self.store.get(conversation_id).await? {
                out.push(conversation);
            }
        }
        Ok(self.hydrate_conversations(out).await)
    }

    pub async fn list_paginated(
        &self,
        cursor: Option<&str>,
        limit: Option<u32>,
        include_archived: bool,
    ) -> Result<Vec<Conversation>> {
        self.list_by_query(&ConversationListQuery {
            include_archived,
            cursor: cursor.map(str::to_string),
            limit,
            ..ConversationListQuery::default()
        })
        .await
    }

    pub async fn list_raw(&self) -> Result<Vec<Conversation>> {
        self.store.list().await
    }
}

/// The legacy `last_sender_nickname` and the last sender's avatar. The column holds a
/// real name or nothing: the cached profile name when it is a name, else what the row
/// already holds, and never the sender's id. Lists title the last sender by `last_sender`.
fn project_legacy_last_sender(conversation: &mut Conversation, book: &NameBook) {
    let Some(sender_id) = conversation
        .last_message()
        .map(|last| last.sender_id.trim().to_owned())
        .filter(|id| !id.is_empty())
    else {
        return;
    };
    if real_name(&conversation.last_sender_nickname, &[&sender_id]).is_none() {
        conversation.last_sender_nickname.clear();
    }
    let Some(profile) = book.profile(&sender_id) else {
        return;
    };
    let nickname = real_name(&profile.nickname, &[&sender_id])
        .map(str::to_owned)
        .unwrap_or_else(|| conversation.last_sender_nickname.clone());
    let avatar_url = profile.avatar_url.clone();
    *conversation = std::mem::take(conversation).with_last_sender(nickname, avatar_url);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::domain::{ConversationReader, ConversationWriter};
    use crate::infrastructure::persistence::MemoryUserProfileStore;
    use crate::model::conversation::ConversationType;
    use crate::storage::MemoryConversationStore;

    use super::*;

    #[tokio::test]
    async fn list_repairs_and_persists_blank_channel_id_before_wire_decode() {
        let store = Arc::new(MemoryConversationStore::new());
        let profile_reader = Arc::new(MemoryUserProfileStore::new());
        store
            .save_one(&Conversation {
                conversation_id: "2AGROUPCIDVALUE01".to_string(),
                conversation_type: ConversationType::Group,
                channel_id: String::new(),
                display_name: "Group".to_string(),
                ..Default::default()
            })
            .await
            .expect("seed broken local conversation");

        let assembler = ConversationViewAssembler::new(store.clone(), profile_reader);
        let list = assembler.list(false).await.expect("list conversations");

        assert_eq!(list.len(), 1);
        assert_eq!(list[0].channel_id, "2AGROUPCIDVALUE01");

        let persisted = store
            .get("2AGROUPCIDVALUE01")
            .await
            .expect("load repaired conversation")
            .expect("conversation exists");
        assert_eq!(persisted.channel_id, "2AGROUPCIDVALUE01");
    }

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::domain::{
        DisplayName, NameEntity, NameSource, ParticipantDisplaySnapshot, UserProfile, UserWriter,
    };
    use crate::model::{ConversationParticipant, MessagePreviewElem};

    /// Counts how the assembler reads the identity cache.
    #[derive(Default)]
    struct CountingProfiles {
        inner: MemoryUserProfileStore,
        gets: AtomicUsize,
        get_manys: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl UserReader for CountingProfiles {
        async fn get(&self, user_id: &str) -> Result<Option<UserProfile>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(user_id).await
        }

        async fn get_many(&self, user_ids: &[String]) -> Result<HashMap<String, UserProfile>> {
            self.get_manys.fetch_add(1, Ordering::SeqCst);
            let mut out = HashMap::new();
            for id in user_ids {
                if let Some(profile) = self.inner.get(id).await? {
                    out.insert(id.clone(), profile);
                }
            }
            Ok(out)
        }
    }

    fn member(user_id: &str) -> ConversationParticipant {
        ConversationParticipant {
            user_id: user_id.to_string(),
            ..Default::default()
        }
    }

    fn last_from(sender: &str, at: u64) -> Option<MessagePreviewElem> {
        Some(MessagePreviewElem {
            message_id: format!("m-{at}"),
            sender_id: sender.to_string(),
            r#type: 1,
            text: "hi".to_string(),
            time: at,
        })
    }

    async fn seeded() -> (Arc<MemoryConversationStore>, Arc<CountingProfiles>) {
        let store = Arc::new(MemoryConversationStore::new());
        let profiles = Arc::new(CountingProfiles::default());
        profiles
            .inner
            .save_batch(&[
                UserProfile::new("u_bob")
                    .with_nickname("Bob")
                    .with_alias("Bobby"),
                UserProfile::new("u_ann").with_nickname("Ann"),
                UserProfile::new("u_cai").with_nickname("u_cai"),
            ])
            .await
            .unwrap();
        let mut conversations = Vec::new();
        for i in 0..6u64 {
            conversations.push(Conversation {
                conversation_id: format!("1A_single_{i}"),
                conversation_type: ConversationType::Single,
                channel_id: if i % 2 == 0 { "u_bob" } else { "u_cai9876" }.to_string(),
                last_message_at: Some(10 + i),
                last_message: last_from("u_bob", 10 + i),
                last_sender_id: Some("u_bob".to_string()),
                ..Default::default()
            });
        }
        conversations.push(Conversation {
            conversation_id: "2A_group_1".to_string(),
            conversation_type: ConversationType::Group,
            channel_id: "g_1".to_string(),
            display_name: "g_1".to_string(),
            members_count: 4,
            member_preview: vec![
                member("me"),
                member("u_ann"),
                member("u_cai"),
                member("u_dan"),
            ],
            last_message_at: Some(100),
            last_message: last_from("u_dan", 100),
            last_sender_id: Some("u_dan".to_string()),
            ..Default::default()
        });
        store.save_batch(&conversations).await.unwrap();
        (store, profiles)
    }

    fn me() -> CurrentUserIdStore {
        Arc::new(tokio::sync::RwLock::new("me".to_string()))
    }

    /// 会话列表投影名字只读一次身份缓存：N 个会话一次 `get_many`，不逐会话 `get`。
    #[tokio::test]
    async fn listing_reads_the_identity_cache_once_whatever_the_number_of_conversations() {
        let (store, profiles) = seeded().await;
        let assembler =
            ConversationViewAssembler::new(store, profiles.clone()).with_current_user(me());
        let list = assembler.list(false).await.unwrap();
        assert_eq!(list.len(), 7);
        assert_eq!(profiles.get_manys.load(Ordering::SeqCst), 1);
        assert_eq!(profiles.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn listed_conversations_carry_projected_titles_and_last_senders() {
        let (store, profiles) = seeded().await;
        store
            .save_member_nicknames(&[ParticipantDisplaySnapshot::new(
                "2A_group_1",
                "u_dan",
                "Dan in group",
            )])
            .await
            .unwrap();
        let assembler = ConversationViewAssembler::new(store, profiles).with_current_user(me());
        let list = assembler.list(false).await.unwrap();
        let by_id = |id: &str| list.iter().find(|c| c.conversation_id == id).unwrap();

        let bob = by_id("1A_single_0");
        assert_eq!(bob.title, DisplayName::named("Bobby", NameSource::Alias));
        assert_eq!(
            bob.last_sender,
            Some(DisplayName::named("Bobby", NameSource::Alias))
        );
        // Legacy fields keep their old values.
        assert_eq!(bob.display_name, "");
        assert_eq!(bob.last_sender_nickname, "Bob");

        let unnamed = by_id("1A_single_1");
        assert_eq!(
            unnamed.title,
            DisplayName::unnamed(NameEntity::Person, "u_cai9876")
        );

        let group = by_id("2A_group_1");
        assert_eq!(
            group.title,
            DisplayName::Members {
                names: vec!["Ann".to_string(), "Dan in group".to_string()],
                total: 4
            },
            "the group name equal to its id is no name; members other than me title it"
        );
        assert_eq!(
            group.last_sender,
            Some(DisplayName::named(
                "Dan in group",
                NameSource::MemberNickname
            ))
        );
        let names: Vec<_> = group
            .member_preview
            .iter()
            .map(|m| m.name.clone())
            .collect();
        assert_eq!(names[1], DisplayName::named("Ann", NameSource::Profile));
        assert_eq!(names[2], DisplayName::unnamed(NameEntity::Person, "u_cai"));
    }

    /// The legacy `last_sender_nickname` holds a real name or nothing: a cached profile
    /// without a name (or named by its id) never writes the id, and a stored id is cleared.
    #[tokio::test]
    async fn the_legacy_last_sender_field_never_holds_the_sender_id() {
        let store = Arc::new(MemoryConversationStore::new());
        let profiles = Arc::new(CountingProfiles::default());
        profiles
            .inner
            .save_batch(&[
                UserProfile::new("u_nameless").with_avatar("https://a/nameless.png"),
                UserProfile::new("u_cai").with_nickname("u_cai"),
                UserProfile::new("u_bob").with_nickname("Bob"),
            ])
            .await
            .unwrap();
        let single = |id: &str, sender: &str, stored: &str| Conversation {
            conversation_id: id.to_string(),
            conversation_type: ConversationType::Single,
            channel_id: sender.to_string(),
            last_message: last_from(sender, 10),
            last_sender_id: Some(sender.to_string()),
            last_sender_nickname: stored.to_string(),
            ..Default::default()
        };
        store
            .save_batch(&[
                single("1A_nameless", "u_nameless", "Snapshot name"),
                single("1A_by_id", "u_cai", "u_cai"),
                single("1A_uncached", "u_gone", "u_gone"),
                single("1A_named", "u_bob", ""),
            ])
            .await
            .unwrap();

        let assembler = ConversationViewAssembler::new(store, profiles).with_current_user(me());
        let list = assembler.list(false).await.unwrap();
        let by_id = |id: &str| list.iter().find(|c| c.conversation_id == id).unwrap();

        let nameless = by_id("1A_nameless");
        assert_eq!(
            nameless.last_sender_nickname, "Snapshot name",
            "a profile without a name keeps the name the row holds"
        );
        assert_eq!(nameless.last_sender_avatar_url, "https://a/nameless.png");
        assert_eq!(
            by_id("1A_by_id").last_sender_nickname,
            "",
            "a profile name equal to the id is no name"
        );
        assert_eq!(
            by_id("1A_uncached").last_sender_nickname,
            "",
            "a stored id is no name"
        );
        assert_eq!(by_id("1A_named").last_sender_nickname, "Bob");
    }
}
