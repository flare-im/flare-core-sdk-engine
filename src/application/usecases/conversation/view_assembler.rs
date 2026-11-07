use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::application::projections::names::{NameBook, NameRequest};
use crate::domain::{ConversationStore, MessageReader, UserReader, real_name};
use crate::kernel::CurrentUserIdStore;
use crate::model::conversation::ConversationType;
use crate::model::{Conversation, ConversationListQuery};
use crate::shared::error::Result;
use tracing::warn;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ConversationSourceKind {
    Conversation,
    Channel,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ConversationListIdentity {
    business_type: String,
    conversation_type: String,
    source_kind: ConversationSourceKind,
    source_id: String,
}

impl ConversationListIdentity {
    fn from_summary(conversation: &Conversation) -> Self {
        let conversation_id = conversation.conversation_id.trim();
        let channel_id = conversation.channel_id.trim();
        let use_channel = match conversation.conversation_type {
            ConversationType::Single => !channel_id.is_empty() && channel_id != conversation_id,
            ConversationType::Group => !channel_id.is_empty(),
            _ => false,
        };

        Self {
            business_type: conversation.business_type.trim().to_string(),
            conversation_type: conversation.conversation_type.as_str().to_string(),
            source_kind: if use_channel {
                ConversationSourceKind::Channel
            } else {
                ConversationSourceKind::Conversation
            },
            source_id: if use_channel {
                channel_id.to_string()
            } else {
                conversation_id.to_string()
            },
        }
    }
}

/// Collapse duplicate summaries for list presentation only. The selected
/// conversation ID remains a real persisted ID for open/read/write calls, and
/// the store is never modified by this projection.
fn deduplicate_conversation_summaries(summaries: Vec<Conversation>) -> Vec<Conversation> {
    let mut positions = HashMap::with_capacity(summaries.len());
    let mut groups: Vec<Vec<Conversation>> = Vec::new();

    for summary in summaries {
        let identity = ConversationListIdentity::from_summary(&summary);
        let index = match positions.get(&identity) {
            Some(index) => *index,
            None => {
                let index = groups.len();
                positions.insert(identity, index);
                groups.push(Vec::new());
                index
            }
        };
        groups[index].push(summary);
    }

    groups.into_iter().map(merge_conversation_group).collect()
}

fn summary_activity(conversation: &Conversation) -> u64 {
    conversation
        .last_message_at
        .unwrap_or(conversation.updated_at)
}

fn has_message_summary(conversation: &Conversation) -> bool {
    conversation.last_message.is_some()
        || conversation
            .last_message_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || conversation
            .last_message_preview
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || conversation.last_message_at.is_some_and(|value| value > 0)
}

fn nonempty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn merge_conversation_group(group: Vec<Conversation>) -> Conversation {
    let mut primary_index = 0;
    for index in 1..group.len() {
        let candidate = &group[index];
        let primary = &group[primary_index];
        if candidate.max_seq > primary.max_seq
            || (candidate.max_seq == primary.max_seq
                && summary_activity(candidate) > summary_activity(primary))
        {
            primary_index = index;
        }
    }

    let mut merged = group[primary_index].clone();
    let mut latest_message_index = None;
    let mut latest_draft_index = None;

    for (index, conversation) in group.iter().enumerate() {
        if has_message_summary(conversation)
            && latest_message_index.is_none_or(|current| {
                summary_activity(conversation) > summary_activity(&group[current])
            })
        {
            latest_message_index = Some(index);
        }
        if nonempty(conversation.draft.as_deref())
            && latest_draft_index.is_none_or(|current| {
                summary_activity(conversation) > summary_activity(&group[current])
            })
        {
            latest_draft_index = Some(index);
        }

        merged.max_seq = merged.max_seq.max(conversation.max_seq);
        merged.peer_read_seq = merged.peer_read_seq.max(conversation.peer_read_seq);
        merged.unread_count = merged.unread_count.max(conversation.unread_count);
        merged.last_read_seq = merged.last_read_seq.max(conversation.last_read_seq);
        merged.visible_after_seq = merged.visible_after_seq.max(conversation.visible_after_seq);
        merged.members_count = merged.members_count.max(conversation.members_count);
        merged.mention_count = merged.mention_count.max(conversation.mention_count);
        merged.mention_me |= conversation.mention_me;
        merged.is_pinned |= conversation.is_pinned;
        merged.is_muted |= conversation.is_muted;
        merged.is_archived &= conversation.is_archived;
        merged.updated_at = merged.updated_at.max(conversation.updated_at);
        merged.version = merged.version.max(conversation.version);
        if conversation.participant_version > merged.participant_version
            || (merged.participants.is_empty() && !conversation.participants.is_empty())
        {
            merged.participant_version = conversation.participant_version;
            merged.participants = conversation.participants.clone();
        }
        merged.updated_at_ts = match (merged.updated_at_ts, conversation.updated_at_ts) {
            (Some(current), Some(candidate)) => Some(current.max(candidate)),
            (None, candidate) => candidate,
            (current, None) => current,
        };

        if merged.display_name.trim().is_empty() && !conversation.display_name.trim().is_empty() {
            merged.display_name = conversation.display_name.clone();
        }
        if merged.avatar_url.trim().is_empty() && !conversation.avatar_url.trim().is_empty() {
            merged.avatar_url = conversation.avatar_url.clone();
        }
        if merged.channel_id.trim().is_empty() && !conversation.channel_id.trim().is_empty() {
            merged.channel_id = conversation.channel_id.clone();
        }
        if !nonempty(merged.remark.as_deref()) && nonempty(conversation.remark.as_deref()) {
            merged.remark = conversation.remark.clone();
        }
        if !nonempty(merged.description.as_deref()) && nonempty(conversation.description.as_deref())
        {
            merged.description = conversation.description.clone();
        }
        if !nonempty(merged.badge.as_deref()) && nonempty(conversation.badge.as_deref()) {
            merged.badge = conversation.badge.clone();
        }
        if !nonempty(merged.role.as_deref()) && nonempty(conversation.role.as_deref()) {
            merged.role = conversation.role.clone();
        }
        if merged.member_preview.is_empty() && !conversation.member_preview.is_empty() {
            merged.member_preview = conversation.member_preview.clone();
        }
    }

    if let Some(index) = latest_message_index {
        let latest = &group[index];
        merged.last_message_id = latest.last_message_id.clone();
        merged.last_sender_id = latest.last_sender_id.clone();
        merged.last_message_at = latest.last_message_at;
        merged.last_message_preview = latest.last_message_preview.clone();
        merged.last_message = latest.last_message.clone();
        merged.last_sender_nickname = latest.last_sender_nickname.clone();
        merged.last_sender_avatar_url = latest.last_sender_avatar_url.clone();
    }
    if !nonempty(merged.draft.as_deref()) {
        merged.draft = latest_draft_index.and_then(|index| group[index].draft.clone());
    }

    merged
}

pub struct ConversationViewAssembler {
    store: Arc<dyn ConversationStore>,
    profile_reader: Arc<dyn UserReader>,
    current_user_id: Option<CurrentUserIdStore>,
    message_reader: Option<Arc<dyn MessageReader>>,
}

impl ConversationViewAssembler {
    pub fn new(store: Arc<dyn ConversationStore>, profile_reader: Arc<dyn UserReader>) -> Self {
        Self {
            store,
            profile_reader,
            current_user_id: None,
            message_reader: None,
        }
    }

    /// The signed-in user, so unnamed groups are titled by the other members and a
    /// single chat finds its peer in the member preview.
    pub fn with_current_user(mut self, current_user_id: CurrentUserIdStore) -> Self {
        self.current_user_id = Some(current_user_id);
        self
    }

    pub fn with_message_reader(mut self, reader: Arc<dyn MessageReader>) -> Self {
        self.message_reader = Some(reader);
        self
    }

    async fn hydrate_message_previews(&self, conversations: &mut [Conversation]) {
        let Some(reader) = &self.message_reader else {
            return;
        };
        let ids = conversations
            .iter()
            .filter_map(|c| c.last_message_id.clone())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return;
        }
        let Ok(mut messages) = reader.get_by_message_ids(&ids).await else {
            return;
        };
        let known = messages
            .iter()
            .flat_map(|m| [&m.server_id, &m.client_msg_id])
            .collect::<HashSet<_>>();
        let missing = ids
            .iter()
            .filter(|id| !known.contains(id))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty()
            && let Ok(pending) = reader.get_by_client_msg_ids(&missing).await
        {
            messages.extend(pending);
        }
        let mut by_id = HashMap::new();
        for message in &messages {
            if !message.server_id.is_empty() {
                by_id.insert(message.server_id.as_str(), message);
            }
            if !message.client_msg_id.is_empty() {
                by_id.insert(message.client_msg_id.as_str(), message);
            }
        }
        for conversation in conversations {
            let Some(message) = conversation
                .last_message_id
                .as_deref()
                .and_then(|id| by_id.get(id))
            else {
                continue;
            };
            if message.conversation_id != conversation.conversation_id {
                continue;
            }
            // The summary owns ordering and read watermarks; the materialized message owns edits and recall.
            // Project only the referenced message, without writing an older summary over concurrent arrivals.
            let preview = if message.is_recalled
                || message.status == flare_proto::common::MessageStatus::Recalled as i32
            {
                Some(
                    serde_json::json!({"k":"im.preview.system","a":{"ek":"message.recalled"}})
                        .to_string(),
                )
            } else {
                message.text_for_storage()
            };
            conversation.last_message_preview = preview.clone();
            conversation.last_sender_id = Some(message.sender_id.clone());
            conversation.last_message = Some(crate::content::message_elem::MessagePreviewElem {
                message_id: conversation.last_message_id.clone().unwrap_or_default(),
                sender_id: message.sender_id.clone(),
                r#type: message.message_type,
                text: preview.unwrap_or_default(),
                time: message.display_time_ms(),
            });
        }
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
        self.hydrate_message_previews(&mut conversations).await;
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
        let mut summaries = self.store.list_by_query(query).await?;
        if let Some(cursor) = query
            .cursor
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            && let Some(cursor_index) = summaries
                .iter()
                .position(|conversation| conversation.conversation_id == cursor)
        {
            // A cursor may reference a duplicate whose representative is
            // different from the one selected for display. Mark every
            // identity through the cursor as already emitted so a hidden
            // duplicate cannot reappear on the next page.
            let emitted_identities: HashSet<_> = summaries[..=cursor_index]
                .iter()
                .map(ConversationListIdentity::from_summary)
                .collect();
            summaries = summaries
                .into_iter()
                .enumerate()
                .filter(|(index, conversation)| {
                    *index > cursor_index
                        && !emitted_identities
                            .contains(&ConversationListIdentity::from_summary(conversation))
                })
                .map(|(_, conversation)| conversation)
                .collect();
        }
        let mut list = deduplicate_conversation_summaries(summaries);
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
    async fn list_projects_latest_message_mutations_without_replacing_summary_watermarks() {
        use crate::domain::MessageWriter;
        use crate::storage::MemoryMessageStore;
        use flare_proto::common::{Message, MessageContent, TextContent, message_content::Content};
        let conversations = Arc::new(MemoryConversationStore::new());
        let messages = Arc::new(MemoryMessageStore::new());
        conversations
            .save_one(&Conversation {
                conversation_id: "chat-1".into(),
                last_message_id: Some("server-1".into()),
                last_message_preview: Some("original text".into()),
                max_seq: 99,
                unread_count: 3,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut message = crate::model::IMMessage::new(Message {
            server_id: "server-1".into(),
            client_msg_id: "client-1".into(),
            conversation_id: "chat-1".into(),
            conversation_seq: 4,
            content: Some(MessageContent {
                content: Some(Content::Text(TextContent {
                    text: "edited text".into(),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        });
        messages.save_batch(&[message.clone()]).await.unwrap();
        let assembler = ConversationViewAssembler::new(
            conversations.clone(),
            Arc::new(MemoryUserProfileStore::new()),
        )
        .with_message_reader(messages.clone());
        let edited = assembler.list(false).await.unwrap().remove(0);
        assert!(
            edited
                .last_message_preview
                .as_deref()
                .unwrap()
                .contains("edited text")
        );
        assert_eq!(edited.max_seq, 99);
        assert_eq!(edited.unread_count, 3);
        message.is_recalled = true;
        message.status = flare_proto::common::MessageStatus::Recalled as i32;
        messages.save_batch(&[message]).await.unwrap();
        let recalled = assembler.list(false).await.unwrap().remove(0);
        let preview = recalled.last_message_preview.unwrap();
        assert!(preview.contains("message.recalled"));
        assert!(!preview.contains("edited text"));
        assert_eq!(recalled.last_message.unwrap().text, preview);
        assert_eq!(
            conversations.get("chat-1").await.unwrap().unwrap().max_seq,
            99
        );
    }

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

    #[tokio::test]
    async fn list_collapses_duplicate_single_summaries_without_mutating_stored_rows() {
        let store = Arc::new(MemoryConversationStore::new());
        let profile_reader = Arc::new(MemoryUserProfileStore::new());
        store
            .save_batch(&[
                Conversation {
                    conversation_id: "single-with-draft".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "chat".to_string(),
                    channel_id: "peer-1".to_string(),
                    last_message_at: Some(200),
                    draft: Some("keep this draft".to_string()),
                    max_seq: 7,
                    unread_count: 1,
                    last_read_seq: 2,
                    ..Default::default()
                },
                Conversation {
                    conversation_id: "single-with-history".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "chat".to_string(),
                    channel_id: "peer-1".to_string(),
                    last_message_at: Some(300),
                    last_message_preview: Some("latest message".to_string()),
                    max_seq: 9,
                    unread_count: 3,
                    last_read_seq: 5,
                    is_pinned: true,
                    ..Default::default()
                },
                Conversation {
                    conversation_id: "another-peer".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "chat".to_string(),
                    channel_id: "peer-2".to_string(),
                    ..Default::default()
                },
                Conversation {
                    conversation_id: "other-business".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "support".to_string(),
                    channel_id: "peer-1".to_string(),
                    ..Default::default()
                },
            ])
            .await
            .expect("seed duplicate conversation summaries");

        let assembler = ConversationViewAssembler::new(store.clone(), profile_reader);
        let list = assembler.list(false).await.expect("list conversations");

        assert_eq!(list.len(), 3);
        let peer = list
            .iter()
            .find(|conversation| {
                conversation.channel_id == "peer-1" && conversation.business_type == "chat"
            })
            .expect("chat peer summary");
        assert_eq!(peer.conversation_id, "single-with-history");
        assert_eq!(peer.last_message_preview.as_deref(), Some("latest message"));
        assert_eq!(peer.draft.as_deref(), Some("keep this draft"));
        assert_eq!(peer.max_seq, 9);
        assert_eq!(peer.unread_count, 3);
        assert_eq!(peer.last_read_seq, 5);
        assert!(peer.is_pinned);

        let raw = assembler
            .list_raw()
            .await
            .expect("read raw stored summaries");
        assert_eq!(raw.len(), 4, "the display projection must not delete rows");
    }

    #[tokio::test]
    async fn paged_list_does_not_resurface_a_hidden_duplicate_after_cursor() {
        let store = Arc::new(MemoryConversationStore::new());
        let profile_reader = Arc::new(MemoryUserProfileStore::new());
        store
            .save_batch(&[
                Conversation {
                    conversation_id: "peer-1-first".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "chat".to_string(),
                    channel_id: "peer-1".to_string(),
                    last_message_at: Some(300),
                    max_seq: 9,
                    ..Default::default()
                },
                Conversation {
                    conversation_id: "peer-2".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "chat".to_string(),
                    channel_id: "peer-2".to_string(),
                    last_message_at: Some(200),
                    max_seq: 3,
                    ..Default::default()
                },
                Conversation {
                    conversation_id: "peer-1-duplicate".to_string(),
                    conversation_type: ConversationType::Single,
                    business_type: "chat".to_string(),
                    channel_id: "peer-1".to_string(),
                    last_message_at: Some(100),
                    max_seq: 2,
                    ..Default::default()
                },
            ])
            .await
            .expect("seed duplicate conversation summaries");

        let assembler = ConversationViewAssembler::new(store, profile_reader);
        let next_page = assembler
            .list_paginated(Some("peer-1-first"), Some(20), false)
            .await
            .expect("list after cursor");

        assert_eq!(next_page.len(), 1);
        assert_eq!(next_page[0].conversation_id, "peer-2");
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
                business_type: format!("fixture_{i}"),
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
