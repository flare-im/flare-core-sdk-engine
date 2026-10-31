use std::collections::HashMap;
use std::sync::Arc;

use crate::application::projections::names::NameRequest;
use crate::domain::{
    ConversationStore, MessageStore, UserReader, member_nicknames_apply, real_name,
};
use crate::model::conversation::ConversationType;
use crate::model::{Conversation, IMMessage, MessageSearchQuery};
use crate::shared::error::Result;

pub struct MessageViewAssembler {
    store: Arc<dyn MessageStore>,
    profile_reader: Arc<dyn UserReader>,
    /// Group read summaries of the viewer's own messages (`receipt.readSummary`).
    read_summaries: Option<Arc<crate::application::services::ReadSummaryService>>,
    /// Where group member nicknames are read from when sender names are projected.
    conversations: Option<Arc<dyn ConversationStore>>,
}

impl MessageViewAssembler {
    pub fn new(store: Arc<dyn MessageStore>, profile_reader: Arc<dyn UserReader>) -> Self {
        Self {
            store,
            profile_reader,
            read_summaries: None,
            conversations: None,
        }
    }

    /// Sender names in group and channel timelines use the members' nicknames, read in
    /// one batch from `conversations`.
    pub(crate) fn with_member_names(mut self, conversations: Arc<dyn ConversationStore>) -> Self {
        self.conversations = Some(conversations);
        self
    }

    /// View messages carry the group read summary of the viewer's own messages,
    /// fetched in the background for the messages views show.
    pub(crate) fn with_read_summaries(
        mut self,
        read_summaries: Arc<crate::application::services::ReadSummaryService>,
    ) -> Self {
        self.read_summaries = Some(read_summaries);
        self
    }

    pub async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        let msg = match self.store.get_by_client_msg_id(message_id).await? {
            Some(message) => Some(message),
            None => self.store.get(message_id).await?,
        };
        let Some(mut view) = msg else {
            return Ok(None);
        };
        self.hydrate_reactions_for_messages(std::slice::from_mut(&mut view))
            .await?;
        self.fill_sender_profiles_for_messages(std::slice::from_mut(&mut view))
            .await?;
        Ok(Some(view))
    }

    pub async fn get_raw(&self, message_id: &str) -> Result<Option<IMMessage>> {
        let mut msg = match self.store.get_by_client_msg_id(message_id).await? {
            Some(message) => Some(message),
            None => self.store.get(message_id).await?,
        };
        if let Some(view) = msg.as_mut() {
            self.hydrate_reactions_for_messages(std::slice::from_mut(view))
                .await?;
        }
        Ok(msg)
    }

    pub async fn list(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        let mut views = self
            .store
            .get_by_conversation(conversation_id, before_seq, limit)
            .await?;
        self.hydrate_messages_for_view(&mut views).await?;
        Ok(views)
    }

    pub async fn hydrate_messages_for_view(&self, views: &mut [IMMessage]) -> Result<()> {
        self.hydrate_reactions_for_messages(views).await?;
        self.fill_sender_profiles_for_messages(views).await?;
        if let Some(read_summaries) = &self.read_summaries {
            read_summaries.decorate(views).await;
        }
        Ok(())
    }

    /// 批量投影发送者名：所有发送者的资料**单次** `get_many`，群与频道会话的成员昵称单次
    /// `member_nicknames`（另一次 `get_many` 读这些会话行里的成员预览），按 map 回填。
    /// 取代此前的逐条查询（N+1：每条消息一次 SQLite 查询），是打开大时间线慢/超时的主因。
    ///
    /// `sender` 走人名解析链；旧字段 `sender_display_name` 只在资料里有真名字时写入资料名，
    /// 不再把 id 写进去盖掉发送时快照（`sender_name`）。
    async fn fill_sender_profiles_for_messages(&self, views: &mut [IMMessage]) -> Result<()> {
        let mut request = NameRequest::default();
        let mut group_conversation_ids: Vec<String> = Vec::new();
        for view in views.iter() {
            let kind = ConversationType::from_proto_int(view.conversation_type);
            request.person(&view.conversation_id, kind, view.sender_id());
            if member_nicknames_apply(kind)
                && !view.conversation_id.trim().is_empty()
                && !group_conversation_ids.contains(&view.conversation_id)
            {
                group_conversation_ids.push(view.conversation_id.clone());
            }
        }
        let conversations = self.conversations.as_deref();
        let book = request
            .load(
                self.profile_reader.as_ref(),
                conversations.map(|store| store as &dyn crate::domain::ConversationReader),
            )
            .await;
        let rows: HashMap<String, Conversation> = match conversations {
            Some(store) if !group_conversation_ids.is_empty() => store
                .get_many(&group_conversation_ids)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|conversation| (conversation.conversation_id.clone(), conversation))
                .collect(),
            _ => HashMap::new(),
        };
        for view in views.iter_mut() {
            let sender_id = view.sender_id().trim().to_owned();
            if real_name(&view.sender_display_name, &[&sender_id]).is_none() {
                view.sender_display_name.clear();
            }
            if let Some(profile) = book.profile(&sender_id)
                && let Some(name) = real_name(&profile.nickname, &[&sender_id])
            {
                view.sender_display_name = name.to_owned();
            }
            let row_nickname = rows
                .get(&view.conversation_id)
                .and_then(|row| {
                    row.member_preview
                        .iter()
                        .chain(row.participants.iter())
                        .find(|member| member.user_id.trim() == sender_id)
                })
                .map(|member| member.nickname.as_str())
                .unwrap_or_default();
            view.sender = Some(book.person(
                &view.conversation_id,
                ConversationType::from_proto_int(view.conversation_type),
                &sender_id,
                row_nickname,
                &view.sender_name,
            ));
        }
        Ok(())
    }

    pub async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.search_by_query(&MessageSearchQuery::text(keyword, limit))
            .await
    }

    pub async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.search_by_query(&MessageSearchQuery::in_conversation(
            conversation_id,
            keyword,
            limit,
        ))
        .await
    }

    pub async fn search_by_query(&self, query: &MessageSearchQuery) -> Result<Vec<IMMessage>> {
        let mut views = self.store.search_by_query(query).await?;
        self.hydrate_messages_for_view(&mut views).await?;
        Ok(views)
    }

    async fn hydrate_reactions_for_messages(&self, views: &mut [IMMessage]) -> Result<()> {
        let mut message_ids: Vec<String> = Vec::with_capacity(views.len() * 2);
        for message in views.iter() {
            let sid = message.server_id.trim();
            if !sid.is_empty() {
                message_ids.push(sid.to_string());
            }
            let cid = message.client_msg_id.trim();
            if !cid.is_empty() {
                message_ids.push(cid.to_string());
            }
        }
        message_ids.sort();
        message_ids.dedup();
        if message_ids.is_empty() {
            return Ok(());
        }
        let reaction_map = self.store.list_reactions(&message_ids).await?;
        for view in views.iter_mut() {
            let sid = view.server_id.trim();
            let cid = view.client_msg_id.trim();
            let reactions = if !sid.is_empty() {
                reaction_map.get(sid)
            } else {
                None
            }
            .or_else(|| {
                if cid.is_empty() {
                    None
                } else {
                    reaction_map.get(cid)
                }
            });
            if let Some(reactions) = reactions {
                view.reactions = reactions.clone();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::MessageViewAssembler;
    use crate::domain::{
        ConversationWriter, DisplayName, MessageWriter, NameEntity, NameSource,
        ParticipantDisplaySnapshot, UserProfile, UserWriter,
    };
    use crate::infrastructure::persistence::MemoryUserProfileStore;
    use crate::model::IMMessage;
    use crate::model::conversation::ConversationType;
    use crate::storage::{MemoryConversationStore, MemoryMessageStore};

    fn message(
        server_id: &str,
        conversation_id: &str,
        kind: ConversationType,
        sender: &str,
    ) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = server_id.to_string();
        message.client_msg_id = server_id.to_string();
        message.conversation_id = conversation_id.to_string();
        message.conversation_type = kind.to_proto_int();
        message.sender_id = sender.to_string();
        message.conversation_seq = server_id.trim_start_matches('s').parse().unwrap_or(1);
        message.created_at = 1_000;
        message
    }

    async fn assembler(
        messages: &[IMMessage],
        profiles: &[UserProfile],
    ) -> (MessageViewAssembler, Arc<MemoryConversationStore>) {
        let store = Arc::new(MemoryMessageStore::new());
        store.save_batch(messages).await.unwrap();
        let cache = Arc::new(MemoryUserProfileStore::new());
        cache.save_batch(profiles).await.unwrap();
        let conversations = Arc::new(MemoryConversationStore::new());
        let assembler =
            MessageViewAssembler::new(store, cache).with_member_names(conversations.clone());
        (assembler, conversations)
    }

    /// 身份缓存里有这个人但没有名字时，不把 id 写进旧字段盖掉发送时快照。
    #[tokio::test]
    async fn a_cached_profile_without_a_name_does_not_cover_the_send_time_snapshot() {
        let mut snapshot = message("s1", "1A_c", ConversationType::Single, "u2");
        snapshot.sender_name = "Bob at send time".to_string();
        let (assembler, _) = assembler(&[snapshot], &[UserProfile::new("u2")]).await;

        let view = assembler.get("s1").await.unwrap().expect("message");
        assert_eq!(view.sender_display_name, "", "an id is not a display name");
        assert_eq!(view.display_name(), "Bob at send time");
        assert_eq!(
            view.sender,
            Some(DisplayName::named("Bob at send time", NameSource::Snapshot))
        );
    }

    #[tokio::test]
    async fn group_senders_are_named_by_alias_then_member_nickname_then_profile() {
        let messages = [
            message("s1", "2A_g", ConversationType::Group, "u_a"),
            message("s2", "2A_g", ConversationType::Group, "u_b"),
            message("s3", "2A_g", ConversationType::Group, "u_c9876"),
            message("s4", "1A_c", ConversationType::Single, "u_b"),
        ];
        let (assembler, conversations) = assembler(
            &messages,
            &[
                UserProfile::new("u_a")
                    .with_nickname("Ann")
                    .with_alias("Annie"),
                UserProfile::new("u_b").with_nickname("Ben"),
            ],
        )
        .await;
        conversations
            .save_member_nicknames(&[
                ParticipantDisplaySnapshot::new("2A_g", "u_a", "Group Ann"),
                ParticipantDisplaySnapshot::new("2A_g", "u_b", "Group Ben"),
                ParticipantDisplaySnapshot::new("1A_c", "u_b", "Not used in a single chat"),
            ])
            .await
            .unwrap();

        let group = assembler.list("2A_g", 0, 20).await.unwrap();
        let sender_of = |list: &[IMMessage], id: &str| {
            list.iter()
                .find(|m| m.server_id == id)
                .and_then(|m| m.sender.clone())
                .expect("projected sender")
        };
        assert_eq!(
            sender_of(&group, "s1"),
            DisplayName::named("Annie", NameSource::Alias)
        );
        assert_eq!(
            sender_of(&group, "s2"),
            DisplayName::named("Group Ben", NameSource::MemberNickname)
        );
        assert_eq!(
            sender_of(&group, "s3"),
            DisplayName::unnamed(NameEntity::Person, "u_c9876")
        );
        let single = assembler.list("1A_c", 0, 20).await.unwrap();
        assert_eq!(
            sender_of(&single, "s4"),
            DisplayName::named("Ben", NameSource::Profile)
        );
    }
}
