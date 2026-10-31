//! Read-time name projection: what conversations and messages are called when they
//! are read. Names are never written back to the stores; they are projected from the
//! identity cache, the member nicknames business SDKs wrote, and the snapshots the
//! rows carry, with the rules in [`crate::domain::display_name`].

use std::collections::{BTreeSet, HashMap};

use tracing::warn;

use crate::domain::{
    ConversationReader, ConversationTitleInput, DisplayName, MemberKey, PersonNameCandidates,
    UserProfile, UserReader, member_nicknames_apply, resolve_conversation_title,
    resolve_person_name,
};
use crate::model::conversation::ConversationType;
use crate::model::{Conversation, ConversationParticipant};

/// What the projection needs to know about people, read in one batch per source.
#[derive(Default)]
pub(crate) struct NameBook {
    profiles: HashMap<String, UserProfile>,
    /// conversation id → user id → member nickname a business SDK wrote.
    nicknames: HashMap<String, HashMap<String, String>>,
}

/// The ids a batch of reads needs names for.
#[derive(Default)]
pub(crate) struct NameRequest {
    user_ids: BTreeSet<String>,
    members: BTreeSet<MemberKey>,
}

impl NameRequest {
    /// Asks for `user_id`'s profile and, in a group or channel, their member nickname.
    pub(crate) fn person(
        &mut self,
        conversation_id: &str,
        conversation_type: ConversationType,
        user_id: &str,
    ) {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return;
        }
        self.user_ids.insert(user_id.to_owned());
        if member_nicknames_apply(conversation_type) && !conversation_id.trim().is_empty() {
            self.members
                .insert((conversation_id.trim().to_owned(), user_id.to_owned()));
        }
    }

    /// Asks for everyone `conversation`'s title, last sender and member list name.
    pub(crate) fn conversation(&mut self, conversation: &Conversation, current_user_id: &str) {
        let id = conversation.conversation_id.as_str();
        let kind = conversation.conversation_type;
        if let Some(sender) = last_sender_id(conversation) {
            self.person(id, kind, sender);
        }
        if kind == ConversationType::Single
            && let Some(peer) = single_chat_peer(conversation, current_user_id)
        {
            self.person(id, kind, peer);
        }
        for member in conversation
            .member_preview
            .iter()
            .chain(conversation.participants.iter())
        {
            self.person(id, kind, &member.user_id);
        }
    }

    /// Reads the profiles with one `get_many` and the member nicknames with one
    /// `member_nicknames`. A failed read projects without that source.
    pub(crate) async fn load(
        self,
        profiles: &dyn UserReader,
        conversations: Option<&dyn ConversationReader>,
    ) -> NameBook {
        let mut book = NameBook::default();
        if !self.user_ids.is_empty() {
            let ids: Vec<String> = self.user_ids.into_iter().collect();
            match profiles.get_many(&ids).await {
                Ok(found) => book.profiles = found,
                Err(error) => warn!(%error, "name projection: profile read failed"),
            }
        }
        if let Some(conversations) = conversations
            && !self.members.is_empty()
        {
            let pairs: Vec<MemberKey> = self.members.into_iter().collect();
            match conversations.member_nicknames(&pairs).await {
                Ok(found) => {
                    for ((conversation_id, user_id), nickname) in found {
                        book.nicknames
                            .entry(conversation_id)
                            .or_default()
                            .insert(user_id, nickname);
                    }
                }
                Err(error) => warn!(%error, "name projection: member nickname read failed"),
            }
        }
        book
    }
}

impl NameBook {
    pub(crate) fn profile(&self, user_id: &str) -> Option<&UserProfile> {
        self.profiles.get(user_id.trim())
    }

    /// The names of `user_id` in `conversation_id`: the cached alias and profile name,
    /// the member nickname a business SDK wrote (else `row_nickname`, the one the
    /// conversation row carries), and `snapshot`.
    pub(crate) fn candidates<'a>(
        &'a self,
        conversation_id: &str,
        user_id: &str,
        row_nickname: &'a str,
        snapshot: &'a str,
    ) -> PersonNameCandidates<'a> {
        let profile = self.profile(user_id);
        let written = self
            .nicknames
            .get(conversation_id.trim())
            .and_then(|members| members.get(user_id.trim()))
            .map(String::as_str)
            .filter(|nickname| !nickname.trim().is_empty());
        PersonNameCandidates {
            alias: profile.map(|p| p.alias.as_str()).unwrap_or_default(),
            member_nickname: written.unwrap_or(row_nickname),
            profile_name: profile.map(|p| p.nickname.as_str()).unwrap_or_default(),
            snapshot,
        }
    }

    /// What `user_id` is called in a conversation of `conversation_type`.
    pub(crate) fn person(
        &self,
        conversation_id: &str,
        conversation_type: ConversationType,
        user_id: &str,
        row_nickname: &str,
        snapshot: &str,
    ) -> DisplayName {
        resolve_person_name(
            user_id,
            &self.candidates(conversation_id, user_id, row_nickname, snapshot),
            Some(conversation_type),
        )
    }

    /// Projects `conversation`'s title, last sender and member names in place.
    pub(crate) fn project_conversation(
        &self,
        conversation: &mut Conversation,
        current_user_id: &str,
    ) {
        let member_ids: Vec<&str> = conversation
            .member_preview
            .iter()
            .map(|member| member.user_id.as_str())
            .collect();
        let input = ConversationTitleInput {
            conversation_id: &conversation.conversation_id,
            conversation_type: conversation.conversation_type,
            channel_id: &conversation.channel_id,
            remark: conversation.remark.as_deref().unwrap_or_default(),
            display_name: &conversation.display_name,
            current_user_id,
            member_ids: &member_ids,
            members_count: conversation.members_count,
        };
        let title = resolve_conversation_title(&input, |id| {
            self.candidates(
                &conversation.conversation_id,
                id,
                row_nickname(conversation, id),
                "",
            )
        });
        let last_sender = last_sender_id(conversation).map(|sender| {
            self.person(
                &conversation.conversation_id,
                conversation.conversation_type,
                sender,
                row_nickname(conversation, sender),
                &conversation.last_sender_nickname,
            )
        });
        conversation.title = Some(title);
        conversation.last_sender = last_sender;
        let conversation_id = conversation.conversation_id.clone();
        let conversation_type = conversation.conversation_type;
        for member in conversation
            .member_preview
            .iter_mut()
            .chain(conversation.participants.iter_mut())
        {
            member.name = Some(self.member_name(&conversation_id, conversation_type, member));
        }
    }

    /// What a member is called in the member list of `conversation_id`.
    pub(crate) fn member_name(
        &self,
        conversation_id: &str,
        conversation_type: ConversationType,
        member: &ConversationParticipant,
    ) -> DisplayName {
        self.person(
            conversation_id,
            conversation_type,
            &member.user_id,
            &member.nickname,
            "",
        )
    }
}

/// The sender of a conversation's last message: the preview's sender, else
/// `last_sender_id`.
fn last_sender_id(conversation: &Conversation) -> Option<&str> {
    conversation
        .last_message
        .as_ref()
        .map(|last| last.sender_id.trim())
        .filter(|id| !id.is_empty())
        .or_else(|| {
            conversation
                .last_sender_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
        })
}

/// The nickname the conversation row itself carries for `user_id` (from the member
/// preview or the synced members).
fn row_nickname<'a>(conversation: &'a Conversation, user_id: &str) -> &'a str {
    let user_id = user_id.trim();
    conversation
        .member_preview
        .iter()
        .chain(conversation.participants.iter())
        .find(|member| member.user_id.trim() == user_id)
        .map(|member| member.nickname.as_str())
        .unwrap_or_default()
}

/// The peer of a single chat as the title rule finds it.
fn single_chat_peer<'a>(conversation: &'a Conversation, current_user_id: &str) -> Option<&'a str> {
    let channel = conversation.channel_id.trim();
    if !channel.is_empty() {
        return Some(channel);
    }
    let me = current_user_id.trim();
    conversation
        .member_preview
        .iter()
        .map(|member| member.user_id.trim())
        .find(|id| !id.is_empty() && *id != me)
}
