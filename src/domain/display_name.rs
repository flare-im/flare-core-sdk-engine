//! Names for people and conversations, decided in one place.
//!
//! A name field only ever holds a real name. An account id or a conversation id is
//! never a name: the one test for "this is not a name" is that the value, trimmed,
//! is empty or equal to the id of the thing it names. Nothing guesses by shape
//! (length, digits, prefixes), so `0401` or `ChristopherSmith` are names.
//!
//! When nothing names the subject the result is a structured [`DisplayName::Unnamed`]
//! (what kind of thing it is plus the last four characters of its id); the UI kit
//! turns that into words. Core carries no localized text.
//!
//! The two chains here are pure functions so business SDKs that list people or
//! conversations of their own call the same rules instead of writing another
//! priority order.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::model::conversation::ConversationType;

/// Most member names an unnamed group's title joins.
pub const MAX_TITLE_MEMBER_NAMES: usize = 3;

/// Where a [`DisplayName::Named`] text came from. For tests and diagnostics only:
/// hosts render every named value the same way.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NameSource {
    /// The private label the current user gave the account.
    Alias,
    /// The account's name inside this conversation.
    MemberNickname,
    /// The account's own name in the identity cache.
    Profile,
    /// The sender name a message carried when it was sent.
    Snapshot,
    /// The private label the current user gave the conversation.
    Remark,
    /// The conversation's business name.
    Name,
}

/// What an unnamed value names; follows [`ConversationType`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NameEntity {
    #[default]
    Person,
    Group,
    System,
    Ai,
    Customer,
    Channel,
    Broadcast,
    Temp,
}

impl NameEntity {
    /// The entity an unnamed conversation of this type is. A single chat (and a
    /// conversation whose type is not known) is a person.
    pub fn of_conversation(conversation_type: ConversationType) -> Self {
        match conversation_type {
            ConversationType::Unspecified | ConversationType::Single => Self::Person,
            ConversationType::Group => Self::Group,
            ConversationType::Ai => Self::Ai,
            ConversationType::System => Self::System,
            ConversationType::Customer => Self::Customer,
            ConversationType::Temp => Self::Temp,
            ConversationType::Channel => Self::Channel,
            ConversationType::Broadcast => Self::Broadcast,
        }
    }
}

/// The final name of a person or a conversation, ready for the kit to render.
///
/// Wire (camelCase JSON, tagged by `kind`):
/// - `{"kind":"named","text":"Alice","source":"alias"}`
/// - `{"kind":"members","names":["Zhang San","Li Si"],"total":12}`: an unnamed
///   group titled by up to three of its named members (never the current user);
///   `total` counts every member, the current user included.
/// - `{"kind":"unnamed","entity":"person","idTail":"9876"}`: `idTail` is
///   [`id_tail`] of the id, empty when there is no id (and for unnamed groups).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum DisplayName {
    Named {
        text: String,
        source: NameSource,
    },
    Members {
        names: Vec<String>,
        total: u32,
    },
    #[serde(rename_all = "camelCase")]
    Unnamed {
        entity: NameEntity,
        id_tail: String,
    },
}

impl Default for DisplayName {
    fn default() -> Self {
        Self::Unnamed {
            entity: NameEntity::Person,
            id_tail: String::new(),
        }
    }
}

impl DisplayName {
    pub fn named(text: impl Into<String>, source: NameSource) -> Self {
        Self::Named {
            text: text.into(),
            source,
        }
    }

    /// The unnamed value for `entity` whose id is `id`.
    pub fn unnamed(entity: NameEntity, id: &str) -> Self {
        Self::Unnamed {
            entity,
            id_tail: id_tail(id),
        }
    }

    /// The text of a named value; `None` for members and unnamed values.
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Named { text, .. } => Some(text),
            _ => None,
        }
    }

    pub fn is_named(&self) -> bool {
        matches!(self, Self::Named { .. })
    }
}

/// The last four Unicode scalar values of `id` after trimming; the whole id when it
/// is four or shorter; empty for an empty id. Same rule as the kit's plugin
/// fallback name.
pub fn id_tail(id: &str) -> String {
    let id = id.trim();
    let count = id.chars().count();
    id.chars().skip(count.saturating_sub(4)).collect()
}

/// `candidate` trimmed when it is a name of the thing whose ids are `own_ids`;
/// `None` when it is empty or equal to one of those ids.
pub fn real_name<'a>(candidate: &'a str, own_ids: &[&str]) -> Option<&'a str> {
    let name = candidate.trim();
    if name.is_empty() {
        return None;
    }
    let is_own_id = own_ids.iter().any(|id| {
        let id = id.trim();
        !id.is_empty() && id == name
    });
    if is_own_id { None } else { Some(name) }
}

/// What a name column may store for the thing whose ids are `own_ids`: the trimmed
/// name, or an empty string when `candidate` is not a name.
pub fn business_name(candidate: &str, own_ids: &[&str]) -> String {
    real_name(candidate, own_ids).unwrap_or("").to_owned()
}

/// Whether member nicknames name people in conversations of this type (groups and
/// channels have per-conversation member names; a single chat does not).
pub fn member_nicknames_apply(conversation_type: ConversationType) -> bool {
    matches!(
        conversation_type,
        ConversationType::Group | ConversationType::Channel
    )
}

/// Names one account can go by, as the caller knows them. Empty means unknown.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PersonNameCandidates<'a> {
    /// The current user's private label for the account (a friend remark).
    pub alias: &'a str,
    /// The account's name in the conversation (a group nickname).
    pub member_nickname: &'a str,
    /// The account's own name.
    pub profile_name: &'a str,
    /// The sender name a message carried; only for message senders.
    pub snapshot: &'a str,
}

/// What `user_id` is called: alias, then (in a group or channel) the member
/// nickname, then the profile name, then the message snapshot, then unnamed.
///
/// `conversation_type` is the conversation the name is shown in; `None` when it is
/// shown outside any conversation (a contact list, a single chat title).
pub fn resolve_person_name(
    user_id: &str,
    names: &PersonNameCandidates<'_>,
    conversation_type: Option<ConversationType>,
) -> DisplayName {
    let own = [user_id];
    let in_group = conversation_type.is_some_and(member_nicknames_apply);
    let chain = [
        (names.alias, NameSource::Alias, true),
        (names.member_nickname, NameSource::MemberNickname, in_group),
        (names.profile_name, NameSource::Profile, true),
        (names.snapshot, NameSource::Snapshot, true),
    ];
    for (candidate, source, applies) in chain {
        if applies && let Some(name) = real_name(candidate, &own) {
            return DisplayName::named(name, source);
        }
    }
    DisplayName::unnamed(NameEntity::Person, user_id)
}

/// The facts a conversation title is decided from.
#[derive(Clone, Copy, Debug)]
pub struct ConversationTitleInput<'a> {
    pub conversation_id: &'a str,
    pub conversation_type: ConversationType,
    /// The peer of a single chat; the group or channel id otherwise.
    pub channel_id: &'a str,
    /// The current user's private label for the conversation.
    pub remark: &'a str,
    /// The conversation's business name.
    pub display_name: &'a str,
    pub current_user_id: &'a str,
    /// Member ids in server order (the summary's member preview).
    pub member_ids: &'a [&'a str],
    /// How many members the conversation has, the current user included; 0 when
    /// not known.
    pub members_count: u32,
}

/// The title of a conversation: its remark; then for a single chat the peer's name
/// ([`resolve_person_name`] outside any conversation), for any other type its
/// business name; then for an unnamed group up to [`MAX_TITLE_MEMBER_NAMES`] of its
/// named members other than the current user, in server order; then unnamed.
///
/// `names` gives what the caller knows of a member or the peer.
pub fn resolve_conversation_title<'n>(
    input: &ConversationTitleInput<'_>,
    names: impl Fn(&str) -> PersonNameCandidates<'n>,
) -> DisplayName {
    let own = [input.channel_id, input.conversation_id];
    if let Some(remark) = real_name(input.remark, &own) {
        return DisplayName::named(remark, NameSource::Remark);
    }
    if input.conversation_type == ConversationType::Single {
        return match single_chat_peer(input) {
            Some(peer) => resolve_person_name(peer, &names(peer), None),
            None => DisplayName::unnamed(NameEntity::Person, ""),
        };
    }
    if let Some(name) = real_name(input.display_name, &own) {
        return DisplayName::named(name, NameSource::Name);
    }
    if input.conversation_type == ConversationType::Group {
        let me = input.current_user_id.trim();
        let mut seen: Vec<&str> = Vec::new();
        let mut member_names = Vec::new();
        for id in input.member_ids.iter().map(|id| id.trim()) {
            if id.is_empty() || seen.contains(&id) {
                continue;
            }
            seen.push(id);
            if id == me || member_names.len() == MAX_TITLE_MEMBER_NAMES {
                continue;
            }
            let name = resolve_person_name(id, &names(id), Some(ConversationType::Group));
            if let DisplayName::Named { text, .. } = name {
                member_names.push(text);
            }
        }
        if !member_names.is_empty() {
            let known = u32::try_from(seen.len()).unwrap_or(u32::MAX);
            return DisplayName::Members {
                names: member_names,
                total: input.members_count.max(known),
            };
        }
    }
    DisplayName::unnamed(NameEntity::of_conversation(input.conversation_type), "")
}

/// The peer of a single chat: its channel id; without one, the first member that is
/// not the current user; failing that the current user (a chat with oneself).
fn single_chat_peer<'a>(input: &ConversationTitleInput<'a>) -> Option<&'a str> {
    let channel = input.channel_id.trim();
    if !channel.is_empty() {
        return Some(channel);
    }
    let me = input.current_user_id.trim();
    let members = input
        .member_ids
        .iter()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty());
    let mut first = None;
    for id in members {
        if id != me {
            return Some(id);
        }
        first.get_or_insert(id);
    }
    first
}

/// Whether the title or last-sender name of `conversation` is drawn from `user_id`'s
/// names: a single chat with them, a conversation whose last message they sent, or
/// an unnamed group with them in its member preview. A name change of that person
/// (profile, alias) changes what such a conversation shows.
pub fn conversation_shows_person(conversation: &crate::model::Conversation, user_id: &str) -> bool {
    let user_id = user_id.trim();
    if user_id.is_empty() {
        return false;
    }
    if conversation.conversation_type == ConversationType::Single
        && conversation.channel_id.trim() == user_id
    {
        return true;
    }
    if conversation
        .last_sender_id
        .as_deref()
        .is_some_and(|sender| sender.trim() == user_id)
    {
        return true;
    }
    conversation.conversation_type == ConversationType::Group
        && group_is_titled_by_members(conversation)
        && conversation
            .member_preview
            .iter()
            .any(|member| member.user_id.trim() == user_id)
}

/// Whether the conversation's name column holds one of its own ids instead of a name
/// (what local stores written before names were projected may still contain).
pub fn stored_name_is_an_id(conversation: &crate::model::Conversation) -> bool {
    !conversation.display_name.trim().is_empty()
        && real_name(
            &conversation.display_name,
            &[&conversation.channel_id, &conversation.conversation_id],
        )
        .is_none()
}

/// Whether a group has neither a remark nor a business name, so its members title it.
pub fn group_is_titled_by_members(conversation: &crate::model::Conversation) -> bool {
    let own = [
        conversation.channel_id.as_str(),
        conversation.conversation_id.as_str(),
    ];
    real_name(conversation.remark.as_deref().unwrap_or_default(), &own).is_none()
        && real_name(&conversation.display_name, &own).is_none()
}

/// The name an account has inside one conversation, as a business SDK tells the
/// core (a group nickname). An empty `nickname` clears it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParticipantDisplaySnapshot {
    pub conversation_id: String,
    pub user_id: String,
    pub nickname: String,
}

impl ParticipantDisplaySnapshot {
    pub fn new(
        conversation_id: impl Into<String>,
        user_id: impl Into<String>,
        nickname: impl Into<String>,
    ) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            user_id: user_id.into(),
            nickname: nickname.into(),
        }
    }

    /// The snapshot with ids and nickname trimmed and a nickname equal to the user
    /// id dropped; `None` when an id is missing.
    pub fn normalized(&self) -> Option<Self> {
        let conversation_id = self.conversation_id.trim();
        let user_id = self.user_id.trim();
        if conversation_id.is_empty() || user_id.is_empty() {
            return None;
        }
        Some(Self {
            conversation_id: conversation_id.to_owned(),
            user_id: user_id.to_owned(),
            nickname: business_name(&self.nickname, &[user_id]),
        })
    }
}

#[cfg(test)]
mod tests;
