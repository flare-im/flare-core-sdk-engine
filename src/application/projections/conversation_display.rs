//! Conversation display projection.
//!
//! Business SDKs provide typed snapshots of a conversation's business name, avatar and
//! remark. The name column only stores a business name; the title a host renders is
//! projected when the conversation is read (`Conversation.title`).

use crate::infrastructure::persistence::StoreProvider;
use crate::model::Conversation;
use crate::model::conversation::ConversationType;
use crate::shared::error::Result;

#[derive(Clone, Debug)]
pub struct ConversationDisplaySnapshot {
    pub conversation_id: String,
    pub conversation_type: ConversationType,
    pub business_type: String,
    pub channel_id: String,
    pub display_name: String,
    pub avatar_url: String,
    /// `None` means leave remark unchanged; `Some("")` clears remark.
    pub remark: Option<String>,
    pub members_count: Option<u32>,
    pub create_if_missing: bool,
}

impl ConversationDisplaySnapshot {
    pub fn new(
        conversation_id: impl Into<String>,
        conversation_type: ConversationType,
        channel_id: impl Into<String>,
    ) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            conversation_type,
            business_type: String::new(),
            channel_id: channel_id.into(),
            display_name: String::new(),
            avatar_url: String::new(),
            remark: None,
            members_count: None,
            create_if_missing: false,
        }
    }
}

pub struct ConversationDisplayProjectionApplier;

impl ConversationDisplayProjectionApplier {
    pub async fn apply(
        stores: &StoreProvider,
        snapshot: ConversationDisplaySnapshot,
    ) -> Result<Option<String>> {
        let conversation_id = snapshot.conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(None);
        }

        let existing = stores.conversations.get(conversation_id).await?;
        if existing.is_none() && !snapshot.create_if_missing {
            return Ok(None);
        }

        let mut conversation = existing
            .clone()
            .unwrap_or_else(|| Conversation::from_conversation_id(conversation_id.to_string()));
        let before = conversation.clone();

        if conversation.conversation_type == ConversationType::Unspecified {
            conversation.conversation_type = snapshot.conversation_type;
        }
        if !snapshot.business_type.trim().is_empty() {
            conversation.business_type = snapshot.business_type.trim().to_string();
        } else if conversation.business_type.trim().is_empty()
            && snapshot.conversation_type != ConversationType::Unspecified
        {
            conversation.business_type = snapshot.conversation_type.as_str().to_string();
        }
        if !snapshot.channel_id.trim().is_empty() {
            conversation.channel_id = snapshot.channel_id.trim().to_string();
        }
        if let Some(count) = snapshot.members_count {
            conversation.members_count = count;
        }

        let peer_profile = if conversation.conversation_type.is_single_chat_conversation()
            && !conversation.channel_id.trim().is_empty()
        {
            stores
                .user_profiles_or_memory()
                .get(conversation.channel_id.trim())
                .await?
        } else {
            None
        };

        if let Some(remark) = snapshot.remark.as_ref() {
            conversation.remark = normalize_text(remark);
        }
        if let Some(avatar) = normalize_text(&snapshot.avatar_url) {
            conversation.avatar_url = avatar;
        } else if conversation.avatar_url.trim().is_empty()
            && let Some(profile) = peer_profile.as_ref()
            && !profile.avatar_url.trim().is_empty()
        {
            conversation.avatar_url = profile.avatar_url.trim().to_string();
        }

        // 名字列只存快照给的业务名：单聊的名字来自对端人名（读取时投影），不从资料取名写进来；
        // 备注另存在 `remark`；没有新名字时保留已存的业务名。
        let primary_name = normalize_text(&snapshot.display_name)
            .or_else(|| normalize_text(&conversation.display_name));
        conversation.display_name = resolve_display_name(
            primary_name.as_deref(),
            &conversation.channel_id,
            &conversation.conversation_id,
        );

        let now = crate::shared::util::id::now_millis();
        if conversation.created_at == 0 {
            conversation.created_at = now;
        }
        if conversation.updated_at == 0 || conversation_display_changed(&before, &conversation) {
            conversation.updated_at = now;
        }

        if existing.is_none() || conversation_display_changed(&before, &conversation) {
            stores.conversations.save_one(&conversation).await?;
            return Ok(Some(conversation.conversation_id));
        }
        Ok(None)
    }
}

fn conversation_display_changed(left: &Conversation, right: &Conversation) -> bool {
    left.conversation_type != right.conversation_type
        || left.business_type != right.business_type
        || left.channel_id != right.channel_id
        || left.display_name != right.display_name
        || left.avatar_url != right.avatar_url
        || left.remark != right.remark
        || left.members_count != right.members_count
}

fn normalize_text(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// 名字列存什么：业务名本身；它等于对端、群或会话自己的 id 时不是名字，存空串。
///
/// 不回落到 id，也不写占位文案：没有名字时由读取时的投影给出结构化的「无名」
/// （[`crate::domain::resolve_conversation_title`]），文案在 UI Kit。
pub fn resolve_display_name(
    primary_name: Option<&str>,
    channel_id: &str,
    conversation_id: &str,
) -> String {
    crate::domain::business_name(
        primary_name.unwrap_or_default(),
        &[channel_id, conversation_id],
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ConversationDisplayProjectionApplier, ConversationDisplaySnapshot, resolve_display_name,
    };
    use crate::domain::{ConversationReader, ConversationWriter, UserProfile, UserWriter};
    use crate::infrastructure::persistence::in_memory_im_provider;
    use crate::model::Conversation;
    use crate::model::conversation::ConversationType;

    #[test]
    fn the_business_name_is_stored_as_given() {
        assert_eq!(
            resolve_display_name(Some(" 研发群 "), "g1", "cid"),
            "研发群"
        );
    }

    #[test]
    fn without_a_name_nothing_is_stored() {
        assert_eq!(resolve_display_name(None, "u2", "cid"), "");
        assert_eq!(resolve_display_name(Some("  "), "u2", "cid"), "");
        assert_eq!(resolve_display_name(None, "", ""), "");
    }

    #[test]
    fn an_id_is_never_stored_as_a_name() {
        assert_eq!(resolve_display_name(Some("u2"), "u2", "cid"), "");
        assert_eq!(resolve_display_name(Some("cid"), "u2", "cid"), "");
    }

    /// 单聊的名字来自对端人名的读取时投影：资料名、备注都不写进名字列。
    #[tokio::test]
    async fn single_chat_snapshot_stores_neither_the_profile_name_nor_the_remark() {
        let stores = in_memory_im_provider();
        stores
            .user_profiles_writer_or_memory()
            .save_batch(&[UserProfile::new("u2").with_nickname("Bob")])
            .await
            .unwrap();
        let mut snapshot =
            ConversationDisplaySnapshot::new("1A_cid", ConversationType::Single, "u2");
        snapshot.remark = Some("好友备注".to_string());
        snapshot.create_if_missing = true;
        ConversationDisplayProjectionApplier::apply(&stores, snapshot)
            .await
            .unwrap();
        let stored = stores.conversations.get("1A_cid").await.unwrap().unwrap();
        assert_eq!(stored.display_name, "");
        assert_eq!(stored.remark.as_deref(), Some("好友备注"));
    }

    #[tokio::test]
    async fn a_snapshot_name_equal_to_the_channel_id_is_not_stored() {
        let stores = in_memory_im_provider();
        let mut snapshot =
            ConversationDisplaySnapshot::new("2A_cid", ConversationType::Group, "g1");
        snapshot.display_name = "g1".to_string();
        snapshot.create_if_missing = true;
        ConversationDisplayProjectionApplier::apply(&stores, snapshot)
            .await
            .unwrap();
        let stored = stores.conversations.get("2A_cid").await.unwrap().unwrap();
        assert_eq!(stored.display_name, "");
    }

    #[tokio::test]
    async fn an_empty_snapshot_name_keeps_the_stored_business_name() {
        let stores = in_memory_im_provider();
        stores
            .conversations
            .save_one(&Conversation {
                conversation_id: "2A_cid".to_string(),
                conversation_type: ConversationType::Group,
                channel_id: "g1".to_string(),
                display_name: "研发群".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut snapshot =
            ConversationDisplaySnapshot::new("2A_cid", ConversationType::Group, "g1");
        snapshot.members_count = Some(9);
        ConversationDisplayProjectionApplier::apply(&stores, snapshot)
            .await
            .unwrap();
        let stored = stores.conversations.get("2A_cid").await.unwrap().unwrap();
        assert_eq!(stored.display_name, "研发群");
        assert_eq!(stored.members_count, 9);
    }
}
