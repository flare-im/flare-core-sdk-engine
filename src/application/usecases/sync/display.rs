use crate::domain::business_name;
use crate::model::{Conversation, conversation::ConversationType};

/// A remote Core summary without a group business name carries no profile
/// change. Explicit business profile writes still use the ordinary store path,
/// so renaming a group to an empty name remains possible.
pub(super) fn retain_projected_group_name(
    incoming: &mut Conversation,
    local: Option<&Conversation>,
) {
    if incoming.conversation_type != ConversationType::Group {
        return;
    }
    let own = [&incoming.channel_id[..], &incoming.conversation_id[..]];
    if !business_name(&incoming.display_name, &own).is_empty() {
        return;
    }
    if let Some(local) = local.filter(|local| {
        local.conversation_type == ConversationType::Group
            && local.channel_id == incoming.channel_id
            && local.conversation_id == incoming.conversation_id
    }) {
        incoming.display_name = business_name(&local.display_name, &own);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ConversationReader, ConversationWriter};
    use crate::storage::MemoryConversationStore;

    fn group(name: &str) -> Conversation {
        Conversation {
            conversation_id: "2A_g1".into(),
            channel_id: "g1".into(),
            conversation_type: ConversationType::Group,
            display_name: name.into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn summary_does_not_erase_group_profile_but_explicit_clear_still_works() {
        let store = MemoryConversationStore::new();
        store.save_one(&group("IM 验收群")).await.unwrap();
        let local = store.get("2A_g1").await.unwrap();
        let mut incoming = group("");
        retain_projected_group_name(&mut incoming, local.as_ref());
        store.save_one(&incoming).await.unwrap();
        assert_eq!(
            store.get("2A_g1").await.unwrap().unwrap().display_name,
            "IM 验收群"
        );
        store.save_one(&group("")).await.unwrap();
        assert_eq!(store.get("2A_g1").await.unwrap().unwrap().display_name, "");
    }

    #[test]
    fn accepts_remote_rename_and_never_reuses_an_id_or_another_groups_name() {
        let local = group("IM 验收群");
        let mut renamed = group("新群名");
        retain_projected_group_name(&mut renamed, Some(&local));
        assert_eq!(renamed.display_name, "新群名");
        let mut id_named = group("g1");
        retain_projected_group_name(&mut id_named, Some(&group("2A_g1")));
        assert_eq!(id_named.display_name, "");
        let mut other = group("");
        other.channel_id = "g2".into();
        retain_projected_group_name(&mut other, Some(&local));
        assert_eq!(other.display_name, "");
    }
}
