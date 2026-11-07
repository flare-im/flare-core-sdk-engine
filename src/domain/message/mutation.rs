use crate::domain::{MessageActor, MessageTransportAction, ResolvedMessage};
use crate::model::message::{MarkType, ReactionAction};
use crate::shared::error::{ErrorCode, FlareError, Result};

pub(crate) const DELETE_TYPE_SOFT: i32 = 1;
pub(crate) const DELETE_SCOPE_USER_PRIVATE: i32 = 1;
pub(crate) const DELETE_SCOPE_CONVERSATION_GLOBAL: i32 = 2;
pub(crate) const MESSAGE_PIN_SCOPE_CONVERSATION: i32 = 0;
pub(crate) const MESSAGE_PIN_SCOPE_SELF: i32 = 1;

pub(crate) fn normalize_message_pin_scope(scope: i32) -> i32 {
    if scope == MESSAGE_PIN_SCOPE_SELF {
        MESSAGE_PIN_SCOPE_SELF
    } else {
        MESSAGE_PIN_SCOPE_CONVERSATION
    }
}

#[derive(Debug, Clone)]
pub enum MessageLocalUpdate {
    None,
    UpdateContent {
        message_id: String,
        new_content: Vec<u8>,
    },
    SetPinned {
        message_id: String,
        pinned: bool,
    },
    Delete {
        message_id: String,
    },
}

#[derive(Debug, Clone)]
pub struct MessageMutationPlan {
    pub transport_action: MessageTransportAction,
    pub local_update: MessageLocalUpdate,
}

impl MessageMutationPlan {
    pub fn conversation_id(&self) -> &str {
        self.transport_action.conversation_id()
    }

    pub fn server_msg_id(&self) -> Option<&str> {
        self.transport_action.server_msg_id()
    }
}

pub struct MessageMutationService;

impl MessageMutationService {
    pub fn plan_recall(
        &self,
        actor: &MessageActor,
        target: &ResolvedMessage,
    ) -> Result<MessageMutationPlan> {
        if target.sender_id() != actor.user_id {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "sdk.message.recall.not_allowed",
            ));
        }
        Ok(MessageMutationPlan {
            transport_action: MessageTransportAction::Recall {
                conversation_id: target.conversation_id().to_string(),
                server_msg_id: target.server_id().to_string(),
            },
            local_update: MessageLocalUpdate::None,
        })
    }

    pub fn plan_edit(
        &self,
        actor: &MessageActor,
        conversation_id: &str,
        target: &ResolvedMessage,
        new_content: Vec<u8>,
    ) -> Result<MessageMutationPlan> {
        if target.sender_id() != actor.user_id {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "sdk.message.edit.not_allowed",
            ));
        }
        Ok(MessageMutationPlan {
            transport_action: MessageTransportAction::Edit {
                conversation_id: conversation_id.to_string(),
                server_msg_id: target.server_id().to_string(),
                new_content: new_content.clone(),
                reason: String::new(),
                show_edited_mark: true,
            },
            local_update: MessageLocalUpdate::UpdateContent {
                message_id: target.server_id().to_string(),
                new_content,
            },
        })
    }

    pub fn plan_delete_for_self(
        &self,
        actor: &MessageActor,
        target: &ResolvedMessage,
        reason: Option<String>,
    ) -> MessageMutationPlan {
        MessageMutationPlan {
            transport_action: MessageTransportAction::Delete {
                conversation_id: target.conversation_id().to_string(),
                server_msg_id: target.server_id().to_string(),
                delete_type: DELETE_TYPE_SOFT,
                scope: DELETE_SCOPE_USER_PRIVATE,
                reason,
                notify_others: false,
                target_user_id: Some(actor.user_id.clone()),
            },
            local_update: MessageLocalUpdate::Delete {
                message_id: target.server_id().to_string(),
            },
        }
    }

    pub fn plan_delete_for_everyone(
        &self,
        actor: &MessageActor,
        target: &ResolvedMessage,
        reason: Option<String>,
    ) -> Result<MessageMutationPlan> {
        if target.sender_id() != actor.user_id {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "sdk.message.delete.for_everyone.not_allowed",
            ));
        }
        Ok(MessageMutationPlan {
            transport_action: MessageTransportAction::Delete {
                conversation_id: target.conversation_id().to_string(),
                server_msg_id: target.server_id().to_string(),
                delete_type: DELETE_TYPE_SOFT,
                scope: DELETE_SCOPE_CONVERSATION_GLOBAL,
                reason,
                notify_others: true,
                target_user_id: None,
            },
            local_update: MessageLocalUpdate::Delete {
                message_id: target.server_id().to_string(),
            },
        })
    }

    pub fn plan_reaction(
        &self,
        actor: &MessageActor,
        conversation_id: &str,
        server_msg_id: &str,
        emoji: &str,
        action: ReactionAction,
    ) -> MessageMutationPlan {
        MessageMutationPlan {
            transport_action: MessageTransportAction::Reaction {
                conversation_id: conversation_id.to_string(),
                server_msg_id: server_msg_id.to_string(),
                user_id: actor.user_id.clone(),
                emoji: emoji.to_string(),
                action: action as i32,
            },
            local_update: MessageLocalUpdate::None,
        }
    }

    pub fn plan_pin(
        &self,
        actor: &MessageActor,
        conversation_id: &str,
        server_msg_id: &str,
        scope: i32,
    ) -> MessageMutationPlan {
        let scope = normalize_message_pin_scope(scope);
        MessageMutationPlan {
            transport_action: MessageTransportAction::Pin {
                conversation_id: conversation_id.to_string(),
                server_msg_id: server_msg_id.to_string(),
                pinned_by: actor.user_id.clone(),
                scope,
            },
            local_update: MessageLocalUpdate::SetPinned {
                message_id: server_msg_id.to_string(),
                pinned: true,
            },
        }
    }

    pub fn plan_unpin(
        &self,
        actor: &MessageActor,
        conversation_id: &str,
        server_msg_id: &str,
        scope: i32,
    ) -> MessageMutationPlan {
        let scope = normalize_message_pin_scope(scope);
        MessageMutationPlan {
            transport_action: MessageTransportAction::Unpin {
                conversation_id: conversation_id.to_string(),
                server_msg_id: server_msg_id.to_string(),
                unpinned_by: actor.user_id.clone(),
                scope,
            },
            local_update: MessageLocalUpdate::SetPinned {
                message_id: server_msg_id.to_string(),
                pinned: false,
            },
        }
    }

    pub fn plan_mark(
        &self,
        actor: &MessageActor,
        conversation_id: &str,
        server_msg_id: &str,
        mark_type: MarkType,
        color: &str,
    ) -> MessageMutationPlan {
        MessageMutationPlan {
            transport_action: MessageTransportAction::Mark {
                conversation_id: conversation_id.to_string(),
                server_msg_id: server_msg_id.to_string(),
                user_id: actor.user_id.clone(),
                mark_type: mark_type as i32,
                color: color.to_string(),
            },
            local_update: MessageLocalUpdate::None,
        }
    }

    pub fn plan_unmark(
        &self,
        actor: &MessageActor,
        conversation_id: &str,
        server_msg_id: &str,
        mark_type: MarkType,
    ) -> MessageMutationPlan {
        MessageMutationPlan {
            transport_action: MessageTransportAction::Unmark {
                conversation_id: conversation_id.to_string(),
                server_msg_id: server_msg_id.to_string(),
                user_id: actor.user_id.clone(),
                mark_type: mark_type as i32,
            },
            local_update: MessageLocalUpdate::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageLocalUpdate, MessageMutationService};
    use crate::domain::{MessageActor, MessageTransportAction, ResolvedMessage};
    use crate::model::IMMessage;
    use crate::model::message::{MarkType, ReactionAction};
    use flare_proto::common::Message;

    fn resolved_message(sender_id: &str) -> ResolvedMessage {
        let mut message = IMMessage::new(Message::default());
        message.conversation_id = "c1".to_string();
        message.server_id = "s1".to_string();
        message.sender_id = sender_id.to_string();
        ResolvedMessage::new(message)
    }

    #[test]
    fn edit_produces_edit_transport_and_local_content_update() {
        let service = MessageMutationService;
        let new_content = vec![1, 2, 3];

        let actor = MessageActor::require("u1".to_string()).unwrap();
        let plan = service
            .plan_edit(&actor, "c1", &resolved_message("u1"), new_content.clone())
            .unwrap();

        match plan.transport_action {
            MessageTransportAction::Edit {
                conversation_id,
                server_msg_id,
                new_content: transport_content,
                show_edited_mark,
                ..
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert_eq!(transport_content, new_content);
                assert!(show_edited_mark);
            }
            _ => panic!("expected edit transport action"),
        }

        match plan.local_update {
            MessageLocalUpdate::UpdateContent {
                message_id,
                new_content: local_content,
            } => {
                assert_eq!(message_id, "s1");
                assert_eq!(local_content, vec![1, 2, 3]);
            }
            _ => panic!("expected local content update"),
        }
    }

    #[test]
    fn reaction_produces_reaction_transport_without_local_shadow_update() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();

        let plan = service.plan_reaction(&actor, "c1", "s1", "👍", ReactionAction::Add);

        match plan.transport_action {
            MessageTransportAction::Reaction {
                conversation_id,
                server_msg_id,
                user_id,
                emoji,
                action,
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert_eq!(user_id, "u1");
                assert_eq!(emoji, "👍");
                assert_eq!(action, ReactionAction::Add as i32);
            }
            _ => panic!("expected reaction transport action"),
        }
        assert!(matches!(plan.local_update, MessageLocalUpdate::None));
    }

    #[test]
    fn mark_and_unmark_produce_typed_transport_actions_without_local_shadow_update() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();

        let mark = service.plan_mark(&actor, "c1", "s1", MarkType::Todo, "#7c3aed");
        match mark.transport_action {
            MessageTransportAction::Mark {
                conversation_id,
                server_msg_id,
                user_id,
                mark_type,
                color,
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert_eq!(user_id, "u1");
                assert_eq!(mark_type, MarkType::Todo as i32);
                assert_eq!(color, "#7c3aed");
            }
            _ => panic!("expected mark transport action"),
        }
        assert!(matches!(mark.local_update, MessageLocalUpdate::None));

        let unmark = service.plan_unmark(&actor, "c1", "s1", MarkType::Todo);
        match unmark.transport_action {
            MessageTransportAction::Unmark {
                conversation_id,
                server_msg_id,
                user_id,
                mark_type,
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert_eq!(user_id, "u1");
                assert_eq!(mark_type, MarkType::Todo as i32);
            }
            _ => panic!("expected unmark transport action"),
        }
        assert!(matches!(unmark.local_update, MessageLocalUpdate::None));
    }

    #[test]
    fn delete_for_everyone_requires_sender_match_actor() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();
        let mut message = IMMessage::new(Message::default());
        message.conversation_id = "c1".to_string();
        message.server_id = "s1".to_string();
        message.sender_id = "u2".to_string();

        let result = service.plan_delete_for_everyone(&actor, &ResolvedMessage::new(message), None);

        assert!(result.is_err());
    }

    #[test]
    fn delete_for_self_produces_delete_transport_and_local_delete() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();

        let plan =
            service.plan_delete_for_self(&actor, &resolved_message("u1"), Some("r".to_string()));

        match plan.transport_action {
            MessageTransportAction::Delete {
                conversation_id,
                server_msg_id,
                notify_others,
                ..
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert!(!notify_others);
            }
            _ => panic!("expected delete transport action"),
        }

        match plan.local_update {
            MessageLocalUpdate::Delete { message_id } => assert_eq!(message_id, "s1"),
            _ => panic!("expected local delete update"),
        }
    }

    #[test]
    fn pin_produces_pin_transport_and_local_flag_update() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();

        let plan = service.plan_pin(&actor, "c1", "s1", super::MESSAGE_PIN_SCOPE_SELF);

        match plan.transport_action {
            MessageTransportAction::Pin {
                conversation_id,
                server_msg_id,
                pinned_by,
                scope,
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert_eq!(pinned_by, "u1");
                assert_eq!(scope, super::MESSAGE_PIN_SCOPE_SELF);
            }
            _ => panic!("expected pin transport action"),
        }

        match plan.local_update {
            MessageLocalUpdate::SetPinned { message_id, pinned } => {
                assert_eq!(message_id, "s1");
                assert!(pinned);
            }
            _ => panic!("expected local pinned update"),
        }
    }

    #[test]
    fn unpin_produces_unpin_transport_and_local_flag_update() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();

        let plan = service.plan_unpin(&actor, "c1", "s1", super::MESSAGE_PIN_SCOPE_SELF);

        match plan.transport_action {
            MessageTransportAction::Unpin {
                conversation_id,
                server_msg_id,
                unpinned_by,
                scope,
            } => {
                assert_eq!(conversation_id, "c1");
                assert_eq!(server_msg_id, "s1");
                assert_eq!(unpinned_by, "u1");
                assert_eq!(scope, super::MESSAGE_PIN_SCOPE_SELF);
            }
            _ => panic!("expected unpin transport action"),
        }

        match plan.local_update {
            MessageLocalUpdate::SetPinned { message_id, pinned } => {
                assert_eq!(message_id, "s1");
                assert!(!pinned);
            }
            _ => panic!("expected local unpinned update"),
        }
    }

    #[test]
    fn pin_scope_defaults_to_conversation_scope_when_invalid() {
        let service = MessageMutationService;
        let actor = MessageActor::require("u1".to_string()).unwrap();

        let plan = service.plan_pin(&actor, "c1", "s1", 99);

        match plan.transport_action {
            MessageTransportAction::Pin { scope, .. } => {
                assert_eq!(scope, super::MESSAGE_PIN_SCOPE_CONVERSATION);
            }
            _ => panic!("expected pin transport action"),
        }
    }
}
