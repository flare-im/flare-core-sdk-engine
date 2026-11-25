use crate::domain::{SeqRange, TimelineCoverage};
use crate::model::Conversation;
use crate::shared::error::{ErrorCode, FlareError, Result};

/// Business label for a locally materialized conversation shell.
pub fn projected_business_type(message: &crate::model::IMMessage) -> &str {
    let conv_type =
        crate::model::conversation::ConversationType::from_proto_int(message.conversation_type);
    if message.local_state.is_local && !message.channel_id.trim().is_empty() {
        message
            .attributes
            .get("business_type")
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| conv_type.as_str())
    } else if conv_type.is_single_chat_conversation() {
        "single"
    } else {
        "chat"
    }
}

pub const EXT_MESSAGE_PROJECTION: &str = "_local.messageProjectionRanges";

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageProjectionReceipt {
    pub ranges: Vec<SeqRange>,
    pub materialized_seq: u64,
    #[serde(default)]
    pub summary_seq: u64,
}

pub fn message_projection_receipt(conversation: &Conversation) -> Result<MessageProjectionReceipt> {
    let receipt: MessageProjectionReceipt = match conversation.ext.get(EXT_MESSAGE_PROJECTION) {
        Some(value) => serde_json::from_str(value)
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?,
        None => MessageProjectionReceipt::default(),
    };
    if receipt
        .ranges
        .iter()
        .any(|range| range.from == 0 || range.from > range.to)
    {
        return Err(FlareError::localized(
            ErrorCode::DatabaseError,
            "invalid message projection receipt",
        ));
    }
    Ok(receipt)
}

pub fn message_projection_coverage(conversation: &Conversation) -> Result<TimelineCoverage> {
    let mut coverage =
        TimelineCoverage::from_ranges(message_projection_receipt(conversation)?.ranges);
    let floor = conversation
        .last_read_seq
        .max(crate::domain::sync_visibility_floor(conversation));
    if let Some(range) = SeqRange::new(1, floor) {
        coverage.insert(range);
    }
    Ok(coverage)
}

/// Compare only the fields replaced by a message projection.
pub fn message_projection_matches(current: &Conversation, expected: &Conversation) -> bool {
    current.unread_count == expected.unread_count
        && current.last_read_seq == expected.last_read_seq
        && current.max_seq == expected.max_seq
        && current.mention_count == expected.mention_count
        && current.mention_me == expected.mention_me
        && current.last_message_id == expected.last_message_id
        && current.last_sender_id == expected.last_sender_id
        && current.last_message_at == expected.last_message_at
        && current.last_message_preview == expected.last_message_preview
        && current.ext == expected.ext
}

pub fn copy_message_projection(current: &mut Conversation, projected: &Conversation) {
    current.unread_count = projected.unread_count;
    current.last_read_seq = projected.last_read_seq;
    current.max_seq = projected.max_seq;
    current.mention_count = projected.mention_count;
    current.mention_me = projected.mention_me;
    current.last_message_id = projected.last_message_id.clone();
    current.last_sender_id = projected.last_sender_id.clone();
    current.last_message_at = projected.last_message_at;
    current.updated_at_ts = current.updated_at_ts.max(projected.last_message_at);
    current.last_message_preview = projected.last_message_preview.clone();
    current.last_message = projected.last_message.clone();
    current.ext = projected.ext.clone();
}

/// A server summary cannot supply or replace a local completion receipt.
pub fn preserve_message_projection(local: Option<&Conversation>, incoming: &mut Conversation) {
    incoming.ext.remove(EXT_MESSAGE_PROJECTION);
    if let Some(value) = local.and_then(|c| c.ext.get(EXT_MESSAGE_PROJECTION)) {
        let mut value = value.clone();
        if let Some(local) = local
            && (incoming.max_seq > local.max_seq || incoming.last_read_seq > local.last_read_seq)
            && let Ok(mut receipt) = message_projection_receipt(local)
        {
            receipt.summary_seq = receipt.summary_seq.max(
                incoming
                    .last_read_seq
                    .saturating_add(incoming.unread_count as u64),
            );
            value = serde_json::to_string(&receipt).unwrap_or(value);
        }
        incoming.ext.insert(EXT_MESSAGE_PROJECTION.into(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrupt_receipts_fail_instead_of_treating_projected_messages_as_new() {
        for value in [
            "bad json",
            r#"{"ranges":[{"from":0,"to":3}],"materializedSeq":3}"#,
            r#"{"ranges":[{"from":9,"to":3}],"materializedSeq":9}"#,
        ] {
            let mut conversation = Conversation::default();
            conversation
                .ext
                .insert(EXT_MESSAGE_PROJECTION.into(), value.into());
            assert!(message_projection_coverage(&conversation).is_err());
        }
    }

    #[test]
    fn remote_receipts_are_discarded_and_local_receipts_keep_their_gaps() {
        let mut local = Conversation::default();
        local.ext.insert(
            EXT_MESSAGE_PROJECTION.into(),
            r#"{"ranges":[{"from":1,"to":3},{"from":5,"to":5}],"materializedSeq":5}"#.into(),
        );
        let mut remote = Conversation::default();
        remote.ext.insert(
            EXT_MESSAGE_PROJECTION.into(),
            r#"[{"from":1,"to":999}]"#.into(),
        );
        preserve_message_projection(Some(&local), &mut remote);
        let coverage = message_projection_coverage(&remote).unwrap();
        assert!(coverage.range_containing(3).is_some());
        assert!(coverage.range_containing(4).is_none());
        preserve_message_projection(None, &mut remote);
        assert!(!remote.ext.contains_key(EXT_MESSAGE_PROJECTION));
    }
    #[test]
    fn a_same_watermark_summary_keeps_projected_mentions_until_the_read_position_moves() {
        let local = Conversation {
            max_seq: 5,
            unread_count: 1,
            mention_count: 1,
            mention_me: true,
            ..Default::default()
        };
        let mut remote = Conversation {
            max_seq: 5,
            ..Default::default()
        };
        let merged = crate::domain::merge_incoming_conversation_summary(Some(&local), &remote);
        assert_eq!(
            (merged.unread_count, merged.mention_count, merged.mention_me),
            (1, 1, true)
        );
        remote.last_read_seq = 5;
        let read = crate::domain::merge_incoming_conversation_summary(Some(&local), &remote);
        assert_eq!(
            (read.unread_count, read.mention_count, read.mention_me),
            (0, 0, false)
        );
    }

    #[test]
    fn summaries_preserve_projection_holes_without_inventing_a_read_position() {
        let mut local = Conversation {
            max_seq: 2,
            unread_count: 1,
            ..Default::default()
        };
        local.ext.insert(
            EXT_MESSAGE_PROJECTION.into(),
            r#"{"ranges":[{"from":2,"to":2}],"materializedSeq":2,"summarySeq":0}"#.into(),
        );
        let stale = Conversation {
            max_seq: 2,
            unread_count: 1,
            ..Default::default()
        };
        let merged = crate::domain::merge_incoming_conversation_summary(Some(&local), &stale);
        assert_eq!(merged.last_read_seq, 0);
        assert_eq!(message_projection_receipt(&merged).unwrap().summary_seq, 0);
        assert!(
            message_projection_coverage(&merged)
                .unwrap()
                .range_containing(1)
                .is_none()
        );

        let fresh = Conversation {
            max_seq: 3,
            unread_count: 3,
            ..Default::default()
        };
        let merged = crate::domain::merge_incoming_conversation_summary(Some(&local), &fresh);
        assert_eq!(message_projection_receipt(&merged).unwrap().summary_seq, 3);
        assert!(
            message_projection_coverage(&merged)
                .unwrap()
                .range_containing(1)
                .is_none(),
            "a count summary cannot prove that a message projection committed"
        );
    }
}
