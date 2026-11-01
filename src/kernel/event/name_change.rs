//! The inputs of name projection changed.
//!
//! The core projects the names of conversation titles and message senders when it reads them,
//! from the identity cache (profile name, alias) and the member nicknames of conversations. When
//! those inputs are written, the open views reread the rows that show the people concerned. The
//! notice only travels inside the process to the views: it is not an [`super::SdkEvent`], so it
//! never reaches the bindings or the host.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tokio::sync::Notify;

/// Whose names may read differently now.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NameChange {
    /// People whose identity-cache entry (profile name, alias, avatar) was written.
    pub users: BTreeSet<String>,
    /// `(conversation id, user id)` pairs whose member nickname changed.
    pub member_nicknames: BTreeSet<(String, String)>,
}

impl NameChange {
    /// The identity-cache entries of these people were written.
    pub fn users<I, S>(user_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            users: user_ids
                .into_iter()
                .map(|id| id.as_ref().trim().to_string())
                .filter(|id| !id.is_empty())
                .collect(),
            member_nicknames: BTreeSet::new(),
        }
    }

    /// What these people are called inside these conversations changed.
    pub fn member_nicknames<I, C, U>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (C, U)>,
        C: AsRef<str>,
        U: AsRef<str>,
    {
        Self {
            users: BTreeSet::new(),
            member_nicknames: pairs
                .into_iter()
                .map(|(conversation_id, user_id)| {
                    (
                        conversation_id.as_ref().trim().to_string(),
                        user_id.as_ref().trim().to_string(),
                    )
                })
                .filter(|(conversation_id, user_id)| {
                    !conversation_id.is_empty() && !user_id.is_empty()
                })
                .collect(),
        }
    }

    /// The member nicknames a batch wrote, keeping only the conversations whose stored
    /// nicknames changed (what the store reports back).
    pub fn written_member_nicknames(
        snapshots: &[crate::domain::ParticipantDisplaySnapshot],
        changed_conversation_ids: &[String],
    ) -> Self {
        Self::member_nicknames(
            snapshots
                .iter()
                .filter(|snapshot| {
                    changed_conversation_ids
                        .iter()
                        .any(|id| id.trim() == snapshot.conversation_id.trim())
                })
                .map(|snapshot| (&snapshot.conversation_id, &snapshot.user_id)),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty() && self.member_nicknames.is_empty()
    }

    pub fn merge(&mut self, other: NameChange) {
        self.users.extend(other.users);
        self.member_nicknames.extend(other.member_nicknames);
    }

    /// Whether the name of `sender_id` in `conversation_id` may read differently now.
    pub fn renames_sender(&self, conversation_id: &str, sender_id: &str) -> bool {
        let sender_id = sender_id.trim();
        if sender_id.is_empty() {
            return false;
        }
        self.users.contains(sender_id)
            || self.member_nicknames.iter().any(|(conversation, user)| {
                conversation == conversation_id.trim() && user == sender_id
            })
    }

    /// The people of this change whose names `conversation_id` may show: everyone whose
    /// identity-cache entry changed, and those whose nickname changed in this conversation.
    pub fn people_in<'a>(&'a self, conversation_id: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        let conversation_id = conversation_id.trim();
        self.users.iter().map(String::as_str).chain(
            self.member_nicknames
                .iter()
                .filter(move |(conversation, _)| conversation == conversation_id)
                .map(|(_, user)| user.as_str()),
        )
    }
}

/// One listener's pending changes: everything published since it last took them, merged into
/// one change, so a burst of writes costs one slot and nothing is ever dropped.
#[derive(Default)]
struct NameChangeInbox {
    pending: Mutex<NameChange>,
    wake: Notify,
}

impl NameChangeInbox {
    fn pending(&self) -> MutexGuard<'_, NameChange> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The receiving end of one listener (the view refresh worker).
pub(crate) struct NameChangeReceiver {
    inbox: Arc<NameChangeInbox>,
}

impl NameChangeReceiver {
    /// Waits until names change, then takes everything that changed since the last take.
    pub async fn recv(&self) -> NameChange {
        loop {
            if let Some(change) = self.try_recv() {
                return change;
            }
            self.inbox.wake.notified().await;
        }
    }

    /// Takes what changed since the last take, if anything did.
    pub fn try_recv(&self) -> Option<NameChange> {
        let change = std::mem::take(&mut *self.inbox.pending());
        (!change.is_empty()).then_some(change)
    }
}

/// The in-process listeners of [`NameChange`] notices.
#[derive(Clone, Default)]
pub(crate) struct NameChangeListeners {
    inboxes: Arc<Mutex<Vec<Weak<NameChangeInbox>>>>,
}

impl NameChangeListeners {
    pub fn subscribe(&self) -> NameChangeReceiver {
        let inbox = Arc::new(NameChangeInbox::default());
        self.inboxes().push(Arc::downgrade(&inbox));
        NameChangeReceiver { inbox }
    }

    /// Merges the change into every listener's pending changes and wakes it; listeners that
    /// went away are dropped. An empty change is not handed out.
    pub fn publish(&self, change: NameChange) {
        if change.is_empty() {
            return;
        }
        self.inboxes().retain(|inbox| match inbox.upgrade() {
            Some(inbox) => {
                inbox.pending().merge(change.clone());
                inbox.wake.notify_one();
                true
            }
            None => false,
        });
    }

    fn inboxes(&self) -> MutexGuard<'_, Vec<Weak<NameChangeInbox>>> {
        self.inboxes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::{NameChange, NameChangeListeners};
    use crate::domain::ParticipantDisplaySnapshot;

    #[test]
    fn a_change_names_its_senders_and_member_nicknames_only_in_their_conversation() {
        let mut change = NameChange::users([" u1 ", ""]);
        change.merge(NameChange::member_nicknames([("g1", "u2")]));
        assert!(change.renames_sender("any", "u1"));
        assert!(change.renames_sender("g1", " u2"));
        assert!(
            !change.renames_sender("g2", "u2"),
            "a nickname is per conversation"
        );
        assert!(!change.renames_sender("g1", ""));
        assert_eq!(change.people_in("g1").collect::<Vec<_>>(), vec!["u1", "u2"]);
        assert_eq!(change.people_in("g2").collect::<Vec<_>>(), vec!["u1"]);
    }

    #[test]
    fn only_the_nicknames_of_changed_conversations_are_noticed() {
        let snapshots = [
            ParticipantDisplaySnapshot::new("g1", "u1", "Ann"),
            ParticipantDisplaySnapshot::new("g2", "u1", "Ann"),
        ];
        let change = NameChange::written_member_nicknames(&snapshots, &["g2".to_string()]);
        assert_eq!(
            change.member_nicknames.into_iter().collect::<Vec<_>>(),
            vec![("g2".to_string(), "u1".to_string())]
        );
    }

    #[tokio::test]
    async fn listeners_take_merged_changes_and_empty_changes_are_not_handed_out() {
        let listeners = NameChangeListeners::default();
        let rx = listeners.subscribe();
        let dropped = listeners.subscribe();
        drop(dropped);
        listeners.publish(NameChange::default());
        assert!(rx.try_recv().is_none(), "an empty change is not handed out");
        listeners.publish(NameChange::users(["u1"]));
        listeners.publish(NameChange::member_nicknames([("g1", "u2")]));
        let mut expected = NameChange::users(["u1"]);
        expected.merge(NameChange::member_nicknames([("g1", "u2")]));
        assert_eq!(rx.recv().await, expected, "a burst is one merged change");
        assert!(rx.try_recv().is_none());
        assert_eq!(listeners.inboxes().len(), 1, "the dropped listener is gone");

        // A change published while nobody waits still wakes the next wait.
        listeners.publish(NameChange::users(["u3"]));
        let woken = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await;
        assert_eq!(woken.unwrap(), NameChange::users(["u3"]));
    }
}
