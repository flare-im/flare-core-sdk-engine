use std::collections::{HashMap, HashSet};

use crate::model::IMMessage;

/// The IndexedDB adapter uses these in-memory indexes for its hydrated account.
/// Rows and indexes share the same store lock, including ACK and alias rewrites.
#[derive(Default)]
pub(super) struct MemoryMessageRows {
    rows: HashMap<String, IMMessage>,
    by_conversation: HashMap<String, HashSet<String>>,
    by_client_id: HashMap<String, HashSet<String>>,
}

impl MemoryMessageRows {
    pub(super) fn get(&self, key: &str) -> Option<&IMMessage> {
        self.rows.get(key)
    }

    pub(super) fn get_mut(&mut self, key: &str) -> Option<&mut IMMessage> {
        self.rows.get_mut(key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &IMMessage> {
        self.rows.values()
    }

    pub(super) fn conversation<'a>(
        &'a self,
        id: &str,
    ) -> impl Iterator<Item = &'a IMMessage> + use<'a> {
        self.by_conversation
            .get(id)
            .into_iter()
            .flatten()
            .filter_map(|key| self.rows.get(key))
    }

    pub(super) fn client_rows<'a>(
        &'a self,
        id: &str,
    ) -> impl Iterator<Item = (&'a str, &'a IMMessage)> + use<'a> {
        self.by_client_id
            .get(id)
            .into_iter()
            .flatten()
            .filter_map(|key| self.rows.get(key).map(|row| (key.as_str(), row)))
    }

    pub(super) fn insert(&mut self, key: String, row: IMMessage) {
        let previous = self.rows.insert(key.clone(), row);
        let row = &self.rows[&key];
        if previous
            .as_ref()
            .is_none_or(|old| old.conversation_id != row.conversation_id)
        {
            if let Some(old) = &previous {
                remove_index_key(&mut self.by_conversation, &old.conversation_id, &key);
            }
            self.by_conversation
                .entry(row.conversation_id.clone())
                .or_default()
                .insert(key.clone());
        }
        if previous
            .as_ref()
            .is_none_or(|old| old.client_msg_id != row.client_msg_id)
        {
            if let Some(old) = &previous {
                remove_index_key(&mut self.by_client_id, &old.client_msg_id, &key);
            }
            self.by_client_id
                .entry(row.client_msg_id.clone())
                .or_default()
                .insert(key);
        }
    }

    pub(super) fn remove(&mut self, key: &str) {
        if let Some(row) = self.rows.remove(key) {
            remove_index_key(&mut self.by_conversation, &row.conversation_id, key);
            remove_index_key(&mut self.by_client_id, &row.client_msg_id, key);
        }
    }

    pub(super) fn remove_client_rows(
        &mut self,
        id: &str,
        mut should_remove: impl FnMut(&str, &IMMessage) -> bool,
    ) {
        let keys: Vec<_> = self
            .client_rows(id)
            .filter(|(key, row)| should_remove(key, row))
            .map(|(key, _)| key.to_owned())
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }

    pub(super) fn for_each_in_conversation_mut(
        &mut self,
        id: &str,
        mut visit: impl FnMut(&mut IMMessage),
    ) {
        for key in self.by_conversation.get(id).into_iter().flatten() {
            if let Some(row) = self.rows.get_mut(key) {
                visit(row);
            }
        }
    }

    pub(super) fn move_conversation(&mut self, from: &str, to: &str) -> u64 {
        let Some(keys) = self.by_conversation.remove(from) else {
            return 0;
        };
        for key in &keys {
            if let Some(row) = self.rows.get_mut(key) {
                row.conversation_id = to.to_owned();
            }
        }
        let count = keys.len() as u64;
        self.by_conversation
            .entry(to.to_owned())
            .or_default()
            .extend(keys);
        count
    }
}

fn remove_index_key(index: &mut HashMap<String, HashSet<String>>, id: &str, key: &str) {
    if let Some(keys) = index.get_mut(id) {
        keys.remove(key);
        if keys.is_empty() {
            index.remove(id);
        }
    }
}
