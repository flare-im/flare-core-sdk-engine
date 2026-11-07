//! 纯内存仓储 — 用于未配置 SQLite 时的 fallback（如 StoreProvider::user_profiles_or_memory）。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::domain::{
    PendingSendReader, PendingSendVo, PendingSendWriter, UserProfile, UserReader, UserWriter,
};
use crate::shared::error::Result;

/// 内存用户资料存储（未配置 SQLite 时由 StoreProvider 提供）
pub struct MemoryUserProfileStore {
    data: Arc<RwLock<HashMap<String, UserProfile>>>,
}

impl MemoryUserProfileStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryUserProfileStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl UserReader for MemoryUserProfileStore {
    async fn get(&self, user_id: &str) -> Result<Option<UserProfile>> {
        let data = self.data.read().await;
        Ok(data.get(user_id).cloned())
    }
}

#[async_trait]
impl UserWriter for MemoryUserProfileStore {
    async fn save_batch(&self, profiles: &[UserProfile]) -> Result<()> {
        let mut data = self.data.write().await;
        for p in profiles {
            data.insert(p.user_id.clone(), p.clone());
        }
        Ok(())
    }
}

// ---------- PendingSend ----------

/// 内存待发送队列（可选，未配置 SQLite 时使用）。
///
/// 发送顺序按入队时刻（发件箱保证它在一个队列里严格递增）；入队时刻相同的旧条目按写入先后，
/// 与 SQLite 实现（`rowid`）一致，不看 `client_msg_id`（随机值，按它排会打乱会话内顺序）。
pub struct MemoryPendingSendStore {
    data: Arc<RwLock<PendingEntries>>,
}

#[derive(Default)]
struct PendingEntries {
    /// client_msg_id -> (写入序号, 条目)
    by_id: HashMap<String, (u64, PendingSendVo)>,
    next_seq: u64,
}

impl PendingEntries {
    fn ordered(&self) -> Vec<&(u64, PendingSendVo)> {
        let mut entries = self.by_id.values().collect::<Vec<_>>();
        entries.sort_by_key(|(seq, entry)| (entry.enqueued_at_ms, *seq));
        entries
    }
}

impl MemoryPendingSendStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(PendingEntries::default())),
        }
    }
}

impl Default for MemoryPendingSendStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PendingSendReader for MemoryPendingSendStore {
    async fn get(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let data = self.data.read().await;
        Ok(data
            .by_id
            .get(client_msg_id)
            .map(|(_, entry)| entry.clone()))
    }

    async fn list(&self) -> Result<Vec<PendingSendVo>> {
        let data = self.data.read().await;
        Ok(data
            .ordered()
            .into_iter()
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn take_oldest(&self) -> Result<Option<PendingSendVo>> {
        let data = self.data.read().await;
        Ok(data.ordered().first().map(|(_, entry)| entry.clone()))
    }

    async fn list_oldest_excluding(
        &self,
        excluded_client_msg_ids: &[String],
        limit: usize,
    ) -> Result<Vec<PendingSendVo>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let excluded = excluded_client_msg_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let data = self.data.read().await;
        Ok(data
            .ordered()
            .into_iter()
            .filter(|(_, entry)| !excluded.contains(entry.client_msg_id.as_str()))
            .take(limit)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

#[async_trait]
impl PendingSendWriter for MemoryPendingSendStore {
    async fn push(&self, entry: PendingSendVo) -> Result<()> {
        let mut data = self.data.write().await;
        let seq = data.next_seq;
        data.next_seq += 1;
        data.by_id.insert(entry.client_msg_id.clone(), (seq, entry));
        Ok(())
    }

    async fn pop(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let mut data = self.data.write().await;
        Ok(data.by_id.remove(client_msg_id).map(|(_, entry)| entry))
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryPendingSendStore;
    use crate::domain::{PendingSendReader, PendingSendVo, PendingSendWriter};
    use crate::model::IMMessage;

    fn pending(client_msg_id: &str, enqueued_at_ms: u64) -> PendingSendVo {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = client_msg_id.to_string();
        message.conversation_id = "conv".to_string();
        PendingSendVo {
            client_msg_id: client_msg_id.to_string(),
            conversation_id: "conv".to_string(),
            message,
            enqueued_at_ms,
        }
    }

    /// 按入队时刻；入队时刻相同时按写入先后，不按 client_msg_id（与 SQLite 实现一致）。
    #[tokio::test]
    async fn pending_order_is_enqueue_time_then_write_order() {
        let store = MemoryPendingSendStore::new();
        store.push(pending("00-later", 8)).await.unwrap();
        for id in ["zz-first", "mm-second", "aa-third"] {
            store.push(pending(id, 7)).await.unwrap();
        }
        let ids = |rows: Vec<PendingSendVo>| {
            rows.into_iter()
                .map(|entry| entry.client_msg_id)
                .collect::<Vec<_>>()
        };
        let order = vec!["zz-first", "mm-second", "aa-third", "00-later"];
        assert_eq!(
            ids(store.list_oldest_excluding(&[], 10).await.unwrap()),
            order
        );
        assert_eq!(ids(store.list().await.unwrap()), order);
        assert_eq!(
            store.take_oldest().await.unwrap().unwrap().client_msg_id,
            "zz-first"
        );
        assert_eq!(
            ids(store
                .list_oldest_excluding(&["zz-first".to_string()], 1)
                .await
                .unwrap()),
            vec!["mm-second"]
        );
    }
}
