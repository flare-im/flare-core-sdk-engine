//! 已证实连续区间的内存实现，以及存进同步游标键值的实现（浏览器 IndexedDB 存储用）。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};

use crate::domain::{SeqRange, SyncCursorStore, TimelineCoverage, TimelineCoverageStore};
use crate::shared::error::Result;

/// 纯内存实现（内存仓储与测试）。
#[derive(Default)]
pub struct MemoryTimelineCoverageStore {
    data: RwLock<HashMap<String, TimelineCoverage>>,
}

impl MemoryTimelineCoverageStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TimelineCoverageStore for MemoryTimelineCoverageStore {
    async fn coverage(&self, conversation_id: &str) -> Result<TimelineCoverage> {
        Ok(self
            .data
            .read()
            .await
            .get(conversation_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn add(&self, conversation_id: &str, range: SeqRange) -> Result<()> {
        self.data
            .write()
            .await
            .entry(conversation_id.to_string())
            .or_default()
            .insert(range);
        Ok(())
    }

    async fn clear(&self, conversation_id: &str) -> Result<()> {
        self.data.write().await.remove(conversation_id);
        Ok(())
    }
}

const COVERAGE_CURSOR_KEY_PREFIX: &str = "timeline_coverage:";

/// 区间在同步游标里的键：`timeline_coverage:<会话 id>`。
pub fn timeline_coverage_cursor_key(conversation_id: &str) -> String {
    format!("{COVERAGE_CURSOR_KEY_PREFIX}{conversation_id}")
}

/// 每个会话的区间存成同步游标里的一条键值（`timeline_coverage:<会话 id>` → `from-to,from-to`）。
///
/// 浏览器端的 IndexedDB 存储经 JS 宿主持久化游标键值、在启动快照里整体载回，区间随之持久化，
/// 不需要宿主新增对象仓库或方法。读写都经同一个游标仓储，写入串行化，读出时重新规范化。
pub struct CursorBackedTimelineCoverageStore {
    cursors: Arc<dyn SyncCursorStore>,
    writes: Mutex<()>,
}

impl CursorBackedTimelineCoverageStore {
    pub fn new(cursors: Arc<dyn SyncCursorStore>) -> Self {
        Self {
            cursors,
            writes: Mutex::new(()),
        }
    }
}

fn encode_coverage(coverage: &TimelineCoverage) -> String {
    coverage
        .ranges()
        .iter()
        .map(|range| format!("{}-{}", range.from, range.to))
        .collect::<Vec<_>>()
        .join(",")
}

/// 读不懂的段跳过（宁可漏标）。
fn decode_coverage(value: &str) -> TimelineCoverage {
    TimelineCoverage::from_ranges(value.split(',').filter_map(|part| {
        let (from, to) = part.trim().split_once('-')?;
        SeqRange::new(from.parse().ok()?, to.parse().ok()?)
    }))
}

#[async_trait]
impl TimelineCoverageStore for CursorBackedTimelineCoverageStore {
    async fn coverage(&self, conversation_id: &str) -> Result<TimelineCoverage> {
        Ok(self
            .cursors
            .get_raw(&timeline_coverage_cursor_key(conversation_id))
            .await?
            .map(|value| decode_coverage(&value))
            .unwrap_or_default())
    }

    async fn add(&self, conversation_id: &str, range: SeqRange) -> Result<()> {
        let _write = self.writes.lock().await;
        let mut coverage = self.coverage(conversation_id).await?;
        if coverage.insert(range) {
            self.cursors
                .save_raw(
                    &timeline_coverage_cursor_key(conversation_id),
                    &encode_coverage(&coverage),
                )
                .await?;
        }
        Ok(())
    }

    async fn clear(&self, conversation_id: &str) -> Result<()> {
        let _write = self.writes.lock().await;
        let key = timeline_coverage_cursor_key(conversation_id);
        if self.cursors.get_raw(&key).await?.is_some() {
            // 游标仓储没有删除；空值读出来就是没有区间。
            self.cursors.save_raw(&key, "").await?;
        }
        Ok(())
    }
}

/// 每个存储实现都跑的同一组区间用例。
#[cfg(test)]
pub(crate) mod contract {
    use crate::domain::{SeqRange, TimelineCoverageStore};

    fn pairs(coverage: &crate::domain::TimelineCoverage) -> Vec<(u64, u64)> {
        coverage
            .ranges()
            .iter()
            .map(|range| (range.from, range.to))
            .collect()
    }

    async fn add(store: &dyn TimelineCoverageStore, conversation_id: &str, from: u64, to: u64) {
        store
            .add(conversation_id, SeqRange::new(from, to).expect("range"))
            .await
            .expect("add range");
    }

    async fn read(store: &dyn TimelineCoverageStore, conversation_id: &str) -> Vec<(u64, u64)> {
        pairs(
            &store
                .coverage(conversation_id)
                .await
                .expect("read coverage"),
        )
    }

    pub(crate) async fn assert_store_contract(store: &dyn TimelineCoverageStore) {
        assert_eq!(read(store, "c1").await, vec![], "no range before any proof");

        add(store, "c1", 10, 20).await;
        add(store, "c1", 30, 40).await;
        assert_eq!(read(store, "c1").await, vec![(10, 20), (30, 40)]);

        add(store, "c1", 21, 29).await;
        assert_eq!(
            read(store, "c1").await,
            vec![(10, 40)],
            "adjacent ranges merge"
        );

        add(store, "c1", 1, 5).await;
        add(store, "c1", 50, 60).await;
        assert_eq!(read(store, "c1").await, vec![(1, 5), (10, 40), (50, 60)]);

        add(store, "c1", 4, 55).await;
        assert_eq!(
            read(store, "c1").await,
            vec![(1, 60)],
            "one range overlapping three merges them all"
        );

        add(store, "c1", 7, 9).await;
        assert_eq!(
            read(store, "c1").await,
            vec![(1, 60)],
            "a contained range changes nothing"
        );

        add(store, "c2", 9_000_000_000_000, 9_000_000_000_100).await;
        assert_eq!(
            read(store, "c2").await,
            vec![(9_000_000_000_000, 9_000_000_000_100)],
            "large seqs round-trip"
        );
        assert_eq!(
            read(store, "c1").await,
            vec![(1, 60)],
            "conversations are apart"
        );

        store.clear("c1").await.expect("clear");
        assert_eq!(read(store, "c1").await, vec![]);
        assert_eq!(
            read(store, "c2").await,
            vec![(9_000_000_000_000, 9_000_000_000_100)]
        );

        add(store, "c1", 3, 4).await;
        assert_eq!(
            read(store, "c1").await,
            vec![(3, 4)],
            "a cleared conversation starts over"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;

    use super::{CursorBackedTimelineCoverageStore, MemoryTimelineCoverageStore, contract};
    use crate::domain::{
        SeqRange, SyncCursorReader, SyncCursorVo, SyncCursorWriter, TimelineCoverageStore,
    };
    use crate::infrastructure::persistence::MemorySyncCursorStore;
    use crate::shared::error::Result;

    #[tokio::test]
    async fn the_memory_store_follows_the_contract() {
        contract::assert_store_contract(&MemoryTimelineCoverageStore::new()).await;
    }

    #[tokio::test]
    async fn the_cursor_backed_store_follows_the_contract() {
        let store = CursorBackedTimelineCoverageStore::new(Arc::new(MemorySyncCursorStore::new()));
        contract::assert_store_contract(&store).await;
    }

    /// 浏览器端的游标仓储：内存热路径，每次写游标再交给宿主持久化（这里记进 `persisted`）。
    struct HostPersistedCursors {
        inner: MemorySyncCursorStore,
        persisted: Arc<StdMutex<HashMap<String, String>>>,
    }

    #[async_trait]
    impl SyncCursorReader for HostPersistedCursors {
        async fn get_raw(&self, key: &str) -> Result<Option<String>> {
            self.inner.get_raw(key).await
        }

        async fn get_conversation_cursor(
            &self,
            user_id: &str,
            conversation_id: &str,
        ) -> Result<Option<SyncCursorVo>> {
            self.inner
                .get_conversation_cursor(user_id, conversation_id)
                .await
        }
    }

    #[async_trait]
    impl SyncCursorWriter for HostPersistedCursors {
        async fn save_raw(&self, key: &str, cursor: &str) -> Result<()> {
            self.inner.save_raw(key, cursor).await?;
            self.persisted
                .lock()
                .unwrap()
                .insert(key.to_string(), cursor.to_string());
            Ok(())
        }

        async fn save_conversation_cursor(&self, cursor: &SyncCursorVo) -> Result<()> {
            self.inner.save_conversation_cursor(cursor).await
        }
    }

    /// 与 IndexedDB 存储的启动路径相同：宿主快照里的游标键值逐条 `save_raw` 回内存仓储，
    /// 区间从同一组键值读回，逐段不变。
    #[tokio::test]
    async fn ranges_written_through_the_host_come_back_after_a_reload() {
        let persisted = Arc::new(StdMutex::new(HashMap::new()));
        let before = CursorBackedTimelineCoverageStore::new(Arc::new(HostPersistedCursors {
            inner: MemorySyncCursorStore::new(),
            persisted: persisted.clone(),
        }));
        for (from, to) in [(980, 1000), (1, 36), (37, 40)] {
            before
                .add("c1", SeqRange::new(from, to).unwrap())
                .await
                .unwrap();
        }
        before
            .add("c2", SeqRange::new(5, 6).unwrap())
            .await
            .unwrap();
        before.clear("c2").await.unwrap();

        let reloaded_cursors = MemorySyncCursorStore::new();
        let snapshot = persisted.lock().unwrap().clone();
        for (key, value) in snapshot {
            reloaded_cursors.save_raw(&key, &value).await.unwrap();
        }
        let after = CursorBackedTimelineCoverageStore::new(Arc::new(reloaded_cursors));
        assert_eq!(
            after.coverage("c1").await.unwrap(),
            before.coverage("c1").await.unwrap()
        );
        assert_eq!(
            after
                .coverage("c1")
                .await
                .unwrap()
                .ranges()
                .iter()
                .map(|range| (range.from, range.to))
                .collect::<Vec<_>>(),
            vec![(1, 40), (980, 1000)]
        );
        assert!(after.coverage("c2").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unreadable_stored_ranges_are_skipped() {
        let cursors = Arc::new(MemorySyncCursorStore::new());
        cursors
            .save_raw("timeline_coverage:c1", "1-5,x-9,20-10,30-31")
            .await
            .unwrap();
        let store = CursorBackedTimelineCoverageStore::new(cursors);
        assert_eq!(
            store
                .coverage("c1")
                .await
                .unwrap()
                .ranges()
                .iter()
                .map(|range| (range.from, range.to))
                .collect::<Vec<_>>(),
            vec![(1, 5), (30, 31)]
        );
    }
}
