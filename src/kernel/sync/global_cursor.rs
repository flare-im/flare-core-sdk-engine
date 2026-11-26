use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::domain::SyncCursorStore;
use crate::shared::error::{FlareError, Result};

use super::{DomainCursor, DomainId};

const DEVICE_GLOBAL_SYNC_POINT_KEY_PREFIX: &str = "sync:device_global";

/// 设备级全局同步点。
///
/// **顺序契约（I1，数据不丢红线）**：`global_seq` / 任何 domain cursor 只有在其覆盖的增量
/// **durably 落盘成功后**才可推进——先落数据、后推游标；崩溃只会造成幂等重放，绝不产生
/// "游标已越过、数据未落盘"的永久缺口。全局流消费方（B2/B3 接线）必须以
/// `route_global_stream` 报告的 applied 结果 + 落库确认为推进依据，与
/// `apply_single_conversation_page` 的 per-conversation 游标语义一致。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceGlobalSyncPoint {
    pub user_id: String,
    pub global_seq: u64,
    pub synced_at: u64,
    domain_cursors: BTreeMap<String, String>,
}

impl DeviceGlobalSyncPoint {
    pub fn new(user_id: impl Into<String>, global_seq: u64, synced_at: u64) -> Self {
        Self {
            user_id: user_id.into(),
            global_seq,
            synced_at,
            domain_cursors: BTreeMap::new(),
        }
    }

    pub fn key_for_user(user_id: &str) -> String {
        format!("{DEVICE_GLOBAL_SYNC_POINT_KEY_PREFIX}:{user_id}")
    }

    pub fn set_domain_cursor(&mut self, domain_id: DomainId, cursor: DomainCursor) {
        match cursor.0 {
            Some(value) => {
                self.domain_cursors.insert(domain_id.0, value);
            }
            None => {
                self.domain_cursors.remove(domain_id.as_str());
            }
        }
    }

    pub fn domain_cursor(&self, domain_id: &DomainId) -> DomainCursor {
        self.domain_cursors
            .get(domain_id.as_str())
            .cloned()
            .map(DomainCursor::at)
            .unwrap_or_else(DomainCursor::empty)
    }
}

#[derive(Clone)]
pub struct DeviceGlobalSyncPointStore {
    cursors: Arc<dyn SyncCursorStore>,
}

impl DeviceGlobalSyncPointStore {
    pub fn new(cursors: Arc<dyn SyncCursorStore>) -> Self {
        Self { cursors }
    }

    pub async fn load(&self, user_id: &str) -> Result<Option<DeviceGlobalSyncPoint>> {
        let key = DeviceGlobalSyncPoint::key_for_user(user_id);
        let Some(raw) = self.cursors.get_raw(&key).await? else {
            return Ok(None);
        };
        crate::shared::util::json::from_str(&raw)
            .map(Some)
            .map_err(|error| {
                FlareError::general_error(format!("invalid global sync point: {error}"))
            })
    }

    pub async fn save(&self, point: &DeviceGlobalSyncPoint) -> Result<()> {
        let key = DeviceGlobalSyncPoint::key_for_user(&point.user_id);
        let raw = serde_json::to_string(point).map_err(|error| {
            FlareError::general_error(format!("encode global sync point: {error}"))
        })?;
        self.cursors.save_raw(&key, &raw).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::domain::SyncCursorReader;
    use crate::infrastructure::persistence::MemorySyncCursorStore;
    use crate::kernel::{DomainCursor, DomainId};

    use super::{DeviceGlobalSyncPoint, DeviceGlobalSyncPointStore};

    #[tokio::test]
    async fn global_sync_point_round_trips_without_conversation_cursor_shape() {
        let cursors = Arc::new(MemorySyncCursorStore::new());
        let store = DeviceGlobalSyncPointStore::new(cursors.clone());
        let mut point = DeviceGlobalSyncPoint::new("u1", 42, 1000);
        point.set_domain_cursor(DomainId::new("im.messages"), DomainCursor::at("msg-42"));
        point.set_domain_cursor(
            DomainId::new("example.items"),
            DomainCursor::at("friends-7"),
        );

        store.save(&point).await.expect("save global point");
        let loaded = store.load("u1").await.expect("load global point").unwrap();

        assert_eq!(loaded.user_id, "u1");
        assert_eq!(loaded.global_seq, 42);
        assert_eq!(loaded.synced_at, 1000);
        assert_eq!(
            loaded.domain_cursor(&DomainId::new("im.messages")),
            DomainCursor::at("msg-42")
        );
        assert_eq!(
            loaded.domain_cursor(&DomainId::new("example.items")),
            DomainCursor::at("friends-7")
        );
        assert!(
            cursors
                .get_conversation_cursor("u1", "sync:device_global:u1")
                .await
                .unwrap()
                .is_none(),
            "global point must not pollute per-conversation SyncCursorVo shape"
        );
    }

    #[tokio::test]
    async fn empty_domain_cursor_removes_child_cursor() {
        let mut point = DeviceGlobalSyncPoint::new("u1", 1, 10);
        let domain = DomainId::new("biz.orders");
        point.set_domain_cursor(domain.clone(), DomainCursor::at("orders-1"));
        point.set_domain_cursor(domain.clone(), DomainCursor::empty());

        assert!(point.domain_cursor(&domain).is_empty());
    }
}
