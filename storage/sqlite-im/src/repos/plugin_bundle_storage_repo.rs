//! What plugin web bundles keep through the bridge, in the user database
//! (`plugin_bundle_storage`, created by schema migration v6). A table of its
//! own: nothing else reads it, and signing out deletes the user's rows.
use async_trait::async_trait;
use serde_json::Value;
use sqlx::{Row, SqlitePool};

use flare_im_core_sdk::storage::{
    PluginBundleStorage, PluginBundleStorageScope, bundle_storage_cost, bundle_storage_error,
    bundle_storage_fits, bundle_storage_quota_exceeded, valid_bundle_storage_key,
};
use flare_im_core_sdk::{FlareError, Result};

#[derive(Clone)]
pub struct SqlitePluginBundleStorageRepo {
    pool: SqlitePool,
}

impl SqlitePluginBundleStorageRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn storage_error(_: sqlx::Error) -> FlareError {
    FlareError::system("plugin bundle storage failure")
}

/// The table, for schema migration v6. Idempotent. `cost` is the key's bytes
/// plus the value's encoded bytes, so the quota is one `SUM`.
pub(super) async fn create_table(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS plugin_bundle_storage (
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            plugin_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            cost INTEGER NOT NULL CHECK(cost > 0),
            updated_at_ms INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tenant_id, user_id, plugin_id, key)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(storage_error)?;
    Ok(())
}

fn checked(scope: &PluginBundleStorageScope, key: &str) -> Result<()> {
    scope.validate()?;
    if !valid_bundle_storage_key(key) {
        return Err(bundle_storage_error("invalid_key"));
    }
    Ok(())
}

#[async_trait]
impl PluginBundleStorage for SqlitePluginBundleStorageRepo {
    async fn get(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<Option<Value>> {
        checked(scope, key)?;
        let row = sqlx::query(
            "SELECT value FROM plugin_bundle_storage \
             WHERE tenant_id = ? AND user_id = ? AND plugin_id = ? AND key = ?",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(&scope.plugin_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(|row| {
            let raw: String = row.get("value");
            serde_json::from_str(&raw).map_err(|_| bundle_storage_error("corrupt_value"))
        })
        .transpose()
    }

    async fn set(&self, scope: &PluginBundleStorageScope, key: &str, value: &Value) -> Result<()> {
        checked(scope, key)?;
        let cost = bundle_storage_cost(key, value)?;
        let encoded =
            serde_json::to_string(value).map_err(|_| bundle_storage_error("invalid_value"))?;
        // One transaction: two writers of a namespace cannot both pass the
        // quota check.
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        let (used, replaced): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(cost), 0), COALESCE(SUM(CASE WHEN key = ? THEN cost ELSE 0 END), 0) \
             FROM plugin_bundle_storage WHERE tenant_id = ? AND user_id = ? AND plugin_id = ?",
        )
        .bind(key)
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(&scope.plugin_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_error)?;
        if !bundle_storage_fits(used.max(0) as usize, replaced.max(0) as usize, cost) {
            return Err(bundle_storage_quota_exceeded());
        }
        sqlx::query(
            "INSERT INTO plugin_bundle_storage \
             (tenant_id, user_id, plugin_id, key, value, cost, updated_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, user_id, plugin_id, key) DO UPDATE SET \
             value = excluded.value, cost = excluded.cost, updated_at_ms = excluded.updated_at_ms",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(&scope.plugin_id)
        .bind(key)
        .bind(encoded)
        .bind(cost as i64)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }

    async fn remove(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<bool> {
        checked(scope, key)?;
        let done = sqlx::query(
            "DELETE FROM plugin_bundle_storage \
             WHERE tenant_id = ? AND user_id = ? AND plugin_id = ? AND key = ?",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(&scope.plugin_id)
        .bind(key)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(done.rows_affected() > 0)
    }

    async fn keys(
        &self,
        scope: &PluginBundleStorageScope,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        scope.validate()?;
        if prefix.is_some_and(|p| !p.is_empty() && !valid_bundle_storage_key(p)) {
            return Err(bundle_storage_error("invalid_key"));
        }
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT key FROM plugin_bundle_storage \
             WHERE tenant_id = ? AND user_id = ? AND plugin_id = ? \
             AND substr(key, 1, length(?)) = ? ORDER BY key",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(&scope.plugin_id)
        .bind(prefix.unwrap_or_default())
        .bind(prefix.unwrap_or_default())
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(rows.into_iter().map(|(key,)| key).collect())
    }

    async fn clear_user(&self, tenant_id: &str, user_id: &str) -> Result<u64> {
        let done =
            sqlx::query("DELETE FROM plugin_bundle_storage WHERE tenant_id = ? AND user_id = ?")
                .bind(tenant_id)
                .bind(user_id)
                .execute(&self.pool)
                .await
                .map_err(storage_error)?;
        Ok(done.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        create_table(&pool).await.unwrap();
        pool
    }

    fn scope(plugin: &str) -> PluginBundleStorageScope {
        PluginBundleStorageScope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            plugin_id: plugin.into(),
        }
    }

    #[tokio::test]
    async fn round_trip_prefix_quota_and_sign_out() {
        let repo = SqlitePluginBundleStorageRepo::new(pool().await);
        repo.set(&scope("a"), "draft/1", &json!({"x": 1}))
            .await
            .unwrap();
        repo.set(&scope("a"), "other", &json!(true)).await.unwrap();
        assert_eq!(
            repo.get(&scope("a"), "draft/1").await.unwrap(),
            Some(json!({"x": 1}))
        );
        assert_eq!(repo.get(&scope("b"), "draft/1").await.unwrap(), None);
        assert_eq!(
            repo.keys(&scope("a"), Some("draft/")).await.unwrap(),
            vec!["draft/1".to_owned()]
        );
        assert_eq!(repo.keys(&scope("a"), None).await.unwrap().len(), 2);
        let chunk = json!("x".repeat(60 * 1024));
        let mut refused = None;
        for i in 0..32 {
            if let Err(error) = repo.set(&scope("a"), &format!("k{i}"), &chunk).await {
                refused = Some((i, error));
                break;
            }
        }
        let (at, error) = refused.expect("the quota stops the writes");
        assert!(at >= 16 && flare_im_core_sdk::storage::is_bundle_storage_quota_error(&error));
        assert!(repo.remove(&scope("a"), "other").await.unwrap());
        assert!(repo.clear_user("t1", "u1").await.unwrap() > 0);
        assert!(repo.keys(&scope("a"), None).await.unwrap().is_empty());
    }
}
