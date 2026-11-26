//! Extension-owned database. The account opener keeps it in its own file next to the
//! account database, with the same encryption. Core IM migrations never open it.
use async_trait::async_trait;
use flare_im_core_sdk::spi::{
    CheckpointWrite, ExtensionRecord, PartitionState, RecordBackend, RecordScope, RecordWrite,
};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};
use sqlx::SqlitePool;

pub struct SqliteExtensionRecords(SqlitePool);

fn db(error: impl std::fmt::Display) -> FlareError {
    FlareError::localized(ErrorCode::DatabaseError, error.to_string())
}
fn partition(scope: &RecordScope) -> Result<String> {
    serde_json::to_string(&[
        &scope.tenant,
        &scope.user,
        &scope.namespace,
        &scope.partition,
    ])
    .map_err(db)
}
/// JSON-array prefix shared by every partition of one account's namespace.
fn namespace_prefix(scope: &RecordScope) -> Result<String> {
    let mut prefix =
        serde_json::to_string(&[&scope.tenant, &scope.user, &scope.namespace]).map_err(db)?;
    prefix.pop(); // the closing bracket
    prefix.push(',');
    Ok(prefix)
}
impl SqliteExtensionRecords {
    pub async fn open(pool: SqlitePool) -> Result<Self> {
        let mut tx = pool.begin().await.map_err(db)?;
        for ddl in [
            "CREATE TABLE IF NOT EXISTS extension_records (scope TEXT NOT NULL, id TEXT NOT NULL, revision INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,id)) WITHOUT ROWID",
            "CREATE TABLE IF NOT EXISTS extension_record_index (scope TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY(scope,field,value,id)) WITHOUT ROWID",
            "CREATE INDEX IF NOT EXISTS extension_index_owner ON extension_record_index(scope,id)",
            "CREATE TABLE IF NOT EXISTS extension_checkpoints (scope TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID",
            "CREATE TABLE IF NOT EXISTS extension_partitions (scope TEXT PRIMARY KEY, body TEXT NOT NULL) WITHOUT ROWID",
        ] {
            sqlx::query(ddl).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(Self(pool))
    }
}
#[async_trait]
impl RecordBackend for SqliteExtensionRecords {
    async fn get(&self, scope: &RecordScope, id: &str) -> Result<Option<ExtensionRecord>> {
        let body: Option<String> =
            sqlx::query_scalar("SELECT body FROM extension_records WHERE scope=? AND id=?")
                .bind(partition(scope)?)
                .bind(id)
                .fetch_optional(&self.0)
                .await
                .map_err(db)?;
        body.map(|s| serde_json::from_str(&s).map_err(db))
            .transpose()
    }
    async fn checkpoint(&self, scope: &RecordScope) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT value FROM extension_checkpoints WHERE scope=?")
            .bind(partition(scope)?)
            .fetch_optional(&self.0)
            .await
            .map_err(db)
    }
    async fn commit(&self, rows: &[RecordWrite], checkpoints: &[CheckpointWrite]) -> Result<bool> {
        let mut tx = self.0.begin().await.map_err(db)?;
        for write in rows {
            let scope = partition(&write.scope)?;
            let record = &write.record;
            let body = serde_json::to_string(record).map_err(db)?;
            let installed: Option<String> =
                sqlx::query_scalar("SELECT body FROM extension_partitions WHERE scope=?")
                    .bind(&scope)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            let installed = installed
                .map(|s| serde_json::from_str::<PartitionState>(&s).map_err(db))
                .transpose()?;
            if installed.is_some_and(|state| state.schema_version != record.schema_version) {
                tx.rollback().await.map_err(db)?;
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "extension handle schema is not the installed schema",
                ));
            }
            let changed = if write.expected == 0 {
                sqlx::query("INSERT INTO extension_records(scope,id,revision,body) VALUES(?,?,?,?) ON CONFLICT(scope,id) DO NOTHING")
                    .bind(&scope).bind(&record.id).bind(record.revision).bind(body).execute(&mut *tx).await
            } else {
                sqlx::query("UPDATE extension_records SET revision=?,body=? WHERE scope=? AND id=? AND revision=?")
                    .bind(record.revision).bind(body).bind(&scope).bind(&record.id).bind(write.expected).execute(&mut *tx).await
            }.map_err(db)?;
            if changed.rows_affected() == 0 {
                tx.rollback().await.map_err(db)?;
                return Ok(false);
            }
            sqlx::query("DELETE FROM extension_record_index WHERE scope=? AND id=?")
                .bind(&scope)
                .bind(&record.id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            for (field, value) in &write.indexes {
                sqlx::query(
                    "INSERT INTO extension_record_index(scope,field,value,id) VALUES(?,?,?,?)",
                )
                .bind(&scope)
                .bind(field)
                .bind(value)
                .bind(&record.id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
        }
        for write in checkpoints {
            let scope = partition(&write.scope)?;
            let current: Option<String> =
                sqlx::query_scalar("SELECT value FROM extension_checkpoints WHERE scope=?")
                    .bind(&scope)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            if current != write.update.expected {
                tx.rollback().await.map_err(db)?;
                return Ok(false);
            }
            sqlx::query("INSERT INTO extension_checkpoints(scope,value) VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET value=excluded.value")
                .bind(&scope).bind(&write.update.next).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    async fn installed(&self, scope: &RecordScope) -> Result<Option<PartitionState>> {
        let body: Option<String> =
            sqlx::query_scalar("SELECT body FROM extension_partitions WHERE scope=?")
                .bind(partition(scope)?)
                .fetch_optional(&self.0)
                .await
                .map_err(db)?;
        body.map(|s| serde_json::from_str(&s).map_err(db))
            .transpose()
    }
    async fn install(
        &self,
        scope: &RecordScope,
        expected: Option<&PartitionState>,
        next: &PartitionState,
    ) -> Result<bool> {
        let scope = partition(scope)?;
        let mut tx = self.0.begin().await.map_err(db)?;
        let current: Option<String> =
            sqlx::query_scalar("SELECT body FROM extension_partitions WHERE scope=?")
                .bind(&scope)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        let current = current
            .map(|s| serde_json::from_str::<PartitionState>(&s).map_err(db))
            .transpose()?;
        if current.as_ref() != expected {
            tx.rollback().await.map_err(db)?;
            return Ok(false);
        }
        sqlx::query("INSERT INTO extension_partitions(scope,body) VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET body=excluded.body")
            .bind(&scope)
            .bind(serde_json::to_string(next).map_err(db)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    async fn purge_namespace(&self, scope: &RecordScope) -> Result<()> {
        let prefix = namespace_prefix(scope)?;
        let mut tx = self.0.begin().await.map_err(db)?;
        for table in [
            "extension_records",
            "extension_record_index",
            "extension_checkpoints",
            "extension_partitions",
        ] {
            sqlx::query(&format!(
                "DELETE FROM {table} WHERE substr(scope,1,length(?1))=?1"
            ))
            .bind(&prefix)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(())
    }
    async fn scan(&self, scope: &RecordScope, after: &str, limit: u32) -> Result<Vec<String>> {
        sqlx::query_scalar(
            "SELECT id FROM extension_records WHERE scope=? AND id>? ORDER BY id LIMIT ?",
        )
        .bind(partition(scope)?)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.0)
        .await
        .map_err(db)
    }
    async fn find(
        &self,
        scope: &RecordScope,
        index: &str,
        value: &str,
        after: &str,
        limit: u32,
    ) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT id FROM extension_record_index WHERE scope=? AND field=? AND value=? AND id>? ORDER BY id LIMIT ?")
            .bind(partition(scope)?).bind(index).bind(value).bind(after).bind(limit).fetch_all(&self.0).await.map_err(db)
    }
}
