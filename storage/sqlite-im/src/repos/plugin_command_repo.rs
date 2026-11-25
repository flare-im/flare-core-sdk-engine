use async_trait::async_trait;
use serde_json::Value;
use sqlx::{Connection, Row, SqlitePool};

use flare_im_core_sdk::storage::{
    PluginCommand, PluginCommandRecord, PluginCommandScope, PluginCommandStore,
};
use flare_im_core_sdk::storage::{command_error, valid_key};
use flare_im_core_sdk::{FlareError, Result};

#[derive(Clone)]
pub struct SqlitePluginCommandRepo {
    pool: SqlitePool,
}
impl SqlitePluginCommandRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
fn storage_error(_: sqlx::Error) -> FlareError {
    FlareError::system("plugin command journal storage failure")
}
fn decode(row: &sqlx::sqlite::SqliteRow) -> Result<PluginCommandRecord> {
    let command: PluginCommand = serde_json::from_str(&row.get::<String, _>("command_json"))
        .map_err(|_| command_error("corrupt_journal"))?;
    command.validate()?;
    let raw: Option<String> = row.get("response_json");
    let response = raw
        .map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(|_| command_error("corrupt_journal"))?;
    Ok(PluginCommandRecord { command, response })
}
pub(super) async fn init_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS plugin_commands_v1 (tenant_id TEXT NOT NULL, user_id TEXT NOT NULL, slot TEXT NOT NULL, command_json TEXT NOT NULL CHECK(length(CAST(command_json AS BLOB)) <= 32768), response_json TEXT CHECK(length(CAST(response_json AS BLOB)) <= 65536), PRIMARY KEY(tenant_id,user_id,slot))")
        .execute(pool).await.map_err(storage_error)?;
    Ok(())
}
#[async_trait]
impl PluginCommandStore for SqlitePluginCommandRepo {
    async fn prepare(
        &self,
        s: &PluginCommandScope,
        c: &PluginCommand,
    ) -> Result<PluginCommandRecord> {
        s.validate()?;
        c.validate()?;
        // Writer reservation before reading prevents cross-process last-writer
        // wins and makes the 32-slot bound atomic as well.
        let mut connection = self.pool.acquire().await.map_err(storage_error)?;
        // IM stores normally use WAL/NORMAL. An intent must reach durable storage
        // before a financial network request, including after power loss. The
        // acquired connection stays FULL; never weaken another writer's setting.
        sqlx::query("PRAGMA synchronous=FULL")
            .execute(&mut *connection)
            .await
            .map_err(storage_error)?;
        let mut tx = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage_error)?;
        if let Some(row) = sqlx::query("SELECT command_json,response_json FROM plugin_commands_v1 WHERE tenant_id=? AND user_id=? AND slot=?")
            .bind(&s.tenant_id).bind(&s.user_id).bind(&s.slot).fetch_optional(&mut *tx).await.map_err(storage_error)? {
            let record = decode(&row)?;
            if record.command != *c { return Err(command_error("pending_requires_reconciliation")); }
            tx.commit().await.map_err(storage_error)?;
            return Ok(record);
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM plugin_commands_v1 WHERE tenant_id=? AND user_id=?",
        )
        .bind(&s.tenant_id)
        .bind(&s.user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_error)?;
        if count >= 32 {
            return Err(command_error("slot_limit"));
        }
        let encoded = serde_json::to_string(c).map_err(|_| command_error("invalid_command"))?;
        sqlx::query(
            "INSERT INTO plugin_commands_v1(tenant_id,user_id,slot,command_json) VALUES(?,?,?,?)",
        )
        .bind(&s.tenant_id)
        .bind(&s.user_id)
        .bind(&s.slot)
        .bind(encoded)
        .execute(&mut *tx)
        .await
        .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(PluginCommandRecord {
            command: c.clone(),
            response: None,
        })
    }
    async fn pending(&self, s: &PluginCommandScope) -> Result<Option<PluginCommandRecord>> {
        s.validate()?;
        sqlx::query("SELECT command_json,response_json FROM plugin_commands_v1 WHERE tenant_id=? AND user_id=? AND slot=?")
            .bind(&s.tenant_id).bind(&s.user_id).bind(&s.slot).fetch_optional(&self.pool).await.map_err(storage_error)?
            .as_ref().map(decode).transpose()
    }
    async fn complete(
        &self,
        s: &PluginCommandScope,
        c: &PluginCommand,
        response: Value,
    ) -> Result<PluginCommandRecord> {
        s.validate()?;
        c.validate()?;
        if !c.accepts_response(&response) {
            return Err(command_error("outcome_unconfirmed"));
        }
        let encoded = serde_json::to_string(c).map_err(|_| command_error("invalid_command"))?;
        let result =
            serde_json::to_string(&response).map_err(|_| command_error("invalid_response"))?;
        if result.len() > 65536 {
            return Err(command_error("response_limit"));
        }
        let row = sqlx::query("UPDATE plugin_commands_v1 SET response_json=COALESCE(response_json,?) WHERE tenant_id=? AND user_id=? AND slot=? AND command_json=? RETURNING command_json,response_json")
            .bind(result).bind(&s.tenant_id).bind(&s.user_id).bind(&s.slot).bind(encoded).fetch_optional(&self.pool).await.map_err(storage_error)?
            .ok_or_else(|| command_error("command_changed"))?;
        decode(&row)
    }
    async fn acknowledge(&self, s: &PluginCommandScope, id: &str) -> Result<()> {
        s.validate()?;
        if !valid_key(id) {
            return Err(command_error("invalid_command"));
        }
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage_error)?;
        if let Some(row) = sqlx::query("SELECT command_json,response_json FROM plugin_commands_v1 WHERE tenant_id=? AND user_id=? AND slot=?")
            .bind(&s.tenant_id).bind(&s.user_id).bind(&s.slot).fetch_optional(&mut *tx).await.map_err(storage_error)? {
            let record = decode(&row)?;
            if record.command.command_id != id || record.response.is_none() { return Err(command_error("unresolved_command")); }
            sqlx::query("DELETE FROM plugin_commands_v1 WHERE tenant_id=? AND user_id=? AND slot=?")
                .bind(&s.tenant_id).bind(&s.user_id).bind(&s.slot).execute(&mut *tx).await.map_err(storage_error)?;
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "plugin_command_tests.rs"]
mod tests;
