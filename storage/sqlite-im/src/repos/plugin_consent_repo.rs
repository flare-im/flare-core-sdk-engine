//! Plugin capability consents in the user database (`plugin_capability_consents`,
//! created by schema migration v5).
use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use flare_im_core_sdk::storage::{
    PluginConsent, PluginConsentScope, PluginConsentStore, consent_error, valid_consent_key,
};
use flare_im_core_sdk::{FlareError, Result};

#[derive(Clone)]
pub struct SqlitePluginConsentRepo {
    pool: SqlitePool,
}

impl SqlitePluginConsentRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn storage_error(_: sqlx::Error) -> FlareError {
    FlareError::system("plugin consent storage failure")
}

/// The table, for schema migration v5. Idempotent.
pub(super) async fn create_table(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS plugin_capability_consents (
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            plugin_id TEXT NOT NULL,
            capability TEXT NOT NULL,
            manifest_major TEXT NOT NULL,
            level INTEGER NOT NULL DEFAULT 0 CHECK(level BETWEEN 0 AND 255),
            granted_at_ms INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (tenant_id, user_id, plugin_id, capability)
        )"#,
    )
    .execute(pool)
    .await
    .map_err(storage_error)?;
    Ok(())
}

fn decode(row: &sqlx::sqlite::SqliteRow) -> Result<PluginConsent> {
    let level: i64 = row.get("level");
    let consent = PluginConsent {
        plugin_id: row.get("plugin_id"),
        capability: row.get("capability"),
        manifest_major: row.get("manifest_major"),
        level: u8::try_from(level).map_err(|_| consent_error("corrupt_consent"))?,
        granted_at_ms: row.get("granted_at_ms"),
    };
    consent
        .validate()
        .map_err(|_| consent_error("corrupt_consent"))?;
    Ok(consent)
}

#[async_trait]
impl PluginConsentStore for SqlitePluginConsentRepo {
    async fn get(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: &str,
    ) -> Result<Option<PluginConsent>> {
        scope.validate()?;
        sqlx::query(
            "SELECT plugin_id, capability, manifest_major, level, granted_at_ms \
             FROM plugin_capability_consents \
             WHERE tenant_id = ? AND user_id = ? AND plugin_id = ? AND capability = ?",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(plugin_id)
        .bind(capability)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .as_ref()
        .map(decode)
        .transpose()
    }

    async fn grant(&self, scope: &PluginConsentScope, consent: &PluginConsent) -> Result<()> {
        scope.validate()?;
        consent.validate()?;
        sqlx::query(
            "INSERT INTO plugin_capability_consents \
             (tenant_id, user_id, plugin_id, capability, manifest_major, level, granted_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (tenant_id, user_id, plugin_id, capability) DO UPDATE SET \
             manifest_major = excluded.manifest_major, level = excluded.level, \
             granted_at_ms = excluded.granted_at_ms",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(&consent.plugin_id)
        .bind(&consent.capability)
        .bind(&consent.manifest_major)
        .bind(i64::from(consent.level))
        .bind(consent.granted_at_ms)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(())
    }

    async fn revoke(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: Option<&str>,
    ) -> Result<u64> {
        scope.validate()?;
        if !valid_consent_key(plugin_id) || capability.is_some_and(|c| !valid_consent_key(c)) {
            return Err(consent_error("invalid_consent"));
        }
        let done = match capability {
            Some(capability) => {
                sqlx::query(
                    "DELETE FROM plugin_capability_consents \
                 WHERE tenant_id = ? AND user_id = ? AND plugin_id = ? AND capability = ?",
                )
                .bind(&scope.tenant_id)
                .bind(&scope.user_id)
                .bind(plugin_id)
                .bind(capability)
                .execute(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "DELETE FROM plugin_capability_consents \
                 WHERE tenant_id = ? AND user_id = ? AND plugin_id = ?",
                )
                .bind(&scope.tenant_id)
                .bind(&scope.user_id)
                .bind(plugin_id)
                .execute(&self.pool)
                .await
            }
        }
        .map_err(storage_error)?;
        Ok(done.rows_affected())
    }

    async fn list(
        &self,
        scope: &PluginConsentScope,
        plugin_id: Option<&str>,
    ) -> Result<Vec<PluginConsent>> {
        scope.validate()?;
        let rows = sqlx::query(
            "SELECT plugin_id, capability, manifest_major, level, granted_at_ms \
             FROM plugin_capability_consents \
             WHERE tenant_id = ? AND user_id = ? AND (? IS NULL OR plugin_id = ?) \
             ORDER BY plugin_id, capability",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.user_id)
        .bind(plugin_id)
        .bind(plugin_id)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.iter().map(decode).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> PluginConsentScope {
        PluginConsentScope {
            tenant_id: "201".into(),
            user_id: "u1".into(),
        }
    }

    fn consent(capability: &str, major: &str, level: u8) -> PluginConsent {
        PluginConsent {
            plugin_id: "example.identity".into(),
            capability: capability.into(),
            manifest_major: major.into(),
            level,
            granted_at_ms: 1_700_000_000_000,
        }
    }

    #[tokio::test]
    async fn the_v5_migration_keeps_one_consent_per_capability_and_revoke_drops_it() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        super::super::init_schema(&pool).await.unwrap();
        let repo = SqlitePluginConsentRepo::new(pool);
        let scope = scope();

        repo.grant(&scope, &consent("documentCapture", "1", 0))
            .await
            .unwrap();
        repo.grant(&scope, &consent("locationOnce", "1", 1))
            .await
            .unwrap();
        // A grant under a newer major replaces the old one.
        repo.grant(&scope, &consent("documentCapture", "2", 0))
            .await
            .unwrap();
        let kept = repo
            .get(&scope, "example.identity", "documentCapture")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kept.manifest_major, "2");
        assert!(kept.covers("2", 0));
        assert!(!kept.covers("1", 0));
        assert_eq!(repo.list(&scope, None).await.unwrap().len(), 2);

        // Another user sees nothing.
        let other = PluginConsentScope {
            tenant_id: "201".into(),
            user_id: "u2".into(),
        };
        assert!(repo.list(&other, None).await.unwrap().is_empty());

        assert_eq!(
            repo.revoke(&scope, "example.identity", Some("documentCapture"))
                .await
                .unwrap(),
            1
        );
        assert!(
            repo.get(&scope, "example.identity", "documentCapture")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            repo.revoke(&scope, "example.identity", None).await.unwrap(),
            1
        );
        assert!(repo.list(&scope, None).await.unwrap().is_empty());
    }
}
