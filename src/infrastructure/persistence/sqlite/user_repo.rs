use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::{UserProfile, UserReader, UserWriter};
use crate::shared::error::{ErrorCode, FlareError, Result};

pub struct SqliteUserProfileRepo {
    pool: SqlitePool,
}

impl SqliteUserProfileRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserReader for SqliteUserProfileRepo {
    async fn get(&self, user_id: &str) -> Result<Option<UserProfile>> {
        let row: Option<(String, String, String)> = sqlx::query_as(
            "SELECT nickname, avatar_url, alias FROM user_profiles WHERE user_id = ?",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row.map(|(nickname, avatar_url, alias)| UserProfile {
            user_id: user_id.to_string(),
            nickname,
            avatar_url,
            alias,
        }))
    }

    /// 单次批量查询(IN),消除时间线打开时按消息逐条查发送者资料的 N+1。按 SQLite 变量上限分块。
    async fn get_many(
        &self,
        user_ids: &[String],
    ) -> Result<std::collections::HashMap<String, UserProfile>> {
        use std::collections::HashMap;
        let mut unique: Vec<&str> = user_ids
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        unique.sort_unstable();
        unique.dedup();
        let mut out = HashMap::with_capacity(unique.len());
        if unique.is_empty() {
            return Ok(out);
        }
        // SQLite 绑定变量上限默认 999，分块查询。
        for chunk in unique.chunks(super::SQLITE_IN_CHUNK) {
            let placeholders = super::in_placeholders(chunk.len());
            let sql = format!(
                "SELECT user_id, nickname, avatar_url, alias FROM user_profiles WHERE user_id IN ({placeholders})"
            );
            let mut query = sqlx::query_as::<_, (String, String, String, String)>(&sql);
            for id in chunk {
                query = query.bind(*id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            for (user_id, nickname, avatar_url, alias) in rows {
                out.insert(
                    user_id.clone(),
                    UserProfile {
                        user_id,
                        nickname,
                        avatar_url,
                        alias,
                    },
                );
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl UserWriter for SqliteUserProfileRepo {
    async fn save_batch(&self, profiles: &[UserProfile]) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        for p in profiles {
            sqlx::query(
                r#"INSERT OR REPLACE INTO user_profiles (user_id, nickname, avatar_url, alias)
                   VALUES (?, ?, ?, ?)"#,
            )
            .bind(&p.user_id)
            .bind(&p.nickname)
            .bind(&p.avatar_url)
            .bind(&p.alias)
            .execute(&mut *tx)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    async fn repo() -> SqliteUserProfileRepo {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        super::super::schema::init_schema(&pool).await.unwrap();
        SqliteUserProfileRepo::new(pool)
    }

    fn profile(user_id: &str, nickname: &str) -> UserProfile {
        UserProfile {
            user_id: user_id.to_string(),
            nickname: nickname.to_string(),
            avatar_url: String::new(),
            alias: String::new(),
        }
    }

    #[tokio::test]
    async fn get_many_batches_and_returns_found_profiles() {
        let repo = repo().await;
        repo.save_batch(&[
            profile("u1", "Alice"),
            profile("u2", "Bob"),
            profile("u3", "Carol"),
        ])
        .await
        .unwrap();

        // 含重复 + 缺失(u9)；批量一次取回。
        let ids = vec![
            "u1".to_string(),
            "u2".to_string(),
            "u1".to_string(),
            "u9".to_string(),
            "  ".to_string(),
        ];
        let map = repo.get_many(&ids).await.unwrap();

        assert_eq!(map.len(), 2, "only found, deduped, non-empty ids");
        assert_eq!(map.get("u1").unwrap().display_name(), "Alice");
        assert_eq!(map.get("u2").unwrap().display_name(), "Bob");
        assert!(!map.contains_key("u9"), "missing profile absent");
    }

    #[tokio::test]
    async fn aliases_are_stored_with_the_profile() {
        let repo = repo().await;
        repo.save_batch(&[profile("u1", "Alice").with_alias("Boss")])
            .await
            .unwrap();
        assert_eq!(repo.get("u1").await.unwrap().unwrap().alias, "Boss");
        let map = repo.get_many(&["u1".to_string()]).await.unwrap();
        assert_eq!(map.get("u1").unwrap().alias, "Boss");
    }

    #[tokio::test]
    async fn get_many_empty_input_returns_empty() {
        let repo = repo().await;
        assert!(repo.get_many(&[]).await.unwrap().is_empty());
    }
}
