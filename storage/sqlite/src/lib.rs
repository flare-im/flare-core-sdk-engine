//! SQLite 连接与运行时（不依赖 flare-im-core-sdk）
//!
//! 提供：
//! - [create_pool]：创建 SQLite 连接池
//! - [open_pool]：创建池并执行所有已注册 [register_schema_init] 的初始化器
//! - **[SqliteRuntime]**：运行时持有 pool，创建后执行已注册的 schema init
//! - [register_schema_init]：注册自定义 schema，在创建 pool 后统一调用
//!
//! Core 表与仓储由调用方（如 flare-im-core-sdk）在拿到 pool 后自行 init 与构建。

mod runtime;
mod schema_registry;

use anyhow::Result as AnyhowResult;
use log::LevelFilter;
use once_cell::sync::Lazy;
use sqlx::ConnectOptions;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

pub use runtime::SqliteRuntime;
pub use schema_registry::{SchemaInitializer, register_schema_init, register_schema_init_with};

type PoolRegistry = Mutex<HashMap<String, SqlitePool>>;

static POOL_REGISTRY: Lazy<PoolRegistry> = Lazy::new(|| Mutex::new(HashMap::new()));

/// SQLite 安全配置。
///
/// 传入 `encryption_key` 时要求底层 SQLite 真实支持 SQLCipher；普通 SQLite 会直接返回错误，
/// 避免 `PRAGMA key` 被静默忽略后让调用方误以为本地库已经加密。
#[derive(Clone, Default)]
pub struct SqliteSecurityConfig {
    encryption_key: Option<String>,
    require_sqlcipher: bool,
}

impl fmt::Debug for SqliteSecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteSecurityConfig")
            .field(
                "encryption_key",
                &self.encryption_key.as_ref().map(|_| "<redacted>"),
            )
            .field("require_sqlcipher", &self.require_sqlcipher)
            .finish()
    }
}

impl SqliteSecurityConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_encryption_key(mut self, key: impl Into<String>) -> Self {
        self.encryption_key = Some(key.into());
        self.require_sqlcipher = true;
        self
    }

    pub fn require_sqlcipher(mut self, require: bool) -> Self {
        self.require_sqlcipher = require;
        self
    }

    pub fn is_encryption_required(&self) -> bool {
        self.encryption_key.is_some() || self.require_sqlcipher
    }

    fn encryption_key(&self) -> Option<&str> {
        self.encryption_key.as_deref()
    }
}

/// 统一连接选项：WAL + 较长 busy_timeout + 页缓存，减轻多连接争用写锁时的阻塞与 sqlx 慢查询告警。
pub(crate) fn connect_options(database_url: &str) -> AnyhowResult<SqliteConnectOptions> {
    Ok(SqliteConnectOptions::from_str(database_url)
        .map_err(|e| anyhow::anyhow!("invalid sqlite url: {e}"))?
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        // 写锁排队：过短会导致语句失败或总耗时仍被记为 slow；15s 与池 acquire 超时配合
        .busy_timeout(Duration::from_secs(15))
        // 负值表示 KB，约 64MiB page cache，降低大库随机读延迟
        .pragma("cache_size", "-65536")
        // 适度 mmap，减少读系统调用（移动端/桌面均可接受）
        .pragma("mmap_size", "67108864")
        // 仅当单语句（含等锁）≥ 5s 再打 WARN，避免正常锁等待刷屏
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(5)))
}

fn pool_options() -> SqlitePoolOptions {
    let max = std::env::var("FLARE_SQLITE_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|n| (1..=256).contains(n))
        .unwrap_or(1);

    SqlitePoolOptions::new()
        .max_connections(max)
        // 新连接建立 + busy 等锁可能 >2s，提高阈值减少误报
        .acquire_slow_threshold(Duration::from_secs(8))
}

fn pool_registry_guard() -> MutexGuard<'static, HashMap<String, SqlitePool>> {
    match POOL_REGISTRY.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::error!("SQLite pool registry mutex poisoned; recovering pool registry");
            poisoned.into_inner()
        }
    }
}

async fn shared_pool(database_url: &str) -> AnyhowResult<SqlitePool> {
    if let Some(pool) = pool_registry_guard()
        .get(database_url)
        .filter(|pool| !pool.is_closed())
        .cloned()
    {
        return Ok(pool);
    }

    let opened = create_pool(database_url).await?;
    let mut registry = pool_registry_guard();
    if let Some(pool) = registry
        .get(database_url)
        .filter(|pool| !pool.is_closed())
        .cloned()
    {
        return Ok(pool);
    }
    registry.insert(database_url.to_string(), opened.clone());
    Ok(opened)
}

/// 创建 SQLite 连接池（不建表）
///
/// 默认 `max_connections = 1`：嵌入式客户端优先保证本地写入串行，避免同进程多连接互相抢
/// SQLite 写锁。服务端或专项压测可通过 `FLARE_SQLITE_MAX_CONNECTIONS` 显式放开。
pub async fn create_pool(database_url: &str) -> AnyhowResult<SqlitePool> {
    create_pool_with_security(database_url, SqliteSecurityConfig::default()).await
}

/// 创建 SQLite 连接池并应用安全配置（不建表）。
pub async fn create_pool_with_security(
    database_url: &str,
    security: SqliteSecurityConfig,
) -> AnyhowResult<SqlitePool> {
    let mut options = connect_options(database_url)?;
    if let Some(key) = security.encryption_key() {
        if key.trim().is_empty() || key.contains('\0') {
            anyhow::bail!("SQLite encryption key must not be empty or contain NUL");
        }
        options = options
            .pragma("key", format!("'{}'", key.replace('\'', "''")))
            .disable_statement_logging();
    }

    let pool = pool_options()
        .connect_with(options)
        .await
        .map_err(|error| match security.encryption_key() {
            Some(key) => anyhow::anyhow!(
                error
                    .to_string()
                    .replace(&format!("'{}'", key.replace('\'', "''")), "'<redacted>'")
                    .replace(key, "<redacted>")
            ),
            None => error.into(),
        })?;
    if security.is_encryption_required()
        && let Err(error) = verify_sqlcipher_available(&pool).await
    {
        pool.close().await;
        return Err(error);
    }

    Ok(pool)
}

/// 将本地文件路径转为 `sqlite://` URL
pub fn database_url_from_path(path: &Path) -> String {
    if let Ok(file_url) = url::Url::from_file_path(path) {
        return format!("sqlite://{}?mode=rwc", file_url.path());
    }

    let path_str = path.to_string_lossy().replace('\\', "/");
    let encoded = path_str.replace(' ', "%20");
    if encoded.starts_with('/') {
        format!("sqlite://{}?mode=rwc", encoded)
    } else {
        format!("sqlite:///{}?mode=rwc", encoded)
    }
}

/// 创建连接池并执行所有已注册的 [register_schema_init] 初始化器。
pub async fn open_pool(database_url: &str) -> AnyhowResult<SqlitePool> {
    let pool = shared_pool(database_url).await?;
    schema_registry::run_registered_schema_inits(&pool).await?;
    Ok(pool)
}

/// 创建连接池、应用安全配置，并执行所有已注册的 [register_schema_init] 初始化器。
pub async fn open_pool_with_security(
    database_url: &str,
    security: SqliteSecurityConfig,
) -> AnyhowResult<SqlitePool> {
    let pool = create_pool_with_security(database_url, security).await?;
    schema_registry::run_registered_schema_inits(&pool).await?;
    Ok(pool)
}

async fn verify_sqlcipher_available(pool: &SqlitePool) -> AnyhowResult<()> {
    let version: Option<(String,)> = sqlx::query_as("PRAGMA cipher_version;")
        .fetch_optional(pool)
        .await?;
    let version = version.map(|row| row.0).unwrap_or_default();
    if version.trim().is_empty() {
        anyhow::bail!(
            "SQLite encryption requires SQLCipher, but the current sqlite runtime does not report PRAGMA cipher_version"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_url_from_path_encodes_spaces() {
        let url = database_url_from_path(Path::new("/tmp/Application Support/flare_im_core_sdk.db"));

        assert!(url.starts_with("sqlite:///tmp/"));
        assert!(url.contains("Application%20Support"));
        assert!(url.ends_with("?mode=rwc"));
    }

    #[test]
    fn sqlite_security_config_redacts_key_in_debug() {
        let config = SqliteSecurityConfig::new().with_encryption_key("very-secret-key");
        let debug = format!("{config:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("very-secret-key"));
    }

    #[tokio::test]
    async fn encrypted_pool_rejects_empty_key_before_opening() {
        let encrypted = create_pool_with_security(
            "sqlite::memory:",
            SqliteSecurityConfig::new().with_encryption_key("   "),
        )
        .await;

        let err = encrypted.expect_err("empty key rejected");
        assert!(err.to_string().contains("must not be empty"));
    }

    #[tokio::test]
    async fn create_pool_supports_paths_with_spaces() {
        let root =
            std::env::temp_dir().join(format!("flare sqlite path test {}", std::process::id()));
        let db = root.join("flare im sdk.db");
        std::fs::create_dir_all(&root).expect("create temp sqlite dir");

        let url = database_url_from_path(&db);
        let pool = create_pool(&url).await.expect("open sqlite db");
        pool.close().await;

        assert!(db.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn encryption_keys_are_literals_and_never_escape_through_errors() {
        let plain = create_pool("sqlite::memory:").await.unwrap();
        let has_cipher = verify_sqlcipher_available(&plain).await.is_ok();
        plain.close().await;
        for key in [
            "1a".repeat(32),
            "a1".repeat(32),
            "key'; PRAGMA user_version = 91; --".into(),
        ] {
            let result = create_pool_with_security(
                "sqlite::memory:",
                SqliteSecurityConfig::new().with_encryption_key(&key),
            )
            .await;
            match result {
                Ok(pool) => {
                    assert!(has_cipher);
                    pool.close().await;
                }
                Err(error) => {
                    let diagnostic = format!("{error:#}");
                    assert!(!diagnostic.contains(&key), "key must stay out of errors");
                    assert!(
                        !has_cipher && diagnostic.contains("SQLCipher"),
                        "{diagnostic}"
                    );
                }
            }
        }
        let key = "invalid\0secret";
        let error = create_pool_with_security(
            "sqlite::memory:",
            SqliteSecurityConfig::new().with_encryption_key(key),
        )
        .await
        .unwrap_err();
        assert!(!format!("{error:#}").contains(key));
    }

    #[tokio::test]
    async fn encrypted_pool_requires_sqlcipher_runtime() {
        let root = std::env::temp_dir().join(format!(
            "flare sqlite encrypted path test {}",
            std::process::id()
        ));
        let db = root.join("flare im encrypted sdk.db");
        std::fs::create_dir_all(&root).expect("create temp sqlite dir");

        let url = database_url_from_path(&db);
        let plain_pool = create_pool(&url).await.expect("open sqlite db");
        let sqlcipher_available = verify_sqlcipher_available(&plain_pool).await.is_ok();
        plain_pool.close().await;

        let encrypted = create_pool_with_security(
            &url,
            SqliteSecurityConfig::new().with_encryption_key("test-key"),
        )
        .await;

        if sqlcipher_available {
            encrypted
                .expect("SQLCipher runtime should accept encrypted pool")
                .close()
                .await;
        } else {
            encrypted.expect_err("plain SQLite must reject encryption config");
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn open_pool_reuses_default_pool_for_same_database_url() {
        let root = std::env::temp_dir().join(format!(
            "flare sqlite shared pool test {}",
            std::process::id()
        ));
        let db = root.join("flare im sdk.db");
        std::fs::create_dir_all(&root).expect("create temp sqlite dir");

        let url = database_url_from_path(&db);
        let first = open_pool(&url).await.expect("open first sqlite pool");
        let second = open_pool(&url).await.expect("open second sqlite pool");

        first.close().await;
        assert!(second.is_closed(), "same URL should reuse one pool handle");

        let _ = std::fs::remove_dir_all(root);
    }
}
