//! 自定义 schema 注册：在创建 pool 后统一执行 core init + 所有已注册的 init。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Result as AnyhowResult;
use once_cell::sync::Lazy;
use sqlx::SqlitePool;

type SchemaInitEntry = (String, Arc<dyn SchemaInitializer>);
type SchemaInitRegistry = Mutex<Vec<SchemaInitEntry>>;
type SchemaInitSnapshot = Vec<SchemaInitEntry>;

/// 可注册的 schema 初始化器：接收 pool，执行异步初始化（如建表）。
pub trait SchemaInitializer: Send + Sync {
    fn run<'a>(
        &'a self,
        pool: &'a SqlitePool,
    ) -> Pin<Box<dyn Future<Output = AnyhowResult<()>> + Send + 'a>>;
}

/// 闭包形式（返回 anyhow::Result）
struct ClosureSchemaInit<F>(F);

impl<F, Fut> SchemaInitializer for ClosureSchemaInit<F>
where
    F: Fn(&SqlitePool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = AnyhowResult<()>> + Send + 'static,
{
    fn run<'a>(
        &'a self,
        pool: &'a SqlitePool,
    ) -> Pin<Box<dyn Future<Output = AnyhowResult<()>> + Send + 'a>> {
        Box::pin((self.0)(pool))
    }
}

/// 闭包形式（返回 Result<(), E>，E 转为 anyhow::Error），便于直接传入 core-sdk 的 init_schema
struct MapErrSchemaInit<F>(F);

impl<F, Fut, E> SchemaInitializer for MapErrSchemaInit<F>
where
    F: Fn(&SqlitePool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<anyhow::Error> + 'static,
{
    fn run<'a>(
        &'a self,
        pool: &'a SqlitePool,
    ) -> Pin<Box<dyn Future<Output = AnyhowResult<()>> + Send + 'a>> {
        Box::pin(async move { (self.0)(pool).await.map_err(Into::into) })
    }
}

static REGISTRY: Lazy<SchemaInitRegistry> = Lazy::new(|| Mutex::new(Vec::new()));

fn lock_recovering_poison<T>(mutex: &'static Mutex<T>, label: &str) -> MutexGuard<'static, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            log::error!("{label} mutex poisoned; recovering registered schema initializers");
            poisoned.into_inner()
        }
    }
}

fn registry_guard() -> MutexGuard<'static, Vec<SchemaInitEntry>> {
    lock_recovering_poison(&REGISTRY, "schema registry")
}

/// 注册在创建 pool 后统一调用的 schema 初始化逻辑（返回 `anyhow::Result<()>`）。
pub fn register_schema_init<N, F, Fut>(name: N, f: F)
where
    N: Into<String>,
    F: Fn(&SqlitePool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = AnyhowResult<()>> + Send + 'static,
{
    registry_guard().push((name.into(), Arc::new(ClosureSchemaInit(f))));
}

/// 注册返回 `Result<(), E>`（E: Into<anyhow::Error>）的 schema 初始化器，**可直接传入** core-sdk 的
/// `sqlite_init_schema` 等方法（无需再包一层 map_err）。
///
/// # 示例（直接使用 core-sdk 的 init_schema）
///
/// ```ignore
/// use flare_im_core_sdk_storage_sqlite::register_schema_init_with;
/// use flare_im_core_sdk_sqlite_im::repos::init_schema;
///
/// register_schema_init_with("core", |pool| init_schema(pool));
/// ```
pub fn register_schema_init_with<N, F, Fut, E>(name: N, f: F)
where
    N: Into<String>,
    F: Fn(&SqlitePool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Into<anyhow::Error> + 'static,
{
    registry_guard().push((name.into(), Arc::new(MapErrSchemaInit(f))));
}

/// 执行所有已注册的 schema 初始化器（内部使用）
pub async fn run_registered_schema_inits(pool: &SqlitePool) -> AnyhowResult<()> {
    let inits: SchemaInitSnapshot = registry_guard().clone();
    for (name, init) in inits {
        init.run(pool)
            .await
            .map_err(|e| anyhow::anyhow!("schema init {:?} failed: {}", name, e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static POISON_TEST_MUTEX: Lazy<Mutex<Vec<&'static str>>> = Lazy::new(|| Mutex::new(Vec::new()));

    #[test]
    fn lock_recovering_poison_keeps_registered_values_available() {
        let _ = std::thread::spawn(|| {
            let mut guard = POISON_TEST_MUTEX.lock().expect("test mutex");
            guard.push("before panic");
            panic!("poison test mutex");
        })
        .join();

        let mut guard = lock_recovering_poison(&POISON_TEST_MUTEX, "test schema registry");
        guard.push("after poison");

        assert_eq!(guard.as_slice(), ["before panic", "after poison"]);
    }
}
