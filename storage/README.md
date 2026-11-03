# Flare IM Core SDK - 存储

本目录提供与 Flare IM Core SDK 配套的存储能力：**SQLite 连接与运行时**。本 crate **不依赖** `flare-im-core-sdk`，仅提供连接池、schema 注册与运行时封装，便于各平台与扩展复用同一数据库。

## 目录结构

```
storage/
├── README.md           # 本说明
├── sqlite/             # SQLite 连接与运行时（pool + schema 注册）
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs           # create_pool / open_pool / SqliteRuntime / register_schema_init*
│       ├── runtime.rs       # SqliteRuntime
│       └── schema_registry.rs  # 自定义 schema 注册
└── indexeddb/          # IndexedDB（Web 平台，待实现）
    └── README.md
```

## SQLite 存储（`flare-im-core-sdk-storage-sqlite`）

### 职责

- **连接池**：`create_pool` / `open_pool` 创建并返回 `SqlitePool`；`*_with_security` 入口应用 SQLCipher 安全配置。
- **运行时**：`SqliteRuntime::open` / `open_with_security` 持有 pool，供应用与扩展统一使用。
- **Schema 注册**：`register_schema_init` / `register_schema_init_with` 在创建 pool 后统一执行建表逻辑；`register_schema_init_with` 可直接传入 core-sdk 的 `sqlite_init_schema`，与扩展共用同一 DB。

本 crate **不实现**消息/会话等仓储，也不建 core 表；core 表与仓储由 **flare-im-core-sdk**（`storage-sqlite` feature）在拿到 pool 后自行 `init_schema` 与构建。

### API

| API | 说明 |
|-----|------|
| `create_pool(database_url)` | 仅创建 SQLite 连接池，不建表。 |
| `create_pool_with_security(database_url, security)` | 创建连接池并应用 `PRAGMA key`；启用加密时要求运行时真实支持 SQLCipher。 |
| `open_pool(database_url)` | 创建池并执行所有已注册的 schema 初始化器。 |
| `open_pool_with_security(database_url, security)` | 创建加密连接池并执行所有已注册的 schema 初始化器。 |
| `database_url_from_path(path)` | 将本地路径转为 `sqlite://...?mode=rwc`。 |
| `SqliteRuntime::open(url)` | 创建池 + 执行已注册 schema，返回持有 pool 的运行时。 |
| `SqliteRuntime::open_with_security(url, security)` | 创建加密池 + 执行已注册 schema，返回持有 pool 的运行时。 |
| `SqliteRuntime::pool()` | 获取连接池。 |
| `register_schema_init(name, \|pool\| async move { ... })` | 注册返回 `anyhow::Result<()>` 的 schema 初始化器。 |
| `register_schema_init_with(name, f)` | 注册返回 `Result<(), E>` 的初始化器，**可直接传入** core-sdk 的 `sqlite_init_schema`。 |

### 依赖

- 使用 **workspace** 依赖（与 `flare-core-sdk-engine/Cargo.toml` 一致）：`anyhow`、`once_cell`、`sqlx`、`tokio`。

### 使用方式

#### 1. 仅用本 crate：拿池 + 自定义 schema

```rust
use flare_im_core_sdk_storage_sqlite::{open_pool, register_schema_init, SqliteRuntime};

// 可选：注册扩展表
register_schema_init("my_extension", |pool| async move {
    sqlx::query("CREATE TABLE IF NOT EXISTS my_table (id TEXT PRIMARY KEY)")
        .execute(pool)
        .await?;
    Ok(())
});

// 方式 A：直接拿池
let pool = open_pool("sqlite:///data/im.db?mode=rwc").await?;

// 方式 B：运行时（推荐，便于多处复用同一 pool）
let rt = SqliteRuntime::open("sqlite:///data/im.db?mode=rwc").await?;
let pool = rt.pool();
```

#### 2. 与本 SDK 配合：core 表 + 仓储

依赖 **flare-im-core-sdk**（启用 `storage-sqlite`）。推荐：用 `register_schema_init_with` 直接注册 core 的 `sqlite_init_schema`，这样 `open_pool` / `SqliteRuntime::open` 时会自动建 core 表，无需再手动调用。

```rust
use std::sync::Arc;
use flare_im_core_sdk_storage_sqlite::{register_schema_init_with, SqliteRuntime};
use flare_im_core_sdk::store::sqlite_init_schema;
use flare_im_core_sdk::store::{StoreProvider,
    SqliteMessageRepo, SqliteConversationRepo, SqlitePendingSendRepo,
    SqliteUserProfileRepo, SqliteSyncCursorRepo};

// 1) 注册 core 表初始化（直接传入 init_schema，open 时自动执行）
register_schema_init_with("core", |pool| sqlite_init_schema(pool));

// 2) 拿运行时（内部已执行 core + 其他已注册的 schema）
let rt = SqliteRuntime::open("sqlite:///data/im.db?mode=rwc").await?;

// 3) 用同一 pool 构建仓储并组装 StoreProvider
let p = rt.pool();
let pending_repo = Arc::new(SqlitePendingSendRepo::new(p.clone()));
let user_repo = Arc::new(SqliteUserProfileRepo::new(p.clone()));
let stores = StoreProvider {
    messages: Arc::new(SqliteMessageRepo::new(p.clone())),
    conversations: Arc::new(SqliteConversationRepo::new(p.clone())),
    cursors: Arc::new(SqliteSyncCursorRepo::new(p.clone())),
    pending_send_reader: Some(pending_repo.clone()),
    pending_send_writer: Some(pending_repo),
    user_profiles_reader: Some(user_repo.clone()),
    user_profiles_writer: Some(user_repo),
};
```

## IndexedDB 存储

待实现，适用于 Web（wasm32），通过实现 core-sdk 的 domain Reader/Writer trait 接入。参见 core-sdk 的 `store::indexeddb_adapter` 文档。

## 设计原则

1. **存储与 core 解耦**：本目录的 SQLite crate 不依赖 core-sdk，只提供连接与 schema 注册。
2. **扩展友好**：通过 `register_schema_init` / `register_schema_init_with` 与同一 pool，扩展可与 core 共用同一数据库；core 的 `sqlite_init_schema` 可直接注册使用。
3. **依赖统一**：使用 workspace 依赖，与根 `Cargo.toml` 版本一致。
