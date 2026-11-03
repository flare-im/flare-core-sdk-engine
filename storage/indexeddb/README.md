# IndexedDB 存储边界

IndexedDB 是 Web/WASM 平台的持久化后端。它的职责是承载 `flare-im-core-sdk`
的核心仓储语义，而不是让业务 UI 直接读写浏览器数据库。

## 架构定位

- `flare-core-sdk-engine/storage/indexeddb`：core SDK 的 Web/WASM 存储后端位置，负责对齐 core 仓储语义、schema、迁移和事务边界。
- `flare-core-typescript-sdk/src/storage/indexeddb.ts`：TypeScript 运行时的浏览器 IndexedDB driver，服务 Web/H5 adapter。
- `flare-core-typescript-sdk/src/storage/sqlite.ts`：RN/uni-app App 构建使用的 SQLite driver 契约，通过平台 SQLite executor 注入。
- 示例应用不能自建长期存储语义，只能消费 SDK 暴露的 storage driver 或 client facade。

## Core 存储模型

Web/WASM 后端需要覆盖和 SQLite 后端等价的核心能力：

- `SessionStore`：登录用户、连接状态、配置快照、能力快照。
- `ConversationRepository`：会话摘要、未读、置顶、草稿、同步版本。
- `MessageRepository`：消息主体、会话序列、client/server id 对账。
- `SyncCursorRepository`：会话同步游标、消息同步游标。
- `OutboxRepository`：离线操作、发送重试、幂等 client message id。
- `MediaCacheStore`：媒体缓存元信息。
- `SchemaMigrationStore`：schema version 和显式迁移记录。

稳定语义必须使用命名字段、枚举或文档化契约；`metadata`/`extra` 只作为扩展逃生口。

## TypeScript driver 关系

当前浏览器落地实现位于：

```text
flare-core-sdk/packages/flare-core-typescript-sdk/src/storage/indexeddb.ts
```

它使用生成的 TypeScript SDK 模型持久化：

```text
sdk_kv/session
conversations
messages
mediaCache
outbox
```

后续如果 `flare-im-core-sdk` 以 WASM/Worker 方式进入浏览器，应把这里实现为 Rust core repository backend，
并让 TypeScript driver 退化为 bridge/host adapter，避免两套业务持久化规则长期并存。

## 使用场景

适用于 Web/H5 和 WASM core：

- 纯 TypeScript Web runtime：使用 `flare-core-typescript-sdk/web` 导出的 `IndexedDbStorageDriver`。
- WASM core runtime：由 core repository 直接使用 IndexedDB 后端，TypeScript 仅负责平台调用和事件桥接。
- RN/uni-app App：不要使用 IndexedDB，使用 `flare-core-typescript-sdk/react-native` 或 `flare-core-typescript-sdk/uni-app` 的 SQLite driver 契约。
