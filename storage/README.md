# Flare IM Core SDK - 存储

Native 绑定依赖 `sqlite-im` 选择 IM 仓储，Web 绑定依赖 `indexeddb` 接入生产宿主。Engine 提供账号开库契约、仓储端口和内存实现。

| 路径 | 所有者与职责 |
| --- | --- |
| [sqlite](sqlite/) | 独立连接底座，负责 pool、SQLCipher 配置和 schema 注册。 |
| [sqlite-im](sqlite-im/) | 实现 Engine 存储端口，负责 IM DDL、迁移、SQL 查询和发件箱事务。 |
| [indexeddb](indexeddb/) | 把 Web 的消息、会话和发件箱提交给生产 JS 存储宿主，并在成功后更新热缓存。 |

## SQLite 存储（`flare-im-core-sdk-storage-sqlite`）

### 职责

- **连接池**：`create_pool` / `open_pool` 创建并返回 `SqlitePool`；`*_with_security` 入口应用 SQLCipher 安全配置。
- **运行时**：`SqliteRuntime::open` / `open_with_security` 持有 pool，供应用与扩展统一使用。
- **Schema 注册**：`register_schema_init` / `register_schema_init_with` 在创建 pool 后统一执行建表逻辑；`register_schema_init_with` 可直接传入 IM 适配器的 `repos::init_schema`，与扩展共用同一 DB。

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
| `register_schema_init_with(name, f)` | 注册返回 `Result<(), E>` 的初始化器，**可直接传入** IM 适配器的 `repos::init_schema`。 |

### 依赖

- 使用 **workspace** 依赖（与 `flare-im-core-engine/Cargo.toml` 一致）：`anyhow`、`once_cell`、`sqlx`、`tokio`。

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

宿主在准备账号时选定后端。默认 SQLite 开库器按租户和用户定位文件、注册 schema 并组装整套仓储。

```rust
use flare_im_core_sdk::prelude::IMClient;
use flare_im_core_sdk_sqlite_im::sqlite;

let client = IMClient::new();
client.init(None, None).await?;
client.prepare("alice", sqlite()).await?;
```

加密库用 `encrypted_sqlite(key_store, scope)` 注入宿主密钥存储。需要从配置组装完整 Native runtime 的宿主可使用 `SqliteRuntimeAssembler`。自定义后端实现 `AccountStoreOpener`，或通过 `LoginDbKind::Provided` 传入已打开的仓储。账号准备阶段校验原子发送仓储，以及成对的 outbox 读写端口。

### 重开发信对测

在 Engine 仓库执行以下命令。需要 C 编译器、Python 3、Node、wasm-pack，以及相邻 Core SDK 和 Web 示例中已安装的 JS 依赖。Chrome 可用 `CHROME_PATH` 指定，也可使用 Playwright 安装的 Chromium。

```sh
cargo rustc --locked -p flare-im-core-sdk-ffi --lib --crate-type cdylib
export FLARE_SDK_PRODUCT_DIST="$(mktemp -d)"
npm --prefix bindings/wasm run build
native_target="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
node tests/storage_parity/run.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
```

Linux 将动态库后缀换成 `.so`。驱动读取同一个 [发信样本](../tests/storage_parity/send.json)，在临时账号库和浏览器配置目录中各创建两条离线消息。两个独立 C 进程、两次独立的 Chromium 启动分别负责写入和读取。断言覆盖完整消息、完整会话，以及持久化 outbox 的客户端 ID、会话 ID 和顺序。

### C / WASM / Tauri 绑定一致性

沿用上面构建好的动态库与 WASM 包，离线运行：

```sh
node tests/storage_parity/bindings.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
```

脚本首次运行时编译 [Tauri 无窗口驱动](../bindings/tauri/examples/parity_driver.rs)，它走 Tauri 命令相同的 `invoke_api_id_json` 路径；已编译时用 `TAURI_PARITY_DRIVER` 指定。三端各开一个临时账号，执行同一组命令并逐项比对：未 prepare、参数不全与未知操作的结构化错误（`code`、`reason`、`retryable`、`message`），以原始 JSON 传入的 2^53 − 1 `sortTs`（契约对 JS 宿主保证的 u64 上限） 与字节数组 `extensions` 存取后是否原样返回，本地发信前后时间线视图的 revision，`media.cancel_transfer` 找不到任务的结果，以及 `message.await_confirmation` 离线超时。WASM 与 Tauri 的结果必须与 C 完全相同。

存储层的同一组契约样本在 `tests/external-consumer` 中运行：`cargo test --locked -p flare-core-external-consumer storage_contract`。内存与 SQLite 两个实现须给出相同的分页、按键查找、重放收敛与未读/读位结果；第三方后端把自己的 `StoreProvider` 加入同一用例即可对照。

### 真实网关 ACK、重连和补偿对测

沿用上节生成的 C 动态库和 release WASM 包。需要本机的 `psql`、`redis-cli`（故障对测使用 Redis 7 的 `EVAL_RO`），以及已构建的 Core 网关、消息、同步服务和 [隔离栈脚本](../scripts/live_stack.py) 所列的依赖。在同一个 shell 执行以下命令。若已有本次测试专用的运行栈，沿用它的 `LIVE_SMOKE_DIR`、`LIVE_STACK_PREFIX` 和端口配置，从导入环境变量一行开始。

```sh
cargo build --manifest-path ../flare-im-core-server/Cargo.toml --locked -p flare-message-ingest -p flare-storage-reader --bins
export LIVE_SMOKE_DIR="$(mktemp -d)" LIVE_STACK_SOCIAL=0
export LIVE_STACK_PREFIX="flare-parity-$(basename "$LIVE_SMOKE_DIR" | tr A-Z a-z)"
python3 scripts/live_stack.py up >/dev/null
eval "$(python3 scripts/live_stack.py env)"
node tests/storage_parity/live.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
node tests/storage_parity/faults.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
node tests/storage_parity/projection.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
node tests/storage_parity/concurrent_ack.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
python3 scripts/live_stack.py down --destroy
```

每端使用一对新账号进行单聊。第一条样本在线发送，随后 TCP 中继断开现有连接并拒绝新连接。第二条样本留在本地发件箱，此时独立的对端通过真实网关发送第三条见证消息。驱动读 PostgreSQL 验证见证消息已经提交，并检查断网设备尚无该消息。恢复中继后等待 SDK 自动重连，逐条核对持久 ACK 的服务端 ID 与序号，检查本地时间线、会话摘要、未读数、成员版本和空发件箱。最后保持断网，分别重开 C 进程和 Chromium 配置目录，重新读取 SQLite 与 IndexedDB 核对状态。[联测驱动](../tests/storage_parity/live.mjs)

故障对测使用相同的 [发信样本](../tests/storage_parity/send.json)。第一组截住网关发给发送者的持久 ACK，并截住发给接收设备的一条实时消息。核对服务端已经提交后断链，检查推送决策链的 `handoff/link_lost` 和该设备唯一的响铃任务。随后在本地发件箱写入途中终止进程，断网重开，验证事务回滚，再恢复连接并重试。第二组反向发送同一份样本，截住接收设备对第一条来信的送达 ACK，在第二条来信写入途中终止接收端。两组均进行补偿和最终断网冷启动。驱动输出各端结果，任一断言失败时进程返回非零。[故障对测入口](../tests/storage_parity/faults.mjs)

C 驱动使用独立的临时 SQLite 库，观察超出页缓存的未提交 WAL 帧后向该驱动发送 `SIGKILL`。浏览器驱动在真实 IndexedDB 事务内观察目标 `put` 成功，保持事务活动并通过 Chromium DevTools 使渲染进程崩溃。两端分别用同一数据目录重新打开。推送登记使用测试令牌，WASM 场景使用同账号的独立 C 宿主登记原生推送，浏览器通过同步取回消息。联测的推送终点是本地 Worker 任务流。[协议与推送观测](../tests/storage_parity/fault_wire.mjs) · [客户端驱动](../tests/storage_parity/clients.mjs)

分次提交对测使用同一份样本的两条来信，分别中断第二条的会话投影和会话同步游标写入。输出按绑定和故障点区分冷存储、断网 SDK 查询以及重连后的状态。每次重开先保留原始持久化证据，再启动 SDK。核对摘要时同时查看末条预览、未读数和投影完成位点；实际同步游标单独读取。测试等待补偿收敛后再次断网重开，比较五项状态。[对测入口](../tests/storage_parity/projection.mjs) · [两端断网及补偿后的结果](../docs/CORE-SDK-ENGINE-IMPLEMENTATION-PLAN.md#阶段-13-实施记录--applyinbound-与断网末条预览)

并发回执对测让 C 和生产 WASM 在同一网关的不同会话同时发送样本中的相同客户端 ID，分别覆盖两个发送账号和同一发送账号的两台设备。第一轮通过测试库的短事务暂停归档写入，第二轮正常提交。中继在转发每个持久 ACK 前立即查询 PostgreSQL，驱动再将原始网关回执、绑定事件和唯一消息行的会话、服务端 ID、序号逐项比对。此用例独占测试栈，运行时请将其他写入套件排在它之前或之后。[并发回执对测](../tests/storage_parity/concurrent_ack.mjs)

### PostgreSQL 持久 ACK 压测与截止时间验收

在上节新建隔离栈的 `up` 命令前设置 `export LIVE_STACK_ACK_PROFILE=1`。该选项为新 PostgreSQL 实例预加载 `pg_stat_statements`，为网关、摄入服务与 Writer 开启分段计时。编译当前分支的摄入服务，沿用同一 C 动态库与生产 WASM 包。在生成联测环境变量后、销毁栈前依次执行以下命令。

```sh
TOKIO_WORKER_THREADS=2 node tests/storage_parity/ack_load.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
node tests/storage_parity/ack_timeout.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
```

压测默认使用 1、4、16、32 个独立 C 发送会话，每档每轮发送 1024 条，两轮按相反顺序运行。两条原样本交替使用，每次追加唯一 ID 后缀。可用 `ACK_BENCH_LEVELS`、`ACK_BENCH_MESSAGES`、`ACK_BENCH_ROUNDS` 调整。每轮分别输出网关和核验阶段百分位数，以及数据库执行确认 SQL 的次数。采样窗口内请仅运行本套件，脚本会把实际上行尝试数、摄入服务观测数及耗时样本数对账。[压测入口](../tests/storage_parity/ack_load.mjs) · [截止时间验收](../tests/storage_parity/ack_timeout.mjs)

截止时间验收对 C 和生产 WASM 各运行两种恢复方式。原连接用例将客户端 ACK 等待时间设为 2 秒，并等待两端各自重试后放行写入。冷重开用例使用默认等待配置，从原发件箱重发。两种用例都在收到网关持久回执时逐条查库。[实测结果](../docs/CORE-SDK-ENGINE-IMPLEMENTATION-PLAN.md#超过-350-ms-后的真实重试)

#### 16 / 32 并发分段复测

在同级 Transport、Server、Engine 的 `core-engine` 工作树执行。使用迁移工具已审核的相同数据库镜像，两次从空库启动，固定 VM 配额、构建配置和服务线程数。可在启动前设置 `LIVE_STACK_POSTGRES_IMAGE` 指向本机已缓存的镜像引用。本轮使用 PostgreSQL 16.15、TimescaleDB 2.30.0。先编译包含计时点的三个服务，并按上节配置 `LIVE_STACK_ACK_PROFILE=1`。

```sh
cargo build --manifest-path ../flare-im-core-server/Cargo.toml --locked --bins \
  -p flare-signaling-gateway -p flare-storage-writer -p flare-message-ingest
# 对照栈
python3 scripts/live_stack.py ctl restart message-ingest --env MESSAGE_INGEST_LEDGER_MAX_CONNECTIONS=2
# 优化栈使用当前构建的默认值
# python3 scripts/live_stack.py ctl restart message-ingest
ACK_BENCH_STAGES=1 ACK_BENCH_LEVELS=16,32 ACK_BENCH_MESSAGES=1024 ACK_BENCH_ROUNDS=2 \
  TOKIO_WORKER_THREADS=2 node tests/storage_parity/ack_load.mjs \
  "$native_target/debug/libflare_im_core_sdk_ffi.dylib" "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
```

对照栈和优化栈各自执行一次上述负载，结束后在优化栈执行 `ack_timeout.mjs`。输出包含每档合并的 P95/P99、网关持久 ACK 最慢 1% 消息对应的分段均值、实际 SQL 调用次数和由同步单独完成的数量。Writer 的事务与摄入核验并行。核验查询计时包含 PostgreSQL 往返，数据库内部累计执行时间单列为 `sqlExecMs`。[计时与关联](../tests/storage_parity/ack_stages.mjs)

## IndexedDB 存储

生产实现位于 `storage/indexeddb`：Memory 热存储配合 JS IndexedDB 宿主，写入等待宿主确认，恢复失败返回错误。

## 设计原则

1. **存储与 core 解耦**：SQLite 连接底座可独立复用，IM 适配器通过 Engine 的仓储契约接入。
2. **扩展友好**：通过 `register_schema_init` / `register_schema_init_with` 与同一 pool，扩展可与 core 共用同一数据库；IM 适配器的 `repos::init_schema` 可直接注册使用。
3. **依赖统一**：使用 workspace 依赖，与根 `Cargo.toml` 版本一致。


### 应用与用户分区

`build_web_store_provider(namespace, tenant_id, user_id)` 返回 `Result<StoreProvider>`。分区由 Core 的 `local_storage_scope` 生成，覆盖消息、待发和游标；插件存储继续使用显式的 tenant / user / plugin 身份，不把消息缓存的分区键当作真实用户；JS 宿主的 `userId` 是该不透明分区键，不是消息发送者的用户 ID。

SQLite 的 `resolve_user_db_path(base, namespace, tenant_id, user_id)` 使用 `scopes/v2-<SHA-256>/flare_im_sdk.db`，媒体缓存与库同目录；加密和非加密路径都使用同一组身份，Social 也共享该分区。长度前缀与完整摘要避免分隔符和文件名净化导致的身份碰撞。

旧 `users/<user>/flare_im_sdk.db` 和 IndexedDB 中未分区的行保留原位，不自动导入或删除。它们没有可信租户归属，不能因为当前登录者同名就迁移。恢复旧离线草稿时应先停止 SDK、备份原数据，并从宿主配置 / 创建记录核实来源租户，再迁入对应分区；无法核实的旧数据保持隔离。新分区中的服务端历史由正常同步恢复。

IndexedDB 快照保存正文 protobuf 缓存与保留策略 / 状态，和面向界面的消息 JSON 分开；这些存储字段不扩展平台 SDK 的公开消息合同。快照读取或解码失败会阻止登录 / 预热完成，可在存储恢复后重试。

底层 Rust 存储构造与路径函数需要显式身份参数，工作区外调用方升级时也须同步；不保留按 userId 单独分区的旧入口。

Runtime 的 `storage.encryption.key_namespace` 只选择密钥命名空间；数据文件始终由 `storage.namespace / tenant_id / user_id` 分区。未设置密钥命名空间时使用 `storage.namespace`。此前该覆盖项被忽略：若已有加密缓存是用旧位置的密钥创建的，修正配置前需把同一密钥迁到配置位置，或继续显式使用原位置；更换命名空间不会自动重加密文件。

消息写操作优先按 `server_id` 定位；只有本地消息可按唯一 `client_msg_id` 回退，已确认消息应传服务端 ID。卡片状态动作继续支持唯一 client ID。ACK 使用显式的 client ID，只合并同会话、同发送者的发送映射与目标服务端行，不能把同一字符串当成另一条消息的 server ID 来合并或删除它。

私有事件时钟只属于本地状态，wire 收发会剥离；自定义存储宿主应保留现有存储快照 JSON，不能用 `to_proto` 代替本地快照。

消息保留终态不可被旧快照撤销。服务端 reaction 快照不携带本地事件序列时，保留已知版本的用户 / emoji 操作，其他用户的快照反应照常合并；后续更高版本事件可继续更新。标记为单个值，类型共用同一个事件时钟。

## 排除宿主编译的空库对照

先构建并固定两份 Writer、同一份 C 动态库及生产 WASM。原生压测驱动在计时前编译，`ACK_NATIVE_DRIVER` 指向用当前头文件和同一动态库构建的可执行文件。macOS 可用 `otool -L "$ACK_NATIVE_DRIVER"` 核对实际装载路径。以下命令沿用上一节的已启动专属栈、导出的连接变量和产物路径。

```sh
export ACK_NATIVE_DRIVER="$LIVE_SMOKE_DIR/native-driver"
cc tests/storage_parity/native.c -I ../flare-im-core-sdk/native/abi \
  "$native_target/debug/libflare_im_core_sdk_ffi.dylib" -pthread \
  -Wl,-rpath,"$native_target/debug" -o "$ACK_NATIVE_DRIVER"
ACK_BENCH_STAGES=1 ACK_BENCH_LEVELS=16,32 ACK_BENCH_MESSAGES=1024 ACK_BENCH_ROUNDS=2 \
  TOKIO_WORKER_THREADS=2 python3 tests/storage_parity/quiet_host.py "$LIVE_SMOKE_DIR/host.json" \
  node tests/storage_parity/ack_load.mjs "$native_target/debug/libflare_im_core_sdk_ffi.dylib" \
  "$FLARE_SDK_PRODUCT_DIST/wasm/pkg"
```

每组从独立空数据卷启动，固定服务、样本、核验截止和资源配置。分别运行原 Writer 池 2、原 Writer 池 4，以及新 Writer 池 4。池 4 的前后版本可反向再测一对。脚本等到连续 10 秒编译安静后启动客户端，对整个子进程采样，并在结束时复核本工作区 sccache 的请求计数。退出码 75 表示整组作废，需换空库重跑。证据保存在 `host.json`，包括实际最大采样间隔。网关持久 ACK 的百分位仅包含在线收到的持久帧，`syncOnly`、`outcomes.timeout` 和 SDK 百分位另行保留。逐条超时重试在最后一组结束后用本节同一份库运行 `ack_timeout.mjs`。
