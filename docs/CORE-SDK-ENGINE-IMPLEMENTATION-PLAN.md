# Flare Core SDK Engine 架构实施计划

目标是在保留现有收发、同步、会话和媒体能力的同时，让新增业务数据、消息类型和存储后端都能从 Engine 外部接入。实施顺序由一个约束决定：只有消息提交、确认和重放的语义稳定后，才能让更多调用方依赖这些接口。[目标边界图](assets/engine-boundary-target.png) · [架构审计与能力矩阵](CORE-SDK-ENGINE-ARCHITECTURE-AUDIT.md)

阶段编号对应审计报告第 9 节。每个工作包列出代码归属和合并前应取得的证据。涉及相邻仓库的事项按契约联调安排，分别交付。

## 执行记录

**2026-11-25，首批本地实施。** 从各仓本地 `dev` 创建 `core-engine`。本批修改 Engine（基点 `371150641d68`）和 Social SDK（`cd4e7db79fff`）。联调使用 Core SDK `a0484a63d63f`、gRPC 协议 `0828cb0deede`、Flare Protocol `621f6c66fea1`、Transport `0a7ab44a7e63`。这些版本标识各仓 HEAD，测试输入还包含实施开始前已有的工作区修改。

| 分支用途 | 已建立本地 `core-engine` 的仓库 |
| --- | --- |
| 已切换并实施 | `flare-core-sdk-engine`、`flare-social-sdk`（阶段 0）、`flare-core-sdk`、`flare-core-examples` |
| 协议与网关联调 | `flare-protocol`、`flare-protocol-grpc`、`flare-core-server` |
| 后续插件与宿主迁移 | `flare-plugin-devkit`、`flare-commercial-client-runtime`、`flare-plugins-commercial`、`flare-plugin-call` |
| 后续应用集成验收 | `flare-social-examples` |

| 工作包 | 本批变更与验证 |
| --- | --- |
| 0.1 构建与契约 | 复现 SQLite 单独启用存储 feature 时的编译错误，以及 Social 配置直接访问 Core 内部入口的边界失败。旧库测试改用 SQLx 文件连接，仍会关闭并重开同一个库。Social 通过公开 SPI 复用 Core 的 TLS 配置合并。Engine CI 加入两个 SQLite feature 组合和生成物检查，Social CI 在具有两仓源码的作业执行 SPI 边界测试。 |
| 0.2 历史数据样本 | SQLite v1 schema、旧 reaction 表、磁盘重开后的撤回状态、可信卡的 wire 类型及未注册 Custom 的原始字节均有回归用例。新进程重开后，64 条离线消息的顺序和客户端 ID 全部匹配。生产 IndexedDB host 的 16 个 Vitest 用例通过，覆盖旧 pending 键迁移及升级中止重试，使用 fake-indexeddb。另在 Chrome 158.0.8127.37 实测 v7 → v8 和刷新，校验了 pending、分页及账号隔离。多 tab 与真实配额场景见阶段 4。 |
| 1.1 发件箱 | 并发回归在旧实现复现同一待发记录返回两次。SQLite `pop` 改为在单事务内删除、取回并解码，解码失败保留原记录。可靠队列移除了每次入队仅为日志读取并解码最多 100 条消息的查询。后续批次的跨记录事务与验收结果见下方 1.1 实施记录。 |

[SQLite 旧库](../storage/sqlite-im/src/repos/schema.rs#L875) · [历史消息重开样本](../storage/sqlite-im/src/repos/message_repo/tests.rs#L93) · [Web 升级样本](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.test.ts#L227) · [并发取出与回滚测试](../storage/sqlite-im/src/repos/pending_send_repo.rs#L187)

**本地验收。** `storage-sqlite,e2ee` 通过 1,285 项，`lifecycle-sqlite,e2ee` 通过 1,297 项，两组覆盖有重叠。架构检查通过 18 项。Core 生成物检查、shared/C/Tauri 构建检查、wasm32 构建检查及 Social SDK 原生编译全部通过。WASM 编译仍有 3 条推送回执 stub 的未使用告警。远端工作流尚未运行。Engine 工作流的 actionlint ShellCheck 在分支基点与本批均报告相同的 SC2018、SC2019、SC1087，Social 工作流的 actionlint 通过。

### 1.1 实施记录 · 发信提交

本批修改 Engine、Core SDK 的 TypeScript 存储宿主和 Core Examples 的 Electron OPFS 适配器，三个仓库均在本地 `core-engine` 分支。Electron 使用同一 WASM 存储契约，因此随 Web 宿主一起接入。

| 提交边界 | 运行行为 |
| --- | --- |
| 按账号打开 | 复用现有的租户与用户存储分区。每个 `StoreProvider` 提供对应的 `OutgoingSendStore`。装配可靠队列时校验提交端口及 outbox 读写端口齐备。`survives_restart()` 在 Memory 返回 false，在 SQLite 和持久 Web 宿主返回 true。 |
| 入队 | `MessageApi::send` 的文本和无需上传的媒体直接进入可靠队列。SQLite 在同一个事务写消息、首条会话摘要和 outbox。IndexedDB 用一个跨三个 object store 的 `readwrite` 事务，事务成功后才发布热缓存和变更通知。上传本地附件时仍提供独立的上传进度，上传就绪后的入队使用同一提交。 |
| 终态 | 服务端持久确认和终态失败均先提交消息、更新摘要、移除待发，再对外发布。服务端暂时接受会保留待发记录。持久确认遇到本地写盘失败时，队列保留确认，在本地重试期间排除对应的网络重发。此时若进程退出，旧 outbox 用原客户端消息 ID 恢复。已确认消息可抵御迟到的失败结果。 |
| Electron | SQLite OPFS worker 串行处理同一连接上的请求，在一个事务内提交三类数据。首次打开旧库时修复随机键的待发记录，优先保留已有规范键，再按账号和客户端消息 ID 完成确认。 |

[提交契约](../src/domain/repository/pending_send_repository.rs) · [可靠队列](../src/runtime/reliable_queue/actor.rs) · [SQLite](../storage/sqlite-im/src/repos/message_repo/writer.rs) · [Memory](../src/infrastructure/persistence/memory_im.rs) · [Web 存储适配器](../storage/indexeddb/src/provider.rs) · [IndexedDB 宿主](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.ts) · [OPFS worker](../../flare-core-examples/apps/flare-core-electron-app/src/storage/sqliteOpfsWorker.ts)

| 验收路径 | 结果及证据 |
| --- | --- |
| SQLite 故障与重开 | 分别在入队、确认的消息、摘要、outbox 写入处中止事务，共六个断点。每次关闭连接并重开文件库，检查三类记录全部回到旧态。重试、自身消息回放及两次确认最终得到一条服务端消息和空 outbox。另经公共 `MessageApi::send` 验证首条文本入队失败后，三张表均为空。[文件库和公开入口回归](../storage/sqlite-im/src/repos/message_repo/tests.rs) |
| 队列事件与缓存 | 持久 ACK 到达后阻断 outbox 删除，观察到消息维持待发、终态事件未发布。解除故障后仅靠本地定时重试即可发布 ACK。Memory 的缓存故障测试覆盖了持久回调失败时三类旧记录的可见性。[队列回归](../src/runtime/reliable_queue/actor.rs) · [缓存回归](../src/infrastructure/persistence/memory_im.rs) |
| Web 事务与公共 API | 生产 IndexedDB 宿主在 fake-indexeddb 上通过 19 项测试，含六个写入断点和失败时零通知。Chrome 158.0.8127.37 实测生产 WASM 的 `sdk.prepare → message.create_text → message.send`，IndexedDB 和 OPFS 都通过首次拒写、重新发送和刷新后按 ID 读取。另在真实 IndexedDB 中断确认时的 outbox 删除，并在两种宿主回放自己的消息、重复提交确认、刷新核对。OPFS 旧随机键和规范键并存的文件亦通过迁移与清理。[Web 回归](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.test.ts) |
| 验证范围 | 宿主注入的写入异常代表事务失败。实际磁盘耗尽、多标签页交错写入和真实网关回执分别留给阶段 4 和 1.3。Electron 存储模块单独通过类型检查，完整示例的类型检查仍报告既有更新页面引用的三个缺失导出，分别是 `UpdateProgress`、`UpdateResourcePack`、`UpdateStatus`。Chrome 使用了示例的真实 OPFS worker，打包版 Electron 的验收仍待执行。 |

本批本地回归通过 `lifecycle-sqlite,e2ee` 1,300 项、`storage-sqlite,e2ee` 1,287 项、架构边界 18 项，以及 TypeScript Web 适配器 82 项。两组 Rust 测试包含重叠用例。`xtask core-codegen-check` 通过，生产 WASM release 构建成功。

### 阶段 1.2 实施记录

- C 和 Tauri 绑定在账号准备时选择 SQLite。Engine 通过账号开库器接收按数据根、租户、用户打开的仓储，并校验原子发信端口与 outbox 读写端口。SQLite IM 适配器拥有 SQL、schema 迁移、队列事务及密钥装配，SQLite 连接底座仍可独立复用。Social 沿用 Native 的账号分区。 [账号契约](../src/client/lifecycle.rs) · [SQLite IM 适配器](../storage/sqlite-im/src/lib.rs)
- 同一份样本固定两条消息的正文、发送者、会话、客户端 ID 和逻辑时间。C 退出进程后重新装配 SQLite，生产 WASM 关闭并重新启动 Chrome 后打开同一 IndexedDB。两端完整消息和会话 DTO 逐字段相等，待发身份和顺序与样本一致。测试同时断言发送态、未读数、摘要与时间，覆盖首次创建会话及第二条消息推进摘要。[样本及驱动](../tests/storage_parity/run.mjs) · [复跑命令](../storage/README.md#重开发信对测)
- 对测发现并修正了新会话默认业务类型、创建和更新时间的端间差异。热缓存与 SQLite 共用默认类型规则，IndexedDB 在已有摘要上合并单调更新时间。[内存事务](../src/infrastructure/persistence/memory_im.rs) · [生产宿主](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.ts)
- 依赖守卫使用 Cargo 普通依赖闭包覆盖所有 feature 和 target。Engine 的 SQLite 测试 feature 仅选择本仓的原生回归样本。真实网关 ACK、断网重连及服务端补偿列入 1.3。[依赖守卫](../tests/architecture_boundary_test.rs) · [缺失端口回归](../tests/prepare_local_first_test.rs)

SQLite schema 版本仍为 v6，迁移 SQL 与拆分前一致。既有插件表随适配器保留，业务数据归属的迁移按后续阶段实施。Engine 的私有队列、同步回放回归在原生测试配置下编入同一份适配器源码，以便访问测试构建的类型。适配器也作为独立 crate 运行测试，两组结果有重叠。

| 1.2 验收项 | 实测结果 |
| --- | --- |
| 普通依赖闭包 | Engine 全 feature、全 target 的普通依赖中，具体存储后端匹配数为 0。C 命中 SQLite IM 与连接底座，生产 WASM 命中 IndexedDB 适配器。 |
| 同样本重开 | macOS C 动态库与 release WASM + Chrome 均读回两条发送中消息、一个会话、两个待发项。消息列表按时间倒序，发件箱按入队先后恢复。样本客户端 ID 的字典序与入队顺序相反。 |
| 自动回归 | Engine 单测 1,301 项、架构边界 19 项、账号准备 2 项、消息身份 2 项、独立 SQLite IM 适配器 92 项、共享绑定 75 项通过。适配器全 target Clippy（`-D warnings`）通过。TypeScript SDK 295 项通过，其中包含生产 IndexedDB 宿主的 19 项，类型检查通过。 |
| 消费方 | Engine Tauri、Social 原生绑定与 WASM、Social Rust 示例、商业接入实验工具和红包服务端测试目标通过编译。Core / Social 两个 Tauri 示例的工作区源码依赖锁均已更新。 |

### 阶段 1.3 实施记录 · 真实网关 ACK、自动重连与消息补偿

2026-11-25，Engine 和 Core Server 在各自的 `core-engine` 分支本地联测。C 动态库使用 SQLite，release WASM 在 Chromium 中使用生产 IndexedDB 宿主。两个客户端分别与独立对端单聊，使用同一份 1.2 样本的客户端消息 ID、文本和草稿时刻。每次运行分配新账号和真实会话，以隔离服务端幂等记录。本次连接本机隔离的 Core 网关、消息服务和 PostgreSQL。[样本](../tests/storage_parity/send.json) · [可执行验收](../tests/storage_parity/live.mjs) · [复跑方式](../storage/README.md#真实网关-ack重连和补偿对测)

| 观测点 | C / 生产 WASM 的结果 |
| --- | --- |
| 在线发送 | 第一条取得公开绑定事件 `stage=persisted`。事件的服务端 ID 和序号与该租户、该会话下唯一的已提交行相同。 |
| 物理断链 | TCP 中继关闭活动 WebSocket 并拒绝重连。第二条留在本地持久发件箱，序号为 0；独立对端发送见证消息并取得持久 ACK，此时服务端有两行，断网客户端查询见证消息为空。 |
| 自动重连和补偿 | 恢复转发后，每端新建网关连接。同一轮重连同步先报告 `ForegroundFresh`，再报告 `Converged`。本地补回见证消息，离线样本取得持久 ACK。全程只调用一次连接接口。 |
| 三项状态 | 正序为在线样本 1、断线期间的来信 2、补发样本 3。时间线无重复，三条均为 `sent`。会话摘要指向序号 3 的服务端 ID，`lastReadSeq=1`、`unreadCount=1`、`maxSeq=3`、`participantVersion=2`。持久发件箱为 0 项。 |
| 断网冷重开 | C 更换进程，WASM 关闭并重新启动同一浏览器配置目录。只执行本地准备。每端的逐条查询、时间线、会话完整 DTO 和发件箱与该端重开前逐字段相等。 |

一次通过的实测中，C 的三次持久 ACK 依次对应服务端 ID `e44c0d87-3fb3-4084-b801-021c66ba2f5f`、`c4145758-01e3-4e27-b5da-26dfe2079b95`、`05957897-c502-44a4-86ac-096b4cef7985`。WASM 场景对应 `7720c6ac-61b3-4d27-b96d-b925d70a68f3`、`dcbd1e4b-e939-4bbe-8cbd-d5e72ff66fe8`、`f0bf214a-5739-4fa6-99ce-7891a8f3e89d`。两组的第二个 ACK 均来自独立对端。

**对照口径。** 跨端比较映射独立会话生成的身份和运行时钟；消息创建时刻逐条与数据库核对，排序时间与消息和摘要的关系另有断言。实测中两端恢复后的 `clientCreatedAt` 随服务端消息时刻。私有投影收据的 `summarySeq` 记录本设备最后一次从远端摘要计入的进度。复跑曾观察到 C 为 1、WASM 为 2；两端已应用区间均为 [1, 3]、物化位点均为 3，未读均为 1。驱动输出这个数值、检查其范围，并在本端冷重开时要求原值相同。[收据合并语义](../src/domain/conversation/message_projection.rs)

**联测推动的修正。** 缺字段的摘要曾清空 WASM 单聊类型和排序时间，SQLite 会话读取曾固定返回成员版本 0。共享摘要合并现在保留已知分类和排序时刻，部分更新沿用已有成员预览，过期预览无法覆盖较新的版本。SQLite v7 单独持久化摘要宣告的成员版本，v6 旧行以 0 表示未知，后续同步填充。服务端历史读取现在保留字符串属性原文，实时发布由已校验的请求租户填入与存储一致的属性。联测同时核对消息属性内的会话标识、请求上下文和租户。[共享摘要](../src/domain/conversation/summary_merge.rs) · [SQLite 迁移](../storage/sqlite-im/src/repos/schema.rs) · [消息接入](../../flare-core-server/flare-message-ingest/src/application/handlers/message_ingest_handler.rs) · [历史读取](../../flare-core-server/flare-storage/reader/src/infrastructure/persistence/postgres_base.rs)

最终版联测连续两轮更换账号通过，阶段 1.2 的离线重开样本也通过。Rust 默认配置 Engine 单测 1,142 项、SQLite IM 适配器 93 项通过。Server 的 Ingest 单测 73 项、Reader 单测 45 项通过，两套分别有 1 项和 3 项已标记忽略。生产 WASM 的 raw 4,973,867 B、gzip 1,769,301 B 均在仓库预算内。

### 阶段 1.3 故障验收 · 提交、回执丢失和离线推送交接

C 动态库和生产 release WASM 都使用上面的两条发信样本。每组使用新账号和单聊，固定客户端 ID、正文和草稿时间。中继双向解码真实网关帧，PostgreSQL 独立连接核对消息 ID、序号和提交结果。[样本](../tests/storage_parity/send.json) · [故障对测](../tests/storage_parity/faults.mjs) · [复跑命令](../storage/README.md#真实网关-ack重连和补偿对测)

| 故障窗口 | 客观证据 | C / 生产 WASM 恢复结果 |
| --- | --- | --- |
| 网关提交成功，发送者丢失持久 ACK | 中继持有第一条样本的 `SendAck.accepted.durability=PERSISTED`。独立查询匹配服务端 ID 和 seq 1。客户端仍为 sending，持久发件箱只有这条记录。 | 冷重开保留原队列，重连以原服务端 ID 收敛。数据库和本地时间线各只有一条该 ID 的消息。 |
| 发送方提交中崩溃 | 第二条样本写入 SQLite `pending_sends` 后，观察未提交 WAL 帧并终止 C 进程。IndexedDB `wasmPendingSends.put` 成功、事务仍活动时使渲染进程崩溃。 | 断网重开后，第二条消息和队列项均不存在，旧消息和会话完整 DTO 与故障前一致。调用方用第二条原 ID 重试成功。 |
| 实时链路交给离线推送 | 对端发送见证消息。中继截获目标设备的实时下行，当时匹配的离线任务为 0。断链后服务端决策链记录该设备的 `handoff / link_lost` 和未确认条目，Worker 流产生唯一一条匹配设备、消息 ID 和序号的 `LOUD` 任务。 | 同步补回见证消息。最终 3 条消息顺序为原发、对端来信、重试。未读 1、读位 0、maxSeq 3、发件箱 0。 |
| 接收方提交后丢送达 ACK，下一次收信在提交中崩溃 | 反向发送同一对样本，中继丢弃设备上行的 `ConversationAck.lastDeliveredSeq=1`。第二条在 SQLite 消息 INSERT 或 IndexedDB 消息 put 后、提交前终止接收端；活动事务跨过两个送达 ACK 合并窗口，线路上没有 seq 2 的送达 ACK。 | 断网冷读只有第一条，未读 1，摘要仍指向第一条。重连后两条与服务端逐一对应，未读 2、读位 0、maxSeq 2、发件箱 0。最终再断网重开，每端完整状态逐字段相等。 |

2026-11-25 的一次复跑中，推送任务的消息 ID 分别为 C 的 `4e962b7b-6094-4276-9034-2089884ef01d` 和 WASM 账号的 `f1515417-b6ef-4a98-ac69-ab2e5d4af74b`，均对应各自会话的 seq 2。更换账号复跑后，两端通过完整状态比较。原有自动重连补偿和纯离线重开基线也通过；同次执行的 Engine 库测试 1144 项、SQLite IM 适配器库测试 93 项全部通过。

发信场景的读位与上一节不同。首条发信在本机取得持久确认之前即断链；客户端没有执行标记已读。旧逻辑把“最大序号 2、未读 1”推成读位 1，实测导致服务端插入额外的已读事件并占用 seq 3。现在有未读时只上报已存读位；测试同时要求服务端事件序列只有三条消息。收信场景还暴露了浏览器会话缺少排序时间，投影提交时统一按最后消息时间单调更新 `updatedAtTs`。实时 `MessagePush` 路径的送达确认交给同一持久化任务，消息和会话投影都成功才发送；有界串行 worker 继续批量落盘。[读位策略](../src/domain/conversation/read_position.rs) · [跨端投影](../src/domain/conversation/message_projection.rs) · [持久化与确认](../src/runtime/dispatcher/mod.rs)

WASM 场景的原生推送绑定在同账号另一台 C 测试设备上，浏览器从 IndexedDB 和同步恢复。接管证据止于本地推送任务，厂商投递与系统通知展示仍需真机验证。本组覆盖消息写入事务中的进程崩溃。下一组在消息行写入成功后继续验证摘要提交和游标推进。[设备交接观测](../tests/storage_parity/fault_wire.mjs) · [事务故障注入](../tests/storage_parity/clients.mjs)

### 阶段 1.3 故障验收 · 收信消息、会话摘要和游标分次提交

2026-11-25，Engine `core-engine` 分支。独立对端向 C / SQLite 或 release WASM / Chromium IndexedDB 依次发送同一份 1.2 样本。每轮使用新账号和会话。第一条先完成投影，实际持久化的会话同步游标到 1 后，才允许发送第二条。每条的网关持久 ACK 均与 PostgreSQL 的服务端 ID、序号相符。[样本](../tests/storage_parity/send.json) · [验收代码](../tests/storage_parity/projection.mjs) · [复跑命令](../storage/README.md#真实网关-ack重连和补偿对测)

| 崩溃窗口 | 断网重开，C / SQLite | 断网重开，生产 WASM / IndexedDB | 重连补偿后，两端实测 |
| --- | --- | --- | --- |
| seq 2 的消息已提交，完整的会话投影尚未提交 | 2 条消息。末条预览及 `maxSeq` 为 2，投影完成位点 1，未读 1，游标 1，发件箱 0 | 2 条消息。末条预览及 `maxSeq` 为 1，投影完成位点 1，未读 1，游标 1，发件箱 0 | 2 条消息，摘要指向第二条，未读 2，读位 0，投影区间 [1, 2]，游标 2，发件箱 0 |
| seq 2 的会话投影已提交，游标写入尚未提交 | 2 条消息。末条预览、投影完成位点、未读均为 2，游标 1，发件箱 0 | 与 C 相同 | 游标补到 2。消息、摘要和未读维持已提交结果，发件箱 0 |

**如何识别故障窗口。** 第一组在目标会话即将提交 `materializedSeq=2` 的投影写入时中断；第二组在当前账号、当前会话即将提交 `last_seq=2` 的游标写入时中断。SQLite 临时库用定点触发器和可观察的未提交 WAL 帧锁定写入位置，随后向该 C 驱动发送 `SIGKILL`。浏览器在目标对象仓库的真实 `put` 成功后保持事务活动，再使渲染进程崩溃。新进程在 `sdk.prepare` 之前分别使用只读 SQLite 或 IndexedDB 事务读取消息行、会话行、游标和发件箱，随后在断网状态读取公开 SDK 接口。[SQLite 注入](../tests/storage_parity/crash_sqlite.py) · [绑定与 IndexedDB 注入](../tests/storage_parity/clients.mjs)

**修复前第一个窗口的差异。** SQLite 的消息批写在同一事务内更新末条消息快照，未读数和投影完成记录在下一笔投影提交时更新。WASM 中两条消息行已持久化，新的会话摘要仍待提交。断网时两端公开查询都能列出两条消息，此时预览的差异对调用方可见。连续同步游标取自独立的游标存储，两端均为 1。[SQLite 消息批写](../storage/sqlite-im/src/repos/message_repo/writer.rs) · [IndexedDB 消息与会话写入](../storage/indexeddb/src/provider.rs) · [连续游标校验](../src/runtime/dispatcher/seq_repair.rs)

恢复连接后等待登录同步报告 `Converged`，用服务端行校验完整消息、时间线顺序及摘要正文、发送者和时间，再核对未读数、投影区间、持久化游标和空发件箱。跨端比较归一化各自账号、会话、服务端 ID 和时钟。私有 `summarySeq` 沿用前述范围校验，同端冷重开保留原值。同步游标序号要求精确相等，每端还验证游标时间单调，并在再次断网冷重开时原值相等。更换账号复跑，C 和生产 WASM 的两个窗口均通过。原有的丢 ACK、事务中崩溃、推送接管、自动重连和纯离线重开回归也通过。本组改动集中在验收驱动与观测，所有操作事件统一接入 `ApplyInbound` 仍待实施。

### 阶段 1.3 实施记录 · ApplyInbound 与断网末条预览

实时 `MessagePush`、消息事件及其批次、单会话增量页和快照消息共用 `ApplyInbound`。准备阶段按账号规范化消息身份，合并本机回声，检查会话可见下界，将临时通知从持久批次中分出。提交阶段先保存消息行和末条预览，再提交未读变化及本地投影完成记录。实时有界队列仍负责背压和送达确认，同步页仍按连续证据推进游标。操作类事件沿用各自的事件应用器。[共用用例](../src/application/usecases/inbound.rs) · [实时入口](../src/runtime/dispatcher/dispatch.rs) · [补偿入口](../src/application/usecases/sync/mod.rs)

SQLite 已在消息事务中维护预览。浏览器现在在一个 IndexedDB 读写事务内保存消息行和受影响会话的预览，成功后才将已提交的预览写入内存。摘要的未读数、设置和投影记录保持各自的提交语义。宿主升级到数据库版本 9 时按会话探查一个时间线索引键，对旧库中消息尾部超出摘要水位的会话，在账号打开时使用 Rust 消息模型修补预览。[IndexedDB 适配器](../storage/indexeddb/src/provider.rs) · [生产 Web 宿主](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.ts)

| 同一份发信样本，C / SQLite 与生产 WASM / IndexedDB | 消息序号 | 末条预览 / maxSeq | 未读数 / 已读位点 | 投影完成位点 / 实际同步游标 | 发件箱 |
| --- | --- | --- | --- | --- | --- |
| 消息已提交、未读投影提交前崩溃，随后断网重开 | 1、2 | 第二条 / 2 | 1 / 0 | 1 / 1 | 空 |
| 未读投影已提交、游标提交前崩溃，随后断网重开 | 1、2 | 第二条 / 2 | 2 / 0 | 2 / 1 | 空 |
| 两个窗口各自重连补偿完成，并再次断网重开 | 1、2 | 第二条 / 2 | 2 / 0 | 2 / 2 | 空 |

对测先于 `sdk.prepare` 读取真实库行，再通过断网 SDK 的消息详情、时间线、会话详情和列表核对预览。末条正文必须为样本的「Reopen · 第二封」，其 ID、发送者和时间须与本轮服务端第二条提交记录相符。首次断网及恢复后的跨端状态都参加比较，沿用上节的动态身份、时钟和私有摘要水位归一化。两个窗口各自的游标写入时间在断网重开时保持原值。[发信样本](../tests/storage_parity/send.json) · [故障对测](../tests/storage_parity/projection.mjs) · [复跑命令](../storage/README.md#真实网关-ack重连和补偿对测)

2026-11-25，在两个仓库的 `core-engine` 分支用新账号独立复跑两轮，两轮均得到上表结果。纯离线发信重开、真实网关重连补偿、丢 ACK、事务中崩溃和离线推送接管的串行回归均通过。另一次让重连套件和崩溃套件共用同一测试网关、同时发送相同客户端消息 ID 时，重连套件收到持久 ACK 后立即查询本会话得到 0 行；后续定点复现与修复见下节「同网关并发持久回执」。Engine 单测 1144 项、SQLite IM 单测 93 项、架构边界 19 项和 IndexedDB 宿主单测 21 项通过。

### 阶段 1.3 实施记录 · 同网关并发持久回执

C 动态库与 Chromium 中的 release WASM 各使用独立会话，共用同一 WebSocket 网关和租户。分别验证两个独立发送账号，以及同一发送账号的两个设备向不同对端发信。两个会话同时使用原发信样本的 `phase12-client-z` 和 `phase12-client-a`。先在 WASM 会话写入一条见证消息，使两端序号错开，防止把属于另一个会话的序号误判为正确。[原始样本](../tests/storage_parity/send.json) · [可执行验收](../tests/storage_parity/concurrent_ack.mjs)

| 观测点 | 修复前实测 | 修复后验收 |
| --- | --- | --- |
| 阻塞 Writer 的消息 INSERT，允许独立 SELECT | 两端原始回执均为 `PERSISTED(4)`，SDK 均发布 persisted，各自会话 0 行。释放写锁后，两条记录的会话、服务端 ID、序号均与原 ACK 一致 | 两端线上回执为 `BROKER_ACCEPTED(3)`，SDK 发布 accepted，各自 0 行；持久 ACK 数量为 0 |
| 释放锁、再次确认同一次发送 | 两条持久回执已在提交前发出 | 网关持久 ACK 到达、尚未转发给 SDK 时立即查本会话；每个样本恰好 1 行。逐条比对该行、原始回执及 SDK persisted 事件的会话、服务端 ID 和序号 |
| 第二个样本正常并发发送 | — | 同样立即核对三方。两端相同的客户端 ID 分别对应独立服务端 ID；C 两条 seq 为 1、2，WASM 两条为 2、3 |


2026-11-25 使用新账号执行的逐条对账如下。表中三项身份字段在原始持久回执、SDK persisted 事件、PostgreSQL 唯一行和发送设备按服务端 ID 读取的本地消息中均一致。每次确认后发送设备的发件箱为空。

| 发送账号 | 绑定 | clientMsgId | 会话 ID | 服务端消息 ID | 会话序号 |
| --- | --- | --- | --- | --- | ---: |
| 独立账号 | C | `phase12-client-z` | `1AJK3Z7YC0WFBA8TFZ` | `beeae7b9-fd40-49cd-af61-f5f7aa6ae687` | 1 |
| 独立账号 | 生产 WASM | `phase12-client-z` | `1AQB13ZRA1PKRRC26N` | `46688c6d-0bbc-4d91-ba15-e847f4f4e450` | 2 |
| 独立账号 | C | `phase12-client-a` | `1AJK3Z7YC0WFBA8TFZ` | `ed74b68e-847d-407a-84d9-c302084f5359` | 2 |
| 独立账号 | 生产 WASM | `phase12-client-a` | `1AQB13ZRA1PKRRC26N` | `cced64f8-9e51-4e03-8981-6f24130aa4aa` | 3 |
| 同账号双设备 | C | `phase12-client-z` | `1A3FJCX7FKCW5DB0FD` | `e27e1392-eedf-437a-97c1-7ef2c51ae02b` | 1 |
| 同账号双设备 | 生产 WASM | `phase12-client-z` | `1AT7GV6VDP6FGVR7NS` | `5dbe895b-5f9a-481c-951e-b3e71bb12535` | 2 |
| 同账号双设备 | C | `phase12-client-a` | `1A3FJCX7FKCW5DB0FD` | `59fe6af2-c760-449a-a24f-2abef181b04b` | 2 |
| 同账号双设备 | 生产 WASM | `phase12-client-a` | `1AT7GV6VDP6FGVR7NS` | `1372983c-6aaf-4cdf-91d9-5dd65b88ae39` | 3 |

摄入服务曾在 WAL 与 JetStream 接受消息后同步返回 Persisted；Storage Writer 此时仍可能在排队。该复现中，提交后回执均属于原会话。同时，Engine 的实时自回声会在本地待发消息上合成 Persisted，实时推送本身缺少提交凭据。

摄入服务现在先记录队列或 WAL 的实际阶段，按租户、发送者、会话和客户端 ID 查发送账本并联查 `messages`。持久回执取已提交消息行的服务端 ID 和正序号。查询和等待共用 350 ms 上限，最多四次点查；已受理发送在读取失败、未配置数据库或 Writer 落后时保留受理阶段，原 ID 重试可再确认。Redis 旧回执也须核对。签名卡片额外要求已提交 ID 与序号均符合原签名发送的受理结果。[发送处理器](../../flare-core-server/flare-message-ingest/src/application/handlers/message_ingest_handler.rs) · [提交凭据查询](../../flare-core-server/flare-message-ingest/src/infrastructure/persistence/postgres_idempotency_ledger.rs)

Engine 将入站来源传入 ApplyInbound。待发消息的实时自回声保留原发件箱，持久回执或已归档的同步记录负责完成发送。可靠队列核对本地待发会话，阻止其他会话同名客户端 ID 的回执或本地快照推进状态。[入站收敛](../src/application/services/incoming_message_converger.rs) · [可靠发送队列](../src/runtime/reliable_queue/actor.rs)

回归中另按原问题的运行方式，将真实网关重连套件与丢 ACK、崩溃及离线推送套件同时接到同一网关。两组均通过。两个投影崩溃窗口的断网重开及补偿对测、纯离线重开对测也通过。Engine 单测 1144 项通过；摄入服务单测 73 项、账本集成用例 7 项通过，另有 1 项依赖独立 Hook 测试库的用例按既有配置忽略。新增点查在不同并发量下的测量见下节。

### 阶段 1.3 实测 · PostgreSQL 持久 ACK 核验

2026-11-25 在 `core-engine` 工作树上，使用同一真实 WebSocket 网关、摄入服务、JetStream、Storage Writer 和 PostgreSQL 16.15。主机为 Apple M1 Pro、16 GiB、macOS 26.6.2，数据库所在独立虚拟机分配 4 vCPU、5 GiB。Rust 服务与 C 动态库使用本地 dev 构建，浏览器验收使用 Chromium 加载 release WASM。摄入服务核验连接池上限为 2，单次核验的总预算为 350 ms，包含取连接、点查和重查间隔。[连接池](../../flare-core-server/flare-message-ingest/src/infrastructure/persistence/postgres_idempotency_ledger.rs) · [确认逻辑与计时点](../../flare-core-server/flare-message-ingest/src/application/handlers/message_ingest_handler.rs)

1、4、16、32 个已连接的 C 发送会话分别构成闭环负载。每个发送者在收到网关应答及 SDK 最终事件后才发下一条。先预热全部连接和会话，再按升序、降序各跑一轮，每档每轮 1,024 条。两条样本轮流发送，保留文字和原始创建时间，客户端 ID 追加轮次、并发档及序号，使每次逻辑发送都产生一条新消息。账号准备、预热、抓取指标和批后 SQL 对账位于计时窗口外。[同一份发信样本](../tests/storage_parity/send.json) · [负载驱动](../tests/storage_parity/ack_load.mjs)

| 并发 | 持久 ACK/s（两轮合并） | 第一轮 / 第二轮 ACK/s | 核验 P95 / P99 ms | 网关 P95 / P99 ms | SDK P95 / P99 ms | 核验 SQL 次数 | 次/条 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 23.02 | 24.38 / 21.81 | 27.932 / 28.749 | 36.96 / 40.50 | 42.58 / 47.27 | 4,099 | 2.001 |
| 4 | 82.92 | 86.41 / 79.70 | 31.466 / 90.874 | 43.84 / 100.36 | 48.69 / 104.23 | 4,122 | 2.013 |
| 16 | 108.77 | 113.64 / 104.30 | 111.535 / 136.803 | 173.71 / 204.70 | 175.94 / 210.46 | 2,182 | 1.065 |
| 32 | 106.88 | 110.71 / 103.30 | 165.052 / 243.395 | 312.35 / 402.08 | 315.99 / 405.31 | 2,070 | 1.011 |

吞吐按两轮确认总数除以两轮墙钟时间之和计算。每档合并 2,048 个原始延迟样本，排序后以 nearest-rank 取 P95、P99。核验耗时使用摄入服务的单调时钟，在写 trace 前取值。逐窗核对 trace 样本数、服务端直方图的 count 与 sum，以及真实网络上行次数。网关延迟由同一中继从首次消息上行计至原始持久回执抵达。SDK 延迟从该上行计至公开的 persisted 事件。[实测采样与对账](../tests/storage_parity/ack_load.mjs)

8,192 条样本均有网关持久回执，均与各自会话中唯一 PostgreSQL 消息行的发送者、服务端消息 ID 和序号吻合。正式计时窗口内归档核验超时为 0，额外上行重试为 0。每档表内 SQL 次数均取自本库 `pg_stat_statements.calls` 对核验 JOIN 的窗口差值，四档共 12,473 次。旧创建时间另触发 8,192 次发送前幂等回查，单独计数。网关和 SDK 端到端耗时包括核验范围以外的排队和传输。

此机器上 16 并发取得本组最高吞吐。升到 32 时，每条核验 SQL 降至 1.011 次，核验 P99 升到 243.395 ms，网关 P99 升到 402.08 ms。点查次数和尾延迟走向不同，本次测量未单独分解连接获取、Writer 排队和网关排队的耗时。该结果适用于上述本机闭环负载。

#### 超过 350 ms 后的真实重试

同一份发信样本覆盖两种恢复方式。C 与生产 WASM 在不同会话中同时发送相同的原始 `clientMsgId`。分别用 1 条、2 条预热消息初始化会话序号，再用独立测试库的事务锁阻塞 Writer 与核验 SELECT。每轮摄入服务 `archive_receipt{outcome="timeout"}` 增加 2。此时两端线上只收到 BrokerAccepted，SDK 公开状态为 accepted，发送账本尚未提交，各自的持久发件箱都保留原消息。[故障注入与逐条断言](../tests/storage_parity/ack_timeout.mjs) · [数据库锁与服务指标](../tests/storage_parity/archive_receipts.mjs)

- **原连接重试**。通过公开配置把两端 SDK 的 ACK 等待时间设为 2 秒。首次受理后恢复读操作，继续阻塞归档写入，直到中继在两条原连接上捕获 SDK 自行重发的原始帧。此时再次核对数据库中尚无对应消息、两端均未发出 persisted 事件，再放行写事务和缓存的上行帧。表中分别列出重试帧的捕获与转发时间。
- **冷重开重试**。首次受理后断开中继并关闭两端。离线期间放行 Writer，查到唯一提交行后，在原 SQLite 数据目录和浏览器配置目录重开，验证持久发件箱仍保留原 ID。两端沿用默认 ACK 等待配置，重新连接后由 SDK 在新连接上重发。表中计时包含关闭和重开的耗时。

每个样本在每个绑定、每种恢复方式下只调用一次公开发送接口。以下时间从首次真实上行起算。中继在收到持久 ACK 后、向客户端转发前即读取已提交消息行。

| 恢复方式 | 绑定 | clientMsgId | 会话 ID | 服务端消息 ID | seq | 首次受理 ms | 重试捕获 / 转发 ms | 持久 ACK ms |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| 原连接 | C | `phase12-client-z` | `1ASEXGKPFXPJ49HF64` | `28f6dd26-b4f4-4efd-a008-74290a73d203` | 2 | 361 | 2919 / 3064 | 3073 |
| 原连接 | 生产 WASM | `phase12-client-z` | `1A9MFEP44ARCRJ1NWJ` | `9a47868a-6b91-4ca6-8f94-7c976b114dd0` | 3 | 359 | 2919 / 3062 | 3107 |
| 原连接 | C | `phase12-client-a` | `1ASEXGKPFXPJ49HF64` | `09dc3551-64e2-4ca8-889b-3fc6463196e2` | 3 | 360 | 2386 / 2669 | 2716 |
| 原连接 | 生产 WASM | `phase12-client-a` | `1A9MFEP44ARCRJ1NWJ` | `9f30fa61-2896-4752-ba63-f2148b6a9fa0` | 4 | 359 | 2387 / 2667 | 2677 |
| 冷重开 | C | `phase12-client-z` | `1AD56E2D9CV3S36W2H` | `8bf0294a-ab49-4402-b7cf-99382db58b0f` | 2 | 364 | 863 / 863 | 877 |
| 冷重开 | 生产 WASM | `phase12-client-z` | `1A0RFMVCRG11FXGCDZ` | `3c3ff0ef-b21f-469f-988a-d7d6011541c9` | 3 | 362 | 1707 / 1707 | 1715 |
| 冷重开 | C | `phase12-client-a` | `1AD56E2D9CV3S36W2H` | `e3dc7b9e-6fa4-4c7b-8941-a3dabbea522a` | 5 | 364 | 854 / 854 | 866 |
| 冷重开 | 生产 WASM | `phase12-client-a` | `1A0RFMVCRG11FXGCDZ` | `658e735a-233e-47e3-86fb-59692332ecc7` | 6 | 361 | 1672 / 1672 | 1688 |

8 条样本各观察到两次上行和一条唯一的已提交消息。首次受理与重试持久 ACK 中的会话、服务端消息 ID 和序号均与该行吻合。SDK 公开持久事件和按服务端 ID 查回的本地消息也逐条一致，最后发件箱均为空。读回执占用会话事件序号，因此重开前后以实际落库序号对账。另一次对照在原连接上立即放行归档，两端均由同步完成发送，观察到公开 persisted 事件、一次上行和一条 BrokerAccepted。压测单独统计这种完成方式。

### 阶段 1.3 实测 · 持久 ACK 尾延迟分解

**32 并发下，两次账本读取争用摄入服务的 2 个 PostgreSQL 连接。** 在复现上节长尾的同一栈中，网关最慢 1% 的 21 条持久 ACK 平均用时 458.01 ms。其中，发送前的历史幂等回查取连接平均 218.86 ms，受理后的持久 ACK 核验取连接平均 211.58 ms。两个阶段对同一个请求依次发生，合计占 94.0%。同一批慢请求的网关入站队列平均 0.23 ms，核验 SQL 往返平均 6.72 ms。Writer 归档事务平均 42.26 ms，其中 COMMIT 往返 8.38 ms。上一批 402.08 ms 是历史分位数，当时没有逐条分段数据；本次按关联帧定位到可复现的瓶颈。

**计时口径。** 2026-11-25，沿用两条原样本的正文与创建时间，压测时为每次新发送加唯一后缀，断线和超时验收保留原 ID。32 个 C 会话预热，每档 1,024 条，按 16→32、32→16 各测一轮。所有服务运行在同一 macOS 主机，数据库为独立 VM 内的 PostgreSQL 16.15 / TimescaleDB 2.30.0，4 vCPU、5 GiB，每个 Rust 服务两条 Tokio 工作线程。每个消息先在 WS 中继记录首次上行，再以连接 ID 和帧 ID 匹配网关队列，以窗口内唯一的客户端 ID 匹配 Writer 和摄入记录。另按发送者和会话查询唯一已提交消息行，核对服务端 ID 与序号。[负载与对账](../tests/storage_parity/ack_load.mjs) · [采样与关联](../tests/storage_parity/ack_stages.mjs)

下表在同一隔离栈上对比默认池上限。每个格子为 P95 / P99，单位 ms。入站排队从传输层入队到消费者出队。Writer 事务包含取写连接、SQL 与提交，COMMIT 是其中的一个子阶段。读池等待从申请到取得连接，包含 SQLx 的连接检查。SQL 往返从取得连接计至驱动收到查询结果，每条逻辑消息先累加各次点查，再取分位数。Writer 与核验可同时进行，表中各列不能相加。明细中的 `wireToCommit` 和 `commitToAck` 以同宿主时钟对齐中继和 Writer 提交返回后的观测时刻，包含跨进程的排队和传输；其余阶段均由各自的单调时钟直接计时。

| 池上限 | 并发 | 入站排队 | Writer 事务 | COMMIT 往返 | 历史回查取连接 | 核验取连接 | 核验 SQL 往返 | 整段核验 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 16 | 0.28 / 0.63 | 35.72 / 50.27 | 10.36 / 17.23 | 73.53 / 106.09 | 94.92 / 117.79 | 7.92 / 13.88 | 116.73 / 144.82 |
| 2 | 32 | 0.45 / 1.43 | 37.91 / 56.34 | 10.99 / 17.95 | 171.08 / 222.88 | 178.16 / 221.85 | 7.11 / 10.84 | 183.49 / 226.76 |
| 4 | 16 | 0.33 / 0.95 | 45.76 / 59.70 | 12.93 / 19.89 | 35.09 / 46.06 | 67.49 / 82.19 | 17.23 / 22.82 | 154.94 / 179.59 |
| 4 | 32 | 0.62 / 3.51 | 58.41 / 78.38 | 17.64 / 27.08 | 113.26 / 192.00 | 179.55 / 212.44 | 14.76 / 22.97 | 210.57 / 255.20 |

**落地变更。** 摄入服务将历史回查与提交凭据核验共享的懒连接池默认上限调为 4。每个摄入实例可用正整数环境变量 `MESSAGE_INGEST_LEDGER_MAX_CONNECTIONS` 指定自己的连接预算。保留 SQLx 连接健康检查、350 ms 核验总预算、四次点查上限及账本与消息行 JOIN。对照中，上限 8 的 32 并发有 74/2,048 条由同步单独完成，SDK P99 为 637.53 ms。另一次减少热连接健康检查的试验也出现超时。最终构建保留完整健康检查。[池配置](../../flare-core-server/flare-message-ingest/src/infrastructure/persistence/postgres_idempotency_ledger.rs) · [装配入口](../../flare-core-server/flare-message-ingest/src/wire.rs)

**吞吐与端到端延迟。** 同栈组按 2、8、4 依次重启摄入服务。另对上限 2 和最终默认 4 各用一套从空数据库、空队列和空客户端状态启动的栈，使用同一服务二进制。吞吐只统计客户端中继实际收到的网关持久 ACK，分母包含由同步完成发送的耗时。网关分位数仅含收到该帧的样本，SDK 分位数包含全部 2,048 条。各轮独立查库，核验 SQL 次数取 PostgreSQL 执行计数差值。

| 环境 | 池 | 并发 | 持久 ACK/s | 网关 P95 / P99 ms | SDK P95 / P99 ms | 网关持久 ACK / 同步单独完成 | 核验超时 | 核验 SQL 次数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 同栈 | 2 | 16 | 103.31 | 190.22 / 225.44 | 193.10 / 228.83 | 2,048 / 0 | 0 | 2,211 |
| 同栈 | 2 | 32 | 99.35 | 366.97 / 430.46 | 368.88 / 434.96 | 2,048 / 0 | 0 | 2,049 |
| 同栈 | 4 | 16 | 103.99 | 190.85 / 218.19 | 194.59 / 223.93 | 2,047 / 1 | 1 | 4,334 |
| 同栈 | 4 | 32 | 113.45 | 323.13 / 376.39 | 326.03 / 380.11 | 2,048 / 0 | 0 | 2,475 |
| 空库 | 2 | 16 | 76.44 | 240.08 / 301.42 | 244.69 / 306.92 | 2,047 / 1 | 1 | 2,271 |
| 空库 | 2 | 32 | 37.54 | 765.56 / 861.00 | 1,137.72 / 2,213.01 | 1,608 / 440 | 440 | 1,691 |
| 空库 | 4 | 16 | 66.55 | 304.54 / 409.55 | 332.60 / 721.71 | 2,021 / 27 | 27 | 4,274 |
| 空库 | 4 | 32 | 89.79 | 367.50 / 431.85 | 378.39 / 472.23 | 2,034 / 14 | 14 | 2,618 |

同栈 32 并发下，两组都收到全部 2,048 条持久 ACK。上限 4 将吞吐提高 14.2%，网关 P99 降低 12.6%。上限 2 时 PostgreSQL 内核执行核验 JOIN 的均值为 0.051 ms/次，来自 `pg_stat_statements.total_exec_time / calls` 的窗口差值。增加池上限后，1% 慢请求中历史回查取连接均值降至 114.94 ms，核验取连接均值为 216.57 ms，后者仍占主要等待时间。空库对照期间观测到宿主编译任务与 IDE 同时消耗 CPU，绝对值波动较大。对照栈数据分别保留，不能用跨组的最快和最慢读数计算收益。空库组 32 并发的归档核验超时由 440 次降至 14 次，16 并发却从 1 次升至 27 次，吞吐也回落；这组结果支持 32 并发的改进，16 并发的收益尚不稳定。对于触发 350 ms 截止的请求，已取消的读取没有完整 SQL 往返样本；其超时仍计入完整核验和端到端结果。

**超时重试逐条复验。** 最终默认配置下，在网关同时接入 C 与 Chromium 中的生产 release WASM。两种恢复方式各运行两条样本。服务端超时指标逐轮增加 2，首次回执均为 BrokerAccepted。原连接用例捕获 SDK 自行重发的原始消息帧，冷重开用例复用原 SQLite / IndexedDB 发件箱。中继在向客户端转发持久 ACK 前查库，逐项比较会话、服务端 ID 和正序号，并检查两端公开事件、本地消息以及最终的空发件箱。[重试与断言](../tests/storage_parity/ack_timeout.mjs)

| 恢复方式 | 绑定 | clientMsgId | 会话 ID | 服务端 ID | 序号 | 上行次数 | 首次受理 / 持久 ACK ms |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| 原连接 | C | `phase12-client-z` | `1A68QK8BDG9JADZJVW` | `9377d2d9-ad31-4ac7-be6f-4c32aaaf3e43` | 2 | 2 | 373 / 2783 |
| 原连接 | WASM | `phase12-client-z` | `1ARHC2AS8WA0PNHDCT` | `65fe8281-2312-4aa9-befc-781ff0552bb7` | 3 | 2 | 371 / 2872 |
| 原连接 | C | `phase12-client-a` | `1A68QK8BDG9JADZJVW` | `4606087e-62a3-4cbd-b634-4d60eda6e5eb` | 3 | 2 | 387 / 3593 |
| 原连接 | WASM | `phase12-client-a` | `1ARHC2AS8WA0PNHDCT` | `b9e4b3dd-bf36-436f-8b71-d6c6e5183d35` | 4 | 2 | 398 / 3666 |
| 冷重开 | C | `phase12-client-z` | `1A1HJ38VPNY94K376N` | `6a9eb570-1156-4d0d-81b2-0e083ae523c1` | 2 | 2 | 367 / 1489 |
| 冷重开 | WASM | `phase12-client-z` | `1AMD1C6VWGYSFXEEA3` | `9afd8f38-fb6a-4465-a5e7-fd75742dc7ab` | 3 | 2 | 366 / 3572 |
| 冷重开 | C | `phase12-client-a` | `1A1HJ38VPNY94K376N` | `b261259d-5f71-4d26-a49e-abb040d2113b` | 5 | 2 | 405 / 2114 |
| 冷重开 | WASM | `phase12-client-a` | `1AMD1C6VWGYSFXEEA3` | `7b0ea7d7-947f-49d7-9c5a-d1ef54ea0034` | 6 | 2 | 401 / 4837 |

两种恢复方式共 8/8 条持久回执与唯一已提交行一致。既有跨会话同 ID 回归初次在释放 Writer 后等待网关持久回执超时。测试现于解除写锁前捕获 SDK 发出的重试帧，在确认提交后转发，避免同步先完成导致没有第二次上行。复测独立发送者与同账号双设备，另 8/8 条原始持久回执在到达时均可查到唯一匹配行。Transport 的入站观察器测试 6 项通过，摄入服务单测 73 项和幂等账本集成测试 7 项通过；1 项按既有 Hook 测试库配置忽略。[所有分轮计数、分位数、最慢 1% 及回执记录](assets/phase13-ack-stages.json) · [复测命令](../storage/README.md#16--32-并发分段复测)

### 2026-11-25 · 空库长尾排查与统一验收安排

在 `dev` 逐轮回看历史数据。两连接池 16 并发两轮网关 P99 为 316.17、270.30 ms，合并为 301.42 ms。四连接池首轮为 327.46 ms，1,024 条全部在线确认；在两个 32 并发窗口之后，末轮为 451.52 ms，997 条在线确认、27 次核验超时，合并 P99 为 409.55 ms。该末轮相对首轮，Writer 事务 P99 从 98.15 升至 295.63 ms，核验取连接 P99 从 115.21 升至 240.12 ms。在最慢 1% 已确认帧中，网关入站排队均值为 0.22 ms，发送至 Writer 提交观测点均值达 491.32 ms。历史样本定位了长尾发生的窗口及阶段，对宿主编译和连接池各自的因果贡献仍待对照。[历史分轮数据](assets/phase13-ack-stages.json)

Server Writer 已将新写入台账的归档状态和时间合入 INSERT。单条与批量路径的消息、幂等账本及审计证明仍共用一个 PostgreSQL 事务。冲突行保持原状态。本轮已构建基线与候选 Writer、生产 WASM，以及针对台账重投、状态和回滚的真库测试程序。压测补充独立驱动编译、宿主编译监测和归档 UPDATE 次数采集。按要求在栈启动阶段停止，本轮没有性能样本、真库测试结果或新的重试 ACK 记录，Writer 变更的性能收益待测。[压测入口](../tests/storage_parity/ack_load.mjs) · [编译负载记录](../tests/storage_parity/quiet_host.py) · [复跑方法](../storage/README.md#排除宿主编译的空库对照)

**验收顺序。** 完成 1.4、2.1–2.3、3.1–3.4 和阶段 4 的编码后，集中执行各工作包的完成条件，包括 1.3 空库 16/32 并发对照和 C／生产 WASM 原连接及冷重开重试。保留每轮的吞吐、P95/P99、超时、同步补偿数和逐条数据库身份。构建完成、历史通过和本次统一验收分别记账。

### 性能复测口径

在 Engine 根目录运行 `python3 scripts/measure_sqlite_baseline.py`。脚本只编译一次测试程序，为每个冷启动启动独立进程并创建空数据目录。构建、测试进程启动和消息构造均在相应计时区间之外。SQLite 小批提交计入写消息、更新会话摘要及同步写入全文索引的耗时。离线峰值以持久队列的记录数计量。p95 取排序后第 ⌈0.95 × 样本数⌉ 项。[测量入口](../scripts/measure_sqlite_baseline.py) · [负载与断言](../tests/cold_start_baseline.rs#L154)

| 指标 | 首批参考值 | 后续比较条件 |
| --- | --- | --- |
| 新安装本地冷启动 p95 | **44.957 ms**。三轮为 44.942、47.663、44.957 ms | 每轮 20 个进程各执行一次 new → init → prepare(SQLite) → 本地首页，不连接网关 |
| SQLite 批提交 p95 | **5.441 ms**。三轮为 5.441、5.859、4.037 ms | 每轮 20 批 × 32 条文本，单会话，共 640 条，独立临时文件库 |
| 离线 outbox 深度峰值 | 64 条，重开后 64 条 | 从空队列连续发 64 条，无网络 ACK；新进程重开的每个客户端消息 ID 及其顺序均与入队时匹配 |
| 当前 WASM release | raw **4,956,760 B**，gzip **1,762,137 B** | 在 `bindings/wasm` 执行 `npm run build`，用仓内脚本对新输出执行体积检查。SHA-256 为 `b808dbaec0d4f8423484a856cc96d97996d8ab171a9cc48c0ba87f4a8ac23af6` |

上述读数取自本批完成后的工作树，macOS 26.6.2、Apple M1 Pro、16 GiB、rustc 1.99.1。SQLite 使用 Cargo test profile，WASM 使用仓内 release 配置与 wasm-pack 0.15.0。阶段 4 在同机同负载重复三轮，按三轮 p95 的中位数比较。建议以耗时增幅 20% 作为首批复核阈值，离线队列条数及顺序须精确一致。WASM 沿用仓内现行 raw 5,905,000 B、gzip 2,073,000 B 的发布预算。[WASM 预算](../bindings/wasm/scripts/check_wasm_budget.py)

## 阶段 0　建立可复现基线

| 工作包 | 交付与主责 | 完成条件 |
| --- | --- | --- |
| 0.1 构建与契约 | Engine 与绑定维护方记录 Engine、协议、Core SDK、Social SDK 的验证版本。复核审计 R1、R6，修复仍存在的 SQLite 测试 feature 门控及 Social 对内部配置入口的直接依赖。 | 下列基线命令通过。独立开启 `storage-sqlite` 可以运行对应测试。生成器检查无漂移，Social 通过公开 SPI 编译并通过边界检查。 |
| 0.2 数据与性能样本 | Engine 与平台适配方固定一组 SQLite 旧库、IndexedDB 旧库、未完成 outbox、已撤回消息、可信卡与未知内容。记录测试机、数据量、冷启 p95、批提交 p95、队列峰值及 WASM 体积。 | 样本可重复加载。预先记录允许的性能变化和测量方法，作为阶段 4 的比较基准。 |

0.1 可从 Engine 根目录运行下列检查。跨仓 SPI 检查应在同级检出 Social SDK 的布局中执行。[SQLite 旧库测试](../storage/sqlite-im/src/repos/message_repo/tests.rs#L1747) · [Social 架构检查](../tests/architecture_boundary_test.rs#L554)

```bash
rtk cargo test --locked --lib --features storage-sqlite,e2ee
rtk cargo test --locked --lib --features lifecycle-sqlite,e2ee
rtk cargo test --locked --test architecture_boundary_test
rtk cargo run --locked -p xtask -- core-codegen-check
rtk cargo check --locked -p flare-im-core-sdk-bindings-runtime -p flare-im-core-sdk-ffi -p flare-im-core-sdk-tauri
rtk cargo check --locked --target wasm32-unknown-unknown -p flare-im-core-sdk-wasm
```

## 阶段 1　先交付一条可恢复的消息链路

| 工作包 | 交付与主责 | 完成条件 |
| --- | --- | --- |
| 1.1 后端契约与发信 | Engine 定义按账号作用域打开的 `StorageProvider` 和 `Enqueue/CompleteSend` 两类提交。可靠队列继续使用现有 actor。存储适配方在 SQLite 单事务和 IndexedDB 跨 object store 单事务中写消息、摘要与 outbox；Memory 明示易失能力。 | 在入队和确认的每个持久化边缘注入失败，重开后只出现完整旧态或完整新态。重复确认及自己的消息回放都收敛到同一条消息。 |
| 1.2 拆开装配与持久化实现 | 先让 Native 组合根注入后端，再把现有 IM SQL 接到 `sqlite-im` 适配器；保留可供上层复用的 SQLite 连接底座。Web 复用 1.1 已接入的生产 JS host，与 Native 绑定对测。 | Engine 的普通依赖闭包不含具体数据库。相同发信样本经 C 和生产 WASM 路径重开得到相同状态。Custom 后端不满足必需能力时在打开阶段返回明确错误。 |
| 1.3 收信、重放与 ACK | Engine 将实时和同步接到同一 `ApplyInbound`，原子推进消息、操作、摘要和已证实的位点。与协议及网关维护方约定设备可达确认、持久确认以及推送接管的触发条件。 | 注入提交失败、提交后 ACK 丢失、旧包重放及进程终止。持久 ACK 只对应已提交数据，同一消息只增加一次未读。真实网关联测验证离线接管及重连补拉。 |
| 1.4 已读与会话代次 | Engine 在单次本地提交中记录已读位置及待同步意图。凭证、网络、提交、媒体和回调都校验账号作用域与登录代次。 | 离线已读可跨重启重试。同账号重登、换租户和双链交接时，迟到回包不能写入新会话。 |

1.1 是第一条纵向切片。1.3 的网关协议设计可与其并行，启用新的 ACK 语义前须通过双端联测。对跨版本部署，只有完成协商的链路才使用新确认语义。[当前发信窗口](../src/runtime/reliable_queue/actor.rs#L535) · [实时收信](../src/runtime/dispatcher/dispatch.rs#L62) · [Web 后端](../storage/indexeddb/src/provider.rs#L1528)

### 阶段 1.4 编码记录与待验事项

离线 `conversation.mark_read` 在同一次投影 CAS 内写读位、未读和待上报水位。部分已读扣除已落地的对端消息，保留尚未下载的未读。重启后的 ReadStates 同步优先重发该水位，收到覆盖该水位的权威摘要、快照或本人读回执事件才清理意图。过期快照的未读数不能覆盖更高的本地消息水位。[命令](../src/application/usecases/conversation/command.rs) · [重发](../src/application/adapters/sync_protocol_adapter.rs) · [快照与摘要](../src/application/usecases/sync/mod.rs)

Native 与生产浏览器均由 prepare 选择租户和账号。开库前记录登录代次，发布新引擎前再比对。按账号打开的后端保留该账号自己的待发队列。登录、凭证续期、消息、会话和媒体门面绑定代次，旧事件订阅随引擎退休关闭。网关交接同时作废旧链路已排队的持久化及送达确认。[会话装配](../src/client/im_client/session_lifecycle.rs) · [浏览器适配](../storage/indexeddb/src/lib.rs) · [链路代次](../src/runtime/link/epoch.rs)

统一验证时运行相同两条发信样本，在断网时只读第一条。先检查冷库的读位 1、未读 1、待上报 1、消息与发件箱，再联网等待权威确认清除待上报字段。另运行同账号慢开库、旧门面、旧事件订阅和双链交接用例。生产浏览器的会话投影改由宿主 `commitProjection` 在同一个 IndexedDB readwrite 事务内比对并写入；冲突时返回另一标签页已提交的行，Rust 侧据此替换内存镜像，避免两个标签页各按内存摘要覆盖对方的已读水位。统一验证时运行双客户端同版本只提交一次的用例。当前无本轮运行结果。[事务入口](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.ts) · [双标签页用例](../../flare-core-sdk/packages/flare-core-typescript-sdk/src/adapters/web/idbWasmStorageHost.test.ts)[双端脚本](../tests/storage_parity/offline_read.mjs) · [登录代次用例](../src/client/im_client/tests.rs)

### Social 联动待办

| 交接点 | Social 可接手时的改动 | 统一验收 |
| --- | --- | --- |
| 3.2 身份与成员投影 | Social 为资料、群角色、成员属性提供自己的 schema、迁移和同步 checkpoint，明确每个字段的权威写方 | 同账号不同群、不同查看者隔离，迁移中断后可继续 |
| 3.3 商业与应用插件 | 核对 Social 宿主使用的旧卡、已授权页面、支付单与未完成继续动作，再切换相应消费入口 | 撤权、转发、未知信封回放和未完成订单逐笔对照 |
| 3.4 群与好友 | 将建群、角色规则、好友备注及会话名称投影切至 Social；应用处理展示与通知交互 | 纯 Core 按已授权会话收发，Social 端再核对好友名、群权限和历史寻址 |
| 4 旧入口 | 收齐 Social 的公开 SPI、生成绑定和存储读写调用清单后移除对应兼容入口 | 跨仓消费者编译、旧库回放、C / WASM 与性能统一验收 |
| 3.1 扩展上下文 | Core 已移除不受授权约束的 `extension_context()`；Social 改用 `extension_context_for("social")` | 停用 Social 扩展后旧上下文写入被拒，已放行写入排空后才返回 |
| 3.1 扩展注册 | Core 已移除 builder 的 `add_extension_lifecycle_arc`、`add_extension_migration_arc`；生命周期与迁移须随 `SdkExtension` 声明注册，回调拿到本 namespace 的授权上下文，扩展停用时跳过 | Social 的生命周期回调在停用后不再执行 |
| 登录存储 | `LoginDbKind::Provided` 改为装箱，构造统一用 `LoginDbKind::provided(stores)` | Social 与示例登录路径编译通过 |

### 阶段 2–3 编码记录 · 不依赖 Social 的部分

本轮只编码，未编译、未运行测试，验证并入统一验收。2.1 与 2.2 新增的 `message.await_confirmation`、`conversation.member`、`media.resolve_attachment`、`media.cancel_transfer` 只改了契约源 `bindings/contract/apis.json` 与 `dispatch.json`，统一验证时先运行 `xtask core-codegen-check` 的生成步骤，重新生成 shared/C/WASM 分发与五端消费包。

| 工作包 | 本轮改动 | 统一验证时核对 |
| --- | --- | --- |
| 2.1 发送阶段与身份 | `send` 的返回明确为本机已入队；新增 `message.await_confirmation`，先订阅再读本地行，等到服务端持久确认或终态失败，事件丢失时以已提交行为准，超时不影响队列重试。新增 `conversation.member`，按 `MemberRef` 返回服务端同步的参与者事实，名称沿用读取时投影，未装资料 provider 时为结构化「无名」。补 Core-only 收信样本：单聊文本与引用、群文本与转发，以及同批重放不重复计未读。[发送](../src/application/usecases/message/send.rs) · [成员](../src/client/api/conversation.rs) · [样本](../src/application/usecases/inbound.rs) | 入队后断网，`await_confirmation` 在重连并持久 ACK 后返回服务端 ID 与 seq；终态拒发返回 Failed；Core-only 样本两端结果一致 |
| 2.2 媒体 | `MediaRef.message_seq` 改为 u64 并进入契约，新增 `media.resolve_attachment`，在 API 边界换算为端口与 wire 的 int64，越界拒绝。新增 `media.cancel_transfer`，同一 ID 取消上传（`transfer_id`）或下载（`download_key`）；原生下载先写临时文件再改名，取消即删除临时文件。上传恢复能力补字段说明，`requires_source_reselection` 即需重新授权文件。[模型](../src/model/media.rs) · [API](../src/client/api/media.rs) | 取消下载后目标目录无残缺文件、无下载记录；`resolve_attachment` 拒绝越界 seq |
| 2.3 绑定一致性 | 新增 [三端一致性样本](../tests/storage_parity/bindings.mjs)：C ABI、生产 WASM 与 Tauri 命令路径执行同一组离线命令，比对结构化错误、超过 2^53 的 u64、字节数组、视图 revision、取消与超时结果。Tauri 经 [无窗口驱动](../bindings/tauri/examples/parity_driver.rs) 走与命令相同的 `invoke_api_id_json`。C 驱动出错时输出结构化错误行并继续，不再退出进程；harness 三端统一提供原始 JSON 进出的 `raw`。存储层新增 [契约样本](../tests/external-consumer/src/storage_contract.rs)，内存与 SQLite 两个独立实现跑同一组分页、按键查找、重放收敛与未读/读位。[运行方法](../storage/README.md#c--wasm--tauri-绑定一致性) | 三端结果全等；WASM 的 `serde_wasm_bindgen` JSON 兼容模式对超过 2^53 的 u64 可能报错或丢精度，若样本失败按结果修正序列化；新契约入口须先重新生成分发 |
| 3.1 授权与生命周期 | `extension_context_for(namespace)` 返回带授权的上下文，会话、消息、资料和成员昵称写入前检查撤销，并在写入期间持有排空锁。Builder 拒绝互为前缀的 namespace，扩展注册的 codec、迁移和生命周期必须落在自己的 namespace 下。出站消息收到持久 ACK 并提交后，同样经 `CommitSubscription` 通知观察者。[上下文](../src/extension/mod.rs) · [注册校验](../src/client/builder.rs) · [出站提交](../src/runtime/reliable_queue/actor.rs) | 停用期间旧上下文写入失败；`a` 与 `a.b` 注册冲突；出站与入站各一条已提交通知 |
| 3.1 迁移 | 登录准备完成后，按 namespace 依次执行高于已记录版本的 `ExtensionMigration`，版本记在账号游标存储，每步 30 秒超时并捕获 panic。失败停在已完成版本，下次登录续跑，其他 namespace 和 IM 会话不受影响。[执行](../src/extension/mod.rs) · [调用点](../src/client/im_client/session_lifecycle.rs) | 中途失败后重登只执行剩余版本；停用的 namespace 不执行 |
| 3.2 外部 schema | `RecordBackend` 新增分区安装状态、按 namespace 清除。以更高 schema 打开会安装新版本并开启重写；旧版本句柄的写入在同一事务内被拒。`migrate_step` 分批重写旧行并推进独立书签，不占用扩展自己的同步 checkpoint。`purge_extension_records` 先撤权排空，再清除记录、索引、checkpoint 和安装状态。SQLite 与 IndexedDB 两端对等实现，IndexedDB 升至库版本 2，处理多标签页阻塞与配额错误。[分区](../src/extension/records.rs) · [SQLite](../storage/sqlite-im/src/extension_records.rs) · [IndexedDB](../storage/indexeddb/src/extension_records.mjs) · [外部用例](../tests/external-consumer/src/lib.rs) | 外部用例：v1 句柄升级后写入失败、重写后新索引可查、checkpoint 保持、卸载后重装为空；IndexedDB 端补同一组用例 |

### 2026-11-25 · core 各仓统一验证

Social 与上层业务扩展不在本轮范围；core 不为它们保留兼容旁路，Social 跟进点见上表。

| 仓库 | 结果 |
| --- | --- |
| flare-core-sdk-engine | fmt、两组 clippy（1.97.1，`-D warnings`）通过。库测试 e2ee 1157、storage-sqlite 1304、lifecycle-sqlite 1312、SQLite 适配器 93、外部消费者 4 项通过；`--features storage-sqlite` 全量测试通过。11 道 xtask 门禁、生成物检查、依赖闭包检查通过。WASM gzip 1,765,186 B，预算内。运行时样本：C / 生产 WASM 重开、IndexedDB 扩展记录、C / WASM / Tauri 一致性全部一致。 |
| flare-core-server | 迁移台账单测、文档与 README 数字、Hook 契约、面板指标、fmt、clippy 通过；`cargo test --workspace` 2375 项通过，115 项需数据库或外部环境按设计忽略。需独立 Redis 的序列恢复测试未在本机运行。 |
| flare-core-sdk | TS 类型检查、架构守卫、vitest 298 项、sdk-spec JSON 通过；Dart 分析零问题，测试 135 项通过（含真实 FFI 库用例）；Swift 编译通过。更新中心已按边界移出 core（`ed9d5ed`），本轮删去其在 Swift、Kotlin、Dart 模板与产物中的残留编解码、事件分支和四端 `DefaultUpdateApi`；更新中心由应用宿主或插件宿主承担。ArkTS / 仓颉按约定未验证，Kotlin 未在 CI 中编译。 |

验证中修复的既有问题：FFI 运行时线程栈在 debug 构建溢出（调为 8 MiB），测试进程栈经 `.cargo/config.toml` 设为 8 MiB；视图 API 后台任务缺字段、两处 `select!` 借用冲突；生成的 WireCodec 与模板漂移（模板改为生成器规范顺序）；过时门禁（failover、Flutter 多设备清单、Android lifecycle 路径）；server 的格式化、clippy、缺失的测试桩方法、环境变量登记与两处过时守卫。CI 已删去不存在的 `flare-access` 测试步骤。

**范围裁决（2026-11-25）。** 不依赖 Social 的编码到此收口，剩余事项按目标边界裁决如下。

| 事项 | 裁决 | 依据 |
| --- | --- | --- |
| 第三方持久后端 | 不新增"内存仓储 + 持久化回调"通用组合件。宿主自有打开器经 `AccountStoreOpener` 装配后端，与内存、SQLite 一起跑 [存储契约样本](../tests/external-consumer/src/storage_contract.rs)。 | 组合件目前只有 IndexedDB 一个使用方，抽出会改动生产 Web 路径；完成条件要求的是第三方只新增实现与装配，现有入口已满足。出现第二个同类后端时再抽取。 |
| JS 宿主的 u64 | 契约规定 JS 宿主只保证 2^53−1 以内，一致性样本测该上限；WASM 序列化不改。 | seq、revision、毫秒时间均远低于上限，超过的标识以字符串传输。改用 BigInt 会改变全部 TS 消费者的类型。 |
| 进行中调用的取消 | 不加 C ABI 取消令牌；跨调用的工作经 `media.cancel_transfer`、`view.close` 取消，或由 `message.await_confirmation` 自带超时约束。规则写入 [契约清单](../bindings/contract/manifest.json)。 | 令牌是五端 ABI 变更，而可取消的长任务已有显式入口。 |
| 生命周期回调授权 | 已实现：已声明扩展的回调拿到按 namespace 授权的上下文，停用后写入被拒；未声明的回调保持共享上下文。 | 停用扩展须同时停止其生命周期写入，且不改变 Social 现有注册方式。 |
| IndexedDB 扩展记录 | 已补 [浏览器用例](../tests/storage_parity/extension_records.mjs)：安装 CAS、旧 schema 写入被拒、与 SQLite BINARY 一致的 UTF-8 扫描顺序、前缀相同的 namespace 不被误删。 | 两端对等实现须有各自的运行证据。 |
| 插件升级失败回退、可信内容来源证明 | 移出 Engine 范围，归插件宿主与协议适配层（阶段 3.3）。 | 目标边界中 Bundle、升级与卡片来源由插件宿主负责，Engine 只保留不透明的受保护信封。 |
| Web 下载取消 | 浏览器保存由宿主完成，核心侧 `cancel_transfer` 在 WASM 只取消上传。 | 核心不持有浏览器下载任务。 |
| capability `invoke` 超时 | 不加统一超时，由插件声明各自时限。 | 支付确认、文件选择需等待用户操作。 |

## 阶段 2　稳定核心 API 和平台适配

| 工作包 | 交付与主责 | 完成条件 |
| --- | --- | --- |
| 2.1 消息、会话、身份 | Engine 确立稳定消息键、会话拓扑、UserRef/MemberRef、权威参与者事实和可选名称投影。区分本地入队与远端确认。查询公开快照版本、窗口边界及订阅溢出后的恢复方式。 | Core-only 可完成单聊、群会话、引用及转发。关闭资料 provider 后仍可收发。相同输入在两种持久后端得到相同的未读、读位置和分页结果。 |
| 2.2 媒体 | Engine 统一 MediaRef、附件引用、transfer ID、取消与重试。平台适配方保留文件、Blob 和系统权限的处理，分别声明同会话续传、重启恢复、重新授权文件的能力。 | 中断上传只补缺片，取消下载不交付残缺文件。切换账号后旧任务不能发布完成事件。Web 缺少可恢复的文件源时能给出明确结果。 |
| 2.3 稳定绑定与自定义后端 | 绑定维护方从源契约生成跨端接口，验证事件 revision、u64、字节、错误和取消。由外部测试包提供一个 Custom Storage，运行与内置后端相同的契约样本。 | C、WASM、Tauri 对相同命令给出一致的可观察结果。第三方后端只需新增自己的实现和宿主装配，Engine 源码与公共 DTO 无变化。 |

2.1 与 2.2 可在阶段 1 的提交和代次契约确定后并行；2.3 用两者的公开入口验收。[绑定源契约](../bindings/contract) · [现有媒体端口](../src/platform/ports/media.rs) · [现有成员模型](../src/model/conversation.rs#L367)

## 阶段 3　开放扩展，然后迁出业务职责

| 工作包 | 交付与主责 | 完成条件 |
| --- | --- | --- |
| 3.1 注册、权限与生命周期 | Engine 复用现有注册入口，增加 namespace、描述符版本、受限句柄、超时、排空和代次撤销。可信内容保留来源证明。业务观察器消费已提交事件。 | 冲突注册可确定地拒绝。插件超时、停用、升级失败时，IM 提交、ACK 和媒体任务继续；旧句柄不能继续写。 |
| 3.2 实体与独立存储 | 存储适配方实现扩展记录、类型化 patch、CAS、墓碑、声明式索引和分区 checkpoint。先用外部小型 schema 走通安装、升级、卸载，再由 Social 接入用户资料和群成员属性。 | 新增用户头像、性别、会员及成员角色、头衔时 Core 无 diff。同一用户在不同账号、群及查看者范围内的数据隔离。索引迁移中断可重试，核心物理 schema 版本不变。 |
| 3.3 消息与商业插件 | 插件宿主接管卡片解析、目录、授权、Bundle、页面和原生交互。商业 runtime 接管支付确认、订单对账及继续动作。协议适配保留旧卡的来源、可见性、撤权及转发约束。 | 从外部包增加红包消息、领取操作和独立投影。未安装插件的客户端可保存并回放未知信封。重复领取按业务幂等键对账；插件解码失败不影响消息事务。 |
| 3.4 Social 与应用组合 | Social 接管群创建、角色与好友备注规则。应用宿主接管首页、角标、通知呈现和租户业务通知。AI/客服旧值由 wire adapter 映射，RTC 专用占位随宿主契约迁移。 | 核心可凭已授权会话 ID 发送首条消息。新场景无需新增核心会话枚举。对照迁移前样本验证群权限、角标、通知与历史会话寻址。 |

3.3、3.4 按业务族切换。迁移前指定每种数据的权威写方；切换时核对版本、索引和处理中操作，再移交写权限。失败时恢复到能读取当前数据版本的实现。支付结果未知的请求先查单，避免因回切而再次提交。[现有扩展写权限](../src/extension/mod.rs#L248) · [卡态进入重放的位置](../src/application/usecases/sync/event_applier.rs#L85) · [支付确认编排](../src/client/api/capability_native.rs#L1332)

## 阶段 4　移除旧入口并验收

1. **清理入口与存储。** Engine 和各消费 SDK 核对公开导出、生成绑定、构建脚本及已发布消费者。确认替代入口可用、旧数据可迁移、处理中操作已对账后，再移除专用业务类型、插件表、旧 facade 和已证实重复的实现。未确认消费者的条目标为待核对。
2. **固定架构检查。** CI 校验普通依赖闭包、公开 API、生成契约和核心存储 schema。以外部插件和 Custom Storage 作为构建消费者。业务新增字段或类型时，检查 Engine 源、核心 DDL 和绑定产物零差异。
3. **验证故障与性能。** Native 运行锁竞争、旧库迁移及重开用例。生产浏览器运行多 tab 写冲突、升级阻塞、配额拒绝及恢复用例。记录真实设备或浏览器结果，分别标注注入失败与真实配额耗尽。按阶段 0 的环境和负载对比冷启、提交、队列峰值和 WASM 体积。

发布验收逐项引用审计第 5 节的能力 ID，以及第 9 节九个场景的运行记录。场景 1、2、6 由 3.2 交付，场景 3 由 3.3 交付，场景 4、5 由 1.1–2.3 交付，场景 7–9 由 3.1–3.3 联合验收。SQLite 与 IndexedDB 各提供一次经过公共 API 的完整重开记录，网关提供一次 ACK 与接管的联测记录。[审计与验收矩阵](CORE-SDK-ENGINE-ARCHITECTURE-AUDIT.md)
