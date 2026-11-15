# 基础设施层（Infrastructure）

本目录提供持久化、协议、传输等基础设施。本文档说明**消息生命周期与存储关系**、**会话（Conversation）设计与存储关系**，并给出「设计思路 → 数据流 → 实现文件」的对照，便于阅读者理清线路、快速定位实现。

---

## 一、消息生命周期与存储关系

### 1.1 设计思路

- **发送侧**：乐观写库（入队即落库，展示「发送中」）→ 发网络 → 收 ACK 后经 **FSM** 转为 Sent → 存储层做 **主键迁移**（`client_msg_id` → `server_msg_id`）并持久化终态；失败时持久化 Failed 状态。
- **接收侧**：同步/推送消息直接 `save_batch` 落库，本地状态为默认（非发送中、非失败）。
- **状态唯一来源**：消息展示状态由 **MessageState FSM** 驱动；持久化用 **MessageLocalState**（sending / failed / is_local / sort_ts）与 FSM 双向映射，保证读库 ↔ 展示一致。

### 1.2 消息 FSM 与本地状态映射

| 概念 | 说明 | 实现位置 |
|------|------|----------|
| **MessageState** | Pending → Sending → Sent → Delivered → Read；及 Failed / Recalled / Deleted | `src/core/fsm/message_state.rs` |
| **MessageStateEvent** | Enqueued / SendStarted / SendAckReceived / SendFailed / Delivered / Read / Recalled / Deleted | 同上 |
| **MessageStateFsm::transition** | 根据当前状态 + 事件得到下一状态 | 同上 |
| **MessageStateFsm::from_local_state** | 由持久化字段 (sending, failed, is_local) 推断展示用 MessageState | 同上 |
| **MessageStateFsm::to_local_state_flags** | 由 MessageState 得到 (sending, failed, is_local) 写回存储 | 同上 |
| **MessageLocalState** | sending / failed / is_local / sort_ts，对应 DB 列 | `src/model/message.rs` |
| **IMMessage** | SDK 统一消息类型，内含 `local_state` | 同上 |

### 1.3 消息生命周期与存储调用链

```
[用户发送]
    → MessageApi.send / MessageEngine.send_message
    → SendMessageCommand.execute_via_queue (可靠队列) 或 execute (直发)
    → 见下「可靠队列路径」或「直发路径」

[可靠队列路径]
    入队:
        → core::reliable_queue::actor  Enqueue
        → 构造乐观消息 (server_id=client_msg_id, sending=true, is_local=true)
        → MessageStore.save_batch([乐观消息])     ← persistence/message_store.rs (trait)
        → 实现: persistence/sqlite/message_repo.rs (SqliteMessageRepo.save_batch)
        → 同时 PendingSendWriter.push (pending_sends 表)
    发出:
        → PacketSender.send_message (协议层)
    收 ACK:
        → Dispatcher 收到 SendAck → ReliableSendQueue.on_ack
        → core::reliable_queue::actor  AckReceived
        → MessageStateFsm::transition(Sending, SendAckReceived) → Sent
        → MessageStateFsm::to_local_state_flags(Sent) 写回 local_state
        → MessageStore.update_after_ack(client_msg_id, &msg)   ← 原子：删旧行 + 插新行
        → 实现: persistence/sqlite/message_repo.rs (SqliteMessageRepo.update_after_ack)
        → EventBus MessageEvent::SendAck
    没发出去(未连接/写失败/发送超时):
        → 计一次尝试，留在发送中，按超时重发
        → 长连接恢复时（引擎建连后 recover_pending_for_current_user）立即重发，不等超时
    发送失败(超时/重试耗尽):
        → core::reliable_queue::actor check_timeout / try_send_next
        → local_state = { sending: false, failed: true, is_local: true }
        → MessageStore.save_batch([failed_msg])   ← 覆盖原乐观行 (server_id=client_msg_id)
        → EventBus MessageEvent::SendFailed

[直发路径]（存储不带 PendingSend 仓储时）
    → PacketSender.send_message
    → MessageStore.save_batch([message])   ← 无主键迁移，一次落库
    没有暂时性重试：不等 SendAck，服务端的暂时性拒绝不会重发；未连接、发送超时直接返回错误
    （长连接断开后 flare-core 客户端不再自己重连，重连只走会话的重连监视器）。生产登录路径
    （SQLite、加密 SQLite、Web 的 IndexedDB 存储）都带 PendingSend 仓储、走可靠队列；只有
    in_memory_empty_im_provider、运行时 StorageKind::Memory、FlareAppClientBuilder 在登录前的
    默认存储会走直发，供联调与测试使用。
```

### 1.4 消息存储接口与实现位置

| 能力 | 接口定义 | 实现位置（本层） |
|------|----------|------------------|
| 批量/单条写入 | `MessageStore::save_batch` / `save_one` | `persistence/message_store.rs` (trait)<br>**persistence/sqlite/message_repo.rs** (SqliteMessageRepo) |
| 按 server_id 查询 | `MessageStore::get` | 同上 |
| 按 client_msg_id 查询 | `MessageStore::get_by_client_msg_id` | 同上 |
| 按会话分页 | `MessageStore::get_by_conversation(conversation_id, before_seq, limit)` | 同上 |
| 状态/内容更新 | `MessageStore::update_status` / `update_content` | 同上 |
| 删除 | `MessageStore::delete` | 同上 |
| 搜索 | `MessageStore::search`（依赖 text 列） | 同上 |
| **ACK 后主键迁移** | **MessageStore::update_after_ack(client_msg_id, message)** | **persistence/sqlite/message_repo.rs**（事务内 DELETE 旧 server_id + INSERT 新行） |

领域层读写拆分在 **domain**（本目录外）：

- 读：`src/domain/repository/message_repository.rs` → `MessageReader`
- 写：同上 → `MessageWriter`（含 `update_after_ack`）

SQLite 表结构（消息本地状态列）：

- **persistence/sqlite/schema.rs**：`messages` 表，含 `sending` / `failed` / `is_local` / `sort_ts` 等列；主键 `server_id`；索引 `conversation_id + seq`、`client_msg_id`。

### 1.5 消息相关文件速查

| 职责 | 文件路径（相对 crate 根） |
|------|----------------------------|
| 消息 FSM 与状态映射 | `src/core/fsm/message_state.rs` |
| 消息模型与 MessageLocalState | `src/model/message.rs` |
| 消息存储 trait | `src/infrastructure/persistence/message_store.rs` |
| 消息 SQLite 实现 + update_after_ack | `src/infrastructure/persistence/sqlite/message_repo.rs` |
| 消息表结构 | `src/infrastructure/persistence/sqlite/schema.rs` |
| 可靠队列入队/ACK/失败写库 | `src/core/reliable_queue/actor.rs` |
| 领域仓储 trait（MessageReader/Writer） | `src/domain/repository/message_repository.rs` |

---

## 二、会话（Conversation）设计与存储关系

### 2.1 设计思路

- **会话列表**：以「会话」为聚合，每条会话有 last_message、未读、置顶/静音/归档等；列表顺序：置顶优先，再按 `last_message_at` 倒序。
- **会话同步**：连接后同步引擎拉取会话列表（SyncConversations），服务端返回的会话通过 **ConversationStore.save_batch** 落库。
- **单会话消息同步**：按会话拉取消息（SyncRequest/SyncResponse），消息落 **MessageStore**，同时用本批消息的「最后一条」更新该会话的 **last_message_* 与 max_seq**，保证列表预览与排序正确；游标存 **SyncCursorStore**（如 `sync:{conversation_id}` → max_seq）。

### 2.2 会话生命周期与存储调用链

```
[连接后全量同步]
    → SyncManager / Orchestrator 跑 ConversationsSyncTask
    → SyncHandler.sync_conversations_impl
    → 发 SyncConversationsRequest
    → 收 SyncConversationsResponse → handle_sync_conversations_response
    → 将 patches 转为 Conversation 列表
    → ConversationStore.save_batch(conversations)   ← persistence/conversation_store.rs (trait)
    → 实现: persistence/sqlite/conversation_repo.rs (SqliteConversationRepo.save_batch)
    → EventBus ConversationEvent::Synced

[单会话消息同步]
    → SessionSyncRunner.request_message_sync(conversation_id) → SyncHandler.sync_conversation
    → 从 SyncCursorStore.get("sync:{conversation_id}") 取 last_seq
    → 发 SyncRequest(conversation_id, last_seq, limit)
    → 收 SyncResponse → handle_sync_response → apply_sync_response_and_transition
    → 解析 events 得到 Vec<IMMessage>
    → MessageStore.save_batch(messages)
    → 取本批中 seq 最大的消息 latest，ConversationStore.update_last_message(
          conversation_id, latest.server_id(), latest.sender_id(), latest.created_at,
          latest.text_for_storage().as_deref(), max_seq)
    → 实现: persistence/sqlite/conversation_repo.rs (SqliteConversationRepo.update_last_message)
    → SyncCursorStore.save("sync:{conversation_id}", envelope.max_seq)
    → EventBus MessageEvent::Received（逐条）
```

### 2.3 会话存储接口与实现位置

| 能力 | 接口定义 | 实现位置（本层） |
|------|----------|------------------|
| 批量/单条写入 | `ConversationStore::save_batch` / `save_one` | `persistence/conversation_store.rs` (trait)<br>**persistence/sqlite/conversation_repo.rs** (SqliteConversationRepo) |
| 按 id 查询 | `ConversationStore::get` | 同上 |
| 列表（置顶+last_message_at 排序） | `ConversationStore::list` | 同上 |
| 未读更新 | `ConversationStore::update_unread(conversation_id, unread_count, last_read_seq)` | 同上 |
| 置顶/静音/大群全部消息通知/归档 | `ConversationStore::set_pinned` / `set_muted` / `set_notify_all` / `set_archived` | 同上 |
| 草稿 | `ConversationStore::update_draft` | 同上 |
| 删除 | `ConversationStore::delete` | 同上 |
| **最后一条消息更新** | **ConversationStore::update_last_message(conversation_id, last_message_id, last_sender_id, last_message_at, preview, max_seq)** | **persistence/sqlite/conversation_repo.rs**（UPDATE 会话表） |

领域层：

- 读/写：`src/domain/repository/conversation_repository.rs` → `ConversationReader` / `ConversationWriter`（含 `update_last_message`）。

SQLite 表结构：

- **persistence/sqlite/schema.rs**：`conversations` 表，含 `last_message_id` / `last_sender_id` / `last_message_at` / `last_message_preview` / `max_seq` / `unread_count` / `last_read_seq` 等；索引按 `is_archived`、`is_pinned`、`last_message_at` 排序。

### 2.4 会话相关文件速查

| 职责 | 文件路径（相对 crate 根） |
|------|----------------------------|
| 会话存储 trait | `src/infrastructure/persistence/conversation_store.rs` |
| 会话 SQLite 实现 + update_last_message | `src/infrastructure/persistence/sqlite/conversation_repo.rs` |
| 会话表结构 | `src/infrastructure/persistence/sqlite/schema.rs` |
| 会话模型 | `src/model/conversation.rs` |
| 同步响应处理 + 消息落库 + 会话 last_message 更新 | `src/application/handlers/sync_handler.rs` |
| 同步游标存储 | `src/infrastructure/persistence/db.rs` (SyncCursorStore)<br>**persistence/sqlite/cursor_repo.rs** (SqliteSyncCursorRepo) |
| 领域仓储 trait（ConversationReader/Writer） | `src/domain/repository/conversation_repository.rs` |

---

## 三、存储提供者与目录结构

- **StoreProvider**（`persistence/db.rs`）：统一持有 `MessageStore`、`ConversationStore`、`SyncCursorStore`、可选 PendingSend 与 UserProfile 的 Reader/Writer，供应用层与可靠队列注入。
- 本层目录结构（仅列持久化相关）：

```
infrastructure/
├── README.md                    ← 本文档
├── mod.rs
├── persistence/
│   ├── mod.rs
│   ├── db.rs                    ← StoreProvider, SyncCursorStore
│   ├── message_store.rs        ← MessageStore trait
│   ├── conversation_store.rs   ← ConversationStore trait
│   ├── memory.rs                ← 内存兜底（UserProfile / PendingSend 等）
│   ├── layered.rs               ← PendingSend 分层
│   ├── indexeddb_adapter.rs     ← IndexedDB 接入抽象
│   └── sqlite/
│       ├── mod.rs
│       ├── schema.rs            ← messages / conversations / pending_sends / sync_cursors 等表
│       ├── message_repo.rs      ← 消息 CRUD + update_after_ack
│       ├── conversation_repo.rs ← 会话 CRUD + update_last_message
│       ├── cursor_repo.rs       ← 同步游标
│       ├── pending_send_repo.rs
│       └── user_repo.rs
├── protocol/
└── transport/
```

---

## 四、总结表：谁在什么时候写什么

| 场景 | 写什么 | 调用的 Store / 方法 | 实现文件 |
|------|--------|---------------------|----------|
| 发送入队（可靠队列） | 乐观消息（sending=1） | MessageStore.save_batch | sqlite/message_repo.rs |
| 发送 ACK 到达 | 终态消息（主键迁移） | MessageStore.update_after_ack | sqlite/message_repo.rs |
| 发送失败 | 失败态消息 | MessageStore.save_batch | sqlite/message_repo.rs |
| 同步/推送收到消息 | 新消息 | MessageStore.save_batch | sqlite/message_repo.rs |
| 同步响应后更新会话 | 会话 last_message、max_seq | ConversationStore.update_last_message | sqlite/conversation_repo.rs |
| 会话列表同步 | 会话列表 | ConversationStore.save_batch | sqlite/conversation_repo.rs |
| 单会话拉取游标 | 游标 | SyncCursorStore.save | sqlite/cursor_repo.rs |

按上述表格和「消息/会话生命周期与存储调用链」即可从设计理清线路、在对应文件中找到实现。
