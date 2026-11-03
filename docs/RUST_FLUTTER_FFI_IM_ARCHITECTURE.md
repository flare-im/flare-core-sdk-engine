# Rust + Flutter IM SDK 工程化架构（Production）

本文基于以下代码目录落地：

- Rust Core：`/Users/hg/workspace/flare/flare-im/flare-core-sdk-engine/src`
- C FFI：`/Users/hg/workspace/flare/flare-im/flare-core-sdk-engine/bindings/c`
- Flutter Example Host：`/Users/hg/workspace/flare/flare-im/flare-core-sdk-engine/examples/flare`

目标：统一桌面与移动端接入，保持核心能力在 Rust，Flutter 仅做状态展示与交互编排。

---

## 1. 分层与职责（必须遵守）

### 1.1 Rust Core（业务内核）

- `src/core`：连接、下行分发、状态机、同步编排
- `src/application`：usecase（命令/查询/同步任务）
- `src/domain`：仓储契约、消息/会话规则
- `src/infrastructure`：SQLite、协议编解码、传输实现
- `src/event`：类型化事件总线（`SdkEvent`）

规则：

- 业务规则只在 Rust Core 内实现；
- 上层（FFI/Flutter）不得复制 IM 业务状态机；
- 任何端上的行为一致性以 Rust Core 为准。

### 1.2 C FFI（跨语言 ABI 适配层）

- SDK JSON 输入输出统一 camelCase，与 core SDK 生成模型保持一致；C ABI 函数参数和符号名可保持 C/Rust 侧命名风格
- 仅做：
  - 参数反序列化 / 基础校验
  - 异步回调调度（Tokio -> C callback）
  - 错误码映射
  - 事件协议序列化

禁止：

- 在 FFI 层写业务规则
- 在 FFI 层维护会话/消息状态

### 1.3 Flutter（Host/UI 层）

- `application/outbound`：唯一写入口（UI -> SDK）
- `application/bridge/sdk_listener.dart`：唯一下行入口（SDK -> EventBus）
- `application/bridge/event_to_store.dart`：唯一总线到 Store 写入口
- `application/providers/*`：只做状态组织，不直接碰 FFI 回调细节

规则：

- UI 只能 `watch state + 调用 outbound facade`
- 不允许 UI 直接调用 repository / sdk wrapper

---

## 2. 关键链路（端到端）

### 2.1 发送消息

Flutter UI -> `ImOutboundFacade.chatSendTextAndClearDraft`  
-> `MessageService` -> `MessageRepositoryImpl` -> `SdkWrapper`  
-> C FFI `flare_message_*` -> Rust UseCase -> Transport  
-> ACK 下行（EventBus）-> FFI 事件回调 -> Flutter `sdk_listener`  
-> `MessageSendAckEvent` -> `event_to_store` -> `messageProvider.applySendAck`

### 2.2 接收消息

Server Push -> Rust `core/dispatcher`  
-> 写本地消息存储 + 会话投影  
-> `EventBus` 发布 `MessageEvent::Received/ReceivedBatch`  
-> C FFI `flare_event_subscribe` 回调  
-> Flutter `sdk_listener` 解析标准事件  
-> `NewMessageEvent`  
-> `event_to_store` 更新 message + debounce 刷新会话列表

### 2.3 Typing

输入端：Flutter `setConversationInputState` -> Rust `typing` 上行  
接收端：Rust `EventPayload::Typing` -> `MessageEvent::Typing`  
-> FFI JSON `{"type":"message","event":"typing",...}`  
-> Flutter `TypingEvent` -> typing provider

---

## 3. FFI 协议契约（严格模式）

### 3.1 版本握手

- Rust 暴露：`flare_sdk_ffi_contract_version() -> FlareString`
- 当前契约：`flare-im-ffi/v1`
- Flutter `SdkWrapper.init()` 必须先校验契约版本，不匹配立即 fail-fast

### 3.2 事件统一结构

```json
{
  "type": "message|conversation|connection",
  "event": "received|received_batch|typing|send_ack|updated|unread_count_changed|...",
  "...": "event specific fields"
}
```

要求：

- 必须包含 `type` 和 `event`
- SDK JSON 字段全量使用 camelCase
- `conversationId`、`serverMsgId`、`clientMsgId` 使用固定命名

---

## 4. 并发与线程模型

- Rust Core：Tokio runtime
- FFI 回调：可能来自任意线程（Flutter 侧使用 `NativeCallable.listener`）
- 事件订阅：
  - 订阅创建后允许等待 bus 就绪（避免登录瞬时竞态）
  - unsubscribe 必须可取消任务并回收资源
- Flutter：只在 EventBus -> Store 单向写入，避免竞态写穿

---

## 5. 错误模型与可观测性

- Rust：统一错误码 + message key（建议 `sdk.xxx.yyy`）
- FFI：保持稳定 numeric code
- Flutter：SDK 异常保留 code，并映射本地化文案 key

日志建议：

- Rust：`trace_id/user_id/conversation_id/client_msg_id/server_msg_id/seq`
- Flutter：仅打印关键桥接日志（订阅建立、事件分发、Store 应用）

---

## 6. 测试矩阵（必须自动化）

### 6.1 Rust

- 单测：domain 规则、状态机、event applier
- 集成：sync + message send/ack + unread/read
- 回归：断线重连、重复登录、会话切换

### 6.2 FFI

- 契约测试：函数签名、JSON schema、错误码稳定性
- 事件测试：subscribe/unsubscribe、typing/received/read_receipt 下发完整性

### 6.3 Flutter

- 桥接测试：`sdk_listener` 解析与分发
- Store 测试：`event_to_store` 对 unread/message 的更新
- 端到端：双账号发送、会话切换、重登恢复

---

## 7. 目录治理建议（当前仓库直接适用）

### Rust bindings/c

- `lifecycle.rs`：生命周期与版本握手
- `event.rs`：仅事件订阅与序列化
- `dispatch.rs`：Message API 扩展 JSON 分发
- `error_convert.rs`：统一错误模型

### Flutter examples/flare

- `infrastructure/events/`：SDK 事件契约定义与解析（严格）
- `application/bridge/`：SDK->EventBus 与 EventBus->Store
- `application/outbound/`：UI 唯一写入口

---

## 8. 生产落地清单（Done Definition）

- [ ] FFI 契约版本校验已启用
- [ ] 事件结构严格一致（无多协议并存）
- [ ] UI 不直接调用 repository/sdk wrapper
- [ ] 会话未读、消息状态由 Rust 权威计算
- [ ] subscribe/unsubscribe 无泄漏
- [ ] 双端实时消息与 typing 通过回归脚本
- [ ] 崩溃恢复后（重登）会话与消息一致
