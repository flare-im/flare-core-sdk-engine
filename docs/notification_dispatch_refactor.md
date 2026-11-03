# Notification 分发基础设施（Core SDK）

> 完整方案与验收清单：[`flare-social-server/docs/NOTIFICATION_DISPATCH_REFACTOR.md`](https://github.com/flare-im/flare-social-server/blob/main/docs/NOTIFICATIONS.md)（**已实施**）

## 终态摘要

| 项 | 说明 |
|----|------|
| **模块** | `src/notification/` — types / registry / inbound |
| **Push/Sync** | 统一 `partition_notification_durability` + `NotificationInboundPipeline::finish_batch` |
| **去重** | 仅 `MessageDeduper`（`server_id`） |
| **事件** | `SdkEvent::Notification` + `EventBus::on_notification`（与 `on_message` 分离） |
| **扩展** | Business SDK 实现 `NotificationHandler` 并注册到 `NotificationHandlerRegistry` |

## 宿主 API

```rust
// 聊天 UI：persistent 或 show_in_list 的 notification 才会进 on_message
im.on_message(|msg| { /* ... */ })?;

// 所有 IM 下行 Notification（含 BusinessEphemeral）
im.on_notification(|msg| { /* ... */ })?;

// Business SDK 注册 Handler
let registry = im.notification_handlers().await?;
registry.register(Arc::new(MyHandler)).await;
```

## 相关代码

- `src/notification/inbound.rs` — 分流 + pipeline
- `src/notification/registry.rs` — Handler 调度
- `src/core/dispatcher.rs` — Push
- `src/application/usecases/sync/mod.rs` — Sync
- `src/event/event_bus.rs` — `on_notification`
- `src/client/events.rs` — `IMClient::on_notification`
