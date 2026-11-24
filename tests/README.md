# Flare IM Core SDK 测试说明

当前测试以生产 Rust core contract 为中心，验证 builder、typed API、事件总线、协议映射、存储投影等共享行为。

旧 prototype 时代的 JSON route facade、`TextMessageRequest`、`SubscriptionSpec`、`EventKind`、旧 `TransportPort` 测试已移除；这些不再是当前 SDK 的生产合同。Platform bindings 应通过各自 runtime/contract crate 覆盖跨语言 JSON 或 FFI 表面，不再把旧路由层塞回 core SDK。

## 本地确定性测试

```bash
rtk cargo test -p flare-im-core-sdk
rtk cargo test --workspace
```

覆盖范围：

- `core_sdk_contract_test.rs`：当前 `IMClient::builder()` 装配合同、未登录写侧拒绝语义、内存 message/conversation store 对当前 `IMMessage` / `Conversation` 模型的保存与查询。
- `core_endpoints.rs`：用本地 WebSocket 服务验证地址组替换、版本更新、会话保持和发件箱恢复。
- `link_lifecycle.rs`：验证当前地址的连接与重连、网络状态变化、网关排空、临时拒绝以及终止性会话错误。
- 签名目录、商用探活和故障切换由 `flare-commercial-client-runtime` 的测试覆盖。
- `transient_send_rejection_retry.rs`：同样的回环服务端按脚本回 SendAck：暂时性拒绝（`UNAVAILABLE`）时 SDK 退避后用同一个 `client_msg_id` 重发直到接收，本地只有一条消息；次数以 `ack_max_retries` 为界；`TENANT_UNAVAILABLE` 立即判失败、不重发。
- crate 内 `#[cfg(test)]` 单元测试：消息内容 oneof 映射、dispatcher、sync adapter、event applier、reliable queue、SQLite repository 等靠近领域和协议边界的行为。
- `bindings/shared` crate test：binding contract route discovery 覆盖生成 API ids 与 vNext runtime routes。
- `bindings/uniffi` crate test：UniFFI object facade 通过共享 JSON runtime 调用 `sdk.state`，并验证旧 snapshot/restore JSON 入口明确返回 unsupported。

## Binding 和目标平台验证

```bash
rtk cargo check -p flare-im-core-sdk --target wasm32-unknown-unknown
rtk cargo check -p flare-im-core-sdk-wasm --target wasm32-unknown-unknown
rtk cargo check -p flare-im-core-sdk-tauri
rtk cargo check -p flare-im-core-sdk-ffi --all-targets
rtk cargo check --manifest-path bindings/uniffi/Cargo.toml
rtk cargo test -p flare-im-core-sdk-bindings-runtime
rtk cargo test --manifest-path bindings/uniffi/Cargo.toml
rtk bash scripts/verify.sh
```

## 服务端验证状态

`scripts/live_stack.py` 使用独立端口、数据库和消息队列启动本地服务。换址验收用两台客户端先收发消息，再切换发送端的 WebSocket 网关并检查会话世代及接收端消息序号。

```bash
rtk proxy python3 scripts/live_stack.py up
eval "$(rtk proxy python3 scripts/live_stack.py env)"
rtk cargo test --test reachability_acceptance direct_endpoint_refresh_preserves_session_and_message_order -- --ignored --nocapture
rtk proxy python3 scripts/live_stack.py down
```
