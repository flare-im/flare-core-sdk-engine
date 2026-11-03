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
- `transient_rejection_reconnect.rs`：本机回环（127.0.0.1）起 flare-core WebSocket 服务端，驱动 SDK 走完整的建连与重连循环：服务端暂时性拒绝（`SERVICE_UNAVAILABLE`）时退避重连直到放行，`TENANT_UNAVAILABLE` 时终止会话、不再重连；只经 builder 配置的重连间隔与次数同样作用于 SDK 的重连循环。
- `server_shutdown_reconnect.rs`：同样的回环服务端停机（先下发 CLOSE(`SERVER_SHUTDOWN`)），SDK 看到带停机原因的断开，同一端口上的新实例起来后在 1–5 s 的随机等待后重连回到 Ready，而不是按 30 s 的重连间隔退避；会话不终止。
- `single_reconnect_path.rs`：同样的回环服务端，SDK 经本机 TCP 中继连上；切断中继上的连接或注入非法帧：断线期间的发送不建连、只由会话的重连监视器在退避后重连一次、全程最多一条连接；断线期间发出的消息在重连后立即被接收；连接以错误终止时监视器同样接手，协议竞速下（QUIC 不可达、WebSocket 胜出）也一样。
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

本仓当前 vNext 测试不会伪造真实服务端 E2E。服务端健康检查入口在：

```bash
rtk bash ../flare-core-server/scripts/check_services.sh
rtk cargo test -p flare-im-core-sdk --features integration-tests --test <new_live_gateway_test>
```

live gateway E2E 需要按当前 `SocketTransport` / `PacketSender` / typed API 重建，不再沿用旧 `memory://local` 和 route facade。media、presence/typing、rich-doc、capability/RTC/SFU 的 provider-backed 服务端 E2E 可在对应服务暴露稳定测试接口后继续扩展。
