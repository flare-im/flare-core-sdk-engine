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
- `link_failover.rs`：本机回环起两个「接入点」（flare-core WebSocket 服务端 + 可控 TCP 中继），SDK 以开发模式多接入点接入：当前接入点挂掉时切到另一个、不重登（会话世代不变），期间连发 100 条零失败零重复且服务端收到的顺序即发送顺序；过载（OVERLOADED）在同一接入点等待不迁移；排空（CLOSE DRAIN 带窗口与优先接入点）在窗口内重连到建议的接入点；半开连接在发送卡住后验活判死并切换；同一用户重新 prepare 后发件箱原样恢复发送；开了 `quic` 特性时，QUIC 被静默丢弃三次后记住 UDP 封锁、之后直接走 WebSocket。
- `link_lifecycle.rs`：单 / 双接入点：断网只由链路状态机按退避重建、断线期间的消息只发一次；服务端停机在 CLOSE 给的 1–5 s 窗口内重连；暂时性拒绝（`SERVICE_UNAVAILABLE`）退避直到放行；租户不可用与结束会话的踢线终止（`actionRequired`），不再尝试；网络变化对半开连接立即验活并换线路；无网络时挂起、发件箱等待；CONNECT 带上应用信息，每次重连的连接代数递增。
- `link_recovery.rs`：生产模式的目录与信任链（真实验签、LKG）、真实 WebSocket 服务端与 HTTP 假镜像：全部接入点失效时连续两轮失败进入恢复，从镜像拿到新地址后连上，发件箱零丢失、零重复、会话内有序，不重登；重启后新地址来自 LKG；CONNECT 报本地 serial，CONNECT_ACK 报更新的 serial 时客户端自己去拉。
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
