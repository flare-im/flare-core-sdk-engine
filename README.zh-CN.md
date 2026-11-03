# Flare IM Core SDK

**[English](README.md)** · 中文

> ## ⚠ 请与同级仓库一起 clone

本 crate 通过**路径依赖**引用 `../flare-transport`、`../flare-protocol`、`../flare-protocol-grpc`。
单独 clone 这一个仓是编译不过的——失败发生在 `cargo metadata`，还没读到你的代码，
所以报错指向一个内部工作区成员，完全看不出真正的原因。

```bash
mkdir flare && cd flare
git clone https://github.com/flare-im/flare-transport.git
git clone https://github.com/flare-im/flare-protocol.git
git clone https://github.com/flare-im/flare-protocol-grpc.git
git clone https://github.com/flare-im/flare-core-sdk-engine.git
cd flare-im-core-sdk && cargo check
```

> ## ℹ 这是通信基础设施，不是开箱即用的 IM 产品
>
> 说在前面，免得你 clone 完才发现登不上去：**开源部分不含账号体系**
> （没有注册登录、好友关系、群角色/审批/禁言、朋友圈）。
>
> 但它自带完整且可插拔的鉴权契约，两条路都在开源侧：
>
> - **`CoreJwtTokenValidator`** —— 本地验 JWT。手签一个 token 就能跑起来做
>   demo / POC，**不需要任何用户体系**。
> - **`HttpHookTokenValidator`** —— 把 token POST 到你自己的接口，
>   **这是接入自有用户体系的入口**。
>
> 业务规则同理：`flare-core-server/crates/flare-im-hooks` 提供 9 个扩展点
> （PreSend / PostSend / Delivery / Recall / MessageRead / MessageReaction /
> ConversationLifecycle / ConversationMember / GetConversationParticipants）。
>
> 要上生产，你需要自行实现用户体系并按上述契约接入 —— 与 Sendbird /
> Twilio Conversations 的「自带身份」模型一致，区别是 Flare 可自托管、
> 协议与核心可审计。
>
> 边界详情见 [GOVERNANCE.md](.github/GOVERNANCE.md)。


[![Rust](https://img.shields.io/badge/rust-1.94+-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`flare-im-core-sdk` 是 Flare IM 的统一客户端核心。Rust core 负责 SDK 生命周期、消息/会话本地投影、同步任务入口、事件体系、媒体记录、能力包与扩展事件；C、Tauri、UniFFI、Wasm bindings 只做 ABI/IPC/语言边界转换。

## 核心边界

| 层 | 职责 |
| --- | --- |
| `flare-core` | 传输、连接、帧、协商、心跳等基础通信能力 |
| `flare-proto` | 唯一 wire contract，包含 `DataPacket`、`Message`、`SyncRes`、`CapabilityPacket` 等 |
| `flare-im-core-sdk` | 客户端 IM 行为、离线本地状态、投影、outbound queue、事件路由、扩展能力入口 |
| bindings | C/Tauri/UniFFI/Wasm 的边界适配，不复制核心语义 |

## 当前生产模块

`src/lib.rs` 只 re-export 当前 public contract。SDK 行为按边界放在 `client`、`application`、`domain`、`core`、`infrastructure`、`platform` 和 `extension` 中；旧 prototype route facade 不再是生产合同。

| 模块 | 职责 |
| --- | --- |
| `client/` | `IMClient` lifecycle、typed APIs、builder、登录会话、跨端 SDK 入口 |
| `application/` | 用例编排、message/sync/presence/capability adapter、projection 更新 |
| `domain/` | 消息、会话、同步游标、pending send 等业务不变量 |
| `core/` | 事件总线、dispatcher、可靠队列、sync orchestrator |
| `infrastructure/` | protobuf codec、packet sender、socket/http transport、memory/sqlite persistence |
| `platform/` | media、transport、宿主能力端口 |
| `extension/` | capability registry、插件 UI 与页面、middleware |

## 安装

```toml
[dependencies]
# `LoginDbKind::Sqlite` 由 lifecycle-sqlite 提供；不开这个 feature，下面的样例编不过。
flare-im-core-sdk = { version = "1.2", features = ["lifecycle-sqlite"] }
tokio = { version = "1", features = ["full"] }
```

构建前置：`protoc`。依赖链上的 crate 在构建期要编译 `.proto`。

```bash
# Debian / Ubuntu
sudo apt-get install -y protobuf-compiler
# macOS
brew install protobuf
```

## 快速开始

> 客户端 App 不要本地签发：配置 `SdkConfig::auth.token_endpoint`（builder `auth_token_endpoint`）让 SDK 向网关 `/api/v1/auth/tokens` 签发并自动刷新，或传入业务后端签好的 token。`generate_core_token` 只保留给服务端工具与测试（见 flare-core-server/docs/AUTH-TOKEN-ISSUANCE.zh-CN.md）。

```rust
use flare_im_core_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let client = IMClient::new();
    client.init(Some("example".into()), None).await?;

    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret: "insecure-secret".to_string(),
        issuer: "flare-im-core".to_string(),
        user_id: "user_a".to_string(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some("0".to_string()),
    })?;
    let apis = client
        .login("user_a", Some(&token), LoginDbKind::Sqlite, |_, _| {})
        .await?;

    let conversation = apis
        .conversation_api
        .get_one("peer_b", &flare_im_core_sdk::model::conversation::ConversationType::Single)
        .await?;
    let message = apis
        .message_build_api
        .create_text(&conversation.conversation_id, "hello", false, &[])
        .await?;
    let ack = apis.message_api.send_no_oss(message).await?;
    println!("sent: {:?}", ack);
    Ok(())
}
```

## Binding Runtime

Bindings 统一通过 `bindings/shared` crate 的 `BindingRequest` 进入当前 typed SDK。这个 shared runtime 只接入已经明确迁移到 vNext 的 route；旧 prototype JSON route 不会被兼容复活。

```rust
let response = flare_im_core_sdk_bindings_runtime::invoke_json(
    &client,
    r#"{"route":"sdk.state","params":{}}"#,
).await;
```

Contract 源在 `bindings/contract/*.json`；修改后运行 `rtk cargo xtask codegen` 更新生成表。

从 JSON 读消息走 `IMMessage::from_json_str` / `IMMessage::from_json_value`，单独读消息内容走 `Elem::from_json_value`，不要自己调 `serde_json`。它们的读法与 `serde_json` 完全一致；因为只在本 crate 编译一次，链接多个 crate 的产物（例如浏览器包）里消息读取器（递归结构）只保留一份。

## 事件体系

事件统一使用 `SdkEvent`：

- `EventBus::subscribe()` 提供 typed Rust 事件流。
- message mutation 事件来自 `event.proto` oneof payload；typing/presence 不占用 `conversation_seq`，走 DATA realtime。
- 插件实时包（`DataPacket.capability`，`im.realtime.deliver`）以 `SdkEvent::Plugin(PluginEvent::Realtime)` 给出，过期包与 payload 不是 JSON 对象的包直接丢弃。租户的 `flare.plugin.catalog` / `changed` 通知先让插件目录失效，再在 0–3 秒随机延迟后以 `PluginEvent::CatalogChanged` 给出，3 秒内的通知合并为一次。绑定层事件为 `plugin.realtime`（7001，`im://plugin_realtime`）和 `plugin.catalog_changed`（7002，`im://plugin_catalog_changed`）。
- `bindings/contract/events.json` 维护跨平台 event id、C code 和 `im://*` 名称。
- 自定义业务事件走 `ExtensionEvent` 或 `MessageEvent::Custom`，payload bytes 对 core 不透明。

## Outbox 和同步

- 发送时先写入 bounded pending outbox；离线时只入队，`Ready` 且 transport 已连接时才即时发送。
- `SendAck` accepted/error 都会收敛本地消息状态并移除 pending。
- realtime 下行消息发现 `conversation_seq` 缺口时记录 gap，并通过单会话 sync 请求补拉。
- 会话列表分页 cursor 使用服务端 string cursor；本地 adapter 只在需要时解析数值水位。
- `SyncRes` / `EventEnvelope` 当前字段以 `max_conversation_seq`、`next_cursor` 和 oneof payload 为准。

### 断线时不经 outbox 的操作

只有发消息走 outbox。撤回、编辑、删除、表情回应、置顶与标记、已读回执（`mark_read`、`mark_read_and_burn`）、同步请求与自定义数据都是直发：不排队、不重试。长连接断开期间（包括重连监视器还在退避时）它们立即失败，不会自己重连；重连只走重连监视器一条路径。请按错误码分支，不要解析错误文本：

| 错误码 | 含义 | 应用侧建议 |
| --- | --- | --- |
| `NOT_CONNECTED`（C ABI `FLARE_ERR_NOT_CONNECTED`） | 没有可用的连接：已断开、正在重连，或服务端结束了会话（`DEVICE_CONFLICT` / `TENANT_UNAVAILABLE`，同时报 `KickedOff`） | 提示「未连接」，等状态回到 `Ready` 后再试 |
| `OPERATION_TIMEOUT`（C ABI `FLARE_ERR_OPERATION_TIMEOUT`） | 发送没有在时限内完成 | 提示超时；服务端可能已经生效，先刷新再决定是否重试 |
| `CONNECTION_FAILED`（同步请求为 `GENERAL_ERROR`；C ABI 目前都报 `FLARE_ERR_INTERNAL`） | 写入失败，例如发送途中连接断开 | 按暂时性失败处理，回到 `Ready` 后重试 |

输入状态是有损信令：断线时直接丢弃，调用照样返回 `Ok`。

## Bindings

| Binding | 目录 | 验证 |
| --- | --- | --- |
| C ABI | `bindings/c` | `cargo check -p flare-im-core-sdk-ffi --all-targets` |
| Tauri | `bindings/tauri` | `cargo check -p flare-im-core-sdk-tauri` |
| UniFFI | `bindings/uniffi` | `cargo check --manifest-path bindings/uniffi/Cargo.toml`; `cargo test --manifest-path bindings/uniffi/Cargo.toml` |
| Wasm | `bindings/wasm` | `cargo check -p flare-im-core-sdk-wasm --target wasm32-unknown-unknown` |

## 开发验证

```bash
rtk cargo fmt --all -- --check
rtk cargo check --workspace --all-features --all-targets
rtk cargo test --workspace --all-features
rtk cargo check --manifest-path bindings/uniffi/Cargo.toml --all-features --tests
rtk cargo test --manifest-path bindings/uniffi/Cargo.toml --all-features
rtk cargo check -p flare-im-core-sdk-wasm --target wasm32-unknown-unknown --all-features
rtk cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## 服务端联调

本地服务端全栈由 `../flare-core-server/scripts/start_server.sh` 启动，并可用 `../flare-core-server/scripts/check_services.sh` 检查。当前 live gateway E2E 需要按 `SocketTransport` / `PacketSender` / typed API 重建；不要恢复旧 `memory://local` route facade 测试。

```bash
rtk bash ../flare-core-server/scripts/check_services.sh
```

## 许可

Apache License 2.0，与底层 `flare-core` 一致，见对应仓库许可文件。

---

## 下一步

| 想做什么 | 去哪里 |
|---|---|
| **五分钟跑起来** | [QUICKSTART](https://github.com/flare-im/flare-core-server/blob/main/QUICKSTART.md) —— 起服务、手签 token、调通接口，**不需要自建用户体系** |
| 接入自己的用户系统 | 实现 `TokenValidator`（`CoreJwtTokenValidator` 本地验签 / `HttpHookTokenValidator` 调你的接口） |
| 加自己的业务规则 | `flare-im-hooks` 的 9 个扩展点：PreSend / PostSend / Delivery / Recall / MessageRead / MessageReaction / ConversationLifecycle / ConversationMember / GetConversationParticipants |
| 做界面 | [`@flare-im/vue-ui`](https://www.npmjs.com/package/@flare-im/vue-ui) —— 107 个组件，四端一致的契约 |
| 报安全问题 | [SECURITY.md](.github/SECURITY.md)，**请勿开公开 issue** |

## 需要账号体系与社交能力时

开源部分是**通信基础设施**。如果你需要的是现成的账号、好友关系、群治理（角色 / 入群审批 / 禁言）、朋友圈，
这些在商业模块里 —— 自研这一层通常要数月，且都是与通信无关的重复劳动。

企业场景另有 SSO / 组织架构 / 审计导出 / 数据驻留 / SLA 支持。

咨询：`flare1522@163.com`

> 边界划分与不变承诺见 [GOVERNANCE](https://github.com/flare-im/flare-core-server/blob/main/GOVERNANCE.md)。
> 简言之：**已开源的不会被收回，鉴权与 hooks 契约永远开源、不会为逼迫付费而阉割。**
