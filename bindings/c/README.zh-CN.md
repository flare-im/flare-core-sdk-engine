# Flare IM SDK - C ABI Bindings

[English](README.md) · 中文

跨平台 C ABI SDK。它是当前多端客户端 SDK 的唯一通用 native L1
边界，契约来源见 [`../contract/manifest.json`](../contract/manifest.json)。
API、事件、错误的完整机器可读契约分别见：

- [`../contract/apis.json`](../contract/apis.json)
- [`../contract/events.json`](../contract/events.json)
- [`../contract/errors.json`](../contract/errors.json)

支持 iOS、Android、Flutter、鸿蒙、C/C++、Node、Unity、React Native native
module 等通过 native bridge 接入的运行时。

## 架构

- **Rust 内部复杂,C ABI 外部简单**
- **所有对象通过 handle 管理**
- **异步 API 使用 callback**
- **统一 error code 错误模型**
- **显式内存管理**

## 文件结构

```
bindings/c/
├── Cargo.toml          # 项目配置
├── cbindgen.toml       # 头文件生成配置
├── build.rs            # 构建脚本
├── ARCHITECTURE.md     # 架构设计文档
└── src/
    ├── abi.rs           # panic 边界与 FFI 返回保护
    ├── client_sync.rs   # IMClient 同步/状态类透传
    ├── dispatch.rs      # JSON 分发入口
    ├── event.rs         # EventBus 订阅与事件 JSON 映射
    ├── executor.rs      # async -> callback_once
    ├── ffi_runtime.rs   # Tokio runtime
    ├── lib.rs           # 入口
    ├── types.rs         # C ABI 类型定义
    ├── registry.rs      # 句柄注册表
    ├── error_convert.rs # 错误转换
    ├── helpers.rs       # 辅助工具
    └── lifecycle.rs     # 生命周期 API
```

## API

### 生命周期

```c
// 创建/释放
FlareHandle flare_sdk_create();
void flare_sdk_release(FlareHandle handle);

// 初始化/登录/登出
int32_t flare_sdk_init(FlareHandle handle, const char* config_json, void* context, FlareResultCallback callback);
int32_t flare_sdk_login(FlareHandle handle, const char* user_id, const char* token, const char* store_config_json, void* context, FlareResultCallback callback);
int32_t flare_sdk_logout(FlareHandle handle, void* context, FlareResultCallback callback);
int32_t flare_sdk_update_access_token(FlareHandle handle, const char* access_token, const char* tenant_id, void* context, FlareResultCallback callback);

// 同步查询
FlareString flare_sdk_version();
bool flare_sdk_is_connected(FlareHandle handle);

// 当前用户 ID（异步，走 callback）
int32_t flare_sdk_current_user_id(FlareHandle handle, void* context, FlareResultCallback callback);

// 开发用 JWT（同步，不依赖 handle；`secret`/`issuer` 可为 NULL 或空串用内置默认；`tenant_id` 可为 NULL；`ttl_secs==0` 表示 3600；须 flare_string_free）
```

### 内存管理

```c
void flare_string_free(FlareString s);
void flare_bytes_free(FlareBytes b);
void flare_error_free(FlareError e);
```

### API 覆盖

平台边界和稳定形状使用 direct C ABI，例如：

- lifecycle: `flare_sdk_*`
- events: `flare_event_*`
- media transfer/progress/cancel: `flare_media_upload_*`,
  `flare_media_delete_file`, `flare_media_cancel_user_file_download`,
  `flare_media_download_file_to_downloads`
- simple message paths: `flare_message_create_text`, `flare_message_send`,
  `flare_message_list`, `flare_message_recall`, `flare_message_delete`

大型或快速演进的会话、媒体控制、消息构建、消息 mutation 使用 JSON dispatch：

```c
int32_t flare_conversation_dispatch_json(FlareHandle handle, const char* op, const char* params_json, void* context, FlareResultCallback callback);
int32_t flare_media_dispatch_json(FlareHandle handle, const char* op, const char* params_json, void* context, FlareResultCallback callback);
int32_t flare_message_build_json(FlareHandle handle, const char* request_json, void* context, FlareResultCallback callback);
int32_t flare_message_dispatch_json(FlareHandle handle, const char* op, const char* params_json, void* context, FlareResultCallback callback);
```

所有 canonical API id、C entrypoint、Tauri command 的对应关系以
[`../contract/apis.json`](../contract/apis.json) 为准。

### 事件覆盖

`flare_event_subscribe` 会逐条转发完整 `SdkEvent` 域事件。C callback 参数
`event_type` 是稳定整数码，`event_json` 是 camelCase SDK JSON。高吞吐同步场景优先使用
`flare_event_subscribe_batch`，一次 callback 收到:

```json
{ "events": [{ "eventType": 2001, "payload": {} }] }
```

其中 `payload` 与逐条 callback 的 `event_json` 对象完全一致。事件码和 payload
字段以 [`../contract/events.json`](../contract/events.json) 为准。

## 构建

```bash
cargo build -p flare-im-core-sdk-ffi --release
```

### Flutter 参考应用产物同步

将 `flare-core-examples` 与本仓并列检出。从引擎根目录运行 `cargo xtask build host`、
`cargo xtask build ios-universal` 或 `cargo xtask build android`，构建并同步对应平台的
C ABI 产物到 `../flare-core-examples/apps/flare-core-flutter-app`。

完整打包说明见 [`docs/flutter-packaging.md`](docs/flutter-packaging.md)。

## 线程安全

- callback 可能来自任意线程
- 禁止在 callback 中阻塞或持锁
- 所有 API 线程安全

## 平台支持

- iOS (Swift/Objective-C)
- Android (Kotlin/Java)
- Flutter (Dart)：完整应用在同级 **`flare-core-examples/apps/flare-core-flutter-app/`**；使用上述引擎 `cargo xtask build` 目标将 dylib / `.a` / `.so` 同步到对应平台目录。
- 鸿蒙 (ArkTS/C++)
- C/C++
- Node.js (N-API)
- Unity (C# P/Invoke)
