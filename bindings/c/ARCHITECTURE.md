# 跨平台 C ABI SDK 架构设计

## 概述

本 SDK 严格遵守 C ABI 规范,支持以下平台:
- iOS (Swift/Objective-C)
- Android (Kotlin/Java)
- Flutter (Dart)
- 鸿蒙 (ArkTS/C++)
- C/C++
- Node.js (N-API)
- Unity (C# P/Invoke)

## 核心设计原则

### 1. 类型系统

只允许以下 C ABI 类型:

```c
// 基础类型
i32, i64, u32, u64, bool

// 句柄
typedef uint64_t FlareHandle;

// 字符串
typedef struct {
    char* ptr;
    size_t len;
} FlareString;

// 字节
typedef struct {
    uint8_t* ptr;
    size_t len;
    size_t cap;
} FlareBytes;

// 错误
typedef struct {
    int32_t code;
    FlareString message;
    FlareString details_json;
} FlareError;
```

### 2. 句柄管理

所有 Rust 对象通过句柄管理:

```rust
// 内部使用 DashMap 管理对象
static HANDLE_REGISTRY: DashMap<u64, Arc<T>> = DashMap::new();

// 提供 retain/release
FlareHandle flare_sdk_create();
void flare_sdk_release(FlareHandle handle);
```

### 3. 异步模型

所有耗时操作使用回调:

```c
// 结果回调
typedef void (*FlareResultCallback)(
    void* context,
    const FlareError* error,
    FlareString result_json
);

// API 返回 i32
int32_t flare_sdk_init(
    FlareHandle handle,
    const char* config_json,
    void* context,
    FlareResultCallback callback
);
```

**规则:**
- 返回 0 表示提交成功
- 返回非 0 表示立即错误
- 最终结果通过 callback 返回
- callback 保证调用一次

### 4. 错误模型

统一错误码:

```c
#define FLARE_OK 0
#define FLARE_ERR_INVALID_HANDLE 1
#define FLARE_ERR_INVALID_PARAM 2
#define FLARE_ERR_NOT_CONNECTED 3
// ...
```

**规则:**
- 禁止 panic 跨边界
- 禁止字符串判断错误
- 所有错误通过 FlareError 结构

### 5. 内存管理

谁分配谁释放:

```c
// Rust 分配,调用方释放
FlareString flare_sdk_version();
void flare_string_free(FlareString s);

// 调用方分配,Rust 不持有
int32_t flare_sdk_init(
    FlareHandle handle,
    const char* config_json,  // 调用方持有
    ...
);
```

### 6. JSON 传输

复杂参数和返回值使用 JSON:

```c
// 输入: JSON 字符串
int32_t flare_message_send(
    FlareHandle handle,
    const char* message_json,  // JSON 参数
    ...
);

// 输出: JSON 字符串
void callback(
    void* context,
    const FlareError* error,
    FlareString result_json  // JSON 结果
);
```

## API 分类

### 1. 生命周期 API

```c
// 创建/释放
FlareHandle flare_sdk_create();
void flare_sdk_release(FlareHandle handle);

// 初始化/登录/登出
int32_t flare_sdk_init(...);
int32_t flare_sdk_login(...);
int32_t flare_sdk_logout(...);

// 同步查询
FlareString flare_sdk_version();
bool flare_sdk_is_connected(FlareHandle handle);
FlareString flare_sdk_current_user_id(FlareHandle handle);
```

### 2. 命令 API

```c
// 消息
int32_t flare_message_send(...);
int32_t flare_message_recall(...);
int32_t flare_message_delete(...);

// 会话
int32_t flare_conversation_dispatch_json(...);

// 媒体
int32_t flare_media_dispatch_json(...);
int32_t flare_media_upload_file(...);
int32_t flare_media_upload_image(...);
int32_t flare_media_upload_video(...);
int32_t flare_media_upload_bytes(...);
int32_t flare_media_download_file_to_downloads(...);
```

### 3. 查询 API

```c
// 消息
int32_t flare_message_get(...);
int32_t flare_message_list(...);
int32_t flare_message_search(...);

// 会话
int32_t flare_conversation_dispatch_json(...);
```

### 4. 监听 API

```c
// 事件订阅
typedef void (*FlareEventCallback)(
    void* context,
    int32_t event_type,
    FlareString event_json
);

typedef void (*FlareEventBatchCallback)(
    void* context,
    size_t event_count,
    FlareString events_json
);

FlareSubscriptionHandle flare_event_subscribe(...);
FlareSubscriptionHandle flare_event_subscribe_batch(...);
void flare_event_unsubscribe(FlareSubscriptionHandle handle);
```

## 线程安全

### 规则

1. **callback 线程不确定**
   - 可能来自任意 Tokio worker 线程
   - 禁止假设主线程

2. **禁止阻塞**
   - 禁止在 callback 中阻塞
   - 禁止在 callback 中持锁

3. **句柄线程安全**
   - 所有句柄操作线程安全
   - DashMap 保证并发安全

### 文档说明

```c
/**
 * @warning callback 可能来自任意线程
 * @note 不要在 callback 中阻塞或持锁
 */
int32_t flare_message_send(
    FlareHandle handle,
    const char* message_json,
    void* context,
    FlareResultCallback callback
);
```

## 代码生成规则

### 必须包含

```rust
// 1. 句柄注册表
mod registry;

// 2. 错误转换
mod error_convert;

// 3. 辅助工具
mod helpers;

// 4. 回调执行器
mod executor;
```

### 禁止重复

```rust
// ✗ 错误: 每个函数重复 spawn
pub extern "C" fn flare_xxx(...) {
    instance.runtime.spawn(async move {
        // ...
    });
}

// ✓ 正确: 使用执行器
pub extern "C" fn flare_xxx(...) {
    execute_async(instance, ctx, op, to_json);
}
```

### 错误处理

```rust
// ✗ 错误: unwrap/panic
let config = parse_json(config_json).unwrap();

// ✓ 正确: 错误转换
let config = match parse_json(config_json) {
    Ok(c) => c,
    Err(code) => {
        return_error(&ctx, code, "Invalid config");
        return code;
    }
};
```

## 文件结构

```
bindings/c/
├── Cargo.toml
├── cbindgen.toml
├── build.rs
└── src/
    ├── lib.rs           # 入口
    ├── types.rs         # C ABI 类型定义
    ├── registry.rs      # 句柄注册表
    ├── error_convert.rs # 错误转换
    ├── helpers.rs       # 辅助工具
    ├── executor.rs      # 回调执行器
    ├── lifecycle.rs     # 生命周期 API
    ├── message.rs       # 消息 API
    ├── conversation.rs  # 会话 API
    ├── media.rs         # 媒体 API
    └── event.rs         # 事件 API
```

## 平台适配

### iOS (Swift)

```swift
// 加载动态库
let lib = dlopen("libflare_im_sdk.dylib", RTLD_NOW)

// 定义函数类型
typealias FlareSdkCreate = @convention(c) () -> UInt64

// 调用
let handle = flare_sdk_create()
```

### Android (Kotlin)

```kotlin
// 加载动态库
companion object {
    init {
        System.loadLibrary("flare_im_core_sdk")
    }
}

// 定义 native 方法
private external fun nativeCreate(): Long

// 调用
val handle = nativeCreate()
```

### Flutter (Dart)

```dart
// 加载动态库
final lib = DynamicLibrary.open('libflare_im_sdk.so');

// 绑定函数
final flareSdkCreate = lib
    .lookupFunction<Uint64 Function(), int Function()>('flare_sdk_create');

// 调用
final handle = flareSdkCreate();
```

## 总结

本 SDK 设计满足:

1. ✓ 跨 iOS、Android、Flutter、鸿蒙
2. ✓ ABI 稳定 (只使用基础类型)
3. ✓ 异步安全 (callback 模型)
4. ✓ 内存安全 (显式释放)
5. ✓ 易扩展 (模块化设计)
6. ✓ 易生成 bindings (cbindgen 兼容)

核心优势:
- 统一的错误模型
- 统一的异步模型
- 统一的内存管理
- 最小化平台适配代码
