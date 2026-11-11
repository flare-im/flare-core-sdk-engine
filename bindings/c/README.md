# Flare IM SDK - C ABI Bindings

English · [中文](README.zh-CN.md)

Cross-platform C ABI SDK. It is the single common native L1 boundary for the
current multi-platform client SDK; the contract source is
[`../contract/manifest.json`](../contract/manifest.json). The complete
machine-readable contracts for APIs, events, and errors are, respectively:

- [`../contract/apis.json`](../contract/apis.json)
- [`../contract/events.json`](../contract/events.json)
- [`../contract/errors.json`](../contract/errors.json)

It supports runtimes that integrate over a native bridge: iOS, Android, Flutter,
HarmonyOS, C/C++, Node, Unity, React Native native modules, and so on.

## Architecture

- **Complex inside (Rust), simple outside (C ABI)**
- **All objects managed via handles**
- **Async APIs use callbacks**
- **Unified error-code error model**
- **Explicit memory management**

## File layout

```
bindings/c/
├── Cargo.toml          # project config
├── cbindgen.toml       # header-generation config
├── build.rs            # build script
├── ARCHITECTURE.md     # architecture design doc
└── src/
    ├── abi.rs           # panic boundary and FFI return protection
    ├── client_sync.rs   # IMClient sync/state pass-through
    ├── dispatch.rs      # JSON dispatch entry
    ├── event.rs         # EventBus subscription and event JSON mapping
    ├── executor.rs      # async -> callback_once
    ├── ffi_runtime.rs   # Tokio runtime
    ├── lib.rs           # entry point
    ├── types.rs         # C ABI type definitions
    ├── registry.rs      # handle registry
    ├── error_convert.rs # error conversion
    ├── helpers.rs       # helper utilities
    └── lifecycle.rs     # lifecycle API
```

## API

### Lifecycle

```c
// create / release
FlareHandle flare_sdk_create();
void flare_sdk_release(FlareHandle handle);

// init / login / logout
int32_t flare_sdk_init(FlareHandle handle, const char* config_json, void* context, FlareResultCallback callback);
int32_t flare_sdk_login(FlareHandle handle, const char* user_id, const char* token, const char* store_config_json, void* context, FlareResultCallback callback);
int32_t flare_sdk_logout(FlareHandle handle, void* context, FlareResultCallback callback);
int32_t flare_sdk_update_access_token(FlareHandle handle, const char* access_token, const char* tenant_id, void* context, FlareResultCallback callback);

// synchronous queries
FlareString flare_sdk_version();
bool flare_sdk_is_connected(FlareHandle handle);

// current user id (async, via callback)
int32_t flare_sdk_current_user_id(FlareHandle handle, void* context, FlareResultCallback callback);

// dev JWT (synchronous, handle-independent; `secret`/`issuer` may be NULL or empty for the built-in defaults; `tenant_id` may be NULL; `ttl_secs==0` means 3600; must flare_string_free)
```

### Memory management

```c
void flare_string_free(FlareString s);
void flare_bytes_free(FlareBytes b);
void flare_error_free(FlareError e);
```

### API coverage

Platform boundaries and stable shapes use the direct C ABI, e.g.:

- lifecycle: `flare_sdk_*`
- events: `flare_event_*`
- media transfer/progress/cancel: `flare_media_upload_*`,
  `flare_media_delete_file`, `flare_media_cancel_user_file_download`,
  `flare_media_download_file_to_downloads`
- simple message paths: `flare_message_create_text`, `flare_message_send`,
  `flare_message_list`, `flare_message_recall`, `flare_message_delete`

Large or fast-evolving conversation, media-control, message-build, and message
mutation operations use JSON dispatch:

```c
int32_t flare_conversation_dispatch_json(FlareHandle handle, const char* op, const char* params_json, void* context, FlareResultCallback callback);
int32_t flare_media_dispatch_json(FlareHandle handle, const char* op, const char* params_json, void* context, FlareResultCallback callback);
int32_t flare_message_build_json(FlareHandle handle, const char* request_json, void* context, FlareResultCallback callback);
int32_t flare_message_dispatch_json(FlareHandle handle, const char* op, const char* params_json, void* context, FlareResultCallback callback);
```

The mapping between every canonical API id, C entrypoint, and Tauri command is
authoritative in [`../contract/apis.json`](../contract/apis.json).

### Event coverage

`flare_event_subscribe` forwards every full `SdkEvent` domain event one at a
time. In the C callback, `event_type` is a stable integer code and `event_json`
is camelCase SDK JSON. For high-throughput synchronous scenarios prefer
`flare_event_subscribe_batch`, which delivers per callback:

```json
{ "events": [{ "eventType": 2001, "payload": {} }] }
```

where `payload` is identical to the per-event callback's `event_json` object.
Event codes and payload fields are authoritative in
[`../contract/events.json`](../contract/events.json).

## Build

```bash
cargo build -p flare-im-core-sdk-ffi --release
```

### Artifact sync for the Flutter reference app

Check out `flare-core-examples` alongside this repository. From the engine root,
run `cargo xtask build host`, `cargo xtask build ios-universal`, or
`cargo xtask build android` to build and sync C ABI artifacts into
`../flare-core-examples/apps/flare-core-flutter-app` for the selected platform.

For full packaging instructions see [`docs/flutter-packaging.md`](docs/flutter-packaging.md).

## Thread safety

- callbacks may come from any thread
- do not block or hold locks inside a callback
- all APIs are thread-safe

## Platform support

- iOS (Swift/Objective-C)
- Android (Kotlin/Java)
- Flutter (Dart): the full app is in the sibling **`flare-core-examples/apps/flare-core-flutter-app/`**; use the engine's `cargo xtask build` targets above to sync the dylib / `.a` / `.so` into its platform directories.
- HarmonyOS (ArkTS/C++)
- C/C++
- Node.js (N-API)
- Unity (C# P/Invoke)
