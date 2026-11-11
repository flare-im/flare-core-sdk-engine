# Flutter 打包说明

本文说明如何将 `flare-core-sdk-engine/bindings/c` 生成的 C ABI 产物打包给 `flare-core-examples/apps/flare-core-flutter-app` 使用。

Flutter 示例工程位于：

```bash
flare-core-examples/apps/flare-core-flutter-app
```

Dart 侧通过 `dart:ffi` 加载 `libflare_im_core_sdk_ffi`：

- macOS 加载 `libflare_im_core_sdk_ffi.dylib`
- iOS 静态链接 `libflare_im_core_sdk_ffi.a`，运行时用 `DynamicLibrary.process()` 查符号
- Android 加载 `libflare_im_core_sdk_ffi.so`

因此 Rust FFI 产物必须先构建，再同步到 Flutter 工程的固定目录。

## 一键同步

在 `flare-core-sdk-engine` 目录执行：

```bash
cargo xtask build host wasm ios-universal android
```

该命令会执行：

- `host`：构建 host release 产物；macOS 上输出 `arm64+x86_64` universal dylib，并复制到 `flare-core-examples/apps/flare-core-flutter-app/macos/Runner/`
- `wasm`：构建 Web 端 WASM 产物并同步到 `dist/wasm`
- `ios-universal`：在 macOS 上构建 iOS Simulator `arm64+x86_64` universal 静态库，并复制到 `flare-core-examples/apps/flare-core-flutter-app/ios/FFI/build/libflare_im_core_sdk_ffi.a`
- `android`：设置 `ANDROID_NDK_ROOT` 后构建 Android `arm64-v8a`、`armeabi-v7a`、`x86_64` 三 ABI `.so`，并复制到 `flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/<abi>/`

## 按平台打包

macOS 调试：

```bash
cd flare-core-sdk-engine
cargo xtask build host
cd ../flare-core-examples/apps/flare-core-flutter-app
flutter run -d macos
```

iOS Apple Silicon 模拟器：

```bash
cd flare-core-sdk-engine
cargo xtask build ios-universal
cd ../flare-core-examples/apps/flare-core-flutter-app
flutter devices
flutter run -d "<simulator name or device id>"
```

例如当前机器发现的模拟器是 `iPhone 17 Pro`，可执行：

```bash
flutter run -d "iPhone 17 Pro"
```

也可以使用 `flutter devices` 输出中的完整设备 ID：

```bash
flutter run -d 96084A9D-1314-4BD5-B6BD-3D867ACE5909
```

iOS 真机：

```bash
cd flare-core-sdk-engine
cargo xtask build ios-device
cd ../flare-core-examples/apps/flare-core-flutter-app
flutter build ios
```

iOS 模拟器 universal 静态库：

```bash
cd flare-core-sdk-engine
cargo xtask build ios-universal
```

Android 全 ABI：

```bash
export ANDROID_NDK_ROOT=/path/to/android-ndk
cd flare-core-sdk-engine
cargo xtask build android
cd ../flare-core-examples/apps/flare-core-flutter-app
flutter run -d android
```

## 产物路径

Rust 产物路径相对 `flare-core-sdk-engine`；Flutter 目标路径相对工作区根。

| 平台 | Rust 产物 | Flutter 目标路径 |
| --- | --- | --- |
| macOS universal | `dist/host/libflare_im_core_sdk_ffi.dylib` | `flare-core-examples/apps/flare-core-flutter-app/macos/Runner/libflare_im_core_sdk_ffi.dylib` |
| iOS 模拟器 | `target/aarch64-apple-ios-sim/release/libflare_im_core_sdk_ffi.a` + `target/x86_64-apple-ios/release/libflare_im_core_sdk_ffi.a` | `flare-core-examples/apps/flare-core-flutter-app/ios/FFI/build/libflare_im_core_sdk_ffi.a` |
| iOS 真机 | `target/aarch64-apple-ios/release/libflare_im_core_sdk_ffi.a` | `flare-core-examples/apps/flare-core-flutter-app/ios/FFI/build/libflare_im_core_sdk_ffi.a` |
| Android arm64 | `target/aarch64-linux-android/release/libflare_im_core_sdk_ffi.so` | `flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/arm64-v8a/libflare_im_core_sdk_ffi.so` |
| Android armeabi-v7a | `target/armv7-linux-androideabi/release/libflare_im_core_sdk_ffi.so` | `flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/armeabi-v7a/libflare_im_core_sdk_ffi.so` |
| Android x86_64 | `target/x86_64-linux-android/release/libflare_im_core_sdk_ffi.so` | `flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/x86_64/libflare_im_core_sdk_ffi.so` |

## 依赖和注意事项

- 首次构建会自动执行 `rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios aarch64-linux-android armv7-linux-androideabi x86_64-linux-android`。
- Android 必须设置 `ANDROID_NDK_ROOT`。`cargo xtask build android-verify` 会校验三 ABI 均已落地。
- Xcode iOS 构建阶段通过 `cargo xtask build ios-verify` 校验当前 SDK 需要的静态库架构：真机要求 `arm64`，模拟器要求 `arm64+x86_64` universal。
- iOS 使用静态库链接，Flutter 工程需要通过 Xcode 配置把 `ios/FFI/build/libflare_im_core_sdk_ffi.a` 以 `-force_load` 方式链接进 Runner。
- Xcode 不再内置 `build_rust_ffi.sh`；每次修改 `bindings/c`、SDK API 或 Rust 依赖后，提交/打包前都要重新执行对应 `cargo xtask build ...` 目标。
- 如果 Dart FFI 新增函数，必须同时更新 Flutter SDK/adapter 的生成源并重新生成产物，不能手写旧绑定兜底。

## 验证

Rust FFI 编译检查：

```bash
cargo test -p xtask build::tests
cargo xtask build android-verify
cargo xtask build ios-verify
```

Flutter 侧静态检查：

```bash
cd flare-core-examples/apps/flare-core-flutter-app
flutter analyze
```

如果 macOS 运行时报 `无法加载 libflare_im_core_sdk_ffi.dylib`，先确认已执行 `cargo xtask build host`，并检查 `.app/Contents/MacOS/` 或 `.app/Contents/Frameworks/` 是否能找到该 dylib。
