# HarmonyOS / OpenHarmony 交叉编译

## 前置

1. 安装 OpenHarmony SDK 并设置：

```bash
export OHOS_NDK_HOME=/path/to/openharmony/native
```

2. Rust target：

```bash
rustup target add aarch64-unknown-linux-ohos
```

## 构建

```bash
cd flare-core-sdk-engine/bindings/c
make ohos
# 或：
../../flare-core-sdk/tools/sync_harmony_native.sh
```

产物：`target/aarch64-unknown-linux-ohos/release/libflare_im_core_sdk_ffi.so`

## 链接器

`Makefile` 使用 `$OHOS_NDK_HOME/native/llvm/bin/aarch64-unknown-linux-ohos-clang` 作为
`CARGO_TARGET_AARCH64_UNKNOWN_LINUX_OHOS_LINKER`。
