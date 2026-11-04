use anyhow::{Context, Result, bail};
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{core_root, remove_output_paths, upsert_bytes_file, upsert_text_file};

pub(crate) fn emit_bridge_files(root: &Path, check: bool) -> Result<()> {
    let mut drifted = Vec::new();
    if !check {
        clean_bridge_outputs(root)?;
    }
    sync_abi_header(root, check, &mut drifted)?;
    for target in bridge_targets(root) {
        upsert_text_file(&target.path, target.body, check, &mut drifted)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned bridge output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned bridge artifacts generated");
    }
    Ok(())
}

fn clean_bridge_outputs(root: &Path) -> Result<()> {
    remove_output_paths([
        root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge"),
        root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/FlareCoreSdk.swift"),
        root.join("packages/flare-core-apple-sdk/Sources/CFlareImCoreSdkFFI"),
        root.join("packages/flare-core-typescript-sdk/src/bridge"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/FlareCoreSdk.kt"),
        root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge"),
        root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/FlareCoreSdk.ets"),
        root.join("packages/flare-core-harmony-cangjie-sdk/src/bridge"),
        root.join("packages/flare-core-harmony-cangjie-sdk/src/FlareCoreSdk.cj"),
    ])
}

struct BridgeTarget {
    path: PathBuf,
    body: &'static str,
}

fn bridge_targets(root: &Path) -> Vec<BridgeTarget> {
    vec![
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareSdkException.swift",
            ),
            body: include_str!("../../templates/bridge/apple/Bridge/FlareSdkException.swift"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FfiContractVersionGuard.swift",
            ),
            body: include_str!(
                "../../templates/bridge/apple/Bridge/FfiContractVersionGuard.swift"
            ),
        },
        BridgeTarget {
            path: root
                .join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareCAbi.swift"),
            body: include_str!("../../templates/bridge/apple/Bridge/FlareCAbi.swift"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/NativeLibraryLoader.swift",
            ),
            body: include_str!("../../templates/bridge/apple/Bridge/NativeLibraryLoader.swift"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareNativeBindings.swift",
            ),
            body: include_str!("../../templates/bridge/apple/Bridge/FlareNativeBindings.swift"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FfiBridgeSupport.swift",
            ),
            body: include_str!("../../templates/bridge/apple/Bridge/FfiBridgeSupport.swift"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FfiNativeBridge.swift",
            ),
            body: include_str!("../../templates/bridge/apple/Bridge/FfiNativeBridge.swift"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareAppInfo.swift",
            ),
            body: include_str!("../../templates/bridge/apple/Bridge/FlareAppInfo.swift"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/FlareCoreSdk.swift"),
            body: include_str!("../../templates/bridge/apple/FlareCoreSdk.swift"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/bridge/flareSdkException.ts"),
            body: include_str!("../../templates/bridge/typescript/bridge/flareSdkException.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/bridge/contractVersion.ts"),
            body: include_str!("../../templates/bridge/typescript/bridge/contractVersion.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/bridge/wasmNativeBridge.ts"),
            body: include_str!("../../templates/bridge/typescript/bridge/wasmNativeBridge.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/bridge/ffiNativeBridge.ts"),
            body: include_str!("../../templates/bridge/typescript/bridge/ffiNativeBridge.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/bridge/index.ts"),
            body: include_str!("../../templates/bridge/typescript/bridge/index.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/adapters/web/flareCoreSdk.ts"),
            body: include_str!("../../templates/bridge/typescript/adapters/web/flareCoreSdk.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/adapters/react-native/flareCoreSdk.ts"),
            body: include_str!(
                "../../templates/bridge/typescript/adapters/react-native/flareCoreSdk.ts"
            ),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/adapters/uni-app/flareCoreSdk.ts"),
            body: include_str!("../../templates/bridge/typescript/adapters/uni-app/flareCoreSdk.ts"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/adapters/tauri/flareCoreSdk.ts"),
            body: include_str!("../../templates/bridge/typescript/adapters/tauri/flareCoreSdk.ts"),
        },
        BridgeTarget {
            path: root
                .join("packages/flare-core-typescript-sdk/src/adapters/tauri/tauriNativeBridge.ts"),
            body: include_str!(
                "../../templates/bridge/typescript/adapters/tauri/tauriNativeBridge.ts"
            ),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/adapters/tauri/index.ts"),
            body: include_str!("../../templates/bridge/typescript/adapters/tauri/index.ts"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge/FlareSdkException.kt",
            ),
            body: include_str!("../../templates/bridge/android/bridge/FlareSdkException.kt"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge/FfiContractVersionGuard.kt",
            ),
            body: include_str!(
                "../../templates/bridge/android/bridge/FfiContractVersionGuard.kt"
            ),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge/JniNativeBridge.kt",
            ),
            body: include_str!("../../templates/bridge/android/bridge/JniNativeBridge.kt"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge/FlareAppInfo.kt",
            ),
            body: include_str!("../../templates/bridge/android/bridge/FlareAppInfo.kt"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/FlareCoreSdk.kt"),
            body: include_str!("../../templates/bridge/android/FlareCoreSdk.kt"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/FlareSdkException.ets",
            ),
            body: include_str!("../../templates/bridge/arkts/bridge/FlareSdkException.ets"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/FfiContractVersionGuard.ets",
            ),
            body: include_str!(
                "../../templates/bridge/arkts/bridge/FfiContractVersionGuard.ets"
            ),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/FfiNativeBridge.ets",
            ),
            body: include_str!("../../templates/bridge/arkts/bridge/FfiNativeBridge.ets"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/FlareAppInfo.ets",
            ),
            body: include_str!("../../templates/bridge/arkts/bridge/FlareAppInfo.ets"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/index.ets"),
            body: include_str!("../../templates/bridge/arkts/bridge/index.ets"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/FlareCoreSdk.ets",
            ),
            body: include_str!("../../templates/bridge/arkts/adapter/FlareCoreSdk.ets"),
        },
        BridgeTarget {
            path: root.join(
                "packages/flare-core-harmony-cangjie-sdk/src/bridge/FfiContractVersionGuard.cj",
            ),
            body: include_str!(
                "../../templates/bridge/cangjie/bridge/FfiContractVersionGuard.cj"
            ),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-harmony-cangjie-sdk/src/bridge/FfiNativeBridge.cj"),
            body: include_str!("../../templates/bridge/cangjie/bridge/FfiNativeBridge.cj"),
        },
        BridgeTarget {
            path: root.join("packages/flare-core-harmony-cangjie-sdk/src/FlareCoreSdk.cj"),
            body: include_str!("../../templates/bridge/cangjie/FlareCoreSdk.cj"),
        },
    ]
}

fn sync_abi_header(root: &Path, check: bool, drifted: &mut Vec<String>) -> Result<()> {
    let source = core_root(root).join("target/flare_im_core_sdk_ffi.h");
    if !source.is_file() {
        return Ok(());
    }
    let header =
        fs::read(&source).with_context(|| format!("failed to read {}", source.display()))?;
    upsert_bytes_file(
        &root.join("native/abi/flare_im_core_sdk_ffi.h"),
        &header,
        check,
        drifted,
    )?;
    upsert_bytes_file(
        &root.join(
            "packages/flare-core-apple-sdk/Sources/CFlareImCoreSdkFFI/include/flare_im_core_sdk_ffi.h",
        ),
        &header,
        check,
        drifted,
    )?;
    upsert_text_file(
        &root.join("packages/flare-core-apple-sdk/Sources/CFlareImCoreSdkFFI/module.modulemap"),
        "module CFlareImCoreSdkFFI {\n    header \"include/flare_im_core_sdk_ffi.h\"\n    export *\n}\n",
        check,
        drifted,
    )
}
