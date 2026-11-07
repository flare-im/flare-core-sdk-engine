use anyhow::{Context, Result};
use regex::Regex;
use std::{fs, path::Path};

use crate::{
    core_root, emit_errors, fail, file_contains, files_under, find_matching, load_json,
    single_trailing_newline, spec_dir, wire_boundary_targets,
};

const REQUIRED_PATHS: &[&str] = &[
    "sdk-spec/manifest.json",
    "native/README.md",
    "packages/flare-core-flutter-sdk",
    "packages/flare-core-android-sdk",
    "packages/flare-core-apple-sdk",
    "packages/flare-core-harmony-arkts-sdk",
    "packages/flare-core-harmony-cangjie-sdk",
    "packages/flare-core-typescript-sdk",
    "packages/flare-core-flutter-sdk/analysis_options.yaml",
    "packages/flare-core-apple-sdk/Package.swift",
    "packages/flare-core-harmony-arkts-sdk/oh-package.json5",
    "packages/flare-core-harmony-arkts-sdk/build-profile.json5",
    "packages/flare-core-harmony-arkts-sdk/src/main/module.json5",
    "packages/flare-core-harmony-arkts-sdk/src/main/cpp/CMakeLists.txt",
    "packages/flare-core-harmony-arkts-sdk/src/main/cpp/napi_bridge.cpp",
    "packages/flare-core-harmony-cangjie-sdk/cjpm.toml",
    "packages/flare-core-typescript-sdk/package.json",
    "packages/flare-core-typescript-sdk/tsconfig.json",
    // 契约用例不在共享目录：golden 样例在 sdk-spec/golden，消费它们的用例跟着各包走
    // （sdk-spec/README.md「Contract tests」）。原先点名的 packages/shared/contract-tests
    // 只有一份「Planned structure」的 README，已删除并列入 RETIRED_PATHS。
    "sdk-spec/golden/requests",
    "sdk-spec/golden/responses",
    "sdk-spec/golden/events",
    "packages/flare-core-typescript-sdk/test/wire_codec_contract.test.ts",
    "packages/flare-core-typescript-sdk/test/events_api_contract.test.ts",
    "packages/flare-core-typescript-sdk/test/capabilities_api_contract.test.ts",
    "packages/flare-core-flutter-sdk/test/wire_codec_message_test.dart",
    "packages/flare-core-flutter-sdk/test/default_flare_im_client_direct_ffi_test.dart",
    "docs/client-api-reference.md",
    "docs/client-model-reference.md",
    "sdk-spec/GENERATED.md",
    "sdk-spec/generated/client_spec.json",
    "sdk-spec/models/conversations.json",
    "sdk-spec/models/messages.json",
    "sdk-spec/models/message_builder.json",
    "sdk-spec/models/message_content_elems.json",
    "sdk-spec/shared/message_build_catalog.json",
    "sdk-spec/native/c_abi.json",
    "sdk-spec/modules/message_builder.json",
    "sdk-spec/models/events.json",
    "sdk-spec/shared/listeners.json",
    "packages/flare-core-flutter-sdk/lib/flare_core_flutter_sdk.dart",
    "packages/flare-core-flutter-sdk/lib/src/contract/contract.dart",
    "packages/flare-core-flutter-sdk/lib/src/api/api.dart",
    "packages/flare-core-flutter-sdk/lib/src/api/modules/message_builder.dart",
    "packages/flare-core-flutter-sdk/lib/src/callback/callback.dart",
    "packages/flare-core-flutter-sdk/lib/src/contract/bridge_contract.dart",
    "packages/flare-core-flutter-sdk/lib/src/listener/listener.dart",
    "packages/flare-core-flutter-sdk/lib/src/listener/connection.dart",
    "packages/flare-core-flutter-sdk/lib/src/listener/conversation.dart",
    "packages/flare-core-flutter-sdk/lib/src/listener/message.dart",
    "packages/flare-core-flutter-sdk/lib/src/listener/media.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/model.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/entity/conversation.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/entity/message.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/event/sdk_event_envelope.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/event/lifecycle/lifecycle_event.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/event/progress/progress_event.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/common/enums/conversation_type.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/common/enums/message_content_type.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/event/lifecycle/lifecycle_event_name.dart",
    "packages/flare-core-flutter-sdk/lib/src/model/catalog/message_build_catalog.dart",
    "packages/flare-core-flutter-sdk/lib/src/flare_core_sdk.dart",
    "packages/flare-core-flutter-sdk/lib/src/adapter/default_flare_im_client.dart",
    "packages/flare-core-flutter-sdk/lib/src/adapter/codec/wire_codec.dart",
    "packages/flare-core-flutter-sdk/lib/src/adapter/module/default_message_builder_api.dart",
    "packages/flare-core-flutter-sdk/lib/src/lifecycle/platform_lifecycle.dart",
    "packages/flare-core-flutter-sdk/lib/src/bridge/ffi_native_bridge.dart",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/FlareCoreSdk.kt",
    "packages/flare-core-android-sdk/build.gradle.kts",
    "packages/flare-core-android-sdk/src/main/AndroidManifest.xml",
    "packages/flare-core-android-sdk/src/main/cpp/CMakeLists.txt",
    "packages/flare-core-android-sdk/src/main/cpp/flare_jni_bridge.cpp",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/api/FlareImClient.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/api/messagebuilder/MessageBuilderApi.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/callback/MessageSendCallback.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/SdkContract.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/NativeBridge.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/NativeCallDescriptor.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/NativeCallMap.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/listener/FlareImEventListener.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/listener/ConnectionListener.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/listener/ConversationListener.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/listener/MessageListener.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/listener/MediaListener.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/entity/Conversation.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/entity/Message.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/event/SdkEventEnvelope.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/event/lifecycle/LifecycleEvent.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/event/progress/ProgressEvent.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/common/enums/ConversationType.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/common/enums/MessageContentType.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/event/lifecycle/LifecycleEventName.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/catalog/MessageBuildCatalog.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/DefaultFlareImClient.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultMessageBuilderApi.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/codec/NativeInvoke.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/codec/WireCodec.kt",
    "packages/flare-core-android-lifecycle/build.gradle.kts",
    "packages/flare-core-android-lifecycle/src/main/kotlin/com/flare/im/lifecycle/PlatformLifecycle.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/lifecycle/ClientPlatformLifecycle.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge/JniNativeBridge.kt",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge/FlareSdkException.kt",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Contract/SdkContract.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Contract/BridgeContract.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Api/FlareImClientApi.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/FlareCoreSdk.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FfiNativeBridge.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareNativeBindings.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FfiBridgeSupport.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/NativeLibraryLoader.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareCAbi.swift",
    "native/abi/flare_im_core_sdk_ffi.h",
    "packages/flare-core-apple-sdk/Sources/CFlareImCoreSdkFFI/module.modulemap",
    "packages/flare-core-apple-sdk/Sources/CFlareImCoreSdkFFI/include/flare_im_core_sdk_ffi.h",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FlareSdkException.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/DefaultFlareImClient.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Codec/WireCodec.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultMessageBuilderApi.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Callback/MessageSendCallback.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Listener/FlareImEventListener.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Listener/ConnectionListener.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Listener/ConversationListener.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Listener/MessageListener.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Listener/MediaListener.swift",
    // ⚠️ 这一段的大小写必须与 git 里的真实路径逐字一致。Apple 侧 Model 的子目录
    // 是小写（entity / event / common / lifecycle / progress），而文件名是 PascalCase
    // ——这是生成器产出的形态。此前这里写成了 Model/Entity/... ，macOS 的大小写不敏感
    // 文件系统照样能找到，所以本机永远绿；直到这道门禁第一次跑上 Linux runner 才
    // 一次报出 8 个「missing required path」。本机测不出来的差异，只有 CI 能替你测。
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/entity/Conversation.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/entity/Message.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/event/SdkEventEnvelope.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/event/lifecycle/LifecycleEvent.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/event/progress/ProgressEvent.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/common/enums/ConversationType.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/common/enums/MessageContentType.swift",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model/event/lifecycle/LifecycleEventName.swift",
    "packages/flare-core-apple-sdk/Sources/FlarePlatformLifecycle/PlatformLifecycle.swift",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/contract/SdkContract.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/Index.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/api/index.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/api/modules/message_builder.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/contract/BridgeContract.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/listener/index.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/listener/connection.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/listener/conversation.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/listener/message.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/listener/media.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/index.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/entity/conversation.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/entity/message.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/event/sdk_event_envelope.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/event/lifecycle/lifecycle_event.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/event/progress/progress_event.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/common/enums/conversation_type.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/common/enums/message_content_type.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/model/event/lifecycle/lifecycle_event_name.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/index.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/DefaultFlareImClient.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/FlareCoreSdk.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/FfiNativeBridge.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge/index.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/codec/NativeInvoke.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/codec/WireCodec.ets",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/lifecycle/PlatformLifecycle.ets",
    "packages/flare-core-harmony-cangjie-sdk/cjpm.toml",
    "packages/flare-core-harmony-cangjie-sdk/src/FlareCoreSdk.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/bridge/FfiNativeBridge.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/adapter/DefaultFlareImClient.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/contract/SdkContract.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/contract/BridgeContract.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/api/FlareImClient.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/listener/FlareImEventListener.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/listener/ConnectionListener.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/listener/ConversationListener.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/listener/MessageListener.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/listener/MediaListener.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/entity/Conversation.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/entity/Message.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/event/SdkEventEnvelope.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/event/lifecycle/LifecycleEvent.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/event/progress/ProgressEvent.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/common/enums/ConversationType.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/common/enums/MessageContentType.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/model/event/lifecycle/LifecycleEventName.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/adapter/module/DefaultConnectionApi.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/adapter/module/DefaultMessageBuilderApi.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/adapter/codec/WireCodec.cj",
    "packages/flare-core-harmony-cangjie-sdk/src/lifecycle/PlatformLifecycle.cj",
    "packages/flare-core-typescript-sdk/src/index.ts",
    "packages/flare-core-typescript-sdk/src/api/index.ts",
    "packages/flare-core-typescript-sdk/src/contract/bridge_contract.ts",
    "packages/flare-core-typescript-sdk/src/callback/index.ts",
    "packages/flare-core-typescript-sdk/src/listener/index.ts",
    "packages/flare-core-typescript-sdk/src/listener/connection.ts",
    "packages/flare-core-typescript-sdk/src/listener/conversation.ts",
    "packages/flare-core-typescript-sdk/src/listener/message.ts",
    "packages/flare-core-typescript-sdk/src/listener/media.ts",
    "packages/flare-core-typescript-sdk/src/model/index.ts",
    "packages/flare-core-typescript-sdk/src/model/conversation.ts",
    "packages/flare-core-typescript-sdk/src/model/message.ts",
    "packages/flare-core-typescript-sdk/src/model/sdk_event_envelope.ts",
    "packages/flare-core-typescript-sdk/src/model/lifecycle_event.ts",
    "packages/flare-core-typescript-sdk/src/model/progress_event.ts",
    "packages/flare-core-typescript-sdk/src/model/conversation_type.ts",
    "packages/flare-core-typescript-sdk/src/model/message_content_type.ts",
    "packages/flare-core-typescript-sdk/src/model/lifecycle_event_name.ts",
    "packages/flare-core-typescript-sdk/src/lifecycle/platformLifecycle.ts",
    "packages/flare-core-typescript-sdk/src/lifecycle/index.ts",
    "packages/flare-core-typescript-sdk/src/adapters/web/index.ts",
    "packages/flare-core-typescript-sdk/src/adapters/web/flareCoreSdk.ts",
    "packages/flare-core-typescript-sdk/src/bridge/contractVersion.ts",
    "packages/flare-core-typescript-sdk/src/bridge/wasmNativeBridge.ts",
    "packages/flare-core-typescript-sdk/src/bridge/ffiNativeBridge.ts",
    "packages/flare-core-typescript-sdk/src/adapters/react-native/index.ts",
    "packages/flare-core-typescript-sdk/src/adapters/uni-app/index.ts",
    "packages/flare-core-typescript-sdk/src/adapters/tauri/index.ts",
    "packages/flare-core-typescript-sdk/src/adapters/tauri/flareCoreSdk.ts",
    "packages/flare-core-typescript-sdk/src/adapters/tauri/tauriNativeBridge.ts",
];

const RETIRED_PATHS: &[&str] = &[
    // The generated foreground/background bridges, replaced by each platform's PlatformLifecycle.
    "packages/flare-core-typescript-sdk/src/lifecycle/heartbeatLifecycle.ts",
    "packages/flare-core-flutter-sdk/lib/src/lifecycle/heartbeat_lifecycle_bridge.dart",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/lifecycle/HeartbeatLifecycleBridge.kt",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Lifecycle/HeartbeatLifecycleBridge.swift",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/lifecycle/HeartbeatLifecycleBridge.ets",
    "packages/flare-core-harmony-cangjie-sdk/src/lifecycle/HeartbeatLifecycleBridge.cj",
    "CLIENT_API_PARITY.md",
    "ARCHITECTURE.md",
    "tools",
    "tools/codegen/gen_contract.py",
    "tools/codegen/model_layout.py",
    "tools/codegen/platform_arkts_adapter_emit.py",
    "tools/codegen/platform_cangjie_adapter_emit.py",
    "tools/codegen/platform_kotlin_adapter_emit.py",
    "tools/codegen/platform_swift_adapter_emit.py",
    "tools/codegen/platform_wire_boundary_emit.py",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/generated",
    "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/runtime",
    "packages/flare-core-flutter-sdk/lib/src/runtime",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Runtime",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/runtime",
    "packages/flare-core-harmony-cangjie-sdk/src/runtime",
    "packages/flare-core-harmony-cangjie-sdk/src/FlareImClient.cj",
    "packages/flare-core-flutter-sdk/lib/src/generated",
    "packages/flare-core-flutter-sdk/lib/src/models",
    "packages/flare-core-flutter-sdk/lib/src/listeners",
    "packages/flare-core-flutter-sdk/lib/src/client_contract.dart",
    "packages/flare-core-flutter-sdk/lib/src/bridge_contract.dart",
    "packages/flare-core-flutter-sdk/lib/src/callbacks.dart",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Generated",
    "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/FlareImClient.swift",
    "packages/flare-core-harmony-arkts-sdk/src/main/ets/generated",
    "packages/flare-core-harmony-cangjie-sdk/src/generated",
    "packages/flare-core-typescript-sdk/src/models",
    "packages/flare-core-typescript-sdk/src/listeners",
    "packages/flare-core-typescript-sdk/src/client_contract.ts",
    "packages/flare-core-typescript-sdk/src/bridge_contract.ts",
    "packages/flare-core-typescript-sdk/src/callbacks.ts",
    "packages/flare-core-typescript-sdk/src/bridge/tauriNativeBridge.ts",
    "packages/flare-core-typescript-sdk/src/adapters/web/legacy",
    "packages/shared/contract-tests",
];

pub(crate) fn verify_structure(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    if find_matching(root, &["tools/codegen", "tools/verify"], |path| {
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            return false;
        };
        name.starts_with("migrate_") && name.ends_with(".py")
            || name.contains("legacy") && name.ends_with(".py")
    })?
    .is_some()
    {
        fail(
            &mut errors,
            "retired Python migration or legacy helper still exists",
        );
    }
    if find_matching(root, &["tools/codegen"], |path| {
        path.extension()
            .and_then(|value| value.to_str())
            .is_some_and(|extension| extension == "py")
    })?
    .is_some()
    {
        fail(
            &mut errors,
            "Python codegen file found under client-sdk tools/codegen; use core-sdk cargo xtask instead",
        );
    }
    if find_matching(root, &["tools"], |path| {
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        name == "__pycache__" || name.ends_with(".pyc") || name.ends_with(".pyo")
    })?
    .is_some()
    {
        fail(
            &mut errors,
            "Python bytecode/cache artifact found under tools/",
        );
    }

    scan_forbidden_typescript_compat(root, &mut errors)?;
    scan_forbidden_wire_codec_normalization(root, &mut errors)?;
    verify_rust_owned_wire_codecs(root, &mut errors)?;
    scan_forbidden_host_json_boundary_fields(root, &mut errors)?;
    scan_forbidden_event_contract_json_fields(root, &mut errors)?;
    scan_removed_raw_build_api(root, &mut errors)?;
    verify_required_paths(root, &mut errors);
    scan_platform_lifecycle_wiring(root, &mut errors)?;
    scan_l2_adapter_boundaries(root, &mut errors)?;
    verify_retired_paths_absent(root, &mut errors);
    scan_retired_package_refs(root, &mut errors)?;
    load_json(&spec_dir(root).join("manifest.json"))?;

    emit_errors("client SDK structure error", errors)?;
    println!("client SDK structure verified");
    Ok(())
}

fn verify_rust_owned_wire_codecs(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let core = core_root(root);
    let codecs: &[(&str, &str)] = &[
        (
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/codec/WireCodec.kt",
            "xtask/templates/android-adapter/codec/WireCodec.kt",
        ),
        (
            "packages/flare-core-flutter-sdk/lib/src/adapter/codec/wire_codec.dart",
            "xtask/templates/flutter-adapter/codec/wire_codec.dart",
        ),
        (
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Codec/WireCodec.swift",
            "xtask/templates/apple-adapter/codec/WireCodec.swift",
        ),
        (
            "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/codec/WireCodec.ets",
            "xtask/templates/harmony-arkts-adapter/codec/WireCodec.ets",
        ),
        (
            "packages/flare-core-harmony-cangjie-sdk/src/adapter/codec/WireCodec.cj",
            "xtask/templates/harmony-cangjie-adapter/codec/WireCodec.cj",
        ),
    ];
    for (product_rel, template_rel) in codecs {
        let product_path = root.join(product_rel);
        let template_path = core.join(template_rel);
        let product = match fs::read_to_string(&product_path) {
            Ok(product) => product,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fail(
                    errors,
                    format!("missing Rust-owned WireCodec product: {product_rel}"),
                );
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", product_path.display()));
            }
        };
        let template = match fs::read_to_string(&template_path) {
            Ok(template) => template,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fail(
                    errors,
                    format!("missing Rust-owned WireCodec template: {template_rel}"),
                );
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", template_path.display()));
            }
        };
        if product != single_trailing_newline(&template) {
            fail(
                errors,
                format!(
                    "Rust-owned WireCodec drifted: {product_rel}; update {template_rel} or run xtask codegen"
                ),
            );
        }
    }
    Ok(())
}

fn scan_forbidden_typescript_compat(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let re = Regex::new(
        r#"websocket_with_http_fallback|offline_only|ttlSeconds|ttl_secs|value\.user_id|value\.tenant_id|value\.device_id|req\.user_id|cdn_url|local_path|expires_in|'content_type'|"content_type"|'message_type'|"message_type"|sticker_id|package_id"#,
    )?;
    let root_path = root.join("packages/flare-core-typescript-sdk/src");
    for path in files_under(&root_path)? {
        if path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.ends_with(".test.ts"))
        {
            continue;
        }
        if file_contains(&path, &re)? {
            fail(
                errors,
                "removed TypeScript compatibility field or stale transport policy found",
            );
            break;
        }
    }
    Ok(())
}

fn scan_forbidden_wire_codec_normalization(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let re = Regex::new(
        r#"normalizeWireResponseValue|convertFromSnakeCase|fromSnakeCase|snake_case|event\["event_type"\]|@JsonKey|JsonKey\(|SerializedName|raw\.size == 0"#,
    )?;
    for target in wire_boundary_targets(root) {
        if !target.path.is_file() {
            continue;
        }
        if file_contains(&target.path, &re)? {
            fail(
                errors,
                format!(
                    "legacy wire normalization found in {}",
                    target
                        .path
                        .strip_prefix(root)
                        .unwrap_or(&target.path)
                        .display()
                ),
            );
        }
    }
    Ok(())
}

fn scan_forbidden_host_json_boundary_fields(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let path = root.join("storage/indexeddb/src/host.rs");
    if !path.is_file() {
        return Ok(());
    }
    let contents =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    if contents.contains(r#""user_id""#)
        || (contents.contains("user_id: &'a str")
            && !contents.contains(r#"#[serde(rename_all = "camelCase")]"#))
    {
        fail(
            errors,
            "legacy host JSON field found in storage/indexeddb/src/host.rs",
        );
    }
    Ok(())
}

fn scan_forbidden_event_contract_json_fields(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let path = root.join("bindings/contract/events.json");
    if !path.is_file() {
        return Ok(());
    }
    let value = load_json(&path)?;
    let batch_event = value
        .pointer("/cAbi/batchJson/events/0")
        .and_then(|value| value.as_object());
    if let Some(event) = batch_event
        && event.keys().any(|key| key.contains('_'))
    {
        fail(
            errors,
            "legacy event contract JSON field found in cAbi.batchJson",
        );
    }

    let Some(events) = value.get("events").and_then(|value| value.as_array()) else {
        return Ok(());
    };
    for event in events {
        let Some(fields) = event
            .pointer("/cJson/fields")
            .and_then(|value| value.as_array())
        else {
            continue;
        };
        if fields
            .iter()
            .filter_map(|field| field.as_str())
            .any(|field| field.contains('_'))
        {
            fail(
                errors,
                "legacy event contract JSON field found in cJson.fields",
            );
            break;
        }
    }
    Ok(())
}

fn scan_removed_raw_build_api(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let re = Regex::new(
        r#"['"]message\.build['"]|BuildMessageRequest|buildRaw|sdkUiCompat|useLegacyDesktopBreakpoint"#,
    )?;
    for rel in ["sdk-spec", "packages", "docs"] {
        let path = root.join(rel);
        for file in files_under(&path)? {
            if should_skip_scan_file(&file) {
                continue;
            }
            if file_contains(&file, &re)? {
                fail(
                    errors,
                    "removed raw message build or compatibility API found",
                );
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Every platform package ships a PlatformLifecycle that reports network changes and
/// foreground/background to the core, and installs it at SDK init behind the
/// `installPlatformLifecycle` option (see .planning platform-lifecycle contract).
struct PlatformLifecycleRequirement {
    file: &'static str,
    tokens: &'static [&'static str],
    /// Package sources searched for the init-time installation.
    sources: &'static str,
}

const PLATFORM_LIFECYCLE_REQUIREMENTS: &[PlatformLifecycleRequirement] = &[
    PlatformLifecycleRequirement {
        file: "packages/flare-core-typescript-sdk/src/lifecycle/platformLifecycle.ts",
        tokens: &[
            "lifecycle.setAppState",
            "AppState.Foreground",
            "AppState.Background",
            "visibilitychange",
            "pagehide",
            "pageshow",
            "notifyNetworkChange",
            "fingerprintHint",
        ],
        sources: "packages/flare-core-typescript-sdk/src",
    },
    PlatformLifecycleRequirement {
        // Its own Gradle module (flare-core-android-lifecycle), reusable without the core binding.
        file: "packages/flare-core-android-lifecycle/src/main/kotlin/com/flare/im/lifecycle/PlatformLifecycle.kt",
        tokens: &[
            "lifecycle.setAppState",
            "AppState.FOREGROUND",
            "AppState.BACKGROUND",
            "notifyNetworkChange",
            "fingerprintHint",
            "registerDefaultNetworkCallback",
            "ProcessLifecycleOwner",
        ],
        sources: "packages/flare-core-android-sdk/src/main/kotlin",
    },
    PlatformLifecycleRequirement {
        // Its own SwiftPM target (FlarePlatformLifecycle), reusable without the core binding.
        file: "packages/flare-core-apple-sdk/Sources/FlarePlatformLifecycle/PlatformLifecycle.swift",
        tokens: &[
            "lifecycle.setAppState",
            ".foreground",
            ".background",
            "notifyNetworkChange",
            "fingerprintHint",
            "NWPathMonitor",
        ],
        sources: "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK",
    },
    PlatformLifecycleRequirement {
        file: "packages/flare-core-flutter-sdk/lib/src/lifecycle/platform_lifecycle.dart",
        tokens: &[
            "lifecycle.setAppState",
            "AppState.foreground",
            "AppState.background",
            "notifyNetworkChange",
            "fingerprintHint",
        ],
        // The core package is pure Dart: the Flutter sources (AppLifecycleListener, connectivity,
        // app version) live in the flare-core-flutter-lifecycle plugin and are handed in at init.
        sources: "packages/flare-core-flutter-sdk/lib",
    },
    PlatformLifecycleRequirement {
        file: "packages/flare-core-harmony-arkts-sdk/src/main/ets/lifecycle/PlatformLifecycle.ets",
        tokens: &[
            "lifecycle.setAppState",
            "AppState.Foreground",
            "AppState.Background",
            "notifyNetworkChange",
            "fingerprintHint",
            "netAvailable",
            "applicationStateChange",
        ],
        sources: "packages/flare-core-harmony-arkts-sdk/src/main/ets",
    },
    PlatformLifecycleRequirement {
        file: "packages/flare-core-harmony-cangjie-sdk/src/lifecycle/PlatformLifecycle.cj",
        tokens: &[
            "lifecycle.setAppState",
            "AppState.Foreground",
            "AppState.Background",
            "notifyNetworkChange",
            "fingerprintHint",
        ],
        sources: "packages/flare-core-harmony-cangjie-sdk/src",
    },
];

fn scan_platform_lifecycle_wiring(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    for requirement in PLATFORM_LIFECYCLE_REQUIREMENTS {
        let rel = requirement.file;
        let path = root.join(rel);
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fail(errors, format!("missing PlatformLifecycle: {rel}"));
                continue;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", path.display()));
            }
        };
        for token in requirement.tokens {
            if !contents.contains(token) {
                fail(
                    errors,
                    format!("PlatformLifecycle missing `{token}` in {rel}"),
                );
            }
        }
        // Installed at init: some other source of the package builds a PlatformLifecycle behind
        // the `installPlatformLifecycle` option.
        let sources = root.join(requirement.sources);
        let mut installed = false;
        if sources.is_dir() {
            for file in files_under(&sources)? {
                if file == path
                    || file
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.contains(".test.") || name.ends_with("_test.dart"))
                {
                    continue;
                }
                let Ok(text) = fs::read_to_string(&file) else {
                    continue;
                };
                if text.contains("installPlatformLifecycle") && text.contains("PlatformLifecycle(")
                {
                    installed = true;
                    break;
                }
            }
        }
        if !installed {
            fail(
                errors,
                format!(
                    "PlatformLifecycle is not installed at init behind `installPlatformLifecycle` in {}",
                    requirement.sources
                ),
            );
        }
    }
    Ok(())
}

fn scan_l2_adapter_boundaries(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let roots = [
        "packages/flare-core-typescript-sdk/src/adapter",
        "packages/flare-core-typescript-sdk/src/bridge",
        "packages/flare-core-typescript-sdk/src/adapters",
        "packages/flare-core-typescript-sdk/src/lifecycle",
        "packages/flare-core-flutter-sdk/lib/src/adapter",
        "packages/flare-core-flutter-sdk/lib/src/bridge",
        "packages/flare-core-flutter-sdk/lib/src/lifecycle",
        "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter",
        "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/bridge",
        "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/lifecycle",
        "packages/flare-core-android-lifecycle/src/main/kotlin/com/flare/im/lifecycle",
        "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/FlareCoreSdk.kt",
        "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter",
        "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge",
        "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Lifecycle",
        "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter",
        "packages/flare-core-harmony-arkts-sdk/src/main/ets/bridge",
        "packages/flare-core-harmony-arkts-sdk/src/main/ets/lifecycle",
        "packages/flare-core-harmony-cangjie-sdk/src/adapter",
        "packages/flare-core-harmony-cangjie-sdk/src/bridge",
        "packages/flare-core-harmony-cangjie-sdk/src/lifecycle",
    ];

    for rel_root in roots {
        for file in files_under(&root.join(rel_root))? {
            if !is_l2_source_file(&file) || should_skip_l2_boundary_file(&file) {
                continue;
            }
            if l2_source_has_generated_marker(&file)?
                || is_allowed_l2_platform_capability(root, &file)
            {
                continue;
            }
            fail(
                errors,
                format!(
                    "L2 adapter source must be generated or a declared platform capability: {}",
                    file.strip_prefix(root).unwrap_or(&file).display()
                ),
            );
        }
    }

    Ok(())
}

fn is_l2_source_file(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| matches!(extension, "ts" | "kt" | "swift" | "dart" | "ets" | "cj"))
}

fn should_skip_l2_boundary_file(path: &Path) -> bool {
    let text = path.to_string_lossy();
    text.contains("/node_modules/")
        || text.contains("/.build/")
        || text.contains("/.dart_tool/")
        || text.contains("/build/")
        || path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| {
                name.ends_with(".test.ts")
                    || name.ends_with("_test.dart")
                    || name.ends_with("Test.kt")
                    || name.ends_with("Tests.swift")
            })
}

fn l2_source_has_generated_marker(path: &Path) -> Result<bool> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(contents.contains("GENERATED. Do not edit by hand.")
        || contents.contains("RUST-OWNED WIRE BOUNDARY"))
}

fn is_allowed_l2_platform_capability(root: &Path, path: &Path) -> bool {
    let rel = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    let rel = rel.to_ascii_lowercase();
    rel.contains("/bridge/")
        || rel.contains("/lifecycle/")
        || rel.starts_with("packages/flare-core-typescript-sdk/src/adapters/web/")
        || rel.starts_with("packages/flare-core-typescript-sdk/src/adapters/react-native/")
        || rel.starts_with("packages/flare-core-typescript-sdk/src/adapters/uni-app/")
        || rel.starts_with("packages/flare-core-typescript-sdk/src/adapters/tauri/")
        || rel == "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/flarecoresdk.kt"
        || rel
            == "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/codec/nativeinvoke.kt"
        || rel == "packages/flare-core-flutter-sdk/lib/src/adapter/default_flare_im_client.dart"
        || rel == "packages/flare-core-flutter-sdk/lib/src/adapter/events/default_events_api.dart"
        // 以下三条是 2026-08-20 补的。它们**不是** codegen 漏生成，而是本来就该手写的
        // 平台能力，只是白名单成文早于它们：
        //
        // - `_shared/transportProfile.ts`：各 runtime 物理上能提供什么传输与存储的
        //   唯一权威声明（能力包络）。它是 web / react-native / uni-app / tauri 这几个
        //   已在名单里的目录的同级兄弟，只是放在 runtime 中立的 `_shared/` 下。
        // - 两端的 `DefaultUserApi`：身份缓存门面（业务把名字/头像喂进来，读取侧批量
        //   join 出当前身份）。全仓没有它的模板——对照 `DefaultEventsApi` 是有模板的，
        //   而 flutter 的 `default_events_api.dart` 同样以手写身份列在上面。
        || rel.starts_with("packages/flare-core-typescript-sdk/src/adapters/_shared/")
        || rel
            == "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/defaultuserapi.kt"
        || rel
            == "packages/flare-core-apple-sdk/sources/flarecoreapplesdk/adapter/module/defaultuserapi.swift"
}

fn should_skip_scan_file(path: &Path) -> bool {
    let text = path.to_string_lossy();
    text.contains("/node_modules/")
        || text.contains("/.build/")
        || text.contains("/.dart_tool/")
        || text.contains("/build/")
        || path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.ends_with(".test.ts"))
}

fn verify_required_paths(root: &Path, errors: &mut Vec<String>) {
    for path in REQUIRED_PATHS {
        if !root.join(path).exists() {
            fail(errors, format!("missing required path: {path}"));
        }
    }
}

fn verify_retired_paths_absent(root: &Path, errors: &mut Vec<String>) {
    for path in RETIRED_PATHS {
        if root.join(path).exists() {
            fail(
                errors,
                format!("retired SDK generated path still exists: {path}"),
            );
        }
    }
}

fn scan_retired_package_refs(root: &Path, errors: &mut Vec<String>) -> Result<()> {
    let android_re = Regex::new(r#"com\.flare\.im\.generated|com\.flare\.im\.runtime"#)?;
    for file in files_under(&root.join("packages/flare-core-android-sdk/src/main/kotlin"))? {
        if file_contains(&file, &android_re)? {
            fail(errors, "retired Android package reference found");
            break;
        }
    }
    let non_android_re =
        Regex::new(r#"flare_core_harmony_cangjie_sdk\.generated|\.\./generated|Generated/"#)?;
    for rel in [
        "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK",
        "packages/flare-core-harmony-arkts-sdk/src/main/ets",
        "packages/flare-core-harmony-cangjie-sdk/src",
    ] {
        for file in files_under(&root.join(rel))? {
            if file_contains(&file, &non_android_re)? {
                fail(errors, "retired non-Android generated reference found");
                return Ok(());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn platform_lifecycle_scan_reports_missing_tokens_and_installation() {
        let root = env::temp_dir().join(format!(
            "flare-xtask-platform-lifecycle-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        for requirement in PLATFORM_LIFECYCLE_REQUIREMENTS {
            let path = root.join(requirement.file);
            fs::create_dir_all(
                path.parent()
                    .expect("fixture should have a parent directory"),
            )
            .expect("failed to create fixture directory");
            fs::write(&path, requirement.tokens.join(" ")).expect("failed to write fixture");
            let sources = root.join(requirement.sources);
            fs::create_dir_all(&sources).expect("failed to create fixture directory");
            fs::write(
                sources.join("client.src"),
                "installPlatformLifecycle PlatformLifecycle(",
            )
            .expect("failed to write fixture");
        }
        // The TypeScript one drops a token and loses its installation.
        let ts = &PLATFORM_LIFECYCLE_REQUIREMENTS[0];
        fs::write(
            root.join(ts.file),
            "lifecycle.setAppState AppState.Foreground",
        )
        .expect("failed to write fixture");
        fs::remove_file(root.join(ts.sources).join("client.src"))
            .expect("failed to remove fixture");

        let mut errors: Vec<String> = Vec::new();
        let result = scan_platform_lifecycle_wiring(&root, &mut errors);
        let _ = fs::remove_dir_all(&root);

        result.expect("scan should read fixtures");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("visibilitychange")),
            "expected missing visibilitychange token, got {errors:?}"
        );
        assert!(
            errors.iter().any(
                |error| error.contains("not installed at init") && error.contains("typescript")
            ),
            "expected missing installation, got {errors:?}"
        );
        assert!(
            errors.iter().all(|error| error.contains("typescript")),
            "only the TypeScript package should fail, got {errors:?}"
        );
    }

    #[test]
    fn wire_boundary_scan_rejects_apple_event_type_dual_read() {
        let root = env::temp_dir().join(format!(
            "flare-xtask-wire-boundary-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let rel =
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Bridge/FfiBridgeSupport.swift";
        let path = root.join(rel);
        fs::create_dir_all(path.parent().expect("fixture should have parent"))
            .expect("failed to create fixture directory");
        fs::write(
            &path,
            r#"// RUST-OWNED WIRE BOUNDARY
let eventType = FfiNativeCallbacks.eventType(from: event["event_type"] ?? event["eventType"])
"#,
        )
        .expect("failed to write fixture");

        let mut errors: Vec<String> = Vec::new();
        let result = scan_forbidden_wire_codec_normalization(&root, &mut errors);
        let _ = fs::remove_dir_all(&root);

        result.expect("scan should read fixtures");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("legacy wire normalization")),
            "expected event_type dual read to be rejected, got {errors:?}"
        );
    }

    #[test]
    fn host_json_boundary_scan_rejects_indexeddb_user_id_payload() {
        let root = env::temp_dir().join(format!(
            "flare-xtask-host-json-boundary-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let rel = "storage/indexeddb/src/host.rs";
        let path = root.join(rel);
        fs::create_dir_all(path.parent().expect("fixture should have parent"))
            .expect("failed to create fixture directory");
        fs::write(
            &path,
            r#"#[derive(Serialize)]
struct PersistMessageArgs<'a> {
    user_id: &'a str,
}

let payload = serde_json::json!({ "user_id": user_id });
"#,
        )
        .expect("failed to write fixture");

        let mut errors: Vec<String> = Vec::new();
        let result = scan_forbidden_host_json_boundary_fields(&root, &mut errors);
        let _ = fs::remove_dir_all(&root);

        result.expect("scan should read fixtures");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("legacy host JSON field")),
            "expected IndexedDB host user_id payload to be rejected, got {errors:?}"
        );
    }

    #[test]
    fn event_contract_scan_rejects_snake_case_json_fields() {
        let root = env::temp_dir().join(format!(
            "flare-xtask-event-contract-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        let rel = "bindings/contract/events.json";
        let path = root.join(rel);
        fs::create_dir_all(path.parent().expect("fixture should have parent"))
            .expect("failed to create fixture directory");
        fs::write(
            &path,
            r#"{
  "cAbi": {
    "batchJson": {
      "events": [{ "event_type": "i32", "payload": "object" }]
    }
  },
  "events": [
    {
      "id": "message.send_failed",
      "cJson": { "fields": ["client_msg_id", "reason"] }
    }
  ]
}"#,
        )
        .expect("failed to write fixture");

        let mut errors: Vec<String> = Vec::new();
        let result = scan_forbidden_event_contract_json_fields(&root, &mut errors);
        let _ = fs::remove_dir_all(&root);

        result.expect("scan should read fixtures");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("legacy event contract JSON field")),
            "expected event contract snake_case field to be rejected, got {errors:?}"
        );
    }
}
