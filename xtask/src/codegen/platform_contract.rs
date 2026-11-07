use anyhow::{Result, bail};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use super::naming::cangjie_field_name;
use crate::{
    GeneratedTextTarget, all_spec_enums, all_spec_models, arr, child_arr, core_root,
    is_known_ts_model_type, is_list_type_name, json_quote, kotlin_model_package_imports,
    list_inner_type_name, listener_interface_name, listener_payloads, load_expanded_client_spec,
    load_json, lower_first, model_package_suffix, pascal_case, remove_output_paths,
    screaming_snake, single_trailing_newline, snake_case, spec_enum_names, spec_model_names,
    str_field, swift_identifier, typescript_listener_groups, upsert_text_file,
};

pub(crate) fn emit_platform_contract_files(root: &Path, check: bool) -> Result<()> {
    let spec = load_expanded_client_spec(root)?;
    let event_codes = load_event_codes(root)?;
    let mut drifted = Vec::new();
    if !check {
        clean_platform_contract_outputs(root)?;
    }
    for target in platform_contract_targets(root, &spec, &event_codes) {
        let body = single_trailing_newline(&target.body);
        upsert_text_file(&target.path, &body, check, &mut drifted)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned platform contract output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned platform model/listener/callback artifacts generated");
    }
    Ok(())
}

fn clean_platform_contract_outputs(root: &Path) -> Result<()> {
    remove_output_paths([
        root.join("packages/flare-core-flutter-sdk/lib/src/model"),
        root.join("packages/flare-core-flutter-sdk/lib/src/listener"),
        root.join("packages/flare-core-flutter-sdk/lib/src/callback"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/listener"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/callback"),
        root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Model"),
        root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Listener"),
        root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Callback"),
        root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/model"),
        root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/listener"),
        root.join("packages/flare-core-harmony-cangjie-sdk/src/model"),
        root.join("packages/flare-core-harmony-cangjie-sdk/src/listener"),
        // Retired: each platform's hand-written PlatformLifecycle replaces the generated bridge.
        root.join("packages/flare-core-flutter-sdk/lib/src/lifecycle/heartbeat_lifecycle_bridge.dart"),
        root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/lifecycle/HeartbeatLifecycleBridge.kt",
        ),
        root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Lifecycle/HeartbeatLifecycleBridge.swift",
        ),
        root.join(
            "packages/flare-core-harmony-arkts-sdk/src/main/ets/lifecycle/HeartbeatLifecycleBridge.ets",
        ),
        root.join("packages/flare-core-harmony-cangjie-sdk/src/lifecycle/HeartbeatLifecycleBridge.cj"),
    ])
}

fn platform_contract_targets(
    root: &Path,
    spec: &Value,
    event_codes: &[EventCodeEntry],
) -> Vec<GeneratedTextTarget> {
    let mut targets = Vec::new();
    targets.extend(native_bridge_contract_targets(root, spec));
    targets.extend(dart_contract_targets(root, spec, event_codes));
    targets.extend(kotlin_contract_targets(root, spec, event_codes));
    targets.extend(swift_contract_targets(root, spec, event_codes));
    targets.extend(arkts_contract_targets(root, spec));
    targets.extend(cangjie_contract_targets(root, spec));
    targets
}

#[derive(Clone, Debug)]
struct NativeDescriptorEntry {
    module: String,
    method: String,
    operation: String,
    transport: String,
    c_api: String,
    dispatch_op: String,
    request_encoding: String,
    response_encoding: String,
    return_mode: String,
    callback: String,
    handle_policy: String,
    summary: String,
}

fn native_bridge_contract_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let entries = native_descriptor_entries(spec);
    vec![
        GeneratedTextTarget {
            path: root.join("packages/flare-core-flutter-sdk/lib/src/contract/bridge_contract.dart"),
            body: emit_dart_bridge_contract(&entries),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/NativeCallDescriptor.kt",
            ),
            body: emit_kotlin_native_call_descriptor(),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/NativeBridge.kt",
            ),
            body: emit_kotlin_native_bridge(),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/contract/NativeCallMap.kt",
            ),
            body: emit_kotlin_native_call_map(&entries),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Contract/BridgeContract.swift",
            ),
            body: emit_swift_bridge_contract(&entries),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/contract/BridgeContract.ets"),
            body: emit_arkts_bridge_contract(&entries),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-harmony-cangjie-sdk/src/contract/BridgeContract.cj"),
            body: emit_cangjie_bridge_contract(&entries),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-typescript-sdk/src/contract/bridge_contract.ts"),
            body: emit_typescript_bridge_contract(&entries),
        },
    ]
}

fn native_descriptor_entries(spec: &Value) -> Vec<NativeDescriptorEntry> {
    let mut entries = Vec::new();
    for module in child_arr(spec, "modules") {
        let module_key = str_field(module, "key");
        for method in child_arr(module, "methods") {
            let operation = str_field(method, "operation");
            let binding = method.get("nativeBinding").unwrap_or(&Value::Null);
            if operation.is_empty() {
                continue;
            }
            let dispatch_op = str_field(method, "dispatchOp").to_string();
            let c_api = str_field(method, "cApi").to_string();
            let transport = str_field(method, "transport").to_string();
            entries.push(NativeDescriptorEntry {
                module: module_key.to_string(),
                method: str_field(method, "name").to_string(),
                operation: operation.to_string(),
                transport,
                c_api,
                dispatch_op: dispatch_op.clone(),
                request_encoding: str_field(binding, "requestEncoding").to_string(),
                response_encoding: str_field(binding, "responseEncoding").to_string(),
                return_mode: str_field(binding, "returnMode").to_string(),
                callback: str_field(binding, "callback").to_string(),
                handle_policy: str_field(binding, "handlePolicy").to_string(),
                summary: descriptor_summary(
                    str_field(method, "name"),
                    str_field(method, "cApi"),
                    str_field(method, "transport"),
                    operation,
                    &dispatch_op,
                ),
            });
        }
    }
    entries.push(NativeDescriptorEntry {
        module: "message_builder".to_string(),
        method: "dispatchTypedBuild".to_string(),
        operation: "message_builder.dispatch".to_string(),
        transport: "dispatch-json".to_string(),
        c_api: "flare_message_build_json".to_string(),
        dispatch_op: String::new(),
        request_encoding: "json".to_string(),
        response_encoding: "json-object".to_string(),
        return_mode: "callback".to_string(),
        callback: "FlareResultCallback".to_string(),
        handle_policy: "client-handle".to_string(),
        summary: "Internal typed message builder dispatch over `flare_message_build_json`."
            .to_string(),
    });
    entries
}

fn descriptor_summary(
    method: &str,
    c_api: &str,
    transport: &str,
    operation: &str,
    dispatch_op: &str,
) -> String {
    if dispatch_op.is_empty() {
        format!("{method} maps to `{c_api}` via `{transport}`. Operation: `{operation}`.")
    } else {
        format!(
            "{method} maps to `{c_api}` via `{transport}`, dispatch op `{dispatch_op}`. Operation: `{operation}`."
        )
    }
}

fn native_call_map_name(operation: &str) -> String {
    lower_first(&pascal_case(&operation.replace('.', "_")))
}

fn kotlin_native_call_map_name_from_operation(operation: &str) -> String {
    screaming_snake(&operation.replace('.', "_"))
}

fn quoted(value: &str) -> String {
    json_quote(value)
}

fn nullable_ts(value: &str) -> String {
    if value.is_empty() {
        "null".to_string()
    } else {
        quoted(value)
    }
}

fn nullable_swift(value: &str) -> String {
    if value.is_empty() {
        "nil".to_string()
    } else {
        quoted(value)
    }
}

fn emit_typescript_bridge_contract(entries: &[NativeDescriptorEntry]) -> String {
    let mut lines = vec![
        "/**".to_string(),
        " * GENERATED. Do not edit by hand.".to_string(),
        " *".to_string(),
        " * Native bridge descriptors map SDK operations to the real C ABI.".to_string(),
        " */".to_string(),
        "/** Describes how one SDK operation reaches the native layer. */".to_string(),
        "export interface NativeCallDescriptor {".to_string(),
        "  module: string;".to_string(),
        "  method: string;".to_string(),
        "  operation: string;".to_string(),
        "  transport: string;".to_string(),
        "  cApi: string;".to_string(),
        "  dispatchOp?: string;".to_string(),
        "  requestEncoding: string;".to_string(),
        "  responseEncoding: string;".to_string(),
        "  returnMode: string;".to_string(),
        "  callback?: string | null;".to_string(),
        "  handlePolicy: string;".to_string(),
        "}".to_string(),
        String::new(),
        "/** Platform runtimes implement this bridge using FFI, JNI, N-API, WASM or host IPC. */"
            .to_string(),
        "export interface NativeBridge {".to_string(),
        "  invoke<T>(descriptor: NativeCallDescriptor, request?: unknown): Promise<T>;".to_string(),
        "  /**".to_string(),
        "   * Installs (or, with null, removes) the host's connect token refresher. Runtimes without".to_string(),
        "   * a refresh channel leave it out and the client reports capabilityUnavailable.".to_string(),
        "   */".to_string(),
        "  setConnectTokenRefresher?(refresher: ConnectTokenRefresher | null): Promise<void>;".to_string(),
        "}".to_string(),
        String::new(),
        "/**".to_string(),
        " * Host-managed connect token refresh: called once after the gateway rejects a connection on".to_string(),
        " * authentication. Resolve the new token, or null when none can be obtained; the core waits at".to_string(),
        " * most 20 seconds and then treats it as null.".to_string(),
        " */".to_string(),
        "export type ConnectTokenRefresher = () => Promise<string | null>;".to_string(),
        String::new(),
        "/** Complete generated operation-to-native call map. */".to_string(),
        "export const NativeCallMap = {".to_string(),
    ];
    for entry in entries {
        let mut fields = vec![
            format!("module: {}", quoted(&entry.module)),
            format!("method: {}", quoted(&entry.method)),
            format!("operation: {}", quoted(&entry.operation)),
            format!("transport: {}", quoted(&entry.transport)),
            format!("cApi: {}", quoted(&entry.c_api)),
        ];
        if !entry.dispatch_op.is_empty() {
            fields.push(format!("dispatchOp: {}", quoted(&entry.dispatch_op)));
        }
        fields.extend([
            format!("requestEncoding: {}", quoted(&entry.request_encoding)),
            format!("responseEncoding: {}", quoted(&entry.response_encoding)),
            format!("returnMode: {}", quoted(&entry.return_mode)),
            format!("callback: {}", nullable_ts(&entry.callback)),
            format!("handlePolicy: {}", quoted(&entry.handle_policy)),
        ]);
        lines.push(format!("  /** {} */", entry.summary));
        lines.push(format!(
            "  {}: {{ {} }},",
            native_call_map_name(&entry.operation),
            fields.join(", ")
        ));
    }
    lines.push("} as const satisfies Record<string, NativeCallDescriptor>;".to_string());
    lines.join("\n")
}

fn emit_dart_bridge_contract(entries: &[NativeDescriptorEntry]) -> String {
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "// Native bridge descriptors generated from sdk-spec modules.".to_string(),
        String::new(),
        "/// Describes how one SDK operation reaches the native layer.".to_string(),
        "final class NativeCallDescriptor {".to_string(),
        "  const NativeCallDescriptor({".to_string(),
        "    required this.module,".to_string(),
        "    required this.method,".to_string(),
        "    required this.operation,".to_string(),
        "    required this.transport,".to_string(),
        "    required this.cApi,".to_string(),
        "    required this.requestEncoding,".to_string(),
        "    required this.responseEncoding,".to_string(),
        "    required this.returnMode,".to_string(),
        "    required this.handlePolicy,".to_string(),
        "    this.dispatchOp,".to_string(),
        "    this.callback,".to_string(),
        "  });".to_string(),
        String::new(),
        "  final String module;".to_string(),
        "  final String method;".to_string(),
        "  final String operation;".to_string(),
        "  final String transport;".to_string(),
        "  final String cApi;".to_string(),
        "  final String requestEncoding;".to_string(),
        "  final String responseEncoding;".to_string(),
        "  final String returnMode;".to_string(),
        "  final String handlePolicy;".to_string(),
        "  final String? dispatchOp;".to_string(),
        "  final String? callback;".to_string(),
        "}".to_string(),
        String::new(),
        "/// Platform runtimes implement this bridge using FFI, JNI, N-API, WASM or host IPC."
            .to_string(),
        "/// Host-managed connect token refresh: resolves the new connect token, or null when none".to_string(),
        "/// can be obtained. Called on a background isolate task; the core waits at most 20 seconds.".to_string(),
        "typedef ConnectTokenRefresher = Future<String?> Function();".to_string(),
        String::new(),
        "abstract interface class NativeBridge {".to_string(),
        "  Future<T> invoke<T>(NativeCallDescriptor descriptor, [Object? request]);".to_string(),
        "}".to_string(),
        String::new(),
        "/// Implemented by bridges that can call back for a connect token (the FFI bridge); the client".to_string(),
        "/// reports capabilityUnavailable for a bridge without it.".to_string(),
        "abstract interface class ConnectTokenRefresherBridge {".to_string(),
        "  /// Installs (or, with null, removes) the connect token refresher: registers the native".to_string(),
        "  /// trampoline with flare_sdk_set_connect_token_refresher and answers every request with".to_string(),
        "  /// flare_sdk_complete_connect_token_refresh.".to_string(),
        "  Future<void> setConnectTokenRefresher(ConnectTokenRefresher? refresher);".to_string(),
        "}".to_string(),
        String::new(),
        "abstract final class NativeCallMap {".to_string(),
        "  const NativeCallMap._();".to_string(),
        String::new(),
    ];
    for entry in entries {
        lines.push(format!("  /// {}", entry.summary));
        lines.push(format!(
            "  static const {} = NativeCallDescriptor(",
            native_call_map_name(&entry.operation)
        ));
        lines.extend([
            format!("      module: {},", quoted(&entry.module)),
            format!("      method: {},", quoted(&entry.method)),
            format!("      operation: {},", quoted(&entry.operation)),
            format!("      transport: {},", quoted(&entry.transport)),
            format!("      cApi: {},", quoted(&entry.c_api)),
            format!(
                "      requestEncoding: {},",
                quoted(&entry.request_encoding)
            ),
            format!(
                "      responseEncoding: {},",
                quoted(&entry.response_encoding)
            ),
            format!("      returnMode: {},", quoted(&entry.return_mode)),
            format!("      handlePolicy: {},", quoted(&entry.handle_policy)),
        ]);
        if !entry.dispatch_op.is_empty() {
            lines.push(format!("      dispatchOp: {},", quoted(&entry.dispatch_op)));
        }
        if !entry.callback.is_empty() {
            lines.push(format!("      callback: {},", quoted(&entry.callback)));
        }
        let last = lines.pop().unwrap_or_default();
        lines.push(format!("{});", last.trim_end_matches(',')));
        lines.push(String::new());
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_kotlin_native_bridge() -> String {
    [
        "package com.flare.im.contract",
        "",
        "import com.flare.im.bridge.FlareSdkException",
        "",
        "/** GENERATED. Do not edit by hand. */",
        "/**",
        " * Host-managed connect token refresh: returns the new connect token, or null when none can be",
        " * obtained. The core waits at most 20 seconds.",
        " */",
        "typealias ConnectTokenRefresher = suspend () -> String?",
        "",
        "/** Platform runtimes implement this bridge using FFI, JNI, N-API, WASM or host IPC. */",
        "interface NativeBridge {",
        "    suspend fun <T> invoke(descriptor: NativeCallDescriptor, request: Any? = null): T",
        "",
        "    /**",
        "     * Installs (or, with null, removes) the connect token refresher: registers the native",
        "     * trampoline with flare_sdk_set_connect_token_refresher and answers every request with",
        "     * flare_sdk_complete_connect_token_refresh.",
        "     */",
        "    suspend fun setConnectTokenRefresher(refresher: ConnectTokenRefresher?) {",
        "        // Bridges without a refresh channel (test doubles) report capabilityUnavailable.",
        "        throw FlareSdkException(",
        "            code = SdkErrorCodes.CAPABILITYUNAVAILABLE,",
        "            message = \"This bridge cannot call back for a connect token.\",",
        "            operation = SdkOperations.SDK_SET_CONNECT_TOKEN_REFRESHER,",
        "        )",
        "    }",
        "}",
    ]
    .join("\n")
}

fn emit_kotlin_native_call_descriptor() -> String {
    [
        "package com.flare.im.contract",
        "",
        "/** GENERATED. Do not edit by hand. */",
        "/** Describes how one SDK operation reaches the native layer. */",
        "data class NativeCallDescriptor(",
        "    val module: String,",
        "    val method: String,",
        "    val operation: String,",
        "    val transport: String,",
        "    val cApi: String,",
        "    val requestEncoding: String,",
        "    val responseEncoding: String,",
        "    val returnMode: String,",
        "    val handlePolicy: String,",
        "    val dispatchOp: String? = null,",
        "    val callback: String? = null,",
        ")",
    ]
    .join("\n")
}

fn emit_kotlin_native_call_map(entries: &[NativeDescriptorEntry]) -> String {
    let mut lines = vec![
        "package com.flare.im.contract".to_string(),
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "object NativeCallMap {".to_string(),
    ];
    for entry in entries {
        let mut args = vec![
            format!("module = {}", quoted(&entry.module)),
            format!("method = {}", quoted(&entry.method)),
            format!("operation = {}", quoted(&entry.operation)),
            format!("transport = {}", quoted(&entry.transport)),
            format!("cApi = {}", quoted(&entry.c_api)),
            format!("requestEncoding = {}", quoted(&entry.request_encoding)),
            format!("responseEncoding = {}", quoted(&entry.response_encoding)),
            format!("returnMode = {}", quoted(&entry.return_mode)),
            format!("handlePolicy = {}", quoted(&entry.handle_policy)),
        ];
        if !entry.dispatch_op.is_empty() {
            args.push(format!("dispatchOp = {}", quoted(&entry.dispatch_op)));
        }
        if !entry.callback.is_empty() {
            args.push(format!("callback = {}", quoted(&entry.callback)));
        }
        lines.push(format!("    /** {} */", entry.summary));
        lines.push(format!(
            "    val {} = NativeCallDescriptor({})",
            kotlin_native_call_map_name_from_operation(&entry.operation),
            args.join(", ")
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_bridge_contract(entries: &[NativeDescriptorEntry]) -> String {
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        "/// Describes how one SDK operation reaches the native layer.".to_string(),
        "public struct NativeCallDescriptor: Sendable {".to_string(),
        "    public let module: String".to_string(),
        "    public let method: String".to_string(),
        "    public let operation: String".to_string(),
        "    public let transport: String".to_string(),
        "    public let cApi: String".to_string(),
        "    public let dispatchOp: String?".to_string(),
        "    public let requestEncoding: String".to_string(),
        "    public let responseEncoding: String".to_string(),
        "    public let returnMode: String".to_string(),
        "    public let callback: String?".to_string(),
        "    public let handlePolicy: String".to_string(),
        "}".to_string(),
        String::new(),
        "/// Host-managed connect token refresh: returns the new connect token, or nil when none can be".to_string(),
        "/// obtained. The core waits at most 20 seconds.".to_string(),
        "public typealias ConnectTokenRefresher = @Sendable () async -> String?".to_string(),
        String::new(),
        "/// Platform runtimes implement this bridge using FFI, JNI, N-API, WASM or host IPC.".to_string(),
        "public protocol NativeBridgeProtocol: AnyObject {".to_string(),
        "    func invoke(_ descriptor: NativeCallDescriptor, request: AnySendable?) async throws -> AnySendable".to_string(),
        "    /// Installs (or, with nil, removes) the connect token refresher: registers the C trampoline".to_string(),
        "    /// with flare_sdk_set_connect_token_refresher and answers every request with".to_string(),
        "    /// flare_sdk_complete_connect_token_refresh.".to_string(),
        "    func setConnectTokenRefresher(_ refresher: ConnectTokenRefresher?) async throws".to_string(),
        "}".to_string(),
        String::new(),
        "extension NativeBridgeProtocol {".to_string(),
        "    /// Bridges without a refresh channel (test doubles) report capabilityUnavailable.".to_string(),
        "    public func setConnectTokenRefresher(_ refresher: ConnectTokenRefresher?) async throws {".to_string(),
        "        throw FlareSdkException(".to_string(),
        "            code: SdkErrorCodes.capabilityUnavailable,".to_string(),
        "            message: \"This bridge cannot call back for a connect token.\",".to_string(),
        "            operation: SdkOperations.sdkSetConnectTokenRefresher".to_string(),
        "        )".to_string(),
        "    }".to_string(),
        "}".to_string(),
        String::new(),
        "public enum NativeCallMap {".to_string(),
    ];
    for entry in entries {
        lines.push(format!("    /// {}", entry.summary));
        lines.push(format!(
            "    public static let {} = NativeCallDescriptor(module: {}, method: {}, operation: {}, transport: {}, cApi: {}, dispatchOp: {}, requestEncoding: {}, responseEncoding: {}, returnMode: {}, callback: {}, handlePolicy: {})",
            swift_identifier(&native_call_map_name(&entry.operation)),
            quoted(&entry.module),
            quoted(&entry.method),
            quoted(&entry.operation),
            quoted(&entry.transport),
            quoted(&entry.c_api),
            nullable_swift(&entry.dispatch_op),
            quoted(&entry.request_encoding),
            quoted(&entry.response_encoding),
            quoted(&entry.return_mode),
            nullable_swift(&entry.callback),
            quoted(&entry.handle_policy)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_bridge_contract(entries: &[NativeDescriptorEntry]) -> String {
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "// Native bridge descriptors generated from sdk-spec modules.".to_string(),
        String::new(),
        "/** Describes how one SDK operation reaches the native layer. */".to_string(),
        "export interface NativeCallDescriptor {".to_string(),
        "  module: string;".to_string(),
        "  method: string;".to_string(),
        "  operation: string;".to_string(),
        "  transport: string;".to_string(),
        "  cApi: string;".to_string(),
        "  dispatchOp?: string;".to_string(),
        "  requestEncoding: string;".to_string(),
        "  responseEncoding: string;".to_string(),
        "  returnMode: string;".to_string(),
        "  callback?: string | null;".to_string(),
        "  handlePolicy: string;".to_string(),
        "}".to_string(),
        String::new(),
        "/** Platform runtimes implement this bridge using FFI, JNI, N-API, WASM or host IPC. */"
            .to_string(),
        "/** Host-managed connect token refresh: resolves the new connect token, or null when none can be obtained. */".to_string(),
        "export type ConnectTokenRefresher = () => Promise<string | null>;".to_string(),
        String::new(),
        "export interface NativeBridge {".to_string(),
        "  invoke<T>(descriptor: NativeCallDescriptor, request?: Object): Promise<T>;".to_string(),
        "  /**".to_string(),
        "   * Installs (or, with null, removes) the connect token refresher: registers the native".to_string(),
        "   * trampoline and answers every request with flare_sdk_complete_connect_token_refresh.".to_string(),
        "   */".to_string(),
        "  setConnectTokenRefresher?(refresher: ConnectTokenRefresher | null): Promise<void>;".to_string(),
        "}".to_string(),
        String::new(),
        "export const NativeCallMap = {".to_string(),
    ];
    for entry in entries {
        let mut fields = vec![
            format!("module: {}", quoted(&entry.module)),
            format!("method: {}", quoted(&entry.method)),
            format!("operation: {}", quoted(&entry.operation)),
            format!("transport: {}", quoted(&entry.transport)),
            format!("cApi: {}", quoted(&entry.c_api)),
        ];
        if !entry.dispatch_op.is_empty() {
            fields.push(format!("dispatchOp: {}", quoted(&entry.dispatch_op)));
        }
        fields.extend([
            format!("requestEncoding: {}", quoted(&entry.request_encoding)),
            format!("responseEncoding: {}", quoted(&entry.response_encoding)),
            format!("returnMode: {}", quoted(&entry.return_mode)),
            format!("callback: {}", nullable_ts(&entry.callback)),
            format!("handlePolicy: {}", quoted(&entry.handle_policy)),
        ]);
        lines.push(format!("  /** {} */", entry.summary));
        lines.push(format!(
            "  {}: {{ {} }},",
            native_call_map_name(&entry.operation),
            fields.join(", ")
        ));
    }
    lines.push("} as const;".to_string());
    lines.join("\n")
}

fn emit_cangjie_bridge_contract(entries: &[NativeDescriptorEntry]) -> String {
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.contract".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        "// Native bridge descriptors generated from sdk-spec modules.".to_string(),
        String::new(),
        "public class NativeCallDescriptor {".to_string(),
        "    public let module: String".to_string(),
        "    public let method: String".to_string(),
        "    public let operation: String".to_string(),
        "    public let transport: String".to_string(),
        "    public let cApi: String".to_string(),
        "    public let dispatchOp: String".to_string(),
        "    public let requestEncoding: String".to_string(),
        "    public let responseEncoding: String".to_string(),
        "    public let returnMode: String".to_string(),
        "    public let callback: String".to_string(),
        "    public let handlePolicy: String".to_string(),
        String::new(),
        "    public init(module!: String, method!: String, operation!: String, transport!: String, cApi!: String, dispatchOp!: String, requestEncoding!: String, responseEncoding!: String, returnMode!: String, callback!: String, handlePolicy!: String) {".to_string(),
        "        this.module = module".to_string(),
        "        this.method = method".to_string(),
        "        this.operation = operation".to_string(),
        "        this.transport = transport".to_string(),
        "        this.cApi = cApi".to_string(),
        "        this.dispatchOp = dispatchOp".to_string(),
        "        this.requestEncoding = requestEncoding".to_string(),
        "        this.responseEncoding = responseEncoding".to_string(),
        "        this.returnMode = returnMode".to_string(),
        "        this.callback = callback".to_string(),
        "        this.handlePolicy = handlePolicy".to_string(),
        "    }".to_string(),
        "}".to_string(),
        String::new(),
        "// Runtime implementations call the real C ABI here and return native result JSON.".to_string(),
        "// Host-managed connect token refresh: returns the new connect token, or None when none can be".to_string(),
        "// obtained. Called off the caller's thread; the core waits at most 20 seconds.".to_string(),
        "public type ConnectTokenRefresher = () -> ?String".to_string(),
        String::new(),
        "public interface NativeBridge {".to_string(),
        "    func invoke(descriptor!: NativeCallDescriptor, requestJson!: String): String".to_string(),
        "    // Installs (or, with None, removes) the connect token refresher: registers the native".to_string(),
        "    // trampoline and answers every request with flare_sdk_complete_connect_token_refresh.".to_string(),
        "    func setConnectTokenRefresher(refresher: ?ConnectTokenRefresher): Unit {".to_string(),
        "        // Bridges without a refresh channel (test doubles) report capabilityUnavailable.".to_string(),
        "        throw Exception(\"FlareSdkException(code=capabilityUnavailable, operation=sdk.set_connect_token_refresher, message=This bridge cannot call back for a connect token.)\")".to_string(),
        "    }".to_string(),
        "}".to_string(),
        String::new(),
        "public class NativeCallMap {".to_string(),
    ];
    for entry in entries {
        lines.push(format!("    // {}", entry.summary));
        lines.push(format!(
            "    public static let {}: NativeCallDescriptor = NativeCallDescriptor(module: {}, method: {}, operation: {}, transport: {}, cApi: {}, dispatchOp: {}, requestEncoding: {}, responseEncoding: {}, returnMode: {}, callback: {}, handlePolicy: {})",
            native_call_map_name(&entry.operation),
            quoted(&entry.module),
            quoted(&entry.method),
            quoted(&entry.operation),
            quoted(&entry.transport),
            quoted(&entry.c_api),
            quoted(&entry.dispatch_op),
            quoted(&entry.request_encoding),
            quoted(&entry.response_encoding),
            quoted(&entry.return_mode),
            quoted(&entry.callback),
            quoted(&entry.handle_policy)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

#[derive(Clone, Debug)]
struct EventCodeEntry {
    id: String,
    code: i64,
    c_code_name: String,
}

fn load_event_codes(root: &Path) -> Result<Vec<EventCodeEntry>> {
    let path = core_root(root).join("bindings/contract/events.json");
    let json = load_json(&path)?;
    let mut entries = arr(json.get("events").unwrap_or(&Value::Null))
        .iter()
        .filter_map(|event| {
            let id = str_field(event, "id");
            let c_code_name = str_field(event, "cCodeName");
            let code = event.get("cCode")?.as_i64()?;
            Some(EventCodeEntry {
                id: id.to_string(),
                code,
                c_code_name: c_code_name.to_string(),
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| a.code.cmp(&b.code).then_with(|| a.id.cmp(&b.id)));
    Ok(entries)
}

fn event_code_camel_name(id: &str) -> String {
    lower_first(&pascal_case(&id.replace('.', "_")))
}

fn event_code_const_name(entry: &EventCodeEntry) -> String {
    entry
        .c_code_name
        .strip_prefix("FLARE_EVENT_")
        .unwrap_or(&entry.c_code_name)
        .to_string()
}

#[derive(Clone, Debug)]
struct ContractModelEntry {
    name: String,
    suffix: String,
}

fn contract_model_entries(spec: &Value) -> Vec<ContractModelEntry> {
    let mut entries = Vec::new();
    for group in child_arr(spec, "modelGroups") {
        for enum_value in child_arr(group, "enums") {
            let name = str_field(enum_value, "name").to_string();
            entries.push(ContractModelEntry {
                suffix: model_package_suffix(&name).to_string(),
                name,
            });
        }
        for model in child_arr(group, "models") {
            let name = str_field(model, "name").to_string();
            entries.push(ContractModelEntry {
                suffix: model_package_suffix(&name).to_string(),
                name,
            });
        }
    }
    entries
}

fn contract_type_suffixes(spec: &Value) -> BTreeMap<String, String> {
    contract_model_entries(spec)
        .into_iter()
        .map(|entry| (entry.name, entry.suffix))
        .collect()
}

fn contract_model_dependency_names(model: &Value, spec: &Value) -> BTreeSet<String> {
    let known = spec_model_names(spec)
        .into_iter()
        .chain(spec_enum_names(spec))
        .collect::<BTreeSet<_>>();
    child_arr(model, "fields")
        .iter()
        .filter_map(|field| {
            let type_name = str_field(field, "type");
            let inner = list_inner_type_name(type_name);
            if known.contains(inner) && inner != str_field(model, "name") {
                Some(inner.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn contract_field_required(field: &Value) -> bool {
    field
        .get("required")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn suffix_path(suffix: &str) -> PathBuf {
    let mut path = PathBuf::new();
    for part in suffix.split('.').filter(|part| !part.is_empty()) {
        path.push(part);
    }
    path
}

fn relative_contract_path(from_suffix: &str, to_suffix: &str, file: &str) -> String {
    if from_suffix == to_suffix {
        return file.to_string();
    }
    let mut parts = Vec::<String>::new();
    for _ in from_suffix.split('.').filter(|part| !part.is_empty()) {
        parts.push("..".to_string());
    }
    for part in to_suffix.split('.').filter(|part| !part.is_empty()) {
        parts.push(part.to_string());
    }
    parts.push(file.to_string());
    parts.join("/")
}

fn model_file_stem(name: &str, snake: bool) -> String {
    if snake {
        snake_case(name)
    } else {
        name.to_string()
    }
}

fn dart_contract_targets(
    root: &Path,
    spec: &Value,
    event_codes: &[EventCodeEntry],
) -> Vec<GeneratedTextTarget> {
    let src_root = root.join("packages/flare-core-flutter-sdk/lib/src");
    let model_root = src_root.join("model");
    let listener_root = src_root.join("listener");
    let callback_root = src_root.join("callback");
    let mut targets = Vec::new();
    for enum_value in all_spec_enums(spec) {
        let name = str_field(enum_value, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{}.dart", snake_case(name))),
            body: emit_dart_model_enum(enum_value),
        });
    }
    for model in all_spec_models(spec) {
        let name = str_field(model, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{}.dart", snake_case(name))),
            body: emit_dart_model_class(model, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: model_root.join("catalog/message_build_catalog.dart"),
        body: emit_dart_message_build_catalog(spec),
    });
    targets.push(GeneratedTextTarget {
        path: model_root.join("event/event_code.dart"),
        body: emit_dart_event_code(event_codes),
    });
    targets.push(GeneratedTextTarget {
        path: model_root.join("model.dart"),
        body: emit_dart_model_index(spec, true),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("common.dart"),
        body: emit_dart_listener_common(),
    });
    for (kind, listeners) in typescript_listener_groups(spec) {
        targets.push(GeneratedTextTarget {
            path: listener_root.join(format!("{kind}.dart")),
            body: emit_dart_listener_group(&kind, &listeners),
        });
    }
    targets.push(GeneratedTextTarget {
        path: listener_root.join("listener.dart"),
        body: emit_dart_listener_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: callback_root.join("message_send_callback.dart"),
        body: emit_dart_message_send_callback(),
    });
    targets.push(GeneratedTextTarget {
        path: callback_root.join("callback.dart"),
        body: "// GENERATED. Do not edit by hand.\nexport 'message_send_callback.dart';"
            .to_string(),
    });
    targets
}

fn emit_dart_event_code(event_codes: &[EventCodeEntry]) -> String {
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "final class EventCode {".to_string(),
        "  const EventCode._();".to_string(),
    ];
    for entry in event_codes {
        lines.push(format!(
            "  static const int {} = {};",
            event_code_camel_name(&entry.id),
            entry.code
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn dart_contract_type(type_name: &str, spec: &Value) -> String {
    match type_name {
        "String" => "String".to_string(),
        "Boolean" => "bool".to_string(),
        "Int32" | "Int64" | "UInt32" | "UInt64" => "int".to_string(),
        "Float" | "Double" => "double".to_string(),
        "JsonObject" => "Map<String, Object?>".to_string(),
        "StringMap" => "Map<String, String>".to_string(),
        "BinaryMap" => "Map<String, List<int>>".to_string(),
        "StringList" => "List<String>".to_string(),
        _ if is_list_type_name(type_name) => {
            let inner = list_inner_type_name(type_name);
            format!("List<{}>", dart_contract_type(inner, spec))
        }
        _ if is_known_ts_model_type(type_name, spec) => type_name.to_string(),
        _ => "Map<String, Object?>".to_string(),
    }
}

fn dart_field_type(field: &Value, spec: &Value) -> String {
    let mut ty = dart_contract_type(str_field(field, "type"), spec);
    if !contract_field_required(field) {
        ty.push('?');
    }
    ty
}

fn dart_default_value(type_name: &str) -> Option<&'static str> {
    match type_name {
        "String" => Some("''"),
        "Boolean" => Some("false"),
        "Int32" | "Int64" | "UInt32" | "UInt64" => Some("0"),
        "Float" | "Double" => Some("0.0"),
        "JsonObject" | "StringMap" | "BinaryMap" => Some("const {}"),
        "StringList" => Some("const []"),
        _ if is_list_type_name(type_name) => Some("const []"),
        _ => None,
    }
}

fn dart_model_imports(model: &Value, spec: &Value) -> Vec<String> {
    let suffixes = contract_type_suffixes(spec);
    let current_suffix = model_package_suffix(str_field(model, "name"));
    contract_model_dependency_names(model, spec)
        .into_iter()
        .filter_map(|name| {
            let dep_suffix = suffixes.get(&name)?;
            let file = format!("{}.dart", snake_case(&name));
            let rel = relative_contract_path(current_suffix, dep_suffix, &file);
            Some(format!("import '{}';", rel))
        })
        .collect()
}

fn emit_dart_model_enum(enum_value: &Value) -> String {
    let values = child_arr(enum_value, "values")
        .iter()
        .filter_map(Value::as_str)
        .map(|value| lower_first(&pascal_case(value)))
        .collect::<Vec<_>>()
        .join(", ");
    [
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("/// {}", str_field(enum_value, "description")),
        format!("enum {} {{ {values} }}", str_field(enum_value, "name")),
    ]
    .join("\n")
}

fn emit_dart_model_class(model: &Value, spec: &Value) -> String {
    if str_field(model, "name") == "ViewSnapshot" {
        return [
            "// GENERATED. Do not edit by hand.",
            "",
            "/// Tagged snapshot emitted by core observable views.",
            "final class ViewSnapshot {",
            "  /// wire: `viewType`. Snapshot tag: timeline or conversationList.",
            "  final String viewType;",
            "  /// wire: `data`. Snapshot payload selected by viewType.",
            "  final Object? data;",
            "",
            "  const ViewSnapshot({",
            "    this.viewType = '',",
            "    this.data,",
            "  });",
            "}",
        ]
        .join("\n");
    }

    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    lines.extend(dart_model_imports(model, spec));
    lines.extend([
        String::new(),
        format!("/// {}", str_field(model, "description")),
        format!("final class {} {{", str_field(model, "name")),
    ]);
    for field in child_arr(model, "fields") {
        lines.push(format!(
            "  /// wire: `{}`. {}",
            str_field(field, "wireName"),
            str_field(field, "description")
        ));
        lines.push(format!(
            "  final {} {};",
            dart_field_type(field, spec),
            str_field(field, "name")
        ));
    }
    // Dart 不接受空的命名参数列表。
    if child_arr(model, "fields").is_empty() {
        lines.extend([
            format!("  const {}();", str_field(model, "name")),
            "}".to_string(),
        ]);
        return lines.join("\n");
    }
    lines.extend([
        String::new(),
        format!("  const {}({{", str_field(model, "name")),
    ]);
    for field in child_arr(model, "fields") {
        let name = str_field(field, "name");
        let type_name = str_field(field, "type");
        if !contract_field_required(field) {
            lines.push(format!("    this.{name},"));
        } else if let Some(default_value) = dart_default_value(type_name) {
            lines.push(format!("    this.{name} = {default_value},"));
        } else {
            lines.push(format!("    required this.{name},"));
        }
    }
    lines.extend(["  });".to_string(), "}".to_string()]);
    lines.join("\n")
}

fn emit_dart_message_build_catalog(spec: &Value) -> String {
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import 'message_build_catalog_entry.dart';".to_string(),
        "import 'message_build_op.dart';".to_string(),
        "import '../common/enums/message_content_type.dart';".to_string(),
        String::new(),
        "/// All supported quick-build operations for MessageBuilderApi.".to_string(),
        "const List<MessageBuildCatalogEntry> messageBuildCatalog = [".to_string(),
    ];
    for entry in child_arr(
        spec.get("messageBuildCatalog").unwrap_or(&Value::Null),
        "entries",
    ) {
        lines.push(format!(
            "  MessageBuildCatalogEntry(op: MessageBuildOp.{}, method: {}, requestType: {}, contentType: MessageContentType.{}, messageType: {}, summary: {}, stability: {}),",
            lower_first(&pascal_case(str_field(entry, "op"))),
            json_quote(str_field(entry, "method")),
            json_quote(str_field(entry, "request")),
            lower_first(&pascal_case(str_field(entry, "contentType"))),
            entry.get("messageType").and_then(Value::as_i64).unwrap_or_default(),
            json_quote(str_field(entry, "summary")),
            json_quote(entry.get("stability").and_then(Value::as_str).unwrap_or("stable")),
        ));
    }
    lines.push("];".to_string());
    lines.join("\n")
}

fn emit_dart_model_index(spec: &Value, include_event_code: bool) -> String {
    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    for entry in contract_model_entries(spec) {
        lines.push(format!(
            "export '{}{}.dart';",
            if entry.suffix.is_empty() {
                String::new()
            } else {
                format!("{}/", entry.suffix.replace('.', "/"))
            },
            model_file_stem(&entry.name, true)
        ));
    }
    lines.push("export 'catalog/message_build_catalog.dart';".to_string());
    if include_event_code {
        lines.push("export 'event/event_code.dart';".to_string());
    }
    lines.join("\n")
}

fn emit_dart_listener_common() -> String {
    [
        "// GENERATED. Do not edit by hand.",
        "/// Callback invoked for one typed SDK notification.",
        "typedef EventCallback<T> = void Function(T event);",
        "",
        "/// Disposable local listener registration returned by high-level `on*` APIs.",
        "abstract interface class EventSubscription {",
        "  Object get id;",
        "  void unsubscribe();",
        "}",
    ]
    .join("\n")
}

fn emit_dart_listener_group(kind: &str, listeners: &[&Value]) -> String {
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import '../model/model.dart';".to_string(),
        String::new(),
        format!("/// {} listener callbacks.", pascal_case(kind)),
        format!("abstract class {} {{", listener_interface_name(kind)),
        format!("  const {}();", listener_interface_name(kind)),
    ];
    for listener in listeners {
        lines.push(format!("  /// {}", str_field(listener, "description")));
        lines.push(format!(
            "  void {}({} event) {{}}",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_dart_listener_index(spec: &Value) -> String {
    let groups = typescript_listener_groups(spec);
    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    lines.push("export 'common.dart';".to_string());
    for kind in groups.keys() {
        lines.push(format!("export '{kind}.dart';"));
    }
    lines.push(String::new());
    for kind in groups.keys() {
        lines.push(format!("import '{kind}.dart';"));
    }
    lines.extend([
        String::new(),
        "/// Optional callback surface for apps that prefer one listener object.".to_string(),
        format!(
            "abstract class FlareImEventListener\n    implements {} {{",
            groups
                .keys()
                .map(|kind| listener_interface_name(kind))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "  const FlareImEventListener();".to_string(),
        "}".to_string(),
    ]);
    lines.join("\n")
}

fn emit_dart_message_send_callback() -> String {
    [
        "// GENERATED. Do not edit by hand.",
        "import '../model/model.dart';",
        "",
        "/// Direct callback for `messages.sendMessage(request, callback)` progress and terminal states.",
        "abstract class MessageSendCallback {",
        "  const MessageSendCallback();",
        "  /// Message upload or send progress changed.",
        "  void onProgress(ProgressEvent event) {}",
        "  /// Message send completed successfully.",
        "  void onSuccess(MessageSendAckEvent event) {}",
        "  /// Message send failed.",
        "  void onFailure(MessageSendFailedEvent event) {}",
        "}",
    ]
    .join("\n")
}

fn kotlin_contract_targets(
    root: &Path,
    spec: &Value,
    event_codes: &[EventCodeEntry],
) -> Vec<GeneratedTextTarget> {
    let src_root = root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im");
    let model_root = src_root.join("model");
    let listener_root = src_root.join("listener");
    let callback_root = src_root.join("callback");
    let mut targets = Vec::new();
    for enum_value in all_spec_enums(spec) {
        let name = str_field(enum_value, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{name}.kt")),
            body: emit_kotlin_model_enum(enum_value),
        });
    }
    for model in all_spec_models(spec) {
        let name = str_field(model, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{name}.kt")),
            body: emit_kotlin_model_class(model, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: model_root.join("catalog/MessageBuildCatalog.kt"),
        body: emit_kotlin_message_build_catalog(spec),
    });
    targets.push(GeneratedTextTarget {
        path: model_root.join("event/EventCode.kt"),
        body: emit_kotlin_event_code(event_codes),
    });
    for (kind, listeners) in typescript_listener_groups(spec) {
        targets.push(GeneratedTextTarget {
            path: listener_root.join(format!("{}Listener.kt", pascal_case(&kind))),
            body: emit_kotlin_listener_group(&kind, &listeners, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: listener_root.join("Common.kt"),
        body: emit_kotlin_listener_common(),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("FlareImEventListener.kt"),
        body: emit_kotlin_listener_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: callback_root.join("MessageSendCallback.kt"),
        body: emit_kotlin_message_send_callback(spec),
    });
    targets
}

fn emit_kotlin_event_code(event_codes: &[EventCodeEntry]) -> String {
    let mut lines = vec![
        "package com.flare.im.model.event".to_string(),
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "object EventCode {".to_string(),
    ];
    for entry in event_codes {
        lines.push(format!(
            "    const val {}: Int = {}",
            event_code_const_name(entry),
            entry.code
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn kotlin_contract_type(type_name: &str, spec: &Value) -> String {
    match type_name {
        "String" => "String".to_string(),
        "Boolean" => "Boolean".to_string(),
        "Int32" | "UInt32" => "Int".to_string(),
        "Int64" | "UInt64" => "Long".to_string(),
        "Float" | "Double" => "Double".to_string(),
        "JsonObject" => "Map<String, Any?>".to_string(),
        "StringMap" => "Map<String, String>".to_string(),
        "BinaryMap" => "Map<String, ByteArray>".to_string(),
        "StringList" => "List<String>".to_string(),
        _ if is_list_type_name(type_name) => {
            let inner = list_inner_type_name(type_name);
            format!("List<{}>", kotlin_contract_type(inner, spec))
        }
        _ if is_known_ts_model_type(type_name, spec) => type_name.to_string(),
        _ => "Map<String, Any?>".to_string(),
    }
}

fn kotlin_field_type(field: &Value, spec: &Value) -> String {
    let mut ty = kotlin_contract_type(str_field(field, "type"), spec);
    if !contract_field_required(field) {
        ty.push('?');
    }
    ty
}

fn kotlin_default_value(type_name: &str) -> Option<&'static str> {
    match type_name {
        "String" => Some("\"\""),
        "Boolean" => Some("false"),
        "Int32" | "UInt32" => Some("0"),
        "Int64" | "UInt64" => Some("0L"),
        "Float" | "Double" => Some("0.0"),
        "JsonObject" | "StringMap" | "BinaryMap" => Some("emptyMap()"),
        "StringList" => Some("emptyList()"),
        _ if is_list_type_name(type_name) => Some("emptyList()"),
        _ => None,
    }
}

fn kotlin_model_imports(model: &Value, spec: &Value) -> Vec<String> {
    let suffixes = contract_type_suffixes(spec);
    let current_suffix = model_package_suffix(str_field(model, "name"));
    contract_model_dependency_names(model, spec)
        .into_iter()
        .filter_map(|name| {
            let dep_suffix = suffixes.get(&name)?;
            if dep_suffix == current_suffix {
                None
            } else {
                Some(format!("import com.flare.im.model.{dep_suffix}.{name}"))
            }
        })
        .collect()
}

fn emit_kotlin_model_enum(enum_value: &Value) -> String {
    let name = str_field(enum_value, "name");
    let values = child_arr(enum_value, "values")
        .iter()
        .filter_map(Value::as_str)
        .map(screaming_snake)
        .collect::<Vec<_>>()
        .join(", ");
    [
        format!("package com.flare.im.model.{}", model_package_suffix(name)),
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        format!("/** {} */", str_field(enum_value, "description")),
        format!("enum class {name} {{ {values} }}"),
    ]
    .join("\n")
}

fn kotlin_property_name(name: &str) -> String {
    if matches!(
        name,
        "as" | "break"
            | "class"
            | "continue"
            | "do"
            | "else"
            | "false"
            | "for"
            | "fun"
            | "if"
            | "in"
            | "interface"
            | "is"
            | "null"
            | "object"
            | "package"
            | "return"
            | "super"
            | "this"
            | "throw"
            | "true"
            | "try"
            | "typealias"
            | "typeof"
            | "val"
            | "var"
            | "when"
            | "while"
    ) {
        format!("`{name}`")
    } else {
        name.to_string()
    }
}

fn emit_kotlin_model_class(model: &Value, spec: &Value) -> String {
    let name = str_field(model, "name");
    let mut lines = vec![
        format!("package com.flare.im.model.{}", model_package_suffix(name)),
        String::new(),
    ];
    lines.extend(kotlin_model_imports(model, spec));
    if lines.last().is_some_and(|line| !line.is_empty()) {
        lines.push(String::new());
    }
    lines.extend([
        "/** GENERATED. Do not edit by hand. */".to_string(),
        format!("/** {} */", str_field(model, "description")),
    ]);
    // data class 至少要一个参数。
    if child_arr(model, "fields").is_empty() {
        lines.push(format!("class {name}"));
        return lines.join("\n");
    }
    lines.push(format!("data class {name}("));
    for field in child_arr(model, "fields") {
        lines.push(format!(
            "    /** wire: `{}`. {} */",
            str_field(field, "wireName"),
            str_field(field, "description")
        ));
        let default = if !contract_field_required(field) {
            " = null".to_string()
        } else {
            kotlin_default_value(str_field(field, "type"))
                .map(|value| format!(" = {value}"))
                .unwrap_or_default()
        };
        lines.push(format!(
            "    val {}: {}{},",
            kotlin_property_name(str_field(field, "name")),
            kotlin_field_type(field, spec),
            default
        ));
    }
    lines.push(")".to_string());
    lines.join("\n")
}

fn emit_kotlin_message_build_catalog(spec: &Value) -> String {
    let mut lines = vec![
        "package com.flare.im.model.catalog".to_string(),
        String::new(),
        "import com.flare.im.model.common.enums.MessageContentType".to_string(),
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "/** All supported quick-build operations for MessageBuilderApi. */".to_string(),
        "val MESSAGE_BUILD_CATALOG: List<MessageBuildCatalogEntry> = listOf(".to_string(),
    ];
    for entry in child_arr(
        spec.get("messageBuildCatalog").unwrap_or(&Value::Null),
        "entries",
    ) {
        lines.push(format!(
            "    MessageBuildCatalogEntry(op = MessageBuildOp.{}, method = {}, requestType = {}, contentType = MessageContentType.{}, messageType = {}, summary = {}, stability = {}),",
            screaming_snake(str_field(entry, "op")),
            json_quote(str_field(entry, "method")),
            json_quote(str_field(entry, "request")),
            screaming_snake(str_field(entry, "contentType")),
            entry.get("messageType").and_then(Value::as_i64).unwrap_or_default(),
            json_quote(str_field(entry, "summary")),
            json_quote(entry.get("stability").and_then(Value::as_str).unwrap_or("stable")),
        ));
    }
    lines.push(")".to_string());
    lines.join("\n")
}

fn emit_kotlin_listener_common() -> String {
    [
        "package com.flare.im.listener",
        "",
        "/** GENERATED. Do not edit by hand. */",
        "typealias EventCallback<T> = (T) -> Unit",
        "",
        "/** Disposable local listener registration returned by high-level `on*` APIs. */",
        "interface EventSubscription {",
        "    val id: Any",
        "    fun unsubscribe()",
        "}",
    ]
    .join("\n")
}

fn emit_kotlin_listener_group(kind: &str, listeners: &[&Value], spec: &Value) -> String {
    let mut lines = vec!["package com.flare.im.listener".to_string(), String::new()];
    lines.extend(kotlin_model_package_imports(spec));
    lines.extend([
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        format!("/** {} listener callbacks. */", pascal_case(kind)),
        format!("interface {} {{", listener_interface_name(kind)),
    ]);
    for listener in listeners {
        lines.push(format!("    /** {} */", str_field(listener, "description")));
        lines.push(format!(
            "    fun {}(event: {}) {{}}",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_kotlin_listener_index(spec: &Value) -> String {
    let groups = typescript_listener_groups(spec);
    [
        "package com.flare.im.listener".to_string(),
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "/** Optional callback surface for apps that prefer one listener object. */".to_string(),
        format!(
            "interface FlareImEventListener : {} {{}}",
            groups
                .keys()
                .map(|kind| listener_interface_name(kind))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ]
    .join("\n")
}

fn emit_kotlin_message_send_callback(spec: &Value) -> String {
    let mut lines = vec!["package com.flare.im.callback".to_string(), String::new()];
    lines.extend(kotlin_model_package_imports(spec));
    lines.extend([
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "/** Direct callback for `messages.sendMessage(request, callback)` progress and terminal states. */".to_string(),
        "interface MessageSendCallback {".to_string(),
        "    /** Message upload or send progress changed. */".to_string(),
        "    fun onProgress(event: ProgressEvent) {}".to_string(),
        "    /** Message send completed successfully. */".to_string(),
        "    fun onSuccess(event: MessageSendAckEvent) {}".to_string(),
        "    /** Message send failed. */".to_string(),
        "    fun onFailure(event: MessageSendFailedEvent) {}".to_string(),
        "}".to_string(),
    ]);
    lines.join("\n")
}

fn swift_contract_targets(
    root: &Path,
    spec: &Value,
    event_codes: &[EventCodeEntry],
) -> Vec<GeneratedTextTarget> {
    let src_root = root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK");
    let model_root = src_root.join("Model");
    let listener_root = src_root.join("Listener");
    let callback_root = src_root.join("Callback");
    let mut targets = Vec::new();
    for enum_value in all_spec_enums(spec) {
        let name = str_field(enum_value, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{name}.swift")),
            body: emit_swift_model_enum(enum_value),
        });
    }
    for model in all_spec_models(spec) {
        let name = str_field(model, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{name}.swift")),
            body: emit_swift_model_struct(model, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: model_root.join("event/EventCode.swift"),
        body: emit_swift_event_code(event_codes),
    });
    for (kind, listeners) in typescript_listener_groups(spec) {
        targets.push(GeneratedTextTarget {
            path: listener_root.join(format!("{}Listener.swift", pascal_case(&kind))),
            body: emit_swift_listener_group(&kind, &listeners),
        });
    }
    targets.push(GeneratedTextTarget {
        path: listener_root.join("Common.swift"),
        body: emit_swift_listener_common(),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("FlareImEventListener.swift"),
        body: emit_swift_listener_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: callback_root.join("MessageSendCallback.swift"),
        body: emit_swift_message_send_callback(),
    });
    targets
}

fn emit_swift_event_code(event_codes: &[EventCodeEntry]) -> String {
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        "public enum EventCode {".to_string(),
    ];
    for entry in event_codes {
        lines.push(format!(
            "    public static let {}: Int = {}",
            event_code_camel_name(&entry.id),
            entry.code
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn swift_contract_type(type_name: &str, spec: &Value) -> String {
    match type_name {
        "String" => "String".to_string(),
        "Boolean" => "Bool".to_string(),
        "Int32" => "Int32".to_string(),
        "Int64" => "Int64".to_string(),
        "UInt32" => "UInt32".to_string(),
        "UInt64" => "UInt64".to_string(),
        "Float" | "Double" => "Double".to_string(),
        "JsonObject" => "[String: AnySendable]".to_string(),
        "StringMap" => "[String: String]".to_string(),
        "BinaryMap" => "[String: [UInt8]]".to_string(),
        "StringList" => "[String]".to_string(),
        _ if is_list_type_name(type_name) => {
            let inner = list_inner_type_name(type_name);
            format!("[{}]", swift_contract_type(inner, spec))
        }
        _ if is_known_ts_model_type(type_name, spec) => type_name.to_string(),
        _ => "[String: String]".to_string(),
    }
}

fn swift_model_field_type(model_name: &str, field: &Value, spec: &Value) -> String {
    let mut ty = if model_name == "ViewSnapshot" && str_field(field, "name") == "data" {
        "[String: AnySendable]".to_string()
    } else {
        swift_contract_type(str_field(field, "type"), spec)
    };
    if !contract_field_required(field) {
        ty.push('?');
    }
    ty
}

fn swift_type_contains_json_object(
    type_name: &str,
    spec: &Value,
    visiting: &mut BTreeSet<String>,
) -> bool {
    match type_name {
        "JsonObject" => true,
        _ if is_list_type_name(type_name) => {
            swift_type_contains_json_object(list_inner_type_name(type_name), spec, visiting)
        }
        _ if is_known_ts_model_type(type_name, spec) => {
            swift_model_contains_json_object(type_name, spec, visiting)
        }
        _ => false,
    }
}

fn swift_model_contains_json_object(
    model_name: &str,
    spec: &Value,
    visiting: &mut BTreeSet<String>,
) -> bool {
    if !visiting.insert(model_name.to_string()) {
        return false;
    }
    let Some(model) = all_spec_models(spec)
        .into_iter()
        .find(|model| str_field(model, "name") == model_name)
    else {
        return false;
    };
    child_arr(model, "fields")
        .iter()
        .any(|field| swift_type_contains_json_object(str_field(field, "type"), spec, visiting))
}

fn swift_default_value(type_name: &str) -> Option<&'static str> {
    match type_name {
        "String" => Some("\"\""),
        "Boolean" => Some("false"),
        "Int32" | "Int64" | "UInt32" | "UInt64" => Some("0"),
        "Float" | "Double" => Some("0.0"),
        "JsonObject" | "StringMap" | "BinaryMap" => Some("[:]"),
        "StringList" => Some("[]"),
        _ if is_list_type_name(type_name) => Some("[]"),
        _ => None,
    }
}

fn emit_swift_model_enum(enum_value: &Value) -> String {
    let name = str_field(enum_value, "name");
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        format!("/// {}", str_field(enum_value, "description")),
        format!("public enum {name}: String, Codable, Sendable {{"),
    ];
    for value in child_arr(enum_value, "values")
        .iter()
        .filter_map(Value::as_str)
    {
        lines.push(format!(
            "    case {} = {}",
            swift_identifier(&lower_first(&pascal_case(value))),
            json_quote(value)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_model_struct(model: &Value, spec: &Value) -> String {
    let name = str_field(model, "name");
    let conformances = if swift_model_contains_json_object(name, spec, &mut BTreeSet::new()) {
        "Sendable"
    } else {
        "Codable, Sendable"
    };
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        format!("/// {}", str_field(model, "description")),
        format!("public struct {name}: {conformances} {{"),
    ];
    for field in child_arr(model, "fields") {
        lines.push(format!(
            "    /// wire: `{}`. {}",
            str_field(field, "wireName"),
            str_field(field, "description")
        ));
        lines.push(format!(
            "    public let {}: {}",
            swift_identifier(str_field(field, "name")),
            swift_model_field_type(name, field, spec)
        ));
    }
    lines.push(String::new());
    let params = child_arr(model, "fields")
        .iter()
        .map(|field| {
            let default = if !contract_field_required(field) {
                " = nil".to_string()
            } else {
                swift_default_value(str_field(field, "type"))
                    .map(|value| format!(" = {value}"))
                    .unwrap_or_default()
            };
            format!(
                "{}: {}{}",
                swift_identifier(str_field(field, "name")),
                swift_model_field_type(name, field, spec),
                default
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    lines.push(format!("    public init({params}) {{"));
    for field in child_arr(model, "fields") {
        let name = swift_identifier(str_field(field, "name"));
        lines.push(format!("        self.{name} = {name}"));
    }
    lines.push("    }".to_string());
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_listener_common() -> String {
    [
        "import Foundation",
        "",
        "/// GENERATED. Do not edit by hand.",
        "public typealias EventCallback<T> = @Sendable (T) -> Void",
        "",
        "public protocol EventSubscription: AnyObject {",
        "    var id: String { get }",
        "    func unsubscribe()",
        "}",
    ]
    .join("\n")
}

fn emit_swift_listener_group(kind: &str, listeners: &[&Value]) -> String {
    let iface = listener_interface_name(kind);
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        format!("/// {} listener callbacks.", pascal_case(kind)),
        format!("public protocol {iface}: AnyObject {{"),
    ];
    for listener in listeners {
        lines.push(format!("    /// {}", str_field(listener, "description")));
        lines.push(format!(
            "    func {}(_ event: {})",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    lines.extend([
        "}".to_string(),
        String::new(),
        format!("public extension {iface} {{"),
    ]);
    for listener in listeners {
        lines.push(format!(
            "    func {}(_ event: {}) {{}}",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_listener_index(spec: &Value) -> String {
    let groups = typescript_listener_groups(spec);
    [
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        "/// Optional callback surface for apps that prefer one listener object.".to_string(),
        format!(
            "public protocol FlareImEventListener: AnyObject, {} {{}}",
            groups
                .keys()
                .map(|kind| listener_interface_name(kind))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ]
    .join("\n")
}

fn emit_swift_message_send_callback() -> String {
    [
        "import Foundation",
        "",
        "/// GENERATED. Do not edit by hand.",
        "/// Direct callback for `messages.sendMessage(request, callback)` progress and terminal states.",
        "public protocol MessageSendCallback: AnyObject {",
        "    /// Message upload or send progress changed.",
        "    func onProgress(_ event: ProgressEvent)",
        "    /// Message send completed successfully.",
        "    func onSuccess(_ event: MessageSendAckEvent)",
        "    /// Message send failed.",
        "    func onFailure(_ event: MessageSendFailedEvent)",
        "}",
        "",
        "public extension MessageSendCallback {",
        "    func onProgress(_ event: ProgressEvent) {}",
        "    func onSuccess(_ event: MessageSendAckEvent) {}",
        "    func onFailure(_ event: MessageSendFailedEvent) {}",
        "}",
    ]
    .join("\n")
}

fn arkts_contract_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let src_root = root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets");
    let model_root = src_root.join("model");
    let listener_root = src_root.join("listener");
    let mut targets = Vec::new();
    for enum_value in all_spec_enums(spec) {
        let name = str_field(enum_value, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{}.ets", snake_case(name))),
            body: emit_arkts_model_enum(enum_value),
        });
    }
    for model in all_spec_models(spec) {
        let name = str_field(model, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{}.ets", snake_case(name))),
            body: emit_arkts_model_interface(model, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: model_root.join("index.ets"),
        body: emit_arkts_model_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("common.ets"),
        body: emit_arkts_listener_common(),
    });
    for (kind, listeners) in typescript_listener_groups(spec) {
        targets.push(GeneratedTextTarget {
            path: listener_root.join(format!("{kind}.ets")),
            body: emit_arkts_listener_group(&kind, &listeners),
        });
    }
    targets.push(GeneratedTextTarget {
        path: listener_root.join("index.ets"),
        body: emit_arkts_listener_index(spec),
    });
    targets
}

fn arkts_contract_type(type_name: &str, spec: &Value) -> String {
    match type_name {
        "String" => "string".to_string(),
        "Boolean" => "boolean".to_string(),
        "Int32" | "Int64" | "UInt32" | "UInt64" | "Float" | "Double" => "number".to_string(),
        "JsonObject" => "Record<string, Object>".to_string(),
        "StringMap" => "Record<string, string>".to_string(),
        "BinaryMap" => "Record<string, Array<number>>".to_string(),
        "StringList" => "Array<string>".to_string(),
        _ if is_list_type_name(type_name) => {
            let inner = list_inner_type_name(type_name);
            format!("Array<{}>", arkts_contract_type(inner, spec))
        }
        _ if is_known_ts_model_type(type_name, spec) => type_name.to_string(),
        _ => "Record<string, Object>".to_string(),
    }
}

fn arkts_model_imports(model: &Value, spec: &Value) -> Vec<String> {
    let suffixes = contract_type_suffixes(spec);
    let current_suffix = model_package_suffix(str_field(model, "name"));
    contract_model_dependency_names(model, spec)
        .into_iter()
        .filter_map(|name| {
            let dep_suffix = suffixes.get(&name)?;
            let file = snake_case(&name);
            let mut rel = relative_contract_path(current_suffix, dep_suffix, &file);
            if !rel.starts_with("..") {
                rel = format!("./{rel}");
            }
            Some(format!("import {{ {name} }} from '{rel}';"))
        })
        .collect()
}

fn emit_arkts_model_enum(enum_value: &Value) -> String {
    let name = str_field(enum_value, "name");
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("/** {} */", str_field(enum_value, "description")),
        format!("export enum {name} {{"),
    ];
    for value in child_arr(enum_value, "values")
        .iter()
        .filter_map(Value::as_str)
    {
        lines.push(format!("  {} = {},", pascal_case(value), json_quote(value)));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_model_interface(model: &Value, spec: &Value) -> String {
    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    lines.extend(arkts_model_imports(model, spec));
    lines.extend([
        String::new(),
        format!("/** {} */", str_field(model, "description")),
        format!("export interface {} {{", str_field(model, "name")),
    ]);
    for field in child_arr(model, "fields") {
        let optional = if contract_field_required(field) {
            ""
        } else {
            "?"
        };
        lines.push(format!(
            "  /** wire: `{}`. {} */",
            str_field(field, "wireName"),
            str_field(field, "description")
        ));
        lines.push(format!(
            "  {}{}: {};",
            str_field(field, "name"),
            optional,
            arkts_contract_type(str_field(field, "type"), spec)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_model_index(spec: &Value) -> String {
    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    for entry in contract_model_entries(spec) {
        lines.push(format!(
            "export * from './{}{}';",
            if entry.suffix.is_empty() {
                String::new()
            } else {
                format!("{}/", entry.suffix.replace('.', "/"))
            },
            model_file_stem(&entry.name, true)
        ));
    }
    lines.join("\n")
}

fn emit_arkts_listener_common() -> String {
    [
        "// GENERATED. Do not edit by hand.",
        "/** Callback invoked for one typed SDK notification. */",
        "export type EventCallback<T> = (event: T) => void;",
        "",
        "/** Disposable local listener registration returned by high-level `on*` APIs. */",
        "export interface EventSubscription {",
        "  readonly id: string;",
        "  unsubscribe(): void;",
        "}",
    ]
    .join("\n")
}

fn emit_arkts_listener_group(kind: &str, listeners: &[&Value]) -> String {
    let payloads = listeners
        .iter()
        .map(|listener| str_field(listener, "payload").to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("import {{ {payloads} }} from '../model';"),
        String::new(),
        format!("/** {} listener callbacks. */", pascal_case(kind)),
        format!("export interface {} {{", listener_interface_name(kind)),
    ];
    for listener in listeners {
        lines.push(format!("  /** {} */", str_field(listener, "description")));
        lines.push(format!(
            "  {}?(event: {}): void;",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    if kind == "message" {
        lines.extend([
            "}".to_string(),
            String::new(),
            "/** Direct callback for `messages.sendMessage(request, callback)` progress and terminal states. */".to_string(),
            "export interface MessageSendCallback {".to_string(),
            "  /** Message upload or send progress changed. */".to_string(),
            "  onProgress?(event: ProgressEvent): void;".to_string(),
            "  /** Message send completed successfully. */".to_string(),
            "  onSuccess?(event: MessageSendAckEvent): void;".to_string(),
            "  /** Message send failed. */".to_string(),
            "  onFailure?(event: MessageSendFailedEvent): void;".to_string(),
        ]);
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_listener_index(spec: &Value) -> String {
    let groups = typescript_listener_groups(spec);
    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    lines.push("export * from './common';".to_string());
    for kind in groups.keys() {
        lines.push(format!("export * from './{kind}';"));
    }
    lines.push(String::new());
    for kind in groups.keys() {
        lines.push(format!(
            "import {{ {} }} from './{kind}';",
            listener_interface_name(kind)
        ));
    }
    lines.extend([
        String::new(),
        "/** Optional callback surface for apps that prefer one listener object. */".to_string(),
        format!(
            "export interface FlareImEventListener extends {} {{}}",
            groups
                .keys()
                .map(|kind| listener_interface_name(kind))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ]);
    lines.join("\n")
}

fn cangjie_contract_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let src_root = root.join("packages/flare-core-harmony-cangjie-sdk/src");
    let model_root = src_root.join("model");
    let listener_root = src_root.join("listener");
    let mut targets = Vec::new();
    for enum_value in all_spec_enums(spec) {
        let name = str_field(enum_value, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{name}.cj")),
            body: emit_cangjie_model_enum(enum_value),
        });
    }
    for model in all_spec_models(spec) {
        let name = str_field(model, "name");
        targets.push(GeneratedTextTarget {
            path: model_root
                .join(suffix_path(model_package_suffix(name)))
                .join(format!("{name}.cj")),
            body: emit_cangjie_model_class(model, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: model_root.join("README.md"),
        body: [
            "# Generated Cangjie Models",
            "",
            "One generated `.cj` file is emitted for each model and enum in `sdk-spec/models`.",
            "Collection/map payloads are represented as raw JSON strings at the bridge boundary and are serialized through the runtime JSON codec.",
        ]
        .join("\n"),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("Common.cj"),
        body: emit_cangjie_listener_common(),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("Callbacks.cj"),
        body: emit_cangjie_callbacks(spec),
    });
    for (kind, listeners) in typescript_listener_groups(spec) {
        targets.push(GeneratedTextTarget {
            path: listener_root.join(format!("{}Listener.cj", pascal_case(&kind))),
            body: emit_cangjie_listener_group(&kind, &listeners),
        });
    }
    targets.push(GeneratedTextTarget {
        path: listener_root.join("FlareImEventListener.cj"),
        body: emit_cangjie_listener_index(spec),
    });
    targets
}

fn cangjie_contract_type(type_name: &str, spec: &Value) -> String {
    match type_name {
        "String" => "String".to_string(),
        "Boolean" => "Bool".to_string(),
        "Int32" => "Int32".to_string(),
        "Int64" => "Int64".to_string(),
        "UInt32" => "UInt32".to_string(),
        "UInt64" => "UInt64".to_string(),
        "Float" | "Double" => "Float64".to_string(),
        "JsonObject" | "StringMap" | "BinaryMap" | "StringList" => "String".to_string(),
        _ if is_list_type_name(type_name) => "String".to_string(),
        _ if is_known_ts_model_type(type_name, spec) => type_name.to_string(),
        _ => "String".to_string(),
    }
}

fn cangjie_field_type(field: &Value, spec: &Value) -> String {
    let ty = cangjie_contract_type(str_field(field, "type"), spec);
    if contract_field_required(field) {
        ty
    } else {
        format!("?{ty}")
    }
}

fn emit_cangjie_model_enum(enum_value: &Value) -> String {
    let name = str_field(enum_value, "name");
    let mut lines = vec![
        format!(
            "package flare_core_harmony_cangjie_sdk.model.{}",
            model_package_suffix(name)
        ),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("// {}", str_field(enum_value, "description")),
        format!("public enum {name} {{"),
    ];
    for value in child_arr(enum_value, "values")
        .iter()
        .filter_map(Value::as_str)
    {
        lines.push(format!("    | {}", pascal_case(value)));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_cangjie_model_class(model: &Value, spec: &Value) -> String {
    let name = str_field(model, "name");
    let mut lines = vec![
        format!(
            "package flare_core_harmony_cangjie_sdk.model.{}",
            model_package_suffix(name)
        ),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        "// Collection/map payloads are represented as raw JSON strings at the bridge boundary."
            .to_string(),
        format!("// {}", str_field(model, "description")),
        format!("public class {name} {{"),
    ];
    for field in child_arr(model, "fields") {
        let optional = if contract_field_required(field) {
            ""
        } else {
            "Optional. "
        };
        lines.push(format!(
            "    // wire: `{}`. {optional}{}",
            str_field(field, "wireName"),
            str_field(field, "description")
        ));
        lines.push(format!(
            "    public let {}: {}",
            cangjie_field_name(str_field(field, "name")),
            cangjie_field_type(field, spec)
        ));
    }
    let params = child_arr(model, "fields")
        .iter()
        .map(|field| {
            format!(
                "{}!: {}",
                cangjie_field_name(str_field(field, "name")),
                cangjie_field_type(field, spec)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    lines.extend([String::new(), format!("    public init({params}) {{")]);
    for field in child_arr(model, "fields") {
        let name = cangjie_field_name(str_field(field, "name"));
        lines.push(format!("        this.{name} = {name}"));
    }
    lines.extend(["    }".to_string(), "}".to_string()]);
    lines.join("\n")
}

fn emit_cangjie_listener_common() -> String {
    [
        "package flare_core_harmony_cangjie_sdk.listener",
        "",
        "import flare_core_harmony_cangjie_sdk.model.*",
        "",
        "// GENERATED. Do not edit by hand.",
        "public interface EventSubscription {",
        "    prop id: String",
        "    func unsubscribe(): Unit",
        "}",
    ]
    .join("\n")
}

fn emit_cangjie_callbacks(spec: &Value) -> String {
    let payloads = listener_payloads(spec);
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.listener".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.model.*".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
    ];
    for payload in payloads {
        lines.extend([
            format!("public interface {payload}Callback {{"),
            format!("    func handle(event: {payload}): Unit"),
            "}".to_string(),
            String::new(),
        ]);
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines.join("\n")
}

fn emit_cangjie_listener_group(kind: &str, listeners: &[&Value]) -> String {
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.listener".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.model.*".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("// {} listener callbacks.", pascal_case(kind)),
        format!("public interface {} {{", listener_interface_name(kind)),
    ];
    for listener in listeners {
        lines.push(format!("    // {}", str_field(listener, "description")));
        lines.push(format!(
            "    func {}(event: {}): Unit",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    if kind == "message" {
        lines.extend([
            "}".to_string(),
            String::new(),
            "// Direct callback for `messages.sendMessage(request, callback)` progress and terminal states.".to_string(),
            "public interface MessageSendCallback {".to_string(),
            "    // Message upload or send progress changed.".to_string(),
            "    func onProgress(event: ProgressEvent): Unit".to_string(),
            "    // Message send completed successfully.".to_string(),
            "    func onSuccess(event: MessageSendAckEvent): Unit".to_string(),
            "    // Message send failed.".to_string(),
            "    func onFailure(event: MessageSendFailedEvent): Unit".to_string(),
        ]);
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_cangjie_listener_index(spec: &Value) -> String {
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.listener".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.model.*".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        "// Optional callback surface for apps that prefer one listener object.".to_string(),
        "public interface FlareImEventListener {".to_string(),
    ];
    for listener in child_arr(spec, "listeners") {
        lines.push(format!("    // {}", str_field(listener, "description")));
        lines.push(format!(
            "    func {}(event: {}): Unit",
            str_field(listener, "name"),
            str_field(listener, "payload")
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}
