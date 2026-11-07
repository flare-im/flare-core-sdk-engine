use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    GeneratedTextTarget, cangjie_identifier, child_arr, facade_prop, is_bridge_internal_method,
    is_connect_token_refresher_method, is_known_ts_model_type, is_list_type_name,
    kotlin_model_package_imports, list_inner_type_name, listener_payloads,
    load_expanded_client_spec, pascal_case, remove_output_paths, single_trailing_newline,
    str_field, swift_identifier, ts_api_interface_name, ts_api_module_key, upsert_text_file,
};

pub(crate) fn emit_platform_api_files(root: &Path, check: bool) -> Result<()> {
    let spec = load_expanded_client_spec(root)?;
    let mut drifted = Vec::new();
    if !check {
        clean_platform_api_outputs(root)?;
    }
    for target in platform_api_targets(root, &spec)? {
        let body = single_trailing_newline(&target.body);
        upsert_text_file(&target.path, &body, check, &mut drifted)?;
    }
    if !check {
        clean_retired_platform_api_shims(root)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned platform API output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned platform API artifacts generated");
    }
    Ok(())
}

fn platform_api_targets(root: &Path, spec: &Value) -> Result<Vec<GeneratedTextTarget>> {
    let mut targets = Vec::new();
    targets.extend(emit_dart_api_targets(root, spec));
    targets.extend(emit_kotlin_api_targets(root, spec));
    targets.extend(emit_swift_api_targets(root, spec));
    targets.extend(emit_arkts_api_targets(root, spec));
    targets.extend(emit_cangjie_api_targets(root, spec));
    Ok(targets)
}

fn clean_platform_api_outputs(root: &Path) -> Result<()> {
    remove_output_paths(platform_api_roots(root))?;
    clean_retired_platform_api_shims(root)
}

fn clean_retired_platform_api_shims(root: &Path) -> Result<()> {
    for path in [
        root.join("packages/flare-core-flutter-sdk/lib/src/callbacks.dart"),
        root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/generated/Callbacks.kt",
        ),
        root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/generated/callbacks.ets"),
    ] {
        if path.is_file() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    Ok(())
}

fn platform_api_roots(root: &Path) -> Vec<PathBuf> {
    vec![
        root.join("packages/flare-core-flutter-sdk/lib/src/api"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/api"),
        root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Api"),
        root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/api"),
        root.join("packages/flare-core-harmony-cangjie-sdk/src/api"),
        root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/generated/api"),
    ]
}

fn method_summary(method: &Value) -> String {
    let extra = if method.get("dispatchOp").is_some() {
        format!(", dispatch op `{}`", str_field(method, "dispatchOp"))
    } else {
        String::new()
    };
    format!(
        "{} maps to `{}` via `{}`{extra}. Operation: `{}`.",
        str_field(method, "name"),
        str_field(method, "cApi"),
        str_field(method, "transport"),
        str_field(method, "operation")
    )
}

/// The element model of a `<Model>List` API type (a bare JSON array of `Model` on the wire),
/// when `Model` is a known spec model. Other names ending in `List` are not model lists.
pub(crate) fn api_model_list_element<'a>(name: &'a str, spec: &Value) -> Option<&'a str> {
    if !is_list_type_name(name) {
        return None;
    }
    let inner = list_inner_type_name(name);
    is_known_ts_model_type(inner, spec).then_some(inner)
}

/// Doc line for the facade's `setConnectTokenRefresher`, identical on every platform.
const CONNECT_TOKEN_REFRESHER_DOC: &str = "Installs (or, with null, removes) the host-managed connect token refresher: an async function returning the new connect token, or null when none can be obtained. The core calls it once after the gateway rejects a connection on authentication and waits at most 20 seconds. Not needed with SDK-managed tokens (an auth block in the init config, even an empty one).";

fn module_has_connect_token_refresher(module: &Value) -> bool {
    child_arr(module, "methods")
        .iter()
        .any(is_connect_token_refresher_method)
}

pub(crate) fn dart_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "void".to_string(),
        "BooleanResponse" => "bool".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => match api_model_list_element(name, spec) {
            Some(inner) => format!("List<{inner}>"),
            None => "Map<String, Object?>".to_string(),
        },
    }
}

pub(crate) fn kotlin_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "Unit".to_string(),
        "BooleanResponse" => "Boolean".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => match api_model_list_element(name, spec) {
            Some(inner) => format!("List<{inner}>"),
            None => "Map<String, Any?>".to_string(),
        },
    }
}

pub(crate) fn swift_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "Void".to_string(),
        "BooleanResponse" => "Bool".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => match api_model_list_element(name, spec) {
            Some(inner) => format!("[{inner}]"),
            None => "[String: AnySendable]".to_string(),
        },
    }
}

pub(crate) fn arkts_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "void".to_string(),
        "BooleanResponse" => "boolean".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => match api_model_list_element(name, spec) {
            Some(inner) => format!("Array<{inner}>"),
            None => "Record<string, Object>".to_string(),
        },
    }
}

pub(crate) fn cangjie_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "Unit".to_string(),
        "BooleanResponse" => "BooleanResponse".to_string(),
        "JsonValue" => "JsonValue".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => match api_model_list_element(name, spec) {
            Some(inner) => format!("Array<{inner}>"),
            None => "String".to_string(),
        },
    }
}

fn emit_dart_api_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let api_root = root.join("packages/flare-core-flutter-sdk/lib/src/api");
    let modules_root = api_root.join("modules");
    let mut targets = Vec::new();
    for module in child_arr(spec, "modules") {
        let key = ts_api_module_key(module);
        targets.push(GeneratedTextTarget {
            path: modules_root.join(format!("{key}.dart")),
            body: emit_dart_api_module(spec, module),
        });
    }
    let mut module_exports = vec!["// GENERATED. Do not edit by hand.".to_string()];
    for module in child_arr(spec, "modules") {
        module_exports.push(format!(
            "export '{}.dart' show {};",
            ts_api_module_key(module),
            ts_api_interface_name(module)
        ));
    }
    targets.push(GeneratedTextTarget {
        path: modules_root.join("modules.dart"),
        body: module_exports.join("\n"),
    });
    let mut client_lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import 'modules/modules.dart';".to_string(),
        String::new(),
        "typedef JsonObject = Map<String, Object?>;".to_string(),
        String::new(),
        "/// Root SDK client. Create one instance per app/session boundary.".to_string(),
        "abstract interface class FlareImClient implements SessionApi {".to_string(),
    ];
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        client_lines.push(format!("  /// {}", str_field(module, "description")));
        client_lines.push(format!(
            "  {} get {};",
            ts_api_interface_name(module),
            facade_prop(module)
        ));
    }
    client_lines.push("}".to_string());
    targets.push(GeneratedTextTarget {
        path: api_root.join("client.dart"),
        body: client_lines.join("\n"),
    });
    targets.push(GeneratedTextTarget {
        path: api_root.join("api.dart"),
        body: [
            "// GENERATED. Do not edit by hand.",
            "export 'client.dart';",
            "export 'modules/modules.dart';",
        ]
        .join("\n"),
    });
    targets
}

fn dart_module_needs_models(spec: &Value, module: &Value) -> bool {
    child_arr(module, "methods").iter().any(|method| {
        [str_field(method, "request"), str_field(method, "response")]
            .into_iter()
            .any(|type_name| {
                is_known_ts_model_type(type_name, spec)
                    || api_model_list_element(type_name, spec).is_some()
            })
    })
}

fn emit_dart_api_module(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        format!(
            "// Module API: `{}` — {}",
            str_field(module, "key"),
            str_field(module, "description")
        ),
    ];
    if dart_module_needs_models(spec, module) {
        lines.push("import '../../model/model.dart';".to_string());
    }
    if module_has_connect_token_refresher(module) {
        lines.push(
            "import '../../contract/bridge_contract.dart' show ConnectTokenRefresher;".to_string(),
        );
    }
    if str_field(module, "key") == "events" {
        lines.push("import '../../model/model.dart';".to_string());
        lines.push("import '../../listener/listener.dart';".to_string());
    } else if str_field(module, "key") == "messages" {
        lines.push("import '../../callback/callback.dart';".to_string());
    }
    lines.extend([
        String::new(),
        format!("/// {}", str_field(module, "description")),
        format!("abstract interface class {iface} {{"),
    ]);
    for method in child_arr(module, "methods") {
        if is_bridge_internal_method(method) {
            continue;
        }
        if is_connect_token_refresher_method(method) {
            lines.push(format!("  /// {}", CONNECT_TOKEN_REFRESHER_DOC));
            lines.push(
                "  Future<void> setConnectTokenRefresher(ConnectTokenRefresher? refresher);"
                    .to_string(),
            );
            continue;
        }
        let req = dart_api_type(str_field(method, "request"), spec);
        let res = dart_api_type(str_field(method, "response"), spec);
        let arg = if str_field(method, "name") == "sendMessage" {
            "SendMessageRequest request, [MessageSendCallback? callback]".to_string()
        } else if req == "void" {
            String::new()
        } else {
            format!("{req} request")
        };
        lines.push(format!("  /// {}", method_summary(method)));
        lines.push(format!(
            "  Future<{res}> {}({arg});",
            str_field(method, "name")
        ));
    }
    if str_field(module, "key") == "events" {
        lines.extend([
            "  /// Registers a listener object for typed SDK runtime notifications.".to_string(),
            "  EventSubscription addEventListener(FlareImEventListener listener);".to_string(),
            "  /// Removes one local listener registration.".to_string(),
            "  void removeEventListener(EventSubscription subscription);".to_string(),
        ]);
        for listener in child_arr(spec, "listeners") {
            lines.push(format!("  /// {}", str_field(listener, "description")));
            lines.push(format!(
                "  EventSubscription {}(EventCallback<{}> listener);",
                str_field(listener, "name"),
                str_field(listener, "payload")
            ));
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_kotlin_api_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let api_root = root.join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/api");
    let mut targets = Vec::new();
    for module in child_arr(spec, "modules") {
        targets.push(GeneratedTextTarget {
            path: api_root
                .join(kotlin_api_module_dir(module))
                .join(format!("{}.kt", ts_api_interface_name(module))),
            body: emit_kotlin_api_module(spec, module),
        });
    }
    let mut api_imports = vec!["import com.flare.im.api.session.SessionApi".to_string()];
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        api_imports.push(format!(
            "import com.flare.im.api.{}.{}",
            kotlin_api_module_dir(module),
            ts_api_interface_name(module)
        ));
    }
    let mut client = vec!["package com.flare.im.api".to_string(), String::new()];
    client.extend(api_imports);
    client.extend([
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "/** Root SDK client. Create one instance per app/session boundary. */".to_string(),
        "interface FlareImClient : SessionApi {".to_string(),
    ]);
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        let prop = facade_prop(module);
        client.push(format!("    /** {} */", str_field(module, "description")));
        client.push(format!("    val {prop}: {}Api", pascal_case(prop)));
    }
    client.push("}".to_string());
    targets.push(GeneratedTextTarget {
        path: api_root.join("FlareImClient.kt"),
        body: client.join("\n"),
    });
    targets
}

pub(crate) fn kotlin_api_module_dir(module: &Value) -> String {
    if str_field(module, "facade") == "client" {
        "session".to_string()
    } else {
        facade_prop(module).to_ascii_lowercase()
    }
}

fn kotlin_api_module_package(module: &Value) -> String {
    format!("com.flare.im.api.{}", kotlin_api_module_dir(module))
}

fn emit_kotlin_api_module(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        format!("package {}", kotlin_api_module_package(module)),
        String::new(),
        "import com.flare.im.callback.*".to_string(),
        "import com.flare.im.listener.*".to_string(),
    ];
    if module_has_connect_token_refresher(module) {
        lines.push("import com.flare.im.contract.ConnectTokenRefresher".to_string());
    }
    lines.extend(kotlin_model_package_imports(spec));
    lines.extend([
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        format!("/** {} */", str_field(module, "description")),
        format!("interface {iface} {{"),
    ]);
    for method in child_arr(module, "methods") {
        if is_bridge_internal_method(method) {
            continue;
        }
        if is_connect_token_refresher_method(method) {
            lines.push(format!("    /** {} */", CONNECT_TOKEN_REFRESHER_DOC));
            lines.push(
                "    suspend fun setConnectTokenRefresher(refresher: ConnectTokenRefresher?): Unit"
                    .to_string(),
            );
            continue;
        }
        let req = kotlin_api_type(str_field(method, "request"), spec);
        let res = kotlin_api_type(str_field(method, "response"), spec);
        let arg = if str_field(method, "name") == "sendMessage" {
            "request: SendMessageRequest, callback: MessageSendCallback? = null".to_string()
        } else if req == "Unit" {
            String::new()
        } else {
            format!("request: {req}")
        };
        lines.push(format!("    /** {} */", method_summary(method)));
        lines.push(format!(
            "    suspend fun {}({arg}): {res}",
            str_field(method, "name")
        ));
    }
    if str_field(module, "key") == "events" {
        lines.extend([
            "    fun addEventListener(listener: FlareImEventListener): EventSubscription"
                .to_string(),
            "    fun removeEventListener(subscription: EventSubscription)".to_string(),
        ]);
        for listener in child_arr(spec, "listeners") {
            lines.push(format!(
                "    fun {}(listener: EventCallback<{}>): EventSubscription",
                str_field(listener, "name"),
                str_field(listener, "payload")
            ));
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_api_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let api_root = root.join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Api");
    let mut targets = Vec::new();
    for module in child_arr(spec, "modules") {
        let iface = ts_api_interface_name(module);
        targets.push(GeneratedTextTarget {
            path: api_root.join("Modules").join(format!("{iface}.swift")),
            body: emit_swift_api_module(spec, module),
        });
    }
    let mut client = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        "public struct AnySendable: @unchecked Sendable { public let value: Any; public init(_ value: Any) { self.value = value } }".to_string(),
        String::new(),
        "public protocol FlareImClientProtocol: SessionApiProtocol {".to_string(),
    ];
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        client.push(format!(
            "    var {}: any {}Protocol {{ get }}",
            facade_prop(module),
            ts_api_interface_name(module)
        ));
    }
    client.push("}".to_string());
    targets.push(GeneratedTextTarget {
        path: api_root.join("FlareImClientApi.swift"),
        body: client.join("\n"),
    });
    targets
}

fn emit_swift_api_module(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        format!("/// {}", str_field(module, "description")),
        format!("public protocol {iface}Protocol: AnyObject {{"),
    ];
    for method in child_arr(module, "methods") {
        if is_bridge_internal_method(method) {
            continue;
        }
        if is_connect_token_refresher_method(method) {
            lines.push(format!("    /// {}", CONNECT_TOKEN_REFRESHER_DOC));
            lines.push(
                "    func setConnectTokenRefresher(_ refresher: ConnectTokenRefresher?) async throws"
                    .to_string(),
            );
            continue;
        }
        let req = swift_api_type(str_field(method, "request"), spec);
        let res = swift_api_type(str_field(method, "response"), spec);
        let arg = if str_field(method, "name") == "sendMessage" {
            "_ request: SendMessageRequest, callback: (any MessageSendCallback)?".to_string()
        } else if req == "Void" {
            String::new()
        } else {
            format!("_ request: {req}")
        };
        lines.push(format!(
            "    func {}({arg}) async throws -> {res}",
            swift_identifier(str_field(method, "name"))
        ));
    }
    if str_field(module, "key") == "events" {
        lines.extend([
            "    func addEventListener(_ listener: any FlareImEventListener) -> any EventSubscription".to_string(),
            "    func removeEventListener(_ subscription: any EventSubscription)".to_string(),
        ]);
        for listener in child_arr(spec, "listeners") {
            lines.push(format!(
                "    func {}(_ listener: @escaping EventCallback<{}>) -> any EventSubscription",
                str_field(listener, "name"),
                str_field(listener, "payload")
            ));
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_api_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let api_root = root.join("packages/flare-core-harmony-arkts-sdk/src/main/ets/api");
    let mut targets = Vec::new();
    for module in child_arr(spec, "modules") {
        targets.push(GeneratedTextTarget {
            path: api_root
                .join("modules")
                .join(format!("{}.ets", ts_api_module_key(module))),
            body: emit_arkts_api_module(spec, module),
        });
    }
    let mut exports = vec!["// GENERATED. Do not edit by hand.".to_string()];
    for module in child_arr(spec, "modules") {
        exports.push(format!(
            "export type {{ {} }} from './modules/{}';",
            ts_api_interface_name(module),
            ts_api_module_key(module)
        ));
    }
    exports.push("export type { FlareImClient } from './client';".to_string());
    targets.push(GeneratedTextTarget {
        path: api_root.join("index.ets"),
        body: exports.join("\n"),
    });
    let mut client = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { SessionApi } from './modules/session';".to_string(),
    ];
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        client.push(format!(
            "import type {{ {} }} from './modules/{}';",
            ts_api_interface_name(module),
            ts_api_module_key(module)
        ));
    }
    client.extend([
        String::new(),
        "export interface FlareImClient extends SessionApi {".to_string(),
    ]);
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        client.push(format!(
            "  readonly {}: {};",
            facade_prop(module),
            ts_api_interface_name(module)
        ));
    }
    client.push("}".to_string());
    targets.push(GeneratedTextTarget {
        path: api_root.join("client.ets"),
        body: client.join("\n"),
    });
    targets
}

fn emit_arkts_api_module(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    // Listener payloads plus every model the module's methods take or return.
    let model_imports = listener_payloads(spec)
        .into_iter()
        .chain(
            child_arr(module, "methods")
                .iter()
                .filter(|method| {
                    !is_bridge_internal_method(method) && !is_connect_token_refresher_method(method)
                })
                .flat_map(|method| {
                    [str_field(method, "request"), str_field(method, "response")]
                        .into_iter()
                        .filter_map(|type_name| {
                            if is_known_ts_model_type(type_name, spec) {
                                Some(type_name.to_string())
                            } else {
                                api_model_list_element(type_name, spec).map(str::to_string)
                            }
                        })
                }),
        )
        .collect::<std::collections::BTreeSet<_>>();
    let mut imports = vec![format!(
        "import {{ {} }} from '../../model';",
        model_imports.into_iter().collect::<Vec<_>>().join(", ")
    )];
    if matches!(str_field(module, "key"), "events" | "messages") {
        imports.push("import { EventCallback, EventSubscription, FlareImEventListener, MessageSendCallback } from '../../listener';".to_string());
    }
    if module_has_connect_token_refresher(module) {
        imports.push(
            "import { ConnectTokenRefresher } from '../../contract/BridgeContract';".to_string(),
        );
    }
    let mut lines = vec!["// GENERATED. Do not edit by hand.".to_string()];
    lines.extend(imports);
    lines.extend([
        String::new(),
        format!("/** {} */", str_field(module, "description")),
        format!("export interface {iface} {{"),
    ]);
    for method in child_arr(module, "methods") {
        if is_bridge_internal_method(method) {
            continue;
        }
        if is_connect_token_refresher_method(method) {
            lines.push(format!("  /** {} */", CONNECT_TOKEN_REFRESHER_DOC));
            lines.push(
                "  setConnectTokenRefresher(refresher: ConnectTokenRefresher | null): Promise<void>;"
                    .to_string(),
            );
            continue;
        }
        let req = arkts_api_type(str_field(method, "request"), spec);
        let res = arkts_api_type(str_field(method, "response"), spec);
        let arg = if str_field(method, "name") == "sendMessage" {
            "request: SendMessageRequest, callback?: MessageSendCallback".to_string()
        } else if req == "void" {
            String::new()
        } else {
            format!("request: {req}")
        };
        lines.push(format!(
            "  {}({arg}): Promise<{res}>;",
            str_field(method, "name")
        ));
    }
    if str_field(module, "key") == "events" {
        lines.extend([
            "  addEventListener(listener: FlareImEventListener): EventSubscription;".to_string(),
            "  removeEventListener(subscription: EventSubscription): void;".to_string(),
        ]);
        for listener in child_arr(spec, "listeners") {
            lines.push(format!(
                "  {}(listener: EventCallback<{}>): EventSubscription;",
                str_field(listener, "name"),
                str_field(listener, "payload")
            ));
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

pub(crate) fn cangjie_api_arg(method: &Value, spec: &Value) -> String {
    let req = cangjie_api_type(str_field(method, "request"), spec);
    if req == "Unit" {
        String::new()
    } else if is_known_ts_model_type(str_field(method, "request"), spec) {
        format!("request: {req}")
    } else {
        "requestJson: String".to_string()
    }
}

fn emit_cangjie_api_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let api_root = root.join("packages/flare-core-harmony-cangjie-sdk/src/api");
    let mut targets = Vec::new();
    for module in child_arr(spec, "modules") {
        let iface = ts_api_interface_name(module);
        targets.push(GeneratedTextTarget {
            path: api_root.join("modules").join(format!("{iface}.cj")),
            body: emit_cangjie_api_module(spec, module),
        });
    }
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.api".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.api.modules.*".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        "public interface FlareImClient <: SessionApi {".to_string(),
    ];
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        lines.push(format!(
            "    prop {}: {}",
            facade_prop(module),
            ts_api_interface_name(module)
        ));
    }
    lines.push("}".to_string());
    targets.push(GeneratedTextTarget {
        path: api_root.join("FlareImClient.cj"),
        body: lines.join("\n"),
    });
    targets
}

fn emit_cangjie_api_module(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.api.modules".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.model.*".to_string(),
    ];
    if module_has_connect_token_refresher(module) {
        lines.push(
            "import flare_core_harmony_cangjie_sdk.contract.ConnectTokenRefresher".to_string(),
        );
    }
    lines.extend([
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("// {}", str_field(module, "description")),
        format!("public interface {iface} {{"),
    ]);
    for method in child_arr(module, "methods") {
        if is_bridge_internal_method(method) {
            continue;
        }
        if is_connect_token_refresher_method(method) {
            lines.push(format!("    // {}", CONNECT_TOKEN_REFRESHER_DOC));
            lines.push(
                "    func setConnectTokenRefresher(refresher: ?ConnectTokenRefresher): Unit"
                    .to_string(),
            );
            continue;
        }
        lines.push(format!(
            "    func {}({}): {}",
            cangjie_identifier(str_field(method, "name")),
            cangjie_api_arg(method, spec),
            cangjie_api_type(str_field(method, "response"), spec)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

#[cfg(test)]
mod connect_token_refresher_tests {
    use super::*;

    fn session(spec: &Value) -> &Value {
        child_arr(spec, "modules")
            .iter()
            .find(|module| str_field(module, "facade") == "client")
            .expect("session module")
    }

    /// Every native facade takes the host's async function and hides the bridge-level answer.
    #[test]
    fn session_facades_take_the_refresher_function_and_hide_the_bridge_answer() {
        let root = crate::context::workspace_root().unwrap();
        let spec = load_expanded_client_spec(&root).unwrap();
        let module = session(&spec);
        let cases = [
            (
                emit_kotlin_api_module(&spec, module),
                "suspend fun setConnectTokenRefresher(refresher: ConnectTokenRefresher?): Unit",
            ),
            (
                emit_swift_api_module(&spec, module),
                "func setConnectTokenRefresher(_ refresher: ConnectTokenRefresher?) async throws",
            ),
            (
                emit_dart_api_module(&spec, module),
                "Future<void> setConnectTokenRefresher(ConnectTokenRefresher? refresher);",
            ),
            (
                emit_arkts_api_module(&spec, module),
                "setConnectTokenRefresher(refresher: ConnectTokenRefresher | null): Promise<void>;",
            ),
            (
                emit_cangjie_api_module(&spec, module),
                "func setConnectTokenRefresher(refresher: ?ConnectTokenRefresher): Unit",
            ),
        ];
        for (body, signature) in cases {
            assert!(
                body.contains(signature),
                "missing `{signature}` in:\n{body}"
            );
            assert!(
                !body.contains("completeConnectTokenRefresh"),
                "the bridge-level answer leaked into a facade:\n{body}"
            );
            assert!(
                !body.contains("SetConnectTokenRefresherRequest"),
                "the facade still takes the bridge-level request:\n{body}"
            );
        }
    }
}
