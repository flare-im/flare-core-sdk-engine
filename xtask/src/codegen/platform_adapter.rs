use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};

use crate::{
    GeneratedTextTarget, arkts_api_type, camel_const, cangjie_api_arg, cangjie_api_type,
    cangjie_identifier, child_arr, dart_api_type, find_model, is_known_ts_model_type, json_quote,
    kotlin_api_module_dir, kotlin_api_type, kotlin_model_package_imports,
    load_expanded_client_spec, lower_first, message_build_catalog_entries,
    message_builder_extra_methods, message_builder_request_models, pascal_case, screaming_snake,
    single_trailing_newline, str_field, swift_api_type, swift_identifier, ts_api_interface_name,
    ts_api_module_key, ts_model_from_json_fn, upsert_text_file,
};

fn dart_native_key(operation: &str) -> String {
    camel_const(operation)
}

fn dart_model_from_json_fn(model_name: &str) -> String {
    ts_model_from_json_fn(model_name)
}

fn dart_build_request_wire_lines(model: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    for field in child_arr(model, "fields") {
        let name = str_field(field, "name");
        let wire = str_field(field, "wireName");
        let type_name = str_field(field, "type");
        let required = field
            .get("required")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        match type_name {
            "String" | "Boolean" | "Int32" | "Int64" | "UInt32" | "UInt64" | "Float" | "Double" => {
                if type_name == "Boolean" && !required {
                    let default = field
                        .get("default")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    lines.push(format!("      '{wire}': request.{name} ?? {default},"));
                } else if required || type_name == "Boolean" {
                    lines.push(format!("      '{wire}': request.{name},"));
                } else {
                    lines.push(format!(
                        "      if (request.{name} != null) '{wire}': request.{name}!"
                    ));
                }
            }
            "StringList" => {
                if required {
                    lines.push(format!("      '{wire}': request.{name},"));
                } else if field.get("default").and_then(Value::as_array).is_some() {
                    lines.push(format!(
                        "      '{wire}': request.{name} ?? const <String>[],"
                    ));
                } else {
                    lines.push(format!(
                        "      if (request.{name} != null) '{wire}': request.{name}!"
                    ));
                }
            }
            "JsonObject" => lines.push(format!("      '{wire}': request.{name},")),
            // Card attributes, mini program extra: sent as is, absent when unset.
            "StringMap" => {
                if required {
                    lines.push(format!("      '{wire}': request.{name},"));
                } else {
                    lines.push(format!(
                        "      if (request.{name} != null) '{wire}': request.{name}!"
                    ));
                }
            }
            "MessageContent" => lines.push(format!(
                "      '{wire}': messageContentToWireMap(request.{name}),"
            )),
            "ImageGroupContentPayload" => lines.extend([
                "      'images': request.payload.images".to_string(),
                "          .map((item) => {".to_string(),
                "        'imageId': item.imageId,".to_string(),
                "        if (item.url != null) 'url': item.url!,".to_string(),
                "        if (item.title != null) 'title': item.title!,".to_string(),
                "        if (item.width != null) 'width': item.width!,".to_string(),
                "        if (item.height != null) 'height': item.height!,".to_string(),
                "      }).toList(growable: false),".to_string(),
                "      if (request.payload.title != null) 'title': request.payload.title!,"
                    .to_string(),
            ]),
            "ForwardSourceMessageList" => lines.extend([
                "      'sourceMessages': request.sourceMessages".to_string(),
                "          .map((item) => {".to_string(),
                "        'sourceMessageId': item.sourceMessageId,".to_string(),
                "        if (item.sourceConversationId != null)".to_string(),
                "          'sourceConversationId': item.sourceConversationId!,".to_string(),
                "        if (item.sourceSenderId != null)".to_string(),
                "          'sourceSenderId': item.sourceSenderId!,".to_string(),
                "        if (item.plainText != null) 'plainText': item.plainText!,".to_string(),
                "      }).toList(growable: false),".to_string(),
            ]),
            _ => {}
        }
    }
    lines
}

fn emit_dart_adapter_message_builder(spec: &Value) -> Option<String> {
    let entries = message_build_catalog_entries(spec);
    let models = message_builder_request_models(spec);
    let extra_methods = message_builder_extra_methods(spec);
    if entries.is_empty() {
        return None;
    }
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import '../../api/api.dart';".to_string(),
        "import '../../contract/bridge_contract.dart';".to_string(),
        "import '../../model/model.dart';".to_string(),
        "import '../codec/wire_codec.dart';".to_string(),
        String::new(),
        "final class DefaultMessageBuilderApi implements MessageBuilderApi {".to_string(),
        "  DefaultMessageBuilderApi(this._bridge);".to_string(),
        String::new(),
        "  final NativeBridge _bridge;".to_string(),
        String::new(),
        "  Future<Message> _dispatchBuildMap(Map<String, Object?> request) async {".to_string(),
        "    final raw = await _bridge.invoke<Map<String, Object?>>(NativeCallMap.messageBuilderDispatch, request);".to_string(),
        "    return messageFromJson(raw['message'] ?? raw);".to_string(),
        "  }".to_string(),
        String::new(),
        "  @override".to_string(),
        "  Future<ListMessageBuildCatalogResponse> listSupportedBuildOperations() async {".to_string(),
        "    return const ListMessageBuildCatalogResponse(entries: messageBuildCatalog);".to_string(),
        "  }".to_string(),
    ];
    for method in extra_methods {
        let req = str_field(method, "request");
        let res = dart_api_type(str_field(method, "response"), spec);
        let descriptor = format!(
            "NativeCallMap.{}",
            dart_native_key(str_field(method, "operation"))
        );
        let request_model = find_model(spec, req);
        let (arg, request_expr) = if matches!(req, "Unit" | "DisposeRequest") {
            (String::new(), String::new())
        } else if request_model.is_some() {
            (format!("{req} request"), ", requestMap".to_string())
        } else {
            (
                "Map<String, Object?> request".to_string(),
                ", request".to_string(),
            )
        };
        lines.extend([
            String::new(),
            "  @override".to_string(),
            format!(
                "  Future<{res}> {}({arg}) async {{",
                str_field(method, "name")
            ),
        ]);
        if let Some(model) = request_model {
            lines.push("    final requestMap = <String, Object?>{".to_string());
            lines.extend(dart_build_request_wire_lines(model));
            lines.push("    };".to_string());
        }
        if res == "void" {
            lines.push(format!(
                "    await _bridge.invoke<void>({descriptor}{request_expr});"
            ));
        } else if res == "Message" {
            lines.push(format!(
                "    final raw = await _bridge.invoke<Map<String, Object?>>({descriptor}{request_expr});"
            ));
            lines.push("    return messageFromJson(raw['message'] ?? raw);".to_string());
        } else if res == "Map<String, Object?>" {
            lines.push(format!(
                "    return _bridge.invoke<Map<String, Object?>>({descriptor}{request_expr});"
            ));
        } else if str_field(method, "response") == "RichDocV2Normalized" {
            lines.push(format!(
                "    final raw = await _bridge.invoke<Map<String, Object?>>({descriptor}{request_expr});"
            ));
            lines.push(format!(
                "    return {}(raw);",
                dart_model_from_json_fn(str_field(method, "response"))
            ));
        } else {
            lines.push(format!(
                "    return _bridge.invoke<{res}>({descriptor}{request_expr});"
            ));
        }
        lines.push("  }".to_string());
    }
    for entry in entries {
        let method = str_field(entry, "method");
        let request_type = str_field(entry, "request");
        let op = str_field(entry, "op");
        lines.extend([
            String::new(),
            "  @override".to_string(),
            format!("  Future<Message> {method}({request_type} request) => _dispatchBuildMap({{"),
            format!("      'op': {},", json_quote(op)),
        ]);
        if let Some(model) = models.get(request_type) {
            let wire_lines = dart_build_request_wire_lines(model);
            for (index, mut wire_line) in wire_lines.into_iter().enumerate() {
                if wire_line.trim_end().ends_with('!') && index + 1 < wire_lines_len(model) {
                    wire_line = format!("{},", wire_line.trim_end());
                }
                lines.push(wire_line);
            }
        }
        lines.push("    });".to_string());
    }
    lines.push("}".to_string());
    Some(lines.join("\n"))
}

pub(crate) fn emit_platform_adapter_files(root: &Path, check: bool) -> Result<()> {
    let spec = load_expanded_client_spec(root)?;
    let mut drifted = Vec::new();
    for target in platform_adapter_targets(root, &spec) {
        let body = single_trailing_newline(&target.body);
        upsert_text_file(&target.path, &body, check, &mut drifted)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned platform adapter output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned platform adapter artifacts generated");
    }
    Ok(())
}

fn platform_adapter_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let mut targets = Vec::new();
    // Root runtime composition is source-owned too. Keeping it in templates
    // prevents generated SessionApi growth from leaving native clients abstract.
    for (path, body) in [
        (
            "packages/flare-core-flutter-sdk/lib/src/adapter/default_flare_im_client.dart",
            include_str!("../../templates/flutter-adapter/client.dart"),
        ),
        (
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/DefaultFlareImClient.kt",
            include_str!("../../templates/android-adapter/client.kt"),
        ),
        (
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/DefaultFlareImClient.swift",
            include_str!("../../templates/apple-adapter/client.swift"),
        ),
        (
            "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/DefaultFlareImClient.ets",
            include_str!("../../templates/harmony-arkts-adapter/client.ets"),
        ),
        (
            "packages/flare-core-harmony-cangjie-sdk/src/adapter/DefaultFlareImClient.cj",
            include_str!("../../templates/harmony-cangjie-adapter/client.cj"),
        ),
    ] {
        targets.push(GeneratedTextTarget {
            path: root.join(path),
            body: body.to_string(),
        });
    }
    targets.extend(platform_wire_codec_targets(root));
    if let Some(builder) = emit_dart_adapter_message_builder(spec) {
        targets.push(GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-flutter-sdk/lib/src/adapter/module/default_message_builder_api.dart",
            ),
            body: builder,
        });
    }
    if let Some(connection_module) = find_module(spec, "connection") {
        targets.extend(platform_connection_adapter_targets(
            root,
            spec,
            connection_module,
        ));
    }
    targets.push(GeneratedTextTarget {
        path: root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultEventsApi.swift",
        ),
        body: include_str!("../../templates/apple-adapter/module/DefaultEventsApi.swift").to_string(),
    });
    targets.push(GeneratedTextTarget {
        path: root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultEventsApi.kt",
        ),
        body: include_str!("../../templates/android-adapter/module/DefaultEventsApi.kt").to_string(),
    });
    for key in platform_map_adapter_module_keys() {
        if let Some(module) = find_module(spec, key) {
            targets.extend(platform_map_adapter_targets(root, spec, module));
        }
    }
    targets.push(GeneratedTextTarget {
        path: root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Catalog/MessageBuildCatalog.swift",
        ),
        body: emit_swift_adapter_message_build_catalog(spec),
    });
    targets.push(GeneratedTextTarget {
        path: root.join(
            "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/catalog/MessageBuildCatalog.ets",
        ),
        body: emit_arkts_adapter_message_build_catalog(spec),
    });
    targets.push(GeneratedTextTarget {
        path: root.join(
            "packages/flare-core-harmony-cangjie-sdk/src/adapter/catalog/MessageBuildCatalog.cj",
        ),
        body: emit_cangjie_adapter_message_build_catalog(),
    });
    targets.push(GeneratedTextTarget {
        path: root.join(
            "packages/flare-core-harmony-cangjie-sdk/src/adapter/catalog/MessageBuildCatalogJson.cj",
        ),
        body: emit_cangjie_adapter_message_build_catalog_json(spec),
    });
    targets
}

fn platform_wire_codec_targets(root: &Path) -> Vec<GeneratedTextTarget> {
    vec![
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/codec/WireCodec.kt",
            ),
            body: include_str!("../../templates/android-adapter/codec/WireCodec.kt").to_string(),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-flutter-sdk/lib/src/adapter/codec/wire_codec.dart"),
            body: include_str!("../../templates/flutter-adapter/codec/wire_codec.dart").to_string(),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Codec/WireCodec.swift",
            ),
            body: include_str!("../../templates/apple-adapter/codec/WireCodec.swift").to_string(),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/codec/WireCodec.ets",
            ),
            body: include_str!("../../templates/harmony-arkts-adapter/codec/WireCodec.ets")
                .to_string(),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-harmony-cangjie-sdk/src/adapter/codec/WireCodec.cj",
            ),
            body: include_str!("../../templates/harmony-cangjie-adapter/codec/WireCodec.cj")
                .to_string(),
        },
    ]
}

fn platform_map_adapter_module_keys() -> &'static [&'static str] {
    &[
        "diagnostics",
        "presence",
        "media",
        "capabilities",
        "views",
        "message_builder",
    ]
}

fn find_module<'a>(spec: &'a Value, key: &str) -> Option<&'a Value> {
    child_arr(spec, "modules")
        .iter()
        .find(|module| str_field(module, "key") == key)
}

fn platform_connection_adapter_targets(
    root: &Path,
    spec: &Value,
    module: &Value,
) -> Vec<GeneratedTextTarget> {
    vec![
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultConnectionApi.kt",
            ),
            body: emit_kotlin_connection_adapter(module, spec),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultConnectionApi.swift",
            ),
            body: emit_swift_connection_adapter(module),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/module/DefaultConnectionApi.ets",
            ),
            body: emit_arkts_connection_adapter(module),
        },
        GeneratedTextTarget {
            path: root.join(
                "packages/flare-core-harmony-cangjie-sdk/src/adapter/module/DefaultConnectionApi.cj",
            ),
            body: emit_cangjie_connection_adapter(module, spec),
        },
    ]
}

fn platform_map_adapter_targets(
    root: &Path,
    spec: &Value,
    module: &Value,
) -> Vec<GeneratedTextTarget> {
    let iface = ts_api_interface_name(module);
    vec![
        GeneratedTextTarget {
            path: root
                .join("packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module")
                .join(format!("Default{iface}.kt")),
            body: emit_kotlin_map_adapter(spec, module),
        },
        GeneratedTextTarget {
            path: root
                .join("packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module")
                .join(format!("Default{iface}.swift")),
            body: emit_swift_map_adapter(spec, module),
        },
        GeneratedTextTarget {
            path: root
                .join("packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/module")
                .join(format!("Default{iface}.ets")),
            body: emit_arkts_map_adapter(spec, module),
        },
        GeneratedTextTarget {
            path: root
                .join("packages/flare-core-harmony-cangjie-sdk/src/adapter/module")
                .join(format!("Default{iface}.cj")),
            body: emit_cangjie_map_adapter(spec, module),
        },
    ]
}

fn kotlin_native_call_map_name(method: &Value) -> String {
    screaming_snake(str_field(method, "operation"))
}

fn swift_native_call_map_name(method: &Value) -> String {
    swift_identifier(&camel_const(str_field(method, "operation")))
}

fn arkts_native_call_map_name(method: &Value) -> String {
    camel_const(str_field(method, "operation"))
}

fn cangjie_native_call_map_name(method: &Value) -> String {
    cangjie_identifier(&camel_const(str_field(method, "operation")))
}

fn kotlin_adapter_arg(method: &Value, spec: &Value) -> String {
    let req = kotlin_api_type(str_field(method, "request"), spec);
    if req == "Unit" {
        String::new()
    } else {
        format!("request: {req}")
    }
}

fn kotlin_adapter_request_expr(method: &Value, spec: &Value) -> String {
    let req = str_field(method, "request");
    if kotlin_api_type(req, spec) == "Unit" {
        String::new()
    } else if is_known_ts_model_type(req, spec) {
        format!(", {}ToMap(request)", lower_first(req))
    } else {
        ", request".to_string()
    }
}

fn swift_adapter_arg(method: &Value, spec: &Value) -> String {
    let req = swift_api_type(str_field(method, "request"), spec);
    if req == "Void" {
        String::new()
    } else {
        format!("_ request: {req}")
    }
}

fn swift_adapter_map_request(method: &Value, spec: &Value) -> String {
    let req = str_field(method, "request");
    if swift_api_type(req, spec) == "Void" {
        "nil".to_string()
    } else if req == "SendMessageRequest" {
        "messageToWireMap(request.message)".to_string()
    } else if is_known_ts_model_type(req, spec) {
        format!("{}ToMap(request)", lower_first(req))
    } else {
        "unwrapRequest(AnySendable(request))".to_string()
    }
}

fn swift_adapter_void_request(method: &Value, spec: &Value) -> &'static str {
    if swift_api_type(str_field(method, "request"), spec) == "Void" {
        "nil"
    } else {
        "AnySendable(request)"
    }
}

fn arkts_adapter_arg(method: &Value, spec: &Value) -> String {
    let req = arkts_api_type(str_field(method, "request"), spec);
    if req == "void" {
        String::new()
    } else {
        format!("request: {req}")
    }
}

fn arkts_adapter_request_expr(method: &Value, spec: &Value) -> String {
    let req = str_field(method, "request");
    if arkts_api_type(req, spec) == "void" {
        String::new()
    } else if is_known_ts_model_type(req, spec) {
        format!(", {}ToMap(request)", lower_first(req))
    } else {
        ", request".to_string()
    }
}

fn cangjie_adapter_request_expr(method: &Value, spec: &Value) -> String {
    let req = str_field(method, "request");
    if cangjie_api_type(req, spec) == "Unit" {
        "wireEncodeRequest(\"{}\")".to_string()
    } else if is_known_ts_model_type(req, spec) {
        format!("wireEncodeRequest({}ToJson(request))", lower_first(req))
    } else {
        "wireEncodeRequest(requestJson)".to_string()
    }
}

fn emit_kotlin_map_adapter(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "package com.flare.im.adapter.module".to_string(),
        String::new(),
    ];
    lines.push("import com.flare.im.adapter.codec.*".to_string());
    lines.push("import com.flare.im.api.ConnectionState".to_string());
    lines.extend([
        format!(
            "import com.flare.im.api.{}.{}",
            kotlin_api_module_dir(module),
            iface
        ),
        "import com.flare.im.callback.*".to_string(),
        "import com.flare.im.contract.NativeBridge".to_string(),
        "import com.flare.im.contract.NativeCallMap".to_string(),
        "import com.flare.im.listener.*".to_string(),
    ]);
    lines.extend(kotlin_model_package_imports(spec));
    lines.extend([
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        format!("class Default{iface}("),
        "    private val bridge: NativeBridge,".to_string(),
        format!(") : {iface} {{"),
    ]);
    for method in child_arr(module, "methods") {
        let name = str_field(method, "name");
        let arg = kotlin_adapter_arg(method, spec);
        let req = kotlin_adapter_request_expr(method, spec);
        let res = kotlin_api_type(str_field(method, "response"), spec);
        let descriptor = kotlin_native_call_map_name(method);
        lines.push(String::new());
        lines.push(format!("    override suspend fun {name}({arg}): {res} {{"));
        match res.as_str() {
            "ListMessageBuildCatalogResponse" => lines.push("        return ListMessageBuildCatalogResponse(entries = MESSAGE_BUILD_CATALOG)".into()),
            "Message" => lines.extend([
                format!("        val raw = invokeMap(bridge, NativeCallMap.{descriptor}{req})"),
                "        return messageFromJson(raw[\"message\"] ?: raw)".into(),
            ]),
            "RichDocV2Normalized" => lines.push(format!("        return richDocV2NormalizedFromJson(invokeMap(bridge, NativeCallMap.{descriptor}{req}))")),
            "Unit" => lines.push(format!(
                "        invokeUnit(bridge, NativeCallMap.{descriptor}{req})"
            )),
            "Boolean" => lines.push(format!(
                "        return invokeBool(bridge, NativeCallMap.{descriptor}{req})"
            )),
            "ViewOpenResponse" => lines.push(format!(
                "        return viewOpenResponseFromJson(invokeMap(bridge, NativeCallMap.{descriptor}{req}))"
            )),
            "TimelineViewPageResponse" => lines.push(format!(
                "        return timelineViewPageResponseFromJson(invokeMap(bridge, NativeCallMap.{descriptor}{req}))"
            )),
            "CloseViewResponse" => lines.push(format!(
                "        return closeViewResponseFromJson(invokeMap(bridge, NativeCallMap.{descriptor}{req}))"
            )),
            "RuntimeHealthResponse" => lines.push(format!(
                "        return runtimeHealthResponseFromJson(invokeMap(bridge, NativeCallMap.{descriptor}{req}))"
            )),
            _ => lines.push(format!(
                "        return invokeMap(bridge, NativeCallMap.{descriptor}{req})"
            )),
        }
        lines.push("    }".to_string());
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_map_adapter(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        format!("public final class Default{iface}: {iface}Protocol {{"),
        "    private let bridge: any NativeBridgeProtocol".to_string(),
        String::new(),
        "    public init(bridge: any NativeBridgeProtocol) {".to_string(),
        "        self.bridge = bridge".to_string(),
        "    }".to_string(),
    ];
    for method in child_arr(module, "methods") {
        let name = swift_identifier(str_field(method, "name"));
        let arg = swift_adapter_arg(method, spec);
        let res = swift_api_type(str_field(method, "response"), spec);
        let descriptor = swift_native_call_map_name(method);
        lines.push(String::new());
        lines.push(format!(
            "    public func {name}({arg}) async throws -> {res} {{"
        ));
        match res.as_str() {
            "ListMessageBuildCatalogResponse" => lines.push("        return ListMessageBuildCatalogResponse(entries: messageBuildCatalog)".into()),
            "Message" | "RichDocV2Normalized" => lines.extend([
                format!("        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",swift_adapter_map_request(method,spec)),
                if res == "Message" { "        return try messageFromJson(raw[\"message\"] ?? raw)".into() } else { "        return try richDocV2NormalizedFromJson(raw)".into() },
            ]),
            "Void" => lines.push(format!(
                "        try await invokeVoid(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                swift_adapter_void_request(method, spec)
            )),
            "Bool" => lines.push(format!(
                "        return try await invokeBool(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                swift_adapter_void_request(method, spec)
            )),
            "ViewOpenResponse" => lines.extend([
                format!(
                    "        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                    swift_adapter_map_request(method, spec)
                ),
                "        return try viewOpenResponseFromJson(raw)".to_string(),
            ]),
            "TimelineViewPageResponse" => lines.extend([
                format!(
                    "        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                    swift_adapter_map_request(method, spec)
                ),
                "        return try timelineViewPageResponseFromJson(raw)".to_string(),
            ]),
            "CloseViewResponse" => lines.extend([
                format!(
                    "        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                    swift_adapter_map_request(method, spec)
                ),
                "        return try closeViewResponseFromJson(raw)".to_string(),
            ]),
            "RuntimeHealthResponse" => lines.extend([
                format!(
                    "        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                    swift_adapter_map_request(method, spec)
                ),
                "        return try runtimeHealthResponseFromJson(raw)".to_string(),
            ]),
            _ => lines.push(format!(
                "        return try await invokeMap(bridge, descriptor: NativeCallMap.{descriptor}, request: {})",
                swift_adapter_map_request(method, spec)
            )),
        }
        lines.push("    }".to_string());
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_map_adapter(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let key = ts_api_module_key(module);
    let model_types = child_arr(module, "methods")
        .iter()
        .flat_map(|method| [str_field(method, "request"), str_field(method, "response")])
        .filter(|type_name| is_known_ts_model_type(type_name, spec))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut wire_imports = child_arr(module, "methods")
        .iter()
        .filter_map(|method| {
            let req = str_field(method, "request");
            is_known_ts_model_type(req, spec).then(|| format!("{}ToMap", lower_first(req)))
        })
        .collect::<BTreeSet<_>>();
    for method in child_arr(module, "methods") {
        match str_field(method, "response") {
            "ViewOpenResponse"
            | "TimelineViewPageResponse"
            | "CloseViewResponse"
            | "Message"
            | "RichDocV2Normalized" => {
                wire_imports.insert(ts_model_from_json_fn(str_field(method, "response")));
            }
            _ => {}
        }
    }
    let method_responses = child_arr(module, "methods")
        .iter()
        .map(|method| arkts_api_type(str_field(method, "response"), spec))
        .collect::<Vec<_>>();
    let mut invoke_imports = Vec::new();
    if method_responses.iter().any(|res| res == "boolean") {
        invoke_imports.push("invokeBool");
    }
    if method_responses
        .iter()
        .any(|res| !matches!(res.as_str(), "boolean" | "void"))
    {
        invoke_imports.push("invokeMap");
    }
    if method_responses.iter().any(|res| res == "void") {
        invoke_imports.push("invokeVoid");
    }
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { NativeBridge, NativeCallMap } from '../../contract/BridgeContract';".to_string(),
        format!("import type {{ {iface} }} from '../../api/modules/{key}';"),
    ];
    if ts_api_module_key(module) == "message_builder" {
        lines
            .push("import { MESSAGE_BUILD_CATALOG } from '../catalog/MessageBuildCatalog';".into());
    }
    if !model_types.is_empty() {
        lines.push(format!(
            "import type {{ {} }} from '../../model';",
            model_types.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    lines.push(format!(
        "import {{ {} }} from '../codec/NativeInvoke';",
        invoke_imports.join(", ")
    ));
    if !wire_imports.is_empty() {
        lines.push(format!(
            "import {{ {} }} from '../codec/WireCodec';",
            wire_imports.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    lines.extend([
        String::new(),
        format!("export class Default{iface} implements {iface} {{"),
        "  constructor(private readonly bridge: NativeBridge) {}".to_string(),
    ]);
    for method in child_arr(module, "methods") {
        let name = str_field(method, "name");
        let arg = arkts_adapter_arg(method, spec);
        let req = arkts_adapter_request_expr(method, spec);
        let res = arkts_api_type(str_field(method, "response"), spec);
        let descriptor = arkts_native_call_map_name(method);
        lines.push(String::new());
        lines.push(format!("  async {name}({arg}): Promise<{res}> {{"));
        match res.as_str() {
            "ListMessageBuildCatalogResponse" => {
                lines.push("    return { entries: MESSAGE_BUILD_CATALOG };".into())
            }
            "Message" | "RichDocV2Normalized" => lines.extend([
                format!(
                    "    const raw = await invokeMap(this.bridge, NativeCallMap.{descriptor}{req});"
                ),
                if res == "Message" {
                    "    return messageFromJson(raw['message'] ?? raw);".into()
                } else {
                    "    return richDocV2NormalizedFromJson(raw);".into()
                },
            ]),
            "void" => lines.push(format!(
                "    await invokeVoid(this.bridge, NativeCallMap.{descriptor}{req});"
            )),
            "boolean" => lines.push(format!(
                "    return await invokeBool(this.bridge, NativeCallMap.{descriptor}{req});"
            )),
            "ViewOpenResponse" => lines.extend([
                format!(
                    "    const raw = await invokeMap(this.bridge, NativeCallMap.{descriptor}{req});"
                ),
                "    return viewOpenResponseFromJson(raw);".to_string(),
            ]),
            "TimelineViewPageResponse" => lines.extend([
                format!(
                    "    const raw = await invokeMap(this.bridge, NativeCallMap.{descriptor}{req});"
                ),
                "    return timelineViewPageResponseFromJson(raw);".to_string(),
            ]),
            "CloseViewResponse" => lines.extend([
                format!(
                    "    const raw = await invokeMap(this.bridge, NativeCallMap.{descriptor}{req});"
                ),
                "    return closeViewResponseFromJson(raw);".to_string(),
            ]),
            _ => lines.push(format!(
                "    return await invokeMap(this.bridge, NativeCallMap.{descriptor}{req});"
            )),
        }
        lines.push("  }".to_string());
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_cangjie_map_adapter(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.adapter".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.contract.*".to_string(),
        "import flare_core_harmony_cangjie_sdk.api.modules.*".to_string(),
        "import flare_core_harmony_cangjie_sdk.model.*".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        format!("public class Default{iface} <: {iface} {{"),
        "    private let bridge: NativeBridge".to_string(),
        String::new(),
        "    public init(bridge: NativeBridge) {".to_string(),
        "        this.bridge = bridge".to_string(),
        "    }".to_string(),
    ];
    for method in child_arr(module, "methods") {
        let name = cangjie_identifier(str_field(method, "name"));
        let arg = cangjie_api_arg(method, spec);
        let res = cangjie_api_type(str_field(method, "response"), spec);
        let descriptor = cangjie_native_call_map_name(method);
        let request = cangjie_adapter_request_expr(method, spec);
        lines.push(String::new());
        lines.push(format!("    public func {name}({arg}): {res} {{"));
        match res.as_str() {
            "ListMessageBuildCatalogResponse" => lines.push("        return ListMessageBuildCatalogResponse(entries: MESSAGE_BUILD_CATALOG_ENTRIES_JSON)".into()),
            "Message" | "RichDocV2Normalized" => lines.extend([
                format!("        let raw = bridge.invoke(descriptor: NativeCallMap.{descriptor}, requestJson: {request})"),
                if res == "Message" { "        return messageFromEnvelopeJson(raw)".into() } else { "        return richDocV2NormalizedFromJson(raw)".into() },
            ]),
            "Unit" => lines.push(format!(
                "        let _ignored = bridge.invoke(descriptor: NativeCallMap.{descriptor}, requestJson: {request})"
            )),
            "BooleanResponse" => lines.push(format!(
                "        return booleanResponseFromWire(bridge.invoke(descriptor: NativeCallMap.{descriptor}, requestJson: {request}))"
            )),
            "ConnectionState" => {
                lines.push(format!(
                    "        let raw = bridge.invoke(descriptor: NativeCallMap.{descriptor}, requestJson: {request})"
                ));
                lines.push("        return connectionStateFromWire(raw)".to_string());
            }
            _ => lines.push(format!(
                "        return bridge.invoke(descriptor: NativeCallMap.{descriptor}, requestJson: {request})"
            )),
        }
        lines.push("    }".to_string());
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_kotlin_connection_adapter(module: &Value, _spec: &Value) -> String {
    let mut lines = vec![
        "package com.flare.im.adapter.module".to_string(),
        String::new(),
        "import com.flare.im.adapter.codec.*".to_string(),
        "import com.flare.im.api.ConnectionState".to_string(),
        "import com.flare.im.api.connection.ConnectionApi".to_string(),
        "import com.flare.im.contract.NativeBridge".to_string(),
        "import com.flare.im.contract.NativeCallMap".to_string(),
        "import com.flare.im.model.command.NetworkChangeRequest".to_string(),
        "import com.flare.im.model.response.*".to_string(),
        String::new(),
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "class DefaultConnectionApi(".to_string(),
        "    private val bridge: NativeBridge,".to_string(),
        ") : ConnectionApi {".to_string(),
    ];
    for method in child_arr(module, "methods") {
        lines.push(String::new());
        match str_field(method, "response") {
            "ConnectionStateResponse" => lines.extend([
                format!(
                    "    override suspend fun {}(): ConnectionState {{",
                    str_field(method, "name")
                ),
                format!(
                    "        return invokeConnectionState(bridge, NativeCallMap.{})",
                    kotlin_native_call_map_name(method)
                ),
                "    }".to_string(),
            ]),
            "Unit" => lines.extend([
                format!(
                    "    override suspend fun {}(): Unit {{",
                    str_field(method, "name")
                ),
                format!(
                    "        invokeUnit(bridge, NativeCallMap.{})",
                    kotlin_native_call_map_name(method)
                ),
                "    }".to_string(),
            ]),
            "NetworkChangeResponse" => lines.extend([
                format!(
                    "    override suspend fun {}(request: NetworkChangeRequest): NetworkChangeResponse {{",
                    str_field(method, "name")
                ),
                format!(
                    "        return networkChangeResponseFromJson(invokeMap(bridge, NativeCallMap.{}, networkChangeRequestToMap(request)))",
                    kotlin_native_call_map_name(method)
                ),
                "    }".to_string(),
            ]),
            // Any other response is a model read back through its codec, `<model>FromJson`.
            other => lines.extend([
                format!(
                    "    override suspend fun {}(): {other} {{",
                    str_field(method, "name")
                ),
                format!(
                    "        return {}FromJson(invokeMap(bridge, NativeCallMap.{}))",
                    lower_first(other),
                    kotlin_native_call_map_name(method)
                ),
                "    }".to_string(),
            ]),
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_connection_adapter(module: &Value) -> String {
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        "public final class DefaultConnectionApi: ConnectionApiProtocol {".to_string(),
        "    private let bridge: any NativeBridgeProtocol".to_string(),
        String::new(),
        "    public init(bridge: any NativeBridgeProtocol) {".to_string(),
        "        self.bridge = bridge".to_string(),
        "    }".to_string(),
    ];
    for method in child_arr(module, "methods") {
        lines.push(String::new());
        match str_field(method, "response") {
            "ConnectionStateResponse" => lines.extend([
                format!(
                    "    public func {}() async throws -> ConnectionState {{",
                    swift_identifier(str_field(method, "name"))
                ),
                format!(
                    "        return try await invokeConnectionState(bridge, descriptor: NativeCallMap.{}, request: nil)",
                    swift_native_call_map_name(method)
                ),
                "    }".to_string(),
            ]),
            "NetworkChangeResponse" => lines.extend([
                format!(
                    "    public func {}(_ request: NetworkChangeRequest) async throws -> NetworkChangeResponse {{",
                    swift_identifier(str_field(method, "name"))
                ),
                format!(
                    "        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{}, request: unwrapRequest(AnySendable(networkChangeRequestToMap(request))))",
                    swift_native_call_map_name(method)
                ),
                "        return try networkChangeResponseFromJson(raw)".to_string(),
                "    }".to_string(),
            ]),
            "Unit" => lines.extend([
                format!(
                    "    public func {}() async throws -> Void {{",
                    swift_identifier(str_field(method, "name"))
                ),
                format!(
                    "        try await invokeVoid(bridge, descriptor: NativeCallMap.{}, request: nil)",
                    swift_native_call_map_name(method)
                ),
                "    }".to_string(),
            ]),
            // Any other response is a model read back through its codec, `<model>FromJson`.
            other => lines.extend([
                format!(
                    "    public func {}() async throws -> {other} {{",
                    swift_identifier(str_field(method, "name"))
                ),
                format!(
                    "        let raw = try await invokeMap(bridge, descriptor: NativeCallMap.{}, request: nil)",
                    swift_native_call_map_name(method)
                ),
                format!("        return try {}FromJson(raw)", lower_first(other)),
                "    }".to_string(),
            ]),
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_arkts_connection_adapter(module: &Value) -> String {
    // Responses read back through their codec (`<model>FromJson`), besides the ones handled below.
    let decoded = child_arr(module, "methods")
        .iter()
        .map(|method| str_field(method, "response"))
        .filter(|response| {
            !matches!(
                *response,
                "ConnectionStateResponse" | "Unit" | "NetworkChangeResponse"
            )
        })
        .collect::<BTreeSet<_>>();
    let model_imports = std::iter::once("NetworkChangeRequest")
        .chain(std::iter::once("NetworkChangeResponse"))
        .chain(decoded.iter().copied())
        .collect::<Vec<_>>()
        .join(", ");
    let codec_imports = [
        "networkChangeRequestToMap".to_string(),
        "networkChangeResponseFromJson".to_string(),
    ]
    .into_iter()
    .chain(
        decoded
            .iter()
            .map(|response| format!("{}FromJson", lower_first(response))),
    )
    .collect::<Vec<_>>()
    .join(", ");
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { NativeBridge, NativeCallMap } from '../../contract/BridgeContract';".to_string(),
        "import type { ConnectionApi } from '../../api/modules/connection';".to_string(),
        "import type { ConnectionState } from '../../api/client';".to_string(),
        format!("import type {{ {model_imports} }} from '../../model';"),
        "import { invokeConnectionState, invokeMap, invokeVoid } from '../codec/NativeInvoke';"
            .to_string(),
        format!("import {{ {codec_imports} }} from '../codec/WireCodec';"),
        String::new(),
        "export class DefaultConnectionApi implements ConnectionApi {".to_string(),
        "  constructor(private readonly bridge: NativeBridge) {}".to_string(),
    ];
    for method in child_arr(module, "methods") {
        lines.push(String::new());
        match str_field(method, "response") {
            "ConnectionStateResponse" => lines.extend([
                format!(
                    "  async {}(): Promise<ConnectionState> {{",
                    str_field(method, "name")
                ),
                format!(
                    "    return await invokeConnectionState(this.bridge, NativeCallMap.{});",
                    arkts_native_call_map_name(method)
                ),
                "  }".to_string(),
            ]),
            "Unit" => lines.extend([
                format!("  async {}(): Promise<void> {{", str_field(method, "name")),
                format!(
                    "    await invokeVoid(this.bridge, NativeCallMap.{});",
                    arkts_native_call_map_name(method)
                ),
                "  }".to_string(),
            ]),
            "NetworkChangeResponse" => lines.extend([
                format!(
                    "  async {}(request: NetworkChangeRequest): Promise<NetworkChangeResponse> {{",
                    str_field(method, "name")
                ),
                format!(
                    "    const raw = await invokeMap(this.bridge, NativeCallMap.{}, networkChangeRequestToMap(request));",
                    arkts_native_call_map_name(method)
                ),
                "    return networkChangeResponseFromJson(raw);".to_string(),
                "  }".to_string(),
            ]),
            other => lines.extend([
                format!(
                    "  async {}(): Promise<{other}> {{",
                    str_field(method, "name")
                ),
                format!(
                    "    const raw = await invokeMap(this.bridge, NativeCallMap.{});",
                    arkts_native_call_map_name(method)
                ),
                format!("    return {}FromJson(raw);", lower_first(other)),
                "  }".to_string(),
            ]),
        }
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_cangjie_connection_adapter(module: &Value, spec: &Value) -> String {
    let mut lines = vec![
        "package flare_core_harmony_cangjie_sdk.adapter".to_string(),
        String::new(),
        "import flare_core_harmony_cangjie_sdk.contract.*".to_string(),
        "import flare_core_harmony_cangjie_sdk.api.modules.*".to_string(),
        "import flare_core_harmony_cangjie_sdk.model.*".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        "public class DefaultConnectionApi <: ConnectionApi {".to_string(),
        "    private let bridge: NativeBridge".to_string(),
        String::new(),
        "    public init(bridge: NativeBridge) {".to_string(),
        "        this.bridge = bridge".to_string(),
        "    }".to_string(),
    ];
    for method in child_arr(module, "methods") {
        let return_ty = cangjie_api_type(str_field(method, "response"), spec);
        let arg = cangjie_api_arg(method, spec);
        lines.push(String::new());
        lines.push(format!(
            "    public func {}({}): {} {{",
            cangjie_identifier(str_field(method, "name")),
            arg,
            return_ty
        ));
        match str_field(method, "response") {
            "ConnectionStateResponse" => {
                lines.push(format!(
                    "        let raw = bridge.invoke(descriptor: NativeCallMap.{}, requestJson: wireEncodeRequest(\"{{}}\"))",
                    cangjie_native_call_map_name(method)
                ));
                lines.push("        return connectionStateFromWire(raw)".to_string());
            }
            "Unit" => {
                lines.push(format!(
                    "        let _ignored = bridge.invoke(descriptor: NativeCallMap.{}, requestJson: wireEncodeRequest(\"{{}}\"))",
                    cangjie_native_call_map_name(method)
                ));
            }
            "NetworkChangeResponse" => {
                lines.push(format!(
                    "        let raw = bridge.invoke(descriptor: NativeCallMap.{}, requestJson: wireEncodeRequest(networkChangeRequestToJson(request)))",
                    cangjie_native_call_map_name(method)
                ));
                lines.push("        return networkChangeResponseFromJson(raw)".to_string());
            }
            // Any other response is a model read back through its codec, `<model>FromJson`.
            other => {
                lines.push(format!(
                    "        let raw = bridge.invoke(descriptor: NativeCallMap.{}, requestJson: wireEncodeRequest(\"{{}}\"))",
                    cangjie_native_call_map_name(method)
                ));
                lines.push(format!(
                    "        return {}FromJson(raw)",
                    lower_first(other)
                ));
            }
        }
        lines.push("    }".to_string());
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_swift_adapter_message_build_catalog(spec: &Value) -> String {
    let mut lines = vec![
        "import Foundation".to_string(),
        String::new(),
        "/// GENERATED. Do not edit by hand.".to_string(),
        "let messageBuildCatalog: [MessageBuildCatalogEntry] = [".to_string(),
    ];
    for entry in child_arr(
        spec.get("messageBuildCatalog").unwrap_or(&Value::Null),
        "entries",
    ) {
        lines.push(format!(
            "    MessageBuildCatalogEntry(op: .{}, method: {}, requestType: {}, contentType: .{}, messageType: {}, summary: {}, stability: {}),",
            swift_identifier(&lower_first(&pascal_case(str_field(entry, "op")))),
            json_quote(str_field(entry, "method")),
            json_quote(str_field(entry, "request")),
            swift_identifier(&lower_first(&pascal_case(str_field(entry, "contentType")))),
            entry.get("messageType").and_then(Value::as_i64).unwrap_or_default(),
            json_quote(str_field(entry, "summary")),
            json_quote(entry.get("stability").and_then(Value::as_str).unwrap_or("stable")),
        ));
    }
    lines.push("]".to_string());
    lines.join("\n")
}

fn emit_arkts_adapter_message_build_catalog(spec: &Value) -> String {
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { MessageBuildCatalogEntry, MessageBuildOp, MessageContentType } from '../../model';"
            .to_string(),
        String::new(),
        "/** All supported quick-build operations for MessageBuilderApi. */".to_string(),
        "export const MESSAGE_BUILD_CATALOG: MessageBuildCatalogEntry[] = [".to_string(),
    ];
    for entry in child_arr(
        spec.get("messageBuildCatalog").unwrap_or(&Value::Null),
        "entries",
    ) {
        lines.push(format!(
            "  {{ op: MessageBuildOp.{}, method: {}, requestType: {}, contentType: MessageContentType.{}, messageType: {}, summary: {}, stability: {} }},",
            pascal_case(str_field(entry, "op")),
            json_quote(str_field(entry, "method")),
            json_quote(str_field(entry, "request")),
            pascal_case(str_field(entry, "contentType")),
            entry.get("messageType").and_then(Value::as_i64).unwrap_or_default(),
            json_quote(str_field(entry, "summary")),
            json_quote(entry.get("stability").and_then(Value::as_str).unwrap_or("stable")),
        ));
    }
    lines.push("];".to_string());
    lines.join("\n")
}

fn cangjie_catalog_entry_json(entry: &Value) -> Value {
    json!({
        "op": str_field(entry, "op"),
        "method": str_field(entry, "method"),
        "requestType": str_field(entry, "request"),
        "contentType": str_field(entry, "contentType"),
        "messageType": entry.get("messageType").and_then(Value::as_i64).unwrap_or_default(),
        "summary": str_field(entry, "summary"),
        "stability": entry.get("stability").and_then(Value::as_str).unwrap_or("stable"),
    })
}

fn emit_cangjie_adapter_message_build_catalog() -> String {
    [
        "package flare_core_harmony_cangjie_sdk.adapter",
        "",
        "// GENERATED. Do not edit by hand.",
        "public func messageBuildCatalogJson(): String {",
        "    return MESSAGE_BUILD_CATALOG_JSON",
        "}",
        "",
        "public func messageBuildCatalogEntriesJson(): String {",
        "    return MESSAGE_BUILD_CATALOG_ENTRIES_JSON",
        "}",
    ]
    .join("\n")
}

fn emit_cangjie_adapter_message_build_catalog_json(spec: &Value) -> String {
    let entries = child_arr(
        spec.get("messageBuildCatalog").unwrap_or(&Value::Null),
        "entries",
    )
    .iter()
    .map(cangjie_catalog_entry_json)
    .collect::<Vec<_>>();
    let catalog = json!({ "entries": entries });
    let catalog_json = serde_json::to_string(&catalog).expect("catalog json serialization failed");
    let entries_json = serde_json::to_string(catalog.get("entries").unwrap_or(&Value::Null))
        .expect("catalog entries json serialization failed");
    [
        "package flare_core_harmony_cangjie_sdk.adapter".to_string(),
        String::new(),
        "// GENERATED. Do not edit by hand.".to_string(),
        "// Full JSON object for raw bridge consumers.".to_string(),
        format!(
            "public let MESSAGE_BUILD_CATALOG_JSON: String = {}",
            json_quote(&catalog_json)
        ),
        String::new(),
        "// JSON array matching ListMessageBuildCatalogResponse.entries.".to_string(),
        format!(
            "public let MESSAGE_BUILD_CATALOG_ENTRIES_JSON: String = {}",
            json_quote(&entries_json)
        ),
    ]
    .join("\n")
}

fn wire_lines_len(model: &Value) -> usize {
    dart_build_request_wire_lines(model).len()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dart_capability_template_covers_the_source_spec() {
        let root = crate::context::workspace_root().unwrap();
        let spec = load_expanded_client_spec(&root).unwrap();
        let module = find_module(&spec, "capabilities").unwrap();
        let template = include_str!("../../templates/flutter-adapter/client.dart");
        for method in child_arr(module, "methods") {
            let name = str_field(method, "name");
            assert!(
                template.contains(&format!(" {name}(")),
                "Dart runtime adapter is missing {name}"
            );
        }
    }
    #[test]
    fn native_message_builder_adapters_cover_every_contract_method() {
        let root = crate::context::workspace_root().unwrap();
        let spec = load_expanded_client_spec(&root).unwrap();
        let module = find_module(&spec, "message_builder").unwrap();
        let targets = platform_map_adapter_targets(&root, &spec, module);
        assert_eq!(targets.len(), 4);
        for target in targets {
            for method in child_arr(module, "methods") {
                assert!(
                    target
                        .body
                        .contains(&format!("{}(", str_field(method, "name"))),
                    "{} missing {}",
                    target.path.display(),
                    str_field(method, "name")
                );
            }
        }
    }
}
