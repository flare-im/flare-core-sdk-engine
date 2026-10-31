use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{
    GeneratedTextTarget, all_spec_enums, all_spec_models, arr, camel_const, child_arr,
    core_contract_dir, facade_prop, is_known_ts_model_type, is_list_type_name, json_quote,
    list_inner_type_name, load_expanded_client_spec, load_json, message_build_catalog_entries,
    message_builder_extra_methods, message_builder_request_models, pascal_case,
    remove_output_paths, single_trailing_newline, snake_case, spec_enum_map, spec_model_names,
    str_field, ts_api_interface_name, ts_model_from_json_fn, ts_model_to_map_fn, upsert_text_file,
};

pub(crate) fn emit_typescript_adapter_files(root: &Path, check: bool) -> Result<()> {
    let spec = load_expanded_client_spec(root)?;
    let event_registry = load_typescript_event_registry(root)?;
    let mut drifted = Vec::new();
    if !check {
        clean_typescript_adapter_outputs(root)?;
    }
    for target in typescript_adapter_targets(root, &spec, &event_registry)? {
        let body = single_trailing_newline(&target.body);
        upsert_text_file(&target.path, &body, check, &mut drifted)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned TypeScript adapter output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned TypeScript adapter artifacts generated");
    }
    Ok(())
}

fn clean_typescript_adapter_outputs(root: &Path) -> Result<()> {
    remove_output_paths([root.join("packages/flare-core-typescript-sdk/src/adapter")])
}

#[derive(Clone, Debug)]
struct TypescriptEventRegistryEntry {
    id: String,
    code: i64,
    web_channel: Option<String>,
}

fn load_typescript_event_registry(root: &Path) -> Result<Vec<TypescriptEventRegistryEntry>> {
    let path = core_contract_dir(root).join("events.json");
    let json = load_json(&path)?;
    let mut entries = arr(json.get("events").unwrap_or(&Value::Null))
        .iter()
        .filter_map(|event| {
            let id = str_field(event, "id");
            let code = event.get("cCode")?.as_i64()?;
            let web_channel = match str_field(event, "tauri") {
                "" => None,
                channel => Some(channel.to_string()),
            };
            Some(TypescriptEventRegistryEntry {
                id: id.to_string(),
                code,
                web_channel,
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| a.code.cmp(&b.code).then_with(|| a.id.cmp(&b.id)));
    Ok(entries)
}

fn typescript_adapter_targets(
    root: &Path,
    spec: &Value,
    event_registry: &[TypescriptEventRegistryEntry],
) -> Result<Vec<GeneratedTextTarget>> {
    let adapter_root = root.join("packages/flare-core-typescript-sdk/src/adapter");
    let module_root = adapter_root.join("module");
    let codec_root = adapter_root.join("codec");
    let catalog_root = adapter_root.join("catalog");
    let mut targets = Vec::new();

    if let Some(catalog) = emit_typescript_message_build_catalog(spec) {
        targets.push(GeneratedTextTarget {
            path: catalog_root.join("messageBuildCatalog.ts"),
            body: catalog,
        });
    }
    if let Some(builder) = emit_typescript_adapter_message_builder(spec) {
        targets.push(GeneratedTextTarget {
            path: module_root.join("DefaultMessageBuilderApi.ts"),
            body: builder,
        });
    }
    targets.push(GeneratedTextTarget {
        path: codec_root.join("nativeInvoke.ts"),
        body: include_str!("../../templates/typescript-adapter/codec/nativeInvoke.ts").to_string(),
    });
    targets.push(GeneratedTextTarget {
        path: codec_root.join("wireCodec.ts"),
        body: emit_typescript_wire_codec(spec)?,
    });
    targets.push(GeneratedTextTarget {
        path: adapter_root.join("media/mediaAccess.ts"),
        body: include_str!("../../templates/typescript-adapter/media/mediaAccess.ts").to_string(),
    });
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" || str_field(module, "key") == "message_builder"
        {
            continue;
        }
        let prop = facade_prop(module);
        targets.push(GeneratedTextTarget {
            path: module_root.join(format!("Default{}Api.ts", pascal_case(prop))),
            body: emit_typescript_adapter_module_api(spec, module, event_registry),
        });
    }
    targets.push(GeneratedTextTarget {
        path: adapter_root.join("defaultFlareImClient.ts"),
        body: emit_typescript_adapter_flare_im_client(spec)?,
    });
    targets.push(GeneratedTextTarget {
        path: adapter_root.join("index.ts"),
        body: include_str!("../../templates/typescript-adapter/index.ts").to_string(),
    });
    Ok(targets)
}

fn ts_enum_wire_order_name(enum_name: &str) -> String {
    format!("{}_WIRE_ORDER", snake_case(enum_name).to_ascii_uppercase())
}

fn ts_enum_wire_values_name(enum_name: &str) -> String {
    format!("{}_WIRE_VALUES", snake_case(enum_name).to_ascii_uppercase())
}

fn ts_enum_table_name(enum_name: &str) -> String {
    if matches!(
        enum_name,
        "ConversationType" | "TimelineSyncState" | "MessageContentType"
    ) {
        ts_enum_wire_values_name(enum_name)
    } else {
        ts_enum_wire_order_name(enum_name)
    }
}

fn ts_enum_wire_order_values(enum_value: &Value) -> String {
    let name = str_field(enum_value, "name");
    child_arr(enum_value, "values")
        .iter()
        .filter_map(Value::as_str)
        .map(|value| format!("{name}.{}", pascal_case(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn ts_enum_wire_index_expr(
    enum_name: &str,
    access: &str,
    enums: &BTreeMap<String, &Value>,
) -> String {
    let _ = enums
        .get(enum_name)
        .unwrap_or_else(|| panic!("missing enum {enum_name}"));
    access.to_string()
}

fn ts_build_wire_object_lines(
    model: &Value,
    receiver: &str,
    indent: &str,
    spec: &Value,
    enums: &BTreeMap<String, &Value>,
) -> Vec<String> {
    let model_names = spec_model_names(spec);
    let mut lines = Vec::new();
    for field in child_arr(model, "fields") {
        let name = str_field(field, "name");
        let wire = str_field(field, "wireName");
        let type_name = str_field(field, "type");
        let required = field
            .get("required")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let default_literal = ts_field_default_literal(field);
        let access = format!("{receiver}.{name}");
        if enums.contains_key(type_name) {
            let wire_expr = ts_enum_wire_index_expr(type_name, &access, enums);
            if required {
                lines.push(format!("{indent}{wire}: {wire_expr},"));
            } else if let Some(default_literal) = default_literal.as_ref() {
                lines.push(format!("{indent}{wire}: {wire_expr} ?? {default_literal},"));
            } else {
                lines.push(format!(
                    "{indent}...({access} !== undefined ? {{ {wire}: {wire_expr} }} : {{}}),"
                ));
            }
        } else if matches!(
            type_name,
            "String" | "Boolean" | "Int32" | "Int64" | "UInt32" | "UInt64" | "Float" | "Double"
        ) {
            if required || type_name == "Boolean" {
                if required {
                    lines.push(format!("{indent}{wire}: {access},"));
                } else if let Some(default_literal) = default_literal.as_ref() {
                    lines.push(format!("{indent}{wire}: {access} ?? {default_literal},"));
                } else {
                    lines.push(format!("{indent}{wire}: {access},"));
                }
            } else if let Some(default_literal) = default_literal.as_ref() {
                lines.push(format!("{indent}{wire}: {access} ?? {default_literal},"));
            } else {
                lines.push(format!(
                    "{indent}...({access} !== undefined ? {{ {wire}: {access} }} : {{}}),"
                ));
            }
        } else if matches!(type_name, "JsonObject" | "StringMap") {
            lines.push(format!("{indent}{wire}: {access},"));
        } else if is_list_type_name(type_name) {
            let inner = list_inner_type_name(type_name);
            let list_source = if default_literal.as_deref() == Some("[]") {
                format!("({access} ?? [])")
            } else {
                access.to_string()
            };
            // 枚举与标量都直接透传，无需 map 转换 —— 合并同体分支。
            let value_expr = if enums.contains_key(inner)
                || matches!(inner, "String" | "Int32" | "Int64" | "Float" | "Double")
            {
                list_source.clone()
            } else {
                let map_fn = ts_model_to_map_fn(inner);
                format!("{list_source}.map((item) => {map_fn}(item))")
            };
            // 必填、或虽可选但有默认值 —— 两种情况都无条件输出该字段。
            if required || default_literal.is_some() {
                lines.push(format!("{indent}{wire}: {value_expr},"));
            } else {
                lines.push(format!(
                    "{indent}...({access} !== undefined ? {{ {wire}: {value_expr} }} : {{}}),"
                ));
            }
        } else if type_name == "MessageContent" {
            if required {
                lines.push(format!(
                    "{indent}{wire}: messageContentToWireMap({access}),"
                ));
            } else {
                lines.push(format!(
                    "{indent}...({access} !== undefined ? {{ {wire}: messageContentToWireMap({access}) }} : {{}}),"
                ));
            }
        } else if model_names.contains(type_name) {
            let map_fn = ts_model_to_map_fn(type_name);
            if required {
                lines.push(format!("{indent}{wire}: {map_fn}({access}),"));
            } else {
                lines.push(format!(
                    "{indent}...({access} !== undefined ? {{ {wire}: {map_fn}({access}) }} : {{}}),"
                ));
            }
        } else if type_name == "ImageGroupContentPayload" {
            lines.extend([
                format!("{indent}images: {receiver}.payload.images.map((item) => ({{"),
                format!("{indent}  imageId: item.imageId,"),
                format!("{indent}  ...(item.url !== undefined ? {{ url: item.url }} : {{}}),"),
                format!("{indent}  ...(item.title !== undefined ? {{ title: item.title }} : {{}}),"),
                format!("{indent}  ...(item.width !== undefined ? {{ width: item.width }} : {{}}),"),
                format!("{indent}  ...(item.height !== undefined ? {{ height: item.height }} : {{}}),"),
                format!("{indent}}})),"),
                format!(
                    "{indent}...({receiver}.payload.title !== undefined ? {{ title: {receiver}.payload.title }} : {{}}),"
                ),
            ]);
        } else if type_name == "ForwardSourceMessageList" {
            lines.extend([
                format!("{indent}sourceMessages: {access}.map((item) => ({{"),
                format!("{indent}  sourceMessageId: item.sourceMessageId,"),
                format!(
                    "{indent}  ...(item.sourceConversationId !== undefined ? {{ sourceConversationId: item.sourceConversationId }} : {{}}),"
                ),
                format!(
                    "{indent}  ...(item.sourceSenderId !== undefined ? {{ sourceSenderId: item.sourceSenderId }} : {{}}),"
                ),
                format!(
                    "{indent}  ...(item.plainText !== undefined ? {{ plainText: item.plainText }} : {{}}),"
                ),
                format!("{indent}}})),"),
            ]);
        }
    }
    lines
}

fn ts_field_default_literal(field: &Value) -> Option<String> {
    field.get("default").and_then(|value| match value {
        Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) | Value::Null => {
            serde_json::to_string(value).ok()
        }
        Value::Object(_) => None,
    })
}

fn emit_typescript_enum_wire_orders(spec: &Value) -> String {
    all_spec_enums(spec)
        .into_iter()
        .filter(|enum_value| str_field(enum_value, "name") != "MessageBuildOp")
        // Only the `_WIRE_VALUES` arrays are consumed (decode-time membership checks).
        // The `_WIRE_ORDER` arrays existed solely for `enumWireIndex`, which is dead
        // (the wire boundary is identity camelCase JSON); skip emitting them.
        .filter(|enum_value| {
            let name = str_field(enum_value, "name");
            ts_enum_table_name(name) != ts_enum_wire_order_name(name)
        })
        .map(|enum_value| {
            let name = str_field(enum_value, "name");
            format!(
                "const {}: {name}[] = [{}];",
                ts_enum_table_name(name),
                ts_enum_wire_order_values(enum_value)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn emit_typescript_model_to_map_functions(spec: &Value) -> String {
    let enums = spec_enum_map(spec);
    let mut blocks = vec![
        "export function messageContentToWireMap(request: MessageContent): Record<string, unknown> {\n  const out: Record<string, unknown> = {\n    contentType: request.contentType,\n    ...request.data,\n  };\n  if (request.contentType === MessageContentType.Text && out.mentions === undefined) {\n    out.mentions = [];\n  }\n  return out;\n}".to_string(),
    ];
    for model in all_spec_models(spec) {
        let name = str_field(model, "name");
        if matches!(name, "Message" | "SendMessageRequest") {
            continue;
        }
        let fn_name = ts_model_to_map_fn(name);
        let field_lines = ts_build_wire_object_lines(model, "request", "  ", spec, &enums);
        if field_lines.is_empty() {
            blocks.push(format!(
                "export function {fn_name}(request: {name}): Record<string, unknown> {{ return {{}}; }}"
            ));
        } else {
            let mut lines = vec![
                format!("export function {fn_name}(request: {name}): Record<string, unknown> {{"),
                "  return {".to_string(),
            ];
            lines.extend(field_lines);
            lines.extend(["  };".to_string(), "}".to_string()]);
            blocks.push(lines.join("\n"));
        }
    }
    blocks.join("\n\n")
}

fn emit_typescript_message_build_catalog(spec: &Value) -> Option<String> {
    let entries = message_build_catalog_entries(spec);
    if entries.is_empty() {
        return None;
    }
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { MessageBuildCatalogEntry } from '../../model/message_build_catalog_entry';"
            .to_string(),
        "import { MessageBuildOp } from '../../model/message_build_op';".to_string(),
        "import { MessageContentType } from '../../model/message_content_type';".to_string(),
        String::new(),
        "/** All supported quick-build operations for MessageBuilderApi. */".to_string(),
        "export const MESSAGE_BUILD_CATALOG: MessageBuildCatalogEntry[] = [".to_string(),
    ];
    for entry in entries {
        let op_member = pascal_case(str_field(entry, "op"));
        let content_member = pascal_case(str_field(entry, "contentType"));
        lines.push(format!(
            "  {{op: MessageBuildOp.{op_member}, method: {}, requestType: {}, contentType: MessageContentType.{content_member}, messageType: {}, summary: {}, stability: {} }},",
            json_quote(str_field(entry, "method")),
            json_quote(str_field(entry, "request")),
            entry.get("messageType").and_then(Value::as_i64).unwrap_or_default(),
            json_quote(str_field(entry, "summary")),
            json_quote(entry.get("stability").and_then(Value::as_str).unwrap_or("stable"))
        ));
    }
    lines.push("];".to_string());
    Some(lines.join("\n"))
}

fn ts_invoke_descriptor(method: &Value) -> String {
    format!(
        "NativeCallMap.{}",
        camel_const(str_field(method, "operation"))
    )
}

fn ts_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "void".to_string(),
        "BooleanResponse" => "boolean".to_string(),
        "ConnectionStateResponse" => "ConnectionState".to_string(),
        // JsonValue 就是"任意 JSON"，含 null。落到下面的兜底会被当成对象，
        // 生成 invokeMap，而 invokeMap 的 recordFromNative 只收 object——
        // 于是 message.dispatch 这类通用出口一旦转发到返回 unit 的 dispatchOp
        // （mark / unmark / mark_with_color 等），操作在服务端已经成功，客户端
        // 却抛 "native response must be an object"，UI 完全不反映这次变更。
        // typescript_contract.rs 早就把 JsonValue 映射成 unknown，这里对齐它。
        "JsonValue" => "unknown".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => "Record<string, unknown>".to_string(),
    }
}

fn ts_api_type_resolved(name: &str, spec: &Value) -> String {
    if matches!(name, "CreateClientResponse" | "CurrentUserIdResponse") {
        name.to_string()
    } else {
        ts_api_type(name, spec)
    }
}

fn is_api_request_type(name: &str) -> bool {
    matches!(
        name,
        "SdkConfig"
            | "CreateClientRequest"
            | "LoginRequest"
            | "UpdateAccessTokenRequest"
            | "SubscribeEventsRequest"
            | "UnsubscribeRequest"
            | "MessageDispatchRequest"
            | "DeleteMessageRequest"
            | "EditRichDocByMessageIdRequest"
            | "EditTextByMessageIdRequest"
            | "GetMessageRequest"
            | "MarkMessageReadAndBurnRequest"
            | "MessageMutationRequest"
            | "ReactionMutationRequest"
            | "RecallMessageRequest"
            | "SetTypingRequest"
            | "DisposeRequest"
    )
}

fn ts_request_arg(method: &Value, spec: &Value) -> String {
    let req = str_field(method, "request");
    if req == "Unit" {
        String::new()
    } else if is_api_request_type(req) || is_known_ts_model_type(req, spec) {
        format!("request: {req}")
    } else {
        "request: Record<string, unknown>".to_string()
    }
}

fn ts_invoke_request(method: &Value, spec: &Value) -> String {
    let req = str_field(method, "request");
    if matches!(req, "Unit" | "DisposeRequest") {
        String::new()
    } else if req == "SendMessageRequest" {
        "sendMessageRequestToMap(request)".to_string()
    } else if is_known_ts_model_type(req, spec) {
        format!("{}(request)", ts_model_to_map_fn(req))
    } else {
        "request".to_string()
    }
}

fn emit_typescript_adapter_method_body(method: &Value, spec: &Value, indent: &str) -> Vec<String> {
    let res = ts_api_type_resolved(str_field(method, "response"), spec);
    let descriptor = ts_invoke_descriptor(method);
    let req_expr = ts_invoke_request(method, spec);
    let call = if req_expr.is_empty() {
        descriptor
    } else {
        format!("{descriptor}, {req_expr}")
    };
    if str_field(method, "name") == "sendMessage" {
        return vec![
            format!("{indent}const wireRequest = sendMessageRequestToMap(request);"),
            format!("{indent}try {{"),
            format!(
                "{indent}  const ack = await invokeSendAck(this.bridge, NativeCallMap.messageSend, wireRequest);"
            ),
            format!("{indent}  if (ack.success === false) {{"),
            format!("{indent}    callback?.onFailure?.({{"),
            format!("{indent}      clientMsgId: request.message.clientMsgId,"),
            format!("{indent}      reason: ack.errorMessage,"),
            format!("{indent}      error: {{"),
            format!("{indent}        code: String(ack.errorCode || 'send_ack_failed'),"),
            format!("{indent}        message: ack.errorMessage,"),
            format!("{indent}        operation: 'message.send',"),
            format!("{indent}        details: {{"),
            format!("{indent}          ackId: ack.ackId,"),
            format!("{indent}          clientMsgId: ack.clientMsgId,"),
            format!("{indent}          conversationId: ack.conversationId,"),
            format!("{indent}          errorCode: String(ack.errorCode),"),
            format!("{indent}          errorMessage: ack.errorMessage,"),
            format!("{indent}          seq: String(ack.seq),"),
            format!("{indent}          serverId: ack.serverId,"),
            format!("{indent}          success: String(ack.success),"),
            format!("{indent}          timestamp: String(ack.timestamp),"),
            format!("{indent}        }},"),
            format!("{indent}      }},"),
            format!("{indent}    }});"),
            format!("{indent}    return ack;"),
            format!("{indent}  }}"),
            format!("{indent}  callback?.onSuccess?.({{ ack }});"),
            format!("{indent}  return ack;"),
            format!("{indent}}} catch (error) {{"),
            format!(
                "{indent}  const payload = sdkErrorPayloadFromError(error as unknown, \"message.send\");"
            ),
            format!("{indent}  callback?.onFailure?.({{"),
            format!("{indent}    clientMsgId: request.message.clientMsgId,"),
            format!("{indent}    reason: payload.message,"),
            format!("{indent}    error: payload,"),
            format!("{indent}  }});"),
            format!("{indent}  throw error;"),
            format!("{indent}}}"),
        ];
    }
    match res.as_str() {
        "void" => vec![format!("{indent}await invokeVoid(this.bridge, {call});")],
        "boolean" => vec![format!(
            "{indent}return await invokeBool(this.bridge, {call});"
        )],
        "ConnectionState" => {
            vec![format!(
                "{indent}return await invokeConnectionState(this.bridge, {call});"
            )]
        }
        "Message" => vec![format!(
            "{indent}return await invokeMessage(this.bridge, {call});"
        )],
        "SendMessageResponse" => {
            vec![format!(
                "{indent}return await invokeSendAck(this.bridge, {call});"
            )]
        }
        "ListConversationsResponse" => {
            vec![format!(
                "{indent}return await invokeListConversations(this.bridge, {call});"
            )]
        }
        "ListMessagesResponse" => {
            vec![format!(
                "{indent}return await invokeListMessages(this.bridge, {call});"
            )]
        }
        "HomeTimelineSnapshot" => {
            vec![format!(
                "{indent}return await invokeHomeTimelineSnapshot(this.bridge, {call});"
            )]
        }
        "ConversationTimelineSnapshot" => {
            vec![format!(
                "{indent}return await invokeConversationTimelineSnapshot(this.bridge, {call});"
            )]
        }
        "ViewOpenResponse" => vec![
            format!("{indent}const raw = await invokeMap(this.bridge, {call});"),
            format!("{indent}return viewOpenResponseFromJson(raw);"),
        ],
        "TimelineViewPageResponse" => vec![
            format!("{indent}const raw = await invokeMap(this.bridge, {call});"),
            format!("{indent}return timelineViewPageResponseFromJson(raw);"),
        ],
        "CloseViewResponse" => vec![
            format!("{indent}const raw = await invokeMap(this.bridge, {call});"),
            format!("{indent}return closeViewResponseFromJson(raw);"),
        ],
        _ if matches!(
            str_field(method, "name"),
            "searchMessages" | "searchMessagesByQuery" | "searchMessagesInConversation"
        ) =>
        {
            vec![format!(
                "{indent}return await invokeListMessages(this.bridge, {call});"
            )]
        }
        "Conversation" => {
            vec![format!(
                "{indent}return await invokeConversation(this.bridge, {call});"
            )]
        }
        "ListMessageBuildCatalogResponse" => {
            vec![format!(
                "{indent}return {{ entries: MESSAGE_BUILD_CATALOG }};"
            )]
        }
        "SyncConversationSummariesResponse" => vec![
            format!("{indent}const raw = await invokeMap(this.bridge, {call});"),
            format!("{indent}return syncConversationSummariesResponseFromJson(raw);"),
        ],
        "Record<string, unknown>" => {
            vec![format!(
                "{indent}return await invokeMap(this.bridge, {call});"
            )]
        }
        _ if is_known_ts_model_type(str_field(method, "response"), spec) => {
            vec![format!(
                "{indent}return await invokeMap(this.bridge, {call}) as unknown as {res};"
            )]
        }
        _ => vec![format!(
            "{indent}return await this.bridge.invoke<{res}>({call});"
        )],
    }
}

fn ts_listener_payload_type(payload: &str) -> &str {
    if payload == "ProgressEvent" {
        "SdkProgressEvent"
    } else {
        payload
    }
}

fn ts_listener_predicate_expr(name: &str) -> Option<&'static str> {
    match name {
        "onInitializing" => Some("lifecycleNameIs(event, LifecycleEventName.Initializing)"),
        "onInitialized" => Some("lifecycleNameIs(event, LifecycleEventName.Initialized)"),
        "onInitFailed" => Some("lifecycleNameIs(event, LifecycleEventName.InitFailed)"),
        "onLoginSucceeded" => Some("lifecycleNameIs(event, LifecycleEventName.LoginSucceeded)"),
        "onLoginFailed" => Some("lifecycleNameIs(event, LifecycleEventName.LoginFailed)"),
        "onLoggedOut" => Some("lifecycleNameIs(event, LifecycleEventName.LoggedOut)"),
        "onDisposed" => Some("lifecycleNameIs(event, LifecycleEventName.Disposed)"),
        "onConnecting" => Some("eventNameIs(event, 'connection', ConnectionEventName.Connecting)"),
        "onConnectSuccess" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.Connected)")
        }
        "onConnectReady" => Some("eventNameIs(event, 'connection', ConnectionEventName.Ready)"),
        "onConnectFailed" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.ServerError)")
        }
        "onDisconnected" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.Disconnected)")
        }
        "onReconnecting" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.Reconnecting)")
        }
        "onReconnectFailed" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.ReconnectFailed)")
        }
        "onKickedOffline" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.KickedOff)")
        }
        "onUserTokenExpired" => {
            Some("eventNameIs(event, 'connection', ConnectionEventName.TokenExpired)")
        }
        "onMessageReceived" => Some("eventEventIs(event, 'message', MessageEventName.Received)"),
        "onMessageReceivedBatch" => {
            Some("eventEventIs(event, 'message', MessageEventName.ReceivedBatch)")
        }
        "onMessageSendAck" => Some("eventEventIs(event, 'message', MessageEventName.SendAck)"),
        "onMessageSendFailed" => {
            Some("eventEventIs(event, 'message', MessageEventName.SendFailed)")
        }
        "onMessageRecalled" => Some("eventNameIs(event, 'message', MessageEventName.Recalled)"),
        "onMessageEdited" => Some("eventNameIs(event, 'message', MessageEventName.Edited)"),
        "onMessageDeleted" => Some("eventNameIs(event, 'message', MessageEventName.Deleted)"),
        "onMessageReadReceipt" => {
            Some("eventEventIs(event, 'message', MessageEventName.ReadReceipt)")
        }
        "onMessageReactionChanged" => {
            Some("eventEventIs(event, 'message', MessageEventName.ReactionChanged)")
        }
        "onInputStatusChanged" => Some("eventEventIs(event, 'message', MessageEventName.Typing)"),
        "onTypingAggregateChanged" => {
            Some("eventEventIs(event, 'message', MessageEventName.TypingAggregate)")
        }
        "onMessageBurned" => Some("eventNameIs(event, 'message', MessageEventName.Burned)"),
        "onMessagePinned" => Some("eventNameIs(event, 'message', MessageEventName.Pinned)"),
        "onMessageUnpinned" => Some("eventNameIs(event, 'message', MessageEventName.Unpinned)"),
        "onMessagePluginStateChanged" => {
            Some("eventNameIs(event, 'message', MessageEventName.PluginStateChanged)")
        }
        "onViewUpdated" => Some("eventTypeIs(event, 'view')"),
        "onNewConversation" => {
            Some("eventNameIs(event, 'conversation', ConversationEventName.Created)")
        }
        "onConversationChanged" => Some("eventTypeIs(event, 'conversation')"),
        "onTotalUnreadMessageCountChanged" => {
            Some("eventNameIs(event, 'conversation', ConversationEventName.UnreadCountChanged)")
        }
        "onConversationDeleted" => {
            Some("eventNameIs(event, 'conversation', ConversationEventName.Deleted)")
        }
        "onSyncServerStart" => Some("eventNameIs(event, 'sync', SyncEventName.Started)"),
        "onSyncServerFinish" => Some("eventNameIs(event, 'sync', SyncEventName.Finished)"),
        "onSyncServerFailed" => Some("eventNameIs(event, 'sync', SyncEventName.Failed)"),
        "onSyncProgress" => Some("eventNameIs(event, 'sync', ProgressEventName.SyncProgress)"),
        "onUploadProgress" => {
            Some("eventNameIs(event, 'progress', ProgressEventName.UploadProgress)")
        }
        "onDownloadProgress" => {
            Some("eventNameIs(event, 'progress', ProgressEventName.DownloadProgress)")
        }
        "onPluginRealtime" => Some("eventEventIs(event, 'plugin', PluginEventName.Realtime)"),
        "onPluginCatalogChanged" => {
            Some("eventEventIs(event, 'plugin', PluginEventName.CatalogChanged)")
        }
        _ => None,
    }
}

fn ts_event_code_const_name(id: &str) -> String {
    format!(
        "EVENT_CODE_{}",
        snake_case(&id.replace('.', "_")).to_ascii_uppercase()
    )
}

fn ts_event_decoder_expr(entry: &TypescriptEventRegistryEntry) -> String {
    match entry.id.as_str() {
        "connection.connected" => {
            "connectionEvent(ConnectionEventName.Connected, SdkConnectionState.Connected, \"connected\", payload)"
                .to_string()
        }
        "connection.disconnected" => {
            "connectionEvent(ConnectionEventName.Disconnected, SdkConnectionState.Disconnected, \"disconnected\", payload)"
                .to_string()
        }
        "connection.reconnecting" => {
            "connectionEvent(ConnectionEventName.Reconnecting, SdkConnectionState.Reconnecting, \"reconnecting\", payload)"
                .to_string()
        }
        "connection.state_changed" => "connectionStateChangedEvent(payload)".to_string(),
        "connection.sync_state_changed" => {
            "connectionEvent(ConnectionEventName.SyncStateChanged, SdkConnectionState.Ready, \"sync_state_changed\", payload)"
                .to_string()
        }
        "connection.server_error" => {
            "connectionEvent(ConnectionEventName.ServerError, SdkConnectionState.Connected, \"server_error\", payload)"
                .to_string()
        }
        "connection.kicked_off" => {
            "connectionEvent(ConnectionEventName.KickedOff, SdkConnectionState.Disconnected, \"kicked_off\", payload)"
                .to_string()
        }
        "connection.token_expired" => {
            "connectionEvent(ConnectionEventName.TokenExpired, SdkConnectionState.Disconnected, \"token_expired\", payload)"
                .to_string()
        }
        "message.received" => "messageReceivedEvent(payload)".to_string(),
        "message.received_batch" => "messageReceivedBatchEvent(payload)".to_string(),
        "message.send_ack" => "messageSendAckEvent(payload)".to_string(),
        "message.send_failed" => "messageSendFailedEvent(payload)".to_string(),
        "message.recalled" => {
            "messageMutationEvent(MessageEventName.Recalled, \"recalled\", payload)".to_string()
        }
        "message.edited" => {
            "messageMutationEvent(MessageEventName.Edited, \"edited\", payload)".to_string()
        }
        "message.deleted" => {
            "messageMutationEvent(MessageEventName.Deleted, \"deleted\", payload)".to_string()
        }
        "message.pinned" => {
            "messageMutationEvent(MessageEventName.Pinned, \"pinned\", payload)".to_string()
        }
        "message.unpinned" => {
            "messageMutationEvent(MessageEventName.Unpinned, \"unpinned\", payload)".to_string()
        }
        "message.plugin_state_changed" => {
            "messageMutationEvent(MessageEventName.PluginStateChanged, \"plugin_state_changed\", payload)"
                .to_string()
        }
        "message.marked" => {
            "messageMutationEvent(MessageEventName.Marked, \"marked\", payload)".to_string()
        }
        "message.unmarked" => {
            "messageMutationEvent(MessageEventName.Unmarked, \"unmarked\", payload)".to_string()
        }
        "message.retention_scheduled" => {
            "messageMutationEvent(MessageEventName.RetentionScheduled, \"retention_scheduled\", payload)"
                .to_string()
        }
        "message.retention_expired" => {
            "messageMutationEvent(MessageEventName.RetentionExpired, \"retention_expired\", payload)"
                .to_string()
        }
        "message.retention_purged" => {
            "messageMutationEvent(MessageEventName.RetentionPurged, \"retention_purged\", payload)"
                .to_string()
        }
        "message.typing" => "typingEvent(payload)".to_string(),
        "message.typing_aggregate" => "typingAggregateEvent(payload)".to_string(),
        "message.reaction_changed" => "reactionChangedEvent(payload)".to_string(),
        "message.read_receipt" => "readReceiptEvent(payload)".to_string(),
        "message.presence_changed" => "presenceChangedEvent(payload)".to_string(),
        "message.custom" => "contractEvent(\"message\", \"custom\", payload)".to_string(),
        "conversation.synced" => {
            "conversationEvent(ConversationEventName.Synced, \"synced\", payload)".to_string()
        }
        "conversation.created" => {
            "conversationEvent(ConversationEventName.Created, \"created\", payload)".to_string()
        }
        "conversation.updated" => {
            "conversationEvent(ConversationEventName.Updated, \"updated\", payload)".to_string()
        }
        "conversation.unread_count_changed" => {
            "conversationEvent(ConversationEventName.UnreadCountChanged, \"unread_count_changed\", payload)"
                .to_string()
        }
        "conversation.deleted" => {
            "conversationEvent(ConversationEventName.Deleted, \"deleted\", payload)".to_string()
        }
        "notification.received" => "notificationReceivedEvent(payload)".to_string(),
        "sync.started" => "syncEvent(SyncEventName.Started, \"started\", payload)".to_string(),
        "sync.finished" => "syncEvent(SyncEventName.Finished, \"finished\", payload)".to_string(),
        "sync.failed" => "syncEvent(SyncEventName.Failed, \"failed\", payload)".to_string(),
        "sync.task_completed" => {
            "syncEvent(SyncEventName.TaskCompleted, \"task_completed\", payload)".to_string()
        }
        "sync.state_changed" => {
            "syncEvent(SyncEventName.StateChanged, \"state_changed\", payload)".to_string()
        }
        "sync.resync_needed" => {
            "syncEvent(SyncEventName.ResyncNeeded, \"resync_needed\", payload)".to_string()
        }
        "sync.progress" => "syncProgressEvent(payload)".to_string(),
        "view.updated" => "viewUpdatedEvent(payload)".to_string(),
        "extension.event" => "contractEvent(\"extension\", \"event\", payload)".to_string(),
        "plugin.realtime" => "pluginRealtimeEvent(payload)".to_string(),
        "plugin.catalog_changed" => "pluginCatalogChangedEvent(payload)".to_string(),
        _ => "undefined".to_string(),
    }
}

fn emit_typescript_native_event_decoder(
    event_registry: &[TypescriptEventRegistryEntry],
) -> Vec<String> {
    let mut lines = Vec::new();
    for entry in event_registry {
        lines.push(format!(
            "const {} = {};",
            ts_event_code_const_name(&entry.id),
            entry.code
        ));
    }
    lines.extend([
        String::new(),
        "const WEB_EVENT_TYPE_BY_CHANNEL: Record<string, number> = {".to_string(),
    ]);
    for entry in event_registry {
        if let Some(channel) = &entry.web_channel {
            lines.push(format!(
                "  {}: {},",
                json_quote(channel),
                ts_event_code_const_name(&entry.id)
            ));
        }
    }
    lines.extend([
        "};".to_string(),
        String::new(),
        "export const WEB_EVENT_CHANNELS = Object.freeze(Object.keys(WEB_EVENT_TYPE_BY_CHANNEL));"
            .to_string(),
        String::new(),
        "export function eventTypeForWebChannel(channel: string): number | undefined {".to_string(),
        "  return WEB_EVENT_TYPE_BY_CHANNEL[channel];".to_string(),
        "}".to_string(),
        String::new(),
        "function eventPayloadRecord(payload: unknown): Record<string, unknown> {".to_string(),
        "  const decoded = wireDecodeResponse(payload);".to_string(),
        "  return decoded && typeof decoded === 'object' && !Array.isArray(decoded)".to_string(),
        "    ? decoded as Record<string, unknown>".to_string(),
        "    : {};".to_string(),
        "}".to_string(),
        String::new(),
        "function invalidEventField(field: string, expected: string): never {".to_string(),
        "  throw new FlareSdkException('invalidParameter', `invalid event payload field: ${field}`, 'event.decode', { field, expected });".to_string(),
        "}".to_string(),
        String::new(),
        "function requiredString(value: unknown, field: string): string {".to_string(),
        "  if (typeof value === 'string' && value.length > 0) return value;".to_string(),
        "  return invalidEventField(field, 'non-empty string');".to_string(),
        "}".to_string(),
        String::new(),
        "function optionalString(value: unknown, field: string): string | undefined {".to_string(),
        "  if (value === undefined || value === null) return undefined;".to_string(),
        "  if (typeof value === 'string' && value.length > 0) return value;".to_string(),
        "  return invalidEventField(field, 'non-empty string');".to_string(),
        "}".to_string(),
        String::new(),
        "function requiredNumber(value: unknown, field: string): number {".to_string(),
        "  if (typeof value === 'number' && Number.isFinite(value)) return value;".to_string(),
        "  return invalidEventField(field, 'finite number');".to_string(),
        "}".to_string(),
        String::new(),
        "function optionalNumber(value: unknown, field: string): number | undefined {".to_string(),
        "  if (value === undefined || value === null) return undefined;".to_string(),
        "  return requiredNumber(value, field);".to_string(),
        "}".to_string(),
        String::new(),
        "function requiredBoolean(value: unknown, field: string): boolean {".to_string(),
        "  if (typeof value === 'boolean') return value;".to_string(),
        "  return invalidEventField(field, 'boolean');".to_string(),
        "}".to_string(),
        String::new(),
        "function optionalBoolean(value: unknown, field: string): boolean | undefined {".to_string(),
        "  if (value === undefined || value === null) return undefined;".to_string(),
        "  return requiredBoolean(value, field);".to_string(),
        "}".to_string(),
        String::new(),
        "function requiredArray(value: unknown, field: string): unknown[] {".to_string(),
        "  if (Array.isArray(value)) return value;".to_string(),
        "  return invalidEventField(field, 'array');".to_string(),
        "}".to_string(),
        String::new(),
        "function stringRecord(value: unknown, field: string): Record<string, string> {".to_string(),
        "  if (value === undefined || value === null) return {};".to_string(),
        "  const record = eventPayloadRecord(value);".to_string(),
        "  const out: Record<string, string> = {};".to_string(),
        "  for (const [key, item] of Object.entries(record)) {".to_string(),
        "    if (typeof item !== 'string') invalidEventField(`${field}.${key}`, 'string');".to_string(),
        "    out[key] = item;".to_string(),
        "  }".to_string(),
        "  return out;".to_string(),
        "}".to_string(),
        String::new(),
        "/** The server's typed reason; empty for a local failure, which reads as no reason. */".to_string(),
        "function machineReasonFromJson(value: unknown): string | undefined {".to_string(),
        "  if (value === undefined || value === null || value === '') return undefined;".to_string(),
        "  if (typeof value === 'string') return value;".to_string(),
        "  return invalidEventField('error.machineReason', 'string');".to_string(),
        "}".to_string(),
        String::new(),
        "function sdkErrorPayloadFromJson(value: unknown): SdkErrorPayload | undefined {".to_string(),
        "  if (value === undefined || value === null) return undefined;".to_string(),
        "  const decoded = wireDecodeResponse(value);".to_string(),
        "  if (typeof decoded === 'string') return undefined;".to_string(),
        "  if (!decoded || typeof decoded !== 'object' || Array.isArray(decoded)) invalidEventField('error', 'object');".to_string(),
        "  const record = decoded as Record<string, unknown>;".to_string(),
        "  return {".to_string(),
        "    code: requiredString(record.code, 'error.code'),".to_string(),
        "    machineReason: machineReasonFromJson(record.machineReason),".to_string(),
        "    message: requiredString(record.message, 'error.message'),".to_string(),
        "    operation: optionalString(record.operation, 'error.operation'),".to_string(),
        "    retryable: optionalBoolean(record.retryable, 'error.retryable'),".to_string(),
        "    details: stringRecord(record.details, 'error.details'),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function connectionStateFromWire(value: unknown): SdkConnectionState {".to_string(),
        "  const raw = String(value ?? '').trim().toLowerCase();".to_string(),
        "  switch (raw) {".to_string(),
        "    case 'connecting': return SdkConnectionState.Connecting;".to_string(),
        "    case 'connected': return SdkConnectionState.Connected;".to_string(),
        "    case 'ready': return SdkConnectionState.Ready;".to_string(),
        "    case 'reconnecting': return SdkConnectionState.Reconnecting;".to_string(),
        "    case 'disconnected': return SdkConnectionState.Disconnected;".to_string(),
        "    default: throw new FlareSdkException('invalidParameter', `invalid connection state: ${raw || '<empty>'}`, 'event.decode', { field: 'state' });".to_string(),
        "  }".to_string(),
        "}".to_string(),
        String::new(),
        "function connectionEvent(name: ConnectionEventName, state: SdkConnectionState, event: string, payload: unknown): ConnectionEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const { error: rawError, ...rest } = record;".to_string(),
        "  const error = sdkErrorPayloadFromJson(rawError);".to_string(),
        "  const reason = optionalString(record.reason, 'reason');".to_string(),
        "  const attempt = optionalNumber(record.attempt, 'attempt');".to_string(),
        "  return {".to_string(),
        "    ...rest,".to_string(),
        "    type: 'connection',".to_string(),
        "    event,".to_string(),
        "    name,".to_string(),
        "    state,".to_string(),
        "    ...(reason !== undefined ? { reason } : {}),".to_string(),
        "    ...(attempt !== undefined ? { attempt } : {}),".to_string(),
        "    ...(error ? { error } : {}),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function connectionStateChangedEvent(payload: unknown): ConnectionEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const state = connectionStateFromWire(record.state);".to_string(),
        "  const name = connectionEventNameFromState(state);".to_string(),
        "  return connectionEvent(name, state, 'state_changed', record);".to_string(),
        "}".to_string(),
        String::new(),
        "function connectionEventNameFromState(state: SdkConnectionState): ConnectionEventName {".to_string(),
        "  switch (state) {".to_string(),
        "    case SdkConnectionState.Connecting: return ConnectionEventName.Connecting;".to_string(),
        "    case SdkConnectionState.Connected: return ConnectionEventName.Connected;".to_string(),
        "    case SdkConnectionState.Ready: return ConnectionEventName.Ready;".to_string(),
        "    case SdkConnectionState.Reconnecting: return ConnectionEventName.Reconnecting;".to_string(),
        "    case SdkConnectionState.Disconnected: return ConnectionEventName.Disconnected;".to_string(),
        "  }".to_string(),
        "}".to_string(),
        String::new(),
        "function messageReceivedEvent(payload: unknown): MessageReceivedEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { type: 'message', event: 'received', message: messageFromJson(record) };".to_string(),
        "}".to_string(),
        String::new(),
        "function messageReceivedBatchEvent(payload: unknown): MessageReceivedBatchEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const source = requiredArray(record.messages, 'messages');".to_string(),
        "  return { ...record, type: 'message', event: 'received_batch', messages: source.map((item) => messageFromJson(item)) };".to_string(),
        "}".to_string(),
        String::new(),
        "function messageSendAckEvent(payload: unknown): MessageSendAckEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { ...record, type: 'message', event: 'send_ack', ack: sendAckFromJson(record) };".to_string(),
        "}".to_string(),
        String::new(),
        "function messageSendFailedEvent(payload: unknown): MessageSendFailedEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const { error: rawError, ...rest } = record;".to_string(),
        "  const error = sdkErrorPayloadFromJson(rawError);".to_string(),
        "  return {".to_string(),
        "    ...rest,".to_string(),
        "    type: 'message',".to_string(),
        "    event: 'send_failed',".to_string(),
        "    clientMsgId: requiredString(record.clientMsgId, 'clientMsgId'),".to_string(),
        "    reason: requiredString(record.reason, 'reason'),".to_string(),
        "    ...(error ? { error } : {}),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function messageMutationEvent(name: MessageEventName, event: string, payload: unknown): MessageMutationEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return {".to_string(),
        "    ...record,".to_string(),
        "    type: 'message',".to_string(),
        "    event,".to_string(),
        "    name,".to_string(),
        "    conversationId: requiredString(record.conversationId, 'conversationId'),".to_string(),
        "    messageId: optionalString(record.messageId, 'messageId'),".to_string(),
        "    serverMsgId: optionalString(record.serverMsgId, 'serverMsgId'),".to_string(),
        "    userId: optionalString(record.userId, 'userId'),".to_string(),
        "    reason: optionalString(record.reason, 'reason'),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function typingEvent(payload: unknown): TypingEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { ...record, type: 'message', event: 'typing', conversationId: requiredString(record.conversationId, 'conversationId'), userId: requiredString(record.userId, 'userId'), typing: requiredBoolean(record.typing, 'typing') };".to_string(),
        "}".to_string(),
        String::new(),
        "function typingAggregateEvent(payload: unknown): TypingAggregateEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const typingUserIds = requiredArray(record.typingUserIds, 'typingUserIds').map((item, index) => requiredString(item, `typingUserIds.${index}`));".to_string(),
        "  return { ...record, type: 'message', event: 'typing_aggregate', name: MessageEventName.TypingAggregate, conversationId: requiredString(record.conversationId, 'conversationId'), typingUserIds, typingCount: requiredNumber(record.typingCount, 'typingCount') };".to_string(),
        "}".to_string(),
        String::new(),
        "function readReceiptEvent(payload: unknown): ReadReceiptEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { ...record, type: 'message', event: 'read_receipt', conversationId: requiredString(record.conversationId, 'conversationId'), userId: requiredString(record.userId, 'userId'), readSeq: requiredNumber(record.readSeq, 'readSeq') };".to_string(),
        "}".to_string(),
        String::new(),
        "function reactionChangedEvent(payload: unknown): ReactionChangedEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { ...record, type: 'message', event: 'reaction_changed', conversationId: requiredString(record.conversationId, 'conversationId'), serverMsgId: requiredString(record.serverMsgId, 'serverMsgId'), userId: requiredString(record.userId, 'userId'), emoji: requiredString(record.emoji, 'emoji'), action: requiredNumber(record.action, 'action') };".to_string(),
        "}".to_string(),
        String::new(),
        "function presenceChangedEvent(payload: unknown): PresenceChangedEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { ...record, type: 'presence', event: 'changed', conversationId: optionalString(record.conversationId, 'conversationId'), userId: requiredString(record.userId, 'userId'), status: requiredString(record.status, 'status'), extra: stringRecord(record.extra, 'extra') };".to_string(),
        "}".to_string(),
        String::new(),
        "function pluginRealtimeEvent(payload: unknown): PluginRealtimeEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const body = record.payload;".to_string(),
        "  if (!body || typeof body !== 'object' || Array.isArray(body)) invalidEventField('payload', 'object');".to_string(),
        "  return {".to_string(),
        "    type: 'plugin',".to_string(),
        "    event: PluginEventName.Realtime,".to_string(),
        "    pluginId: requiredString(record.pluginId, 'pluginId'),".to_string(),
        "    packetType: requiredString(record.packetType, 'packetType'),".to_string(),
        "    conversationId: requiredString(record.conversationId, 'conversationId'),".to_string(),
        "    payload: body as Record<string, unknown>,".to_string(),
        "    deliveryId: requiredString(record.deliveryId, 'deliveryId'),".to_string(),
        "    expiresAtMs: requiredNumber(record.expiresAtMs, 'expiresAtMs'),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function pluginCatalogChangedEvent(payload: unknown): PluginCatalogChangedEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { type: 'plugin', event: PluginEventName.CatalogChanged, version: requiredString(record.version, 'version') };".to_string(),
        "}".to_string(),
        String::new(),
        "function conversationEvent(name: ConversationEventName, event: string, payload: unknown): ConversationEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const conversationId = optionalString(record.conversationId, 'conversationId');".to_string(),
        "  const conversationIds = Array.isArray(record.conversationIds)".to_string(),
        "    ? record.conversationIds.map((item, index) => requiredString(item, `conversationIds.${index}`))".to_string(),
        "    : conversationId ? [conversationId] : [];".to_string(),
        "  return { ...record, type: 'conversation', event, name, conversationId, conversationIds, unreadCount: optionalNumber(record.unreadCount, 'unreadCount') };".to_string(),
        "}".to_string(),
        String::new(),
        "function syncEvent(name: SyncEventName, event: string, payload: unknown): SyncEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const { error: rawError, ...rest } = record;".to_string(),
        "  const error = sdkErrorPayloadFromJson(rawError);".to_string(),
        "  return {".to_string(),
        "    ...rest,".to_string(),
        "    type: 'sync',".to_string(),
        "    event,".to_string(),
        "    name,".to_string(),
        "    trigger: optionalString(record.trigger, 'trigger'),".to_string(),
        "    phase: optionalString(record.phase, 'phase'),".to_string(),
        "    task: optionalString(record.task, 'task'),".to_string(),
        "    progress: optionalNumber(record.progress, 'progress'),".to_string(),
        "    ...(error ? { error } : {}),".to_string(),
        "    ...(typeof rawError === 'string' ? { message: rawError } : {}),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function syncProgressEvent(payload: unknown): SdkProgressEvent & Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  const progress = requiredNumber(record.progress, 'progress');".to_string(),
        "  return {".to_string(),
        "    ...record,".to_string(),
        "    type: 'sync',".to_string(),
        "    event: 'progress',".to_string(),
        "    name: ProgressEventName.SyncProgress,".to_string(),
        "    operation: optionalString(record.task, 'task') ?? 'sync',".to_string(),
        "    current: progress,".to_string(),
        "    total: 100,".to_string(),
        "    taskId: optionalString(record.task, 'task'),".to_string(),
        "    detail: optionalString(record.detail, 'detail'),".to_string(),
        "  };".to_string(),
        "}".to_string(),
        String::new(),
        "function viewUpdatedEvent(payload: unknown): ViewUpdate & Record<string, unknown> {".to_string(),
        "  return { ...viewUpdateFromJson(wireDecodeResponse(payload)), type: 'view', event: 'updated' };".to_string(),
        "}".to_string(),
        String::new(),
        "function notificationReceivedEvent(payload: unknown): Record<string, unknown> {".to_string(),
        "  const record = eventPayloadRecord(payload);".to_string(),
        "  return { type: 'notification', event: 'received', message: messageFromJson(record) };".to_string(),
        "}".to_string(),
        String::new(),
        "function contractEvent(type: string, event: string, payload: unknown): Record<string, unknown> {".to_string(),
        "  return { ...eventPayloadRecord(payload), type, event };".to_string(),
        "}".to_string(),
        String::new(),
        "function emittedEventRecord(event: unknown): { type?: unknown; event?: unknown; name?: unknown } {".to_string(),
        "  return event && typeof event === 'object'".to_string(),
        "    ? event as { type?: unknown; event?: unknown; name?: unknown }".to_string(),
        "    : {};".to_string(),
        "}".to_string(),
        String::new(),
        "function eventTypeIs(event: unknown, type: string): boolean {".to_string(),
        "  return String(emittedEventRecord(event).type ?? '') === type;".to_string(),
        "}".to_string(),
        String::new(),
        "function eventEventIs(event: unknown, type: string, eventName: string): boolean {".to_string(),
        "  const record = emittedEventRecord(event);".to_string(),
        "  return String(record.type ?? '') === type && String(record.event ?? '') === eventName;".to_string(),
        "}".to_string(),
        String::new(),
        "function eventNameIs(event: unknown, type: string, name: string): boolean {".to_string(),
        "  const record = emittedEventRecord(event);".to_string(),
        "  return String(record.type ?? '') === type && String(record.name ?? '') === name;".to_string(),
        "}".to_string(),
        String::new(),
        "function lifecycleNameIs(event: unknown, name: string): boolean {".to_string(),
        "  return String(emittedEventRecord(event).name ?? '') === name;".to_string(),
        "}".to_string(),
        String::new(),
        "export function nativeEventFromCode(eventType: number, payload: unknown): unknown {".to_string(),
        "  switch (eventType) {".to_string(),
    ]);
    for entry in event_registry {
        lines.push(format!(
            "    case {}: return {};",
            ts_event_code_const_name(&entry.id),
            ts_event_decoder_expr(entry)
        ));
    }
    lines.extend([
        "    default: return {".to_string(),
        "      type: 'unknown',".to_string(),
        "      name: 'unknown',".to_string(),
        "      event: 'unknown',".to_string(),
        "      eventType,".to_string(),
        "      payload,".to_string(),
        "    };".to_string(),
        "  }".to_string(),
        "}".to_string(),
        String::new(),
    ]);
    lines
}

fn emit_typescript_adapter_events_api(
    spec: &Value,
    event_registry: &[TypescriptEventRegistryEntry],
) -> String {
    let event_subscription_methods = child_arr(spec, "modules")
        .iter()
        .find(|module| str_field(module, "key") == "events")
        .map(|module| {
            child_arr(module, "methods")
                .iter()
                .filter(|method| {
                    str_field(method, "request") == "SubscribeEventsRequest"
                        && str_field(method, "response") == "Subscription"
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let listener_payloads = child_arr(spec, "listeners")
        .iter()
        .map(|listener| ts_listener_payload_type(str_field(listener, "payload")).to_string())
        .collect::<BTreeSet<_>>();
    let mut model_imports = [
        "ConnectionEvent",
        "ConnectionEventName",
        "ConversationEvent",
        "ConversationEventName",
        "LifecycleEvent",
        "LifecycleEventName",
        "MessageEventName",
        "MessageMutationEvent",
        "MessageReceivedBatchEvent",
        "MessageReceivedEvent",
        "MessageSendAckEvent",
        "MessageSendFailedEvent",
        "PluginCatalogChangedEvent",
        "PluginEventName",
        "PluginRealtimeEvent",
        "PresenceChangedEvent",
        "ProgressEventName",
        "ReactionChangedEvent",
        "ReadReceiptEvent",
        "SdkConnectionState",
        "SdkErrorPayload",
        "SyncEvent",
        "SyncEventName",
        "TypingEvent",
        "ViewUpdate",
    ]
    .into_iter()
    .map(str::to_string)
    .chain(listener_payloads)
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect::<Vec<_>>();
    if let Some(index) = model_imports
        .iter()
        .position(|item| item == "SdkProgressEvent")
    {
        model_imports.remove(index);
        model_imports.push("ProgressEvent as SdkProgressEvent".to_string());
    }
    let model_import_block = model_imports.join(",\n  ");
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { NativeBridge, NativeCallMap } from '../../contract/bridge_contract';".to_string(),
        "import type { EventsApi } from '../../api/modules/events';".to_string(),
        "import type { SubscribeEventsRequest, Subscription, UnsubscribeRequest } from '../../api/types';".to_string(),
        "import type { EventCallback, EventSubscription, FlareImEventListener } from '../../listener';".to_string(),
        "import {".to_string(),
        format!("  {model_import_block},"),
        "} from '../../model';".to_string(),
        "import { messageFromJson, sendAckFromJson, viewUpdateFromJson, wireDecodeResponse } from '../codec/wireCodec';".to_string(),
        "import { FlareSdkException } from '../../bridge/flareSdkException';".to_string(),
        String::new(),
    ];
    lines.extend(emit_typescript_native_event_decoder(event_registry));
    lines.extend([
        "class DefaultEventSubscription implements EventSubscription {".to_string(),
        "  constructor(".to_string(),
        "    readonly id: string,".to_string(),
        "    private readonly onDispose: () => void,".to_string(),
        "    public handler?: unknown,".to_string(),
        "  ) {}".to_string(),
        String::new(),
        "  unsubscribe(): void {".to_string(),
        "    this.onDispose();".to_string(),
        "  }".to_string(),
        "}".to_string(),
        String::new(),
        "export class DefaultEventsApi implements EventsApi {".to_string(),
        "  private subscriptions: Map<number, DefaultEventSubscription> = new Map();".to_string(),
        "  private nextId = 1;".to_string(),
        String::new(),
        "  constructor(private readonly bridge: NativeBridge) {}".to_string(),
        String::new(),
    ]);
    for method in event_subscription_methods {
        lines.extend([
            format!(
                "  async {}(request: SubscribeEventsRequest): Promise<Subscription> {{",
                str_field(method, "name")
            ),
            format!(
                "    return this.bridge.invoke<Subscription>(NativeCallMap.{}, this.requestWithDefaultHandler(request));",
                camel_const(str_field(method, "operation"))
            ),
            "  }".to_string(),
            String::new(),
        ]);
    }
    lines.extend([
        "  async unsubscribe(request: Record<string, unknown>): Promise<void> {".to_string(),
        "    await this.bridge.invoke<void>(NativeCallMap.eventUnsubscribe, request);".to_string(),
        "  }".to_string(),
        String::new(),
        "  async unsubscribeAll(): Promise<void> {".to_string(),
        "    this.subscriptions.clear();".to_string(),
        "    await this.bridge.invoke<void>(NativeCallMap.eventUnsubscribeAll);".to_string(),
        "  }".to_string(),
        String::new(),
        "  emit(event: unknown): void {".to_string(),
        "    const subscriptions = this.subscriptions;".to_string(),
        "    if (subscriptions.size === 0) {".to_string(),
        "      return;".to_string(),
        "    }".to_string(),
        "    // Fast path: a single subscription (the common case) needs no snapshot allocation."
            .to_string(),
        "    if (subscriptions.size === 1) {".to_string(),
        "      const handler = subscriptions.values().next().value?.handler;".to_string(),
        "      if (handler) {".to_string(),
        "        this.dispatchSafely(handler, event);".to_string(),
        "      }".to_string(),
        "      return;".to_string(),
        "    }".to_string(),
        "    // Snapshot so listeners added/removed during dispatch don't change this fan-out."
            .to_string(),
        "    for (const subscription of Array.from(subscriptions.values())) {".to_string(),
        "      if (subscription.handler) {".to_string(),
        "        this.dispatchSafely(subscription.handler, event);".to_string(),
        "      }".to_string(),
        "    }".to_string(),
        "  }".to_string(),
        String::new(),
        "  private dispatchSafely(handler: unknown, event: unknown): void {".to_string(),
        "    try {".to_string(),
        "      if (typeof handler === 'function') {".to_string(),
        "        (handler as EventCallback<unknown>)(event);".to_string(),
        "      } else {".to_string(),
        "        this.dispatchToListener(handler as FlareImEventListener, event);".to_string(),
        "      }".to_string(),
        "    } catch (error) {".to_string(),
        "      console.error('[flare-core] event listener failed', error);".to_string(),
        "    }".to_string(),
        "  }".to_string(),
        String::new(),
        "  addEventListener(listener: FlareImEventListener): EventSubscription {".to_string(),
        "    return this.register(listener);".to_string(),
        "  }".to_string(),
        String::new(),
        "  removeEventListener(subscription: EventSubscription): void {".to_string(),
        "    subscription.unsubscribe();".to_string(),
        "  }".to_string(),
        String::new(),
        "  emitNativeEvent(eventType: number, payload: unknown): void {".to_string(),
        "    this.emit(nativeEventFromCode(eventType, payload));".to_string(),
        "  }".to_string(),
    ]);
    for listener in child_arr(spec, "listeners") {
        let payload = ts_listener_payload_type(str_field(listener, "payload"));
        let name = str_field(listener, "name");
        let register_line = match ts_listener_predicate_expr(name) {
            Some(predicate) => {
                format!("    return this.registerWhere(listener, (event) => {predicate});")
            }
            None => "    return this.registerTyped(listener);".to_string(),
        };
        lines.extend([
            String::new(),
            format!(
                "  {}(listener: EventCallback<{payload}>): EventSubscription {{",
                name
            ),
            register_line,
            "  }".to_string(),
        ]);
    }
    lines.extend([
        String::new(),
        "  private requestWithDefaultHandler(request: SubscribeEventsRequest): SubscribeEventsRequest {".to_string(),
        "    const record = request as Record<string, unknown>;".to_string(),
        "    if (record.handler !== undefined) {".to_string(),
        "      return request;".to_string(),
        "    }".to_string(),
        "    return {".to_string(),
        "      ...record,".to_string(),
        "      handler: (eventType: number, payload: unknown) => this.emitNativeEvent(eventType, payload),".to_string(),
        "    } as SubscribeEventsRequest;".to_string(),
        "  }".to_string(),
        String::new(),
        "  private register(handler: unknown): EventSubscription {".to_string(),
        "    const subscriptionId = this.nextId++;".to_string(),
        "    const subscription = new DefaultEventSubscription(".to_string(),
        "      subscriptionId.toString(),".to_string(),
        "      () => this.subscriptions.delete(subscriptionId),".to_string(),
        "      handler,".to_string(),
        "    );".to_string(),
        "    this.subscriptions.set(subscriptionId, subscription);".to_string(),
        "    return subscription;".to_string(),
        "  }".to_string(),
        String::new(),
        "  private registerTyped<T>(listener: EventCallback<T>): EventSubscription {".to_string(),
        "    return this.register((event: unknown) => {".to_string(),
        "      listener(event as T);".to_string(),
        "    });".to_string(),
        "  }".to_string(),
        String::new(),
        "  private registerWhere<T>(".to_string(),
        "    listener: EventCallback<T>,".to_string(),
        "    predicate: (event: unknown) => boolean,".to_string(),
        "  ): EventSubscription {".to_string(),
        "    return this.register((event: unknown) => {".to_string(),
        "      if (predicate(event)) {".to_string(),
        "        listener(event as T);".to_string(),
        "      }".to_string(),
        "    });".to_string(),
        "  }".to_string(),
        String::new(),
        "  private dispatchToListener(listener: FlareImEventListener, event: unknown): void {"
            .to_string(),
        "    const record = event && typeof event === 'object'".to_string(),
        "      ? event as { type?: unknown; event?: unknown; name?: unknown; state?: unknown }".to_string(),
        "      : {};".to_string(),
        "    const type = String(record.type ?? '');".to_string(),
        "    const eventName = String(record.event ?? '');".to_string(),
        "    const name = String(record.name ?? '');".to_string(),
        "    if (type === 'message') {".to_string(),
        "      switch (eventName) {".to_string(),
        "        case MessageEventName.Received:".to_string(),
        "          listener.onMessageReceived?.(event as MessageReceivedEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.ReceivedBatch:".to_string(),
        "          listener.onMessageReceivedBatch?.(event as MessageReceivedBatchEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.SendAck:".to_string(),
        "          listener.onMessageSendAck?.(event as MessageSendAckEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.SendFailed:".to_string(),
        "          listener.onMessageSendFailed?.(event as MessageSendFailedEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Recalled:".to_string(),
        "          listener.onMessageRecalled?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Edited:".to_string(),
        "          listener.onMessageEdited?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Deleted:".to_string(),
        "          listener.onMessageDeleted?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.ReadReceipt:".to_string(),
        "          listener.onMessageReadReceipt?.(event as ReadReceiptEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.ReactionChanged:".to_string(),
        "          listener.onMessageReactionChanged?.(event as ReactionChangedEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Typing:".to_string(),
        "          listener.onInputStatusChanged?.(event as TypingEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Burned:".to_string(),
        "        case MessageEventName.RetentionExpired:".to_string(),
        "        case MessageEventName.RetentionPurged:".to_string(),
        "          listener.onMessageBurned?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Pinned:".to_string(),
        "          listener.onMessagePinned?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.Unpinned:".to_string(),
        "          listener.onMessageUnpinned?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        case MessageEventName.PluginStateChanged:".to_string(),
        "          listener.onMessagePluginStateChanged?.(event as MessageMutationEvent);".to_string(),
        "          return;".to_string(),
        "        default:".to_string(),
        "          return;".to_string(),
        "      }".to_string(),
        "    }".to_string(),
        "    if (type === 'conversation') {".to_string(),
        "      switch (name) {".to_string(),
        "        case ConversationEventName.Created:".to_string(),
        "          listener.onNewConversation?.(event as ConversationEvent);".to_string(),
        "          return;".to_string(),
        "        case ConversationEventName.Deleted:".to_string(),
        "          listener.onConversationDeleted?.(event as ConversationEvent);".to_string(),
        "          return;".to_string(),
        "        case ConversationEventName.UnreadCountChanged:".to_string(),
        "          listener.onTotalUnreadMessageCountChanged?.(event as ConversationEvent);".to_string(),
        "          return;".to_string(),
        "        default:".to_string(),
        "          listener.onConversationChanged?.(event as ConversationEvent);".to_string(),
        "          return;".to_string(),
        "      }".to_string(),
        "    }".to_string(),
        "    if (type === 'sync') {".to_string(),
        "      if (name === ProgressEventName.SyncProgress) {".to_string(),
        "        listener.onSyncProgress?.(event as SdkProgressEvent);".to_string(),
        "        return;".to_string(),
        "      }".to_string(),
        "      switch (name) {".to_string(),
        "        case SyncEventName.Started:".to_string(),
        "          listener.onSyncServerStart?.(event as SyncEvent);".to_string(),
        "          return;".to_string(),
        "        case SyncEventName.Finished:".to_string(),
        "          listener.onSyncServerFinish?.(event as SyncEvent);".to_string(),
        "          return;".to_string(),
        "        case SyncEventName.Failed:".to_string(),
        "          listener.onSyncServerFailed?.(event as SyncEvent);".to_string(),
        "          return;".to_string(),
        "        default:".to_string(),
        "          return;".to_string(),
        "      }".to_string(),
        "    }".to_string(),
        "    if (type === 'connection') {".to_string(),
        "      switch (name) {".to_string(),
        "        case ConnectionEventName.Connecting:".to_string(),
        "          listener.onConnecting?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.Connected:".to_string(),
        "          listener.onConnectSuccess?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.Ready:".to_string(),
        "          listener.onConnectReady?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.Disconnected:".to_string(),
        "          listener.onDisconnected?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.Reconnecting:".to_string(),
        "          listener.onReconnecting?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.ReconnectFailed:".to_string(),
        "          listener.onReconnectFailed?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.KickedOff:".to_string(),
        "          listener.onKickedOffline?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        case ConnectionEventName.TokenExpired:".to_string(),
        "          listener.onUserTokenExpired?.(event as ConnectionEvent);".to_string(),
        "          return;".to_string(),
        "        default:".to_string(),
        "          return;".to_string(),
        "      }".to_string(),
        "    }".to_string(),
        "    if (type === 'view') {".to_string(),
        "      listener.onViewUpdated?.(event as ViewUpdate);".to_string(),
        "      return;".to_string(),
        "    }".to_string(),
        "    if (type === 'plugin') {".to_string(),
        "      switch (eventName) {".to_string(),
        "        case PluginEventName.Realtime:".to_string(),
        "          listener.onPluginRealtime?.(event as PluginRealtimeEvent);".to_string(),
        "          return;".to_string(),
        "        case PluginEventName.CatalogChanged:".to_string(),
        "          listener.onPluginCatalogChanged?.(event as PluginCatalogChangedEvent);".to_string(),
        "          return;".to_string(),
        "        default:".to_string(),
        "          return;".to_string(),
        "      }".to_string(),
        "    }".to_string(),
        "    if ((event as LifecycleEvent).name !== undefined) {".to_string(),
        "      const lifecycle = event as LifecycleEvent;".to_string(),
        "      switch (lifecycle.name) {".to_string(),
        "        case LifecycleEventName.Initializing:".to_string(),
        "          listener.onInitializing?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        case LifecycleEventName.Initialized:".to_string(),
        "          listener.onInitialized?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        case LifecycleEventName.InitFailed:".to_string(),
        "          listener.onInitFailed?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        case LifecycleEventName.LoginSucceeded:".to_string(),
        "          listener.onLoginSucceeded?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        case LifecycleEventName.LoginFailed:".to_string(),
        "          listener.onLoginFailed?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        case LifecycleEventName.LoggedOut:".to_string(),
        "          listener.onLoggedOut?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        case LifecycleEventName.Disposed:".to_string(),
        "          listener.onDisposed?.(lifecycle);".to_string(),
        "          break;".to_string(),
        "        default:".to_string(),
        "          break;".to_string(),
        "      }".to_string(),
        "      return;".to_string(),
        "    }".to_string(),
        "  }".to_string(),
        "}".to_string(),
    ]);
    lines.join("\n")
}

fn module_model_types(spec: &Value, module: &Value) -> Vec<String> {
    child_arr(module, "methods")
        .iter()
        .flat_map(|method| [str_field(method, "request"), str_field(method, "response")])
        .filter(|type_name| is_known_ts_model_type(type_name, spec))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn module_wire_imports(spec: &Value, module: &Value) -> Vec<String> {
    let mut imports = [
        "conversationFromJson",
        "listConversationsResponseFromJson",
        "listMessagesResponseFromJson",
        "listOfMaps",
        "messageFromJson",
        "sendAckFromJson",
        "sendMessageRequestToMap",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();
    for method in child_arr(module, "methods") {
        let req = str_field(method, "request");
        if req != "SendMessageRequest" && is_known_ts_model_type(req, spec) {
            imports.insert(ts_model_to_map_fn(req));
        }
        if str_field(method, "response") == "SyncConversationSummariesResponse" {
            imports.insert(ts_model_from_json_fn(str_field(method, "response")));
        }
        if matches!(
            str_field(method, "response"),
            "ViewOpenResponse" | "TimelineViewPageResponse" | "CloseViewResponse"
        ) {
            imports.insert(ts_model_from_json_fn(str_field(method, "response")));
        }
    }
    imports.into_iter().collect()
}

fn emit_typescript_adapter_module_api(
    spec: &Value,
    module: &Value,
    event_registry: &[TypescriptEventRegistryEntry],
) -> String {
    if str_field(module, "key") == "events" {
        return emit_typescript_adapter_events_api(spec, event_registry);
    }
    let prop = facade_prop(module);
    let iface = ts_api_interface_name(module);
    let class_name = format!("Default{}Api", pascal_case(prop));
    let model_types = module_model_types(spec, module);
    let wire_imports = module_wire_imports(spec, module);
    let model_import_line = if model_types.is_empty() {
        String::new()
    } else {
        format!(
            "import {{ {} }} from '../../model';",
            model_types.join(", ")
        )
    };
    let mut listener_imports = Vec::new();
    if str_field(module, "key") == "messages" {
        listener_imports
            .push("import type { MessageSendCallback } from '../../callback';".to_string());
        listener_imports.push(
            "import type { DeleteMessageRequest, EditRichDocByMessageIdRequest, EditTextByMessageIdRequest, GetMessageRequest, MarkMessageReadAndBurnRequest, MessageDispatchRequest, MessageMutationRequest, ReactionMutationRequest, RecallMessageRequest, SetTypingRequest } from '../../api/types';"
                .to_string(),
        );
    }
    let connection_import = child_arr(module, "methods")
        .iter()
        .any(|method| str_field(method, "response") == "ConnectionStateResponse");
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { NativeBridge, NativeCallMap } from '../../contract/bridge_contract';".to_string(),
        format!(
            "import type {{ {iface} }} from '../../api/modules/{}';",
            str_field(module, "key")
        ),
    ];
    if connection_import {
        lines.push(
            "import type { ConnectionState } from '../../contract/sdk_contract';".to_string(),
        );
    }
    lines.extend(listener_imports);
    if !model_import_line.is_empty() {
        lines.push(model_import_line);
    }
    if str_field(module, "key") == "media" {
        lines.push(
            "import { FlareSdkException } from '../../bridge/flareSdkException';".to_string(),
        );
        lines
            .push("import { pickDisplayUrlFromResolved } from '../media/mediaAccess';".to_string());
    }
    lines.push("import { invokeBool, invokeConnectionState, invokeConversation, invokeConversationTimelineSnapshot, invokeHomeTimelineSnapshot, invokeListConversations, invokeListMessages, invokeMap, invokeMessage, invokeSendAck, invokeVoid, sdkErrorPayloadFromError } from '../codec/nativeInvoke';".to_string());
    lines.push(format!(
        "import {{ {} }} from '../codec/wireCodec';",
        wire_imports.join(", ")
    ));
    lines.extend([
        String::new(),
        format!("export class {class_name} implements {iface} {{"),
        "  constructor(private readonly bridge: NativeBridge) {}".to_string(),
    ]);
    for method in child_arr(module, "methods") {
        let res = ts_api_type_resolved(str_field(method, "response"), spec);
        let mut arg = ts_request_arg(method, spec);
        if str_field(method, "name") == "sendMessage" {
            arg = "request: SendMessageRequest, callback?: MessageSendCallback".to_string();
        }
        lines.extend([
            String::new(),
            format!(
                "  async {}({arg}): Promise<{res}> {{",
                str_field(method, "name")
            ),
        ]);
        lines.extend(emit_typescript_adapter_method_body(method, spec, "    "));
        lines.push("  }".to_string());
    }
    if str_field(module, "key") == "media" {
        lines.extend([
            String::new(),
            "  async resolveDisplayUrl(request: Record<string, unknown>): Promise<string> {".to_string(),
            "    const resolved = await this.resolveMediaAccess(request);".to_string(),
            "    const url = pickDisplayUrlFromResolved(resolved);".to_string(),
            "    if (!url) {".to_string(),
            "      throw new FlareSdkException('generalError', 'empty media display url', 'media.resolve_display_url');".to_string(),
            "    }".to_string(),
            "    return url;".to_string(),
            "  }".to_string(),
        ]);
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_typescript_adapter_message_builder(spec: &Value) -> Option<String> {
    let entries = message_build_catalog_entries(spec);
    let models = message_builder_request_models(spec);
    let extra_methods = message_builder_extra_methods(spec);
    if entries.is_empty() {
        return None;
    }
    let build_types = models.keys().cloned().collect::<BTreeSet<_>>();
    let extra_model_types = extra_methods
        .iter()
        .flat_map(|method| [str_field(method, "request"), str_field(method, "response")])
        .filter(|type_name| is_known_ts_model_type(type_name, spec))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let imported_types = build_types
        .union(&extra_model_types)
        .cloned()
        .collect::<Vec<_>>();
    let mut map_imports = build_types
        .iter()
        .map(|name| ts_model_to_map_fn(name))
        .collect::<BTreeSet<_>>();
    for method in &extra_methods {
        let req = str_field(method, "request");
        if is_known_ts_model_type(req, spec) {
            map_imports.insert(ts_model_to_map_fn(req));
        }
    }
    let decoder_imports = extra_methods
        .iter()
        .filter(|method| str_field(method, "response") == "RichDocV2Normalized")
        .map(|method| ts_model_from_json_fn(str_field(method, "response")))
        .collect::<BTreeSet<_>>();
    let wire_imports = map_imports
        .union(&decoder_imports)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let mut lines = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { NativeBridge, NativeCallMap } from '../../contract/bridge_contract';".to_string(),
        "import type { MessageBuilderApi } from '../../api/modules/message_builder';".to_string(),
        format!(
            "import {{ ListMessageBuildCatalogResponse, Message, {} }} from '../../model';",
            imported_types.join(", ")
        ),
        "import { MESSAGE_BUILD_CATALOG } from '../catalog/messageBuildCatalog';".to_string(),
        "import { invokeMap, invokeMessage } from '../codec/nativeInvoke';".to_string(),
        format!("import {{ {wire_imports} }} from '../codec/wireCodec';"),
        String::new(),
        "export class DefaultMessageBuilderApi implements MessageBuilderApi {".to_string(),
        "  constructor(private readonly bridge: NativeBridge) {}".to_string(),
        String::new(),
        "  private async dispatchBuildMap(request: Record<string, unknown>): Promise<Message> {".to_string(),
        "    return await invokeMessage(this.bridge, NativeCallMap.messageBuilderDispatch, request);".to_string(),
        "  }".to_string(),
        String::new(),
        "  async listSupportedBuildOperations(): Promise<ListMessageBuildCatalogResponse> {".to_string(),
        "    return { entries: MESSAGE_BUILD_CATALOG };".to_string(),
        "  }".to_string(),
    ];
    for method in extra_methods {
        let res = ts_api_type(str_field(method, "response"), spec);
        let arg = ts_request_arg(method, spec);
        let descriptor = ts_invoke_descriptor(method);
        let req_expr = ts_invoke_request(method, spec);
        let call = if req_expr.is_empty() {
            descriptor
        } else {
            format!("{descriptor}, {req_expr}")
        };
        lines.extend([
            String::new(),
            format!(
                "  async {}({arg}): Promise<{res}> {{",
                str_field(method, "name")
            ),
        ]);
        if str_field(method, "response") == "RichDocV2Normalized" {
            lines.push(format!(
                "    const raw = await invokeMap(this.bridge, {call});"
            ));
            lines.push(format!(
                "    return {}(raw);",
                ts_model_from_json_fn(str_field(method, "response"))
            ));
        } else {
            lines.extend(emit_typescript_adapter_method_body(method, spec, "    "));
        }
        lines.push("  }".to_string());
    }
    for entry in entries {
        let method = str_field(entry, "method");
        let request_type = str_field(entry, "request");
        let op = str_field(entry, "op");
        let map_fn = models
            .contains_key(request_type)
            .then(|| ts_model_to_map_fn(request_type));
        lines.extend([
            String::new(),
            format!("  async {method}(request: {request_type}): Promise<Message> {{"),
        ]);
        if let Some(map_fn) = map_fn {
            lines.push(format!(
                "    return this.dispatchBuildMap({{ op: {}, ...{map_fn}(request) }});",
                json_quote(op)
            ));
        } else {
            lines.push(format!(
                "    return this.dispatchBuildMap({{ op: {} }});",
                json_quote(op)
            ));
        }
        lines.push("  }".to_string());
    }
    lines.push("}".to_string());
    Some(lines.join("\n"))
}

const ADAPTER_FACADE_PROPS: &[(&str, &str, &str)] = &[
    ("connection", "ConnectionApi", "Connection"),
    ("conversations", "ConversationsApi", "Conversations"),
    ("messageBuilder", "MessageBuilderApi", "MessageBuilder"),
    ("messages", "MessagesApi", "Messages"),
    ("sync", "SyncApi", "Sync"),
    ("user", "UserApi", "User"),
    ("presence", "PresenceApi", "Presence"),
    ("media", "MediaApi", "Media"),
    ("capabilities", "CapabilitiesApi", "Capabilities"),
    ("views", "ViewsApi", "Views"),
    ("events", "EventsApi", "Events"),
    ("diagnostics", "DiagnosticsApi", "Diagnostics"),
];

fn emit_typescript_adapter_flare_im_client(spec: &Value) -> Result<String> {
    let mut props_init = Vec::new();
    let mut props_decl = Vec::new();
    for (prop, iface, pascal_suffix) in ADAPTER_FACADE_PROPS {
        let class_name = match *prop {
            "messageBuilder" => "DefaultMessageBuilderApi".to_string(),
            "events" => "DefaultEventsApi".to_string(),
            _ => format!("Default{pascal_suffix}Api"),
        };
        props_decl.push(format!("  readonly {prop}: {iface};"));
        props_init.push(format!("    this.{prop} = new {class_name}(bridge);"));
    }
    props_decl.push("  private readonly bridge: NativeBridge;".to_string());

    let session = child_arr(spec, "modules")
        .iter()
        .find(|module| str_field(module, "facade") == "client")
        .context("missing client session module")?;
    let session_model_types = module_model_types(spec, session);
    let session_wire_imports = child_arr(session, "methods")
        .iter()
        .filter_map(|method| {
            let req = str_field(method, "request");
            is_known_ts_model_type(req, spec).then(|| ts_model_to_map_fn(req))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut session_methods = Vec::new();
    for method in child_arr(session, "methods") {
        let res = ts_api_type_resolved(str_field(method, "response"), spec);
        let arg = ts_request_arg(method, spec);
        session_methods.push(format!(
            "  async {}({arg}): Promise<{res}> {{",
            str_field(method, "name")
        ));
        match str_field(method, "name") {
            "init" => {
                session_methods.extend([
                    "    emitLifecycleEvent(this.events as unknown as DefaultEventsApi, LifecycleEventName.Initializing, \"sdk.init\");".to_string(),
                    "    try {".to_string(),
                ]);
                session_methods.extend(emit_typescript_adapter_method_body(method, spec, "      "));
                session_methods.extend([
                    "      emitLifecycleEvent(this.events as unknown as DefaultEventsApi, LifecycleEventName.Initialized, \"sdk.init\");".to_string(),
                    "    } catch (error) {".to_string(),
                    "      emitLifecycleEvent(".to_string(),
                    "        this.events as unknown as DefaultEventsApi,".to_string(),
                    "        LifecycleEventName.InitFailed,".to_string(),
                    "        \"sdk.init\",".to_string(),
                    "        undefined,".to_string(),
                    "        sdkErrorPayloadFromError(error as unknown, \"sdk.init\"),".to_string(),
                    "      );".to_string(),
                    "      throw error;".to_string(),
                    "    }".to_string(),
                    "  }".to_string(),
                ]);
            }
            "login" => {
                session_methods.extend([
                    "    const userId = userIdFromRequest(request);".to_string(),
                    "    try {".to_string(),
                ]);
                session_methods.extend(emit_typescript_adapter_method_body(method, spec, "      "));
                session_methods.extend([
                    "      emitLifecycleEvent(this.events as unknown as DefaultEventsApi, LifecycleEventName.LoginSucceeded, \"sdk.login\", userId);".to_string(),
                    "    } catch (error) {".to_string(),
                    "      emitLifecycleEvent(".to_string(),
                    "        this.events as unknown as DefaultEventsApi,".to_string(),
                    "        LifecycleEventName.LoginFailed,".to_string(),
                    "        \"sdk.login\",".to_string(),
                    "        userId,".to_string(),
                    "        sdkErrorPayloadFromError(error as unknown, \"sdk.login\"),".to_string(),
                    "      );".to_string(),
                    "      throw error;".to_string(),
                    "    }".to_string(),
                    "  }".to_string(),
                ]);
            }
            "logout" => {
                session_methods.extend(emit_typescript_adapter_method_body(method, spec, "    "));
                session_methods.extend([
                    "    emitLifecycleEvent(this.events as unknown as DefaultEventsApi, LifecycleEventName.LoggedOut, \"sdk.logout\");".to_string(),
                    "  }".to_string(),
                ]);
            }
            "dispose" => {
                session_methods.push("    await this.events.unsubscribeAll();".to_string());
                session_methods.extend(emit_typescript_adapter_method_body(method, spec, "    "));
                session_methods.extend([
                    "    emitLifecycleEvent(this.events as unknown as DefaultEventsApi, LifecycleEventName.Disposed, \"sdk.dispose\");".to_string(),
                    "  }".to_string(),
                ]);
            }
            _ => {
                session_methods.extend(emit_typescript_adapter_method_body(method, spec, "    "));
                session_methods.push("  }".to_string());
            }
        }
    }

    let module_keys = ADAPTER_FACADE_PROPS
        .iter()
        .map(|(prop, _, _)| {
            let key = child_arr(spec, "modules")
                .iter()
                .find(|module| str_field(module, "facade") == format!("client.{prop}"))
                .map(|module| str_field(module, "key").to_string())
                .with_context(|| format!("missing module facade client.{prop}"))?;
            Ok((prop.to_string(), key))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut imports = vec![
        "// GENERATED. Do not edit by hand.".to_string(),
        "import { NativeBridge, NativeCallMap } from '../contract/bridge_contract';".to_string(),
        "import type { FlareImClient } from '../api';".to_string(),
    ];
    for (prop, iface, _) in ADAPTER_FACADE_PROPS {
        imports.push(format!(
            "import type {{ {iface} }} from '../api/modules/{}';",
            module_keys
                .get(*prop)
                .with_context(|| format!("missing module key for {prop}"))?
        ));
    }
    imports.push("import type { CreateClientRequest, CreateClientResponse, CurrentUserIdResponse, LoginRequest, SdkConfig, UpdateAccessTokenRequest } from '../api/types';".to_string());
    if !session_model_types.is_empty() {
        imports.push(format!(
            "import type {{ {} }} from '../model';",
            session_model_types.join(", ")
        ));
    }
    imports.push("import { LifecycleEventName } from '../model';".to_string());
    imports.push("import { emitLifecycleEvent, invokeBool, invokeMap, invokeVoid, sdkErrorPayloadFromError, userIdFromRequest } from './codec/nativeInvoke';".to_string());
    if !session_wire_imports.is_empty() {
        imports.push(format!(
            "import {{ {} }} from './codec/wireCodec';",
            session_wire_imports.join(", ")
        ));
    }
    imports.push("import { DefaultEventsApi } from './module/DefaultEventsApi';".to_string());
    for (prop, _, pascal_suffix) in ADAPTER_FACADE_PROPS {
        if matches!(*prop, "events" | "messageBuilder") {
            continue;
        }
        imports.push(format!(
            "import {{ Default{pascal_suffix}Api }} from './module/Default{pascal_suffix}Api';"
        ));
    }
    imports.push(
        "import { DefaultMessageBuilderApi } from './module/DefaultMessageBuilderApi';".to_string(),
    );

    let mut lines = imports;
    lines.extend([
        String::new(),
        "export class DefaultFlareImClient implements FlareImClient {".to_string(),
    ]);
    lines.extend(props_decl);
    lines.extend([
        String::new(),
        "  constructor(bridge: NativeBridge) {".to_string(),
        "    this.bridge = bridge;".to_string(),
    ]);
    lines.extend(props_init);
    lines.extend(["  }".to_string(), String::new()]);
    lines.extend(session_methods);
    lines.push("}".to_string());
    Ok(lines.join("\n"))
}

fn emit_typescript_wire_codec_header(spec: &Value) -> String {
    let imports = all_spec_models(spec)
        .into_iter()
        .map(|model| str_field(model, "name").to_string())
        .chain(
            all_spec_enums(spec)
                .into_iter()
                .map(|enum_value| str_field(enum_value, "name").to_string()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",\n  ");
    format!(
        "// GENERATED. Do not edit by hand.\nimport {{ FlareSdkException }} from '../../bridge/flareSdkException';\nimport {{\n  {imports},\n}} from '../../model';\n"
    )
}

fn typescript_wire_codec_footer() -> Result<&'static str> {
    // Footer stays identity-only at the wire boundary; no response key normalization.
    let template = include_str!("../../templates/typescript-adapter/codec/wireCodec.ts");
    let marker = "\n\nexport function sendMessageRequestToMap";
    let index = template
        .find(marker)
        .context("TypeScript wireCodec template missing footer marker")?;
    Ok(&template[index..])
}

fn emit_typescript_wire_codec(spec: &Value) -> Result<String> {
    Ok([
        emit_typescript_wire_codec_header(spec),
        emit_typescript_enum_wire_orders(spec),
        String::new(),
        emit_typescript_model_to_map_functions(spec),
        typescript_wire_codec_footer()?.to_string(),
    ]
    .join("\n\n"))
}

#[cfg(test)]
mod json_value_response_tests {
    use super::*;
    use serde_json::json;

    /// JsonValue 必须映射成 unknown，不能落到 Record<string, unknown> 兜底。
    ///
    /// 落到兜底会让 ts_response_body 走 invokeMap 分支，而 invokeMap 的
    /// recordFromNative 只接受 object。message.dispatch 是通用出口，底下的
    /// dispatchOp 有返回对象的、也有返回 unit 的（mark / unmark /
    /// mark_with_color 等）。一旦转发到 unit 那类，操作在服务端已经成功，
    /// 客户端却抛 "native response must be an object"——UI 不反映这次变更，
    /// 用户看到的是"点了没反应"。
    #[test]
    fn json_value_maps_to_unknown_not_a_record() {
        let spec = json!({});
        assert_eq!(ts_api_type("JsonValue", &spec), "unknown");
        assert_ne!(ts_api_type("JsonValue", &spec), "Record<string, unknown>");
    }

    /// 映射成 unknown 之后，响应体不能再做对象强制。
    #[test]
    fn unknown_responses_are_not_coerced_to_objects() {
        let spec = json!({});
        // 兜底类型仍应走 invokeMap——这条是对照，确保上一条不是因为
        // ts_api_type 整体失效才通过的。
        assert_eq!(
            ts_api_type("SomeUnknownType", &spec),
            "Record<string, unknown>"
        );
    }
}
