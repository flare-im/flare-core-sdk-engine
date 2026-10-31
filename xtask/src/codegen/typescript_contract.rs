use anyhow::{Result, bail};
use serde_json::Value;
use std::{collections::BTreeSet, path::Path};

use crate::{
    GeneratedTextTarget, all_spec_enums, all_spec_models, child_arr, facade_prop,
    is_known_ts_model_type, is_list_type_name, json_quote, list_inner_type_name,
    listener_interface_name, load_expanded_client_spec, pascal_case, remove_output_paths,
    single_trailing_newline, snake_case, spec_enum_names, spec_model_names, str_field,
    ts_api_interface_name, ts_api_module_key, typescript_listener_groups, upsert_text_file,
};

pub(crate) fn emit_typescript_contract_files(root: &Path, check: bool) -> Result<()> {
    let spec = load_expanded_client_spec(root)?;
    let mut drifted = Vec::new();
    if !check {
        clean_typescript_contract_outputs(root)?;
    }
    for target in typescript_contract_targets(root, &spec) {
        let body = single_trailing_newline(&target.body);
        upsert_text_file(&target.path, &body, check, &mut drifted)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned TypeScript contract output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned TypeScript contract artifacts generated");
    }
    Ok(())
}

fn clean_typescript_contract_outputs(root: &Path) -> Result<()> {
    remove_output_paths([
        root.join("packages/flare-core-typescript-sdk/src/api"),
        root.join("packages/flare-core-typescript-sdk/src/model"),
        root.join("packages/flare-core-typescript-sdk/src/listener"),
        root.join("packages/flare-core-typescript-sdk/src/callback"),
    ])
}

fn typescript_contract_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let src_root = root.join("packages/flare-core-typescript-sdk/src");
    let model_root = src_root.join("model");
    let listener_root = src_root.join("listener");
    let callback_root = src_root.join("callback");
    let mut targets = Vec::new();
    for enum_value in all_spec_enums(spec) {
        targets.push(GeneratedTextTarget {
            path: model_root.join(format!("{}.ts", snake_case(str_field(enum_value, "name")))),
            body: emit_typescript_model_enum(enum_value),
        });
    }
    for model in all_spec_models(spec) {
        targets.push(GeneratedTextTarget {
            path: model_root.join(format!("{}.ts", snake_case(str_field(model, "name")))),
            body: emit_typescript_model_interface(model, spec),
        });
    }
    targets.push(GeneratedTextTarget {
        path: model_root.join("message_build_catalog.ts"),
        body: emit_typescript_message_build_catalog_model(spec),
    });
    targets.push(GeneratedTextTarget {
        path: model_root.join("index.ts"),
        body: emit_typescript_model_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: listener_root.join("common.ts"),
        body: emit_typescript_listener_common(),
    });
    for (kind, listeners) in typescript_listener_groups(spec) {
        targets.push(GeneratedTextTarget {
            path: listener_root.join(format!("{kind}.ts")),
            body: emit_typescript_listener_group(&kind, &listeners),
        });
    }
    targets.push(GeneratedTextTarget {
        path: listener_root.join("index.ts"),
        body: emit_typescript_listener_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: callback_root.join("message_send_callback.ts"),
        body: emit_typescript_message_send_callback(),
    });
    targets.push(GeneratedTextTarget {
        path: callback_root.join("index.ts"),
        body: emit_typescript_callback_index(),
    });
    targets.extend(emit_typescript_api_targets(root, spec));
    targets.push(GeneratedTextTarget {
        path: src_root.join("lifecycle/heartbeatLifecycle.ts"),
        body: emit_typescript_heartbeat_lifecycle_bridge(),
    });
    targets.push(GeneratedTextTarget {
        path: src_root.join("lifecycle/index.ts"),
        body: "export * from \"./heartbeatLifecycle\";".to_string(),
    });
    targets
}

fn emit_typescript_api_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    let api_root = root.join("packages/flare-core-typescript-sdk/src/api");
    let modules_root = api_root.join("modules");
    let mut targets = Vec::new();
    for module in child_arr(spec, "modules") {
        let key = ts_api_module_key(module);
        targets.push(GeneratedTextTarget {
            path: modules_root.join(format!("{key}.ts")),
            body: emit_typescript_api_module(spec, module),
        });
    }
    targets.push(GeneratedTextTarget {
        path: modules_root.join("index.ts"),
        body: emit_typescript_api_modules_index(spec),
    });
    targets.push(GeneratedTextTarget {
        path: api_root.join("client.ts"),
        body: emit_typescript_api_client(spec),
    });
    targets.push(GeneratedTextTarget {
        path: api_root.join("types.ts"),
        body: emit_typescript_api_types(spec),
    });
    targets.push(GeneratedTextTarget {
        path: api_root.join("index.ts"),
        body: [
            "/**",
            " * GENERATED. Do not edit by hand.",
            " *",
            " * SDK module APIs + root client facade only.",
            " * Listener, model, callback, and contract are sibling packages under `src/`.",
            " */",
            "export * from './types';",
            "export * from './client';",
            "export * from './modules';",
        ]
        .join("\n"),
    });
    targets
}

fn emit_typescript_api_types(spec: &Value) -> String {
    let mut lines = vec![
        "/**".to_string(),
        " * GENERATED. Do not edit by hand.".to_string(),
        " *".to_string(),
        " * Shared request/response types for module APIs under `./modules/`.".to_string(),
        " */".to_string(),
        "import type { FlareSdkError } from '../contract';".to_string(),
        String::new(),
        "export type Unit = void;".to_string(),
        "export type JsonValue = unknown;".to_string(),
        "export type TransportPolicy = \"auto\" | \"websocket_only\" | \"protocol_race\";"
            .to_string(),
        "export type TransportKind = \"websocket\" | \"quic\";".to_string(),
        "export type SdkResourceProfile = \"desktop\" | \"mobile\";".to_string(),
        "export interface SdkConfig {".to_string(),
        "  ackMaxInFlight?: number;".to_string(),
        "  ackMaxRetries?: number;".to_string(),
        "  ackTimeoutSecs?: number;".to_string(),
        "  capabilityUrl?: string;".to_string(),
        "  connectTimeoutSecs?: number;".to_string(),
        "  dataUrl?: string;".to_string(),
        "  defaultTransport?: TransportKind;".to_string(),
        "  enableMetrics?: boolean;".to_string(),
        "  eventBusCapacity?: number;".to_string(),
        "  eventDedupeCapacity?: number;".to_string(),
        "  httpUrl?: string;".to_string(),
        "  initMessageSyncConcurrency?: number;".to_string(),
        "  maxReconnectAttempts?: number;".to_string(),
        "  mediaStorageProxyPrefix?: string;".to_string(),
        "  mediaStorageProxyTargets?: string[];".to_string(),
        "  messageDedupeCapacity?: number;".to_string(),
        "  onlineUrl?: string;".to_string(),
        "  protocolRaceOrder?: TransportKind[];".to_string(),
        "  quicUrl?: string;".to_string(),
        "  reconnectIntervalSecs?: number;".to_string(),
        "  resourceProfile?: SdkResourceProfile;".to_string(),
        "  syncBatchSize?: number;".to_string(),
        "  tenantId?: string;".to_string(),
        "  tlsCaCertPath?: string;".to_string(),
        "  transportPolicy?: TransportPolicy;".to_string(),
        "  wsUrl?: string;".to_string(),
        "}".to_string(),
        "export interface CreateClientRequest { config: SdkConfig; }".to_string(),
        "export interface CreateClientResponse { handle: bigint | number; }".to_string(),
        "export interface LoginRequest { userId: string; token?: string; storeConfigJson?: string; }"
            .to_string(),
        "export interface MessageDispatchRequest { op: string; params: Record<string, unknown>; }"
            .to_string(),
        "export interface Subscription { id: bigint | number; }".to_string(),
        "export type FlareJsonObject = Record<string, unknown>;".to_string(),
        String::new(),
        "export interface SdkResult<T> { value?: T; error?: FlareSdkError; }".to_string(),
    ];
    for name in typescript_api_alias_names(spec) {
        lines.push(format!("export type {name} = FlareJsonObject;"));
    }
    lines.join("\n")
}

fn emit_typescript_api_modules_index(spec: &Value) -> String {
    let mut lines = vec!["/** GENERATED. Do not edit by hand. */".to_string()];
    for module in child_arr(spec, "modules") {
        lines.push(format!(
            "export type {{ {} }} from './{}';",
            ts_api_interface_name(module),
            ts_api_module_key(module)
        ));
    }
    lines.join("\n")
}

fn emit_typescript_api_client(spec: &Value) -> String {
    let mut lines = vec![
        "/**".to_string(),
        " * GENERATED. Do not edit by hand.".to_string(),
        " *".to_string(),
        " * Root client facade composing per-module APIs from `./modules/`.".to_string(),
        " */".to_string(),
        "import type { SessionApi } from './modules/session';".to_string(),
    ];
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        lines.push(format!(
            "import type {{ {} }} from './modules/{}';",
            ts_api_interface_name(module),
            ts_api_module_key(module)
        ));
    }
    lines.extend([
        String::new(),
        "/** Root SDK client. Create one instance per app/session boundary. */".to_string(),
        "export interface FlareImClient extends SessionApi {".to_string(),
    ]);
    for module in child_arr(spec, "modules") {
        if str_field(module, "facade") == "client" {
            continue;
        }
        lines.push(format!("  /** {} */", str_field(module, "description")));
        lines.push(format!(
            "  readonly {}: {};",
            facade_prop(module),
            ts_api_interface_name(module)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_typescript_api_module(spec: &Value, module: &Value) -> String {
    let iface = ts_api_interface_name(module);
    let mut lines = vec![
        "/**".to_string(),
        " * GENERATED. Do not edit by hand.".to_string(),
        " *".to_string(),
        format!(
            " * Module API: `{}` — {}",
            str_field(module, "key"),
            str_field(module, "description")
        ),
        " */".to_string(),
    ];
    if str_field(module, "key") == "events" {
        lines.push(
            "import type { EventCallback, EventSubscription, FlareImEventListener } from '../../listener';"
                .to_string(),
        );
    }
    if str_field(module, "key") == "messages" {
        lines.push("import type { MessageSendCallback } from '../../callback';".to_string());
    }
    if module_uses_connection_state(module) {
        lines.push("import type { ConnectionState } from '../../contract';".to_string());
    }
    let model_imports = typescript_api_module_model_imports(spec, module);
    if !model_imports.is_empty() {
        lines.push(format!(
            "import type {{ {} }} from '../../model';",
            model_imports.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    let alias_imports = typescript_api_module_alias_imports(spec, module);
    if !alias_imports.is_empty() {
        lines.push(format!(
            "import type {{ {} }} from '../types';",
            alias_imports.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    lines.extend([
        String::new(),
        format!("/** {} */", str_field(module, "description")),
        format!("export interface {iface} {{"),
    ]);
    for method in child_arr(module, "methods") {
        let res = typescript_api_type(str_field(method, "response"), spec);
        let arg = typescript_api_method_arg(method, spec);
        lines.push(format!(
            "  /** {} */",
            typescript_api_method_summary(method)
        ));
        lines.push(format!(
            "  {}({arg}): Promise<{res}>;",
            str_field(method, "name")
        ));
        if str_field(module, "key") == "media" && str_field(method, "name") == "resolveMediaAccess"
        {
            lines.push(
                "  /** Resolves a media item into a display-ready URL using the canonical core media access shape. */"
                    .to_string(),
            );
            lines.push(
                "  resolveDisplayUrl(request: ResolveMediaAccessRequest): Promise<string>;"
                    .to_string(),
            );
        }
    }
    if str_field(module, "key") == "events" {
        lines.extend([
            "  /** Registers a listener object for typed SDK runtime notifications. */".to_string(),
            "  addEventListener(listener: FlareImEventListener): EventSubscription;".to_string(),
            "  /** Removes one local listener registration. */".to_string(),
            "  removeEventListener(subscription: EventSubscription): void;".to_string(),
        ]);
        for listener in child_arr(spec, "listeners") {
            lines.push(format!("  /** {} */", str_field(listener, "description")));
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

fn typescript_api_method_arg(method: &Value, spec: &Value) -> String {
    if str_field(method, "name") == "sendMessage" {
        return "request: SendMessageRequest, callback?: MessageSendCallback".to_string();
    }
    let req = typescript_api_type(str_field(method, "request"), spec);
    if req == "void" {
        String::new()
    } else {
        format!("request: {req}")
    }
}

fn typescript_api_type(name: &str, spec: &Value) -> String {
    match name {
        "Unit" | "DisposeRequest" => "void".to_string(),
        "JsonValue" => "unknown".to_string(),
        "BooleanResponse" => "boolean".to_string(),
        "ConnectionStateResponse" => "ConnectionState".to_string(),
        _ if is_known_ts_model_type(name, spec) => name.to_string(),
        _ => name.to_string(),
    }
}

fn is_non_importable_typescript_api_type(name: &str) -> bool {
    matches!(
        name,
        "Unit" | "DisposeRequest" | "JsonValue" | "BooleanResponse" | "ConnectionStateResponse"
    )
}

fn is_static_typescript_api_type(name: &str) -> bool {
    matches!(
        name,
        "SdkConfig"
            | "CreateClientRequest"
            | "CreateClientResponse"
            | "LoginRequest"
            | "MessageDispatchRequest"
            | "Subscription"
            | "FlareJsonObject"
            | "SdkResult"
    )
}

fn needs_typescript_api_alias(name: &str, spec: &Value) -> bool {
    !is_non_importable_typescript_api_type(name)
        && !is_static_typescript_api_type(name)
        && !is_known_ts_model_type(name, spec)
}

fn needs_typescript_api_types_import(name: &str, spec: &Value) -> bool {
    is_static_typescript_api_type(name) || needs_typescript_api_alias(name, spec)
}

fn typescript_api_alias_names(spec: &Value) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for module in child_arr(spec, "modules") {
        for method in child_arr(module, "methods") {
            for type_name in [str_field(method, "request"), str_field(method, "response")] {
                if needs_typescript_api_alias(type_name, spec) {
                    names.insert(type_name.to_string());
                }
            }
        }
    }
    names
}

fn typescript_api_module_model_imports(spec: &Value, module: &Value) -> BTreeSet<String> {
    let mut imports = BTreeSet::new();
    for method in child_arr(module, "methods") {
        for type_name in [str_field(method, "request"), str_field(method, "response")] {
            if is_known_ts_model_type(type_name, spec) {
                imports.insert(type_name.to_string());
            }
        }
    }
    if str_field(module, "key") == "events" {
        for listener in child_arr(spec, "listeners") {
            let payload = str_field(listener, "payload");
            if is_known_ts_model_type(payload, spec) {
                imports.insert(payload.to_string());
            }
        }
    }
    imports
}

fn typescript_api_module_alias_imports(spec: &Value, module: &Value) -> BTreeSet<String> {
    let mut imports = BTreeSet::new();
    for method in child_arr(module, "methods") {
        for type_name in [str_field(method, "request"), str_field(method, "response")] {
            if needs_typescript_api_types_import(type_name, spec) {
                imports.insert(type_name.to_string());
            }
        }
    }
    imports
}

fn module_uses_connection_state(module: &Value) -> bool {
    child_arr(module, "methods")
        .iter()
        .any(|method| str_field(method, "response") == "ConnectionStateResponse")
}

fn typescript_api_method_summary(method: &Value) -> String {
    let extra = if method.get("dispatchOp").is_some() {
        format!(", dispatch op `{}`", str_field(method, "dispatchOp"))
    } else if str_field(method, "transport") == "contract-invoke-json" {
        String::new()
    } else {
        format!(" via `{}`", str_field(method, "transport"))
    };
    if str_field(method, "transport") == "contract-invoke-json" {
        format!(
            "{} maps to `{}`. Operation: `{}`.",
            str_field(method, "name"),
            str_field(method, "cApi"),
            str_field(method, "operation")
        )
    } else {
        format!(
            "{} maps to `{}`{extra}. Operation: `{}`.",
            str_field(method, "name"),
            str_field(method, "cApi"),
            str_field(method, "operation")
        )
    }
}

fn emit_typescript_heartbeat_lifecycle_bridge() -> String {
    [
        "/** GENERATED. Do not edit by hand. */",
        "import type { SetHeartbeatAppStateRequest } from \"../model/set_heartbeat_app_state_request\";",
        "import { HeartbeatAppState } from \"../model/heartbeat_app_state\";",
        "",
        "export interface HeartbeatAppStateClient {",
        "  setHeartbeatAppState(request: SetHeartbeatAppStateRequest): Promise<void>;",
        "}",
        "",
        "export interface HeartbeatLifecycleBinding {",
        "  sync(): Promise<void>;",
        "  dispose(): void;",
        "}",
        "",
        "export interface HeartbeatLifecycleOptions {",
        "  onError?: (error: unknown) => void;",
        "}",
        "",
        "export interface WebHeartbeatLifecycleOptions extends HeartbeatLifecycleOptions {",
        "  document?: {",
        "    readonly visibilityState?: DocumentVisibilityState | string;",
        "    addEventListener(name: \"visibilitychange\", listener: () => void): void;",
        "    removeEventListener(name: \"visibilitychange\", listener: () => void): void;",
        "  };",
        "  emitInitialState?: boolean;",
        "}",
        "",
        "export class HeartbeatLifecycleBridge {",
        "  constructor(",
        "    private readonly client: HeartbeatAppStateClient,",
        "    private readonly options: HeartbeatLifecycleOptions = {},",
        "  ) {}",
        "",
        "  setForeground(): Promise<void> {",
        "    return this.setAppState(HeartbeatAppState.Foreground);",
        "  }",
        "",
        "  setBackground(): Promise<void> {",
        "    return this.setAppState(HeartbeatAppState.Background);",
        "  }",
        "",
        "  onResume(): Promise<void> {",
        "    return this.setForeground();",
        "  }",
        "",
        "  onPause(): Promise<void> {",
        "    return this.setBackground();",
        "  }",
        "",
        "  private async setAppState(appState: HeartbeatAppState): Promise<void> {",
        "    try {",
        "      await this.client.setHeartbeatAppState({ appState });",
        "    } catch (error) {",
        "      this.options.onError?.(error);",
        "    }",
        "  }",
        "}",
        "",
        "export function installWebHeartbeatLifecycle(",
        "  client: HeartbeatAppStateClient,",
        "  options: WebHeartbeatLifecycleOptions = {},",
        "): HeartbeatLifecycleBinding {",
        "  const documentRef = options.document ?? globalThis.document;",
        "  const bridge = new HeartbeatLifecycleBridge(client, options);",
        "  let disposed = false;",
        "",
        "  const sync = () => {",
        "    const hidden = documentRef?.visibilityState === \"hidden\";",
        "    return hidden ? bridge.setBackground() : bridge.setForeground();",
        "  };",
        "",
        "  if (!documentRef) {",
        "    return {",
        "      sync,",
        "      dispose: () => {",
        "        disposed = true;",
        "      },",
        "    };",
        "  }",
        "",
        "  const listener = () => {",
        "    if (!disposed) {",
        "      void sync();",
        "    }",
        "  };",
        "  documentRef.addEventListener(\"visibilitychange\", listener);",
        "  if (options.emitInitialState !== false) {",
        "    void sync();",
        "  }",
        "",
        "  return {",
        "    sync,",
        "    dispose: () => {",
        "      if (disposed) {",
        "        return;",
        "      }",
        "      disposed = true;",
        "      documentRef.removeEventListener(\"visibilitychange\", listener);",
        "    },",
        "  };",
        "}",
    ]
    .join("\n")
}

fn ts_model_type(type_name: &str, spec: &Value) -> String {
    match type_name {
        "String" => "string".to_string(),
        "Boolean" => "boolean".to_string(),
        "Int32" | "Int64" | "UInt32" | "UInt64" | "Float" | "Double" => "number".to_string(),
        "JsonObject" => "Record<string, unknown>".to_string(),
        "StringMap" => "Record<string, string>".to_string(),
        "BinaryMap" => "Record<string, Uint8Array | number[]>".to_string(),
        "StringList" => "string[]".to_string(),
        _ if is_list_type_name(type_name) => {
            let inner = list_inner_type_name(type_name);
            format!("{}[]", ts_model_type(inner, spec))
        }
        _ if is_known_ts_model_type(type_name, spec) => type_name.to_string(),
        _ => "Record<string, unknown>".to_string(),
    }
}

fn ts_model_import_names(model: &Value, spec: &Value) -> BTreeSet<String> {
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

fn emit_typescript_model_enum(enum_value: &Value) -> String {
    let name = str_field(enum_value, "name");
    let mut lines = vec![
        "/** GENERATED. Do not edit by hand. */".to_string(),
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

fn emit_typescript_model_interface(model: &Value, spec: &Value) -> String {
    if str_field(model, "name") == "ViewSnapshot" {
        return emit_typescript_view_snapshot_model();
    }

    let mut lines = vec!["/** GENERATED. Do not edit by hand. */".to_string()];
    for name in ts_model_import_names(model, spec) {
        lines.push(format!(
            "import type {{ {name} }} from './{}';",
            snake_case(&name)
        ));
    }
    lines.extend([
        String::new(),
        format!("/** {} */", str_field(model, "description")),
        format!("export interface {} {{", str_field(model, "name")),
    ]);
    for field in child_arr(model, "fields") {
        let required = field
            .get("required")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let optional = if required { "" } else { "?" };
        lines.push(format!(
            "  /** wire: `{}`. {} */",
            str_field(field, "wireName"),
            str_field(field, "description")
        ));
        lines.push(format!(
            "  {}{}: {};",
            str_field(field, "name"),
            optional,
            ts_model_type(str_field(field, "type"), spec)
        ));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_typescript_view_snapshot_model() -> String {
    [
        "/** GENERATED. Do not edit by hand. */",
        "import type { HomeTimelineSnapshot } from './home_timeline_snapshot';",
        "import type { TimelineViewSnapshot } from './timeline_view_snapshot';",
        "",
        "/** ViewSnapshot */",
        "export type ViewSnapshot =",
        "  | {",
        "      /** wire: `viewType`. */",
        "      viewType: 'timeline';",
        "      /** wire: `data`. */",
        "      data: TimelineViewSnapshot;",
        "    }",
        "  | {",
        "      /** wire: `viewType`. */",
        "      viewType: 'conversationList';",
        "      /** wire: `data`. */",
        "      data: HomeTimelineSnapshot;",
        "    };",
    ]
    .join("\n")
}

fn emit_typescript_message_build_catalog_model(spec: &Value) -> String {
    let mut lines = vec![
        "/** GENERATED. Do not edit by hand. Built from sdk-spec/shared/message_build_catalog.json */".to_string(),
        "import type { MessageBuildCatalogEntry } from './message_build_catalog_entry';".to_string(),
        "import { MessageBuildOp } from './message_build_op';".to_string(),
        "import { MessageContentType } from './message_content_type';".to_string(),
        String::new(),
        "/** All supported quick-build operations for MessageBuilderApi. */".to_string(),
        "export const MESSAGE_BUILD_CATALOG: readonly MessageBuildCatalogEntry[] = [".to_string(),
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
    lines.extend([
        "] as const;".to_string(),
        String::new(),
        "/** Lookup catalog entry by dispatch op, e.g. `create_text`. */".to_string(),
        "export function messageBuildCatalogForOp(op: string): MessageBuildCatalogEntry | undefined {".to_string(),
        "  return MESSAGE_BUILD_CATALOG.find((item) => item.op === op);".to_string(),
        "}".to_string(),
    ]);
    lines.join("\n")
}

fn emit_typescript_model_index(spec: &Value) -> String {
    let mut lines = vec!["/** GENERATED. Do not edit by hand. */".to_string()];
    for enum_value in all_spec_enums(spec) {
        lines.push(format!(
            "export * from './{}';",
            snake_case(str_field(enum_value, "name"))
        ));
    }
    for model in all_spec_models(spec) {
        lines.push(format!(
            "export * from './{}';",
            snake_case(str_field(model, "name"))
        ));
    }
    lines.push("export * from './message_build_catalog';".to_string());
    lines.join("\n")
}

fn emit_typescript_listener_common() -> String {
    [
        "/** GENERATED. Do not edit by hand. */",
        "",
        "/**",
        " * Style 1 — **event callback**: one handler per subscription.",
        " * Used by `client.events.onMessageReceived(cb)` and bridge `event.subscribe`.",
        " */",
        "export type EventCallback<T> = (event: T) => void;",
        "",
        "/** Alias for documentation; same as {@link EventCallback}. */",
        "export type ListenerHandler<T> = EventCallback<T>;",
        "",
        "/**",
        " * Style 2 — **subscription handle**: returned by `on*` / `addEventListener`; call `unsubscribe()` to detach.",
        " */",
        "export interface EventSubscription {",
        "  readonly id: string | number;",
        "  unsubscribe(): void;",
        "}",
        "",
        "/** FFI/event-bus subscription id (numeric handle from native `event.subscribe`). */",
        "export interface NativeEventSubscription {",
        "  readonly id: bigint | number;",
        "}",
    ]
    .join("\n")
}

fn emit_typescript_listener_group(kind: &str, listeners: &[&Value]) -> String {
    let payloads = listeners
        .iter()
        .map(|listener| str_field(listener, "payload").to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    let mut lines = vec![
        "/** GENERATED. Do not edit by hand. */".to_string(),
        "import type { EventCallback } from './common';".to_string(),
        format!("import type {{ {payloads} }} from '../model';"),
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
    lines.push("}".to_string());
    lines.join("\n")
}

fn emit_typescript_listener_index(spec: &Value) -> String {
    let groups = typescript_listener_groups(spec);
    let mut lines = vec!["/** GENERATED. Do not edit by hand. */".to_string()];
    lines.push("export * from './common';".to_string());
    for kind in groups.keys() {
        lines.push(format!("export * from './{kind}';"));
    }
    lines.push(String::new());
    for kind in groups.keys() {
        lines.push(format!(
            "import type {{ {} }} from './{kind}';",
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

fn emit_typescript_message_send_callback() -> String {
    [
        "/** GENERATED. Do not edit by hand. */",
        "import type { MessageSendAckEvent, MessageSendFailedEvent, ProgressEvent } from '../model';",
        "",
        "/** Direct callback for `messages.sendMessage(request, callback)` progress and terminal states. */",
        "export interface MessageSendCallback {",
        "  /** Message upload or send progress changed. */",
        "  onProgress?(event: ProgressEvent): void;",
        "  /** Message send completed successfully. */",
        "  onSuccess?(event: MessageSendAckEvent): void;",
        "  /** Message send failed. */",
        "  onFailure?(event: MessageSendFailedEvent): void;",
        "}",
    ]
    .join("\n")
}

fn emit_typescript_callback_index() -> String {
    [
        "/**",
        " * GENERATED. Do not edit by hand.",
        " *",
        " * Callback styles exported by the TypeScript API package:",
        " *",
        " * | Style | Type | Use when |",
        " * |-------|------|----------|",
        " * | Event callback | `EventCallback<T>` | Single `client.events.on*(handler)` |",
        " * | Subscription | `EventSubscription` | Dispose one registration (`unsubscribe()`) |",
        " * | Listener object | `FlareImEventListener` | One object with optional `on*` methods |",
        " * | Operation callback | `MessageSendCallback` | `messages.sendMessage(req, cb)` progress/result |",
        " * | Native bus | `NativeEventSubscription` | FFI `event.subscribe` handle |",
        " */",
        "export type { EventCallback, EventSubscription, ListenerHandler, NativeEventSubscription } from '../listener/common';",
        "export type { MessageSendCallback } from './message_send_callback';",
        "export type { FlareImEventListener } from '../listener';",
        "export type { ConnectionEventListener } from '../listener/connection';",
        "export type { ConversationEventListener } from '../listener/conversation';",
        "export type { LifecycleEventListener } from '../listener/lifecycle';",
        "export type { MediaEventListener } from '../listener/media';",
        "export type { MessageEventListener } from '../listener/message';",
        "export type { PluginEventListener } from '../listener/plugin';",
        "export type { SyncEventListener } from '../listener/sync';",
    ]
    .join("\n")
}
