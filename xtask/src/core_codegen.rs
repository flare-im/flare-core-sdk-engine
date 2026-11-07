use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use flare_im_core_sdk::client::SdkConfigOverlay;
use flare_im_core_sdk::model::conversation::ConversationType as SdkConversationType;
use flare_im_core_sdk::model::message::{MessageLocalState, ReactionEntry};
use flare_im_core_sdk::model::{
    BootstrapHomeTimelineRequest, Conversation, ConversationListQuery, ConversationParticipant,
    ConversationTimelineSnapshot, ConversationVersion, Elem, HomeTimelineSnapshot, IMMessage,
    MediaAccessUrl, MediaResolvedAccess, MessagePreviewElem, MessageSearchKind, MessageSearchQuery,
    OpenConversationTimelineRequest, StartupHomeSyncRequest, StartupHomeSyncResponse,
    SyncConversationSummariesRequest, SyncConversationSummariesResponse, TimelineSyncState,
    UploadedMedia,
};
use flare_im_core_sdk::schemars::schema_for;
use flare_im_core_sdk::serde::Deserialize;
use flare_im_core_sdk::serde_json::{self, Value};
use flare_proto::common::ConversationType as ProtoConversationType;

pub(crate) fn run(command: &str) -> Result<()> {
    let root = workspace_root()?;
    ensure_no_python_contract_tools(&root)?;
    let contracts = Contracts::load(&root)?;
    let client_config = ClientConfigContract::load(&root)?;

    match command {
        "verify" => {
            contracts.verify()?;
            client_config.verify()?;
            SchemaCatalog::new().verify()?;
            ensure_conversation_type_proto_parity()?;
            println!("contract verify passed");
        }
        "schema" => {
            SchemaCatalog::new().write(&root, false)?;
        }
        "schema-check" => {
            SchemaCatalog::new().write(&root, true)?;
        }
        "codegen" => {
            contracts.verify()?;
            client_config.verify()?;
            ensure_conversation_type_proto_parity()?;
            SchemaCatalog::new().write(&root, false)?;
            contracts.write_contract_outputs(&root, false)?;
            contracts.write_dispatch_outputs(&root, false)?;
            contracts.write_platform_outputs(&root, false)?;
            contracts.write_direct_invoke_outputs(&root, false)?;
            contracts.write_c_typed_abi_outputs(&root, false)?;
            contracts.write_event_outputs(&root, false)?;
            client_config.write(&root, false)?;
        }
        "check" => {
            contracts.verify()?;
            client_config.verify()?;
            ensure_conversation_type_proto_parity()?;
            SchemaCatalog::new().write(&root, true)?;
            contracts.write_contract_outputs(&root, true)?;
            contracts.write_dispatch_outputs(&root, true)?;
            contracts.write_platform_outputs(&root, true)?;
            contracts.write_direct_invoke_outputs(&root, true)?;
            contracts.write_c_typed_abi_outputs(&root, true)?;
            contracts.write_event_outputs(&root, true)?;
            client_config.write(&root, true)?;
        }
        "help" | "-h" | "--help" => print_help(),
        other => {
            print_help();
            bail!("unknown xtask command: {other}");
        }
    }

    Ok(())
}

fn print_help() {
    eprintln!("Usage: cargo xtask <schema|schema-check|core-codegen|core-codegen-check>");
    eprintln!(
        "  verify        Validate binding contract source files and generated model schema rules"
    );
    eprintln!("  schema        Generate Rust DTO JSON Schema artifacts");
    eprintln!("  schema-check  Verify generated Rust DTO JSON Schema artifacts are fresh");
    eprintln!("  codegen       Verify and generate binding artifacts");
    eprintln!("  check         Verify and assert generated binding artifacts are fresh");
}

fn ensure_no_python_contract_tools(root: &Path) -> Result<()> {
    let tools = root.join("bindings/contract/tools");
    let mut offenders = Vec::new();
    if tools.exists() {
        collect_python_contract_tool_offenders(&tools, &mut offenders)?;
    }

    if !offenders.is_empty() {
        bail!(
            "Python contract generators are retired; keep Rust codegen as the single generator: {}",
            offenders.join(", ")
        );
    }

    Ok(())
}

fn ensure_conversation_type_proto_parity() -> Result<()> {
    let expected = [
        (
            SdkConversationType::Unspecified,
            ProtoConversationType::Unspecified,
        ),
        (SdkConversationType::Single, ProtoConversationType::Single),
        (SdkConversationType::Group, ProtoConversationType::Group),
        (SdkConversationType::Ai, ProtoConversationType::Ai),
        (SdkConversationType::System, ProtoConversationType::System),
        (
            SdkConversationType::Customer,
            ProtoConversationType::Customer,
        ),
        (SdkConversationType::Temp, ProtoConversationType::Temp),
        (SdkConversationType::Channel, ProtoConversationType::Channel),
        (
            SdkConversationType::Broadcast,
            ProtoConversationType::Broadcast,
        ),
    ];

    let sdk_order = SdkConversationType::wire_order();
    let expected_order = expected.iter().map(|(sdk, _)| *sdk).collect::<Vec<_>>();
    if sdk_order != expected_order.as_slice() {
        bail!("SDK ConversationType wire order does not match proto parity contract");
    }

    for (sdk, proto) in expected {
        let sdk_wire_value = sdk.to_proto_int();
        let proto_wire_value = proto as i32;
        if sdk_wire_value != proto_wire_value {
            bail!(
                "ConversationType parity mismatch for {:?}: sdk={} proto={}",
                sdk,
                sdk_wire_value,
                proto_wire_value
            );
        }
    }
    Ok(())
}

fn collect_python_contract_tool_offenders(path: &Path, offenders: &mut Vec<String>) -> Result<()> {
    for entry in
        std::fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", path.display()))?;
        let child = entry.path();
        if child.file_name().is_some_and(|name| name == "__pycache__")
            || child
                .extension()
                .is_some_and(|extension| matches!(extension.to_str(), Some("py" | "pyc" | "pyo")))
        {
            offenders.push(child.display().to_string());
            continue;
        }
        if entry
            .file_type()
            .with_context(|| format!("failed to read file type for {}", child.display()))?
            .is_dir()
        {
            collect_python_contract_tool_offenders(&child, offenders)?;
        }
    }
    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map(Path::to_path_buf)
        .context("xtask crate must live under the flare-im-core-sdk workspace")
}

struct GeneratedOutput {
    path: PathBuf,
    content: String,
}

#[derive(Debug)]
struct CDispatchChannel {
    symbol: String,
    channel: String,
    runtime_group: String,
}

#[derive(Debug)]
struct ApiOperationRow {
    id: String,
    module: String,
    core: Option<String>,
    c_symbol: Option<String>,
    c_dispatch_op: Option<String>,
    tauri: Option<String>,
    dev_only: bool,
}

#[derive(Debug)]
struct MessageBuildOpRow {
    op: String,
    method: String,
    source_operation: String,
}

#[derive(Debug)]
struct ClientConfigContract {
    doc: Value,
}

impl ClientConfigContract {
    fn load(root: &Path) -> Result<Self> {
        let path = root.join("bindings/contract/client_config.json");
        let data = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let doc = serde_json::from_str(&data)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(Self { doc })
    }

    fn verify(&self) -> Result<()> {
        self.init_request_example()?;
        self.sdk_config_example()?;
        ensure_client_config_keys_are_camel_case(&self.doc)?;
        ensure_client_config_has_no_removed_aliases(&self.doc)
    }

    fn write(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(self.outputs(root)?, check, "cargo xtask codegen")
    }

    fn outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        Ok(vec![
            GeneratedOutput {
                path: root.join("bindings/shared/src/generated/client_config.rs"),
                content: self.render_runtime()?,
            },
            GeneratedOutput {
                path: root.join("bindings/wasm/src/generated/client_config.rs"),
                content: self.render_wasm(),
            },
            GeneratedOutput {
                path: root.join("bindings/uniffi/src/generated/client_config.rs"),
                content: self.render_uniffi(),
            },
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/client_config.rs"),
                content: self.render_c()?,
            },
        ])
    }

    fn init_request_example(&self) -> Result<&Value> {
        self.doc
            .pointer("/initRequest/example")
            .filter(|value| value.is_object())
            .context("client_config.json must define object initRequest.example")
    }

    fn sdk_config_example(&self) -> Result<&Value> {
        self.doc
            .pointer("/initRequest/example/sdkConfig")
            .filter(|value| value.is_object())
            .context("client_config.json must define object initRequest.example.sdkConfig")
    }

    fn render_runtime(&self) -> Result<String> {
        let schema = serde_json::to_string(&self.doc)
            .context("failed to serialize client config contract JSON")?;
        let example = serde_json::to_string_pretty(self.init_request_example()?)
            .context("failed to serialize client init example JSON")?;
        Ok(format!(
            "{header}\n\
             /// Full client config contract document (JSON).\n\
             pub const CLIENT_CONFIG_CONTRACT_JSON: &str = {schema};\n\n\
             /// Example `client.init` / `sdk.init` request body.\n\
             pub const CLIENT_INIT_REQUEST_EXAMPLE_JSON: &str = {example};\n",
            header = client_config_header(),
            schema = rust_raw_string_literal(&schema),
            example = rust_raw_string_literal(&example),
        ))
    }

    fn render_wasm(&self) -> String {
        format!(
            "{}\n{}",
            client_config_header(),
            concat!(
                "use wasm_bindgen::prelude::*;\n\n",
                "/// Example init JSON for Web hosts (same shape as C/Tauri `sdkConfig`).\n",
                "#[wasm_bindgen(js_name = flareClientInitExampleJson)]\n",
                "pub fn flare_client_init_example_json() -> String {\n",
                "    flare_im_core_sdk_bindings_runtime::CLIENT_INIT_REQUEST_EXAMPLE_JSON.to_string()\n",
                "}\n\n",
                "/// Full client config contract (access, app, timeouts, TLS).\n",
                "#[wasm_bindgen(js_name = flareClientConfigContractJson)]\n",
                "pub fn flare_client_config_contract_json() -> String {\n",
                "    flare_im_core_sdk_bindings_runtime::CLIENT_CONFIG_CONTRACT_JSON.to_string()\n",
                "}\n",
            ),
        )
    }

    fn render_uniffi(&self) -> String {
        format!(
            "{}\n{}",
            client_config_header(),
            concat!(
                "/// Init/config contract JSON for mobile FFI planners.\n",
                "pub fn client_config_contract_json() -> String {\n",
                "    flare_im_core_sdk_bindings_runtime::CLIENT_CONFIG_CONTRACT_JSON.to_string()\n",
                "}\n\n",
                "pub fn client_init_request_example_json() -> String {\n",
                "    flare_im_core_sdk_bindings_runtime::CLIENT_INIT_REQUEST_EXAMPLE_JSON.to_string()\n",
                "}\n",
            ),
        )
    }

    fn render_c(&self) -> Result<String> {
        let example = serde_json::to_string_pretty(self.sdk_config_example()?)
            .context("failed to serialize C SDK config example JSON")?;
        Ok(format!(
            "{header}\n\
             //! C `flare_sdk_init` accepts UTF-8 JSON matching [flare_im_core_sdk::client::SdkConfigOverlay].\n\
             //! Optional wrapper: `{{ \"environment\": \"...\", \"sdkConfig\": {{ ... }} }}` via `flare_sdk_invoke_json(\"client.init\", ...)`.\n\n\
             /// Example SdkConfigOverlay JSON (development access endpoints + app info).\n\
             pub const FLARE_SDK_CONFIG_EXAMPLE_JSON: &str = {example};\n",
            header = client_config_header(),
            example = rust_raw_string_literal(&example),
        ))
    }
}

fn client_config_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/client_config.json\n"
}

fn contract_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/*.json\n"
}

fn c_events_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/events.json\n"
}

fn c_errors_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/errors.json\n"
}

fn write_generated_outputs(
    outputs: Vec<GeneratedOutput>,
    check: bool,
    command: &str,
) -> Result<()> {
    for output in outputs {
        if check {
            let current = std::fs::read_to_string(&output.path)
                .with_context(|| format!("generated output missing: {}", output.path.display()))?;
            if current != output.content {
                bail!(
                    "generated output is stale: {}; run `{command}`",
                    output.path.display()
                );
            }
        } else {
            if let Some(parent) = output.path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            std::fs::write(&output.path, output.content)
                .with_context(|| format!("failed to write {}", output.path.display()))?;
        }
    }
    Ok(())
}

fn rust_raw_string_literal(content: &str) -> String {
    for hashes in 1..=8 {
        let hashes = "#".repeat(hashes);
        if !content.contains(&format!("\"{hashes}")) {
            return format!("r{hashes}\"{content}\"{hashes}");
        }
    }
    panic!("generated JSON contains unsupported raw string delimiter");
}

fn rust_string_literal(content: &str) -> String {
    serde_json::to_string(content).expect("string literal serialization cannot fail")
}

fn rust_option_string(value: Option<&str>) -> String {
    value
        .map(|value| format!("Some({})", rust_string_literal(value)))
        .unwrap_or_else(|| "None".to_string())
}

fn rustfmt_generated_rust(content: &str) -> Result<String> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "flare-im-core-sdk-xtask-{}-{unique}.rs",
        std::process::id()
    ));

    std::fs::write(&path, content)
        .with_context(|| format!("failed to write rustfmt temp file {}", path.display()))?;
    let output = Command::new("rustfmt")
        .arg("--edition")
        .arg("2024")
        .arg("--config")
        .arg("skip_children=true")
        .arg(&path)
        .output()
        .with_context(|| format!("failed to spawn rustfmt for {}", path.display()))?;

    if !output.status.success() {
        let _ = std::fs::remove_file(&path);
        bail!(
            "rustfmt failed for generated Rust output: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let formatted = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read rustfmt temp file {}", path.display()))?;
    let _ = std::fs::remove_file(&path);
    Ok(formatted)
}

fn direct_invoke_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/direct_invoke.json\n"
}

fn platform_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/apis.json\n"
}

fn c_typed_abi_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/c_typed_abi.json\n"
}

fn dispatch_header() -> &'static str {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/dispatch.json\n"
}

fn owned_clone_expr(value_expr: &str) -> String {
    value_expr.strip_prefix('&').map_or_else(
        || format!("{value_expr}.clone()"),
        |inner| format!("{inner}.clone()"),
    )
}

fn wire_key(name: &str) -> String {
    if name.starts_with('@') {
        return name.to_string();
    }
    let mut parts = name.split('_');
    let Some(first) = parts.next() else {
        return String::new();
    };
    let mut out = first.to_string();
    for part in parts {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            out.extend(chars);
        }
    }
    out
}

fn render_generated_mod(modules: &[&str]) -> String {
    let mut lines = modules
        .iter()
        .map(|module| format!("pub mod {module};"))
        .collect::<Vec<_>>();
    lines.push(String::new());
    lines.join("\n")
}

fn render_shared_generated_mod() -> String {
    [
        "pub mod client_config;\n",
        "pub mod contract;\n",
        "pub mod direct_invoke;\n",
        "pub mod dispatch;\n",
        "pub mod event_codes;\n",
        "pub mod event_registry;\n",
        "\n",
        "pub use client_config::{CLIENT_CONFIG_CONTRACT_JSON, CLIENT_INIT_REQUEST_EXAMPLE_JSON};\n",
        "pub use dispatch::{\n",
        "    CAPABILITY_DISPATCH_OPERATIONS, CONVERSATION_DISPATCH_OPERATIONS,\n",
        "    MEDIA_DISPATCH_OPERATIONS, MESSAGE_BUILD_OPERATIONS, MESSAGE_DISPATCH_OPERATIONS,\n",
        "};\n",
    ]
    .concat()
}

fn c_api_first_parts(value: Option<&Value>) -> (Option<String>, Option<String>) {
    c_api_entries(value)
        .into_iter()
        .next()
        .map_or((None, None), |(symbol, dispatch)| (Some(symbol), dispatch))
}

fn build_op_from_api_id(api_id: &str) -> Option<String> {
    let op = api_id.strip_prefix("message_builder.")?;
    op.starts_with("create_").then(|| op.to_string())
}

fn method_name_for_build_op(op: &str) -> String {
    format!(
        "build{}",
        pascal_case(op.strip_prefix("create_").unwrap_or(op), '_')
    )
}

fn message_build_ops(api_operations: &[ApiOperationRow]) -> Vec<MessageBuildOpRow> {
    let mut rows = BTreeMap::<String, MessageBuildOpRow>::new();
    for operation in api_operations {
        let Some(op) = build_op_from_api_id(&operation.id) else {
            continue;
        };
        rows.entry(op.clone()).or_insert_with(|| MessageBuildOpRow {
            method: method_name_for_build_op(&op),
            op,
            source_operation: operation.id.clone(),
        });
    }
    rows.into_values().collect()
}

fn uniffi_error_variant(name: &str) -> String {
    let stem = name
        .strip_prefix("FLARE_")
        .unwrap_or(name)
        .strip_prefix("ERR_")
        .unwrap_or_else(|| name.strip_prefix("FLARE_").unwrap_or(name));
    if stem == "OK" {
        return "Ok".to_string();
    }
    pascal_case(&stem.to_ascii_lowercase(), '_')
}

fn uniffi_event_variant(event_id: &str) -> String {
    pascal_case(&event_id.replace('.', "_"), '_')
}

fn pascal_case(value: &str, delimiter: char) -> String {
    value
        .split(delimiter)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut out = String::new();
            out.push(first.to_ascii_uppercase());
            out.extend(chars);
            out
        })
        .collect::<String>()
}

fn parse_dispatch_arg(spec: &Value) -> Result<DispatchArg> {
    let values = spec
        .as_array()
        .with_context(|| format!("dispatch arg spec must be an array: {spec}"))?;
    let first = values
        .first()
        .and_then(Value::as_str)
        .context("dispatch arg spec missing name")?;
    if first == "literal" {
        let kind = values
            .get(1)
            .and_then(Value::as_str)
            .context("literal dispatch arg missing kind")?
            .to_string();
        return Ok(DispatchArg {
            name: "literal".to_string(),
            kind,
            value: values.get(2).cloned(),
            ..DispatchArg::default()
        });
    }

    let kind = values
        .get(1)
        .and_then(Value::as_str)
        .context("dispatch arg spec missing kind")?
        .to_string();
    let mut out = DispatchArg {
        name: first.to_string(),
        kind,
        ..DispatchArg::default()
    };
    if let Some(extra) = values.get(2) {
        match extra {
            Value::Object(map) => {
                out.wire = map.get("wire").and_then(Value::as_str).map(str::to_string);
                out.pass = map.get("pass").and_then(Value::as_str).map(str::to_string);
                out.default = map.get("default").and_then(Value::as_i64);
                out.min = map.get("min").and_then(Value::as_i64);
                out.value = map.get("value").cloned();
            }
            Value::Number(number) => out.default = number.as_i64(),
            Value::Bool(_) | Value::String(_) => out.value = Some(extra.clone()),
            _ => {}
        }
    }
    Ok(out)
}

fn render_dispatch_group(group: &DispatchGroup) -> Result<String> {
    let native_ops = group
        .operations
        .iter()
        .filter(|op| op.cfg.as_deref() == Some("not(target_arch = \"wasm32\")"))
        .collect::<Vec<_>>();
    let common_ops = group
        .operations
        .iter()
        .filter(|op| op.cfg.as_deref() != Some("not(target_arch = \"wasm32\")"))
        .collect::<Vec<_>>();

    let mut lines = vec![
        dispatch_header().to_string(),
        "// Do not edit by hand.".to_string(),
        "#![allow(clippy::too_many_lines, unused_imports)]".to_string(),
        String::new(),
    ];
    lines.extend(
        dispatch_group_imports(&group.id)
            .into_iter()
            .map(str::to_string),
    );
    lines.extend([
        "use flare_im_core_sdk::serde_json::{self, Value};".to_string(),
        "use flare_im_core_sdk::Result;".to_string(),
        "use crate::dispatch_support::*;".to_string(),
        "use crate::{binding_operation_not_supported, BindingResponse};".to_string(),
        String::new(),
        format!("pub const {}: &[&str] = &[", group.ops_const),
    ]);
    let mut seen_ops = BTreeSet::new();
    for op in &group.operations {
        for name in operation_names(op) {
            if seen_ops.insert(name.clone()) {
                lines.push(format!("    {},", rust_string_literal(&name)));
            }
        }
    }
    lines.extend([
        "];".to_string(),
        String::new(),
        format!("pub fn {}(operation: &str) -> bool {{", group.is_fn),
        format!("    {}.contains(&operation)", group.ops_const),
        "}".to_string(),
        String::new(),
    ]);

    if !native_ops.is_empty() && group.id == "media" {
        lines.extend(render_native_dispatch_helper(group, &native_ops)?);
    }

    let value_name = if group.op_from_request {
        "request"
    } else {
        "params"
    };
    let mut sig_args = vec![format!("api: {}", group.receiver.binding)];
    for extra in &group.extra_receivers {
        let name = if group.id == "capability" && extra.name == "client" {
            "_client"
        } else {
            extra.name.as_str()
        };
        sig_args.push(format!("{name}: {}", extra.binding));
    }
    if !group.op_from_request {
        sig_args.push("operation: &str".to_string());
    }
    sig_args.push(format!("{value_name}: Value"));
    lines.push(format!("pub async fn {}(", group.dispatch_fn));
    lines.push(format!("    {},", sig_args.join(",\n    ")));
    lines.push(") -> Result<BindingResponse> {".to_string());
    if group.op_from_request {
        lines.push("    let operation = json_string(&request, \"op\")?;".to_string());
    }
    if !native_ops.is_empty() && group.id == "media" {
        let native_pattern = native_ops
            .iter()
            .map(|op| rust_string_literal(&op.op))
            .collect::<Vec<_>>()
            .join(" | ");
        lines.push("    #[cfg(not(target_arch = \"wasm32\"))]".to_string());
        lines.push(format!("    if matches!(operation, {native_pattern}) {{"));
        lines.push(format!(
            "        return {}_native_only(api, operation, params).await;",
            group.dispatch_fn
        ));
        lines.push("    }".to_string());
        lines.push(String::new());
    }
    let match_expr = if group.op_from_request {
        "operation.as_str()"
    } else {
        "operation"
    };
    lines.push(format!("    match {match_expr} {{"));
    for op in common_ops {
        lines.push(format!("        {} => {{", op_patterns(op)));
        lines.extend(render_operation_body(op, group)?);
        lines.push("        }".to_string());
    }
    lines.push("        _ => Err(binding_operation_not_supported(operation)),".to_string());
    lines.push("    }".to_string());
    lines.push("}".to_string());
    lines.push(String::new());
    lines.extend(render_dispatch_group_json_fn(group)?);
    Ok(lines.join("\n"))
}

fn render_dispatch_group_json_fn(group: &DispatchGroup) -> Result<Vec<String>> {
    let json_name = if group.op_from_request {
        "request_json"
    } else {
        "params_json"
    };
    let value_name = if group.op_from_request {
        "request"
    } else {
        "params"
    };
    let mut sig_args = vec![format!("api: {}", group.receiver.binding)];
    let mut fallback_args = vec!["api".to_string()];
    for extra in &group.extra_receivers {
        sig_args.push(format!("{}: {}", extra.name, extra.binding));
        fallback_args.push(extra.name.clone());
    }
    if !group.op_from_request {
        sig_args.push("operation: &str".to_string());
        fallback_args.push("operation".to_string());
    }
    sig_args.push(format!("{json_name}: &str"));

    let mut lines = vec![
        format!("pub async fn {}_json(", group.dispatch_fn),
        format!("    {},", sig_args.join(",\n    ")),
        ") -> Result<BindingResponse> {".to_string(),
    ];
    let mut json_arms = Vec::new();
    for op in &group.operations {
        if let Some(body) = render_operation_body_json(op, group)? {
            json_arms.push(format!("        {} => {{", op_patterns(op)));
            json_arms.extend(body);
            json_arms.push("        }".to_string());
        }
    }
    if json_arms.is_empty() {
        lines.push(format!(
            "    let {value_name} = dispatch_params_from_json({json_name})?;"
        ));
        fallback_args.push(value_name.to_string());
        lines.push(format!(
            "    {}({}).await",
            group.dispatch_fn,
            fallback_args.join(", ")
        ));
        lines.push("}".to_string());
        lines.push(String::new());
        return Ok(lines);
    }
    if group.op_from_request {
        lines.push(format!(
            "    let operation = dispatch_operation_from_json({json_name})?;"
        ));
    }
    let match_expr = if group.op_from_request {
        "operation.as_str()"
    } else {
        "operation"
    };
    lines.push(format!("    match {match_expr} {{"));
    lines.extend(json_arms);
    lines.push("        _ => {".to_string());
    lines.push(format!(
        "            let {value_name} = dispatch_params_from_json({json_name})?;"
    ));
    fallback_args.push(value_name.to_string());
    lines.push(format!(
        "            {}({}).await",
        group.dispatch_fn,
        fallback_args.join(", ")
    ));
    lines.push("        }".to_string());
    lines.push("    }".to_string());
    lines.push("}".to_string());
    lines.push(String::new());
    Ok(lines)
}

fn dispatch_group_imports(group_id: &str) -> Vec<&'static str> {
    match group_id {
        "message" => vec![
            "use flare_im_core_sdk::client::api::MessageApi;",
            "use flare_im_core_sdk::model::MessageSearchQuery;",
        ],
        "message_build" => vec![
            "use std::sync::Arc;",
            "use flare_im_core_sdk::client::api::MessageBuildApi;",
        ],
        "conversation" => vec![
            "use flare_im_core_sdk::client::api::ConversationApi;",
            "use flare_im_core_sdk::model::{BootstrapHomeTimelineRequest, ConversationListQuery, OpenConversationTimelineRequest};",
        ],
        "media" => vec!["use flare_im_core_sdk::client::api::MediaApi;"],
        "capability" => vec![
            "use std::sync::Arc;",
            "use flare_im_core_sdk::client::api::CapabilityApi;",
            "use flare_im_core_sdk::client::IMClient;",
        ],
        _ => Vec::new(),
    }
}

fn render_native_dispatch_helper(
    group: &DispatchGroup,
    native_ops: &[&DispatchOperation],
) -> Result<Vec<String>> {
    let mut lines = vec![
        "#[cfg(not(target_arch = \"wasm32\"))]".to_string(),
        format!("async fn {}_native_only(", group.dispatch_fn),
        format!("    api: {},", group.receiver.binding),
        "    operation: &str,".to_string(),
        "    params: Value,".to_string(),
        ") -> Result<BindingResponse> {".to_string(),
        "    match operation {".to_string(),
    ];
    for op in native_ops {
        lines.push(format!("        {} => {{", op_patterns(op)));
        lines.extend(render_operation_body(op, group)?);
        lines.push("        }".to_string());
    }
    lines.push("        _ => Err(binding_operation_not_supported(operation)),".to_string());
    lines.push("    }".to_string());
    lines.push("}".to_string());
    lines.push(String::new());
    Ok(lines)
}

fn operation_names(op: &DispatchOperation) -> Vec<String> {
    let mut names = vec![op.op.clone()];
    if let Some(aliases) = &op.aliases {
        names.extend(aliases.iter().cloned());
    }
    names
}

fn op_patterns(op: &DispatchOperation) -> String {
    operation_names(op)
        .iter()
        .map(|name| rust_string_literal(name))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn render_operation_body(op: &DispatchOperation, group: &DispatchGroup) -> Result<Vec<String>> {
    let value_expr = if group.op_from_request {
        "&request"
    } else {
        "&params"
    };
    let mut lets = Vec::new();
    let mut call_args = Vec::new();
    for raw in &op.args {
        let arg = parse_dispatch_arg(raw)?;
        if arg.name == "literal" {
            match arg.kind.as_str() {
                "bool" => call_args.push(
                    if arg.value.as_ref().and_then(Value::as_bool).unwrap_or(false) {
                        "true"
                    } else {
                        "false"
                    }
                    .to_string(),
                ),
                "none" => call_args.push("None".to_string()),
                other => bail!("unsupported literal dispatch arg kind: {other}"),
            }
            continue;
        }
        let (let_stmt, expr) = render_dispatch_arg_extract(&arg, value_expr)?;
        if !let_stmt.is_empty() {
            lets.extend(split_let_statements(&let_stmt));
        }
        call_args.push(expr);
    }

    let mut lines = Vec::new();
    for let_line in lets {
        if !let_line.trim().is_empty() {
            lines.push(format!("            {};", let_line.trim()));
        }
    }
    let call = format!("api.{}({})", op.method, call_args.join(", "));
    match op.result.as_deref().unwrap_or("json") {
        "unit" => {
            lines.push(format!("            {call}.await?;"));
            lines.push("            Ok(BindingResponse::unit())".to_string());
        }
        "send_ack" => lines.push(format!("            json_send_ack({call}.await?)")),
        "json_object" => {
            if op.fields.len() == 1 {
                let (key, var) = op.fields.iter().next().expect("len checked");
                lines.push(format!("            let {var} = {call}.await?;"));
                lines.push(format!(
                    "            json(serde_json::json!({{ {}: {var} }}))",
                    rust_string_literal(key)
                ));
            } else {
                lines.push(format!("            json({call}.await?)"));
            }
        }
        "json" => lines.push(format!("            json({call}.await?)")),
        other => bail!("unsupported dispatch result kind: {other}"),
    }
    Ok(lines)
}

fn render_operation_body_json(
    op: &DispatchOperation,
    group: &DispatchGroup,
) -> Result<Option<Vec<String>>> {
    let json_expr = if group.op_from_request {
        "request_json"
    } else {
        "params_json"
    };
    let mut lets = Vec::new();
    let mut call_args = Vec::new();
    let mut used_raw_json = false;
    for raw in &op.args {
        let arg = parse_dispatch_arg(raw)?;
        if arg.name == "literal" {
            match arg.kind.as_str() {
                "bool" => call_args.push(
                    if arg.value.as_ref().and_then(Value::as_bool).unwrap_or(false) {
                        "true"
                    } else {
                        "false"
                    }
                    .to_string(),
                ),
                "none" => call_args.push("None".to_string()),
                other => bail!("unsupported literal dispatch arg kind: {other}"),
            }
            continue;
        }
        let Some((let_stmt, expr)) = render_dispatch_arg_extract_json(&arg, json_expr)? else {
            return Ok(None);
        };
        used_raw_json = true;
        if !let_stmt.is_empty() {
            lets.extend(split_let_statements(&let_stmt));
        }
        call_args.push(expr);
    }
    if !used_raw_json {
        return Ok(None);
    }

    let mut lines = Vec::new();
    for let_line in lets {
        if !let_line.trim().is_empty() {
            lines.push(format!("            {};", let_line.trim()));
        }
    }
    let call = format!("api.{}({})", op.method, call_args.join(", "));
    match op.result.as_deref().unwrap_or("json") {
        "unit" => {
            lines.push(format!("            {call}.await?;"));
            lines.push("            Ok(BindingResponse::unit())".to_string());
        }
        "send_ack" => lines.push(format!("            json_send_ack({call}.await?)")),
        "json_object" => {
            if op.fields.len() == 1 {
                let (key, var) = op.fields.iter().next().expect("len checked");
                lines.push(format!("            let {var} = {call}.await?;"));
                lines.push(format!(
                    "            json(serde_json::json!({{ {}: {var} }}))",
                    rust_string_literal(key)
                ));
            } else {
                lines.push(format!("            json({call}.await?)"));
            }
        }
        "json" => lines.push(format!("            json({call}.await?)")),
        other => bail!("unsupported dispatch result kind: {other}"),
    }
    Ok(Some(lines))
}

fn split_let_statements(statement: &str) -> Vec<String> {
    statement
        .split(';')
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn render_dispatch_arg_extract_json(
    arg: &DispatchArg,
    json_expr: &str,
) -> Result<Option<(String, String)>> {
    let name = arg.name.as_str();
    let kind = arg.kind.as_str();

    if name == "@value" && kind.starts_with("deserialize:") {
        let ty = kind.trim_start_matches("deserialize:");
        return Ok(Some((
            format!(
                "let bound_value: {ty} = from_json_str({}, {})?",
                json_expr,
                rust_string_literal(ty)
            ),
            "bound_value".to_string(),
        )));
    }

    let rendered = match kind {
        "im_message" => (
            String::new(),
            format!("message_from_json_str({json_expr})?"),
        ),
        _ if kind.starts_with("deserialize:") => {
            let ty = kind.trim_start_matches("deserialize:");
            let v = format!("{name}_v");
            let label = ty.replace(':', " ");
            (
                format!(
                    "let {v}: {ty} = from_json_str({}, {})?",
                    json_expr,
                    rust_string_literal(&label)
                ),
                v,
            )
        }
        "rich_doc_edit_request" => (
            String::new(),
            format!("from_json_str::<EditRichDocJson>({json_expr}, \"rich doc edit\")?.into()"),
        ),
        "rich_doc_create_request" => (
            String::new(),
            format!("from_json_str::<CreateRichDocJson>({json_expr}, \"rich doc create\")?.into()"),
        ),
        _ => return Ok(None),
    };
    Ok(Some(rendered))
}

fn render_dispatch_arg_extract(arg: &DispatchArg, value_expr: &str) -> Result<(String, String)> {
    let name = arg.name.as_str();
    let kind = arg.kind.as_str();
    let key = arg.wire.clone().unwrap_or_else(|| wire_key(name));

    if name == "@value" && kind.starts_with("deserialize:") {
        let ty = kind.trim_start_matches("deserialize:");
        return Ok((
            format!(
                "let bound_value: {ty} = from_value({}, {})?",
                owned_clone_expr(value_expr),
                rust_string_literal(ty)
            ),
            "bound_value".to_string(),
        ));
    }

    match kind {
        "str_ref" => {
            let v = format!("{name}_s");
            Ok((
                format!(
                    "let {v} = json_string({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                format!("&{v}"),
            ))
        }
        "conversation_id" => {
            let v = format!("{name}_s");
            Ok((
                format!("let {v} = conversation_id({value_expr})?"),
                format!("&{v}"),
            ))
        }
        "optional_str_ref" => {
            let v = format!("{name}_o");
            Ok((
                format!(
                    "let {v} = optional_string({value_expr}, {})",
                    rust_string_literal(&key)
                ),
                format!("{v}.as_deref()"),
            ))
        }
        "optional_media_reference" => {
            let v = format!("{name}_o");
            Ok((
                format!("let {v} = optional_media_reference({value_expr})?"),
                format!("{v}.as_ref()"),
            ))
        }
        "optional_string" => {
            let v = format!("{name}_o");
            Ok((
                format!(
                    "let {v} = optional_string({value_expr}, {})",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "string" => {
            let v = format!("{name}_s");
            Ok((
                format!(
                    "let {v} = json_string({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "u64" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = json_u64({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "i64" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = json_i64({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "i32" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = json_i32({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "bool" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = json_bool({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "bool_default_false" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = optional_bool({value_expr}, {})?.unwrap_or(false)",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "mark_type" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = parse_mark_type(json_i32({value_expr}, {})?)",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "conversation_type" => {
            let v = format!("{name}_v");
            Ok((
                format!("let {v} = conversation_type({value_expr})?"),
                format!("&{v}"),
            ))
        }
        "vec_string" => {
            let v = format!("{name}_v");
            let expr = if arg.pass.as_deref() == Some("ref") {
                format!("&{v}")
            } else {
                v.clone()
            };
            Ok((
                format!(
                    "let {v} = json_vec_string({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                expr,
            ))
        }
        "vec_string_default_empty" => {
            let v = format!("{name}_v");
            let expr = if arg.pass.as_deref() == Some("ref") {
                format!("&{v}")
            } else {
                v.clone()
            };
            Ok((
                format!(
                    "let {v} = optional_value::<Vec<String>>({value_expr}, {})?.unwrap_or_default()",
                    rust_string_literal(&key)
                ),
                expr,
            ))
        }
        "vec_im_message" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = json_vec_message({value_expr}, {})?",
                    rust_string_literal(&key)
                ),
                v,
            ))
        }
        "image_group_images" => {
            let v = format!("{name}_v");
            Ok((format!("let {v} = image_group_images({value_expr})?"), v))
        }
        "image_group_description" => {
            let v = format!("{name}_v");
            Ok((
                format!("let {v} = image_group_description({value_expr})"),
                v,
            ))
        }
        "image_group_metadata" => {
            let v = format!("{name}_v");
            Ok((format!("let {v} = image_group_metadata({value_expr})?"), v))
        }
        _ => render_dispatch_arg_extract_extended(arg, value_expr, &key),
    }
}

fn render_dispatch_arg_extract_extended(
    arg: &DispatchArg,
    value_expr: &str,
    key: &str,
) -> Result<(String, String)> {
    let name = arg.name.as_str();
    let kind = arg.kind.as_str();
    match kind {
        "optional_vec_string" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = optional_value::<Vec<String>>({value_expr}, {})?",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        "optional_hashmap_string" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = optional_value::<std::collections::HashMap<String, String>>({value_expr}, {})?",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        "optional_built_content" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = match {value_expr}.get(\"quotedContent\") {{\n\
                     \x20   Some(v) => Some(built_content_from_value(v)?),\n\
                     \x20   None => None,\n\
                     }}"
                ),
                v,
            ))
        }
        "optional_u32" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = optional_u32({value_expr}, {})?",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        "im_message" => Ok((String::new(), format!("message_from_params({value_expr})?"))),
        _ if kind.starts_with("deserialize:") => {
            let ty = kind.trim_start_matches("deserialize:");
            let v = format!("{name}_v");
            let label = ty.replace(':', " ");
            Ok((
                format!(
                    "let {v}: {ty} = from_value({}, {})?",
                    owned_clone_expr(value_expr),
                    rust_string_literal(&label)
                ),
                v,
            ))
        }
        "rich_doc_edit_request" => Ok((
            String::new(),
            format!(
                "from_value::<EditRichDocJson>({}, \"rich doc edit\")?.into()",
                owned_clone_expr(value_expr)
            ),
        )),
        "rich_doc_create_request" => Ok((
            String::new(),
            format!(
                "from_value::<CreateRichDocJson>({}, \"rich doc create\")?.into()",
                owned_clone_expr(value_expr)
            ),
        )),
        "create_location_request" => Ok((
            String::new(),
            format!(
                "build_create_location_request({})?",
                owned_clone_expr(value_expr)
            ),
        )),
        "user_download_request" => Ok((
            String::new(),
            format!(
                "build_user_download_request({})?",
                owned_clone_expr(value_expr)
            ),
        )),
        "create_sticker_request" => Ok((
            String::new(),
            format!(
                "build_create_sticker_request({})?",
                owned_clone_expr(value_expr)
            ),
        )),
        "built_content_field" => Ok((
            format!(
                "let content = {value_expr}.get(\"content\").ok_or_else(|| crate::binding_invalid_parameter(\"missing content\"))?"
            ),
            "built_content_from_value(content)?".to_string(),
        )),
        "json_null_default" => {
            let base = value_expr.strip_prefix('&').unwrap_or(value_expr);
            Ok((
                String::new(),
                format!("{base}.get(\"payload\").cloned().unwrap_or(serde_json::Value::Null)"),
            ))
        }
        _ if kind.starts_with("str_any:") => {
            let keys = dispatch_key_literals(kind.trim_start_matches("str_any:"));
            let v = format!("{name}_s");
            Ok((
                format!("let {v} = string_any({value_expr}, &[{keys}])?"),
                format!("&{v}"),
            ))
        }
        _ if kind.starts_with("optional_str_any:") => {
            let keys = dispatch_key_literals(kind.trim_start_matches("optional_str_any:"));
            let v = format!("{name}_o");
            Ok((
                format!("let {v} = optional_string_any({value_expr}, &[{keys}])"),
                format!("{v}.as_deref()"),
            ))
        }
        "optional_i32_default" => {
            let default = arg.default.unwrap_or(3600);
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = optional_i32({value_expr}, {})?.unwrap_or({default})",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        "i32_u32" => {
            let default = arg.default.unwrap_or(50);
            let min = arg.min.unwrap_or(1);
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = optional_i32({value_expr}, {})?.unwrap_or({default}).max({min}) as u32",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        _ if kind.starts_with("str_ref_alt:") => {
            let alts = kind
                .trim_start_matches("str_ref_alt:")
                .split(',')
                .map(wire_key)
                .collect::<Vec<_>>();
            let v = format!("{name}_s");
            let mut inner = format!(
                "json_string({value_expr}, {})",
                rust_string_literal(alts.last().map(String::as_str).unwrap_or(""))
            );
            for alt in alts.iter().rev().skip(1) {
                inner = format!(
                    "json_string({value_expr}, {}).or_else(|_| {inner})",
                    rust_string_literal(alt)
                );
            }
            Ok((format!("let {v} = {inner}?"), format!("&{v}")))
        }
        "optional_upload_options" => {
            let v = format!("{name}_o");
            Ok((
                format!(
                    "let {v} = optional_upload_options({value_expr}, {})?",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        "bytes_vec" => {
            let v = format!("{name}_v");
            Ok((
                format!(
                    "let {v} = json_bytes_vec({value_expr}, {})?",
                    rust_string_literal(key)
                ),
                v,
            ))
        }
        "json_object" => bail!("json_object is result-only"),
        other => bail!("unsupported dispatch arg kind: {other} for {name}"),
    }
}

fn dispatch_key_literals(keys: &str) -> String {
    keys.split(',')
        .map(wire_key)
        .map(|key| rust_string_literal(&key))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_cstr_arg(param: &str, label: &str) -> String {
    let msg = rust_string_literal(label);
    format!(
        "let {param} = match c_str_to_string({param}) {{\n\
         \x20   Ok(s) => s,\n\
         \x20   Err(code) => {{\n\
         \x20       let ctx = CallbackContext::new(context, callback);\n\
         \x20       return_error(&ctx, code, {msg});\n\
         \x20       return code;\n\
         \x20   }}\n\
         }};"
    )
}

fn render_upload_options_prelude(name: &str) -> String {
    format!(
        "let upload_options = match parse_upload_options({name}) {{\n\
         \x20   Ok(v) => v,\n\
         \x20   Err(code) => {{\n\
         \x20       let ctx = CallbackContext::new(context, callback);\n\
         \x20       return_error(&ctx, code, \"Invalid upload options\");\n\
         \x20       return code;\n\
         \x20   }}\n\
         }};\n\
         let upload_options_value = match upload_options {{\n\
         \x20   Some(opts) => serde_json::json!({{ \"chunkSize\": opts.chunk_size }}),\n\
         \x20   None => serde_json::Value::Null,\n\
         }};"
    )
}

fn render_c_typed_arg_parse(
    arg: &CTypedAbiArg,
    with_callback: bool,
) -> Result<(String, Vec<String>)> {
    let name = arg.name.as_str();
    let ctx_setup = if with_callback {
        "let ctx = CallbackContext::new(context, callback);"
    } else {
        ""
    };
    match arg.arg_type.as_str() {
        "c_str" => {
            if with_callback {
                Ok((
                    format!("{name}: *const c_char"),
                    vec![render_cstr_arg(name, &format!("Invalid {name}"))],
                ))
            } else {
                Ok((
                    format!("{name}: *const c_char"),
                    vec![format!(
                        "let {name} = match c_str_to_string({name}) {{\n\
                         \x20   Ok(s) => s,\n\
                         \x20   Err(_) => return false,\n\
                         }};"
                    )],
                ))
            }
        }
        "u64" => Ok((format!("{name}: u64"), Vec::new())),
        "i32" => Ok((format!("{name}: i32"), Vec::new())),
        "bool" => Ok((format!("{name}: bool"), Vec::new())),
        "upload_options" => Ok((
            format!("{name}: *const c_char"),
            vec![render_upload_options_prelude(name)],
        )),
        "bytes_view" => Ok((
            format!("{name}: FlareBytesView"),
            vec![format!(
                "if {name}.ptr.is_null() || {name}.len == 0 {{\n\
                 \x20   {ctx_setup}\n\
                 \x20   return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid bytes\");\n\
                 \x20   return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
                 }}\n\
                 let payload = unsafe {{ std::slice::from_raw_parts({name}.ptr, {name}.len) }}.to_vec();"
            )],
        )),
        "request_json" => Ok((
            format!("{name}: *const c_char"),
            vec![format!(
                "let params_json = match c_str_to_string({name}) {{\n\
                 \x20   Ok(s) => s,\n\
                 \x20   Err(code) => {{\n\
                 \x20       {ctx_setup}\n\
                 \x20       return_error(&ctx, code, \"Invalid request JSON\");\n\
                 \x20       return code;\n\
                 \x20   }}\n\
                 }};\n\
                 if serde_json::from_str::<Box<serde_json::value::RawValue>>(&params_json).is_err() {{\n\
                 \x20   {ctx_setup}\n\
                 \x20   return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid request JSON\");\n\
                 \x20   return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
                 }};"
            )],
        )),
        "json_vec" => Ok((
            format!("{name}: *const c_char"),
            vec![
                render_cstr_arg(name, &format!("Invalid {name}")),
                format!(
                    "let {name} = match serde_json::from_str::<Vec<String>>(&{name}) {{\n\
                     \x20   Ok(v) => v,\n\
                     \x20   Err(_) => {{\n\
                     \x20       {ctx_setup}\n\
                     \x20       return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid user_ids_json\");\n\
                     \x20       return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
                     \x20   }}\n\
                     }};"
                ),
            ],
        )),
        "json_message" => Ok((
            format!("{name}: *const c_char"),
            vec![format!(
                "let message: flare_im_core_sdk::model::IMMessage = match parse_json({name}) {{\n\
                 \x20   Ok(m) => m,\n\
                 \x20   Err(code) => {{\n\
                 \x20       {ctx_setup}\n\
                 \x20       return_error(&ctx, code, \"Invalid message JSON\");\n\
                 \x20       return code;\n\
                 \x20   }}\n\
                 }};"
            )],
        )),
        other => bail!("unsupported c_typed_abi arg type: {other}"),
    }
}

fn render_c_typed_params_build(args: &[CTypedAbiArg], with_callback: bool) -> Vec<String> {
    if args.is_empty() {
        return vec!["let params_json = \"null\".to_string();".to_string()];
    }
    if args.len() == 1 && args[0].arg_type == "request_json" {
        return Vec::new();
    }
    if args.len() == 1 && args[0].arg_type == "json_message" {
        return vec![
            "let params_json = match serde_json::to_string(&message) {\n\
             \x20   Ok(s) => s,\n\
             \x20   Err(_) => {\n\
             \x20       let ctx = CallbackContext::new(context, callback);\n\
             \x20       return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid message\");\n\
             \x20       return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
             \x20   }\n\
             };"
                .to_string(),
        ];
    }

    let mut prelude = Vec::new();
    let mut fields = Vec::new();
    for arg in args {
        let key = arg.json_key.clone().unwrap_or_else(|| wire_key(&arg.name));
        match arg.arg_type.as_str() {
            "request_json" => {}
            "json_message" => {
                prelude.push(
                    "let message_value = match serde_json::to_value(&message) {\n\
                     \x20   Ok(v) => v,\n\
                     \x20   Err(_) => {\n\
                     \x20       let ctx = CallbackContext::new(context, callback);\n\
                     \x20       return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid message\");\n\
                     \x20       return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
                     \x20   }\n\
                     };"
                        .to_string(),
                );
                fields.push(format!("{}: message_value", rust_string_literal(&key)));
            }
            "json_vec" => {
                prelude.push(format!(
                    "let {name}_value = match serde_json::to_value(&{name}) {{\n\
                     \x20   Ok(v) => v,\n\
                     \x20   Err(_) => {{\n\
                     \x20       let ctx = CallbackContext::new(context, callback);\n\
                     \x20       return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid user_ids\");\n\
                     \x20       return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
                     \x20   }}\n\
                     }};",
                    name = arg.name
                ));
                fields.push(format!("{}: {}_value", rust_string_literal(&key), arg.name));
            }
            "upload_options" => {
                fields.push(format!(
                    "{}: upload_options_value",
                    rust_string_literal(&key)
                ));
            }
            "bytes_view" => {
                prelude.push(
                    "let bytes_value = match serde_json::to_value(&payload) {\n\
                     \x20   Ok(v) => v,\n\
                     \x20   Err(_) => {\n\
                     \x20       let ctx = CallbackContext::new(context, callback);\n\
                     \x20       return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid bytes\");\n\
                     \x20       return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
                     \x20   }\n\
                     };"
                        .to_string(),
                );
                fields.push(format!("{}: bytes_value", rust_string_literal(&key)));
            }
            _ => fields.push(format!("{}: {}", rust_string_literal(&key), arg.name)),
        }
    }
    prelude.push(format!(
        "let params = serde_json::json!({{{}}});",
        fields.join(", ")
    ));
    if with_callback {
        prelude.push(
            "let params_json = match serde_json::to_string(&params) {\n\
             \x20   Ok(s) => s,\n\
             \x20   Err(_) => {\n\
             \x20       let ctx = CallbackContext::new(context, callback);\n\
             \x20       return_error(&ctx, crate::error_convert::FLARE_ERR_INVALID_PARAM, \"Invalid typed ABI params\");\n\
             \x20       return crate::error_convert::FLARE_ERR_INVALID_PARAM;\n\
             \x20   }\n\
             };"
                .to_string(),
        );
    } else {
        prelude.push(
            "let params_json = match serde_json::to_string(&params) {\n\
             \x20   Ok(s) => s,\n\
             \x20   Err(_) => return false,\n\
             };"
            .to_string(),
        );
    }
    prelude
}

fn render_c_typed_abi_export(entry: &CTypedAbiExport) -> Result<String> {
    match entry.kind.as_str() {
        "invoke_unit" | "invoke_json" | "invoke_send_ack" => render_c_typed_async_invoke(entry),
        other => bail!("unsupported c_typed_abi export kind: {other}"),
    }
}

fn render_c_typed_async_invoke(entry: &CTypedAbiExport) -> Result<String> {
    let api_id = entry
        .api_id
        .as_deref()
        .with_context(|| format!("c_typed_abi export {} requires api_id", entry.symbol))?;
    let invoke_fn = match entry.kind.as_str() {
        "invoke_unit" => "typed_invoke_unit",
        "invoke_json" => "typed_invoke_json",
        "invoke_send_ack" => "typed_invoke_send_ack",
        other => bail!("unsupported c_typed_abi invoke kind: {other}"),
    };
    let mut arg_decls = Vec::new();
    let mut body_lines = Vec::new();
    for arg in &entry.args {
        let (decl, lines) = render_c_typed_arg_parse(arg, true)?;
        arg_decls.push(decl);
        body_lines.extend(lines);
    }
    body_lines.extend(render_c_typed_params_build(&entry.args, true));
    let body = indent_lines(&body_lines, "        ");
    let arg_section = render_c_arg_section(&arg_decls);
    Ok(format!(
        "\n#[unsafe(no_mangle)]\n\
         pub extern \"C\" fn {symbol}(\n\
         \x20   handle: FlareHandle,\n\
         {arg_section}    context: *mut c_void,\n\
         \x20   callback: FlareResultCallback,\n\
         ) -> i32 {{\n\
         \x20   abi::catch_ffi_i32(|| {{\n\
         \x20       let instance = match require_instance(handle) {{\n\
         \x20           Ok(i) => i,\n\
         \x20           Err(e) => return e,\n\
         \x20       }};\n\
         {body}\n\
         \x20       let ctx = CallbackContext::new(context, callback);\n\
         \x20       let api_id = {api_id};\n\
         \x20       {invoke_fn}(instance, ctx, api_id, params_json);\n\
         \x20       0\n\
         \x20   }})\n\
         }}\n",
        symbol = entry.symbol,
        api_id = rust_string_literal(api_id),
    ))
}

fn render_c_arg_section(arg_decls: &[String]) -> String {
    if arg_decls.is_empty() {
        return String::new();
    }
    format!("    {},\n", arg_decls.join(",\n    "))
}

fn indent_lines(lines: &[String], indent: &str) -> String {
    if lines.is_empty() {
        return String::new();
    }
    lines
        .iter()
        .map(|line| {
            line.lines()
                .map(|inner| format!("{indent}{inner}"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn c_symbol_channel(symbol: &str) -> String {
    symbol
        .strip_prefix("flare_")
        .unwrap_or(symbol)
        .strip_suffix("_json")
        .unwrap_or_else(|| symbol.strip_prefix("flare_").unwrap_or(symbol))
        .to_string()
}

fn c_dispatch_channel_order(channel: &str) -> usize {
    match channel {
        "message_dispatch" => 0,
        "message_build" => 1,
        "conversation_dispatch" => 2,
        "media_dispatch" => 3,
        _ => 99,
    }
}

fn render_c_json_dispatch_block(channel: &CDispatchChannel) -> String {
    if channel.channel == "message_build" {
        return format!(
            "/// C ABI: `{symbol}`\n\
             #[unsafe(no_mangle)]\n\
             pub extern \"C\" fn {symbol}(\n\
             \x20   handle: FlareHandle,\n\
             \x20   request_json: *const c_char,\n\
             \x20   context: *mut c_void,\n\
             \x20   callback: FlareResultCallback,\n\
             ) -> i32 {{\n\
             \x20   message_build_dispatch_entry(handle, request_json, context, callback)\n\
             }}\n\n",
            symbol = channel.symbol
        );
    }

    let group = &channel.runtime_group;
    let check = format!("flare_im_core_sdk_bindings_runtime::{group}::is_{group}_operation");
    let dispatch = format!("flare_im_core_sdk_bindings_runtime::{group}::dispatch_{group}_json");
    let api = format!("{group}_api");
    let extra = if group == "capability" {
        ", &inst.client"
    } else {
        ""
    };
    format!(
        "/// C ABI: `{symbol}`\n\
         #[unsafe(no_mangle)]\n\
         pub extern \"C\" fn {symbol}(\n\
         \x20   handle: FlareHandle,\n\
         \x20   op: *const c_char,\n\
         \x20   params_json: *const c_char,\n\
         \x20   context: *mut c_void,\n\
         \x20   callback: FlareResultCallback,\n\
         ) -> i32 {{\n\
         \x20   json_dispatch_entry(handle, op, params_json, context, callback, |inst, operation, params_json| {{\n\
         \x20       Box::pin(async move {{\n\
         \x20           let api = inst.{api}().await?;\n\
         \x20           if !{check}(&operation) {{\n\
         \x20               return Err(flare_im_core_sdk_bindings_runtime::binding_operation_not_supported(&operation));\n\
         \x20           }}\n\
         \x20           {dispatch}(&api{extra}, &operation, &params_json).await\n\
         \x20       }})\n\
         \x20   }})\n\
         }}\n\n",
        symbol = channel.symbol
    )
}

fn render_c_invoke() -> String {
    format!(
        "{header}\
         use std::ffi::{{c_char, c_void}};\n\n\
         use crate::dispatch_common::invoke_entry;\n\
         use crate::types::{{FlareHandle, FlareResultCallback}};\n\n\
         /// Universal contract invoke: `api_id` + JSON params (canonical ids from `contract/apis.json`).\n\
         #[unsafe(no_mangle)]\n\
         pub extern \"C\" fn flare_sdk_invoke_json(\n\
         \x20   handle: FlareHandle,\n\
         \x20   api_id: *const c_char,\n\
         \x20   params_json: *const c_char,\n\
         \x20   context: *mut c_void,\n\
         \x20   callback: FlareResultCallback,\n\
         ) -> i32 {{\n\
         \x20   invoke_entry(handle, api_id, params_json, context, callback)\n\
         }}\n",
        header = platform_header()
    )
}

fn render_tauri_invoke() -> String {
    format!(
        "{header}\
         use flare_im_core_sdk::serde_json::Value;\n\
         use tauri::State;\n\n\
         use crate::state::SdkState;\n\
         use flare_im_core_sdk_bindings_runtime::{{\n\
         \x20   binding_response_to_value, invoke_api_id_json, sdk_error_value,\n\
         }};\n\n\
         /// Rejects with the structured error (`sdk_error_value`), never its text.\n\
         #[tauri::command(rename_all = \"camelCase\")]\n\
         pub async fn sdk_invoke_json(\n\
         \x20   state: State<'_, SdkState>,\n\
         \x20   api_id: String,\n\
         \x20   request_json: String,\n\
         ) -> Result<Value, Value> {{\n\
         \x20   invoke_api_id_json(&*state, &api_id, &request_json)\n\
         \x20       .await\n\
         \x20       .map(binding_response_to_value)\n\
         \x20       .map_err(|e| sdk_error_value(&e, Some(&api_id)))\n\
         }}\n",
        header = platform_header()
    )
}

fn render_wasm_bindings() -> String {
    format!(
        "{header}\
         use wasm_bindgen::prelude::*;\n\n\
         use flare_im_core_sdk_bindings_runtime::BINDING_CONTRACT_VERSION;\n\n\
         /// Contract version string for L2/L3 SDK parity checks.\n\
         #[wasm_bindgen(js_name = flareBindingContractVersion)]\n\
         pub fn flare_binding_contract_version() -> String {{\n\
         \x20   BINDING_CONTRACT_VERSION.to_string()\n\
         }}\n\n\
         /// Canonical API invoke: `api_id` from `contract/apis.json` + JSON request body.\n\
         #[cfg(feature = \"local-smoke-runtime\")]\n\
         #[wasm_bindgen(js_name = flareInvoke)]\n\
         pub fn flare_invoke(\n\
         \x20   runtime: &mut crate::smoke::FlareImWasmRuntime,\n\
         \x20   api_id: &str,\n\
         \x20   request_json: &str,\n\
         ) -> Result<JsValue, JsValue> {{\n\
         \x20   runtime.invoke(api_id, request_json)\n\
         }}\n",
        header = platform_header()
    )
}

fn render_uniffi_invoke() -> String {
    format!(
        "{header}\
         /// UniFFI invoke placeholder - wire to `bindings_runtime::invoke_api_id_json` when session adapter exists.\n\
         pub fn invoke_contract_api(_api_id: &str, _request_json: &str) -> Result<String, String> {{\n\
         \x20   Err(\"UniFFI invoke adapter not implemented; use contract metadata only\".to_string())\n\
         }}\n",
        header = platform_header()
    )
}

fn render_direct_invoke_route_item(route: &DirectInvokeRoute) -> String {
    let route_literal = rust_string_literal(&route.route);
    if let Some(cfg) = route.cfg_condition() {
        format!("#[cfg({cfg})]\n    {route_literal}")
    } else {
        route_literal
    }
}

fn render_direct_invoke_route_bool_arm(route: &DirectInvokeRoute) -> String {
    let route_literal = rust_string_literal(&route.route);
    if let Some(cfg) = route.cfg_condition() {
        format!("#[cfg({cfg})]\n        {route_literal} => true,")
    } else {
        format!("        {route_literal} => true,")
    }
}

fn render_direct_invoke_match_arm(route: &DirectInvokeRoute) -> Result<String> {
    let tail = match route.result.as_str() {
        "unit" => "Ok(BindingResponse::unit())".to_string(),
        "json" => format!(
            "dispatch_support::json({})",
            route
                .json_expr
                .as_deref()
                .unwrap_or("serde_json::to_value(())?")
                .trim()
        ),
        other => bail!(
            "direct_invoke.json route {} has unsupported result {other:?}",
            route.route
        ),
    };

    let mut out = String::new();
    if let Some(cfg) = route.cfg_condition() {
        out.push_str(&format!("        #[cfg({cfg})]\n"));
    }
    out.push_str(&format!(
        "        {} => {{\n",
        rust_string_literal(&route.route)
    ));
    if !route.skip_client {
        out.push_str("            let client = session.client();\n");
    }
    let body = route.body.trim();
    if !body.is_empty() {
        out.push_str("            ");
        out.push_str(body);
        out.push('\n');
    }
    out.push_str("            ");
    out.push_str(&tail);
    out.push('\n');
    out.push_str("        }");
    Ok(out)
}

fn ensure_client_config_keys_are_camel_case(doc: &Value) -> Result<()> {
    let mut invalid = Vec::new();
    collect_non_camel_json_keys("$", doc, &mut invalid);
    if !invalid.is_empty() {
        bail!(
            "client_config.json contains non-camelCase keys: {}",
            invalid.join(", ")
        );
    }
    Ok(())
}

fn collect_non_camel_json_keys(path: &str, value: &Value, invalid: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if !is_lower_camel_or_plain(key) {
                    invalid.push(format!("{path}.{key}"));
                }
                let child_path = if path == "$" {
                    format!("$.{key}")
                } else {
                    format!("{path}.{key}")
                };
                collect_non_camel_json_keys(&child_path, child, invalid);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_non_camel_json_keys(&format!("{path}[{index}]"), child, invalid);
            }
        }
        _ => {}
    }
}

fn ensure_client_config_has_no_removed_aliases(doc: &Value) -> Result<()> {
    let content = serde_json::to_string(doc).context("client config JSON serialization")?;
    let removed = [
        "\"sdk_config\"",
        "\"ws_url\"",
        "\"quic_url\"",
        "\"http_url\"",
        "\"sourceUrl\"",
    ];
    let mut offenders = removed
        .into_iter()
        .filter(|needle| content.contains(needle))
        .map(str::to_string)
        .collect::<Vec<_>>();
    // 地址、传输策略与重连参数已由 `access`（接入清单）接管；它们出现在 sdkConfig 示例或
    // sdkConfigOverlay 说明里，就是在教宿主配一个 SDK 不再读取的字段。
    // `wsUrl` 等名字在 `access.developmentEndpoints` 里仍合法，所以只查这两层的直接键与分组列表。
    let removed_overlay_keys = [
        "wsUrl",
        "quicUrl",
        "httpUrl",
        "capabilityUrl",
        "onlineUrl",
        "mediaStorageProxyPrefix",
        "mediaStorageProxyTargets",
        "transportPolicy",
        "defaultTransport",
        "protocolRaceOrder",
        "reconnectIntervalSecs",
        "maxReconnectAttempts",
        "appVersion",
        "appBuild",
        // 证书钉扎按接入点来自接入清单（`spkiPins`），没有全局配置项。
        "tlsSpkiSha256Pins",
        "tlsCertificateSha256Pins",
    ];
    for pointer in ["/initRequest/example/sdkConfig", "/sdkConfigOverlay"] {
        let Some(Value::Object(map)) = doc.pointer(pointer) else {
            continue;
        };
        for (key, value) in map {
            if removed_overlay_keys.contains(&key.as_str()) {
                offenders.push(format!("{pointer}/{key}"));
            }
            if let Value::Array(items) = value {
                for item in items {
                    if let Some(name) = item.as_str()
                        && removed_overlay_keys.contains(&name)
                    {
                        offenders.push(format!("{pointer}/{key}[{name}]"));
                    }
                }
            }
        }
    }
    if !offenders.is_empty() {
        bail!(
            "client_config.json contains removed compatibility keys: {}",
            offenders.join(", ")
        );
    }
    Ok(())
}

struct SchemaArtifact {
    file_name: &'static str,
    type_name: &'static str,
    schema: Value,
}

struct SchemaCatalog {
    artifacts: Vec<SchemaArtifact>,
}

impl SchemaCatalog {
    fn new() -> Self {
        let artifacts = vec![
            schema_artifact::<flare_im_core_sdk::client::push_lifecycle::PushTokenInput>(
                "push_token_input.schema.json",
                "PushTokenInput",
            ),
            schema_artifact::<flare_im_core_sdk::client::push_lifecycle::ClearPushTokenRequest>(
                "clear_push_token_request.schema.json",
                "ClearPushTokenRequest",
            ),
            schema_artifact::<flare_im_core_sdk::client::push_lifecycle::SetPushPermissionRequest>(
                "set_push_permission_request.schema.json",
                "SetPushPermissionRequest",
            ),
            schema_artifact::<flare_im_core_sdk::client::push_lifecycle::PushLifecycleStatus>(
                "push_lifecycle_status.schema.json",
                "PushLifecycleStatus",
            ),
            schema_artifact::<SdkConversationType>(
                "conversation_type.schema.json",
                "ConversationType",
            ),
            schema_artifact::<ConversationParticipant>(
                "conversation_participant.schema.json",
                "ConversationParticipant",
            ),
            schema_artifact::<MessagePreviewElem>(
                "message_preview.schema.json",
                "MessagePreviewElem",
            ),
            schema_artifact::<Conversation>("conversation.schema.json", "Conversation"),
            schema_artifact::<TimelineSyncState>(
                "timeline_sync_state.schema.json",
                "TimelineSyncState",
            ),
            schema_artifact::<BootstrapHomeTimelineRequest>(
                "bootstrap_home_timeline_request.schema.json",
                "BootstrapHomeTimelineRequest",
            ),
            schema_artifact::<OpenConversationTimelineRequest>(
                "open_conversation_timeline_request.schema.json",
                "OpenConversationTimelineRequest",
            ),
            schema_artifact::<HomeTimelineSnapshot>(
                "home_timeline_snapshot.schema.json",
                "HomeTimelineSnapshot",
            ),
            schema_artifact::<ConversationTimelineSnapshot>(
                "conversation_timeline_snapshot.schema.json",
                "ConversationTimelineSnapshot",
            ),
            schema_artifact::<ReactionEntry>("reaction_entry.schema.json", "ReactionEntry"),
            schema_artifact::<MessageLocalState>(
                "message_local_state.schema.json",
                "MessageLocalState",
            ),
            schema_artifact::<Elem>("message_content_elem.schema.json", "Elem"),
            schema_artifact::<IMMessage>("message.schema.json", "IMMessage"),
            schema_artifact::<MessageSearchKind>(
                "message_search_kind.schema.json",
                "MessageSearchKind",
            ),
            schema_artifact::<MessageSearchQuery>(
                "message_search_query.schema.json",
                "MessageSearchQuery",
            ),
            schema_artifact::<ConversationListQuery>(
                "conversation_list_query.schema.json",
                "ConversationListQuery",
            ),
            schema_artifact::<ConversationVersion>(
                "conversation_version.schema.json",
                "ConversationVersion",
            ),
            schema_artifact::<SyncConversationSummariesRequest>(
                "sync_conversation_summaries_request.schema.json",
                "SyncConversationSummariesRequest",
            ),
            schema_artifact::<SyncConversationSummariesResponse>(
                "sync_conversation_summaries_response.schema.json",
                "SyncConversationSummariesResponse",
            ),
            schema_artifact::<StartupHomeSyncRequest>(
                "startup_home_sync_request.schema.json",
                "StartupHomeSyncRequest",
            ),
            schema_artifact::<StartupHomeSyncResponse>(
                "startup_home_sync_response.schema.json",
                "StartupHomeSyncResponse",
            ),
            schema_artifact::<MediaAccessUrl>("media_access_url.schema.json", "MediaAccessUrl"),
            schema_artifact::<MediaResolvedAccess>(
                "media_resolved_access.schema.json",
                "MediaResolvedAccess",
            ),
            schema_artifact::<UploadedMedia>("uploaded_media.schema.json", "UploadedMedia"),
            schema_artifact::<SdkConfigOverlay>(
                "sdk_config_overlay.schema.json",
                "SdkConfigOverlay",
            ),
        ];
        Self { artifacts }
    }

    fn write(&self, root: &Path, check: bool) -> Result<()> {
        self.verify()?;
        let dir = schema_output_dir(root);
        if !check {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("failed to create {}", dir.display()))?;
        }
        for artifact in &self.artifacts {
            let path = dir.join(artifact.file_name);
            let content = generated_schema_content(artifact)?;
            if check {
                let current = std::fs::read_to_string(&path)
                    .with_context(|| format!("generated schema missing: {}", path.display()))?;
                if current != content {
                    bail!(
                        "generated schema is stale: {}; run `cargo xtask schema`",
                        path.display()
                    );
                }
            } else {
                std::fs::write(&path, content)
                    .with_context(|| format!("failed to write {}", path.display()))?;
            }
        }
        Ok(())
    }

    fn verify(&self) -> Result<()> {
        ensure_unique(
            "generated schema file names",
            self.artifacts.iter().map(|artifact| artifact.file_name),
        )?;
        ensure_unique(
            "generated schema type names",
            self.artifacts.iter().map(|artifact| artifact.type_name),
        )?;
        for artifact in &self.artifacts {
            ensure_schema_has_no_removed_alias_strings(artifact)?;
            ensure_schema_properties_are_camel_case(artifact)?;
        }
        Ok(())
    }
}

fn schema_artifact<T>(file_name: &'static str, type_name: &'static str) -> SchemaArtifact
where
    T: schemars::JsonSchema,
{
    SchemaArtifact {
        file_name,
        type_name,
        schema: serde_json::to_value(schema_for!(T)).expect("schema serializes to JSON"),
    }
}

fn schema_output_dir(root: &Path) -> PathBuf {
    root.join("bindings/contract/generated/model_schemas")
}

fn generated_schema_content(artifact: &SchemaArtifact) -> Result<String> {
    let mut value = artifact.schema.clone();
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "xFlareGeneratedBy".to_string(),
            Value::String("flare-im-core-sdk-xtask".to_string()),
        );
        object.insert(
            "xFlareRustType".to_string(),
            Value::String(artifact.type_name.to_string()),
        );
    }
    serde_json::to_string_pretty(&value)
        .map(|mut content| {
            content.push('\n');
            content
        })
        .context("failed to serialize generated schema")
}

fn ensure_schema_has_no_removed_alias_strings(artifact: &SchemaArtifact) -> Result<()> {
    let content = serde_json::to_string(&artifact.schema).context("schema JSON serialization")?;
    let removed = [
        "messages.",
        "conversations.",
        "capabilities.",
        "events.",
        "media.get_file_url",
        "sync.set_conversation_input_state",
        "search_advanced",
        "sourceUrl",
    ];
    let offenders = removed
        .into_iter()
        .filter(|needle| content.contains(needle))
        .collect::<Vec<_>>();
    if !offenders.is_empty() {
        bail!(
            "generated schema {} contains removed compatibility strings: {}",
            artifact.type_name,
            offenders.join(", ")
        );
    }
    Ok(())
}

fn ensure_schema_properties_are_camel_case(artifact: &SchemaArtifact) -> Result<()> {
    let mut invalid = Vec::new();
    collect_non_camel_property_names("$", &artifact.schema, &mut invalid);
    if !invalid.is_empty() {
        bail!(
            "generated schema {} contains non-camelCase property names: {}",
            artifact.type_name,
            invalid.join(", ")
        );
    }
    Ok(())
}

fn collect_non_camel_property_names(path: &str, value: &Value, invalid: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::Object(properties)) = map.get("properties") {
                for name in properties.keys() {
                    if !is_lower_camel_or_plain(name) {
                        invalid.push(format!("{path}.properties.{name}"));
                    }
                }
            }
            for (key, child) in map {
                let child_path = if path == "$" {
                    format!("$.{key}")
                } else {
                    format!("{path}.{key}")
                };
                collect_non_camel_property_names(&child_path, child, invalid);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_non_camel_property_names(&format!("{path}[{index}]"), child, invalid);
            }
        }
        _ => {}
    }
}

fn is_lower_camel_or_plain(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('_')
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '$')
}

#[derive(Debug)]
struct Contracts {
    manifest: Manifest,
    apis: Apis,
    events: Events,
    errors: Errors,
    dispatch: Dispatch,
    direct_invoke: DirectInvoke,
    c_typed_abi: CTypedAbi,
}

impl Contracts {
    fn load(root: &Path) -> Result<Self> {
        let dir = root.join("bindings/contract");
        Ok(Self {
            manifest: read_json(&dir, "manifest.json")?,
            apis: read_json(&dir, "apis.json")?,
            events: read_json(&dir, "events.json")?,
            errors: read_json(&dir, "errors.json")?,
            dispatch: read_json(&dir, "dispatch.json")?,
            direct_invoke: read_json(&dir, "direct_invoke.json")?,
            c_typed_abi: read_json(&dir, "c_typed_abi.json")?,
        })
    }

    fn verify(&self) -> Result<()> {
        require_non_empty("manifest.contractVersion", &self.manifest.contract_version)?;
        require_non_empty("apis.apiContractVersion", &self.apis.api_contract_version)?;
        require_non_empty(
            "events.eventContractVersion",
            &self.events.event_contract_version,
        )?;
        require_non_empty(
            "errors.errorContractVersion",
            &self.errors.error_contract_version,
        )?;

        let api_ids = self.api_ids();
        ensure_unique("apis.json method ids", api_ids.iter().map(String::as_str))?;
        ensure_no_removed_contract_aliases(
            "apis.json method ids",
            api_ids.iter().map(String::as_str),
        )?;

        let event_ids = self
            .events
            .events
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>();
        ensure_unique("events.json event ids", event_ids)?;
        let event_codes = self
            .events
            .events
            .iter()
            .map(|event| event.c_code)
            .collect::<Vec<_>>();
        ensure_unique("events.json C event codes", event_codes)?;

        let error_codes = self.errors.c_abi.codes.as_slice();
        ensure_unique(
            "errors.json error names",
            error_codes.iter().map(|item| item.name.as_str()),
        )?;
        ensure_unique(
            "errors.json error codes",
            error_codes.iter().map(|item| item.code),
        )?;

        let mut dispatch_groups = BTreeMap::<String, BTreeSet<String>>::new();
        for group in &self.dispatch.groups {
            let mut names = Vec::new();
            for operation in &group.operations {
                if operation
                    .aliases
                    .as_ref()
                    .is_some_and(|aliases| !aliases.is_empty())
                {
                    bail!(
                        "dispatch.json group {} contains removed compatibility aliases on op {}",
                        group.id,
                        operation.op
                    );
                }
                names.push(operation.op.clone());
            }
            ensure_unique(
                &format!("dispatch.json group {} operation names", group.id),
                names.iter().map(String::as_str),
            )?;
            dispatch_groups.insert(group.id.clone(), names.into_iter().collect());
        }
        ensure_unique(
            "dispatch.json group ids",
            self.dispatch.groups.iter().map(|group| group.id.as_str()),
        )?;

        ensure_unique(
            "direct_invoke.json routes",
            self.direct_invoke
                .routes
                .iter()
                .map(|route| route.route.as_str()),
        )?;
        ensure_no_removed_contract_aliases(
            "direct_invoke.json routes",
            self.direct_invoke
                .routes
                .iter()
                .map(|route| route.route.as_str()),
        )?;

        for method in self.apis.modules.iter().flat_map(|module| &module.methods) {
            for (symbol, dispatch_op) in c_api_entries(method.c.as_ref()) {
                let Some(dispatch_op) = dispatch_op else {
                    continue;
                };
                let group = c_symbol_runtime_group(&symbol);
                let Some(ops) = dispatch_groups.get(&group) else {
                    bail!(
                        "apis.json method {} references unknown C dispatch group {group:?} via {symbol:?}",
                        method.id
                    );
                };
                if !ops.contains(&dispatch_op) {
                    bail!(
                        "apis.json method {} references missing dispatch op {group}.{dispatch_op}",
                        method.id
                    );
                }
            }
        }

        for export in &self.c_typed_abi.exports {
            if let Some(api_id) = &export.api_id
                && !api_ids.contains(api_id)
            {
                bail!(
                    "c_typed_abi.json export {} references missing api_id {api_id}",
                    export.symbol
                );
            }
        }

        Ok(())
    }

    fn api_ids(&self) -> BTreeSet<String> {
        self.apis
            .modules
            .iter()
            .flat_map(|module| module.methods.iter().map(|method| method.id.clone()))
            .collect()
    }

    fn write_contract_outputs(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(self.contract_outputs(root)?, check, "cargo xtask codegen")
    }

    fn contract_outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        Ok(vec![
            GeneratedOutput {
                path: root.join("bindings/shared/src/generated/contract.rs"),
                content: rustfmt_generated_rust(&self.render_runtime_contract())?,
            },
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/events.rs"),
                content: rustfmt_generated_rust(&self.render_c_events())?,
            },
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/errors.rs"),
                content: rustfmt_generated_rust(&self.render_c_errors())?,
            },
            GeneratedOutput {
                path: root.join("bindings/uniffi/src/generated/types.rs"),
                content: rustfmt_generated_rust(&self.render_uniffi_types())?,
            },
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/contract.rs"),
                content: rustfmt_generated_rust(&self.render_platform_contract("c"))?,
            },
            GeneratedOutput {
                path: root.join("bindings/wasm/src/generated/contract.rs"),
                content: rustfmt_generated_rust(&self.render_platform_contract("wasm"))?,
            },
            GeneratedOutput {
                path: root.join("bindings/tauri/src/generated/contract.rs"),
                content: rustfmt_generated_rust(&self.render_platform_contract("tauri"))?,
            },
            GeneratedOutput {
                path: root.join("bindings/uniffi/src/generated/contract.rs"),
                content: rustfmt_generated_rust(&self.render_platform_contract("uniffi"))?,
            },
            GeneratedOutput {
                path: root.join("bindings/shared/src/generated/mod.rs"),
                content: rustfmt_generated_rust(&render_shared_generated_mod())?,
            },
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/mod.rs"),
                content: rustfmt_generated_rust(&render_generated_mod(&[
                    "client_config",
                    "contract",
                    "events",
                    "errors",
                    "json_dispatch",
                    "invoke",
                    "typed_abi",
                ]))?,
            },
            GeneratedOutput {
                path: root.join("bindings/uniffi/src/generated/mod.rs"),
                content: rustfmt_generated_rust(&render_generated_mod(&[
                    "client_config",
                    "contract",
                    "types",
                    "invoke",
                    "events",
                ]))?,
            },
            GeneratedOutput {
                path: root.join("bindings/wasm/src/generated/mod.rs"),
                content: rustfmt_generated_rust(&render_generated_mod(&[
                    "client_config",
                    "contract",
                    "bindings",
                    "events",
                ]))?,
            },
            GeneratedOutput {
                path: root.join("bindings/tauri/src/generated/mod.rs"),
                content: rustfmt_generated_rust(&render_generated_mod(&[
                    "contract",
                    "event_emit",
                    "handler",
                    "invoke",
                ]))?,
            },
        ])
    }

    fn render_runtime_contract(&self) -> String {
        let api_operations = self.api_operations();
        let build_ops = message_build_ops(&api_operations);
        let mut lines = vec![
            contract_header().to_string(),
            "#![allow(dead_code)]".to_string(),
            String::new(),
            "#[derive(Debug, Clone, Copy, PartialEq, Eq)]".to_string(),
            "pub struct ApiOperation { pub id: &'static str, pub module: &'static str, pub core: Option<&'static str>, pub c_symbol: Option<&'static str>, pub c_dispatch_op: Option<&'static str>, pub tauri: Option<&'static str>, pub dev_only: bool }".to_string(),
            "#[derive(Debug, Clone, Copy, PartialEq, Eq)]".to_string(),
            "pub struct MessageBuildCatalogEntry { pub op: &'static str, pub method: &'static str, pub stability: &'static str, pub source_operation: &'static str }".to_string(),
            "#[derive(Debug, Clone, Copy, PartialEq, Eq)]".to_string(),
            "pub struct EventDescriptor { pub id: &'static str, pub c_code: i32, pub c_code_name: &'static str, pub tauri: Option<&'static str> }".to_string(),
            "#[derive(Debug, Clone, Copy, PartialEq, Eq)]".to_string(),
            "pub struct ErrorCode { pub name: &'static str, pub code: i32, pub meaning: &'static str }".to_string(),
            String::new(),
            format!("pub const BINDING_CONTRACT_VERSION: &str = {};", rust_string_literal(&self.manifest.contract_version)),
            format!("pub const API_CONTRACT_VERSION: &str = {};", rust_string_literal(&self.apis.api_contract_version)),
            format!("pub const EVENT_CONTRACT_VERSION: &str = {};", rust_string_literal(&self.events.event_contract_version)),
            format!("pub const ERROR_CONTRACT_VERSION: &str = {};", rust_string_literal(&self.errors.error_contract_version)),
            String::new(),
            "pub const API_OPERATIONS: &[ApiOperation] = &[".to_string(),
        ];
        for row in &api_operations {
            lines.push(format!(
                "ApiOperation {{ id: {}, module: {}, core: {}, c_symbol: {}, c_dispatch_op: {}, tauri: {}, dev_only: {} }},",
                rust_string_literal(&row.id),
                rust_string_literal(&row.module),
                rust_option_string(row.core.as_deref()),
                rust_option_string(row.c_symbol.as_deref()),
                rust_option_string(row.c_dispatch_op.as_deref()),
                rust_option_string(row.tauri.as_deref()),
                row.dev_only,
            ));
        }
        lines.extend(
            [
                "];",
                "",
                "pub const MESSAGE_BUILD_OPS: &[MessageBuildCatalogEntry] = &[",
            ]
            .into_iter()
            .map(str::to_string),
        );
        for row in &build_ops {
            lines.push(format!(
                "MessageBuildCatalogEntry {{ op: {}, method: {}, stability: \"stable\", source_operation: {} }},",
                rust_string_literal(&row.op),
                rust_string_literal(&row.method),
                rust_string_literal(&row.source_operation),
            ));
        }
        lines.extend(
            [
                "];",
                "",
                "pub const EVENT_DESCRIPTORS: &[EventDescriptor] = &[",
            ]
            .into_iter()
            .map(str::to_string),
        );
        for event in &self.events.events {
            lines.push(format!(
                "EventDescriptor {{ id: {}, c_code: {}, c_code_name: {}, tauri: {} }},",
                rust_string_literal(&event.id),
                event.c_code,
                rust_string_literal(&event.c_code_name),
                rust_option_string(event.tauri.as_deref()),
            ));
        }
        lines.extend(
            ["];", "", "pub const ERROR_CODES: &[ErrorCode] = &["]
                .into_iter()
                .map(str::to_string),
        );
        for error in &self.errors.c_abi.codes {
            lines.push(format!(
                "ErrorCode {{ name: {}, code: {}, meaning: {} }},",
                rust_string_literal(&error.name),
                error.code,
                rust_string_literal(&error.meaning),
            ));
        }
        lines.extend(["];", ""].into_iter().map(str::to_string));
        lines.join("\n")
    }

    fn render_platform_contract(&self, platform: &str) -> String {
        let api_operations = self.api_operations();
        let mut lines = vec![
            contract_header().to_string(),
            "#![allow(dead_code)]".to_string(),
            String::new(),
            format!(
                "pub const PLATFORM_BINDING: &str = {};",
                rust_string_literal(platform)
            ),
            format!(
                "pub const BINDING_CONTRACT_VERSION: &str = {};",
                rust_string_literal(&self.manifest.contract_version)
            ),
            String::new(),
        ];
        match platform {
            "c" => self.render_c_platform_contract(&mut lines),
            "tauri" => self.render_tauri_platform_contract(&mut lines, &api_operations),
            "wasm" => self.render_operation_platform_contract(
                &mut lines,
                "WASM_CANONICAL_OPERATIONS",
                &api_operations,
            ),
            "uniffi" => self.render_operation_platform_contract(
                &mut lines,
                "UNIFFI_CANONICAL_OPERATIONS",
                &api_operations,
            ),
            _ => {}
        }
        lines.extend(
            ["", "pub const EVENT_CODES: &[(&str, i32)] = &["]
                .into_iter()
                .map(str::to_string),
        );
        for event in &self.events.events {
            lines.push(format!(
                "({}, {}),",
                rust_string_literal(&event.id),
                event.c_code
            ));
        }
        lines.extend(
            ["];", "", "pub const ERROR_CODES: &[(&str, i32)] = &["]
                .into_iter()
                .map(str::to_string),
        );
        for error in &self.errors.c_abi.codes {
            lines.push(format!(
                "({}, {}),",
                rust_string_literal(&error.name),
                error.code
            ));
        }
        lines.extend(["];", ""].into_iter().map(str::to_string));
        lines.join("\n")
    }

    fn render_c_platform_contract(&self, lines: &mut Vec<String>) {
        let mut symbols = BTreeSet::new();
        let mut dispatch_ops = Vec::new();
        for module in &self.apis.modules {
            for method in &module.methods {
                if method.dev_only {
                    continue;
                }
                for (symbol, dispatch) in c_api_entries(method.c.as_ref()) {
                    symbols.insert(symbol);
                    if let Some(dispatch) = dispatch {
                        dispatch_ops.push((method.id.clone(), dispatch));
                    }
                }
            }
        }
        dispatch_ops.sort();
        lines.push("pub const C_API_SYMBOLS: &[&str] = &[".to_string());
        for symbol in symbols {
            lines.push(format!("{},", rust_string_literal(&symbol)));
        }
        lines.extend(
            ["];", "", "pub const C_DISPATCH_OPS: &[(&str, &str)] = &["]
                .into_iter()
                .map(str::to_string),
        );
        for (api_id, dispatch) in dispatch_ops {
            lines.push(format!(
                "({}, {}),",
                rust_string_literal(&api_id),
                rust_string_literal(&dispatch)
            ));
        }
        lines.push("];".to_string());
    }

    fn render_tauri_platform_contract(
        &self,
        lines: &mut Vec<String>,
        api_operations: &[ApiOperationRow],
    ) {
        let commands = api_operations
            .iter()
            .filter(|op| !op.dev_only)
            .filter_map(|op| op.tauri.as_deref())
            .filter(|tauri| tauri.starts_with("sdk_"))
            .collect::<BTreeSet<_>>();
        lines.push("pub const TAURI_COMMANDS: &[&str] = &[".to_string());
        for command in commands {
            lines.push(format!("{},", rust_string_literal(command)));
        }
        lines.push("];".to_string());
    }

    fn render_operation_platform_contract(
        &self,
        lines: &mut Vec<String>,
        const_name: &str,
        api_operations: &[ApiOperationRow],
    ) {
        if const_name == "WASM_CANONICAL_OPERATIONS" {
            lines.push("pub const WASM_ACCEPTS_OPERATION_INVOKE: bool = true;".to_string());
        }
        lines.push(format!("pub const {const_name}: &[&str] = &["));
        for op in api_operations.iter().filter(|op| !op.dev_only) {
            lines.push(format!("{},", rust_string_literal(&op.id)));
        }
        lines.push("];".to_string());
    }

    fn render_c_events(&self) -> String {
        let mut lines = vec![
            c_events_header().to_string(),
            "#![allow(dead_code)]".to_string(),
            String::new(),
            "pub const FLARE_EVENT_UNKNOWN: i32 = 0;".to_string(),
        ];
        for event in &self.events.events {
            lines.push(format!(
                "pub const {}: i32 = {};",
                event.c_code_name, event.c_code
            ));
        }
        lines.extend(
            [
                "",
                "pub fn event_code_by_id(id: &str) -> i32 {",
                "    use crate::generated::contract::EVENT_CODES;",
                "    EVENT_CODES",
                "        .iter()",
                "        .find_map(|(event_id, code)| (*event_id == id).then_some(*code))",
                "        .unwrap_or(FLARE_EVENT_UNKNOWN)",
                "}",
                "",
            ]
            .into_iter()
            .map(str::to_string),
        );
        lines.join("\n")
    }

    fn render_c_errors(&self) -> String {
        let mut lines = vec![
            c_errors_header().to_string(),
            "#![allow(dead_code)]".to_string(),
            String::new(),
        ];
        for error in &self.errors.c_abi.codes {
            lines.push(format!("pub const {}: i32 = {};", error.name, error.code));
        }
        lines.extend(
            [
                "",
                "pub fn error_code_to_c(code: flare_im_core_sdk::ErrorCode) -> i32 {",
                "    use flare_im_core_sdk::ErrorCode;",
                "    match code {",
            ]
            .into_iter()
            .map(str::to_string),
        );
        let mut mapping = BTreeMap::<String, String>::new();
        for error in &self.errors.c_abi.codes {
            for core in &error.core_codes {
                mapping
                    .entry(core.clone())
                    .or_insert_with(|| error.name.clone());
            }
        }
        for (core, name) in mapping {
            lines.push(format!("ErrorCode::{core} => {name},"));
        }
        lines.extend(
            ["_ => FLARE_ERR_INTERNAL,", "}", "}", ""]
                .into_iter()
                .map(str::to_string),
        );
        lines.join("\n")
    }

    fn render_uniffi_types(&self) -> String {
        let mut lines = vec![
            contract_header().to_string(),
            "#![allow(dead_code)]".to_string(),
            String::new(),
            "/// Stable C ABI error codes mirrored for UniFFI consumers.".to_string(),
            "#[derive(Debug, Clone, Copy, PartialEq, Eq)]".to_string(),
            "pub enum BindingErrorCode {".to_string(),
        ];
        for error in &self.errors.c_abi.codes {
            lines.push(format!(
                "{} = {},",
                uniffi_error_variant(&error.name),
                error.code
            ));
        }
        lines.extend(
            [
                "}",
                "",
                "/// Canonical SDK event ids for UniFFI consumers.",
                "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
                "pub enum BindingEventId {",
            ]
            .into_iter()
            .map(str::to_string),
        );
        for event in &self.events.events {
            lines.push(format!("{},", uniffi_event_variant(&event.id)));
        }
        lines.extend(["}", ""].into_iter().map(str::to_string));
        lines.join("\n")
    }

    fn api_operations(&self) -> Vec<ApiOperationRow> {
        let mut rows = Vec::new();
        for module in &self.apis.modules {
            for method in &module.methods {
                let (c_symbol, c_dispatch_op) = c_api_first_parts(method.c.as_ref());
                rows.push(ApiOperationRow {
                    id: method.id.clone(),
                    module: module.id.clone(),
                    core: method.core.clone(),
                    c_symbol,
                    c_dispatch_op,
                    tauri: method.tauri.clone(),
                    dev_only: method.dev_only,
                });
            }
        }
        rows
    }

    fn write_dispatch_outputs(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(self.dispatch_outputs(root)?, check, "cargo xtask codegen")
    }

    fn dispatch_outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        let out_dir = root.join("bindings/shared/src/generated/dispatch");
        let mut outputs = vec![GeneratedOutput {
            path: out_dir.join("mod.rs"),
            content: rustfmt_generated_rust(&self.render_dispatch_mod())?,
        }];
        for group in &self.dispatch.groups {
            outputs.push(GeneratedOutput {
                path: out_dir.join(format!("{}.rs", group.id)),
                content: rustfmt_generated_rust(&render_dispatch_group(group)?)?,
            });
        }
        Ok(outputs)
    }

    fn render_dispatch_mod(&self) -> String {
        let mut lines = vec![dispatch_header().to_string()];
        for group in &self.dispatch.groups {
            lines.push(format!("pub mod {};", group.id));
        }
        lines.push(String::new());
        for group in &self.dispatch.groups {
            lines.push(format!("pub use {}::{};", group.id, group.dispatch_fn));
            lines.push(format!("pub use {}::{};", group.id, group.is_fn));
            lines.push(format!("pub use {}::{};", group.id, group.ops_const));
        }
        lines.push(String::new());
        lines.join("\n")
    }

    fn write_platform_outputs(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(self.platform_outputs(root)?, check, "cargo xtask codegen")
    }

    fn platform_outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        Ok(vec![
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/json_dispatch.rs"),
                content: rustfmt_generated_rust(&self.render_c_json_dispatch())?,
            },
            GeneratedOutput {
                path: root.join("bindings/c/src/generated/invoke.rs"),
                content: rustfmt_generated_rust(&render_c_invoke())?,
            },
            GeneratedOutput {
                path: root.join("bindings/tauri/src/generated/handler.rs"),
                content: rustfmt_generated_rust(&self.render_tauri_handler())?,
            },
            GeneratedOutput {
                path: root.join("bindings/tauri/src/generated/invoke.rs"),
                content: rustfmt_generated_rust(&render_tauri_invoke())?,
            },
            GeneratedOutput {
                path: root.join("bindings/wasm/src/generated/bindings.rs"),
                content: rustfmt_generated_rust(&render_wasm_bindings())?,
            },
            GeneratedOutput {
                path: root.join("bindings/uniffi/src/generated/invoke.rs"),
                content: rustfmt_generated_rust(&render_uniffi_invoke())?,
            },
        ])
    }

    fn c_dispatch_channels(&self) -> Vec<CDispatchChannel> {
        let mut seen = BTreeSet::new();
        let mut channels = Vec::new();
        for method in self.apis.modules.iter().flat_map(|module| &module.methods) {
            for (symbol, dispatch_op) in c_api_entries(method.c.as_ref()) {
                if dispatch_op.is_none() || !seen.insert(symbol.clone()) {
                    continue;
                }
                let channel = c_symbol_channel(&symbol);
                let runtime_group = if channel == "message_build" {
                    "message_build".to_string()
                } else {
                    channel.replace("_dispatch", "")
                };
                channels.push(CDispatchChannel {
                    symbol,
                    channel,
                    runtime_group,
                });
            }
        }
        channels.sort_by_key(|channel| c_dispatch_channel_order(&channel.channel));
        channels
    }

    fn tauri_lifecycle_commands(&self) -> Vec<String> {
        // 需要 AppHandle 或宿主状态的专用命令；令牌续期要向前端发事件并保存配对表。
        let keep = [
            "sdk_init",
            "sdk_login",
            "sdk_prepare",
            "sdk_connect",
            "sdk_logout",
            "sdk_set_connect_token_refresher",
            "sdk_complete_connect_token_refresh",
        ];
        let mut found = Vec::new();
        for method in self.apis.modules.iter().flat_map(|module| &module.methods) {
            if let Some(tauri) = method.tauri.as_deref()
                && keep.contains(&tauri)
            {
                found.push(tauri.to_string());
            }
        }
        keep.iter()
            .filter(|command| found.iter().any(|value| value == *command))
            .map(|command| (*command).to_string())
            .collect()
    }

    fn render_c_json_dispatch(&self) -> String {
        let blocks = self
            .c_dispatch_channels()
            .iter()
            .map(render_c_json_dispatch_block)
            .collect::<String>();
        format!(
            "{header}#![allow(clippy::too_many_lines)]\n\n\
             use std::ffi::{{c_char, c_void}};\n\n\
             use crate::dispatch_common::{{json_dispatch_entry, message_build_dispatch_entry}};\n\
             use crate::types::{{FlareHandle, FlareResultCallback}};\n\n\
             {blocks}\n",
            header = platform_header()
        )
    }

    fn render_tauri_handler(&self) -> String {
        let lifecycle_paths = self
            .tauri_lifecycle_commands()
            .iter()
            .map(|command| format!("crate::commands::lifecycle::{command}"))
            .collect::<Vec<_>>()
            .join(",\n        ");
        format!(
            "{header}#![allow(dead_code)]\n\n\
             pub fn im_invoke_handler() -> impl Fn(tauri::ipc::Invoke) -> bool + Send + 'static {{\n\
             \x20   tauri::generate_handler![\n\
             \x20       crate::generated::invoke::sdk_invoke_json,\n\
             \x20       crate::commands::lifecycle::sdk_ffi_contract_version,\n\
             \x20       {lifecycle_paths}\n\
             \x20   ]\n\
             }}\n",
            header = platform_header()
        )
    }

    fn write_direct_invoke_outputs(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(
            self.direct_invoke_outputs(root)?,
            check,
            "cargo xtask codegen",
        )
    }

    fn write_c_typed_abi_outputs(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(
            self.c_typed_abi_outputs(root)?,
            check,
            "cargo xtask codegen",
        )
    }

    fn c_typed_abi_outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        Ok(vec![GeneratedOutput {
            path: root.join("bindings/c/src/generated/typed_abi.rs"),
            content: rustfmt_generated_rust(&self.render_c_typed_abi()?)?,
        }])
    }

    fn render_c_typed_abi(&self) -> Result<String> {
        let exports = self
            .c_typed_abi
            .exports
            .iter()
            .map(render_c_typed_abi_export)
            .collect::<Result<Vec<_>>>()?
            .join("");
        Ok(format!(
            "{header}\n\
             use std::ffi::{{c_char, c_void}};\n\n\
             use crate::abi;\n\
             use crate::dispatch_common::{{typed_invoke_json, typed_invoke_send_ack, typed_invoke_unit}};\n\
             use crate::executor::{{return_error, CallbackContext}};\n\
             use crate::helpers::{{c_str_to_string, parse_json, parse_upload_options}};\n\
             use crate::registry::require_instance;\n\
             use crate::types::{{FlareBytesView, FlareHandle, FlareResultCallback}};\n\
             {exports}",
            header = c_typed_abi_header()
        ))
    }

    fn direct_invoke_outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        Ok(vec![GeneratedOutput {
            path: root.join("bindings/shared/src/generated/direct_invoke.rs"),
            content: rustfmt_generated_rust(&self.render_direct_invoke()?)?,
        }])
    }

    fn render_direct_invoke(&self) -> Result<String> {
        let route_items = self
            .direct_invoke
            .routes
            .iter()
            .map(render_direct_invoke_route_item)
            .collect::<Vec<_>>()
            .join(",\n    ");
        let route_bool_arms = self
            .direct_invoke
            .routes
            .iter()
            .map(render_direct_invoke_route_bool_arm)
            .collect::<Vec<_>>()
            .join("\n");
        let arms = self
            .direct_invoke
            .routes
            .iter()
            .map(render_direct_invoke_match_arm)
            .collect::<Result<Vec<_>>>()?
            .join("\n");

        Ok(format!(
            "{header}\n\
             use flare_im_core_sdk::serde_json::{{self, Value}};\n\n\
             use crate::dispatch_support;\n\
             use crate::{{BindingResponse, InvokeSession}};\n\
             use flare_im_core_sdk::Result;\n\
             use flare_im_core_sdk::SdkState;\n\n\
             /// Routes handled outside JSON dispatch match tables.\n\
             pub const DIRECT_INVOKE_ROUTES: &[&str] = &[\n\
             \x20   {route_items},\n\
             ];\n\n\
             pub fn is_direct_invoke_route(route: &str) -> bool {{\n\
             \x20   match route {{\n\
             {route_bool_arms}\n\
             \x20       _ => false,\n\
             \x20   }}\n\
             }}\n\n\
             fn sdk_state_json(state: SdkState) -> (&'static str, i32) {{\n\
             \x20   use SdkState as S;\n\
             \x20   match state {{\n\
             \x20       S::Disconnected => (\"disconnected\", 0),\n\
             \x20       S::Connecting => (\"connecting\", 1),\n\
             \x20       S::Connected => (\"connected\", 2),\n\
             \x20       S::Ready => (\"ready\", 3),\n\
             \x20       S::Reconnecting => (\"reconnecting\", 4),\n\
             \x20   }}\n\
             }}\n\n\
             #[derive(flare_im_core_sdk::serde::Deserialize)]\n\
             #[serde(crate = \"flare_im_core_sdk::serde\", rename_all = \"camelCase\")]\n\
             struct SyncMessagesDirectRequest {{\n\
             \x20   conversation_id: String,\n\
             \x20   last_seq: u64,\n\
             \x20   #[serde(default)]\n\
             \x20   limit: Option<i32>,\n\
             }}\n\n\
             pub async fn dispatch_direct(\n\
             \x20   session: &impl InvokeSession,\n\
             \x20   route: &str,\n\
             \x20   request: &Value,\n\
             ) -> Result<BindingResponse> {{\n\
             \x20   match route {{\n\
             {arms}\n\
             \x20       _ => Err(crate::binding_operation_not_supported(route)),\n\
             \x20   }}\n\
             }}\n\n\
             pub async fn dispatch_direct_json(\n\
             \x20   session: &impl InvokeSession,\n\
             \x20   route: &str,\n\
             \x20   request_json: &str,\n\
             ) -> Result<BindingResponse> {{\n\
             \x20   match route {{\n\
             \x20       \"sync.messages\" => {{\n\
             \x20           let client = session.client();\n\
             \x20           let request = dispatch_support::from_json_str::<SyncMessagesDirectRequest>(request_json, \"sync messages request\")?;\n\
             \x20           client.sync_messages(&request.conversation_id, request.last_seq, request.limit.unwrap_or(50)).await?;\n\
             \x20           Ok(BindingResponse::unit())\n\
             \x20       }}\n\
             \x20       \"sync.conversation_summaries_with_versions\" => {{\n\
             \x20           let client = session.client();\n\
             \x20           let request = dispatch_support::from_json_str::<flare_im_core_sdk::model::SyncConversationSummariesRequest>(request_json, \"sync conversation summaries request\")?;\n\
             \x20           let response = client.sync_conversation_summaries_with_versions(request).await?;\n\
             \x20           dispatch_support::json(response)\n\
             \x20       }}\n\
             \x20       _ => {{\n\
             \x20           let request = dispatch_support::dispatch_params_from_json(request_json)?;\n\
             \x20           dispatch_direct(session, route, &request).await\n\
             \x20       }},\n\
             \x20   }}\n\
             }}\n",
            header = direct_invoke_header()
        ))
    }

    fn write_event_outputs(&self, root: &Path, check: bool) -> Result<()> {
        write_generated_outputs(self.event_outputs(root)?, check, "cargo xtask codegen")
    }

    fn event_outputs(&self, root: &Path) -> Result<Vec<GeneratedOutput>> {
        Ok(vec![
            GeneratedOutput {
                path: root.join("bindings/shared/src/generated/event_codes.rs"),
                content: rustfmt_generated_rust(&self.render_event_codes())?,
            },
            GeneratedOutput {
                path: root.join("bindings/shared/src/generated/event_registry.rs"),
                content: rustfmt_generated_rust(&self.render_runtime_event_registry())?,
            },
            GeneratedOutput {
                path: root.join("bindings/tauri/src/generated/event_emit.rs"),
                content: rustfmt_generated_rust(&render_tauri_event_emit())?,
            },
            GeneratedOutput {
                path: root.join("bindings/wasm/src/generated/events.rs"),
                content: rustfmt_generated_rust(&self.render_wasm_events())?,
            },
            GeneratedOutput {
                path: root.join("bindings/uniffi/src/generated/events.rs"),
                content: rustfmt_generated_rust(&self.render_uniffi_events())?,
            },
        ])
    }

    fn render_event_codes(&self) -> String {
        let mut lines = vec![
            generated_events_header(),
            "pub const FLARE_EVENT_UNKNOWN: i32 = 0;".to_string(),
        ];
        for event in &self.events.events {
            lines.push(format!(
                "pub const {}: i32 = {};",
                event.c_code_name, event.c_code
            ));
        }
        lines.push(String::new());
        lines.join("\n")
    }

    fn render_runtime_event_registry(&self) -> String {
        let mut lines = vec![
            generated_events_header(),
            "/// Stable C event code -> contract id -> Tauri event name (`im://*`, `connection://*`, `access://*`).".to_string(),
            "pub const EVENT_ROUTE_TABLE: &[(i32, &str, &str)] = &[".to_string(),
        ];
        for event in self
            .events
            .events
            .iter()
            .filter(|event| event.is_tauri_route())
        {
            lines.push(format!(
                "    ({}, {}, {}),",
                event.c_code,
                rust_string_literal(&event.id),
                rust_string_literal(event.tauri.as_deref().unwrap_or_default())
            ));
        }
        lines.extend(
            [
                "];",
                "",
                "pub fn contract_event_id_for_code(code: i32) -> Option<&'static str> {",
                "    EVENT_ROUTE_TABLE",
                "        .iter()",
                "        .find(|(c, _, _)| *c == code)",
                "        .map(|(_, id, _)| *id)",
                "}",
                "",
                "pub fn tauri_event_name_for_code(code: i32) -> Option<&'static str> {",
                "    EVENT_ROUTE_TABLE",
                "        .iter()",
                "        .find(|(c, _, _)| *c == code)",
                "        .map(|(_, _, name)| *name)",
                "}",
                "",
            ]
            .into_iter()
            .map(str::to_string),
        );
        lines.join("\n")
    }

    fn render_wasm_events(&self) -> String {
        let mut lines = vec![
            generated_events_header(),
            "/// Canonical contract event ids (parity with L3 SDK).".to_string(),
            "pub const CONTRACT_EVENT_IDS: &[&str] = &[".to_string(),
        ];
        for event in &self.events.events {
            lines.push(format!("    {},", rust_string_literal(&event.id)));
        }
        lines.extend(
            [
                "];",
                "",
                "/// `(contract_event_id, web_event_name)` when a web adapter maps `im://*` names.",
                "pub const WEB_EVENT_ROUTES: &[(&str, &str)] = &[",
            ]
            .into_iter()
            .map(str::to_string),
        );
        for event in self
            .events
            .events
            .iter()
            .filter(|event| event.is_tauri_route())
        {
            lines.push(format!(
                "    ({}, {}),",
                rust_string_literal(&event.id),
                rust_string_literal(event.tauri.as_deref().unwrap_or_default())
            ));
        }
        lines.extend(["];", ""].into_iter().map(str::to_string));
        lines.join("\n")
    }

    fn render_uniffi_events(&self) -> String {
        let mut lines = vec![
            generated_events_header(),
            "/// Contract event id <-> stable C ABI code.".to_string(),
            "pub const EVENT_CODE_TABLE: &[(&str, i32)] = &[".to_string(),
        ];
        for event in &self.events.events {
            lines.push(format!(
                "    ({}, {}),",
                rust_string_literal(&event.id),
                event.c_code
            ));
        }
        lines.extend(["];", ""].into_iter().map(str::to_string));
        lines.join("\n")
    }
}

fn generated_events_header() -> String {
    "// @generated by flare-im-core-sdk-xtask\n// Source: bindings/contract/events.json\n"
        .to_string()
}

fn render_tauri_event_emit() -> String {
    [
        generated_events_header().as_str(),
        "use flare_im_core_sdk::event::SdkEvent;\n",
        "use tauri::{AppHandle, Emitter};\n",
        "use crate::convert::sdk_event_to_tauri;\n",
        "/// Forward a core [`SdkEvent`] using the contract event names.\n",
        "pub fn emit_sdk_event<R: tauri::Runtime>(app: &AppHandle<R>, ev: &SdkEvent) -> bool {\n",
        "    if let Some((name, payload)) = sdk_event_to_tauri(ev) {\n",
        "        return app.emit(&name, payload).is_ok();\n",
        "    }\n",
        "    false\n",
        "}\n",
    ]
    .concat()
}

fn read_json<T>(dir: &Path, name: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let path = dir.join(name);
    let data = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&data).with_context(|| format!("failed to parse {}", path.display()))
}

fn require_non_empty(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{label} must not be empty");
    }
    Ok(())
}

fn ensure_unique<T, I>(label: &str, values: I) -> Result<()>
where
    T: ToString,
    I: IntoIterator<Item = T>,
{
    let mut seen = BTreeSet::<String>::new();
    let mut duplicates = BTreeSet::<String>::new();
    for value in values {
        let value = value.to_string();
        if !seen.insert(value.clone()) {
            duplicates.insert(value);
        }
    }
    if !duplicates.is_empty() {
        let joined = duplicates
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("{label} contains duplicate values: {joined}");
    }
    Ok(())
}

fn ensure_no_removed_contract_aliases<'a, I>(label: &str, values: I) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let removed_prefixes = ["events.", "messages.", "conversations.", "capabilities."];
    let removed_ids = ["media.get_file_url", "sync.set_conversation_input_state"];
    let offenders = values
        .into_iter()
        .filter(|api_id| {
            removed_ids.contains(api_id)
                || removed_prefixes
                    .iter()
                    .any(|prefix| api_id.starts_with(prefix))
        })
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if !offenders.is_empty() {
        bail!(
            "{label} contains removed compatibility ids; use singular canonical contract ids instead: {}",
            offenders.join(", ")
        );
    }
    Ok(())
}

fn c_api_entries(value: Option<&Value>) -> Vec<(String, Option<String>)> {
    let Some(value) = value else {
        return Vec::new();
    };
    let values = match value {
        Value::Array(items) => items.iter().collect::<Vec<_>>(),
        other => vec![other],
    };

    values
        .into_iter()
        .filter_map(Value::as_str)
        .filter_map(|item| {
            let (symbol, dispatch) = item
                .split_once(':')
                .map_or((item, None), |(symbol, dispatch)| (symbol, Some(dispatch)));
            (!symbol.is_empty()).then(|| (symbol.to_string(), dispatch.map(str::to_string)))
        })
        .collect()
}

fn c_symbol_runtime_group(symbol: &str) -> String {
    let channel = symbol
        .strip_prefix("flare_")
        .unwrap_or(symbol)
        .strip_suffix("_json")
        .unwrap_or(symbol);
    if channel == "message_build" {
        return "message_build".to_string();
    }
    channel.replace("_dispatch", "")
}

#[derive(Debug, Deserialize)]
struct Manifest {
    #[serde(rename = "contractVersion")]
    contract_version: String,
}

#[derive(Debug, Deserialize)]
struct Apis {
    #[serde(rename = "apiContractVersion")]
    api_contract_version: String,
    modules: Vec<ApiModule>,
}

#[derive(Debug, Deserialize)]
struct ApiModule {
    #[allow(dead_code)]
    id: String,
    methods: Vec<ApiMethod>,
}

#[derive(Debug, Deserialize)]
struct ApiMethod {
    id: String,
    #[serde(default)]
    core: Option<String>,
    #[serde(default)]
    c: Option<Value>,
    #[serde(default)]
    tauri: Option<String>,
    #[serde(default)]
    dev_only: bool,
}

#[derive(Debug, Deserialize)]
struct Events {
    #[serde(rename = "eventContractVersion")]
    event_contract_version: String,
    events: Vec<Event>,
}

#[derive(Debug, Deserialize)]
struct Event {
    id: String,
    #[serde(rename = "cCode")]
    c_code: i32,
    #[serde(rename = "cCodeName")]
    c_code_name: String,
    #[serde(default)]
    tauri: Option<String>,
}

impl Event {
    /// 事件名是 `scheme://name`：消息类在 `im://`，链路状态在 `connection://`，接入点在 `access://`。
    fn is_tauri_route(&self) -> bool {
        self.tauri.as_deref().is_some_and(|name| {
            ["im://", "connection://", "access://"]
                .iter()
                .any(|scheme| name.starts_with(scheme))
        })
    }
}

#[derive(Debug, Deserialize)]
struct Errors {
    #[serde(rename = "errorContractVersion")]
    error_contract_version: String,
    #[serde(rename = "cAbi")]
    c_abi: CAbiErrors,
}

#[derive(Debug, Deserialize)]
struct CAbiErrors {
    codes: Vec<CAbiErrorCode>,
}

#[derive(Debug, Deserialize)]
struct CAbiErrorCode {
    name: String,
    code: i32,
    #[serde(default)]
    meaning: String,
    #[serde(default, rename = "coreCodes")]
    core_codes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Dispatch {
    groups: Vec<DispatchGroup>,
}

#[derive(Debug, Deserialize)]
struct DispatchGroup {
    id: String,
    receiver: DispatchReceiver,
    #[serde(rename = "dispatch_fn")]
    dispatch_fn: String,
    #[serde(rename = "is_fn")]
    is_fn: String,
    #[serde(rename = "ops_const")]
    ops_const: String,
    #[serde(default)]
    op_from_request: bool,
    #[serde(default)]
    extra_receivers: Vec<DispatchExtraReceiver>,
    operations: Vec<DispatchOperation>,
}

#[derive(Debug, Deserialize)]
struct DispatchReceiver {
    binding: String,
}

#[derive(Debug, Deserialize)]
struct DispatchExtraReceiver {
    name: String,
    binding: String,
}

#[derive(Debug, Deserialize)]
struct DispatchOperation {
    op: String,
    method: String,
    #[serde(default)]
    args: Vec<Value>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    fields: BTreeMap<String, String>,
    #[serde(default)]
    aliases: Option<Vec<String>>,
    #[serde(default)]
    cfg: Option<String>,
}

#[derive(Debug, Default)]
struct DispatchArg {
    name: String,
    kind: String,
    wire: Option<String>,
    pass: Option<String>,
    default: Option<i64>,
    min: Option<i64>,
    value: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct DirectInvoke {
    routes: Vec<DirectInvokeRoute>,
}

#[derive(Debug, Deserialize)]
struct DirectInvokeRoute {
    route: String,
    result: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    json_expr: Option<String>,
    #[serde(default)]
    skip_client: bool,
    #[serde(default)]
    cfg: Option<String>,
    #[serde(default)]
    dev_only: bool,
}

impl DirectInvokeRoute {
    fn cfg_condition(&self) -> Option<&str> {
        self.cfg.as_deref().or(if self.dev_only {
            Some("feature = \"dev-test-token\"")
        } else {
            None
        })
    }
}

#[derive(Debug, Deserialize)]
struct CTypedAbi {
    exports: Vec<CTypedAbiExport>,
}

#[derive(Debug, Deserialize)]
struct CTypedAbiExport {
    symbol: String,
    kind: String,
    #[serde(default)]
    api_id: Option<String>,
    #[serde(default)]
    args: Vec<CTypedAbiArg>,
}

#[derive(Debug, Deserialize)]
struct CTypedAbiArg {
    name: String,
    #[serde(rename = "type")]
    arg_type: String,
    #[serde(default)]
    json_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_message_typed_abi_uses_raw_im_message_params() {
        let lines = render_c_typed_params_build(
            &[CTypedAbiArg {
                name: "message_json".to_string(),
                arg_type: "json_message".to_string(),
                json_key: None,
            }],
            true,
        )
        .join("\n");

        assert!(lines.contains("serde_json::to_string(&message)"));
        assert!(!lines.contains("messageJson"));
        assert!(!lines.contains("serde_json::json!"));
    }
}
