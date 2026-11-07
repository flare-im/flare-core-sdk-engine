use anyhow::Result;
use regex::Regex;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use crate::{
    CoreAbiRef, arr, bool_field, core_contract_dir, core_root, emit_errors, fail,
    files_with_extension, load_expanded_client_spec, load_json, spec_dir, str_field,
};

const REMOVED_OPERATION_PREFIXES: &[&str] =
    &["messages.", "conversations.", "capabilities.", "events."];
const REMOVED_OPERATION_IDS: &[&str] = &[
    "media.get_file_url",
    "sync.set_conversation_input_state",
    "search_advanced",
    "message.build",
    // 链路状态只有 connection.get_status；前台/后台只经 lifecycle.set_app_state。
    "connection.get_state",
    "sdk.set_heartbeat_app_state",
];
const REMOVED_JSON_KEYS: &[&str] = &["sourceUrl", "sdk_config", "ws_url", "quic_url", "http_url"];
const CORE_TYPED_COVERAGE_MODULES: &[&str] = &[
    "session",
    "sync",
    "events",
    "presence",
    "message_builder",
    "message",
    "conversation",
    "media",
    "capability",
    "rich_doc_v2",
];

pub(crate) fn verify_core_contract(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let contract = core_contract_dir(root);
    let core_direct_invoke = contract.join("direct_invoke.json");
    for path in [
        contract.join("manifest.json"),
        contract.join("apis.json"),
        core_direct_invoke.clone(),
        contract.join("dispatch.json"),
        contract.join("c_typed_abi.json"),
        contract.join("events.json"),
        contract.join("errors.json"),
        spec_dir(root).join("manifest.json"),
    ] {
        if !path.is_file() {
            fail(
                &mut errors,
                format!("missing required contract file: {}", path.display()),
            );
        }
    }
    emit_errors("core contract parity error", errors)?;

    let mut errors = Vec::new();
    let spec = load_expanded_client_spec(root)?;
    let core_apis = load_json(&contract.join("apis.json"))?;
    let direct_invoke = load_json(&core_direct_invoke)?;
    verify_no_removed_contract_ids(&mut errors, &spec, &core_apis, &direct_invoke)?;
    verify_core_direct_invoke_coverage(&mut errors, &spec, &direct_invoke);
    verify_core_typed_operation_coverage(&mut errors, &spec, &core_apis);
    verify_core_abi_coverage(&mut errors, root, &spec, &core_apis)?;
    verify_event_coverage(
        &mut errors,
        &spec,
        &load_json(&contract.join("events.json"))?,
    );
    verify_error_coverage(
        &mut errors,
        &spec,
        &load_json(&contract.join("errors.json"))?,
    );

    emit_errors("core contract parity error", errors)?;
    println!("core contract parity verified");
    Ok(())
}

fn client_methods(spec: &Value) -> Vec<(&Value, &Value)> {
    arr(spec.get("modules").unwrap_or(&Value::Null))
        .iter()
        .flat_map(|module| {
            arr(module.get("methods").unwrap_or(&Value::Null))
                .iter()
                .map(move |method| (module, method))
        })
        .collect()
}

fn is_removed_operation_id(value: &str) -> bool {
    REMOVED_OPERATION_IDS.contains(&value)
        || REMOVED_OPERATION_PREFIXES
            .iter()
            .any(|prefix| value.starts_with(prefix))
}

fn verify_no_removed_contract_ids(
    errors: &mut Vec<String>,
    spec: &Value,
    core_apis: &Value,
    direct_invoke: &Value,
) -> Result<()> {
    for module in arr(core_apis.get("modules").unwrap_or(&Value::Null)) {
        for method in arr(module.get("methods").unwrap_or(&Value::Null)) {
            let api_id = str_field(method, "id");
            if is_removed_operation_id(api_id) {
                fail(
                    errors,
                    format!("core contract contains removed operation id: {api_id}"),
                );
            }
        }
    }
    for route in arr(direct_invoke.get("routes").unwrap_or(&Value::Null)) {
        let route_id = str_field(route, "route");
        if is_removed_operation_id(route_id) {
            fail(
                errors,
                format!("core direct invoke contains removed route: {route_id}"),
            );
        }
    }
    for (module, method) in client_methods(spec) {
        let operation = str_field(method, "operation");
        if is_removed_operation_id(operation) {
            fail(
                errors,
                format!("sdk-spec contains removed operation id: {operation}"),
            );
        }
        let dispatch_op = str_field(method, "dispatchOp");
        if REMOVED_OPERATION_IDS.contains(&dispatch_op) {
            fail(
                errors,
                format!(
                    "sdk-spec contains removed dispatch op: {}.{dispatch_op}",
                    str_field(module, "key")
                ),
            );
        }
    }
    for module in arr(spec.get("modules").unwrap_or(&Value::Null)) {
        for op in arr(module
            .get("dispatch")
            .and_then(|dispatch| dispatch.get("ops"))
            .unwrap_or(&Value::Null))
        {
            if let Some(op) = op.as_str()
                && REMOVED_OPERATION_IDS.contains(&op)
            {
                fail(
                    errors,
                    format!(
                        "sdk-spec dispatch ops contain removed op: {}.{op}",
                        str_field(module, "key")
                    ),
                );
            }
        }
    }
    let catalog = spec.get("messageBuildCatalog").unwrap_or(&Value::Null);
    if str_field(catalog, "source").contains("messages.build") {
        fail(
            errors,
            format!(
                "sdk-spec message build catalog source uses removed id: {}",
                str_field(catalog, "source")
            ),
        );
    }
    for entry in arr(catalog.get("entries").unwrap_or(&Value::Null)) {
        let source_operation = str_field(entry, "sourceOperation");
        if is_removed_operation_id(source_operation) {
            fail(
                errors,
                format!(
                    "sdk-spec message build catalog contains removed sourceOperation: {source_operation}"
                ),
            );
        }
    }
    let encoded = serde_json::to_string(spec)?;
    for key in REMOVED_JSON_KEYS {
        if encoded.contains(&format!("\"{key}\"")) {
            fail(errors, format!("sdk-spec contains removed JSON key: {key}"));
        }
    }
    Ok(())
}

fn verify_core_direct_invoke_coverage(
    errors: &mut Vec<String>,
    spec: &Value,
    direct_invoke: &Value,
) {
    let client_operations = client_methods(spec)
        .into_iter()
        .map(|(_, method)| str_field(method, "operation").to_string())
        .collect::<BTreeSet<_>>();
    for route in arr(direct_invoke.get("routes").unwrap_or(&Value::Null)) {
        let route_id = str_field(route, "route");
        if !route_id.is_empty() && !client_operations.contains(route_id) {
            fail(
                errors,
                format!("core direct invoke route missing from expanded sdk-spec: {route_id}"),
            );
        }
    }
}

fn is_dev_only(method: &Value) -> bool {
    bool_field(method, "dev_only") || bool_field(method, "devOnly")
}

fn verify_core_typed_operation_coverage(errors: &mut Vec<String>, spec: &Value, core_apis: &Value) {
    let client_operations = client_methods(spec)
        .into_iter()
        .map(|(_, method)| str_field(method, "operation").to_string())
        .collect::<BTreeSet<_>>();
    for module in arr(core_apis.get("modules").unwrap_or(&Value::Null)) {
        let module_id = str_field(module, "id");
        if !CORE_TYPED_COVERAGE_MODULES.contains(&module_id) {
            continue;
        }
        for method in arr(module.get("methods").unwrap_or(&Value::Null)) {
            if is_dev_only(method) {
                continue;
            }
            let api_id = str_field(method, "id");
            if !client_operations.contains(api_id) {
                fail(
                    errors,
                    format!("core API missing typed client operation: {module_id}.{api_id}"),
                );
            }
        }
    }
}

fn c_entries(value: Option<&Value>) -> Vec<&str> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        Some(Value::String(raw)) => vec![raw.as_str()],
        _ => vec![],
    }
}

fn parse_core_c_ref(raw: &str) -> Option<(&str, Option<&str>)> {
    if !raw.starts_with("flare_") {
        return None;
    }
    let (symbol, dispatch_op) = raw
        .split_once(':')
        .map_or((raw, None), |(symbol, op)| (symbol, Some(op)));
    Some((symbol, dispatch_op))
}

fn collect_core_abi_refs(core_apis: &Value) -> Vec<CoreAbiRef> {
    let mut refs = Vec::new();
    for module in arr(core_apis.get("modules").unwrap_or(&Value::Null)) {
        for method in arr(module.get("methods").unwrap_or(&Value::Null)) {
            if is_dev_only(method) {
                continue;
            }
            let api_id = str_field(method, "id");
            for raw in c_entries(method.get("c")) {
                if let Some((symbol, dispatch_op)) = parse_core_c_ref(raw) {
                    refs.push(CoreAbiRef {
                        api_id: api_id.to_string(),
                        symbol: symbol.to_string(),
                        dispatch_op: dispatch_op.map(str::to_string),
                    });
                }
            }
        }
    }
    refs
}

fn collect_core_exported_symbols(root: &Path) -> Result<BTreeSet<String>> {
    let mut symbols = BTreeSet::new();
    let dir = core_root(root).join("bindings/c/src");
    if !dir.is_dir() {
        return Ok(symbols);
    }
    let re = Regex::new(r#"pub\s+extern\s+"C"\s+fn\s+(flare_[A-Za-z0-9_]+)"#)?;
    for path in files_with_extension(&dir, "rs")? {
        let text = fs::read_to_string(&path)?;
        symbols.extend(
            re.captures_iter(&text)
                .filter_map(|cap| cap.get(1))
                .map(|m| m.as_str().to_string()),
        );
    }
    Ok(symbols)
}

fn verify_core_abi_coverage(
    errors: &mut Vec<String>,
    root: &Path,
    spec: &Value,
    core_apis: &Value,
) -> Result<()> {
    let refs = collect_core_abi_refs(core_apis);
    let methods = client_methods(spec);
    let client_symbols = methods
        .iter()
        .filter_map(|(_, method)| method.get("cApi").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    let client_dispatch_pairs = methods
        .iter()
        .filter_map(|(_, method)| {
            Some((
                method.get("cApi").and_then(Value::as_str)?,
                method.get("dispatchOp").and_then(Value::as_str)?,
            ))
        })
        .collect::<BTreeSet<_>>();
    let exported_symbols = collect_core_exported_symbols(root)?;

    let mut refs_by_api = BTreeMap::<String, Vec<&CoreAbiRef>>::new();
    for ref_item in &refs {
        refs_by_api
            .entry(ref_item.api_id.clone())
            .or_default()
            .push(ref_item);
    }
    for (api_id, alternatives) in refs_by_api {
        let covered = alternatives
            .iter()
            .any(|ref_item| match ref_item.dispatch_op.as_deref() {
                Some(op) => client_dispatch_pairs.contains(&(ref_item.symbol.as_str(), op)),
                None => client_symbols.contains(ref_item.symbol.as_str()),
            });
        if !covered {
            let alternatives_text = alternatives
                .iter()
                .map(|ref_item| {
                    ref_item.dispatch_op.as_ref().map_or_else(
                        || ref_item.symbol.clone(),
                        |op| format!("{}:{op}", ref_item.symbol),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            fail(
                errors,
                format!(
                    "core API path missing from expanded sdk-spec: {api_id} -> {alternatives_text}"
                ),
            );
        }
    }
    // 这里原先是「每个非 dispatch 的符号都必须出现在 sdk-spec 里」，与上面那段
    // 按 api_id 取**任一备选**的语义直接打架：`apis.json` 允许一个 op 声明多条 C 绑定
    // （`message.send` / `message.recall` / `message.delete` 都是两条），sdk-spec 只会
    // 选其中一条对外暴露。send 选了 dispatch 那条，于是这条逐符号的检查一直报
    // 「core API symbol missing from sdk-spec: message.send -> flare_message_send」——
    // 而按备选语义它本来就该算覆盖到了。recall / delete 没红，只是因为两边碰巧都选了
    // typed 那条。这道门禁从没进过 CI，所以这个自相矛盾一直没人撞上。
    //
    // 逐符号那条删掉，换成一条它原本没做、也确实该做的检查：**apis.json 里声明的
    // 每个符号，都得真的被导出**。上面第三段只验了反方向（sdk-spec 写的 cApi 必须
    // 存在），声明侧一直没人管——apis.json 写错一个符号名，除了这里没有别的信号。
    for ref_item in &refs {
        if !exported_symbols.contains(&ref_item.symbol) {
            fail(
                errors,
                format!(
                    "apis.json declares a C symbol that is not exported: {} -> {}",
                    ref_item.api_id, ref_item.symbol
                ),
            );
        }
    }
    for (_, method) in methods {
        let symbol = str_field(method, "cApi");
        if symbol.starts_with("flare_") && !exported_symbols.contains(symbol) {
            fail(
                errors,
                format!(
                    "sdk-spec cApi is not exported by core C bindings: {} -> {symbol}",
                    str_field(method, "operation")
                ),
            );
        }
    }
    Ok(())
}

fn verify_event_coverage(errors: &mut Vec<String>, spec: &Value, core_events: &Value) {
    let shared_events = arr(spec.get("events").unwrap_or(&Value::Null))
        .iter()
        .filter(|event| !str_field(event, "type").is_empty())
        .map(|event| {
            (
                str_field(event, "type").to_string(),
                arr(event.get("names").unwrap_or(&Value::Null))
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let event_model_group = arr(spec.get("modelGroups").unwrap_or(&Value::Null))
        .iter()
        .find(|group| str_field(group, "group") == "events")
        .cloned()
        .unwrap_or(Value::Null);
    let enum_values = arr(event_model_group.get("enums").unwrap_or(&Value::Null))
        .iter()
        .filter(|enum_value| !str_field(enum_value, "name").is_empty())
        .map(|enum_value| {
            (
                str_field(enum_value, "name").to_string(),
                arr(enum_value.get("values").unwrap_or(&Value::Null))
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    for event in arr(core_events.get("events").unwrap_or(&Value::Null)) {
        let event_id = str_field(event, "id");
        let Some((kind, name)) = event_id.split_once('.') else {
            fail(
                errors,
                format!("core event id is not kind.name: {event_id}"),
            );
            continue;
        };
        if !shared_events
            .get(kind)
            .is_some_and(|names| names.contains(name))
        {
            fail(
                errors,
                format!("core event missing from sdk-spec/shared/events.json: {event_id}"),
            );
        }
        let enum_name = match kind {
            "connection" => Some("ConnectionEventName"),
            "message" => Some("MessageEventName"),
            "conversation" => Some("ConversationEventName"),
            "sync" => Some("SyncEventName"),
            "plugin" => Some("PluginEventName"),
            _ => None,
        };
        if let Some(enum_name) = enum_name
            && !enum_values
                .get(enum_name)
                .is_some_and(|values| values.contains(name))
        {
            fail(
                errors,
                format!(
                    "core event missing from sdk-spec/models/events.json {enum_name}: {event_id}"
                ),
            );
        }
        if kind == "extension"
            && !enum_values
                .get("SdkEventKind")
                .is_some_and(|values| values.contains("extension_event"))
        {
            fail(
                errors,
                "core extension events require SdkEventKind.extension_event",
            );
        }
    }
}

fn verify_error_coverage(errors: &mut Vec<String>, spec: &Value, core_errors: &Value) {
    let spec_errors = arr(spec
        .get("cAbi")
        .and_then(|cabi| cabi.get("codes"))
        .unwrap_or(&Value::Null))
    .iter()
    .filter_map(|item| {
        Some((
            str_field(item, "name").to_string(),
            item.get("code")?.clone(),
        ))
    })
    .collect::<BTreeMap<_, _>>();
    if spec_errors.is_empty() {
        return;
    }
    for item in arr(core_errors
        .get("cAbi")
        .and_then(|cabi| cabi.get("codes"))
        .unwrap_or(&Value::Null))
    {
        let name = str_field(item, "name");
        let code = item.get("code").cloned().unwrap_or(Value::Null);
        if spec_errors.get(name) != Some(&code) {
            fail(
                errors,
                format!("core C ABI error code missing or changed in sdk-spec: {name}={code}"),
            );
        }
    }
}
