use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn arr(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or(&[])
}

pub(crate) fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

pub(crate) fn bool_field(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

/// A bridge-level operation (`"visibility": "bridge"`): platform bridges call it themselves, so it
/// stays in the native call map but out of every public facade.
pub(crate) fn is_bridge_internal_method(method: &Value) -> bool {
    str_field(method, "visibility") == "bridge"
}

/// `setConnectTokenRefresher`: every facade takes the host's async function itself (not the
/// bridge-level `{ enabled }` request) and hands it to its bridge's hook of the same name.
pub(crate) fn is_connect_token_refresher_method(method: &Value) -> bool {
    str_field(method, "operation") == "sdk.set_connect_token_refresher"
}

pub(crate) fn include_paths<'a>(manifest: &'a Value, key: &str) -> Vec<&'a str> {
    manifest
        .get("include")
        .and_then(|include| include.get(key))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

pub(crate) fn include_path<'a>(manifest: &'a Value, key: &str) -> Option<&'a str> {
    manifest
        .get("include")
        .and_then(|include| include.get(key))
        .and_then(Value::as_str)
}

pub(crate) fn child_arr<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

pub(crate) fn all_spec_models(spec: &Value) -> Vec<&Value> {
    child_arr(spec, "modelGroups")
        .iter()
        .flat_map(|group| child_arr(group, "models").iter())
        .collect()
}

pub(crate) fn all_spec_enums(spec: &Value) -> Vec<&Value> {
    child_arr(spec, "modelGroups")
        .iter()
        .flat_map(|group| child_arr(group, "enums").iter())
        .collect()
}

pub(crate) fn spec_model_names(spec: &Value) -> BTreeSet<String> {
    all_spec_models(spec)
        .into_iter()
        .map(|model| str_field(model, "name").to_string())
        .collect()
}

pub(crate) fn spec_enum_names(spec: &Value) -> BTreeSet<String> {
    all_spec_enums(spec)
        .into_iter()
        .map(|enum_value| str_field(enum_value, "name").to_string())
        .collect()
}

pub(crate) fn spec_enum_map(spec: &Value) -> BTreeMap<String, &Value> {
    all_spec_enums(spec)
        .into_iter()
        .map(|enum_value| (str_field(enum_value, "name").to_string(), enum_value))
        .collect()
}

pub(crate) fn typescript_listener_groups(spec: &Value) -> BTreeMap<String, Vec<&Value>> {
    let mut groups = BTreeMap::<String, Vec<&Value>>::new();
    for listener in child_arr(spec, "listeners") {
        groups
            .entry(str_field(listener, "kind").to_string())
            .or_default()
            .push(listener);
    }
    groups
}

pub(crate) fn listener_payloads(spec: &Value) -> Vec<String> {
    child_arr(spec, "listeners")
        .iter()
        .map(|listener| str_field(listener, "payload").to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(crate) fn message_build_catalog_entries(spec: &Value) -> &[Value] {
    spec.get("messageBuildCatalog")
        .and_then(|catalog| catalog.get("entries"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

pub(crate) fn message_builder_request_models(spec: &Value) -> BTreeMap<String, &Value> {
    let mut models = BTreeMap::new();
    for group in child_arr(spec, "modelGroups") {
        if str_field(group, "group") != "message_builder" {
            continue;
        }
        for model in child_arr(group, "models") {
            let name = str_field(model, "name");
            if name.starts_with("Build") && name.ends_with("MessageRequest") {
                models.insert(name.to_string(), model);
            }
        }
    }
    models
}

pub(crate) fn message_builder_extra_methods(spec: &Value) -> Vec<&Value> {
    let catalog_methods = message_build_catalog_entries(spec)
        .iter()
        .map(|entry| str_field(entry, "method").to_string())
        .collect::<BTreeSet<_>>();
    for module in child_arr(spec, "modules") {
        if str_field(module, "key") != "message_builder" {
            continue;
        }
        return child_arr(module, "methods")
            .iter()
            .filter(|method| {
                let name = str_field(method, "name");
                name != "listSupportedBuildOperations" && !catalog_methods.contains(name)
            })
            .collect();
    }
    Vec::new()
}

pub(crate) fn find_model<'a>(spec: &'a Value, name: &str) -> Option<&'a Value> {
    all_spec_models(spec)
        .into_iter()
        .find(|model| str_field(model, "name") == name)
}

pub(crate) fn is_known_ts_model_type(name: &str, spec: &Value) -> bool {
    spec_model_names(spec).contains(name) || spec_enum_names(spec).contains(name)
}

pub(crate) fn is_list_type_name(type_name: &str) -> bool {
    type_name.ends_with("List")
}

pub(crate) fn list_inner_type_name(type_name: &str) -> &str {
    type_name.strip_suffix("List").unwrap_or(type_name)
}
