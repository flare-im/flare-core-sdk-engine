//! 插件 manifest：SDK 内部用于注册校验、平台代码生成和市场目录的稳定契约。

use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SdkPluginManifest {
    pub id: String,
    pub version: String,
    pub display_name: String,
    pub namespaces: Vec<String>,
    #[schemars(length(min = 1, max = 256))]
    pub operations: Vec<SdkPluginOperationManifest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<SdkPluginEventManifest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permissions: Vec<SdkPluginPermissionManifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_sdk_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub platforms: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ui_kits: Vec<SdkPluginUiKitManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SdkPluginOperationManifest {
    pub op: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permissions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SdkPluginEventManifest {
    #[schemars(length(min = 1))]
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SdkPluginPermissionManifest {
    #[schemars(length(min = 1))]
    pub id: String,
    #[schemars(length(min = 1))]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SdkPluginUiKitManifest {
    pub platform: String,
    pub package: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
}

impl SdkPluginManifest {
    pub fn builtin(plugin_id: &str, namespaces: &[&str]) -> Self {
        Self {
            id: plugin_id.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            display_name: plugin_id.to_string(),
            namespaces: namespaces.iter().map(|ns| (*ns).to_string()).collect(),
            operations: Vec::new(),
            events: Vec::new(),
            permissions: Vec::new(),
            min_sdk_version: None,
            platforms: Vec::new(),
            ui_kits: Vec::new(),
        }
    }

    pub fn validate(&self, plugin_id: &str, plugin_namespaces: &[String]) -> Result<()> {
        if self.id.trim().is_empty() {
            return invalid_manifest(plugin_id, "empty_id");
        }
        if self.id != plugin_id {
            return invalid_manifest(plugin_id, "id_mismatch");
        }
        if self.version.trim().is_empty() {
            return invalid_manifest(plugin_id, "empty_version");
        }
        if self.namespaces.is_empty() {
            return invalid_manifest(plugin_id, "empty_namespaces");
        }
        if has_empty_or_duplicate(self.namespaces.iter().map(String::as_str)) {
            return invalid_manifest(plugin_id, "invalid_namespaces");
        }
        if !same_set(
            self.namespaces.iter().map(String::as_str),
            plugin_namespaces.iter().map(String::as_str),
        ) {
            return invalid_manifest(plugin_id, "namespace_mismatch");
        }
        if let Some(min_sdk_version) = self.min_sdk_version.as_deref()
            && !sdk_version_satisfies(env!("CARGO_PKG_VERSION"), min_sdk_version)
        {
            return invalid_manifest(plugin_id, "incompatible_min_sdk_version");
        }

        if has_empty_or_duplicate(
            self.permissions
                .iter()
                .map(|permission| permission.id.as_str()),
        ) {
            return invalid_manifest(plugin_id, "invalid_permissions");
        }
        for permission in &self.permissions {
            if permission.description.trim().is_empty() {
                return invalid_manifest(plugin_id, "permission_description_required");
            }
        }

        let permissions = self.permission_ids();
        if self.operations.is_empty() || self.operations.len() > 256 {
            return invalid_manifest(plugin_id, "operations_required");
        }
        let mut operations = HashSet::new();
        for operation in &self.operations {
            if operation.op.trim().is_empty() {
                return invalid_manifest(plugin_id, "empty_operation");
            }
            if !operations.insert(operation.op.as_str()) {
                return invalid_manifest(plugin_id, "duplicate_operation");
            }
            for permission in &operation.permissions {
                if !permissions.contains(permission.as_str()) {
                    return invalid_manifest(plugin_id, "undeclared_operation_permission");
                }
            }
        }
        let mut events = HashSet::new();
        for event in &self.events {
            if event.id.trim().is_empty() {
                return invalid_manifest(plugin_id, "empty_event");
            }
            if !events.insert(event.id.as_str()) {
                return invalid_manifest(plugin_id, "duplicate_event");
            }
            if !event.schema.is_object() || event.schema.get("type").is_none() {
                return invalid_manifest(plugin_id, "event_schema_required");
            }
        }
        Ok(())
    }

    pub fn owns_capability(&self, capability_id: &str) -> bool {
        if !self.namespaces.iter().any(|ns| {
            capability_id == ns || crate::shared::util::is_under_namespace(capability_id, ns)
        }) {
            return false;
        }
        self.operations
            .iter()
            .any(|operation| operation.matches_capability(capability_id, &self.namespaces))
    }

    fn permission_ids(&self) -> HashSet<&str> {
        self.permissions
            .iter()
            .map(SdkPluginPermissionManifest::id)
            .collect()
    }
}

impl SdkPluginOperationManifest {
    pub fn new(op: impl Into<String>) -> Self {
        Self {
            op: op.into(),
            display_name: None,
            description: None,
            permissions: Vec::new(),
            input_schema: None,
            output_schema: None,
        }
    }

    fn matches_capability(&self, capability_id: &str, namespaces: &[String]) -> bool {
        let op = self.op.as_str();
        if op == capability_id {
            return true;
        }
        namespaces.iter().any(|namespace| {
            capability_id
                .strip_prefix(namespace.as_str())
                .and_then(|suffix| suffix.strip_prefix('.'))
                .is_some_and(|suffix| suffix == op)
        })
    }
}

impl SdkPluginPermissionManifest {
    pub fn id(&self) -> &str {
        &self.id
    }
}

fn invalid_manifest<T>(plugin_id: &str, reason: &str) -> Result<T> {
    Err(FlareError::localized(
        ErrorCode::ConfigurationError,
        format!("sdk.capability.invalid_plugin_manifest:{plugin_id}:{reason}"),
    ))
}

fn has_empty_or_duplicate<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() || !seen.insert(value) {
            return true;
        }
    }
    false
}

fn same_set<'a>(left: impl Iterator<Item = &'a str>, right: impl Iterator<Item = &'a str>) -> bool {
    let left = left.collect::<HashSet<_>>();
    let right = right.collect::<HashSet<_>>();
    left == right
}

fn sdk_version_satisfies(current: &str, min_sdk_version: &str) -> bool {
    let Some(current) = parse_version(current) else {
        return false;
    };
    let Some(min_sdk_version) = parse_version(min_sdk_version) else {
        return false;
    };
    current >= min_sdk_version
}

fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    let patch = parts
        .next()
        .unwrap_or("0")
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or("0")
        .parse()
        .ok()?;
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_manifest() -> SdkPluginManifest {
        SdkPluginManifest {
            id: "sdk.plugin.test".to_string(),
            version: "0.2.0".to_string(),
            display_name: "Test Plugin".to_string(),
            namespaces: vec!["test".to_string()],
            operations: vec![SdkPluginOperationManifest::new("test.echo")],
            events: vec![SdkPluginEventManifest {
                id: "test.event.ready".to_string(),
                description: Some("Ready event.".to_string()),
                schema: json!({
                    "type": "object",
                    "required": ["id"],
                    "properties": {
                        "id": { "type": "string", "minLength": 1 }
                    },
                    "additionalProperties": false
                }),
            }],
            permissions: vec![SdkPluginPermissionManifest {
                id: "test.event".to_string(),
                description: "Allows test event dispatch.".to_string(),
            }],
            min_sdk_version: None,
            platforms: Vec::new(),
            ui_kits: Vec::new(),
        }
    }

    #[test]
    fn dotted_namespaces_match_complete_segments_and_declared_operations() {
        let mut manifest = valid_manifest();
        manifest.namespaces = vec!["flare.redpacket".into()];
        manifest.operations = vec![SdkPluginOperationManifest::new("create")];
        assert!(manifest.owns_capability("flare.redpacket.create"));
        for capability in [
            "flare.create",
            "flare.redpacketx.create",
            "other.redpacket.create",
            "flare.redpacket.claim",
        ] {
            assert!(!manifest.owns_capability(capability));
        }
    }

    #[test]
    fn plugin_manifest_requires_typed_event_schema() {
        let mut manifest = valid_manifest();
        manifest.events[0].schema = json!({});

        assert!(
            manifest
                .validate("sdk.plugin.test", &["test".to_string()])
                .is_err()
        );
    }

    #[test]
    fn plugin_manifest_rejects_duplicate_events() {
        let mut manifest = valid_manifest();
        manifest.events.push(manifest.events[0].clone());

        assert!(
            manifest
                .validate("sdk.plugin.test", &["test".to_string()])
                .is_err()
        );
    }

    #[test]
    fn plugin_manifest_requires_typed_permission_description() {
        let mut manifest = valid_manifest();
        manifest.permissions[0].description.clear();

        assert!(
            manifest
                .validate("sdk.plugin.test", &["test".to_string()])
                .is_err()
        );
    }
}
