//! [`SdkCapabilityRegistry`]：按 `capability_id` 前缀解析插件并派发。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::client::api::CapabilityDispatchResult;
use crate::client::api::session_guard::SessionGuard;
use crate::extension::capability::{SdkCapabilityPlugin, SdkPluginManifest};
use crate::kernel::CurrentUserIdStore;
use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Debug, Clone)]
struct PluginRuntimeState {
    manifest: SdkPluginManifest,
    enabled: bool,
}

pub struct SdkCapabilityRegistry {
    session_guard: SessionGuard,
    plugins: RwLock<HashMap<String, Arc<dyn SdkCapabilityPlugin>>>,
    namespace_index: RwLock<HashMap<String, String>>,
    plugin_states: RwLock<HashMap<String, PluginRuntimeState>>,
}

impl SdkCapabilityRegistry {
    pub fn new() -> Self {
        Self {
            session_guard: SessionGuard::disabled("capability registry"),
            plugins: RwLock::new(HashMap::new()),
            namespace_index: RwLock::new(HashMap::new()),
            plugin_states: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn new_session_bound(current_user_id: CurrentUserIdStore) -> Self {
        Self {
            session_guard: SessionGuard::new(current_user_id, "capability registry"),
            plugins: RwLock::new(HashMap::new()),
            namespace_index: RwLock::new(HashMap::new()),
            plugin_states: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, plugin: Arc<dyn SdkCapabilityPlugin>) -> Result<()> {
        let plugin_id = plugin.plugin_id().to_string();
        let namespaces = plugin_namespaces(plugin.as_ref());
        let manifest = plugin.manifest();
        manifest.validate(&plugin_id, &namespaces)?;

        let mut plugins = self.plugins.write().map_err(|_| registry_lock_failed())?;
        if plugins.contains_key(&plugin_id) {
            return Err(FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.plugin_already_registered:{plugin_id}"),
            ));
        }
        let mut index = self
            .namespace_index
            .write()
            .map_err(|_| registry_lock_failed())?;
        for ns in &namespaces {
            if index.keys().any(|existing| {
                existing == ns
                    || existing.starts_with(&format!("{ns}."))
                    || ns.starts_with(&format!("{existing}."))
            }) {
                return Err(FlareError::localized(
                    ErrorCode::OperationNotSupported,
                    format!("sdk.capability.namespace_already_registered:{ns}"),
                ));
            }
        }
        let mut states = self
            .plugin_states
            .write()
            .map_err(|_| registry_lock_failed())?;

        for ns in &namespaces {
            index.insert(ns.clone(), plugin_id.clone());
        }
        plugins.insert(plugin_id.clone(), plugin);
        states.insert(
            plugin_id,
            PluginRuntimeState {
                manifest,
                enabled: true,
            },
        );
        Ok(())
    }

    pub fn unregister(&self, plugin_id: &str) -> Result<()> {
        let mut plugins = self.plugins.write().map_err(|_| registry_lock_failed())?;
        if plugins.remove(plugin_id).is_none() {
            return Ok(());
        }
        let mut index = self
            .namespace_index
            .write()
            .map_err(|_| registry_lock_failed())?;
        index.retain(|_, v| v != plugin_id);
        if let Ok(mut states) = self.plugin_states.write() {
            states.remove(plugin_id);
        }
        Ok(())
    }

    pub fn enable(&self, plugin_id: &str) -> Result<()> {
        self.set_enabled(plugin_id, true)
    }

    pub fn disable(&self, plugin_id: &str) -> Result<()> {
        self.set_enabled(plugin_id, false)
    }

    pub fn is_enabled(&self, plugin_id: &str) -> bool {
        self.plugin_states
            .read()
            .ok()
            .and_then(|states| states.get(plugin_id).map(|state| state.enabled))
            .unwrap_or(false)
    }

    pub fn manifest(&self, plugin_id: &str) -> Option<SdkPluginManifest> {
        self.plugin_states
            .read()
            .ok()
            .and_then(|states| states.get(plugin_id).map(|state| state.manifest.clone()))
    }

    pub fn list_manifests(&self) -> Vec<SdkPluginManifest> {
        self.plugin_states
            .read()
            .map(|states| {
                states
                    .values()
                    .map(|state| state.manifest.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_plugin(&self, plugin_id: &str) -> Option<Arc<dyn SdkCapabilityPlugin>> {
        self.plugins
            .read()
            .ok()
            .and_then(|plugins| plugins.get(plugin_id).cloned())
    }

    pub fn resolve_by_capability(
        &self,
        capability_id: &str,
    ) -> Option<Arc<dyn SdkCapabilityPlugin>> {
        let plugin_id = self.namespace_index.read().ok().and_then(|index| {
            index
                .iter()
                .find(|(ns, _)| {
                    capability_id == ns.as_str() || capability_id.starts_with(&format!("{ns}."))
                })
                .map(|(_, id)| id.clone())
        })?;
        self.get_plugin(plugin_id.as_str())
    }

    /// 按完整命名空间解析插件并 `invoke`（推荐宿主调用扩展能力的主入口）。
    pub async fn invoke(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        self.session_guard.ensure_active().await?;
        let Some(plugin) = self.resolve_by_capability(capability_id) else {
            return Err(FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.no_plugin_for:{capability_id}"),
            ));
        };
        self.ensure_plugin_can_invoke(plugin.plugin_id(), capability_id)?;
        plugin
            .invoke(capability_id, payload, conversation_id, tenant_id)
            .await
    }

    pub fn list_plugin_ids(&self) -> Vec<String> {
        self.plugins
            .read()
            .map(|plugins| plugins.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn set_enabled(&self, plugin_id: &str, enabled: bool) -> Result<()> {
        let mut states = self
            .plugin_states
            .write()
            .map_err(|_| registry_lock_failed())?;
        let Some(state) = states.get_mut(plugin_id) else {
            return Err(FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.plugin_not_registered:{plugin_id}"),
            ));
        };
        state.enabled = enabled;
        Ok(())
    }

    fn ensure_plugin_can_invoke(&self, plugin_id: &str, capability_id: &str) -> Result<()> {
        let state = self
            .plugin_states
            .read()
            .map_err(|_| registry_lock_failed())?
            .get(plugin_id)
            .cloned()
            .ok_or_else(|| {
                FlareError::localized(
                    ErrorCode::OperationNotSupported,
                    format!("sdk.capability.plugin_not_registered:{plugin_id}"),
                )
            })?;
        if !state.enabled {
            return Err(FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.plugin_disabled:{plugin_id}"),
            ));
        }
        if !state.manifest.owns_capability(capability_id) {
            return Err(FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.operation_not_declared:{capability_id}"),
            ));
        }
        Ok(())
    }
}

impl Default for SdkCapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn plugin_namespaces(plugin: &dyn SdkCapabilityPlugin) -> Vec<String> {
    plugin
        .capability_namespaces()
        .iter()
        .map(|ns| (*ns).to_string())
        .collect()
}

fn registry_lock_failed() -> FlareError {
    FlareError::localized(
        ErrorCode::InternalError,
        "sdk.capability.registry_lock_failed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extension::capability::{
        SdkPluginManifest, SdkPluginOperationManifest, SdkPluginPermissionManifest,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::RwLock;

    struct CountingPlugin {
        invoke_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SdkCapabilityPlugin for CountingPlugin {
        fn plugin_id(&self) -> &'static str {
            "test.plugin"
        }

        fn capability_namespaces(&self) -> &'static [&'static str] {
            &["test"]
        }

        fn manifest(&self) -> SdkPluginManifest {
            let mut manifest =
                SdkPluginManifest::builtin(self.plugin_id(), self.capability_namespaces());
            manifest.operations = vec![SdkPluginOperationManifest::new("test.echo")];
            manifest
        }

        async fn invoke(
            &self,
            capability_id: &str,
            _payload: Value,
            _conversation_id: Option<&str>,
            _tenant_id: Option<&str>,
        ) -> Result<CapabilityDispatchResult> {
            self.invoke_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CapabilityDispatchResult {
                request_id: String::new(),
                success: true,
                plugin_id: self.plugin_id().to_string(),
                capability_id: capability_id.to_string(),
                data: Value::Null,
                error: None,
            })
        }
    }

    struct ManifestPlugin {
        manifest: SdkPluginManifest,
        invoke_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SdkCapabilityPlugin for ManifestPlugin {
        fn plugin_id(&self) -> &'static str {
            "manifest.plugin"
        }

        fn capability_namespaces(&self) -> &'static [&'static str] {
            &["manifest"]
        }

        fn manifest(&self) -> SdkPluginManifest {
            self.manifest.clone()
        }

        async fn invoke(
            &self,
            capability_id: &str,
            _payload: Value,
            _conversation_id: Option<&str>,
            _tenant_id: Option<&str>,
        ) -> Result<CapabilityDispatchResult> {
            self.invoke_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CapabilityDispatchResult {
                request_id: String::new(),
                success: true,
                plugin_id: self.plugin_id().to_string(),
                capability_id: capability_id.to_string(),
                data: Value::Null,
                error: None,
            })
        }
    }

    #[tokio::test]
    async fn session_bound_registry_rejects_plugin_invocation_after_logout() {
        let current_user_id = Arc::new(RwLock::new("alice".to_string()));
        let registry = SdkCapabilityRegistry::new_session_bound(current_user_id.clone());
        let invoke_calls = Arc::new(AtomicUsize::new(0));
        registry
            .register(Arc::new(CountingPlugin {
                invoke_calls: invoke_calls.clone(),
            }))
            .expect("register test plugin");

        *current_user_id.write().await = String::new();

        let err = registry
            .invoke("test.echo", Value::Null, None, None)
            .await
            .expect_err("logged-out registry invoke must fail");
        assert_eq!(err.code(), Some(ErrorCode::NotConnected));
        assert_eq!(invoke_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn registry_disable_blocks_plugin_invocation() {
        let registry = SdkCapabilityRegistry::new();
        let invoke_calls = Arc::new(AtomicUsize::new(0));
        registry
            .register(Arc::new(CountingPlugin {
                invoke_calls: invoke_calls.clone(),
            }))
            .expect("register test plugin");

        registry.disable("test.plugin").expect("disable plugin");
        assert!(!registry.is_enabled("test.plugin"));

        let err = registry
            .invoke("test.echo", Value::Null, None, None)
            .await
            .expect_err("disabled plugin must reject invoke");
        assert_eq!(err.code(), Some(ErrorCode::OperationNotSupported));
        assert_eq!(invoke_calls.load(Ordering::SeqCst), 0);

        registry.enable("test.plugin").expect("enable plugin");
        registry
            .invoke("test.echo", Value::Null, None, None)
            .await
            .expect("enabled plugin accepts invoke");
        assert_eq!(invoke_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn register_rejects_manifest_namespace_mismatch() {
        let registry = SdkCapabilityRegistry::new();
        let mut manifest = SdkPluginManifest::builtin("manifest.plugin", &["other"]);
        manifest.display_name = "Mismatch".to_string();

        let err = registry
            .register(Arc::new(ManifestPlugin {
                manifest,
                invoke_calls: Arc::new(AtomicUsize::new(0)),
            }))
            .expect_err("namespace mismatch must reject registration");
        assert_eq!(err.code(), Some(ErrorCode::ConfigurationError));
    }

    #[test]
    fn empty_operations_do_not_grant_the_entire_namespace() {
        let registry = SdkCapabilityRegistry::new();
        let manifest = SdkPluginManifest::builtin("manifest.plugin", &["manifest"]);
        assert!(!manifest.owns_capability("manifest.anything"));
        assert!(
            registry
                .register(Arc::new(ManifestPlugin {
                    manifest,
                    invoke_calls: Arc::new(AtomicUsize::new(0)),
                }))
                .is_err()
        );
        assert!(
            registry
                .resolve_by_capability("manifest.anything")
                .is_none()
        );
    }

    #[test]
    fn register_rejects_incompatible_min_sdk_version() {
        let registry = SdkCapabilityRegistry::new();
        let mut manifest = SdkPluginManifest::builtin("manifest.plugin", &["manifest"]);
        manifest.min_sdk_version = Some("999.0.0".to_string());

        let err = registry
            .register(Arc::new(ManifestPlugin {
                manifest,
                invoke_calls: Arc::new(AtomicUsize::new(0)),
            }))
            .expect_err("future min sdk version must reject registration");
        assert_eq!(err.code(), Some(ErrorCode::ConfigurationError));
    }

    #[tokio::test]
    async fn manifest_operations_gate_dispatch() {
        let registry = SdkCapabilityRegistry::new();
        let mut manifest = SdkPluginManifest::builtin("manifest.plugin", &["manifest"]);
        manifest.permissions = vec![SdkPluginPermissionManifest {
            id: "call".to_string(),
            description: "Allows call dispatch.".to_string(),
        }];
        let mut operation = SdkPluginOperationManifest::new("echo");
        operation.permissions = vec!["call".to_string()];
        manifest.operations = vec![operation];
        let invoke_calls = Arc::new(AtomicUsize::new(0));
        registry
            .register(Arc::new(ManifestPlugin {
                manifest,
                invoke_calls: invoke_calls.clone(),
            }))
            .expect("register manifest plugin");

        registry
            .invoke("manifest.echo", Value::Null, None, None)
            .await
            .expect("declared operation accepts invoke");
        let err = registry
            .invoke("manifest.other", Value::Null, None, None)
            .await
            .expect_err("undeclared operation rejects invoke");
        assert_eq!(err.code(), Some(ErrorCode::OperationNotSupported));
        assert_eq!(invoke_calls.load(Ordering::SeqCst), 1);
    }
}
