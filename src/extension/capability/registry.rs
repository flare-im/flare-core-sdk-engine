//! [`SdkCapabilityRegistry`]：按 `capability_id` 前缀解析插件并派发。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::client::api::CapabilityDispatchResult;
use crate::client::api::session_guard::SessionGuard;
use crate::extension::capability::{SdkCapabilityPlugin, SdkPluginManifest};
use crate::kernel::CurrentUserIdStore;
use crate::shared::error::{ErrorCode, FlareError, Result};

struct RegisteredPlugin {
    plugin: Arc<dyn SdkCapabilityPlugin>,
    manifest: SdkPluginManifest,
    revoked: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct Catalog {
    plugins: HashMap<String, RegisteredPlugin>,
    namespaces: HashMap<String, String>,
}

impl Catalog {
    fn owner(&self, capability: &str) -> Option<&str> {
        self.namespaces
            .iter()
            .find(|(ns, _)| {
                capability == ns.as_str() || crate::shared::util::is_under_namespace(capability, ns)
            })
            .map(|(_, id)| id.as_str())
    }
}

#[derive(Clone)]
pub struct SdkCapabilityRegistry {
    session_guard: SessionGuard,
    catalog: Arc<RwLock<Catalog>>,
}

impl SdkCapabilityRegistry {
    pub fn new() -> Self {
        Self {
            session_guard: SessionGuard::disabled("capability registry"),
            catalog: Default::default(),
        }
    }
    pub(crate) fn new_session_bound(current_user_id: CurrentUserIdStore) -> Self {
        Self {
            session_guard: SessionGuard::new(current_user_id, "capability registry"),
            ..Self::new()
        }
    }
    pub(crate) fn for_session(mut self, watch: crate::client::im_client::SessionEndWatch) -> Self {
        self.session_guard.bind(watch);
        self
    }
    fn writable(&self) -> Result<std::sync::RwLockWriteGuard<'_, Catalog>> {
        let catalog = self.catalog.write().map_err(|_| registry_lock_failed())?;
        if self.session_guard.retired() {
            return Err(self.session_guard.session_changed_error());
        }
        Ok(catalog)
    }

    pub fn register(&self, plugin: Arc<dyn SdkCapabilityPlugin>) -> Result<()> {
        let plugin_id = plugin.plugin_id().to_owned();
        let namespaces = plugin_namespaces(plugin.as_ref());
        let manifest = plugin.manifest();
        manifest.validate(&plugin_id, &namespaces)?;
        let mut catalog = self.writable()?;
        if catalog.plugins.contains_key(&plugin_id) {
            return Err(FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.plugin_already_registered:{plugin_id}"),
            ));
        }
        for ns in &namespaces {
            if catalog.namespaces.keys().any(|key| {
                key == ns
                    || crate::shared::util::is_under_namespace(key, ns)
                    || crate::shared::util::is_under_namespace(ns, key)
            }) {
                return Err(FlareError::localized(
                    ErrorCode::OperationNotSupported,
                    format!("sdk.capability.namespace_already_registered:{ns}"),
                ));
            }
        }
        for ns in namespaces {
            catalog.namespaces.insert(ns, plugin_id.clone());
        }
        catalog.plugins.insert(
            plugin_id,
            RegisteredPlugin {
                plugin,
                manifest,
                revoked: tokio::sync::watch::channel(false).0,
            },
        );
        Ok(())
    }
    pub fn unregister(&self, plugin_id: &str) -> Result<()> {
        let mut catalog = self.writable()?;
        if let Some(entry) = catalog.plugins.remove(plugin_id) {
            entry.revoked.send_replace(true);
            catalog.namespaces.retain(|_, owner| owner != plugin_id);
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
        self.catalog
            .read()
            .ok()
            .and_then(|c| c.plugins.get(plugin_id).map(|e| !*e.revoked.borrow()))
            .unwrap_or(false)
    }
    pub fn manifest(&self, plugin_id: &str) -> Option<SdkPluginManifest> {
        self.catalog
            .read()
            .ok()?
            .plugins
            .get(plugin_id)
            .map(|entry| entry.manifest.clone())
    }
    pub fn list_manifests(&self) -> Vec<SdkPluginManifest> {
        self.catalog
            .read()
            .map(|catalog| {
                catalog
                    .plugins
                    .values()
                    .map(|e| e.manifest.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn get_plugin(&self, plugin_id: &str) -> Option<Arc<dyn SdkCapabilityPlugin>> {
        self.catalog
            .read()
            .ok()?
            .plugins
            .get(plugin_id)
            .map(|entry| entry.plugin.clone())
    }
    pub fn resolve_by_capability(
        &self,
        capability_id: &str,
    ) -> Option<Arc<dyn SdkCapabilityPlugin>> {
        let catalog = self.catalog.read().ok()?;
        catalog
            .plugins
            .get(catalog.owner(capability_id)?)
            .map(|entry| entry.plugin.clone())
    }
    pub async fn invoke(
        &self,
        capability_id: &str,
        payload: Value,
        conversation_id: Option<&str>,
        tenant_id: Option<&str>,
    ) -> Result<CapabilityDispatchResult> {
        self.session_guard
            .run(async {
                let (plugin, mut revoked) = {
                    let catalog = self.catalog.read().map_err(|_| registry_lock_failed())?;
                    let owner = catalog.owner(capability_id).ok_or_else(|| {
                        FlareError::localized(
                            ErrorCode::OperationNotSupported,
                            format!("sdk.capability.no_plugin_for:{capability_id}"),
                        )
                    })?;
                    let entry = catalog
                        .plugins
                        .get(owner)
                        .ok_or_else(registry_lock_failed)?;
                    if *entry.revoked.borrow() {
                        return Err(disabled(owner));
                    }
                    if !entry.manifest.owns_capability(capability_id) {
                        return Err(FlareError::localized(
                            ErrorCode::OperationNotSupported,
                            format!("sdk.capability.operation_not_declared:{capability_id}"),
                        ));
                    }
                    (entry.plugin.clone(), entry.revoked.subscribe())
                };
                use futures::FutureExt;
                let work = std::panic::AssertUnwindSafe(plugin.invoke(
                    capability_id,
                    payload,
                    conversation_id,
                    tenant_id,
                ))
                .catch_unwind();
                let outcome = tokio::select! {
                    biased;
                    _ = revoked.wait_for(|v| *v) => None,
                    result = work => Some(result),
                };
                match outcome {
                    Some(result) if !*revoked.borrow() => result.map_err(|_| {
                        FlareError::localized(
                            ErrorCode::InternalError,
                            "capability plugin panicked",
                        )
                    })?,
                    _ => Err(disabled(plugin.plugin_id())),
                }
            })
            .await
    }
    pub fn list_plugin_ids(&self) -> Vec<String> {
        self.catalog
            .read()
            .map(|c| c.plugins.keys().cloned().collect())
            .unwrap_or_default()
    }
    fn set_enabled(&self, plugin_id: &str, enabled: bool) -> Result<()> {
        let mut catalog = self.writable()?;
        let entry = catalog.plugins.get_mut(plugin_id).ok_or_else(|| {
            FlareError::localized(
                ErrorCode::OperationNotSupported,
                format!("sdk.capability.plugin_not_registered:{plugin_id}"),
            )
        })?;
        if enabled && *entry.revoked.borrow() {
            entry.revoked = tokio::sync::watch::channel(false).0;
        } else if !enabled {
            entry.revoked.send_replace(true);
        }
        Ok(())
    }
}

fn disabled(plugin_id: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::OperationNotSupported,
        format!("sdk.capability.plugin_disabled:{plugin_id}"),
    )
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
