use std::sync::Arc;

use crate::extension::{ExtensionContext, ExtensionRuntime, LifecycleScope};
use crate::shared::error::Result;

use super::IMClient;

impl IMClient {
    pub(super) async fn extension_lifecycle_snapshot(
        &self,
        current_user_id: Option<String>,
    ) -> Option<(Arc<ExtensionRuntime>, LifecycleScope)> {
        let g = self.inner.read().await;
        let runtime = g.extension_runtime.clone()?;
        if runtime.lifecycle_count() == 0 {
            return None;
        }
        let engine = g.engine.as_ref()?;
        let current_user_id = current_user_id.or_else(|| g.current_user_id.clone());
        Some((
            runtime,
            LifecycleScope {
                stores: engine.stores().clone(),
                bus: engine.bus().clone(),
                current_user_id,
            },
        ))
    }

    pub(super) async fn run_extension_migrations_best_effort(&self) {
        let snapshot = {
            let g = self.inner.read().await;
            match (g.extension_runtime.clone(), g.engine.as_ref()) {
                (Some(runtime), Some(engine)) if runtime.migration_count() > 0 => {
                    Some((runtime, engine.stores().clone(), engine.bus().clone()))
                }
                _ => None,
            }
        };
        if let Some((runtime, stores, bus)) = snapshot {
            runtime.run_migrations(&stores, &bus).await;
        }
    }

    pub(super) async fn notify_extension_login_best_effort(&self, user_id: &str) {
        if let Some((runtime, context)) = self
            .extension_lifecycle_snapshot(Some(user_id.to_string()))
            .await
            && let Err(err) = runtime.notify_login(&context).await
        {
            tracing::warn!(
                target = "flare_sdk.extension",
                user_id = user_id,
                error = %err,
                "SDK extension login lifecycle failed"
            );
        }
    }

    pub(super) async fn notify_extension_connect_best_effort(&self, user_id: &str) {
        if let Some((runtime, context)) = self
            .extension_lifecycle_snapshot(Some(user_id.to_string()))
            .await
            && let Err(err) = runtime.notify_connect(&context).await
        {
            tracing::warn!(
                target = "flare_sdk.extension",
                user_id = user_id,
                error = %err,
                "SDK extension connect lifecycle failed"
            );
        }
    }

    pub(super) async fn notify_extension_disconnect_snapshot(
        lifecycle: &Option<(Arc<ExtensionRuntime>, LifecycleScope)>,
    ) {
        if let Some((runtime, context)) = lifecycle
            && let Err(err) = runtime.notify_disconnect(context).await
        {
            tracing::warn!(
                target = "flare_sdk.extension",
                user_id = context.current_user_id().unwrap_or_default(),
                error = %err,
                "SDK extension disconnect lifecycle failed"
            );
        }
    }

    pub(super) async fn notify_extension_logout_snapshot(
        lifecycle: &Option<(Arc<ExtensionRuntime>, LifecycleScope)>,
    ) {
        if let Some((runtime, context)) = lifecycle
            && let Err(err) = runtime.notify_logout(context).await
        {
            tracing::warn!(
                target = "flare_sdk.extension",
                user_id = context.current_user_id().unwrap_or_default(),
                error = %err,
                "SDK extension logout lifecycle failed"
            );
        }
    }

    /// A slow extension can recover from its checkpoint without stalling apply or ACK.
    pub async fn subscribe_committed_inbound(
        &self,
        namespace: &str,
    ) -> Result<crate::spi::CommitSubscription> {
        let g = self.inner.read().await;
        let engine = g
            .engine
            .as_ref()
            .filter(|_| g.current_user_id.is_some())
            .ok_or_else(Self::not_connected)?;
        let grant = g
            .extension_runtime
            .as_ref()
            .ok_or_else(Self::not_connected)?
            .projection_grant(namespace)?;
        Ok(crate::spi::CommitSubscription::new(
            engine.bus().committed_receiver(),
            self.session_end_watch(g.session_generation),
            grant.revoked,
        ))
    }

    /// The composition root hands only the resulting handle to an extension.
    pub async fn open_extension_partition(
        &self,
        namespace: &str,
        partition: String,
        schema: crate::spi::RecordSchema,
        access: crate::spi::RecordAccess,
        backend: Arc<dyn crate::spi::RecordBackend>,
    ) -> Result<crate::spi::ExtensionPartition> {
        let g = self.inner.read().await;
        let user = g.current_user_id.clone().ok_or_else(Self::not_connected)?;
        let runtime = g
            .extension_runtime
            .as_ref()
            .ok_or_else(Self::not_connected)?;
        let grant = runtime.projection_grant(namespace)?;
        let scope = crate::spi::RecordScope {
            tenant: Self::resolve_tenant_id(&g),
            user,
            namespace: namespace.to_owned(),
            partition,
        };
        let session = self.session_end_watch(g.session_generation);
        drop(g);
        crate::spi::ExtensionPartition::open(backend, scope, schema, session, access, grant).await
    }

    /// Uninstall an extension's records for the current account. Its grant is revoked
    /// first and admitted writes drain, so no handle can write while rows are removed.
    pub async fn purge_extension_records(
        &self,
        namespace: &str,
        backend: Arc<dyn crate::spi::RecordBackend>,
    ) -> Result<()> {
        let (runtime, scope) = {
            let g = self.inner.read().await;
            let user = g.current_user_id.clone().ok_or_else(Self::not_connected)?;
            let runtime = g
                .extension_runtime
                .clone()
                .ok_or_else(Self::not_connected)?;
            let scope = crate::spi::RecordScope {
                tenant: Self::resolve_tenant_id(&g),
                user,
                namespace: namespace.to_owned(),
                partition: String::new(),
            };
            (runtime, scope)
        };
        runtime.set_projection_enabled(namespace, false).await?;
        backend.purge_namespace(&scope).await
    }

    /// 获取扩展运行时快照。
    pub fn extension_runtime(&self) -> Result<Arc<ExtensionRuntime>> {
        self.read_inner()?
            .extension_runtime
            .clone()
            .ok_or_else(Self::not_connected)
    }

    /// 按扩展 namespace 授权的上下文：停用后写入被拒，停用会等已放行的写入完成。
    pub async fn extension_context_for(&self, namespace: &str) -> Result<ExtensionContext> {
        let g = self.inner.read().await;
        let engine = g
            .engine
            .as_ref()
            .filter(|_| g.current_user_id.is_some())
            .ok_or_else(Self::not_connected)?;
        let grant = g
            .extension_runtime
            .as_ref()
            .ok_or_else(Self::not_connected)?
            .projection_grant(namespace)?;
        Ok(ExtensionContext::from_core_granted(
            engine.stores().clone(),
            engine.bus().clone(),
            Some(grant),
        ))
    }
}
