//! Consents kept for the life of the process: the store of a client without a
//! user database, and of tests.
use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::Result;
use crate::domain::{
    PluginConsent, PluginConsentScope, PluginConsentStore, consent_error, valid_consent_key,
};

type Key = (String, String, String, String);

#[derive(Default)]
pub struct MemoryPluginConsentStore {
    consents: Mutex<BTreeMap<Key, PluginConsent>>,
}

impl MemoryPluginConsentStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(scope: &PluginConsentScope, plugin_id: &str, capability: &str) -> Key {
        (
            scope.tenant_id.clone(),
            scope.user_id.clone(),
            plugin_id.to_owned(),
            capability.to_owned(),
        )
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, BTreeMap<Key, PluginConsent>> {
        self.consents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl PluginConsentStore for MemoryPluginConsentStore {
    async fn get(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: &str,
    ) -> Result<Option<PluginConsent>> {
        scope.validate()?;
        Ok(self
            .locked()
            .get(&Self::key(scope, plugin_id, capability))
            .cloned())
    }

    async fn grant(&self, scope: &PluginConsentScope, consent: &PluginConsent) -> Result<()> {
        scope.validate()?;
        consent.validate()?;
        self.locked().insert(
            Self::key(scope, &consent.plugin_id, &consent.capability),
            consent.clone(),
        );
        Ok(())
    }

    async fn revoke(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: Option<&str>,
    ) -> Result<u64> {
        scope.validate()?;
        if !valid_consent_key(plugin_id) || capability.is_some_and(|c| !valid_consent_key(c)) {
            return Err(consent_error("invalid_consent"));
        }
        let mut consents = self.locked();
        let before = consents.len();
        consents.retain(|(tenant, user, plugin, cap), _| {
            !(tenant == &scope.tenant_id
                && user == &scope.user_id
                && plugin == plugin_id
                && capability.is_none_or(|c| c == cap))
        });
        Ok((before - consents.len()) as u64)
    }

    async fn list(
        &self,
        scope: &PluginConsentScope,
        plugin_id: Option<&str>,
    ) -> Result<Vec<PluginConsent>> {
        scope.validate()?;
        Ok(self
            .locked()
            .iter()
            .filter(|((tenant, user, plugin, _), _)| {
                tenant == &scope.tenant_id
                    && user == &scope.user_id
                    && plugin_id.is_none_or(|p| p == plugin)
            })
            .map(|(_, consent)| consent.clone())
            .collect())
    }
}
