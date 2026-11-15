//! The viewer's consent to a plugin using a host capability (camera for an
//! identity document, a location fix, a payment confirmation), kept on this
//! device in the user's store.
//!
//! One record per `(tenant, user, plugin, capability)`. It counts only for
//! the plugin's manifest major it was given under and up to the sensitivity
//! level it was given for; a newer major or a more sensitive request asks
//! again. Only grants are kept: a refusal is asked again the next time.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{ErrorCode, FlareError, Result};

/// Whose consents: the signed-in user of a tenant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginConsentScope {
    pub tenant_id: String,
    pub user_id: String,
}

impl PluginConsentScope {
    pub fn validate(&self) -> Result<()> {
        if !valid_consent_key(&self.tenant_id) || !valid_consent_key(&self.user_id) {
            return Err(consent_error("invalid_scope"));
        }
        Ok(())
    }
}

/// A consent the viewer gave.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginConsent {
    pub plugin_id: String,
    /// The capability's wire name (`documentCapture`, ...).
    pub capability: String,
    /// The compatibility class of the plugin version it was given under:
    /// `"2"` for 2.x, `"0.9"` for 0.9.x (see
    /// [`crate::client::api::manifest_major`]).
    pub manifest_major: String,
    /// The most sensitive parameters it covers (`0` for a capability without
    /// levels; `1` for a fine location against `0` for a coarse one).
    pub level: u8,
    /// Device clock, milliseconds.
    pub granted_at_ms: i64,
}

impl PluginConsent {
    pub fn validate(&self) -> Result<()> {
        if !valid_consent_key(&self.plugin_id)
            || !valid_consent_key(&self.capability)
            || !valid_consent_key(&self.manifest_major)
            || self.granted_at_ms < 0
        {
            return Err(consent_error("invalid_consent"));
        }
        Ok(())
    }

    /// Whether this consent covers a request of `level` from the plugin at
    /// `manifest_major`.
    pub fn covers(&self, manifest_major: &str, level: u8) -> bool {
        self.manifest_major == manifest_major && level <= self.level
    }
}

pub(crate) fn valid_consent_key(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

pub(crate) fn consent_error(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("sdk.plugin_consent.{reason}"),
    )
}

#[async_trait]
pub trait PluginConsentStore: Send + Sync {
    /// The consent kept for `plugin_id`'s `capability`, whatever its major.
    async fn get(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: &str,
    ) -> Result<Option<PluginConsent>>;
    /// Keep `consent`, replacing the one kept for the same plugin and
    /// capability.
    async fn grant(&self, scope: &PluginConsentScope, consent: &PluginConsent) -> Result<()>;
    /// Drop the consents of `plugin_id`: one capability, or all of them when
    /// `capability` is `None`. Returns how many were dropped.
    async fn revoke(
        &self,
        scope: &PluginConsentScope,
        plugin_id: &str,
        capability: Option<&str>,
    ) -> Result<u64>;
    /// The consents kept, of one plugin or of all, ordered by plugin then
    /// capability.
    async fn list(
        &self,
        scope: &PluginConsentScope,
        plugin_id: Option<&str>,
    ) -> Result<Vec<PluginConsent>>;
}
