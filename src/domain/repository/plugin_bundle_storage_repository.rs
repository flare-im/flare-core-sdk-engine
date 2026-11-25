//! What a plugin's web bundle keeps on this device through the bridge's
//! `storage.*` methods, in the user's store, apart from every other table.
//!
//! One namespace per `(tenant, user, plugin)`: every bundle of a plugin shares
//! it, no plugin reads another's, and signing out removes the user's. Keys are
//! `[A-Za-z0-9_.:/-]{1,128}`; a value is any JSON but `null`, at most
//! [`MAX_BRIDGE_STORAGE_VALUE_BYTES`] encoded, and a namespace holds at most
//! [`MAX_BRIDGE_STORAGE_BYTES`] of keys and encoded values together.
use async_trait::async_trait;
use flare_plugin_protocol::bridge::{
    MAX_BRIDGE_STORAGE_BYTES, MAX_BRIDGE_STORAGE_KEY_BYTES, MAX_BRIDGE_STORAGE_VALUE_BYTES,
};
use serde_json::Value;

use crate::{ErrorCode, FlareError, Result};

/// Whose namespace: a plugin of the signed-in user of a tenant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginBundleStorageScope {
    pub tenant_id: String,
    pub user_id: String,
    pub plugin_id: String,
}

impl PluginBundleStorageScope {
    pub fn validate(&self) -> Result<()> {
        if [&self.tenant_id, &self.user_id, &self.plugin_id]
            .iter()
            .any(|s| !super::plugin_consent_repository::valid_consent_key(s))
        {
            return Err(bundle_storage_error("invalid_scope"));
        }
        Ok(())
    }
}

/// A storage key a bundle may use.
pub fn valid_bundle_storage_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_BRIDGE_STORAGE_KEY_BYTES
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'/' | b'-'))
}

/// The bytes `key` and `value` count against the namespace: the key and the
/// value's compact JSON encoding.
pub fn bundle_storage_cost(key: &str, value: &Value) -> Result<usize> {
    let encoded = serde_json::to_vec(value).map_err(|_| bundle_storage_error("invalid_value"))?;
    if value.is_null() || encoded.len() > MAX_BRIDGE_STORAGE_VALUE_BYTES {
        return Err(bundle_storage_error("invalid_value"));
    }
    Ok(key.len() + encoded.len())
}

/// Whether a namespace holding `used` bytes, of which `replaced` belong to the
/// key being written, can take a write of `cost`.
pub fn bundle_storage_fits(used: usize, replaced: usize, cost: usize) -> bool {
    used.saturating_sub(replaced).saturating_add(cost) <= MAX_BRIDGE_STORAGE_BYTES
}

pub fn bundle_storage_error(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("sdk.plugin_bundle_storage.{reason}"),
    )
}

/// The namespace would grow past [`MAX_BRIDGE_STORAGE_BYTES`]: the bridge
/// answers `quota_exceeded`.
pub fn bundle_storage_quota_exceeded() -> FlareError {
    FlareError::localized(
        ErrorCode::ResourceExhausted,
        "sdk.plugin_bundle_storage.quota_exceeded",
    )
}

pub fn is_bundle_storage_quota_error(error: &FlareError) -> bool {
    error.code() == Some(ErrorCode::ResourceExhausted)
}

#[async_trait]
pub trait PluginBundleStorage: Send + Sync {
    async fn get(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<Option<Value>>;
    /// Keep `value` at `key`, replacing what is there; refused with
    /// [`bundle_storage_quota_exceeded`] when the namespace would grow past
    /// its quota (the check and the write are one step).
    async fn set(&self, scope: &PluginBundleStorageScope, key: &str, value: &Value) -> Result<()>;
    /// Whether something was removed.
    async fn remove(&self, scope: &PluginBundleStorageScope, key: &str) -> Result<bool>;
    /// The keys, sorted, that start with `prefix`.
    async fn keys(
        &self,
        scope: &PluginBundleStorageScope,
        prefix: Option<&str>,
    ) -> Result<Vec<String>>;
    /// Remove everything the user's bundles kept (signing out).
    async fn clear_user(&self, tenant_id: &str, user_id: &str) -> Result<u64>;
}
