//! What a plugin view that needs a newer client (`upgrade_required`) offers:
//! whether this channel already has a build that shows it, and the update
//! center's own flow to install it.
//!
//! Both read the update center ([`crate::runtime::update::UpdateRuntime`]),
//! which lives as long as `init`, so they work before login too.
use flare_access_contract::update::UpdateStatus;
use serde::Serialize;

use crate::client::IMClient;
use crate::shared::error::Result;

/// `update.plugin_upgrade_status` result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginUpgradeStatus {
    /// This device is offered a build of at least `minBuild`: show "Update"
    /// ([`IMClient::open_update_flow`]). Otherwise only the text that a
    /// newer version will show it (a release still rolling out to others
    /// counts as none, so nothing about the rollout shows).
    pub available: bool,
    /// The build offered, when `available`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build: Option<u64>,
}

impl PluginUpgradeStatus {
    fn of(build: Option<u64>) -> Self {
        Self {
            available: build.is_some(),
            build,
        }
    }
}

impl IMClient {
    /// `update.plugin_upgrade_status`: `{minBuild?}` (a view's `minBuild`,
    /// absent when the plugin names none). Whether this channel has a build
    /// of at least `minBuild` this device is offered, evaluated like the
    /// update prompt on the release manifest at hand; no network request.
    /// Without `minBuild` any newer build counts. Without the update center
    /// (no access configuration) nothing is available.
    pub async fn plugin_upgrade_status(
        &self,
        min_build: Option<u64>,
    ) -> Result<PluginUpgradeStatus> {
        let Some(update) = self.update_runtime().await else {
            return Ok(PluginUpgradeStatus::default());
        };
        let floor = match (min_build, update.clone().state(false).await.current.build) {
            (Some(floor), _) => floor,
            // Any build newer than this one.
            (None, Some(current)) => current.saturating_add(1),
            (None, None) => 1,
        };
        Ok(PluginUpgradeStatus::of(
            update.release_at_least(floor).await,
        ))
    }

    /// `update.open_flow`: the update center's own update prompt, opened now
    /// because the user asked (a plugin's "Update"), outside the prompt
    /// frequency rules. The flow then runs as from the prompt (download,
    /// install, defer). Returns the update state; its `prompt` is `none` when
    /// there is nothing to update to.
    pub async fn open_update_flow(&self) -> Result<UpdateStatus> {
        let update = self.require_update().await?;
        Ok(update.open_flow().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_status_names_a_build_only_when_one_is_available() {
        assert_eq!(
            serde_json::to_value(PluginUpgradeStatus::of(Some(41_020))).unwrap(),
            serde_json::json!({"available": true, "build": 41_020})
        );
        assert_eq!(
            serde_json::to_value(PluginUpgradeStatus::of(None)).unwrap(),
            serde_json::json!({"available": false})
        );
    }
}
