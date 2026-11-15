//! The viewer's consent to a plugin using a host capability, the third of
//! the three allowlists a capability passes (manifest declaration ∩ host
//! profile ∩ consent, see [`super::native`]).
//!
//! The first time a plugin uses a capability that needs consent
//! ([`NativeCapability::needs_consent`]: everything but `clipboardWrite`,
//! `selectImage` and `selectFile`, which are the viewer's own gestures), the
//! host asks ([`super::PluginCapabilityHost::consent`]). The viewer allows it
//! once (this request only, nothing kept), always, or not at all. An
//! "always" grant is kept in the user's store as `(user, plugin, capability, manifestMajor)` with the
//! sensitivity level it was given for. A newer plugin major, or parameters
//! more sensitive than those granted (a fine location after a coarse one),
//! ask again; so does every use after a refusal or a revoke.
use flare_plugin_protocol::ui::{
    ClientPlugin, NativeCapability, NativeParams, UiLocationPrecision,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::native::{CapabilityConsentPrompt, ConsentDecision, PluginCapabilityHost};
use super::{CapabilityApi, PluginCallFuture};
use crate::domain::{PluginConsent, PluginConsentScope};
use crate::shared::error::{ErrorCode, FlareError, Result};

/// The compatibility class of a plugin version, the `manifestMajor` a
/// consent is kept under: the major for `1.0.0` and later (`"2"` for
/// `2.4.1`), the minor under 1.0 where every minor may break (`"0.9"` for
/// `0.9.3`). A version that does not read as one is its own class.
pub fn manifest_major(version: &str) -> String {
    let core = version
        .trim()
        .trim_start_matches('v')
        .split(['-', '+'])
        .next()
        .unwrap_or_default();
    let mut parts = core.split('.');
    let major = parts.next().and_then(|p| p.parse::<u64>().ok());
    let minor = parts.next().and_then(|p| p.parse::<u64>().ok());
    match (major, minor) {
        (Some(0), Some(minor)) => format!("0.{minor}"),
        (Some(major), _) => major.to_string(),
        _ => {
            let raw: String = version
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                .take(64)
                .collect();
            if raw.is_empty() {
                "unknown".to_owned()
            } else {
                format!("raw-{raw}")
            }
        }
    }
}

/// How sensitive a request is: a consent covers requests up to the level it
/// was given for. A fine location is above a coarse one; video above audio,
/// screen sharing above video.
pub(crate) fn consent_level(capability: NativeCapability, params: Option<&NativeParams>) -> u8 {
    match (capability, params) {
        (NativeCapability::LocationOnce, Some(NativeParams::LocationOnce(p))) => {
            match p.precision {
                UiLocationPrecision::Coarse => 0,
                UiLocationPrecision::Fine => 1,
            }
        }
        (NativeCapability::RtcSession, Some(NativeParams::RtcSession(p))) => p
            .media
            .iter()
            .map(|media| match media {
                flare_plugin_protocol::ui::UiRtcMedia::Audio => 0,
                flare_plugin_protocol::ui::UiRtcMedia::Video => 1,
                flare_plugin_protocol::ui::UiRtcMedia::Screen => 2,
            })
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

fn invalid() -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, "sdk.plugin.invalid_ui_request")
}

fn session_required() -> FlareError {
    FlareError::localized(ErrorCode::NotConnected, "sdk.capability.session_required")
}

/// `plugin_capability_consents` input.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConsentListRequest {
    #[serde(default)]
    plugin_id: Option<String>,
}

/// `plugin_capability_consent_revoke` input.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConsentRevokeRequest {
    plugin_id: String,
    #[serde(default)]
    capability: Option<NativeCapability>,
}

impl CapabilityApi {
    fn consent_scope(&self, user: &str) -> PluginConsentScope {
        PluginConsentScope {
            tenant_id: self.default_tenant_id.clone(),
            user_id: user.to_owned(),
        }
    }

    /// Whether the viewer lets `plugin` use `capability` with `params`: no
    /// question for a capability that needs no consent; otherwise a kept
    /// consent that covers the plugin's current major and the request's
    /// level, or the host's answer to the question: "once" lets this request
    /// through without keeping anything, "always" is kept, "deny" refuses.
    pub(crate) async fn ensure_consent(
        &self,
        host: &dyn PluginCapabilityHost,
        user: &str,
        plugin: &ClientPlugin,
        capability: NativeCapability,
        params: Option<&NativeParams>,
    ) -> Result<bool> {
        if !capability.needs_consent() {
            return Ok(true);
        }
        let scope = self.consent_scope(user);
        let major = manifest_major(&plugin.version);
        let level = consent_level(capability, params);
        let kept = self
            .plugin_consents
            .get(&scope, &plugin.plugin_id, capability.name())
            .await?;
        if kept.as_ref().is_some_and(|c| c.covers(&major, level)) {
            return Ok(true);
        }
        let decision = host
            .consent(CapabilityConsentPrompt {
                plugin_id: plugin.plugin_id.clone(),
                plugin_version: plugin.version.clone(),
                capability,
                params: params.cloned(),
            })
            .await;
        match decision {
            ConsentDecision::Deny => return Ok(false),
            ConsentDecision::Once => return Ok(true),
            ConsentDecision::Always => {}
        }
        // A consent under the same major keeps the higher level it covered.
        let level = kept
            .filter(|c| c.manifest_major == major)
            .map_or(level, |c| c.level.max(level));
        self.plugin_consents
            .grant(
                &scope,
                &PluginConsent {
                    plugin_id: plugin.plugin_id.clone(),
                    capability: capability.name().to_owned(),
                    manifest_major: major,
                    level,
                    granted_at_ms: crate::shared::util::time::now_millis().min(i64::MAX as u64)
                        as i64,
                },
            )
            .await?;
        Ok(true)
    }

    /// `plugin_capability_consents`: `{pluginId?}` → `{consents: [{pluginId,
    /// capability, manifestMajor, level, grantedAtMs}]}`, the consents the
    /// signed-in viewer gave on this device, of one plugin or of all, for a
    /// settings screen.
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn plugin_capability_consents(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.plugin_capability_consents_now(request))
    }
    async fn plugin_capability_consents_now(&self, request: Value) -> Result<Value> {
        let request: ConsentListRequest = if request.is_null() {
            ConsentListRequest { plugin_id: None }
        } else {
            crate::shared::util::json::from_value(request).map_err(|_| invalid())?
        };
        self.session_guard
            .run_with_user(|user| async move {
                let user = user.ok_or_else(session_required)?;
                let consents = self
                    .plugin_consents
                    .list(&self.consent_scope(&user), request.plugin_id.as_deref())
                    .await?;
                Ok(json!({ "consents": consents }))
            })
            .await
    }

    /// `plugin_capability_consent_revoke`: `{pluginId, capability?}` →
    /// `{revoked}`. Drops the viewer's consent to one capability of the
    /// plugin, or to all of them; the next use asks again.
    // Boxed so that its future is compiled once, here, and not again in
    // each crate that awaits it.
    #[inline(never)]
    pub fn revoke_plugin_capability_consent(&self, request: Value) -> PluginCallFuture<'_> {
        Box::pin(self.revoke_plugin_capability_consent_now(request))
    }
    async fn revoke_plugin_capability_consent_now(&self, request: Value) -> Result<Value> {
        let request: ConsentRevokeRequest =
            crate::shared::util::json::from_value(request).map_err(|_| invalid())?;
        self.session_guard
            .run_with_user(|user| async move {
                let user = user.ok_or_else(session_required)?;
                let revoked = self
                    .plugin_consents
                    .revoke(
                        &self.consent_scope(&user),
                        &request.plugin_id,
                        request.capability.map(NativeCapability::name),
                    )
                    .await?;
                Ok(json!({ "revoked": revoked }))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_plugin_protocol::ui::{LocationOnceParams, RtcSessionParams, UiRtcMedia};

    #[test]
    fn the_manifest_major_is_the_compatibility_class_of_the_version() {
        assert_eq!(manifest_major("2.4.1"), "2");
        assert_eq!(manifest_major("v10.0.0-rc.1"), "10");
        assert_eq!(manifest_major("0.9.3"), "0.9");
        assert_eq!(manifest_major("0.10.0+build.7"), "0.10");
        assert_eq!(manifest_major("1"), "1");
        assert_eq!(manifest_major("nightly"), "raw-nightly");
        assert_eq!(manifest_major("  "), "unknown");
    }

    #[test]
    fn a_fine_location_and_screen_sharing_are_more_sensitive() {
        let coarse = NativeParams::LocationOnce(LocationOnceParams {
            precision: UiLocationPrecision::Coarse,
        });
        let fine = NativeParams::LocationOnce(LocationOnceParams {
            precision: UiLocationPrecision::Fine,
        });
        assert_eq!(
            consent_level(NativeCapability::LocationOnce, Some(&coarse)),
            0
        );
        assert_eq!(
            consent_level(NativeCapability::LocationOnce, Some(&fine)),
            1
        );
        let screen = NativeParams::RtcSession(RtcSessionParams {
            media: vec![UiRtcMedia::Audio, UiRtcMedia::Screen],
        });
        assert_eq!(
            consent_level(NativeCapability::RtcSession, Some(&screen)),
            2
        );
        assert_eq!(consent_level(NativeCapability::DocumentCapture, None), 0);
    }
}
