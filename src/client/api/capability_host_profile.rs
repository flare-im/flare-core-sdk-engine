//! The host's plugin UI profile: the design-kit vocabulary that renders
//! plugin views, the native capabilities the app allows, and the web bundle
//! runtime it offers.
//!
//! The engine reads every catalog with the vocabulary made of it
//! ([`PluginClient::vocabulary`]), sends Core the descriptor made of it with
//! every catalog read ([`PluginClient::descriptor`]), and takes the native
//! allowlist of every action from it. Hosts never pass an allowlist with a
//! request.
//!
//! The profile belongs to the host, not to a sign-in: it is kept in a slot
//! the client hands every session's [`super::CapabilityApi`], so a host sets
//! it once after `init`, before or after login.
use std::collections::BTreeSet;
use std::sync::{PoisonError, RwLock};

use flare_plugin_protocol::manifest::ClientPlatform;
use flare_plugin_protocol::ui::{
    BundleRuntime, ClientUiDescriptor, NativeCapability, UiVocabulary, base_vocabulary,
    engine_vocabulary,
};
use serde::Deserialize;
use serde_json::Value;

use crate::client::IMClient;
use crate::shared::error::{ErrorCode, FlareError, Result};

/// `plugin.set_host_profile` input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PluginHostProfile {
    /// The design kit rendering plugin views (`flutter` on a Flutter app,
    /// whatever the device): Core keys `minClient` floors by it.
    pub platform: ClientPlatform,
    /// The kit's `PLUGIN_UI_VOCABULARY`.
    pub kit_vocabulary: UiVocabulary,
    /// Native capabilities the app lets plugins ask for. Names this engine
    /// does not know are ignored: it could not run them anyway.
    #[serde(default)]
    pub native_allowlist: BTreeSet<String>,
    /// The web bundle runtime, when the host has one.
    #[serde(default)]
    pub bundle_runtime: Option<BundleRuntime>,
}

/// The application identity the descriptor carries: the build and
/// distribution the update center already reports
/// ([`crate::client::config::SdkConfig`] `app`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AppIdentity {
    pub build: Option<u64>,
    pub distribution: Option<String>,
}

/// Where the host's profile lives across sessions.
#[derive(Default)]
pub(crate) struct PluginHostSlot {
    profile: RwLock<Option<PluginHostProfile>>,
    /// The host's part of plugin capabilities: requests waiting for
    /// `plugin.capability_complete`, and an embedder's own host.
    pub capability_host: super::native::CapabilityHostSlot,
    /// Open web bundle pages and the bundle cache.
    pub bundles: crate::extension::capability::bundle::BundleSlot,
    /// The private files plugins uploaded through this engine.
    pub uploads: super::file_upload::PluginUploads,
    file_http: RwLock<Option<crate::infrastructure::transport::HttpClient>>,
}

impl PluginHostSlot {
    pub fn set_file_http(&self, http: Option<crate::infrastructure::transport::HttpClient>) {
        *self
            .file_http
            .write()
            .unwrap_or_else(PoisonError::into_inner) =
            http.map(|http| http.without_request_context());
    }

    pub fn file_http(&self) -> Option<crate::infrastructure::transport::HttpClient> {
        self.file_http
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Keep `profile`; whether it differs from the one kept before.
    pub fn set(&self, profile: PluginHostProfile) -> bool {
        let mut slot = self.profile.write().unwrap_or_else(PoisonError::into_inner);
        if slot.as_ref() == Some(&profile) {
            return false;
        }
        *slot = Some(profile);
        true
    }

    pub fn profile(&self) -> Option<PluginHostProfile> {
        self.profile
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// What the engine reads catalogs with and tells Core, from one profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginClient {
    /// Engine ∩ kit, with `native` also ∩ the allowlist.
    pub vocabulary: UiVocabulary,
    /// [`Self::vocabulary`]'s `native` as capabilities: what actions may target.
    pub native: BTreeSet<NativeCapability>,
    pub descriptor: ClientUiDescriptor,
}

impl PluginClient {
    /// The client of `profile`. Without one the host is taken to render
    /// vocabulary version 1 ([`base_vocabulary`]), what every kit rendered
    /// before profiles existed, with no native capability and no web bundle
    /// runtime: nothing the host has not declared is reported.
    pub fn of(
        profile: Option<&PluginHostProfile>,
        identity: &AppIdentity,
        bundle_delivery: bool,
    ) -> Self {
        let engine = engine_vocabulary();
        let (platform, vocabulary, bundle) = match profile {
            Some(profile) => {
                let allowlist = NativeCapability::ALL
                    .iter()
                    .copied()
                    .filter(|c| profile.native_allowlist.contains(c.name()))
                    .collect();
                (
                    profile.platform,
                    engine.for_host(Some(&profile.kit_vocabulary), &allowlist),
                    if bundle_delivery {
                        profile.bundle_runtime.clone()
                    } else {
                        None
                    },
                )
            }
            None => (
                device_platform(),
                engine.for_host(Some(&base_vocabulary()), &BTreeSet::new()),
                None,
            ),
        };
        let native = NativeCapability::ALL
            .iter()
            .copied()
            .filter(|c| vocabulary.native.contains(c.name()))
            .collect();
        let descriptor = ClientUiDescriptor::new(
            &vocabulary,
            platform,
            identity.build.unwrap_or(0).min(i64::MAX as u64),
            distribution(identity.distribution.as_deref()),
            bundle,
        );
        Self {
            vocabulary,
            native,
            descriptor,
        }
    }
}

/// The plugin platform of this device when the host has not said which kit
/// renders: a browser is `web`, a desktop build `tauri`.
fn device_platform() -> ClientPlatform {
    if cfg!(target_arch = "wasm32") {
        return ClientPlatform::Web;
    }
    if cfg!(target_env = "ohos") {
        return ClientPlatform::Harmonyos;
    }
    match std::env::consts::OS {
        "ios" => ClientPlatform::Ios,
        "android" => ClientPlatform::Android,
        _ => ClientPlatform::Tauri,
    }
}

/// The distribution as a descriptor channel (`[a-z0-9._-]{1,32}`): lowercased,
/// other characters left out, `unknown` when nothing is left.
fn distribution(configured: Option<&str>) -> String {
    let channel: String = configured
        .unwrap_or_default()
        .chars()
        .map(|c| c.to_ascii_lowercase())
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
        .take(32)
        .collect();
    if channel.is_empty() {
        "unknown".to_owned()
    } else {
        channel
    }
}

fn invalid_profile() -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, "sdk.plugin.invalid_ui_request")
}

/// Read and check a `plugin.set_host_profile` input: a valid kit vocabulary
/// and a descriptor Core accepts ([`ClientUiDescriptor::validate`]).
pub(crate) fn parse_profile(request: Value, identity: &AppIdentity) -> Result<PluginHostProfile> {
    let profile: PluginHostProfile =
        crate::shared::util::json::from_value(request).map_err(|_| invalid_profile())?;
    profile
        .kit_vocabulary
        .validate()
        .map_err(|_| invalid_profile())?;
    let declared = PluginClient::of(Some(&profile), identity, true).descriptor;
    declared.validate().map_err(|_| invalid_profile())?;
    Ok(profile)
}

impl IMClient {
    /// `plugin.set_host_profile`: `{platform, kitVocabulary, nativeAllowlist?,
    /// bundleRuntime?}`. Declares what the host renders plugin views with:
    /// the kit's `PLUGIN_UI_VOCABULARY`, the native capabilities the app
    /// allows and its web bundle runtime. Call it after `init`, before or
    /// after login; it lasts until the next call. A different profile drops
    /// the catalog read in flight and announces the cards presented lately
    /// again, so hosts present them against what the new profile selects.
    /// Until a host calls it, plugin views are read with vocabulary version 1
    /// and no native capability.
    pub async fn set_plugin_host_profile(&self, request: Value) -> Result<()> {
        let (slot, identity, capability) = {
            let inner = self.inner.read().await;
            let config = inner.configured_config.clone().unwrap_or_default();
            (
                inner.plugin_host.clone(),
                AppIdentity {
                    build: config.app.build,
                    distribution: config.effective_distribution(),
                },
                inner.capability_api.clone(),
            )
        };
        let profile = parse_profile(request, &identity)?;
        if slot.set(profile)
            && let Some(capability) = capability
        {
            capability.invalidate_plugin_catalog().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn identity() -> AppIdentity {
        AppIdentity {
            build: Some(41_020),
            distribution: Some("Play".into()),
        }
    }

    fn kit(blocks: &[&str], native: &[&str]) -> Value {
        json!({
            "version": 2,
            "blocks": blocks,
            "inputs": ["text", "money"],
            "formats": ["money", "percent"],
            "actions": ["operation", "page", "native"],
            "native": native,
            "renderers": ["kit.special"],
            "kinds": ["message-content", "page"],
        })
    }

    #[test]
    fn the_descriptor_is_the_engine_vocabulary_within_the_kit_and_the_allowlist() {
        let profile = parse_profile(
            json!({
                "platform": "flutter",
                "kitVocabulary": kit(
                    &["text", "chart", "from-a-newer-kit"],
                    &["camera", "documentCapture", "teleport"],
                ),
                "nativeAllowlist": ["documentCapture", "clipboardWrite", "teleport"],
            }),
            &identity(),
        )
        .unwrap();
        let client = PluginClient::of(Some(&profile), &identity(), false);
        let d = &client.descriptor;
        assert_eq!(d.platform, ClientPlatform::Flutter);
        assert_eq!(d.app_build, 41_020);
        assert_eq!(d.distribution, "play");
        assert_eq!(
            d.blocks,
            BTreeSet::from(["chart".to_owned(), "text".to_owned()]),
            "a block the engine does not know is not reported"
        );
        assert_eq!(d.native, BTreeSet::from(["documentCapture".to_owned()]));
        assert_eq!(
            client.native,
            BTreeSet::from([NativeCapability::DocumentCapture])
        );
        assert_eq!(d.renderers, BTreeSet::from(["kit.special".to_owned()]));
        assert_eq!(d.kinds.len(), 2);
        assert!(d.bundle.is_none());
        d.validate().unwrap();
    }

    #[test]
    fn bundle_delivery_is_advertised_only_when_installed() {
        let profile = parse_profile(
            json!({
                "platform": "ios",
                "kitVocabulary": kit(&["text"], &[]),
                "bundleRuntime": {"bridge": 1, "engine": "wkwebview", "engineMajor": 17},
            }),
            &identity(),
        )
        .unwrap();
        let unavailable = PluginClient::of(Some(&profile), &identity(), false).descriptor;
        let available = PluginClient::of(Some(&profile), &identity(), true).descriptor;
        assert!(unavailable.bundle.is_none());
        assert!(available.bundle.is_some());
    }

    #[test]
    fn without_a_profile_the_client_reads_version_one_and_offers_no_native_capability() {
        let client = PluginClient::of(None, &AppIdentity::default(), true);
        assert!(client.native.is_empty());
        assert!(client.descriptor.native.is_empty());
        assert!(client.vocabulary.native.is_empty());
        assert!(client.descriptor.renderers.is_empty());
        assert!(client.descriptor.bundle.is_none());
        assert!(!client.vocabulary.blocks.contains("chart"));
        assert!(client.vocabulary.blocks.contains("text"));
        assert_eq!(client.descriptor.distribution, "unknown");
        assert_eq!(client.descriptor.app_build, 0);
        client.descriptor.validate().unwrap();
    }

    #[test]
    fn a_profile_that_makes_no_valid_descriptor_is_refused() {
        for bad in [
            json!({"platform": "ios", "kitVocabulary": kit(&["text"], &[]), "extra": 1}),
            json!({"platform": "ios", "kitVocabulary": {"version": 0}}),
            json!({"platform": "ios", "kitVocabulary": kit(&["bad name"], &[])}),
            json!({"platform": "ios", "kitVocabulary": kit(&["text"], &[]),
                   "bundleRuntime": {"bridge": 0, "engine": "wkwebview", "engineMajor": 17}}),
            json!({"platform": "nowhere", "kitVocabulary": kit(&["text"], &[])}),
        ] {
            assert!(parse_profile(bad.clone(), &identity()).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_slot_reports_only_a_changed_profile() {
        let slot = PluginHostSlot::default();
        let profile = parse_profile(
            json!({"platform": "web", "kitVocabulary": kit(&["text"], &[])}),
            &identity(),
        )
        .unwrap();
        assert!(slot.set(profile.clone()));
        assert!(!slot.set(profile.clone()));
        let mut other = profile;
        other.native_allowlist.insert("camera".into());
        assert!(slot.set(other.clone()));
        assert_eq!(slot.profile(), Some(other));
    }
}
