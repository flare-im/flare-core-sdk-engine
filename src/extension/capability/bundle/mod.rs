//! L3 web bundles on the client: the content-addressed cache ([`cache`],
//! native only), the bridge broker that checks and routes every request a
//! bundle sends ([`broker`]), the bundle's own storage ([`storage`]) and the
//! events it may subscribe to ([`events`]).
//!
//! Hosts only move bytes: they show the bundle's files from the cache (or, on
//! Web, from the sandbox origin), hand every message the bundle posts to
//! `plugin_bundle_bridge`, and post back what the engine answers. Every check
//! of the bridge happens here once, for every platform.

pub mod broker;
#[cfg(not(target_arch = "wasm32"))]
pub mod cache;
pub mod events;
pub mod storage;

use flare_plugin_protocol::manifest::ClientPlatform;
use sha2::{Digest, Sha256};

/// What the client keeps for web bundles across sign-ins: the open pages and,
/// on native targets, the cache. Lives in the host's plugin slot.
#[derive(Default)]
pub struct BundleSlot {
    pub sessions: broker::BundleSessions,
    /// Forwards shared entity state to open pages while any is open.
    pub(crate) entity_watch: std::sync::Mutex<Option<crate::kernel::event::Subscription>>,
    #[cfg(not(target_arch = "wasm32"))]
    cache: std::sync::RwLock<Option<std::sync::Arc<cache::BundleCache>>>,
    /// The deployment's trust roots for downloads: the built-in roots plus the
    /// configured private CA, as update artifacts from the same mirrors use.
    #[cfg(not(target_arch = "wasm32"))]
    trust: std::sync::RwLock<Option<flare_core::common::config_types::TlsConfig>>,
}

impl std::fmt::Debug for BundleSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundleSlot").finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BundleSlot {
    /// Cache bundles under `root` (`{dataRoot}/plugin_bundles`), shared by
    /// every user of the device: a bundle is the same bytes for everyone.
    pub fn set_cache_root(&self, root: std::path::PathBuf) {
        let mut cache = self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache
            .as_ref()
            .is_none_or(|current| current.root() != root.as_path())
        {
            *cache = Some(std::sync::Arc::new(cache::BundleCache::new(root)));
        }
    }

    /// Trust the deployment's roots (`SdkConfig::core_tls_config`) when
    /// downloading bundles: a private deployment's mirrors carry its CA.
    pub fn set_trust(&self, trust: flare_core::common::config_types::TlsConfig) {
        *self
            .trust
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(trust);
    }

    /// A downloader with the deployment's trust roots.
    pub fn fetcher(&self) -> Option<cache::HttpBundleFetcher> {
        let trust = self
            .trust
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(flare_core::common::config_types::TlsConfig::none);
        cache::HttpBundleFetcher::new(&trust)
    }

    /// The cache, under the default data root when none was set.
    pub fn cache(&self) -> std::sync::Arc<cache::BundleCache> {
        if let Some(cache) = self
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            return std::sync::Arc::clone(cache);
        }
        self.set_cache_root(
            crate::shared::util::paths::default_sdk_data_root().join("plugin_bundles"),
        );
        self.cache()
    }
}

/// Characters of [`plugin_key`].
pub const PLUGIN_KEY_LEN: usize = 26;

/// The origin key of a plugin of a tenant (D-03): the first 26 characters of
/// the lowercase, unpadded base32 of `sha256("{tenant}:{plugin}")`. Every
/// bundle of one plugin of one tenant shares an origin (the browser and
/// WebView caches stay useful); two plugins, or two tenants, never do.
pub fn plugin_key(tenant_id: &str, plugin_id: &str) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let digest = Sha256::digest(format!("{tenant_id}:{plugin_id}").as_bytes());
    let mut out = String::with_capacity(PLUGIN_KEY_LEN);
    let (mut buffer, mut bits) = (0u32, 0u32);
    for byte in digest.iter() {
        buffer = (buffer << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 && out.len() < PLUGIN_KEY_LEN {
            bits -= 5;
            out.push(char::from(ALPHABET[((buffer >> bits) & 31) as usize]));
        }
        if out.len() == PLUGIN_KEY_LEN {
            break;
        }
    }
    out
}

/// Where a bundle's document lives on a platform: the origin the host gives
/// the WebView (`https://{key}.plugin.flare.invalid` on Android, a
/// `flare-plugin://{key}` scheme on iOS and Tauri). On Web the iframe has no
/// `allow-same-origin`, so its origin is opaque (`null`) whatever the sandbox
/// domain serving the files is.
pub fn bundle_origin(platform: ClientPlatform, key: &str) -> String {
    match host_kind(platform) {
        HostKind::Android => format!("https://{key}.plugin.flare.invalid"),
        HostKind::Scheme => format!("flare-plugin://{key}"),
        HostKind::Web => "null".to_owned(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostKind {
    Android,
    Scheme,
    Web,
}

fn host_kind(platform: ClientPlatform) -> HostKind {
    match platform {
        ClientPlatform::Web => HostKind::Web,
        ClientPlatform::Android => HostKind::Android,
        ClientPlatform::Ios | ClientPlatform::Tauri | ClientPlatform::Harmonyos => HostKind::Scheme,
        // The Flutter host embeds the native host of the device.
        ClientPlatform::Flutter => {
            if cfg!(target_os = "android") {
                HostKind::Android
            } else {
                HostKind::Scheme
            }
        }
    }
}

/// The content security policy of a bundle document (design 8.5), the same
/// on every platform: scripts, styles, images and fonts only from the
/// bundle's own source, no network (`connect-src 'none'`), no frames,
/// workers, plugins or form posts, and only `frame_ancestors` may embed it
/// (`'none'` for a native WebView, which is never framed). Native hosts send
/// it with every file; the Web sandbox server sends the same.
pub fn content_security_policy(source: &str, frame_ancestors: &str) -> String {
    format!(
        "default-src 'none'; script-src {source}; style-src {source}; \
         style-src-attr 'unsafe-inline'; img-src {source} blob: data:; font-src {source}; \
         media-src blob:; connect-src 'none'; frame-src 'none'; worker-src 'none'; \
         child-src 'none'; object-src 'none'; form-action 'none'; base-uri 'none'; \
         frame-ancestors {frame_ancestors}"
    )
}

/// The scheme and authority of an `https` address (`https://host[:port]`).
pub fn https_origin_of(url: &str) -> Option<String> {
    let rest = url.strip_prefix("https://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    (!authority.is_empty() && !authority.contains('@')).then(|| format!("https://{authority}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_plugin_key_is_per_tenant_and_plugin_and_host_safe() {
        let key = plugin_key("t1", "example.board");
        assert_eq!(key.len(), PLUGIN_KEY_LEN);
        assert!(
            key.bytes()
                .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
        );
        assert_eq!(key, plugin_key("t1", "example.board"), "stable");
        assert_ne!(key, plugin_key("t2", "example.board"), "per tenant");
        assert_ne!(key, plugin_key("t1", "example.other"), "per plugin");
        // The separator keeps `a:bc` and `ab:c` apart.
        assert_ne!(plugin_key("a", "bc"), plugin_key("ab", "c"));
    }

    #[test]
    fn origins_follow_the_platform_and_web_is_opaque() {
        let key = plugin_key("t1", "p");
        assert_eq!(
            bundle_origin(ClientPlatform::Android, &key),
            format!("https://{key}.plugin.flare.invalid")
        );
        assert_eq!(
            bundle_origin(ClientPlatform::Ios, &key),
            format!("flare-plugin://{key}")
        );
        assert_eq!(
            bundle_origin(ClientPlatform::Tauri, &key),
            format!("flare-plugin://{key}")
        );
        assert_eq!(bundle_origin(ClientPlatform::Web, &key), "null");
    }

    #[test]
    fn the_policy_forbids_network_frames_and_eval() {
        let csp = content_security_policy("flare-plugin://k", "'none'");
        for directive in [
            "default-src 'none'",
            "connect-src 'none'",
            "frame-src 'none'",
            "worker-src 'none'",
            "object-src 'none'",
            "base-uri 'none'",
            "form-action 'none'",
            "frame-ancestors 'none'",
            "script-src flare-plugin://k;",
        ] {
            assert!(csp.contains(directive), "{directive}");
        }
        assert!(!csp.contains("unsafe-eval"));
        assert!(!csp.contains("script-src 'unsafe-inline'"));
        assert_eq!(
            https_origin_of("https://sandbox.example.com:8443/b/abc/").as_deref(),
            Some("https://sandbox.example.com:8443")
        );
        assert_eq!(https_origin_of("http://x/"), None);
    }
}
