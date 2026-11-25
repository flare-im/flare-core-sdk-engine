//! Platform adapters.
//!
//! Core IM behavior lives in `domain`, `application`, and `core`.
//! Platform differences are isolated here and behind `ports`.

pub mod media;
mod platform;
pub mod storage;

use crate::platform::runtime::PlatformKind;

pub use media::{MediaAdapterProfile, MediaSourceSupport, UploadOnlyMediaService};
pub use platform::{AdapterPlatform, AdapterProvisioning};
pub use storage::StorageAdapterProfile;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlatformAdapterProfile {
    pub platform: PlatformKind,
    pub adapter_platform: AdapterPlatform,
    pub media: MediaAdapterProfile,
    pub storage: StorageAdapterProfile,
}

impl PlatformAdapterProfile {
    pub fn for_platform(platform: PlatformKind) -> Self {
        let adapter_platform = AdapterPlatform::from_runtime(platform);
        Self {
            platform,
            adapter_platform,
            media: MediaAdapterProfile::for_platform(adapter_platform),
            storage: StorageAdapterProfile::for_platform(adapter_platform),
        }
    }

    pub fn requires_host_media_adapter(&self) -> bool {
        self.media.requires_host_adapter()
    }

    pub fn requires_host_storage_adapter(&self) -> bool {
        self.storage.requires_host_adapter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::runtime::{PlatformKind, StorageKind};

    #[test]
    fn web_requires_host_media_and_storage_adapters() {
        let profile = PlatformAdapterProfile::for_platform(PlatformKind::Web);

        assert_eq!(profile.adapter_platform, AdapterPlatform::Web);
        assert!(profile.requires_host_media_adapter());
        assert!(profile.requires_host_storage_adapter());
        assert_eq!(profile.storage.preferred_kind, StorageKind::IndexedDb);
    }

    #[test]
    fn flutter_uses_native_adapter_family_with_builtin_storage_and_media() {
        let profile = PlatformAdapterProfile::for_platform(PlatformKind::Flutter);

        assert_eq!(profile.adapter_platform, AdapterPlatform::Flutter);
        assert!(!profile.requires_host_media_adapter());
        assert!(!profile.requires_host_storage_adapter());
        assert_eq!(profile.storage.preferred_kind, StorageKind::Sqlite);
    }

    #[test]
    fn harmony_runtimes_share_one_adapter_family() {
        let arkts = PlatformAdapterProfile::for_platform(PlatformKind::HarmonyArkTs);
        let cangjie = PlatformAdapterProfile::for_platform(PlatformKind::HarmonyCangjie);

        assert_eq!(arkts.adapter_platform, AdapterPlatform::Harmony);
        assert_eq!(cangjie.adapter_platform, AdapterPlatform::Harmony);
        assert_eq!(arkts.media.sources, cangjie.media.sources);
        assert_eq!(arkts.storage.preferred_kind, cangjie.storage.preferred_kind);
    }
}
