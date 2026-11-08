//! 接入清单、发布清单的载荷模型与客户端链路参数。

pub mod access;
pub mod policy;
pub mod recovery;
pub mod release;
pub mod schema;

pub use access::{
    AccessManifest, AccessPoint, BootstrapSection, BootstrapSource, Cell, ManifestError,
    ManifestTimes, PathService, Placement, PortService, Region, ReleaseSection, Services,
    is_valid_bootstrap_id, is_valid_id,
};
pub use policy::{
    BackoffPolicy, Bound, BreakerPolicy, ClientPolicy, FailbackPolicy, HeartbeatPolicy,
    POLICY_BOUNDS, ProbePolicy,
};
pub use recovery::{
    MAX_RECOVERY_ACCESS_POINTS, MAX_RECOVERY_LINK_BYTES, MAX_RECOVERY_SOURCES, RecoveryAccessPoint,
    RecoveryManifest,
};
pub use release::{
    ArtifactTarget, BlockedBuilds, ReleaseArtifact, ReleaseEntry, ReleaseManifest, ReleasePolicy,
    ResourcePack, Rollout, is_valid_path_segment, release_platform,
};
pub use schema::{DocType, MAX_DOCUMENT_BYTES};
