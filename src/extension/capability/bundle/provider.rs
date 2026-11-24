//! Native bundle delivery supplied by the application host.

use std::collections::BTreeSet;
use std::path::PathBuf;

use flare_plugin_protocol::bundle::{BundleFile, BundleManifest, WebBundleDecl};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BundleDeliveryError {
    #[error("bundle files are unavailable")]
    Unavailable,
    #[error("bundle failed validation")]
    Rejected,
}

/// Keeps the verified files available until the bridge session is dropped.
pub struct BundleLease {
    _guard: Box<dyn Send + Sync>,
}

impl BundleLease {
    pub fn new(guard: impl Send + Sync + 'static) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

impl std::fmt::Debug for BundleLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BundleLease")
            .finish_non_exhaustive()
    }
}

pub struct DeliveredBundle {
    pub manifest: BundleManifest,
    pub root: PathBuf,
    pub files: Vec<BundleFile>,
    pub lease: BundleLease,
}

/// A trusted host verifies the archive against the catalog before returning it.
#[async_trait::async_trait]
pub trait BundleDelivery: Send + Sync {
    async fn open(
        &self,
        declaration: &WebBundleDecl,
    ) -> Result<DeliveredBundle, BundleDeliveryError>;

    async fn prefetch(&self, declaration: &WebBundleDecl) -> Result<bool, BundleDeliveryError>;

    fn mark_referenced(&self, digests: &BTreeSet<String>);
}
