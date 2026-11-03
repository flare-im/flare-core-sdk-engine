use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

pub(crate) fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let core_root = manifest_dir
        .parent()
        .context("xtask manifest must live under flare-core-sdk-engine/xtask")?;
    let client_root = core_root
        .parent()
        .context("flare-im-core-sdk must live under the monorepo root")?
        .join("flare-core-sdk");
    if !client_root.is_dir() {
        bail!(
            "failed to resolve client SDK root from xtask manifest: {}",
            client_root.display()
        );
    }
    Ok(client_root)
}

pub(crate) fn core_root(root: &Path) -> PathBuf {
    root.parent()
        .unwrap_or_else(|| Path::new(".."))
        .join("flare-core-sdk-engine")
}

pub(crate) fn spec_dir(root: &Path) -> PathBuf {
    root.join("sdk-spec")
}

pub(crate) fn core_contract_dir(root: &Path) -> PathBuf {
    core_root(root).join("bindings/contract")
}
