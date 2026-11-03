//! `cargo xtask plugin-*` runs the plugin codegen tool in `xtask/plugin-codegen`.
//!
//! That tool builds against the private `flare-plugins-commercial/sdk` checkout (it shares the stub
//! emitter with the plugin contracts), so it is a separate workspace. Depending on it
//! from here would make this workspace, and every workspace that uses the SDK as a path
//! dependency, fail to resolve wherever that repository is not checked out.
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};

use crate::context::core_root;

pub(crate) fn run(root: &Path, command: &str) -> Result<()> {
    let sdk_root = core_root(root);
    let plugin_root = sdk_root
        .parent()
        .context("flare-im-core-sdk must live under the monorepo root")?
        .join("flare-plugins-commercial/sdk");
    if !plugin_root
        .join("flare-sdk-plugin-contract-codegen")
        .is_dir()
    {
        bail!(
            "plugin {command} needs the commercial plugin SDK checkout at {}; the SDK itself builds and tests without it",
            plugin_root.display()
        );
    }
    let manifest = sdk_root.join("xtask/plugin-codegen/Cargo.toml");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&sdk_root)
        .args(["run", "-q", "--manifest-path"])
        .arg(&manifest)
        .arg("--")
        .arg(command)
        .status()
        .with_context(|| format!("failed to run {}", manifest.display()))?;
    if !status.success() {
        bail!("plugin {command} failed ({status})");
    }
    Ok(())
}
