//! Plugin manifest schema, catalog checks and six-platform plugin API stubs for the
//! `flare-plugins-commercial` checkout next to this SDK.
//!
//! The stubs come from the emitter the plugin contracts share
//! (`flare-sdk-plugin-contract-codegen`), and that crate lives in the private
//! `flare-plugins-commercial` repository. This tool is therefore its own workspace: the SDK
//! workspace, and every workspace that uses the SDK as a path dependency, resolves and
//! builds without that repository. `cargo xtask plugin-*` runs it.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use flare_im_core_sdk::plugin::SdkPluginManifest;
use flare_im_core_sdk::schemars::schema_for;
use flare_im_core_sdk::serde::Deserialize;
use flare_im_core_sdk::serde_json;
use sha2::{Digest, Sha256};

fn main() {
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "verify".to_string());
    if let Err(error) = run(&sdk_root(), &command) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

/// `xtask/plugin-codegen` sits two levels below the SDK root.
fn sdk_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("xtask/plugin-codegen lives under flare-im-core-sdk")
        .to_path_buf()
}

fn run(sdk_root: &Path, command: &str) -> Result<()> {
    match command {
        "verify" => {
            verify(sdk_root)?;
            println!("plugin manifest verify passed");
            Ok(())
        }
        "schema" => write_schema(sdk_root, false),
        "schema-check" => write_schema(sdk_root, true),
        "codegen" => {
            verify(sdk_root)?;
            write_schema(sdk_root, false)?;
            write_stubs(sdk_root, false)
        }
        "check" => {
            verify(sdk_root)?;
            write_schema(sdk_root, true)?;
            write_stubs(sdk_root, true)
        }
        other => bail!("unknown plugin xtask command: {other}"),
    }
}

fn verify(sdk_root: &Path) -> Result<Vec<PluginManifestFile>> {
    let plugin_root = plugin_root(sdk_root)?;
    let manifests = load_plugin_manifests(&plugin_root)?;
    if manifests.is_empty() {
        bail!(
            "no plugin manifests found under {}; expected at least one plugin.json",
            plugin_root.display()
        );
    }

    let mut ids = BTreeSet::new();
    let mut namespaces = BTreeMap::<String, String>::new();
    for item in &manifests {
        item.manifest
            .validate(&item.manifest.id, &item.manifest.namespaces)
            .map_err(|error| anyhow::anyhow!("{error}"))
            .with_context(|| format!("invalid plugin manifest {}", item.path.display()))?;
        if !ids.insert(item.manifest.id.clone()) {
            bail!("duplicate plugin manifest id {}", item.manifest.id);
        }
        for namespace in &item.manifest.namespaces {
            if let Some(owner) = namespaces.insert(namespace.to_string(), item.manifest.id.clone())
            {
                bail!(
                    "plugin namespace {namespace} is claimed by both {owner} and {}",
                    item.manifest.id
                );
            }
        }
    }

    verify_catalog(&plugin_root, &manifests)?;
    Ok(manifests)
}

fn write_schema(sdk_root: &Path, check: bool) -> Result<()> {
    let plugin_root = plugin_root(sdk_root)?;
    let schema = schema_for!(SdkPluginManifest);
    let content = format!("{}\n", serde_json::to_string_pretty(&schema)?);
    write_text(
        &plugin_root.join("schema/plugin_manifest.schema.json"),
        &content,
        check,
    )
}

fn write_stubs(sdk_root: &Path, check: bool) -> Result<()> {
    let manifests = verify(sdk_root)?;
    let plugin_root = plugin_root(sdk_root)?;
    for item in manifests {
        let plugin_dir = generated_dir(&plugin_root, &item.manifest.id);
        for output in stub_outputs(&item.manifest) {
            write_text(&plugin_dir.join(output.path), &output.content, check)?;
        }
    }
    Ok(())
}

fn verify_catalog(plugin_root: &Path, manifests: &[PluginManifestFile]) -> Result<()> {
    let path = plugin_root.join("catalog/plugins.json");
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read plugin catalog {}", path.display()))?;
    let catalog: PluginCatalog = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse plugin catalog {}", path.display()))?;
    if catalog.catalog_version.trim().is_empty() {
        bail!("plugin catalog catalogVersion must not be empty");
    }
    if catalog.schema.trim().is_empty() {
        bail!("plugin catalog schema must not be empty");
    }

    let by_id = manifests
        .iter()
        .map(|item| (item.manifest.id.as_str(), item))
        .collect::<BTreeMap<_, _>>();
    let mut catalog_ids = BTreeSet::new();
    for entry in &catalog.plugins {
        if !catalog_ids.insert(entry.id.as_str()) {
            bail!("duplicate plugin catalog id {}", entry.id);
        }
        let Some(item) = by_id.get(entry.id.as_str()) else {
            bail!("catalog references missing plugin manifest {}", entry.id);
        };
        if entry.version != item.manifest.version {
            bail!(
                "catalog entry {} version {} does not match manifest version {}",
                entry.id,
                entry.version,
                item.manifest.version
            );
        }
        if entry.display_name != item.manifest.display_name {
            bail!(
                "catalog entry {} displayName does not match manifest displayName",
                entry.id
            );
        }
        if entry.manifest_path.trim().is_empty() || entry.source_path.trim().is_empty() {
            bail!("catalog entry {} paths must not be empty", entry.id);
        }
        let expected = if item.path == call_manifest_path(plugin_root)? {
            CALL_MANIFEST.to_owned()
        } else {
            normalize_relative_path(commercial_root(plugin_root)?, &item.path)?
        };
        // Older commercial catalogs used paths relative to sdk/. Allow those
        // only for manifests discovered under sdk/ during the rollout.
        let catalog_path = if entry.manifest_path == expected {
            expected.as_str()
        } else if expected.strip_prefix("sdk/") == Some(entry.manifest_path.as_str()) {
            entry.manifest_path.as_str()
        } else {
            bail!(
                "catalog entry {} manifestPath {} does not match {}",
                entry.id,
                entry.manifest_path,
                expected
            );
        };
        let Some(expected_source_path) = catalog_path.strip_suffix("/plugin.json") else {
            bail!(
                "catalog entry {} manifestPath must point to plugin.json",
                entry.id
            );
        };
        if entry.source_path != expected_source_path {
            bail!(
                "catalog entry {} sourcePath {} must match manifest directory {}",
                entry.id,
                entry.source_path,
                expected_source_path
            );
        }
        let expected_category = item
            .manifest
            .namespaces
            .first()
            .expect("validated manifest has at least one namespace");
        if entry.category != *expected_category {
            bail!(
                "catalog entry {} category {} must match manifest namespace {}",
                entry.id,
                entry.category,
                expected_category
            );
        }
        let manifest_sha256 = sha256_file(&item.path)?;
        if entry.manifest_sha256 != manifest_sha256 {
            bail!(
                "catalog entry {} manifestSha256 {} does not match {}",
                entry.id,
                entry.manifest_sha256,
                manifest_sha256
            );
        }
        if entry.platforms != item.manifest.platforms {
            bail!(
                "catalog entry {} platforms must match manifest platforms",
                entry.id
            );
        }
        if entry.distribution != "composition-time" {
            bail!(
                "catalog entry {} distribution must be composition-time",
                entry.id,
            );
        }
        if entry.runtime_install != "not-supported" {
            bail!(
                "catalog entry {} runtimeInstall must be not-supported",
                entry.id,
            );
        }
    }

    for item in manifests {
        if !catalog_ids.contains(item.manifest.id.as_str()) {
            bail!(
                "plugin manifest {} is missing from catalog",
                item.manifest.id
            );
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| {
        format!(
            "failed to read plugin manifest for sha256 {}",
            path.display()
        )
    })?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("{digest:x}"))
}

fn load_plugin_manifests(plugin_root: &Path) -> Result<Vec<PluginManifestFile>> {
    let mut paths = Vec::new();
    collect_plugin_manifest_paths(plugin_root, &mut paths)?;
    let commercial = commercial_root(plugin_root)?;
    let plugins = commercial.join("plugins");
    if optional_plugin_directory(&plugins)? {
        for entry in fs::read_dir(&plugins)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let sdk = entry.path().join("sdk");
                if optional_plugin_directory(&sdk)? {
                    collect_plugin_manifest_paths(&sdk, &mut paths)?;
                }
            }
        }
    }

    let call = call_manifest_path(plugin_root)?;
    let call_repo = call
        .ancestors()
        .nth(2)
        .context("missing Call repository root")?;
    let call_repo = call_repo.canonicalize()?;
    let commercial = commercial.canonicalize()?;
    paths.push(call.clone());
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let root = if path == call {
                &call_repo
            } else {
                &commercial
            };
            if !fs::symlink_metadata(&path)
                .with_context(|| format!("failed to inspect plugin manifest {}", path.display()))?
                .is_file()
            {
                bail!("plugin manifest must be a regular file: {}", path.display());
            }
            if !path.canonicalize()?.starts_with(root) {
                bail!(
                    "plugin manifest is outside {}: {}",
                    root.display(),
                    path.display()
                );
            }
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("failed to read plugin manifest {}", path.display()))?;
            let manifest = serde_json::from_str::<SdkPluginManifest>(&raw)
                .with_context(|| format!("failed to parse plugin manifest {}", path.display()))?;
            Ok(PluginManifestFile { path, manifest })
        })
        .collect()
}

fn optional_plugin_directory(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(_) => bail!(
            "plugin directory must be a real directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn collect_plugin_manifest_paths(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let skip_dirs = ["target", "generated", "schema", "catalog", "template"];
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", dir.display()))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if entry.file_type()?.is_dir() {
            if skip_dirs.contains(&file_name.as_ref()) {
                continue;
            }
            collect_plugin_manifest_paths(&path, out)?;
        } else if file_name == "plugin.json" {
            out.push(path);
        }
    }
    Ok(())
}

fn stub_outputs(manifest: &SdkPluginManifest) -> Vec<GeneratedStub> {
    flare_sdk_plugin_contract_codegen::native::plugin_apis(
        &manifest.id,
        &manifest.namespaces,
        &manifest
            .operations
            .iter()
            .map(|operation| operation.op.clone())
            .collect::<Vec<_>>(),
    )
    .into_iter()
    .map(|output| GeneratedStub {
        path: output.path,
        content: output.content,
    })
    .collect()
}

fn generated_dir(plugin_root: &Path, plugin_id: &str) -> PathBuf {
    plugin_root
        .join("generated")
        .join(flare_sdk_plugin_contract_codegen::native::snake_identifier(
            plugin_id,
        ))
}

fn plugin_root(sdk_root: &Path) -> Result<PathBuf> {
    let repo_root = sdk_root
        .parent()
        .context("flare-im-core-sdk must live under repository root")?;
    Ok(repo_root.join("flare-plugins-commercial/sdk"))
}

fn commercial_root(plugin_root: &Path) -> Result<&Path> {
    plugin_root
        .parent()
        .context("missing commercial plugin repository root")
}

const CALL_MANIFEST: &str = "flare-plugin-call/sdk/plugin.json";

fn call_manifest_path(plugin_root: &Path) -> Result<PathBuf> {
    let workspace = plugin_root
        .ancestors()
        .nth(2)
        .context("commercial plugin SDK must live at flare-plugins-commercial/sdk")?;
    Ok(workspace.join(CALL_MANIFEST))
}

fn normalize_relative_path(root: &Path, path: &Path) -> Result<String> {
    Ok(path
        .strip_prefix(root)
        .with_context(|| format!("{} is not under {}", path.display(), root.display()))?
        .to_string_lossy()
        .replace('\\', "/"))
}

fn write_text(path: &Path, content: &str, check: bool) -> Result<()> {
    if check {
        let existing = fs::read_to_string(path)
            .with_context(|| format!("generated file missing: {}", path.display()))?;
        if existing != content {
            bail!("generated file drift: {}", path.display());
        }
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if fs::read_to_string(path).ok().as_deref() != Some(content) {
        fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}

struct PluginManifestFile {
    path: PathBuf,
    manifest: SdkPluginManifest,
}

struct GeneratedStub {
    path: PathBuf,
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluginCatalog {
    catalog_version: String,
    schema: String,
    plugins: Vec<PluginCatalogEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluginCatalogEntry {
    id: String,
    version: String,
    display_name: String,
    manifest_path: String,
    manifest_sha256: String,
    source_path: String,
    category: String,
    distribution: String,
    runtime_install: String,
    platforms: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "flare-plugin-codegen-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            Self(root)
        }

        fn sdk(&self) -> PathBuf {
            self.0.join("flare-core-sdk-engine")
        }

        fn commercial(&self, path: &str) -> PathBuf {
            self.0.join("flare-plugins-commercial").join(path)
        }

        fn manifest(&self, path: &Path, id: &str) {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(
                path,
                json!({
                    "id": id, "version": "1.0.0", "displayName": id, "namespaces": [id],
                    "operations": [{"op": format!("{id}.test")}], "platforms": []
                })
                .to_string(),
            )
            .unwrap();
        }

        fn catalog(&self, paths: &[(&Path, &str, &str)]) {
            let plugins: Vec<_> = paths
                .iter()
                .map(|(path, id, relative)| {
                    json!({
                        "id": id, "version": "1.0.0", "displayName": id, "category": id,
                        "manifestPath": relative,
                        "sourcePath": relative.strip_suffix("/plugin.json").unwrap(),
                        "manifestSha256": sha256_file(path).unwrap(), "platforms": [],
                        "distribution": "composition-time", "runtimeInstall": "not-supported"
                    })
                })
                .collect();
            let path = self.commercial("sdk/catalog/plugins.json");
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(
                path,
                json!({"schema": "test", "catalogVersion": "1", "plugins": plugins}).to_string(),
            )
            .unwrap();
        }

        fn call(&self) -> PathBuf {
            let path = self.0.join(CALL_MANIFEST);
            self.manifest(&path, "flare.call");
            path
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn verifies_both_commercial_layouts_and_the_separate_call_repository() {
        let fixture = Fixture::new();
        let legacy = fixture.commercial("sdk/legacy/plugin.json");
        let poll = fixture.commercial("plugins/poll/sdk/plugin.json");
        let call = fixture.call();
        fixture.manifest(&legacy, "flare.legacy");
        fixture.manifest(&poll, "flare.poll");
        fixture.catalog(&[
            (&legacy, "flare.legacy", "sdk/legacy/plugin.json"),
            (&poll, "flare.poll", "plugins/poll/sdk/plugin.json"),
            (&call, "flare.call", CALL_MANIFEST),
        ]);
        assert_eq!(verify(&fixture.sdk()).unwrap().len(), 3);
    }

    #[test]
    fn verifies_previous_sdk_relative_catalog_during_rollout() {
        let fixture = Fixture::new();
        let legacy = fixture.commercial("sdk/legacy/plugin.json");
        let call = fixture.call();
        fixture.manifest(&legacy, "flare.legacy");
        fixture.catalog(&[
            (&legacy, "flare.legacy", "legacy/plugin.json"),
            (&call, "flare.call", CALL_MANIFEST),
        ]);
        assert_eq!(verify(&fixture.sdk()).unwrap().len(), 2);
    }

    #[test]
    fn rejects_duplicate_ids_across_the_two_commercial_layouts() {
        let fixture = Fixture::new();
        let call = fixture.call();
        let legacy = fixture.commercial("sdk/legacy/plugin.json");
        fixture.manifest(&legacy, "flare.poll");
        fixture.manifest(
            &fixture.commercial("plugins/poll/sdk/plugin.json"),
            "flare.poll",
        );
        fixture.catalog(&[
            (&legacy, "flare.poll", "legacy/plugin.json"),
            (&call, "flare.call", CALL_MANIFEST),
        ]);
        let error = verify(&fixture.sdk()).err().unwrap().to_string();
        assert!(
            error.contains("duplicate plugin manifest id flare.poll"),
            "{error}"
        );
    }

    #[test]
    fn rejects_missing_manifests_and_paths_outside_the_selected_root() {
        let fixture = Fixture::new();
        let call = fixture.call();
        let poll = fixture.commercial("plugins/poll/sdk/plugin.json");
        fixture.manifest(&poll, "flare.poll");
        fixture.catalog(&[
            (&poll, "flare.poll", "../poll/plugin.json"),
            (&call, "flare.call", CALL_MANIFEST),
        ]);
        let error = verify(&fixture.sdk()).err().unwrap().to_string();
        assert!(error.contains("manifestPath"), "{error}");
        fs::remove_file(&poll).unwrap();
        let error = verify(&fixture.sdk()).err().unwrap().to_string();
        assert!(
            error.contains("catalog references missing plugin manifest flare.poll"),
            "{error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_manifest_symlinks_that_leave_a_plugin_repository() {
        let fixture = Fixture::new();
        let call = fixture.call();
        let manifest = fixture.commercial("sdk/linked/plugin.json");
        fs::create_dir_all(manifest.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&call, &manifest).unwrap();
        let error = verify(&fixture.sdk()).err().unwrap().to_string();
        assert!(
            error.contains("plugin manifest must be a regular file"),
            "{error}"
        );
    }
}
