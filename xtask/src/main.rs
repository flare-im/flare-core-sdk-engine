use anyhow::{Context, Result, bail};
use std::{env, fs, path::Path};

mod access_seed;
mod build;
mod codegen;
mod context;
mod core_codegen;
mod fs_util;
mod plugin_codegen;
mod spec_model;
mod spec_query;
mod verify;

use codegen::{
    GeneratedTextTarget, arkts_api_type, camel_const, cangjie_api_arg, cangjie_api_type,
    cangjie_identifier, dart_api_type, emit_bridge_files, emit_doc_files,
    emit_expanded_client_spec_file, emit_platform_adapter_files, emit_platform_api_files,
    emit_platform_contract_files, emit_typescript_adapter_files, emit_typescript_contract_files,
    emit_wire_boundaries, facade_prop, json_quote, kotlin_api_module_dir, kotlin_api_type,
    kotlin_model_package_imports, listener_interface_name, lower_first, model_package_suffix,
    pascal_case, run_codegen, screaming_snake, single_trailing_newline, snake_case, swift_api_type,
    swift_identifier, ts_api_interface_name, ts_api_module_key, ts_model_from_json_fn,
    ts_model_to_map_fn, wire_boundary_targets,
};
use context::{core_contract_dir, core_root, spec_dir, workspace_root};
use fs_util::{
    file_contains, files_under, files_with_extension, find_matching, load_json,
    remove_output_paths, run_command, run_optional_command, upsert_bytes_file, upsert_text_file,
};
use spec_model::{CoreAbiRef, load_expanded_client_spec, sync_shared_contracts};
use spec_query::{
    all_spec_enums, all_spec_models, arr, bool_field, child_arr, find_model, include_path,
    include_paths, is_bridge_internal_method, is_connect_token_refresher_method,
    is_known_ts_model_type, is_list_type_name, list_inner_type_name, listener_payloads,
    message_build_catalog_entries, message_builder_extra_methods, message_builder_request_models,
    spec_enum_map, spec_enum_names, spec_model_names, str_field, typescript_listener_groups,
};
use verify::{
    emit_errors, fail, verify_channel_capability_gate, verify_core_contract,
    verify_e2ee_contract_gate, verify_enterprise_compliance_gate, verify_golden_contracts,
    verify_media_processing_gate, verify_multidevice_state_gate, verify_network_reconnect_gate,
    verify_observability_gate, verify_performance_gate, verify_plugin_marketplace_gate,
    verify_spec, verify_structure,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let root = workspace_root()?;
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "verify".to_string());
    let rest = args.collect::<Vec<_>>();
    match command.as_str() {
        "sync-spec" => sync_shared_contracts(&root, false),
        "spec" | "verify-spec" => verify_spec(&root),
        "core-contract" | "verify-core-contract" => verify_core_contract(&root),
        "multidevice-state" | "verify-multidevice-state" => verify_multidevice_state_gate(&root),
        "network-reconnect" | "verify-network-reconnect" => verify_network_reconnect_gate(&root),
        "observability" | "verify-observability" => verify_observability_gate(&root),
        "performance" | "verify-performance" => verify_performance_gate(&root),
        "plugin-marketplace" | "verify-plugin-marketplace" => verify_plugin_marketplace_gate(&root),
        "e2ee-contract" | "verify-e2ee-contract" => verify_e2ee_contract_gate(&root),
        "enterprise-compliance" | "verify-enterprise-compliance" => {
            verify_enterprise_compliance_gate(&root)
        }
        "channel-capability" | "verify-channel-capability" => verify_channel_capability_gate(&root),
        "media-processing" | "verify-media-processing" => verify_media_processing_gate(&root),
        "structure" | "verify-structure" => verify_structure(&root),
        "access-seed" => access_seed::run(&rest),
        "schema" => core_codegen::run("schema"),
        "schema-check" => core_codegen::run("schema-check"),
        "plugin-verify" => plugin_codegen::run(&root, "verify"),
        "plugin-schema" => plugin_codegen::run(&root, "schema"),
        "plugin-schema-check" => plugin_codegen::run(&root, "schema-check"),
        "plugin-codegen" => plugin_codegen::run(&root, "codegen"),
        "plugin-codegen-check" => plugin_codegen::run(&root, "check"),
        "core-codegen" => core_codegen::run("codegen"),
        "core-codegen-check" => core_codegen::run("check"),
        "expanded-spec" => emit_expanded_client_spec_file(&root, false),
        "expanded-spec-check" => emit_expanded_client_spec_file(&root, true),
        "docs" => emit_doc_files(&root, false),
        "docs-check" => emit_doc_files(&root, true),
        "verify" => {
            core_codegen::run("verify")?;
            plugin_codegen::run(&root, "verify")?;
            sync_shared_contracts(&root, true)?;
            verify_spec(&root)?;
            verify_core_contract(&root)?;
            verify_golden_contracts(&root)?;
            verify_multidevice_state_gate(&root)?;
            verify_network_reconnect_gate(&root)?;
            verify_observability_gate(&root)?;
            verify_performance_gate(&root)?;
            verify_plugin_marketplace_gate(&root)?;
            verify_e2ee_contract_gate(&root)?;
            verify_enterprise_compliance_gate(&root)?;
            verify_channel_capability_gate(&root)?;
            verify_media_processing_gate(&root)?;
            verify_structure(&root)?;
            Ok(())
        }
        "wire-boundary" => emit_wire_boundaries(&root, false),
        "wire-boundary-check" => emit_wire_boundaries(&root, true),
        "bridge" => emit_bridge_files(&root, false),
        "bridge-check" => emit_bridge_files(&root, true),
        "platform-api" => emit_platform_api_files(&root, false),
        "platform-api-check" => emit_platform_api_files(&root, true),
        "typescript-contract" => emit_typescript_contract_files(&root, false),
        "typescript-contract-check" => emit_typescript_contract_files(&root, true),
        "platform-contract" => emit_platform_contract_files(&root, false),
        "platform-contract-check" => emit_platform_contract_files(&root, true),
        "platform-adapter" => emit_platform_adapter_files(&root, false),
        "platform-adapter-check" => emit_platform_adapter_files(&root, true),
        "typescript-adapter" => emit_typescript_adapter_files(&root, false),
        "typescript-adapter-check" => emit_typescript_adapter_files(&root, true),
        "codegen" => {
            core_codegen::run("codegen")?;
            plugin_codegen::run(&root, "codegen")?;
            sync_shared_contracts(&root, false)?;
            verify_core_contract(&root)?;
            run_codegen(&root, false)
        }
        "codegen-check" => {
            core_codegen::run("check")?;
            plugin_codegen::run(&root, "check")?;
            sync_shared_contracts(&root, true)?;
            verify_golden_contracts(&root)?;
            run_codegen(&root, true)
        }
        "build" => build::run(&root, &rest),
        "check" => {
            core_codegen::run("check")?;
            plugin_codegen::run(&root, "check")?;
            sync_shared_contracts(&root, true)?;
            verify_spec(&root)?;
            verify_core_contract(&root)?;
            verify_golden_contracts(&root)?;
            verify_multidevice_state_gate(&root)?;
            verify_network_reconnect_gate(&root)?;
            verify_observability_gate(&root)?;
            verify_performance_gate(&root)?;
            verify_plugin_marketplace_gate(&root)?;
            verify_e2ee_contract_gate(&root)?;
            verify_enterprise_compliance_gate(&root)?;
            verify_channel_capability_gate(&root)?;
            verify_media_processing_gate(&root)?;
            verify_structure(&root)?;
            let ts_sdk = root.join("packages/flare-core-typescript-sdk");
            run_command(&ts_sdk, "npm", &["run", "build"])?;
            run_command(&ts_sdk, "npm", &["test"])?;
            run_command(
                &root.join("packages/flare-core-vue-im-ui"),
                "npm",
                &["run", "typecheck"],
            )?;
            let flutter = root.join("packages/flare-core-flutter-sdk");
            run_command(&flutter, "dart", &["pub", "get"])?;
            run_command(&flutter, "dart", &["analyze"])?;
            run_command(&flutter, "dart", &["test"])?;
            run_command(
                &root.join("packages/flare-core-apple-sdk"),
                "swift",
                &["build"],
            )?;
            run_optional_command(
                &root.join("packages/flare-core-harmony-cangjie-sdk"),
                "cjpm",
                &["build"],
                "skip Cangjie build: cjpm not found",
            )?;
            run_optional_command(
                &root.join("packages/flare-core-harmony-arkts-sdk"),
                "ohpm",
                &["install", "--offline"],
                "skip ArkTS package install: ohpm not found",
            )?;
            Ok(())
        }
        "all" => {
            core_codegen::run("codegen")?;
            plugin_codegen::run(&root, "codegen")?;
            sync_shared_contracts(&root, false)?;
            verify_spec(&root)?;
            verify_core_contract(&root)?;
            verify_golden_contracts(&root)?;
            run_codegen(&root, false)?;
            verify_multidevice_state_gate(&root)?;
            verify_network_reconnect_gate(&root)?;
            verify_observability_gate(&root)?;
            verify_performance_gate(&root)?;
            verify_plugin_marketplace_gate(&root)?;
            verify_e2ee_contract_gate(&root)?;
            verify_enterprise_compliance_gate(&root)?;
            verify_channel_capability_gate(&root)?;
            verify_media_processing_gate(&root)?;
            verify_structure(&root)
        }
        "clean" => clean(&root),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => {
            print_help();
            bail!("unknown xtask command: {other}")
        }
    }
}

fn print_help() {
    eprintln!("Usage: cargo xtask <command>");
    eprintln!("Commands:");
    eprintln!("  access-seed          Pull the latest signed access manifest into a build seed");
    eprintln!("  schema               Generate core Rust DTO JSON Schema artifacts");
    eprintln!("  schema-check         Fail if core Rust DTO JSON Schema artifacts drift");
    eprintln!("  plugin-verify        Verify plugin manifests and marketplace catalog");
    eprintln!("  plugin-schema        Generate plugin manifest JSON Schema");
    eprintln!("  plugin-schema-check  Fail if plugin manifest JSON Schema drifts");
    eprintln!("  plugin-codegen       Generate plugin manifest schema and platform stubs");
    eprintln!("  plugin-codegen-check Fail if plugin generated artifacts drift");
    eprintln!("  core-codegen         Generate core binding artifacts only");
    eprintln!("  core-codegen-check   Fail if core binding generated artifacts drift");
    eprintln!("  sync-spec            Sync message build catalog from core contract");
    eprintln!("  spec                 Verify sdk-spec invariants");
    eprintln!("  core-contract        Verify sdk-spec parity with core bindings contract");
    eprintln!("  multidevice-state   Verify read state sync and draft API anchors");
    eprintln!("  network-reconnect   Verify platform network-change active reconnect entrypoints");
    eprintln!("  observability        Verify client diagnostics and metrics entrypoints");
    eprintln!("  performance          Verify public performance benchmark entrypoints");
    eprintln!(
        "  plugin-marketplace  Verify plugin catalog, schema, stubs, and capability registry"
    );
    eprintln!("  e2ee-contract      Verify E2EE SDK, server push, and privacy anchors");
    eprintln!("  channel-capability Verify Channel/Broadcast and large-conversation anchors");
    eprintln!("  media-processing   Verify media processing port/server processor anchors");
    eprintln!("  structure            Verify repository/package structure and retired paths");
    eprintln!("  verify               Run spec, core-contract, and structure checks");
    eprintln!("  expanded-spec        Generate Rust-owned expanded sdk-spec snapshot");
    eprintln!("  expanded-spec-check  Fail if Rust-owned expanded sdk-spec snapshot drifts");
    eprintln!("  docs                 Generate Rust-owned SDK documentation artifacts");
    eprintln!("  docs-check           Fail if Rust-owned SDK documentation artifacts drift");
    eprintln!("  wire-boundary        Generate Rust-owned platform wire boundary code");
    eprintln!("  wire-boundary-check  Fail if Rust-owned platform wire boundary code drifts");
    eprintln!("  bridge               Generate Rust-owned platform bridge/FFI code");
    eprintln!("  bridge-check         Fail if Rust-owned platform bridge/FFI code drifts");
    eprintln!("  platform-api         Generate Rust-owned platform API contracts");
    eprintln!("  platform-api-check   Fail if Rust-owned platform API contracts drift");
    eprintln!("  typescript-contract  Generate Rust-owned TypeScript model/listener/callback code");
    eprintln!("  typescript-contract-check  Fail if Rust-owned TypeScript contract code drifts");
    eprintln!(
        "  platform-contract    Generate Rust-owned Dart/Kotlin/Swift/ArkTS/Cangjie contracts"
    );
    eprintln!("  platform-contract-check  Fail if Rust-owned platform contract code drifts");
    eprintln!("  platform-adapter     Generate Rust-owned platform adapter static artifacts");
    eprintln!("  platform-adapter-check  Fail if Rust-owned platform adapter code drifts");
    eprintln!("  typescript-adapter   Generate Rust-owned TypeScript adapter code");
    eprintln!("  typescript-adapter-check  Fail if Rust-owned TypeScript adapter code drifts");
    eprintln!("  codegen              Run codegen with Rust-owned artifact post-processing");
    eprintln!("  codegen-check        Run codegen and fail if generated outputs drift");
    eprintln!("  build                Build and place native/wasm artifacts");
    eprintln!("  check                Run verify plus available package checks");
    eprintln!("  all                  Sync spec, verify, codegen, structure");
    eprintln!("  clean                Remove local build caches from SDK packages/tools");
}

fn clean(root: &Path) -> Result<()> {
    for rel in [
        "packages/flare-core-flutter-sdk/.dart_tool",
        "packages/flare-core-apple-sdk/.build",
    ] {
        let path = root.join(rel);
        if path.exists() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    println!("client SDK package/tool caches cleaned");
    Ok(())
}
