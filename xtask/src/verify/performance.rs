use anyhow::Result;
use std::{fs, path::Path};

use crate::{core_root, emit_errors, fail};

pub(crate) fn verify_performance_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let monorepo_root = root.parent().unwrap_or_else(|| Path::new(".."));
    let sdk_root = core_root(root);
    let flare_core_root = monorepo_root.join("flare-transport");

    let public_doc = root.join("docs/performance-baselines.md");
    require_contains_all(
        &mut errors,
        &public_doc,
        "performance baseline doc",
        &[
            "flare-core",
            "flare-core-sdk-engine",
            "send→render",
            "cargo bench",
        ],
    );

    require_contains_all(
        &mut errors,
        &flare_core_root.join("Cargo.toml"),
        "flare-core bench manifest",
        &[
            "[[bench]]",
            "name = \"perf_baseline\"",
            "required-features = [\"server\", \"compression-gzip\"]",
        ],
    );
    require_contains_all(
        &mut errors,
        &flare_core_root.join("benches/perf_baseline.rs"),
        "flare-core performance baseline",
        &[
            "codec.protobuf.round_trip.256b",
            "pipeline.process_raw.validate_no_response.256b",
            "connection_manager.broadcast.1000x256b",
            "connection_manager.cleanup_timeout_trait.1000",
            "serde_json::to_string_pretty",
        ],
    );

    require_contains_all(
        &mut errors,
        &sdk_root.join("Cargo.toml"),
        "core SDK bench manifest",
        &[
            "criterion = \"0.5\"",
            "[[bench]]",
            "name = \"perf_baseline\"",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("benches/perf_baseline.rs"),
        "core SDK performance baseline",
        &[
            "event_bus_publish_steady_state",
            "prepare_text_message",
            "memory_store_save_batch_100",
            "event_bus_publish_and_drain_1000",
            "event_json_serialization",
            "protocol_codec",
            "criterion_group!",
        ],
    );

    emit_errors("performance gate", errors)
}

fn require_contains_all(errors: &mut Vec<String>, path: &Path, label: &str, needles: &[&str]) {
    let Ok(text) = fs::read_to_string(path) else {
        fail(
            errors,
            format!("{label} missing or unreadable: {}", path.display()),
        );
        return;
    };

    for needle in needles {
        if !text.contains(needle) {
            fail(
                errors,
                format!("{label} missing `{needle}` in {}", path.display()),
            );
        }
    }
}
