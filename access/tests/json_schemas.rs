//! 四份公开 JSON Schema 的真源是本 crate 的 Rust 类型；这里保证 `flare-protocol/schemas/` 不漂移。
//!
//! 类型变了就重新生成并审阅 diff：
//! `FLARE_ACCESS_WRITE_SCHEMAS=1 cargo test -p flare-access --test json_schemas`。
//! 这个测试不依赖 `client` feature：控制面只开 `manifest` 与 `trust`，看到的是同一份类型。

use std::fs;
use std::path::PathBuf;

use flare_access::manifest::{AccessManifest, ReleaseManifest};
use flare_access::trust::{KeyCert, KeyState};
use schemars::{JsonSchema, schema_for};
use serde_json::{Value, json};

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../flare-protocol/schemas")
}

fn generate<T: JsonSchema>(file: &str, title: &str) -> Value {
    let mut schema = serde_json::to_value(schema_for!(T)).expect("schema serializes to JSON");
    let object = schema.as_object_mut().expect("schema is an object");
    object.insert(
        "$id".to_owned(),
        json!(format!("https://flare.im/schemas/{file}")),
    );
    object.insert("title".to_owned(), json!(title));
    schema
}

fn expected() -> Vec<(&'static str, Value)> {
    let documents = [
        (
            "access_manifest.v1.schema.json",
            generate::<AccessManifest>(
                "access_manifest.v1.schema.json",
                "Flare access manifest v1",
            ),
        ),
        (
            "release_manifest.v1.schema.json",
            generate::<ReleaseManifest>(
                "release_manifest.v1.schema.json",
                "Flare release manifest v1",
            ),
        ),
        (
            "keycert.v1.schema.json",
            generate::<KeyCert>("keycert.v1.schema.json", "Flare online key certificate v1"),
        ),
        (
            "keystate.v1.schema.json",
            generate::<KeyState>("keystate.v1.schema.json", "Flare key state document v1"),
        ),
    ];
    documents.into_iter().collect()
}

#[test]
fn published_schemas_match_the_rust_types() {
    let write = std::env::var_os("FLARE_ACCESS_WRITE_SCHEMAS").is_some();
    for (file, schema) in expected() {
        let path = schemas_dir().join(file);
        if write {
            let mut text = serde_json::to_string_pretty(&schema).expect("schema serializes");
            text.push('\n');
            fs::write(&path, text).expect("write schema");
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}; regenerate the schemas", path.display()));
        let published: Value = serde_json::from_str(&text).expect("published schema is JSON");
        assert_eq!(
            published, schema,
            "{file} differs from the Rust types; regenerate with FLARE_ACCESS_WRITE_SCHEMAS=1"
        );
    }
}

#[test]
fn schemas_carry_the_wire_field_names() {
    let documents: std::collections::BTreeMap<_, _> = expected().into_iter().collect();
    let properties = |file: &str| documents[file]["properties"].clone();
    assert!(properties("access_manifest.v1.schema.json")["bootstrapId"].is_object());
    assert!(properties("release_manifest.v1.schema.json")["releases"].is_object());
    assert!(properties("keycert.v1.schema.json")["keyid"].is_object());
    assert!(properties("keystate.v1.schema.json")["revokedKeyIds"].is_object());
}
