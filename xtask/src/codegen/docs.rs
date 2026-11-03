use anyhow::{Result, bail};
use serde_json::Value;
use std::path::Path;

use crate::{
    GeneratedTextTarget, child_arr, load_expanded_client_spec, pascal_case,
    single_trailing_newline, str_field, upsert_text_file,
};

pub(crate) fn emit_doc_files(root: &Path, check: bool) -> Result<()> {
    let spec = load_expanded_client_spec(root)?;
    let mut drifted = Vec::new();
    for target in doc_targets(root, &spec) {
        let body = single_trailing_newline(&target.body);
        upsert_text_file(&target.path, &body, check, &mut drifted)?;
    }
    if !drifted.is_empty() {
        let details = drifted.join("\n  - ");
        bail!("Rust-owned SDK documentation output drifted:\n  - {details}");
    }
    if !check {
        println!("Rust-owned SDK documentation artifacts generated");
    }
    Ok(())
}

fn doc_targets(root: &Path, spec: &Value) -> Vec<GeneratedTextTarget> {
    vec![
        GeneratedTextTarget {
            path: root.join("sdk-spec/GENERATED.md"),
            body: emit_generated_contract_index(spec),
        },
        GeneratedTextTarget {
            path: root.join("docs/client-api-reference.md"),
            body: emit_client_api_reference(spec),
        },
        GeneratedTextTarget {
            path: root.join("docs/client-model-reference.md"),
            body: emit_client_model_reference(spec),
        },
        GeneratedTextTarget {
            path: root.join("docs/events.md"),
            body: emit_event_reference(spec),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-flutter-sdk/README.md"),
            body: emit_package_readme("Flare Core Flutter SDK", spec, "dart"),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-android-sdk/README.md"),
            body: emit_package_readme("Flare Core Android SDK", spec, "android"),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-apple-sdk/README.md"),
            body: emit_package_readme("Flare Core Apple SDK", spec, "ios"),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-harmony-arkts-sdk/README.md"),
            body: emit_package_readme("Flare Core HarmonyOS ArkTS SDK", spec, "arkts"),
        },
        GeneratedTextTarget {
            path: root.join("packages/flare-core-harmony-cangjie-sdk/README.md"),
            body: emit_package_readme("Flare Core HarmonyOS Cangjie SDK", spec, "cangjie"),
        },
        // TypeScript 包的 README **刻意不生成**。
        //
        // 其余五个平台包的 README 都是这里产出的 9 行 stub，唯独 `@flare-im/sdk` 这个
        // 是真正发到 npm 的包（package.json 的 `files` 里就带着 README.md），它的
        // README 就是 npm 上的门面页。那份已经被写成 85 行的正式文档（安装、
        // 「本包发的是 TS 源码不是编译产物」的告警、按 runtime 选入口的表、
        // peer dependency 说明），还配了 README.zh-CN.md——符合本项目「英文主 + zh 伴随」
        // 的文档标准。
        //
        // 生成器一跑就会把这 85 行换成那 9 行 stub。此前 docs-check 一直报它漂移，
        // 而这道检查既不在 xtask verify 里也不在 CI 里，所以没人撞上；一旦有人照提示
        // 跑 `xtask docs`，npm 页面就被降级成一句话。
    ]
}

fn generated_header() -> &'static str {
    "Generated from split sdk-spec files"
}

fn markdown_cell(value: &str) -> String {
    value.replace('\n', " ").replace('|', "\\|")
}

fn emit_generated_contract_index(spec: &Value) -> String {
    let mut lines = vec![
        "# Generated Contract Index".to_string(),
        String::new(),
        format!("> {}", generated_header()),
        String::new(),
        format!("- API version: `{}`", str_field(spec, "apiVersion")),
        format!(
            "- FFI contract: `{}`",
            str_field(spec, "ffiContractVersion")
        ),
        format!("- Core source: `{}`", str_field(spec, "sourceOfTruth")),
        String::new(),
        "## Modules".to_string(),
        String::new(),
        "| Module | Facade | Methods |".to_string(),
        "|--------|--------|---------|".to_string(),
    ];
    for module in child_arr(spec, "modules") {
        let methods = child_arr(module, "methods")
            .iter()
            .map(|method| format!("`{}`", str_field(method, "name")))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "| `{}` | `{}` | {} |",
            str_field(module, "key"),
            str_field(module, "facade"),
            methods
        ));
    }
    lines.extend([
        String::new(),
        "## Platforms".to_string(),
        String::new(),
        "| Key | Package | Status | Async model |".to_string(),
        "|-----|---------|--------|-------------|".to_string(),
    ]);
    for platform in child_arr(spec, "platforms") {
        lines.push(format!(
            "| `{}` | `{}` | `{}` | {} |",
            str_field(platform, "key"),
            str_field(platform, "packagePath"),
            str_field(platform, "status"),
            markdown_cell(str_field(platform, "asyncModel"))
        ));
    }
    lines.join("\n")
}

fn emit_client_api_reference(spec: &Value) -> String {
    let mut lines = vec![
        "# Client API Reference".to_string(),
        String::new(),
        format!("> {}", generated_header()),
        String::new(),
        "All platform SDKs expose the same canonical modules and method names. Platform idioms may add wrappers, but the canonical names stay available.".to_string(),
        String::new(),
    ];
    for module in child_arr(spec, "modules") {
        lines.extend([
            format!("## {}", str_field(module, "facade")),
            String::new(),
            markdown_cell(str_field(module, "description")),
            String::new(),
            "| Method | Request | Response | Transport | C API |".to_string(),
            "|--------|---------|----------|-----------|-------|".to_string(),
        ]);
        for method in child_arr(module, "methods") {
            lines.push(format!(
                "| `{}` | `{}` | `{}` | `{}` | `{}` |",
                str_field(method, "name"),
                str_field(method, "request"),
                str_field(method, "response"),
                str_field(method, "transport"),
                str_field(method, "cApi")
            ));
        }
        lines.push(String::new());
    }
    lines.join("\n")
}

fn emit_client_model_reference(spec: &Value) -> String {
    let mut lines = vec![
        "# Client Model Reference".to_string(),
        String::new(),
        format!("> {}", generated_header()),
        String::new(),
        "Models are generated from `sdk-spec/models/*.json` and mirror stable message/conversation fields from `flare-im-core-sdk`.".to_string(),
        "Field names shown here are SDK-facing lowerCamelCase names; `wireName` records the current core JSON field.".to_string(),
        String::new(),
    ];
    for group in child_arr(spec, "modelGroups") {
        lines.extend([
            format!("## {}", pascal_case(str_field(group, "group"))),
            String::new(),
            markdown_cell(str_field(group, "description")),
            String::new(),
        ]);
        if !child_arr(group, "enums").is_empty() {
            lines.extend(["### Enums".to_string(), String::new()]);
            for enum_value in child_arr(group, "enums") {
                let values = child_arr(enum_value, "values")
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|value| format!("`{value}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.extend([
                    format!(
                        "- `{}`: {} Values: {}",
                        str_field(enum_value, "name"),
                        markdown_cell(str_field(enum_value, "description")),
                        values
                    ),
                    String::new(),
                ]);
            }
        }
        for model in child_arr(group, "models") {
            lines.extend([
                format!("### {}", str_field(model, "name")),
                String::new(),
                markdown_cell(str_field(model, "description")),
                String::new(),
                "| Field | Wire name | Type | Required | Description |".to_string(),
                "|-------|-----------|------|----------|-------------|".to_string(),
            ]);
            for field in child_arr(model, "fields") {
                let required = if field
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
                {
                    "yes"
                } else {
                    "no"
                };
                lines.push(format!(
                    "| `{}` | `{}` | `{}` | {} | {} |",
                    str_field(field, "name"),
                    str_field(field, "wireName"),
                    str_field(field, "type"),
                    required,
                    markdown_cell(str_field(field, "description"))
                ));
            }
            lines.push(String::new());
        }
    }
    lines.join("\n")
}

fn emit_event_reference(spec: &Value) -> String {
    let mut lines = vec![
        "# Events".to_string(),
        String::new(),
        format!("> {}", generated_header()),
        String::new(),
        "SDK events have two layers:".to_string(),
        String::new(),
        "- `subscribeEvents` is the canonical bridge-level event stream.".to_string(),
        "- High-level `on*` listener methods are local adapter registrations generated for every platform SDK.".to_string(),
        String::new(),
        "Method return values remain the primary success/failure contract for commands such as `init`, `login`, and `sendMessage`. Events are used for runtime notifications, UI state, diagnostics, and async progress.".to_string(),
        String::new(),
        "## Listener Methods".to_string(),
        String::new(),
        "| Method | Domain | Event name | Payload | Description |".to_string(),
        "|--------|--------|------------|---------|-------------|".to_string(),
    ];
    for listener in child_arr(spec, "listeners") {
        lines.push(format!(
            "| `{}` | `{}` | `{}` | `{}` | {} |",
            str_field(listener, "name"),
            str_field(listener, "kind"),
            str_field(listener, "eventName"),
            str_field(listener, "payload"),
            markdown_cell(str_field(listener, "description"))
        ));
    }
    lines.extend([
        String::new(),
        "## Event Domains".to_string(),
        String::new(),
        "| Domain | Names |".to_string(),
        "|--------|-------|".to_string(),
    ]);
    for event in child_arr(spec, "events") {
        let names = child_arr(event, "names")
            .iter()
            .filter_map(Value::as_str)
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("| `{}` | {} |", str_field(event, "type"), names));
    }
    lines.extend([
        String::new(),
        "Platform adapters should dispatch UI-facing callbacks on the platform's documented application/main context when required, while keeping native bridge callbacks non-blocking.".to_string(),
    ]);
    lines.join("\n")
}

fn emit_package_readme(title: &str, spec: &Value, platform_key: &str) -> String {
    let platform = child_arr(spec, "platforms")
        .iter()
        .find(|platform| str_field(platform, "key") == platform_key)
        .unwrap_or(&Value::Null);
    [
        format!("# {title}"),
        String::new(),
        "This package is a typed adapter over `flare-core-sdk-engine/bindings/c`.".to_string(),
        String::new(),
        format!("- Status: `{}`", str_field(platform, "status")),
        format!("- Async model: {}", str_field(platform, "asyncModel")),
        format!("- FFI contract: `{}`", str_field(spec, "ffiContractVersion")),
        String::new(),
        "Do not add IM business logic here. Add behavior to `flare-im-core-sdk`, expose it through `bindings/c`, then update `sdk-spec/manifest.json`.".to_string(),
    ]
    .join("\n")
}
