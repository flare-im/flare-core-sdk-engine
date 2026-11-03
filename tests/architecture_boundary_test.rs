use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn rust_files_under(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = fs::read_dir(path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|v| v.to_str()).unwrap_or("");
                if matches!(name, "target" | ".git") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().and_then(|v| v.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }
    files
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut out = String::new();
    let mut pending_cfg_test = false;
    let mut in_test_module = false;
    let mut brace_depth: i32 = 0;

    for line in source.lines() {
        let trimmed = line.trim();
        if in_test_module {
            brace_depth += line.matches('{').count() as i32;
            brace_depth -= line.matches('}').count() as i32;
            if brace_depth <= 0 {
                in_test_module = false;
                brace_depth = 0;
            }
            continue;
        }

        if trimmed == "#[cfg(test)]" {
            pending_cfg_test = true;
            continue;
        }

        // 只认 `mod tests` 会漏掉任何自定义命名的测试模块（本仓常按内容命名，
        // 例如 `mod quic_port_probe_tests`），于是测试代码被当成生产代码报违规。
        // 判据应是「带 #[cfg(test)] 的任意 mod」——模块叫什么与它是不是测试无关。
        let declaration = if pending_cfg_test {
            mod_declaration(trimmed)
        } else {
            None
        };
        if let Some(declaration) = declaration {
            pending_cfg_test = false;
            // 外联的 `mod name;` 没有花括号体：模块在另一个文件里，由
            // `cfg_test_module_files` 整份排除；声明之后的行仍是本文件的生产代码。
            if declaration.outline {
                continue;
            }
            in_test_module = true;
            brace_depth += line.matches('{').count() as i32;
            brace_depth -= line.matches('}').count() as i32;
            if brace_depth <= 0 {
                brace_depth = 1;
            }
            continue;
        }

        if !trimmed.starts_with("#[") {
            pending_cfg_test = false;
        }
        out.push_str(line);
        out.push('\n');
    }

    out
}

/// 一行开头的模块声明 `mod name;` 或 `mod name {`，可见性任意
/// （`pub`、`pub(crate)`、`pub(super)`、`pub(in path)`）。
struct ModDeclaration<'a> {
    name: &'a str,
    /// 以 `;` 结尾：模块体在另一个文件里。
    outline: bool,
}

fn mod_declaration(trimmed: &str) -> Option<ModDeclaration<'_>> {
    let mut rest = trimmed;
    if let Some(after_pub) = rest.strip_prefix("pub") {
        rest = if let Some(scope) = after_pub.trim_start().strip_prefix('(') {
            &scope[scope.find(')')? + 1..]
        } else if after_pub.starts_with(char::is_whitespace) {
            after_pub
        } else {
            return None;
        };
    }
    let after_mod = rest.trim_start().strip_prefix("mod")?;
    if !after_mod.starts_with(char::is_whitespace) {
        return None;
    }
    let after_mod = after_mod.trim_start();
    let name = take_ident(after_mod)?;
    let outline = after_mod[name.len()..].trim_start().starts_with(';');
    Some(ModDeclaration { name, outline })
}

/// 文件里的外联模块声明 `mod name;`，连同紧挨着它的 `#[cfg(test)]` 与 `#[path = "..."]`。
struct OutlineModule {
    line: usize,
    name: String,
    path: Option<String>,
    cfg_test: bool,
}

fn outline_modules(source: &str) -> Vec<OutlineModule> {
    let mut modules = Vec::new();
    let mut cfg_test = false;
    let mut path = None;
    for (idx, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed == "#[cfg(test)]" {
            cfg_test = true;
            continue;
        }
        if let Some(attribute) = trimmed.strip_prefix("#[") {
            let value = attribute
                .strip_suffix(']')
                .and_then(|attribute| attribute.strip_prefix("path"))
                .and_then(|rest| rest.trim_start().strip_prefix('='));
            if let Some(value) = value {
                path = Some(value.trim().trim_matches('"').to_string());
            }
            continue;
        }
        if let Some(declaration) = mod_declaration(trimmed)
            && declaration.outline
        {
            modules.push(OutlineModule {
                line: idx + 1,
                name: declaration.name.to_string(),
                path: path.take(),
                cfg_test,
            });
        }
        cfg_test = false;
        path = None;
    }
    modules
}

/// 外联模块的源文件：`#[path]` 相对声明文件所在目录；否则 `mod.rs` / `lib.rs` / `main.rs`
/// 的子模块与它同目录，`foo.rs` 的子模块在 `foo/` 下，各自是 `name.rs` 或 `name/mod.rs`。
fn outline_module_file(declaring_file: &Path, module: &OutlineModule) -> PathBuf {
    let dir = declaring_file
        .parent()
        .expect("source file has a directory");
    let candidates = match &module.path {
        Some(path) => vec![dir.join(path)],
        None => {
            let stem = declaring_file
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or_default();
            let base = if matches!(stem, "mod" | "lib" | "main") {
                dir.to_path_buf()
            } else {
                dir.join(stem)
            };
            vec![
                base.join(format!("{}.rs", module.name)),
                base.join(&module.name).join("mod.rs"),
            ]
        }
    };
    let found = candidates.iter().find(|candidate| candidate.is_file());
    let found = found.unwrap_or_else(|| {
        panic!(
            "{}:{} declares mod {} but none of {:?} exists",
            declaring_file.display(),
            module.line,
            module.name,
            candidates
        )
    });
    fs::canonicalize(found).expect("canonicalize module file")
}

/// 整份只在测试里编译的源文件：`#[cfg(test)]` 修饰的外联模块对应的文件，以及这些文件
/// 再声明的子模块（测试模块下面的一切都只在测试里编译，子模块不必再写 `#[cfg(test)]`）。
/// 返回规范化路径。
fn cfg_test_module_files(files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut pending = Vec::new();
    for file in files {
        let source = fs::read_to_string(file).expect("read Rust source");
        for module in outline_modules(&source) {
            if module.cfg_test {
                pending.push(outline_module_file(file, &module));
            }
        }
    }

    let mut test_files = BTreeSet::new();
    while let Some(file) = pending.pop() {
        if !test_files.insert(file.clone()) {
            continue;
        }
        let source = fs::read_to_string(&file).expect("read test module source");
        for module in outline_modules(&source) {
            pending.push(outline_module_file(&file, &module));
        }
    }
    test_files
}

/// 目录下的生产源码：去掉整份只在测试里编译的文件，其余文件剥掉内联测试模块与行注释。
fn production_sources_under(root: &Path) -> Vec<(PathBuf, String)> {
    let files = rust_files_under(root);
    let test_files = cfg_test_module_files(&files);
    files
        .into_iter()
        .filter(|file| {
            let canonical = fs::canonicalize(file).expect("canonicalize source file");
            !test_files.contains(&canonical)
        })
        .map(|file| {
            let source = fs::read_to_string(&file).expect("read Rust source");
            let production = strip_line_comments(&strip_cfg_test_modules(&source));
            (file, production)
        })
        .collect()
}

fn strip_line_comments(source: &str) -> String {
    let mut out = String::new();
    for line in source.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            out.push('\n');
            continue;
        }
        let code = line.split_once("//").map(|(code, _)| code).unwrap_or(line);
        out.push_str(code);
        out.push('\n');
    }
    out
}

fn line_number_at(source: &str, byte_idx: usize) -> usize {
    source[..byte_idx].bytes().filter(|b| *b == b'\n').count() + 1
}

fn line_snippet(source: &str, line: usize) -> String {
    source
        .lines()
        .nth(line.saturating_sub(1))
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn is_ident_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn take_ident(input: &str) -> Option<&str> {
    let input = input.trim_start();
    let mut end = 0;
    for (idx, ch) in input.char_indices() {
        if idx == 0 && !(ch == '_' || ch.is_ascii_alphabetic()) {
            return None;
        }
        if !is_ident_char(ch) {
            break;
        }
        end = idx + ch.len_utf8();
    }
    (end > 0).then_some(&input[..end])
}

fn find_matching_brace(input: &str) -> Option<usize> {
    let mut depth = 0usize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_braced_body_after(source: &str, needle: &str) -> String {
    let start = source
        .find(needle)
        .unwrap_or_else(|| panic!("{needle} not found"));
    let after_start = &source[start..];
    let brace_start = after_start
        .find('{')
        .unwrap_or_else(|| panic!("{needle} body start not found"));
    let body_start = start + brace_start;
    let body = &source[body_start..];
    let body_end =
        find_matching_brace(body).unwrap_or_else(|| panic!("{needle} body end not found"));
    body[..=body_end].to_string()
}

fn toml_section<'a>(source: &'a str, header: &str) -> &'a str {
    let Some(start) = source.find(header) else {
        return "";
    };
    let rest = &source[start + header.len()..];
    let end = rest
        .find("\n[")
        .map(|idx| start + header.len() + idx)
        .unwrap_or(source.len());
    &source[start + header.len()..end]
}

fn split_top_level_group_items(input: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                items.push(&input[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    items.push(&input[start..]);
    items
}

fn core_sdk_roots_after_path(input: &str) -> Vec<String> {
    let input = input.trim_start();
    let Some(group) = input.strip_prefix('{') else {
        return vec![take_ident(input).unwrap_or("<crate-root>").to_string()];
    };

    let Some(end) = find_matching_brace(input) else {
        return vec!["<unterminated-group>".to_string()];
    };
    let group = &group[..end.saturating_sub(1)];
    split_top_level_group_items(group)
        .into_iter()
        .filter_map(|item| {
            let item = item.trim();
            if item.is_empty() || item == "self" {
                return None;
            }
            Some(take_ident(item).unwrap_or("<crate-root>").to_string())
        })
        .collect()
}

#[derive(Debug)]
struct CoreSdkReference {
    line: usize,
    root: String,
    snippet: String,
}

#[derive(Debug)]
struct CrateReference {
    line: usize,
    root: String,
    snippet: String,
}

fn collect_core_sdk_references(source: &str) -> Vec<CoreSdkReference> {
    const PREFIX: &str = "flare_im_core_sdk::";

    let mut refs = Vec::new();
    let mut search_from = 0;
    while let Some(relative_idx) = source[search_from..].find(PREFIX) {
        let start = search_from + relative_idx;
        let after_prefix = start + PREFIX.len();
        let line = line_number_at(source, start);
        let snippet = line_snippet(source, line);
        for root in core_sdk_roots_after_path(&source[after_prefix..]) {
            refs.push(CoreSdkReference {
                line,
                root,
                snippet: snippet.clone(),
            });
        }
        search_from = after_prefix;
    }
    refs
}

fn collect_crate_references(source: &str) -> Vec<CrateReference> {
    const PREFIX: &str = "crate::";

    let mut refs = Vec::new();
    let mut search_from = 0;
    while let Some(relative_idx) = source[search_from..].find(PREFIX) {
        let start = search_from + relative_idx;
        let after_prefix = start + PREFIX.len();
        let line = line_number_at(source, start);
        let snippet = line_snippet(source, line);
        for root in core_sdk_roots_after_path(&source[after_prefix..]) {
            refs.push(CrateReference {
                line,
                root,
                snippet: snippet.clone(),
            });
        }
        search_from = after_prefix;
    }
    refs
}

#[test]
fn domain_source_does_not_depend_on_upper_layers() {
    let domain_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/domain");
    let forbidden_roots = [
        "application",
        "client",
        "core",
        "extension",
        "infrastructure",
        "platform",
        "runtime",
        "spi",
    ];
    let mut violations = Vec::new();

    for file in rust_files_under(&domain_src) {
        let source = fs::read_to_string(&file).expect("read domain source");
        let production_source = strip_line_comments(&strip_cfg_test_modules(&source));
        for reference in collect_crate_references(&production_source) {
            if forbidden_roots.contains(&reference.root.as_str()) {
                violations.push(format!(
                    "{}:{} uses crate::{} from the domain layer: {}",
                    file.display(),
                    reference.line,
                    reference.root,
                    reference.snippet
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "domain layer must not depend on application/runtime/platform/infrastructure boundaries:\n{}",
        violations.join("\n")
    );
}

#[test]
fn application_source_does_not_depend_on_runtime_layer() {
    let application_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/application");
    let mut violations = Vec::new();

    for file in rust_files_under(&application_src) {
        let source = fs::read_to_string(&file).expect("read application source");
        let production_source = strip_line_comments(&strip_cfg_test_modules(&source));
        for reference in collect_crate_references(&production_source) {
            if reference.root == "runtime" {
                violations.push(format!(
                    "{}:{} uses crate::runtime from the application layer: {}",
                    file.display(),
                    reference.line,
                    reference.snippet
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "application layer must not depend on the runtime layer:\n{}",
        violations.join("\n")
    );
}

#[test]
fn platform_bindings_do_not_construct_resync_markers_directly() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let platform_binding_roots = [
        "bindings/c/src",
        "bindings/tauri/src",
        "bindings/uniffi/src",
        "bindings/wasm/src",
    ];
    let mut violations = Vec::new();

    for rel in platform_binding_roots {
        for file in rust_files_under(&manifest.join(rel)) {
            let source = fs::read_to_string(&file).expect("read binding source");
            let production_source = strip_line_comments(&strip_cfg_test_modules(&source));
            if production_source.contains("SyncNotify::ResyncNeeded") {
                violations.push(format!(
                    "{} constructs SyncNotify::ResyncNeeded outside bindings/shared",
                    file.display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "platform bindings must use bindings/shared resync marker helpers:\n{}",
        violations.join("\n")
    );
}

#[test]
fn core_source_does_not_depend_on_business_or_plugin_crates() {
    let core_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let forbidden = [
        "flare_social",
        "flare-social",
        "flare_sdk_plugin",
        "flare-sdk-plugin",
    ];
    let mut violations = Vec::new();

    for file in rust_files_under(&core_src) {
        let source = fs::read_to_string(&file).expect("read core source");
        for needle in forbidden {
            if source.contains(needle) {
                violations.push(format!("{} contains {needle}", file.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "core must not depend on business/plugin crates:\n{}",
        violations.join("\n")
    );
}

#[test]
fn social_production_code_uses_spi_for_core_extension_boundaries() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest.parent().expect("flare-im root");
    let Some(social_src) = [
        root.join("flare-social-sdk/src"),
        root.join("flare-social-server/flare-social-sdk/src"),
    ]
    .into_iter()
    .find(|src| src.is_dir())
    else {
        return;
    };

    let violations: Vec<_> = core_sdk_facade_violations(&social_src)
        .into_iter()
        .map(|violation| {
            format!(
                "{}:{} uses flare_im_core_sdk::{} outside the extension facade: {}",
                violation.file.display(),
                violation.line,
                violation.root,
                violation.snippet
            )
        })
        .collect();

    assert!(
        violations.is_empty(),
        "social production code may only use flare_im_core_sdk::{{spi, model, prelude}}:\n{}",
        violations.join("\n")
    );
}

/// 扩展门面：核心之外的生产代码只经这几个根用核心 SDK。
const CORE_SDK_FACADE_ROOTS: [&str; 3] = ["spi", "model", "prelude"];

struct FacadeViolation {
    file: PathBuf,
    line: usize,
    root: String,
    snippet: String,
}

fn core_sdk_facade_violations(src: &Path) -> Vec<FacadeViolation> {
    let mut violations = Vec::new();
    for (file, production_source) in production_sources_under(src) {
        for reference in collect_core_sdk_references(&production_source) {
            if !CORE_SDK_FACADE_ROOTS.contains(&reference.root.as_str()) {
                violations.push(FacadeViolation {
                    file: file.clone(),
                    line: reference.line,
                    root: reference.root,
                    snippet: reference.snippet,
                });
            }
        }
    }
    violations
}

/// 临时源码树：给门面检查喂固定的文件布局，结束时删掉。
struct SourceTree {
    root: PathBuf,
}

impl SourceTree {
    fn new(name: &str, files: &[(&str, &str)]) -> Self {
        let root =
            std::env::temp_dir().join(format!("core-sdk-boundary-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        for (path, source) in files {
            let path = root.join(path);
            fs::create_dir_all(path.parent().expect("fixture file has a directory"))
                .expect("create fixture directory");
            fs::write(&path, source).expect("write fixture file");
        }
        Self { root }
    }

    /// 门面检查报出的 (相对路径, 根)，按路径排序。
    fn violations(&self) -> Vec<(String, String)> {
        let mut found: Vec<_> = core_sdk_facade_violations(&self.root)
            .into_iter()
            .map(|violation| {
                let file = violation
                    .file
                    .strip_prefix(&self.root)
                    .expect("violation inside the fixture")
                    .to_string_lossy()
                    .replace('\\', "/");
                (file, violation.root)
            })
            .collect();
        found.sort();
        found
    }
}

impl Drop for SourceTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(file, root)| (file.to_string(), root.to_string()))
        .collect()
}

#[test]
fn facade_check_excludes_cfg_test_outline_modules_as_whole_files() {
    let tree = SourceTree::new(
        "cfg-test-outline",
        &[
            (
                "lib.rs",
                "pub mod names;\npub mod store;\n#[cfg(test)]\npub(crate) mod test_support;\n",
            ),
            (
                "names.rs",
                "pub fn name() {}\n\n#[cfg(test)]\nmod cases;\n\n#[cfg(test)]\n#[path = \"names_tests.rs\"]\nmod tests;\n",
            ),
            (
                "names/cases.rs",
                "use flare_im_core_sdk::model::Elem;\nuse flare_im_core_sdk::Result;\n",
            ),
            (
                "names_tests.rs",
                "use flare_im_core_sdk::event::EventBus;\n",
            ),
            ("store/mod.rs", "#[cfg(test)]\nmod golden_tests;\n"),
            (
                "store/golden_tests.rs",
                "use flare_im_core_sdk::content::ContentBuilder;\n",
            ),
            (
                "test_support/mod.rs",
                "mod fixtures;\nuse flare_im_core_sdk::Result;\n",
            ),
            (
                "test_support/fixtures.rs",
                "use flare_im_core_sdk::client::ImClient;\n",
            ),
        ],
    );

    assert_eq!(tree.violations(), pairs(&[]));
}

#[test]
fn facade_check_still_checks_outline_modules_without_cfg_test() {
    let tree = SourceTree::new(
        "plain-outline",
        &[
            (
                "lib.rs",
                "pub mod geo;\n#[cfg(feature = \"extra\")]\nmod extra;\n",
            ),
            (
                "geo/mod.rs",
                "#[cfg(test)]\nmod golden_tests;\nmod location;\n\npub async fn wait() {\n    flare_im_core_sdk::delay(LIMIT).await;\n}\n",
            ),
            (
                "geo/golden_tests.rs",
                "use flare_im_core_sdk::content::ContentBuilder;\n",
            ),
            (
                "geo/location.rs",
                "use flare_im_core_sdk::event::EventBus;\nuse flare_im_core_sdk::spi::delay;\n",
            ),
            ("extra.rs", "use flare_im_core_sdk::client::ImClient;\n"),
        ],
    );

    assert_eq!(
        tree.violations(),
        pairs(&[
            ("extra.rs", "client"),
            ("geo/location.rs", "event"),
            ("geo/mod.rs", "delay"),
        ])
    );
}

#[test]
fn facade_check_still_strips_inline_cfg_test_modules() {
    let source = "pub fn wait() {}\n\n#[cfg(test)]\nmod tests {\n    use flare_im_core_sdk::event::EventBus;\n\n    fn helper() {\n        let _ = 1;\n    }\n}\n\npub fn after() {}\n";
    assert_eq!(
        strip_cfg_test_modules(source),
        "pub fn wait() {}\n\n\npub fn after() {}\n"
    );

    let tree = SourceTree::new(
        "cfg-test-inline",
        &[(
            "lib.rs",
            "use flare_im_core_sdk::spi::delay;\n\n#[cfg(test)]\nmod tests {\n    use flare_im_core_sdk::event::EventBus;\n}\n\npub fn connect() {\n    let _ = flare_im_core_sdk::client::ImClient::new();\n}\n",
        )],
    );

    assert_eq!(tree.violations(), pairs(&[("lib.rs", "client")]));
}

#[test]
fn native_release_profiles_keep_ffi_panic_guards_effective() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = fs::read_to_string(manifest.join("Cargo.toml")).expect("read Cargo.toml");
    let release = toml_section(&cargo_toml, "[profile.release]");
    assert!(
        release.contains("panic = \"unwind\""),
        "native release must use panic=unwind so C ABI catch_ffi_* guards can catch SDK panics"
    );
    assert!(
        !release.contains("panic = \"abort\""),
        "native release must not abort across FFI panic boundaries"
    );
    let release_mobile = toml_section(&cargo_toml, "[profile.release-mobile]");
    assert!(
        !release_mobile.contains("panic = \"abort\""),
        "release-mobile inherits the native FFI profile and must not override panic=abort"
    );

    // 本仓自己的 .cargo/config.toml 必须存在——它是本仓对 wasm 构建的承诺。
    // 伞仓（上一级）那份属于另一个仓库：多仓开发布局下能看到，单仓 checkout（CI）
    // 里不存在，此时跳过而非失败——否则「布局差异」会被误报成「架构违规」。
    let own_config = manifest.join(".cargo/config.toml");
    let workspace_config = manifest
        .parent()
        .expect("flare-im root")
        .join(".cargo/config.toml");
    let config_paths: Vec<_> = std::iter::once(own_config)
        .chain(workspace_config.exists().then_some(workspace_config))
        .collect();

    for config_path in config_paths {
        let config = fs::read_to_string(&config_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", config_path.display()));
        let wasm = toml_section(&config, "[target.wasm32-unknown-unknown]");
        assert!(
            wasm.contains("\"panic=abort\""),
            "{} must keep wasm panic abort explicit instead of relying on the native release profile",
            config_path.display()
        );
    }
}

#[test]
fn sdk_crates_do_not_enable_tokio_full() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = [
        manifest.join("Cargo.toml"),
        manifest.join("storage/sqlite/Cargo.toml"),
        manifest.join("bindings/shared/Cargo.toml"),
        manifest.join("bindings/wasm/Cargo.toml"),
        manifest.join("bindings/c/Cargo.toml"),
        manifest.join("bindings/tauri/Cargo.toml"),
    ];
    let mut violations = Vec::new();

    for file in files {
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
        for (idx, line) in source.lines().enumerate() {
            let compact = line.replace([' ', '\t'], "");
            if compact.starts_with("tokio=")
                && (compact.contains("features=[\"full\"]")
                    || compact.contains("\"full\"")
                    || compact.contains("features=['full']"))
            {
                violations.push(format!(
                    "{}:{} enables tokio full: {}",
                    file.display(),
                    idx + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "SDK crates must keep tokio features explicit and minimal:\n{}",
        violations.join("\n")
    );
}

#[test]
fn view_refresh_worker_does_not_self_retain_view_api() {
    let source =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/client/api/view.rs"))
            .expect("read view api source");
    let body = extract_braced_body_after(&source, "fn spawn_refresh_worker");

    assert!(
        body.contains("Arc::downgrade(&self.inner)"),
        "ViewApi refresh worker must capture a Weak inner reference"
    );
    assert!(
        body.contains("spawn_background_task"),
        "ViewApi refresh worker must be abortable instead of fire-and-forget"
    );
    assert!(
        !body.contains("self.clone()"),
        "ViewApi refresh worker must not clone self, or the background task retains the view API forever"
    );
    assert!(
        source.contains("impl Drop for ViewApiInner") && source.contains("worker.abort()"),
        "ViewApiInner must abort its refresh worker when the API is released"
    );
}

#[test]
fn ffi_runtime_worker_threads_are_host_adaptive() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("bindings/c/src/ffi_runtime.rs"),
    )
    .expect("read FFI runtime");

    assert!(
        source.contains(".worker_threads(ffi_worker_threads())"),
        "C FFI runtime must size worker threads from host parallelism"
    );
    assert!(
        source.contains("std::thread::available_parallelism()"),
        "C FFI runtime must use available_parallelism for native host adaptation"
    );
    assert!(
        !source.contains(".worker_threads(2)"),
        "C FFI runtime must not hard-code two worker threads"
    );
}

#[test]
fn c_typed_abi_invocations_are_callback_based_and_non_blocking() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = [
        "bindings/contract/c_typed_abi.json",
        "bindings/c/src/generated/typed_abi.rs",
        "xtask/src/core_codegen.rs",
    ];
    let mut violations = Vec::new();

    for relative in files {
        let source = fs::read_to_string(manifest.join(relative)).expect("read typed ABI source");
        if source.contains("sync_bool_invoke") {
            violations.push(format!("{relative} contains sync_bool_invoke"));
        }
        if source.contains(".block_on(") {
            violations.push(format!("{relative} blocks an async typed ABI operation"));
        }
    }

    assert!(
        violations.is_empty(),
        "typed C ABI operations must use the callback bridge, never runtime.block_on:\n{}",
        violations.join("\n")
    );
}

#[test]
fn bindings_read_messages_from_json_through_the_core_readers() {
    // `IMMessage::from_json_str` / `from_json_value` and `Elem::from_json_value` are compiled in
    // the core crate so that every crate reading messages shares one copy of the (recursive)
    // reader; a `serde_json` call that reads a message elsewhere compiles another copy.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = fs::read_to_string(manifest.join("bindings/shared/src/dispatch_support.rs"))
        .expect("read dispatch_support.rs");
    let production = strip_line_comments(&strip_cfg_test_modules(&source));
    let mut violations = Vec::new();
    for (entry, reader) in [
        ("pub fn message_from_params", "IMMessage::from_json_value"),
        ("pub fn message_from_json_str", "IMMessage::from_json_str"),
        ("pub fn json_vec_message", "IMMessage::from_json_value"),
        ("pub fn built_content_from_value", "Elem::from_json_value"),
    ] {
        let body = extract_braced_body_after(&production, entry);
        if !body.contains(reader) {
            violations.push(format!("{entry} does not read through {reader}"));
        }
        // Parsing the params into a plain `Value` first is fine; reading the message is not.
        let reads_directly = [
            "IMMessage::from_json_str",
            "IMMessage::from_json_value",
            "Elem::from_json_value",
            "serde_json::from_str::<Value>",
        ]
        .iter()
        .fold(body.clone(), |rest, allowed| rest.replace(allowed, ""));
        for needle in ["serde_json::from_", "from_value(", "from_json_str("] {
            if reads_directly.contains(needle) {
                violations.push(format!("{entry} reads with {needle}"));
            }
        }
    }
    for root in ["bindings", "storage"] {
        for file in rust_files_under(&manifest.join(root)) {
            if file.components().any(|c| c.as_os_str() == "tests") {
                continue;
            }
            let source = fs::read_to_string(&file).expect("read Rust source");
            let production = strip_line_comments(&strip_cfg_test_modules(&source));
            for message_type in ["IMMessage", "Elem"] {
                for form in ["from_str", "from_slice", "from_value", "from_reader"] {
                    let needle = format!("{form}::<{message_type}>");
                    if production.contains(&needle) {
                        violations.push(format!("{} reads with {needle}", file.display()));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "read messages from JSON through IMMessage::from_json_str / from_json_value and \
         Elem::from_json_value:\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_code_does_not_use_unbounded_channels() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let roots = ["src", "bindings"];
    let forbidden = [
        "mpsc::unbounded_channel",
        "tokio::sync::mpsc::unbounded_channel",
        "UnboundedSender",
        "UnboundedReceiver",
    ];
    let mut violations = Vec::new();

    for root in roots {
        for file in rust_files_under(&manifest.join(root)) {
            if file.file_name().and_then(|name| name.to_str()) == Some("tests.rs")
                || file.components().any(|component| {
                    component.as_os_str() == "tests" || component.as_os_str() == "test"
                })
            {
                continue;
            }
            let source = fs::read_to_string(&file).expect("read Rust source");
            let production_source = strip_line_comments(&strip_cfg_test_modules(&source));
            for needle in forbidden {
                if production_source.contains(needle) {
                    violations.push(format!("{} contains {needle}", file.display()));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production async queues must be bounded for SDK memory safety:\n{}",
        violations.join("\n")
    );
}

#[test]
fn downlink_codec_uses_typed_dispatch_without_unknown_payload_guessing() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/infrastructure/protocol/codec.rs"),
    )
    .expect("read codec source");
    let impl_body = extract_braced_body_after(&source, "impl Codec for ProtobufCodec");
    let typed_decoder = extract_braced_body_after(
        &impl_body,
        "fn decode_server_payload(&self, payload_type: i32, payload: &[u8])",
    );

    assert!(
        typed_decoder.contains("ensure_payload_budget(payload)?"),
        "typed downlink decode must enforce the shared payload budget before prost decode"
    );
    assert!(
        !typed_decoder.contains("self.decode_server(payload)"),
        "typed downlink decode must not fall back to untyped guess-every-protobuf decoding"
    );
    assert!(
        typed_decoder.contains("unsupported payload type"),
        "unknown payload types must fail closed instead of guessing"
    );
}

#[test]
fn platform_event_fanout_isolation_and_unknown_event_codes_are_structural() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest.parent().expect("flare-im root");
    let client_sdk = repo_root.join("flare-core-sdk");

    let sources = [
        manifest.join("xtask/src/codegen/typescript_adapter.rs"),
        client_sdk
            .join("packages/flare-core-typescript-sdk/src/adapter/module/DefaultEventsApi.ts"),
        client_sdk
            .join("packages/flare-core-flutter-sdk/lib/src/adapter/events/default_events_api.dart"),
        manifest.join("xtask/templates/android-adapter/module/DefaultEventsApi.kt"),
        client_sdk.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultEventsApi.kt",
        ),
        manifest.join("xtask/templates/apple-adapter/module/DefaultEventsApi.swift"),
        client_sdk.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultEventsApi.swift",
        ),
    ];
    let mut violations = Vec::new();

    for file in sources {
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
        let normalized = source.replace(['\r', '\n', ' '], "");
        if normalized.contains("invalidnativeeventcode") {
            violations.push(format!(
                "{} throws or reports unknown native event codes",
                file.display()
            ));
        }
        if file.extension().and_then(|ext| ext.to_str()) == Some("kt")
            && !source.contains("else -> unknownEventFromCode(eventType, payload)")
        {
            violations.push(format!(
                "{} native event entrypoint does not preserve unknown event codes",
                file.display()
            ));
        }
    }

    let ts_generator = fs::read_to_string(manifest.join("xtask/src/codegen/typescript_adapter.rs"))
        .expect("read TypeScript generator");
    assert!(
        ts_generator.contains("this.dispatchSafely"),
        "TypeScript event generator must isolate listener failures per subscriber"
    );
    let android_template = fs::read_to_string(
        manifest.join("xtask/templates/android-adapter/module/DefaultEventsApi.kt"),
    )
    .expect("read Android event template");
    assert!(
        android_template.contains("dispatchSafely"),
        "Android event template must isolate listener failures per subscriber"
    );
    let dart_events = fs::read_to_string(
        client_sdk
            .join("packages/flare-core-flutter-sdk/lib/src/adapter/events/default_events_api.dart"),
    )
    .expect("read Flutter event API");
    assert!(
        dart_events.contains("_reportListenerError"),
        "Flutter event API must isolate listener failures per subscriber"
    );

    assert!(
        violations.is_empty(),
        "platform event adapters must preserve forward-compatible unknown events:\n{}",
        violations.join("\n")
    );
}

#[test]
fn im_session_watchers_do_not_self_retain_client_or_event_bus() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/client/im_client/session_watchers.rs"),
    )
    .expect("read session watcher source");

    assert!(
        !source.contains("let client = self.clone()"),
        "session watchers must capture WeakIMClient so release/drop can tear down SDK state"
    );
    assert!(
        !source.contains("bus.publish("),
        "session watchers must not retain EventBus sender clones; publish via current generation engine instead"
    );
    assert!(
        source.contains("let client = self.downgrade()"),
        "session watchers should use IMClient::downgrade before spawning long-lived workers"
    );
}
