//! 归一化入口：Markdown / HTML / 已有 doc JSON → 校验 + 派生字段。

use serde_json::Value;

use super::RichDocV2Error;
use super::extract::{RichDocDerived, derive_from_value};
use super::from_html::html_fragment_to_doc_value;
use super::from_markdown::markdown_to_doc_value;
use super::validate::{validate_doc_json, validate_source_payload_bytes};

pub const CONTENT_SCHEMA_RICH_DOC: &str = "rich_doc";
pub const INPUT_FORMAT_MARKDOWN: &str = "markdown";
pub const INPUT_FORMAT_HTML: &str = "html";

/// Markdown → RichDoc v2 → 校验 → 派生 `plain_text` / `search_text` / `render_hints`。
pub fn normalize_from_markdown(md: &str) -> Result<NormalizeOutput, RichDocV2Error> {
    normalize_from_markdown_with_options(md, NormalizeOptions::default())
}

pub fn normalize_from_markdown_with_options(
    md: &str,
    options: NormalizeOptions,
) -> Result<NormalizeOutput, RichDocV2Error> {
    validate_source_payload_bytes(md)?;
    let doc = markdown_to_doc_value(md)?;
    finalize_doc_value(doc, Some(INPUT_FORMAT_MARKDOWN), md, options)
}

/// HTML 片段 → RichDoc v2 → …（需 `rich-doc-html` feature）。
pub fn normalize_from_html(html: &str) -> Result<NormalizeOutput, RichDocV2Error> {
    normalize_from_html_with_options(html, NormalizeOptions::default())
}

pub fn normalize_from_html_with_options(
    html: &str,
    options: NormalizeOptions,
) -> Result<NormalizeOutput, RichDocV2Error> {
    validate_source_payload_bytes(html)?;
    let doc = html_fragment_to_doc_value(html)?;
    finalize_doc_value(doc, Some(INPUT_FORMAT_HTML), html, options)
}

/// 客户端已持有权威 doc JSON（例如自研编辑器直出）时：仅校验 + 派生。
pub fn normalize_from_doc_json(doc_json: &str) -> Result<NormalizeOutput, RichDocV2Error> {
    validate_doc_json(doc_json)?;
    let v: Value = serde_json::from_slice(doc_json.as_bytes())
        .map_err(|e| RichDocV2Error::InvalidJson(e.to_string()))?;
    let derived = derive_from_value(&v)?;
    Ok(NormalizeOutput::from_parts(
        doc_json.to_string(),
        derived,
        None,
        None,
    ))
}

fn finalize_doc_value(
    doc: Value,
    input_format: Option<&str>,
    source_snapshot: &str,
    options: NormalizeOptions,
) -> Result<NormalizeOutput, RichDocV2Error> {
    let doc_json = serde_json::to_string(&doc)
        .map_err(|e| RichDocV2Error::InvalidStructure(format!("serialize doc: {e}")))?;
    validate_doc_json(&doc_json)?;
    let derived = derive_from_value(&doc)?;
    let snap_key = input_format.unwrap_or("raw");
    let source_payload = if options.include_source_payload {
        Some((snap_key.to_string(), source_snapshot.to_string()))
    } else {
        None
    };
    Ok(NormalizeOutput::from_parts(
        doc_json,
        derived,
        input_format.map(String::from),
        source_payload,
    ))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NormalizeOptions {
    pub include_source_payload: bool,
}

/// 与 `RichTextContent` 对齐的归一化结果（Rust / Tauri 侧 snake_case）。
#[derive(Debug, Clone)]
pub struct NormalizeOutput {
    pub doc_json: String,
    pub content_schema: String,
    pub version: u32,
    pub plain_text: String,
    pub search_text: String,
    /// 渲染提示对象；落库时写入 `render_hints_json`。
    pub render_hints: Value,
    pub input_format: Option<String>,
    /// 可选：`source_payload` 单键快照（如 markdown / html）。
    pub source_payload: Option<(String, String)>,
}

impl NormalizeOutput {
    fn from_parts(
        doc_json: String,
        derived: RichDocDerived,
        input_format: Option<String>,
        source_payload: Option<(String, String)>,
    ) -> Self {
        Self {
            doc_json,
            content_schema: CONTENT_SCHEMA_RICH_DOC.into(),
            version: 2,
            plain_text: derived.plain_text,
            search_text: derived.search_text,
            render_hints: derived.render_hints,
            input_format,
            source_payload,
        }
    }

    pub fn render_hints_json_string(&self) -> String {
        serde_json::to_string(&self.render_hints).unwrap_or_else(|_| "{}".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_normalization_omits_source_payload_by_default() {
        let out = normalize_from_markdown("hello").expect("normalize markdown");

        assert!(out.source_payload.is_none());
    }

    #[test]
    fn markdown_normalization_rejects_oversized_source() {
        let markdown = "a".repeat(256 * 1024 + 1);

        let err = normalize_from_markdown(&markdown).expect_err("oversized source must fail");

        assert!(err.to_string().contains("source"));
    }

    #[cfg(not(feature = "rich-doc-html"))]
    #[test]
    fn html_normalization_reports_unavailable_without_feature() {
        let err = normalize_from_html("<p>Hello</p>").expect_err("html feature is disabled");

        assert!(matches!(err, RichDocV2Error::HtmlUnavailable));
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "rich-doc-html"))]
    #[test]
    fn html_normalization_maps_basic_markup_when_feature_enabled() {
        let out =
            normalize_from_html("<p>Hello <strong>Flare</strong></p>").expect("normalize html");

        assert_eq!(out.input_format.as_deref(), Some(INPUT_FORMAT_HTML));
        assert!(out.plain_text.contains("Flare"));
        assert!(out.doc_json.contains("\"paragraph\""));
        assert!(out.source_payload.is_none());
    }
}
