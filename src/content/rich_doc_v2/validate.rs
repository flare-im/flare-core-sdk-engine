//! Rich Doc v2 结构校验（协议唯一权威入口之一；与 JSON Schema 对齐）。

use serde_json::Value;

use crate::content::url_safety::is_safe_link_href;

use super::RichDocV2Error;

const BLOCK: &[&str] = &[
    "paragraph",
    "heading",
    "quote",
    "code_block",
    "bullet_list",
    "ordered_list",
    "list_item",
    "divider",
    "custom_block",
];

const INLINE: &[&str] = &[
    "text",
    "mention",
    "emoji",
    "link",
    "inline_code",
    "hard_break",
    "custom_inline",
];

const MARK: &[&str] = &["bold", "italic", "underline", "strike", "spoiler"];

pub const MAX_RICH_DOC_SOURCE_BYTES: usize = 256 * 1024;
pub const MAX_RICH_DOC_JSON_BYTES: usize = 512 * 1024;
pub const MAX_RICH_DOC_TEXT_BYTES: usize = 128 * 1024;
pub const MAX_RICH_DOC_NODES: usize = 10_000;
pub const MAX_RICH_DOC_DEPTH: usize = 64;

struct RichDocBudget {
    nodes: usize,
}

impl RichDocBudget {
    fn new() -> Self {
        Self { nodes: 0 }
    }

    fn enter(&mut self, kind: &str, depth: usize) -> Result<(), RichDocV2Error> {
        if depth > MAX_RICH_DOC_DEPTH {
            return Err(RichDocV2Error::InvalidStructure(format!(
                "{kind} depth exceeds {MAX_RICH_DOC_DEPTH}"
            )));
        }
        self.nodes += 1;
        if self.nodes > MAX_RICH_DOC_NODES {
            return Err(RichDocV2Error::InvalidStructure(format!(
                "node count exceeds {MAX_RICH_DOC_NODES}"
            )));
        }
        Ok(())
    }
}

pub fn validate_source_payload_bytes(input: &str) -> Result<(), RichDocV2Error> {
    if input.len() > MAX_RICH_DOC_SOURCE_BYTES {
        return Err(RichDocV2Error::InvalidStructure(format!(
            "source payload exceeds {MAX_RICH_DOC_SOURCE_BYTES} bytes"
        )));
    }
    Ok(())
}

/// 校验整棵 Rich Doc JSON（根须为 `doc` + `version == 2`）。
pub fn validate_doc_json(doc_json: &str) -> Result<(), RichDocV2Error> {
    if doc_json.len() > MAX_RICH_DOC_JSON_BYTES {
        return Err(RichDocV2Error::InvalidStructure(format!(
            "doc_json exceeds {MAX_RICH_DOC_JSON_BYTES} bytes"
        )));
    }
    let v: Value = serde_json::from_slice(doc_json.as_bytes())
        .map_err(|e| RichDocV2Error::InvalidJson(e.to_string()))?;
    validate_value_as_doc(&v)
}

fn validate_value_as_doc(v: &Value) -> Result<(), RichDocV2Error> {
    let mut budget = RichDocBudget::new();
    budget.enter("doc", 0)?;
    let obj = v
        .as_object()
        .ok_or_else(|| RichDocV2Error::InvalidStructure("root must be object".into()))?;
    if obj.get("type").and_then(Value::as_str) != Some("doc") {
        return Err(RichDocV2Error::InvalidStructure(
            "root.type must be \"doc\"".into(),
        ));
    }
    if obj.get("version").and_then(Value::as_u64) != Some(2) {
        return Err(RichDocV2Error::InvalidStructure(
            "root.version must be 2".into(),
        ));
    }
    let children = obj
        .get("children")
        .and_then(Value::as_array)
        .ok_or_else(|| RichDocV2Error::InvalidStructure("doc.children must be array".into()))?;
    for c in children {
        validate_block(c, &mut budget, 1)?;
    }
    Ok(())
}

fn validate_block(
    v: &Value,
    budget: &mut RichDocBudget,
    depth: usize,
) -> Result<(), RichDocV2Error> {
    budget.enter("block", depth)?;
    let obj = v
        .as_object()
        .ok_or_else(|| RichDocV2Error::InvalidStructure("block must be object".into()))?;
    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| RichDocV2Error::InvalidStructure("block.type missing".into()))?;
    if !BLOCK.contains(&ty) {
        return Err(RichDocV2Error::InvalidStructure(format!(
            "unknown block.type: {ty}"
        )));
    }
    match ty {
        "paragraph" => {
            let ch = children_array(obj, "paragraph")?;
            for x in ch {
                validate_inline(x, budget, depth + 1)?;
            }
        }
        "heading" => {
            let level = obj
                .get("level")
                .and_then(Value::as_u64)
                .ok_or_else(|| RichDocV2Error::InvalidStructure("heading.level required".into()))?;
            if !(1..=6).contains(&level) {
                return Err(RichDocV2Error::InvalidStructure(
                    "heading.level must be 1..=6".into(),
                ));
            }
            let ch = children_array(obj, "heading")?;
            for x in ch {
                validate_inline(x, budget, depth + 1)?;
            }
        }
        "quote" => {
            let ch = children_array(obj, "quote")?;
            for x in ch {
                validate_block(x, budget, depth + 1)?;
            }
        }
        "code_block" => {
            let ch = children_array(obj, "code_block")?;
            if ch.is_empty() {
                return Err(RichDocV2Error::InvalidStructure(
                    "code_block.children must be non-empty (use text node)".into(),
                ));
            }
            for x in ch {
                validate_inline(x, budget, depth + 1)?;
            }
        }
        "bullet_list" | "ordered_list" => {
            let ch = children_array(obj, "list")?;
            for x in ch {
                validate_block(x, budget, depth + 1)?;
            }
        }
        "list_item" => {
            let ch = children_array(obj, "list_item")?;
            for x in ch {
                validate_block(x, budget, depth + 1)?;
            }
        }
        "divider" | "custom_block" => {
            // optional children
            if let Some(Value::Array(ch)) = obj.get("children") {
                for x in ch {
                    validate_block(x, budget, depth + 1)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn children_array<'a>(
    obj: &'a serde_json::Map<String, Value>,
    ctx: &str,
) -> Result<&'a Vec<Value>, RichDocV2Error> {
    obj.get("children")
        .and_then(Value::as_array)
        .ok_or_else(|| RichDocV2Error::InvalidStructure(format!("{ctx}.children must be array")))
}

fn validate_inline(
    v: &Value,
    budget: &mut RichDocBudget,
    depth: usize,
) -> Result<(), RichDocV2Error> {
    budget.enter("inline", depth)?;
    let obj = v
        .as_object()
        .ok_or_else(|| RichDocV2Error::InvalidStructure("inline must be object".into()))?;
    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| RichDocV2Error::InvalidStructure("inline.type missing".into()))?;
    if !INLINE.contains(&ty) {
        return Err(RichDocV2Error::InvalidStructure(format!(
            "unknown inline.type: {ty}"
        )));
    }
    match ty {
        "text" => {
            let text = obj
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| RichDocV2Error::InvalidStructure("text.text required".into()))?;
            if text.len() > MAX_RICH_DOC_TEXT_BYTES {
                return Err(RichDocV2Error::InvalidStructure(format!(
                    "text node exceeds {MAX_RICH_DOC_TEXT_BYTES} bytes"
                )));
            }
            if let Some(Value::Array(marks)) = obj.get("marks") {
                for m in marks {
                    validate_mark(m)?;
                }
            }
        }
        "link" => {
            let href = obj.get("href").and_then(Value::as_str).unwrap_or("");
            if href.is_empty() {
                return Err(RichDocV2Error::InvalidStructure(
                    "link.href required".into(),
                ));
            }
            if !is_safe_link_href(href) {
                return Err(RichDocV2Error::InvalidStructure(
                    "link.href scheme not allowed".into(),
                ));
            }
            let ch = children_array(obj, "link")?;
            for x in ch {
                validate_inline(x, budget, depth + 1)?;
            }
        }
        "inline_code" => {
            if !obj.contains_key("text") {
                return Err(RichDocV2Error::InvalidStructure(
                    "inline_code.text required".into(),
                ));
            }
        }
        "mention" => {
            if obj
                .get("user_id")
                .and_then(Value::as_str)
                .map(str::is_empty)
                .unwrap_or(true)
                && obj
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::is_empty)
                    .unwrap_or(true)
            {
                return Err(RichDocV2Error::InvalidStructure(
                    "mention needs non-empty user_id or text".into(),
                ));
            }
        }
        "emoji" => {
            if obj
                .get("key")
                .and_then(Value::as_str)
                .map(str::is_empty)
                .unwrap_or(true)
                && obj
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::is_empty)
                    .unwrap_or(true)
            {
                return Err(RichDocV2Error::InvalidStructure(
                    "emoji needs non-empty key or text".into(),
                ));
            }
        }
        "hard_break" | "custom_inline" => {}
        // `ty` 已为白名单成员；满足 `match` 在 `&str` 上的穷尽性要求
        _ => {}
    }
    Ok(())
}

fn validate_mark(v: &Value) -> Result<(), RichDocV2Error> {
    let obj = v
        .as_object()
        .ok_or_else(|| RichDocV2Error::InvalidStructure("mark must be object".into()))?;
    let t = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| RichDocV2Error::InvalidStructure("mark.type missing".into()))?;
    if !MARK.contains(&t) {
        return Err(RichDocV2Error::InvalidStructure(format!(
            "unknown mark.type: {t}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_doc_ok() {
        validate_doc_json(r#"{"type":"doc","version":2,"children":[]}"#).unwrap();
    }

    #[test]
    fn rejects_non_doc_root() {
        assert!(validate_doc_json(r#"{"type":"paragraph","version":2,"children":[]}"#).is_err());
    }

    #[test]
    fn paragraph_with_text() {
        validate_doc_json(
            r#"{"type":"doc","version":2,"children":[{"type":"paragraph","children":[{"type":"text","text":"hi","marks":[{"type":"bold"}]}]}]}"#,
        )
        .unwrap();
    }

    #[test]
    fn rejects_oversized_text_node() {
        let text = "a".repeat(128 * 1024 + 1);
        let doc = serde_json::json!({
            "type": "doc",
            "version": 2,
            "children": [{
                "type": "paragraph",
                "children": [{ "type": "text", "text": text }]
            }]
        })
        .to_string();

        let err = validate_doc_json(&doc).expect_err("oversized text must fail");

        assert!(err.to_string().contains("text"));
    }

    fn doc_with_link_href(href: &str) -> String {
        serde_json::json!({
            "type": "doc",
            "version": 2,
            "children": [{
                "type": "paragraph",
                "children": [{
                    "type": "link",
                    "href": href,
                    "children": [{ "type": "text", "text": "x" }]
                }]
            }]
        })
        .to_string()
    }

    #[test]
    fn rejects_unsafe_link_href_schemes() {
        // Plain, mixed-case, leading-control, leading-space, and tab/newline
        // injected schemes must all be rejected — browsers strip the latter and
        // would otherwise execute `javascript:`.
        for href in [
            "javascript:alert(1)",
            "JavaScript:alert(1)",
            "java\tscript:alert(1)",
            "java\nscript:alert(1)",
            "\u{1}javascript:alert(1)",
            "  javascript:alert(1)",
            "data:text/html,<script>alert(1)</script>",
            "vbscript:msgbox(1)",
            "file:///etc/passwd",
        ] {
            assert!(
                validate_doc_json(&doc_with_link_href(href)).is_err(),
                "unsafe href must be rejected: {href:?}"
            );
        }
    }

    #[test]
    fn allows_safe_link_href_schemes() {
        for href in [
            "https://example.com/path?q=1#frag",
            "http://example.com",
            "mailto:user@example.com",
            "tel:+15551234567",
            "/relative/path",
            "#anchor",
            "relative.html",
        ] {
            validate_doc_json(&doc_with_link_href(href))
                .unwrap_or_else(|e| panic!("safe href {href:?} must be allowed: {e}"));
        }
    }
}
