//! 从 Rich Doc v2 JSON 派生 `plain_text` / `search_text` / `render_hints`（权威实现）。

use serde_json::{Map, Value, json};

use super::RichDocV2Error;

/// 与 `RichTextContent` 写入字段对齐的派生结果。
#[derive(Debug, Clone)]
pub struct RichDocDerived {
    pub plain_text: String,
    pub search_text: String,
    pub render_hints: Value,
}

/// 从已解析 JSON 派生（调用方应先 `validate_doc_json`）。
pub fn derive_from_value(root: &Value) -> Result<RichDocDerived, RichDocV2Error> {
    let mut hints = HintAcc::default();
    let plain = block_children_plain(root, true, &mut hints)?;
    let search = normalize_for_search(&plain);
    Ok(RichDocDerived {
        plain_text: plain,
        search_text: search,
        render_hints: hints.finish(),
    })
}

pub fn derive_from_json_str(doc_json: &str) -> Result<RichDocDerived, RichDocV2Error> {
    let v: Value =
        serde_json::from_str(doc_json).map_err(|e| RichDocV2Error::InvalidJson(e.to_string()))?;
    derive_from_value(&v)
}

#[derive(Default)]
struct HintAcc {
    block_count: u32,
    has_code_block: bool,
    max_heading_level: Option<u8>,
    plain_char_count: usize,
}

impl HintAcc {
    fn finish(self) -> Value {
        json!({
            "block_count": self.block_count,
            "has_code_block": self.has_code_block,
            "max_heading_level": self.max_heading_level,
            "plain_char_count": self.plain_char_count,
        })
    }
}

fn block_children_plain(
    v: &Value,
    is_root: bool,
    hints: &mut HintAcc,
) -> Result<String, RichDocV2Error> {
    if is_root {
        let obj = v
            .as_object()
            .ok_or_else(|| RichDocV2Error::InvalidStructure("root must be object".into()))?;
        let children = obj
            .get("children")
            .and_then(Value::as_array)
            .ok_or_else(|| RichDocV2Error::InvalidStructure("missing children".into()))?;
        let mut parts = Vec::new();
        for c in children {
            let s = block_plain(c, hints)?;
            if !s.is_empty() {
                parts.push(s);
            }
        }
        let joined = parts.join("\n");
        hints.plain_char_count = joined.chars().count();
        return Ok(joined);
    }
    block_plain(v, hints)
}

fn block_plain(v: &Value, hints: &mut HintAcc) -> Result<String, RichDocV2Error> {
    let obj = v
        .as_object()
        .ok_or_else(|| RichDocV2Error::InvalidStructure("block must be object".into()))?;
    let ty = obj.get("type").and_then(Value::as_str).unwrap_or("");
    hints.block_count = hints.block_count.saturating_add(1);
    match ty {
        "paragraph" => {
            let ch = obj.get("children").and_then(Value::as_array);
            Ok(inlines_plain(ch.map(Vec::as_slice).unwrap_or(&[]))?)
        }
        "heading" => {
            if let Some(l) = obj.get("level").and_then(Value::as_u64) {
                let lv = l as u8;
                hints.max_heading_level = Some(match hints.max_heading_level {
                    Some(m) => m.max(lv),
                    None => lv,
                });
            }
            let ch = obj.get("children").and_then(Value::as_array);
            Ok(inlines_plain(ch.map(Vec::as_slice).unwrap_or(&[]))?)
        }
        "quote" => join_blocks(obj, hints),
        "code_block" => {
            hints.has_code_block = true;
            let ch = obj.get("children").and_then(Value::as_array);
            Ok(inlines_plain(ch.map(Vec::as_slice).unwrap_or(&[]))?)
        }
        "bullet_list" | "ordered_list" => join_blocks(obj, hints),
        "list_item" => join_blocks(obj, hints),
        "divider" => Ok(String::new()),
        "custom_block" => join_blocks(obj, hints),
        _ => Ok(String::new()),
    }
}

fn join_blocks(obj: &Map<String, Value>, hints: &mut HintAcc) -> Result<String, RichDocV2Error> {
    let Some(Value::Array(children)) = obj.get("children") else {
        return Ok(String::new());
    };
    let mut parts = Vec::new();
    for c in children {
        let s = block_plain(c, hints)?;
        if !s.is_empty() {
            parts.push(s);
        }
    }
    Ok(parts.join("\n"))
}

fn inlines_plain(nodes: &[Value]) -> Result<String, RichDocV2Error> {
    let mut out = String::new();
    for n in nodes {
        inline_plain_into(n, &mut out)?;
    }
    Ok(out)
}

fn inline_plain_into(n: &Value, out: &mut String) -> Result<(), RichDocV2Error> {
    let Some(obj) = n.as_object() else {
        return Ok(());
    };
    let ty = obj.get("type").and_then(Value::as_str).unwrap_or("");
    match ty {
        "text" => {
            if let Some(t) = obj.get("text").and_then(Value::as_str) {
                out.push_str(t);
            }
        }
        "hard_break" => out.push('\n'),
        "inline_code" => {
            if let Some(t) = obj.get("text").and_then(Value::as_str) {
                out.push_str(t);
            }
        }
        "mention" => {
            if let Some(uid) = obj.get("user_id").and_then(Value::as_str) {
                out.push('@');
                out.push_str(uid);
            } else if let Some(t) = obj.get("text").and_then(Value::as_str) {
                out.push_str(t);
            }
        }
        "emoji" => {
            if let Some(k) = obj.get("key").and_then(Value::as_str) {
                out.push(':');
                out.push_str(k);
                out.push(':');
            } else if let Some(t) = obj.get("text").and_then(Value::as_str) {
                out.push_str(t);
            }
        }
        "link" => {
            if let Some(Value::Array(ch)) = obj.get("children") {
                for c in ch {
                    inline_plain_into(c, out)?;
                }
            }
        }
        "custom_inline" => {}
        _ => {}
    }
    Ok(())
}

/// NFKC。`unicode-normalization` 的表在 wasm 成品里占十来万字节，浏览器自带同样的能力。
#[cfg(not(target_arch = "wasm32"))]
fn nfkc(text: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    text.chars().nfkc().collect()
}

#[cfg(target_arch = "wasm32")]
fn nfkc(text: &str) -> String {
    js_sys::JsString::from(text).normalize("NFKC").into()
}

/// NFKC + 小写 + 空白折叠（检索管道；与 proto 注释一致）。
pub fn normalize_for_search(plain: &str) -> String {
    let lower = nfkc(plain).to_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut prev_space = true;
    for ch in lower.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            prev_space = false;
            out.push(ch);
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_collapses_space() {
        assert_eq!(normalize_for_search("  Hello   World\n\t"), "hello world");
    }

    #[test]
    fn search_folds_compatibility_forms() {
        // 全角字母和数字、表意空格、连字、带圈数字、单位符号都折成基本形式。
        assert_eq!(normalize_for_search("ＡＢＣ\u{3000}１２３"), "abc 123");
        assert_eq!(
            normalize_for_search("\u{fb01}le \u{2460} \u{338f}"),
            "file 1 kg"
        );
        // 组合字符合成，分解形式和预组合形式检索到同一个串。
        assert_eq!(
            normalize_for_search("cafe\u{301}"),
            normalize_for_search("caf\u{e9}")
        );
    }
}
