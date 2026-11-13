//! 会话列表摘要、`messages.text`、`quote_preview` 等持久化字段使用的**稳定 i18n 载荷**（非展示文案）。
//!
//! 序列化形态（单列字符串，如 SQLite `TEXT`）：
//! `{"k":"im.preview.image","a":{"d":"说明","m":true}}`
//!
//! - **`k`**：应用端用于查表翻译的稳定 key（勿改语义；新增类型请追加新 key）。
//! - **`a`**：可选参数；常用短键：`t` 正文/标题类、`ik` i18n 主键、`ek` 短 event_kind、`p` 插值对象、`fb` 服务端回退文案、`d` 说明、`n` 文件名或数量…
//!
//! 历史数据若为**非 JSON 纯文本**，应用端应原样展示；新写入均为上述 JSON。

use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;
use std::collections::HashMap;

/// 稳定预览类型 key（供各端 i18n 映射）。
pub mod keys {
    pub const USER_TEXT: &str = "im.preview.user_text";
    pub const RICH_TEXT: &str = "im.preview.rich_text";
    pub const FILE: &str = "im.preview.file";
    pub const IMAGE: &str = "im.preview.image";
    pub const VIDEO: &str = "im.preview.video";
    pub const AUDIO: &str = "im.preview.audio";
    pub const LOCATION: &str = "im.preview.location";
    pub const CARD: &str = "im.preview.card";
    pub const STICKER: &str = "im.preview.sticker";
    pub const EMOJI: &str = "im.preview.emoji";
    pub const QUOTE: &str = "im.preview.quote";
    pub const LINK: &str = "im.preview.link";
    pub const FORWARD_EMPTY: &str = "im.preview.forward_empty";
    pub const FORWARD_MANY: &str = "im.preview.forward_many";
    pub const THREAD: &str = "im.preview.thread";
    pub const MINI_PROGRAM: &str = "im.preview.mini_program";
    pub const IMAGE_GROUP: &str = "im.preview.image_group";
    pub const SYSTEM: &str = "im.preview.system";
    pub const NOTIFICATION: &str = "im.preview.notification";
    pub const VOTE: &str = "im.preview.vote";
    pub const TASK: &str = "im.preview.task";
    pub const SCHEDULE: &str = "im.preview.schedule";
    pub const ANNOUNCEMENT: &str = "im.preview.announcement";
    pub const CUSTOM: &str = "im.preview.custom";
    pub const PLACEHOLDER: &str = "im.preview.placeholder";
    /// 解码失败或 `MessageContent` 无 oneof 时使用。
    pub const UNKNOWN: &str = "im.preview.unknown";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewStoragePayload {
    pub k: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub a: Map<String, Value>,
}

impl PreviewStoragePayload {
    /// 纯文本且 `t` 为空时，会话列表等场景视为「无摘要」。
    pub fn is_empty_for_last_preview(&self) -> bool {
        if self.k != keys::USER_TEXT {
            return false;
        }
        self.a
            .get("t")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
    }

    /// 这条摘要里不随读者语言变化的文字：正文、标题、说明、插件卡的「[插件名] 卡片摘要」（`d`）等，与会话
    /// 列表展示这条摘要时照原样画出的那段一致。只能用读者语言的类型词表达的摘要（无说明的图片、文件、位置、
    /// 名片、表情、贴纸、投票、多条转发，以及按 `ik` 翻译的系统/通知）为空串，由展示端按消息内容补上。
    pub fn verbatim_text(&self) -> String {
        let arg = |key: &str| {
            self.a
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        };
        let text = match self.k.as_str() {
            keys::USER_TEXT
            | keys::LINK
            | keys::THREAD
            | keys::MINI_PROGRAM
            | keys::TASK
            | keys::ANNOUNCEMENT
            | keys::PLACEHOLDER => arg("t"),
            keys::RICH_TEXT => match (arg("title"), arg("body")) {
                (Some(title), Some(body)) => Some(format!("{title} {body}")),
                (title, body) => title.or(body),
            },
            keys::IMAGE | keys::VIDEO | keys::AUDIO | keys::CUSTOM => arg("d"),
            keys::QUOTE => self
                .a
                .get("inner")
                .and_then(|inner| {
                    crate::shared::util::json::from_value::<PreviewStoragePayload>(inner.clone())
                        .ok()
                })
                .map(|inner| inner.verbatim_text())
                .filter(|text| !text.is_empty()),
            keys::SYSTEM | keys::NOTIFICATION if !self.a.contains_key("ik") => {
                arg("t").or_else(|| arg("body")).or_else(|| arg("title"))
            }
            _ => None,
        };
        text.unwrap_or_default()
    }
}

/// 若已是合法 JSON 载荷则解析；否则视为用户可见原文。
pub fn decode_or_user_text(s: &str) -> PreviewStoragePayload {
    let t = s.trim();
    if t.is_empty() {
        return PreviewStoragePayload {
            k: keys::USER_TEXT.to_string(),
            a: Map::new(),
        };
    }
    if let Ok(p) = crate::shared::util::json::from_str::<PreviewStoragePayload>(t) {
        return p;
    }
    let mut a = Map::new();
    a.insert("t".into(), Value::String(s.to_string()));
    PreviewStoragePayload {
        k: keys::USER_TEXT.to_string(),
        a,
    }
}

/// 与 [`DecodedContent::Unknown`]、Elem 解析失败等对齐的预览 JSON。
pub fn unknown_preview_json() -> String {
    serde_json::to_string(&PreviewStoragePayload {
        k: keys::UNKNOWN.to_string(),
        a: Map::new(),
    })
    .unwrap_or_default()
}

/// 不应写入 `extra.contentText` 的预览（空串、未知类型、历史 `[未知]` 占位）。
pub fn is_redundant_content_text_extra(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return true;
    }
    if t == "[未知]" {
        return true;
    }
    if let Ok(p) = crate::shared::util::json::from_str::<PreviewStoragePayload>(t)
        && p.k == keys::UNKNOWN
    {
        return true;
    }
    false
}

fn resolve_i18n_key(data: &HashMap<String, String>, fallback_key: &str) -> Option<String> {
    data.get("i18n_key")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            let fk = fallback_key.trim();
            if fk.is_empty() {
                None
            } else {
                Some(fk.to_string())
            }
        })
}

fn attach_i18n_params(a: &mut Map<String, Value>, data: &HashMap<String, String>) {
    if let Some(raw) = data.get("i18n_params")
        && let Ok(params) = serde_json::from_slice::<Map<String, Value>>(raw.as_bytes())
        && !params.is_empty()
    {
        a.insert("p".into(), Value::Object(params));
    }
}

fn localizable_preview(
    storage_key: &str,
    fallback_i18n_key: &str,
    title: Option<&str>,
    body: &str,
    data: &HashMap<String, String>,
    include_event_kind: bool,
) -> PreviewStoragePayload {
    let mut a = Map::new();
    if let Some(ik) = resolve_i18n_key(data, fallback_i18n_key) {
        a.insert("ik".into(), Value::String(ik));
        if include_event_kind {
            let ek = fallback_i18n_key.trim();
            if !ek.is_empty() {
                a.insert("ek".into(), Value::String(ek.to_string()));
            }
        }
        if let Some(t) = title.filter(|s| !s.trim().is_empty()) {
            a.insert("title".into(), Value::String(t.to_string()));
        }
        attach_i18n_params(&mut a, data);
        if !body.trim().is_empty() {
            a.insert("fb".into(), Value::String(body.to_string()));
        }
    } else if storage_key == keys::SYSTEM {
        if !body.trim().is_empty() {
            a.insert("t".into(), Value::String(body.to_string()));
        }
    } else if !body.trim().is_empty() {
        a.insert("body".into(), Value::String(body.to_string()));
    } else if let Some(t) = title.filter(|s| !s.trim().is_empty()) {
        a.insert("title".into(), Value::String(t.to_string()));
    }
    PreviewStoragePayload {
        k: storage_key.to_string(),
        a,
    }
}

/// 系统消息会话摘要：优先 `data.i18n_key`（客户端翻译），`fb` 为服务端回退文案。
pub fn localizable_system_preview(
    event_kind: &str,
    body: &str,
    data: &HashMap<String, String>,
) -> PreviewStoragePayload {
    localizable_preview(keys::SYSTEM, event_kind, None, body, data, true)
}

/// 应用内通知摘要：优先 `data.i18n_key`，否则 `notification_type` 作翻译键。
pub fn localizable_notification_preview(
    notification_type: &str,
    title: &str,
    body: &str,
    data: &HashMap<String, String>,
) -> PreviewStoragePayload {
    localizable_preview(
        keys::NOTIFICATION,
        notification_type,
        Some(title),
        body,
        data,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decode_user_text_roundtrip() {
        let p = decode_or_user_text("hello");
        assert_eq!(p.k, keys::USER_TEXT);
        assert_eq!(p.a.get("t").and_then(|v| v.as_str()), Some("hello"));
    }

    #[test]
    fn localizable_system_with_i18n_key() {
        let mut data = HashMap::new();
        data.insert(
            "i18n_key".into(),
            "social.relation.friendship_established".into(),
        );
        data.insert("i18n_params".into(), json!({"user_a": "a"}).to_string());
        let p =
            localizable_system_preview("relation.friendship_established", "你们已成为好友", &data);
        assert_eq!(p.k, keys::SYSTEM);
        assert_eq!(
            p.a.get("ik").and_then(|v| v.as_str()),
            Some("social.relation.friendship_established")
        );
        assert!(p.a.contains_key("p"));
        assert_eq!(
            p.a.get("fb").and_then(|v| v.as_str()),
            Some("你们已成为好友")
        );
    }

    /// What a preview reads the same in every language, and what only a reader's own words can say.
    #[test]
    fn verbatim_text_keeps_the_words_and_leaves_type_words_to_the_reader() {
        let payload = |k: &str, a: serde_json::Value| PreviewStoragePayload {
            k: k.to_string(),
            a: a.as_object().cloned().unwrap_or_default(),
        };
        assert_eq!(
            payload(keys::USER_TEXT, json!({"t": " 今晚七点见 "})).verbatim_text(),
            "今晚七点见"
        );
        assert_eq!(
            payload(
                keys::RICH_TEXT,
                json!({"title": "周报", "body": "进度正常"})
            )
            .verbatim_text(),
            "周报 进度正常"
        );
        assert_eq!(
            payload(keys::RICH_TEXT, json!({"body": "进度正常"})).verbatim_text(),
            "进度正常"
        );
        assert_eq!(
            payload(keys::CUSTOM, json!({"d": "[红包] 恭喜发财"})).verbatim_text(),
            "[红包] 恭喜发财"
        );
        assert_eq!(
            payload(keys::IMAGE, json!({"d": "海边"})).verbatim_text(),
            "海边"
        );
        let quote = payload(
            keys::QUOTE,
            json!({"inner": {"k": keys::USER_TEXT, "a": {"t": "好的"}}}),
        );
        assert_eq!(quote.verbatim_text(), "好的");
        assert_eq!(
            payload(keys::SYSTEM, json!({"t": "群名已修改"})).verbatim_text(),
            "群名已修改"
        );
        // Only a reader's own words can say these: the record leaves them to the client.
        for empty in [
            payload(keys::IMAGE, json!({"m": true})),
            payload(keys::FILE, json!({"n": "report.pdf"})),
            payload(keys::LOCATION, json!({"label": "西站"})),
            payload(keys::CARD, json!({"label": "林夏"})),
            payload(keys::EMOJI, json!({"e": "hundred_points"})),
            payload(keys::STICKER, json!({})),
            payload(keys::VOTE, json!({})),
            payload(keys::FORWARD_MANY, json!({"n": 3})),
            payload(keys::CUSTOM, json!({})),
            payload(
                keys::SYSTEM,
                json!({"ik": "social.group.created", "fb": "群已创建"}),
            ),
            payload(keys::QUOTE, json!({"inner": {"k": keys::IMAGE}})),
        ] {
            assert_eq!(empty.verbatim_text(), "", "{empty:?}");
        }
    }

    #[test]
    fn localizable_notification_fallback_type() {
        let data = HashMap::new();
        let p = localizable_notification_preview(
            "social.relation.friend_request",
            "好友申请",
            "有人申请加你为好友",
            &data,
        );
        assert_eq!(p.k, keys::NOTIFICATION);
        assert_eq!(
            p.a.get("ik").and_then(|v| v.as_str()),
            Some("social.relation.friend_request")
        );
    }
}
