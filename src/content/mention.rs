//! 提及解析：从正文里判定 `@全员` 与 `@某人`。
//!
//! 这是**产品中立的内容规则**——同一段文字在任何端都必须产出同一条消息，
//! 所以规则只能有一份，放在核心（flare-im-spec 约束 4）。
//!
//! 此前这条规则只存在于 Vue kit 的一行正则 `/(^|\s)@all(\s|$)/i` 里：
//! 中文用户打 `@全员`/`@所有人` 不触发，而 Flutter / iOS / Android 压根不解析，
//! `mentionAll` 恒为 false —— 同样的文本在不同端产生不同的消息。

/// 一个可被提及的人：用户 id + 可能的显示名（昵称、备注等）。
#[derive(Debug, Clone)]
pub struct MentionCandidate {
    pub user_id: String,
    pub display_names: Vec<String>,
}

/// 解析结果。`user_ids` 按正文出现顺序去重。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedMentions {
    pub mention_all: bool,
    pub user_ids: Vec<String>,
}

/// 触发「@全员」的记号。大小写不敏感（仅对 ASCII 有意义）。
const MENTION_ALL_TOKENS: [&str; 4] = ["all", "everyone", "全员", "所有人"];

/// `@` 是否处在一个合法的提及起始位置。
///
/// 要求前一个字符是空白、或本身就是开头，否则 `foo@all.com` 这类邮箱会被误判。
/// 但中文输入普遍不加空格（`你好@全员`），所以额外放行「前一个字符是 CJK」——
/// 邮箱的本地部分不会以中日韩文字结尾，两边都能照顾到。
fn is_mention_boundary(prev: Option<char>) -> bool {
    match prev {
        None => true,
        Some(c) => c.is_whitespace() || is_cjk(c),
    }
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3400..=0x4DBF      // 扩展 A
        | 0x4E00..=0x9FFF    // 基本区
        | 0xF900..=0xFAFF    // 兼容表意
        | 0x3000..=0x303F    // 中日韩符号与标点
        | 0xFF00..=0xFFEF    // 全角
    )
}

/// 提及记号允许的字符：ASCII 字母数字与 `_ . -`，以及 CJK。
/// 空白与常见标点天然成为终止符。
fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') || is_cjk(c) && !is_cjk_punctuation(c)
}

fn is_cjk_punctuation(c: char) -> bool {
    matches!(c as u32, 0x3000..=0x303F | 0xFF00..=0xFF0F | 0xFF1A..=0xFF20)
}

/// A resolved token and its range in Unicode scalar values, as used on the wire.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TextMention {
    /// `None` is an explicit @all token.
    pub user_id: Option<String>,
    pub start: i32,
    pub length: i32,
}

/// 从正文解析提及。匹配采用最长优先，范围与接收端的高亮位置一致。
pub(crate) fn resolve_mentions(text: &str, candidates: &[MentionCandidate]) -> Vec<TextMention> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens: Vec<(Vec<char>, Option<&str>)> = MENTION_ALL_TOKENS
        .iter()
        .map(|token| (token.chars().collect(), None))
        .collect();
    for candidate in candidates {
        let id = candidate.user_id.trim();
        if id.is_empty() {
            continue;
        }
        tokens.push((id.chars().collect(), Some(id)));
        for name in &candidate.display_names {
            let name = name.trim();
            if !name.is_empty() && name != id {
                tokens.push((name.chars().collect(), Some(id)));
            }
        }
    }
    tokens.sort_by_key(|(token, _)| std::cmp::Reverse(token.len()));

    let mut mentions = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '@' || !is_mention_boundary(i.checked_sub(1).map(|prev| chars[prev])) {
            i += 1;
            continue;
        }
        let tail = &chars[i + 1..];
        let Some((token, user_id)) = tokens.iter().find(|(token, _)| {
            tail.get(..token.len()).is_some_and(|head| {
                head.iter()
                    .zip(token)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
                    && ends_mention(&tail[token.len()..])
            })
        }) else {
            i += 1;
            continue;
        };
        let length = token.len() + 1;
        mentions.push(TextMention {
            user_id: user_id.map(str::to_owned),
            start: i as i32,
            length: length as i32,
        });
        i += length;
    }
    mentions
}

fn ends_mention(tail: &[char]) -> bool {
    match tail {
        ['.', next, ..] => !is_token_char(*next),
        ['.'] => true,
        [next, ..] => !is_token_char(*next),
        [] => true,
    }
}

/// `user_ids` 按正文出现顺序去重。消息构建同时使用解析得到的原文范围。
pub fn parse_mentions(text: &str, candidates: &[MentionCandidate]) -> ParsedMentions {
    let mut result = ParsedMentions::default();
    for mention in resolve_mentions(text, candidates) {
        match mention.user_id {
            Some(id) if !result.user_ids.contains(&id) => result.user_ids.push(id),
            None => result.mention_all = true,
            _ => {}
        }
    }
    result
}

/// 从**已解码的内容**还原提及状态。
///
/// wire 上提及走 `TextContent.mentions`，`Message.mention_all` / `mention_users`
/// 是本地派生字段。发送侧写进 content，接收侧却从来没有读回来 ——
/// 于是对端收到的消息 `mentionAll` 恒为 false：任何端都高亮不了收到的 @，
/// 「只接收@我」也无从判断。跨端判据必须看**对端收到的那条**，
/// 只断言本地构建结果会漏掉这一整段。
pub fn mentions_from_content(content: &flare_proto::common::MessageContent) -> ParsedMentions {
    use flare_proto::common::{MentionType, message_content::Content};
    let Some(Content::Text(text)) = content.content.as_ref() else {
        return ParsedMentions::default();
    };
    let mut parsed = ParsedMentions::default();
    for mention in &text.mentions {
        match MentionType::try_from(mention.r#type) {
            Ok(MentionType::All) => parsed.mention_all = true,
            Ok(MentionType::User) => {
                let id = mention.user_id.trim();
                if !id.is_empty() && !parsed.user_ids.iter().any(|x| x == id) {
                    parsed.user_ids.push(id.to_string());
                }
            }
            Ok(MentionType::Multi) => {
                for id in &mention.user_ids {
                    let id = id.trim();
                    if !id.is_empty() && !parsed.user_ids.iter().any(|x| x == id) {
                        parsed.user_ids.push(id.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates() -> Vec<MentionCandidate> {
        vec![
            MentionCandidate {
                user_id: "webtest2".to_string(),
                display_names: vec!["张三".to_string()],
            },
            MentionCandidate {
                user_id: "u2".to_string(),
                display_names: vec!["张三丰".to_string()],
            },
        ]
    }

    #[test]
    fn chinese_at_all_triggers_mention_all() {
        // 这条是本次修复的起点：中文用户打 @全员 从来没生效过。
        for text in ["@全员 开会了", "你好@所有人", "@全员", "开会@全员！"] {
            assert!(
                parse_mentions(text, &candidates()).mention_all,
                "{text} 应当触发 @全员"
            );
        }
    }

    #[test]
    fn ascii_at_all_still_works() {
        assert!(parse_mentions("@all hello", &candidates()).mention_all);
        assert!(parse_mentions("hi @everyone", &candidates()).mention_all);
        assert!(parse_mentions("@ALL", &candidates()).mention_all);
    }

    #[test]
    fn email_like_text_is_not_a_mention() {
        // 前一个字符不是空白也不是 CJK → 不是提及边界。
        let parsed = parse_mentions("联系 foo@all.com", &candidates());
        assert!(!parsed.mention_all, "邮箱不能被误判成 @全员");
    }

    #[test]
    fn mentions_by_user_id_and_display_name() {
        assert_eq!(
            parse_mentions("@webtest2 在吗", &candidates()).user_ids,
            vec!["webtest2".to_string()]
        );
        assert_eq!(
            parse_mentions("@张三 在吗", &candidates()).user_ids,
            vec!["webtest2".to_string()],
            "中文显示名必须能被提及——旧的 ASCII-only 正则做不到"
        );
    }

    #[test]
    fn longest_candidate_wins() {
        // @张三丰 不能被 @张三 抢走，否则提及到错误的人。
        assert_eq!(
            parse_mentions("@张三丰 你好", &candidates()).user_ids,
            vec!["u2".to_string()]
        );
    }

    #[test]
    fn non_member_mention_is_ignored() {
        assert!(
            parse_mentions("@nobody 在吗", &candidates())
                .user_ids
                .is_empty()
        );
    }

    fn text_content_with(
        mentions: Vec<flare_proto::common::Mention>,
    ) -> flare_proto::common::MessageContent {
        flare_proto::common::MessageContent {
            content: Some(flare_proto::common::message_content::Content::Text(
                flare_proto::common::TextContent {
                    text: "hi".to_string(),
                    mentions,
                },
            )),
        }
    }

    #[test]
    fn mention_all_survives_the_round_trip() {
        // 这条钉的是跨端那一段：发送侧把 @全员 写进 content，接收侧必须读回来。
        // 曾经只写不读，对端收到的 mentionAll 恒为 false。
        let content = text_content_with(vec![flare_proto::common::Mention {
            r#type: flare_proto::common::MentionType::All as i32,
            ..Default::default()
        }]);
        assert!(mentions_from_content(&content).mention_all);
    }

    #[test]
    fn user_mentions_survive_the_round_trip() {
        let content = text_content_with(vec![
            flare_proto::common::Mention {
                r#type: flare_proto::common::MentionType::User as i32,
                user_id: "u1".to_string(),
                ..Default::default()
            },
            flare_proto::common::Mention {
                r#type: flare_proto::common::MentionType::Multi as i32,
                user_ids: vec!["u2".to_string(), "u1".to_string()],
                ..Default::default()
            },
        ]);
        let parsed = mentions_from_content(&content);
        assert!(!parsed.mention_all);
        assert_eq!(parsed.user_ids, vec!["u1".to_string(), "u2".to_string()]);
    }

    #[test]
    fn duplicate_mentions_are_deduped_in_order() {
        let parsed = parse_mentions("@张三 @u2 @webtest2", &candidates());
        assert_eq!(
            parsed.user_ids,
            vec!["webtest2".to_string(), "u2".to_string()]
        );
    }
}
