//! 引擎自己出的通知文案：标题、正文、点名标记、条数前缀，按预览设置露出多少。
//!
//! 与推送服务端的兜底文案同一套规则、同一套字符串（规则 crate 的 `text` 模块），区别只在来源：
//! 引擎用本地的会话标题、发送者显示名与应用语言，服务端只有发送端给的文案。
//!
//! - 标题：会话标题（单聊即对方的名字），没有时用通用标题；条数大于 1 时前面带条数；
//! - 正文：消息的摘要文字，按内容类型补占位（`[图片]`）；群里知道发送者时前面带「发送者: 」；
//!   点名到接收者时加点名标记，文字自己已经写了就不重复；
//! - 只显示发送者：保留标题，正文换成「发来一条消息」（群里接在发送者后面）；
//! - 隐藏、内容不可见、E2EE 密文：通用标题与通用正文。

use flare_notify_rules::text::{Lang, Phrase, count_prefix, digest_text, mentions_already, phrase};

use crate::content::message_elem::{Elem, elem_preview_storage_payload};
use crate::content::preview_storage::keys;
use crate::extension::encryption::E2EE_PLACEHOLDER_REASON;
use crate::model::IMMessage;
use crate::model::notification::NotificationPreview;

/// 标题最多多少个字符（含省略号）。
const TITLE_MAX_CHARS: usize = 64;
/// 正文最多多少个字符（含省略号）。
const BODY_MAX_CHARS: usize = 180;
const ELLIPSIS: char = '…';

/// 渲染好的标题与正文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlertText {
    pub title: String,
    pub body: String,
}

/// 文案代表的是消息还是通知类内容：两者拿不到内容时的通用正文不同，「发来一条消息」与点名标记
/// 只对消息有意义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyKind {
    Message,
    Notice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Body {
    /// 内容自己的文字或占位，群里可能带发送者。
    Content {
        sender: Option<String>,
        content: Content,
    },
    /// 「发来一条消息」，群里接在发送者后面。
    SentMessage {
        sender: Option<String>,
    },
    Generic(CopyKind),
    Recalled,
    Digest {
        conversations: u32,
        messages: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Content {
    Text(String),
    Placeholder(Phrase),
    /// 占位加上内容自带的说明（图片说明、文件名、链接标题）。
    Labeled(Phrase, String),
    Generic(CopyKind),
}

/// 还没定语言的一条文案。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlertCopy {
    /// `None` 用通用标题。
    title: Option<String>,
    body: Body,
    mention: bool,
    /// 标题前带的条数；0 与 1 不带。
    count: u32,
}

/// 一条消息的文案需要知道的本地事实。
pub(crate) struct MessageFacts<'a> {
    /// 会话标题（单聊即对方的名字）；本地还没有名字时为 `None`。
    pub title: Option<&'a str>,
    /// 发送者显示名；只在群里、并且知道名字时给。
    pub sender: Option<&'a str>,
    /// 点名到了接收者（含 @所有人）。
    pub mentioned: bool,
    /// 这条通知代表的消息数。
    pub count: u32,
    pub preview: NotificationPreview,
}

impl AlertCopy {
    /// 一条消息（或通知类内容）的文案，按预览设置露出。
    pub(crate) fn message(message: &IMMessage, facts: &MessageFacts<'_>) -> Self {
        let notice = match message.content.as_ref() {
            Some(Elem::Notification(notice)) => Some(notice),
            _ => None,
        };
        let kind = if notice.is_some() {
            CopyKind::Notice
        } else {
            CopyKind::Message
        };
        if facts.preview == NotificationPreview::Hidden || withheld(message) {
            return Self::generic(kind, facts.count);
        }
        // 通知类内容自带标题（「好友申请」），没有时用会话标题；它没有发送者可言。
        let title = notice
            .and_then(|notice| clean(&notice.title, TITLE_MAX_CHARS))
            .or_else(|| facts.title.and_then(|title| clean(title, TITLE_MAX_CHARS)));
        let sender = match kind {
            CopyKind::Message => facts
                .sender
                .and_then(|sender| clean(sender, TITLE_MAX_CHARS)),
            CopyKind::Notice => None,
        };
        let body = match facts.preview {
            NotificationPreview::Hidden => Body::Generic(kind),
            // 「发来一条消息」要接在发送者或标题后面读；两者都没有时只能说通用的那句。
            NotificationPreview::SenderOnly => match kind {
                CopyKind::Message if title.is_some() || sender.is_some() => {
                    Body::SentMessage { sender }
                }
                _ => Body::Generic(kind),
            },
            NotificationPreview::Full => Body::Content {
                sender,
                content: message
                    .content
                    .as_ref()
                    .map_or(Content::Generic(kind), content_of),
            },
        };
        let mut copy = Self {
            title,
            body,
            mention: false,
            count: facts.count,
        };
        copy.mention = kind == CopyKind::Message && facts.mentioned && !copy.says_mentioned();
        copy
    }

    /// 撤回替换：正文换成撤回文案，标题沿用原通知的来源；预览隐藏时用通用标题。
    pub(crate) fn recalled(title: Option<&str>, preview: NotificationPreview, count: u32) -> Self {
        let title = match preview {
            NotificationPreview::Hidden => None,
            _ => title.and_then(|title| clean(title, TITLE_MAX_CHARS)),
        };
        Self {
            title,
            body: Body::Recalled,
            mention: false,
            count,
        }
    }

    /// 摘要：通用标题，正文是「N 个会话有 M 条新消息」；条数已经写在正文里，标题不再带。
    pub(crate) fn digest(conversations: u32, messages: u32) -> Self {
        Self {
            title: None,
            body: Body::Digest {
                conversations,
                messages,
            },
            mention: false,
            count: 0,
        }
    }

    fn generic(kind: CopyKind, count: u32) -> Self {
        Self {
            title: None,
            body: Body::Generic(kind),
            mention: false,
            count,
        }
    }

    /// 标题（不带条数）：撤回替换沿用它。通用标题时为 `None`。
    pub(crate) fn plain_title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    fn says_mentioned(&self) -> bool {
        self.title.as_deref().is_some_and(mentions_already)
            || matches!(
                &self.body,
                Body::Content { content: Content::Text(text), .. } if mentions_already(text)
            )
    }

    pub(crate) fn render(&self, lang: Lang) -> AlertText {
        let title = self
            .title
            .as_deref()
            .unwrap_or_else(|| phrase(lang, Phrase::GenericTitle));
        let body = match &self.body {
            Body::Content { sender, content } => {
                let content = match content {
                    Content::Text(text) => text.clone(),
                    Content::Placeholder(placeholder) => phrase(lang, *placeholder).to_owned(),
                    Content::Labeled(placeholder, label) => {
                        format!("{} {label}", phrase(lang, *placeholder))
                    }
                    Content::Generic(kind) => generic_body(lang, *kind).to_owned(),
                };
                match sender {
                    Some(sender) => format!("{sender}: {content}"),
                    None => content,
                }
            }
            Body::SentMessage { sender } => {
                let sent = phrase(lang, Phrase::SentMessage);
                match (sender, lang) {
                    (Some(sender), Lang::Zh) => format!("{sender}{sent}"),
                    (Some(sender), Lang::En) => format!("{sender} {sent}"),
                    (None, _) => sent.to_owned(),
                }
            }
            Body::Generic(kind) => generic_body(lang, *kind).to_owned(),
            Body::Recalled => phrase(lang, Phrase::Recalled).to_owned(),
            Body::Digest {
                conversations,
                messages,
            } => digest_text(lang, *conversations, *messages),
        };
        let body = if self.mention {
            format!("{} {body}", phrase(lang, Phrase::MentionMarker))
        } else {
            body
        };
        let prefix = count_prefix(lang, self.count).unwrap_or_default();
        AlertText {
            title: fit(&format!("{prefix}{title}"), TITLE_MAX_CHARS),
            body: fit(&body, BODY_MAX_CHARS),
        }
    }
}

fn generic_body(lang: Lang, kind: CopyKind) -> &'static str {
    match kind {
        CopyKind::Message => phrase(lang, Phrase::GenericBody),
        CopyKind::Notice => phrase(lang, Phrase::NoticeBody),
    }
}

/// 内容不能露出：保留期把内容隐藏、抹掉或清除了，或者是还没解开的 E2EE 密文。
fn withheld(message: &IMMessage) -> bool {
    use flare_proto::common::ContentVisibility;

    let hidden = message
        .retention_state
        .as_ref()
        .and_then(|state| ContentVisibility::try_from(state.content_visibility).ok())
        .is_some_and(|visibility| {
            matches!(
                visibility,
                ContentVisibility::Hidden | ContentVisibility::Redacted | ContentVisibility::Purged
            )
        });
    hidden
        || matches!(
            message.content.as_ref(),
            Some(Elem::Placeholder(placeholder)) if placeholder.reason == E2EE_PLACEHOLDER_REASON
        )
}

/// 消息内容能给出的正文：沿用会话列表摘要的规则（[`elem_preview_storage_payload`]），
/// 带文字的取文字，只有类型的按类型用占位。
fn content_of(elem: &Elem) -> Content {
    match elem {
        // 系统消息的展示文案由服务端渲染，摘要载荷里只有翻译键时也照样有它。
        Elem::System(system) => {
            return text_or(&system.body, Phrase::Message);
        }
        Elem::Notification(notice) => {
            return clean(&notice.body, BODY_MAX_CHARS)
                .map_or(Content::Generic(CopyKind::Notice), Content::Text);
        }
        _ => {}
    }
    let payload = elem_preview_storage_payload(elem);
    let words = payload.verbatim_text();
    let arg = |key: &str| {
        payload
            .a
            .get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| clean(value, BODY_MAX_CHARS))
    };
    match payload.k.as_str() {
        keys::IMAGE | keys::IMAGE_GROUP => labeled(Phrase::Image, &words),
        keys::VIDEO => labeled(Phrase::Video, &words),
        keys::AUDIO => labeled(Phrase::Voice, &words),
        keys::LINK => labeled(Phrase::Link, &words),
        keys::FILE => arg("n").map_or(Content::Placeholder(Phrase::File), |name| {
            Content::Labeled(Phrase::File, name)
        }),
        keys::LOCATION => arg("label").map_or(Content::Placeholder(Phrase::Location), |label| {
            Content::Labeled(Phrase::Location, label)
        }),
        keys::CARD => Content::Placeholder(Phrase::Contact),
        keys::STICKER => Content::Placeholder(Phrase::Sticker),
        keys::EMOJI => Content::Placeholder(Phrase::Emoji),
        keys::RICH_TEXT => text_or(&words, Phrase::RichText),
        _ => text_or(&words, Phrase::Message),
    }
}

fn text_or(raw: &str, placeholder: Phrase) -> Content {
    clean(raw, BODY_MAX_CHARS).map_or(Content::Placeholder(placeholder), Content::Text)
}

fn labeled(placeholder: Phrase, raw: &str) -> Content {
    clean(raw, BODY_MAX_CHARS).map_or(Content::Placeholder(placeholder), |label| {
        Content::Labeled(placeholder, label)
    })
}

/// 进通知之前清理一段文字：去掉控制字符、双向文字的覆盖与隔离字符和看不见的格式字符（它们能把
/// 一条通知排成几行、把文字倒着显示），连续空白收成一个空格，再截到上限。清理后为空返回 `None`。
fn clean(raw: &str, max_chars: usize) -> Option<String> {
    let mut cleaned = String::with_capacity(raw.len().min(max_chars * 4));
    let mut pending_space = false;
    for ch in raw.chars() {
        if ch.is_whitespace() {
            pending_space = true;
        } else if !invisible(ch) {
            if pending_space && !cleaned.is_empty() {
                cleaned.push(' ');
            }
            pending_space = false;
            cleaned.push(ch);
        }
    }
    (!cleaned.is_empty()).then(|| fit(&cleaned, max_chars))
}

fn invisible(ch: char) -> bool {
    ch.is_control()
        || matches!(
            ch,
            '\u{200B}'..='\u{200F}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2060}'..='\u{2064}'
                | '\u{2066}'..='\u{2069}'
                | '\u{FEFF}'
                | '\u{3164}'
                | '\u{115F}'
                | '\u{1160}'
        )
}

/// 截到 `max_chars` 个字符，超出时末尾换成省略号（省略号算在上限里）。
fn fit(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    let mut fitted: String = text.chars().take(max_chars.saturating_sub(1)).collect();
    fitted.push(ELLIPSIS);
    fitted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::message_elem::{ImageElem, NotificationElem, PlaceholderElem, TextElem};

    fn message(content: Option<Elem>) -> IMMessage {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.content = content;
        message
    }

    fn text(body: &str) -> IMMessage {
        message(Some(Elem::Text(TextElem {
            text: body.into(),
            mentions: vec![],
        })))
    }

    fn image() -> IMMessage {
        message(Some(Elem::Image(ImageElem {
            source: None,
            thumbnail: None,
            description: String::new(),
            duration_ms: None,
        })))
    }

    fn facts<'a>(title: Option<&'a str>, sender: Option<&'a str>) -> MessageFacts<'a> {
        MessageFacts {
            title,
            sender,
            mentioned: false,
            count: 1,
            preview: NotificationPreview::Full,
        }
    }

    fn render(copy: &AlertCopy, lang: Lang) -> (String, String) {
        let AlertText { title, body } = copy.render(lang);
        (title, body)
    }

    fn pair(title: &str, body: &str) -> (String, String) {
        (title.to_owned(), body.to_owned())
    }

    /// 单聊：标题是对方，正文是内容；群聊：标题是群，正文前带发送者。
    #[test]
    fn titles_are_local_and_groups_name_the_sender() {
        let direct = AlertCopy::message(&text("今晚七点见"), &facts(Some("小明"), None));
        assert_eq!(render(&direct, Lang::Zh), pair("小明", "今晚七点见"));
        let group = AlertCopy::message(&text("今天几点开会"), &facts(Some("项目群"), Some("小明")));
        assert_eq!(
            render(&group, Lang::Zh),
            pair("项目群", "小明: 今天几点开会")
        );
        let untitled = AlertCopy::message(&text("hi"), &facts(None, None));
        assert_eq!(render(&untitled, Lang::En), pair("New message", "hi"));
    }

    /// 只有类型的内容按读者语言用占位；带说明的占位后接说明。
    #[test]
    fn type_only_content_uses_placeholders() {
        let copy = AlertCopy::message(&image(), &facts(Some("小明"), None));
        assert_eq!(render(&copy, Lang::Zh), pair("小明", "[图片]"));
        assert_eq!(render(&copy, Lang::En), pair("小明", "[Image]"));
        let blank = AlertCopy::message(&text("   "), &facts(Some("小明"), None));
        assert_eq!(render(&blank, Lang::Zh), pair("小明", "[消息]"));
        let undecoded = AlertCopy::message(&message(None), &facts(Some("小明"), None));
        assert_eq!(
            render(&undecoded, Lang::Zh),
            pair("小明", "你收到一条新消息")
        );
    }

    /// 点名：正文前加标记，文字自己写了就不重复；条数多于 1 时标题前带条数。
    #[test]
    fn mentions_and_counts_decorate_the_copy() {
        let mut facts = facts(Some("项目群"), Some("小明"));
        facts.mentioned = true;
        facts.count = 3;
        let copy = AlertCopy::message(&text("看下这个"), &facts);
        assert_eq!(
            render(&copy, Lang::Zh),
            pair("[3条] 项目群", "[有人@你] 小明: 看下这个")
        );
        let said = AlertCopy::message(&text("[有人@你] 看下这个"), &facts);
        assert_eq!(render(&said, Lang::Zh).1, "小明: [有人@你] 看下这个");
    }

    /// 预览：只显示发送者时正文是「发来一条消息」，点名标记保留；隐藏时通用标题与正文。
    #[test]
    fn the_preview_setting_decides_what_shows() {
        let mut group = facts(Some("项目群"), Some("小明"));
        group.mentioned = true;
        group.preview = NotificationPreview::SenderOnly;
        let copy = AlertCopy::message(&text("机密"), &group);
        assert_eq!(
            render(&copy, Lang::Zh),
            pair("项目群", "[有人@你] 小明发来一条消息")
        );
        assert_eq!(
            render(&copy, Lang::En),
            pair("项目群", "[Mentioned you] 小明 sent you a message")
        );
        let mut direct = facts(Some("小明"), None);
        direct.preview = NotificationPreview::SenderOnly;
        let copy = AlertCopy::message(&text("机密"), &direct);
        assert_eq!(render(&copy, Lang::Zh), pair("小明", "发来一条消息"));
        group.preview = NotificationPreview::Hidden;
        let copy = AlertCopy::message(&text("机密"), &group);
        assert_eq!(render(&copy, Lang::Zh), pair("新消息", "你收到一条新消息"));
    }

    /// E2EE 密文与通知类内容：前者一律通用文案，后者用自己的标题正文。
    #[test]
    fn ciphertext_is_withheld_and_notices_speak_for_themselves() {
        let cipher = message(Some(Elem::Placeholder(PlaceholderElem {
            reason: E2EE_PLACEHOLDER_REASON.into(),
            payload: vec![],
            fallback_text: "secret".into(),
            metadata: Default::default(),
        })));
        let copy = AlertCopy::message(&cipher, &facts(Some("小明"), None));
        assert_eq!(render(&copy, Lang::Zh), pair("新消息", "你收到一条新消息"));

        let notice = |title: &str, body: &str| {
            message(Some(Elem::Notification(NotificationElem {
                title: title.into(),
                body: body.into(),
                notification_type: "social.relation.friend_request_created".into(),
                data: Default::default(),
                target_user_ids: vec![],
                target_role_id: String::new(),
                notify_all: false,
                persistent: true,
                show_in_list: true,
                show_badge: true,
                play_sound: true,
            })))
        };
        let copy = AlertCopy::message(
            &notice("好友申请", "小明请求添加你为好友"),
            &facts(None, None),
        );
        assert_eq!(
            render(&copy, Lang::Zh),
            pair("好友申请", "小明请求添加你为好友")
        );
        let copy = AlertCopy::message(&notice("", ""), &facts(Some("通知"), None));
        assert_eq!(
            render(&copy, Lang::En),
            pair("通知", "You have a new notification")
        );
    }

    /// 撤回与摘要的文案。
    #[test]
    fn recall_and_digest_copies() {
        let copy = AlertCopy::recalled(Some("项目群"), NotificationPreview::Full, 2);
        assert_eq!(
            render(&copy, Lang::Zh),
            pair("[2条] 项目群", "撤回了一条消息")
        );
        let copy = AlertCopy::recalled(Some("项目群"), NotificationPreview::Hidden, 1);
        assert_eq!(
            render(&copy, Lang::En),
            pair("New message", "Message recalled")
        );
        let copy = AlertCopy::digest(7, 42);
        assert_eq!(
            render(&copy, Lang::Zh),
            pair("新消息", "7 个会话有 42 条新消息")
        );
    }

    /// 清理：控制字符与双向覆盖去掉，空白收成一个，超长截断带省略号。
    #[test]
    fn text_is_cleaned_and_fitted() {
        assert_eq!(clean(" a\n\tb\u{202E}c ", 64).as_deref(), Some("a bc"));
        assert_eq!(clean("\u{200B}\n", 64), None);
        let long = "字".repeat(200);
        let fitted = clean(&long, BODY_MAX_CHARS).unwrap();
        assert_eq!(fitted.chars().count(), BODY_MAX_CHARS);
        assert!(fitted.ends_with(ELLIPSIS));
    }
}
