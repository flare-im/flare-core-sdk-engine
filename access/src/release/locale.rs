//! 更新说明与封禁原因的语言选择（§7.8）：回退链 `zh-Hant-TW → zh-Hant → zh → en`。
//!
//! 链上都没有时再看同语言的其它写法（iOS 报 `zh-Hans-CN`，清单常写 `zh-CN`），最后取第一条，
//! 宁可显示别的语言也不让这段文字空着。

use std::collections::BTreeMap;

const LAST_RESORT: &str = "en";

fn normalize(locale: &str) -> String {
    locale.trim().replace('_', "-")
}

/// 从长到短逐段截断，以 `en` 结尾。
pub fn locale_chain(locale: &str) -> Vec<String> {
    let locale = normalize(locale);
    let mut chain = Vec::new();
    let mut current = locale.as_str();
    while !current.is_empty() {
        chain.push(current.to_owned());
        current = current.rfind('-').map_or("", |index| &current[..index]);
    }
    if !chain
        .iter()
        .any(|tag| tag.eq_ignore_ascii_case(LAST_RESORT))
    {
        chain.push(LAST_RESORT.to_owned());
    }
    chain
}

fn primary(tag: &str) -> &str {
    tag.split('-').next().unwrap_or(tag)
}

fn select_key<'a>(keys: &[&'a str], locale: Option<&str>) -> Option<&'a str> {
    let wanted = locale.map(normalize).unwrap_or_default();
    let chain = locale_chain(&wanted);
    let (exact, last_resort) = chain.split_at(chain.len() - 1);
    let find = |tag: &str| {
        keys.iter()
            .copied()
            .find(|key| key.eq_ignore_ascii_case(tag))
    };
    exact
        .iter()
        .find_map(|tag| find(tag))
        .or_else(|| {
            let language = primary(&wanted);
            keys.iter()
                .copied()
                .find(|key| !language.is_empty() && primary(key).eq_ignore_ascii_case(language))
        })
        .or_else(|| last_resort.iter().find_map(|tag| find(tag)))
        .or_else(|| keys.first().copied())
}

/// 按语言挑更新说明。
pub fn select_notes(notes: &BTreeMap<String, Vec<String>>, locale: Option<&str>) -> Vec<String> {
    let keys: Vec<&str> = notes.keys().map(String::as_str).collect();
    select_key(&keys, locale)
        .and_then(|key| notes.get(key))
        .cloned()
        .unwrap_or_default()
}

/// 按语言挑一段文字（封禁原因）。规则与更新说明相同。
pub fn select_text(texts: &BTreeMap<String, String>, locale: Option<&str>) -> Option<String> {
    let keys: Vec<&str> = texts.keys().map(String::as_str).collect();
    select_key(&keys, locale)
        .and_then(|key| texts.get(key))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notes(keys: &[&str]) -> BTreeMap<String, Vec<String>> {
        keys.iter()
            .map(|key| ((*key).to_owned(), vec![(*key).to_owned()]))
            .collect()
    }

    #[test]
    fn the_chain_truncates_then_ends_in_english() {
        assert_eq!(
            locale_chain("zh-Hant-TW"),
            vec!["zh-Hant-TW", "zh-Hant", "zh", "en"]
        );
        assert_eq!(locale_chain("en-GB"), vec!["en-GB", "en"]);
        assert_eq!(locale_chain("zh_CN"), vec!["zh-CN", "zh", "en"]);
        assert_eq!(locale_chain(""), vec!["en"]);
    }

    #[test]
    fn notes_follow_the_chain() {
        let pick = |keys: &[&str], locale| select_notes(&notes(keys), locale);
        assert_eq!(
            pick(&["en", "zh-Hant"], Some("zh-Hant-TW")),
            vec!["zh-Hant"]
        );
        assert_eq!(pick(&["en", "zh"], Some("zh-Hant-TW")), vec!["zh"]);
        assert_eq!(pick(&["en", "ja"], Some("zh-Hant-TW")), vec!["en"]);
        assert_eq!(pick(&["en", "zh-CN"], Some("zh-Hans-CN")), vec!["zh-CN"]);
        assert_eq!(pick(&["en", "zh-CN"], None), vec!["en"]);
        assert_eq!(pick(&["ja", "ko"], Some("de")), vec!["ja"]);
        assert_eq!(pick(&["zh-cn"], Some("zh-CN")), vec!["zh-cn"]);
        assert!(pick(&[], Some("en")).is_empty());
    }

    #[test]
    fn a_text_follows_the_same_chain() {
        let texts: BTreeMap<String, String> = [("en", "Security fix"), ("zh-CN", "安全修复")]
            .into_iter()
            .map(|(key, text)| (key.to_owned(), text.to_owned()))
            .collect();
        let pick = |locale| select_text(&texts, locale);
        assert_eq!(pick(Some("zh-Hans-CN")).as_deref(), Some("安全修复"));
        assert_eq!(pick(Some("ja")).as_deref(), Some("Security fix"));
        assert_eq!(pick(None).as_deref(), Some("Security fix"));
        assert_eq!(select_text(&BTreeMap::new(), Some("en")), None);
    }
}
