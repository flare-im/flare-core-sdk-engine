pub(crate) fn escaped_like_contains(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('%');
    for ch in value.chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('%');
    out
}

pub(crate) enum SqliteKeywordSearch {
    Fts(String),
    ContentLike(String),
}

pub(crate) fn sqlite_keyword_search(value: &str) -> Option<SqliteKeywordSearch> {
    let value = value.trim();
    if value.is_empty() || !value.chars().any(|ch| ch.is_alphanumeric()) {
        return None;
    }

    let searchable_chars = value
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .take(3)
        .count();
    if searchable_chars < 3 {
        return Some(SqliteKeywordSearch::ContentLike(escaped_like_contains(
            value,
        )));
    }

    fts5_phrase_query(value).map(SqliteKeywordSearch::Fts)
}

fn fts5_phrase_query(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || !value.chars().any(|ch| ch.is_alphanumeric()) {
        return None;
    }

    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    Some(out)
}
