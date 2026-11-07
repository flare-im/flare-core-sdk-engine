//! 引导源簿：每个源的 ETag、上次成功时刻与降权到期。
//!
//! 持久化在 `AccessStore`：重启后「上次成功的排前面」「被劫持的源降权 24 小时」仍然成立，
//! 条件请求也不用整份重拉。条目不参与任何信任判定。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::time::DAY_MS;

/// 被判可疑的源降权多久：还可以试，但排在最后，它的 304 也不再算数。
pub const DEMOTION_MS: i64 = DAY_MS;
/// 记录的源数上限（清单 ≤16 个源、接入点几十个、TXT 指针若干）；超出时丢最久没用过的。
const MAX_TRACKED_SOURCES: usize = 64;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct SourceRecord {
    /// ETag 与它对应的文档 serial：只有本机正持有这份文档时才能拿它发条件请求，
    /// 否则一个 304 会让本机错过一份其实没有的文档。
    access_etag: Option<String>,
    access_serial: Option<u64>,
    keys_etag: Option<String>,
    keys_serial: Option<u64>,
    last_success_ms: Option<i64>,
    demoted_until_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct SourceBook {
    sources: BTreeMap<String, SourceRecord>,
    last_refresh_ms: Option<i64>,
}

impl SourceBook {
    fn entry(&mut self, key: &str) -> &mut SourceRecord {
        if !self.sources.contains_key(key) && self.sources.len() >= MAX_TRACKED_SOURCES {
            let oldest = self
                .sources
                .iter()
                .min_by_key(|(_, record)| record.last_success_ms.unwrap_or(i64::MIN))
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.sources.remove(&oldest);
            }
        }
        self.sources.entry(key.to_owned()).or_default()
    }

    pub fn is_demoted(&self, key: &str, now_ms: i64) -> bool {
        self.sources
            .get(key)
            .and_then(|record| record.demoted_until_ms)
            .is_some_and(|until| until > now_ms)
    }

    pub fn last_success_ms(&self, key: &str) -> Option<i64> {
        self.sources.get(key)?.last_success_ms
    }

    pub fn access_etag(&self, key: &str, current_serial: u64, now_ms: i64) -> Option<String> {
        let record = self.sources.get(key)?;
        (record.access_serial == Some(current_serial) && !self.is_demoted(key, now_ms))
            .then(|| record.access_etag.clone())
            .flatten()
    }

    pub fn keys_etag(
        &self,
        key: &str,
        keystate_serial: Option<u64>,
        now_ms: i64,
    ) -> Option<String> {
        let record = self.sources.get(key)?;
        (keystate_serial.is_some()
            && record.keys_serial == keystate_serial
            && !self.is_demoted(key, now_ms))
        .then(|| record.keys_etag.clone())
        .flatten()
    }

    /// 源确认了当前文档（304）。
    pub fn confirmed(&mut self, key: &str, now_ms: i64) {
        self.entry(key).last_success_ms = Some(now_ms);
    }

    /// 源返回了一份验签通过的文档。
    pub fn served_access(&mut self, key: &str, etag: Option<String>, serial: u64, now_ms: i64) {
        let record = self.entry(key);
        record.access_etag = etag;
        record.access_serial = Some(serial);
        record.last_success_ms = Some(now_ms);
        record.demoted_until_ms = None;
    }

    pub fn served_keys(&mut self, key: &str, etag: Option<String>, serial: u64) {
        let record = self.entry(key);
        record.keys_etag = etag;
        record.keys_serial = Some(serial);
    }

    pub fn demote(&mut self, key: &str, now_ms: i64) {
        self.entry(key).demoted_until_ms = Some(now_ms + DEMOTION_MS);
    }

    pub fn last_refresh_ms(&self) -> Option<i64> {
        self.last_refresh_ms
    }

    pub fn touch(&mut self, now_ms: i64) {
        self.last_refresh_ms = Some(now_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etags_only_apply_to_the_document_held_and_to_trusted_sources() {
        let mut book = SourceBook::default();
        book.served_access("a", Some("\"1\"".into()), 10, 100);
        assert_eq!(book.access_etag("a", 10, 100).as_deref(), Some("\"1\""));
        assert_eq!(book.access_etag("a", 11, 100), None);
        assert_eq!(book.access_etag("b", 10, 100), None);

        book.demote("a", 200);
        assert!(book.is_demoted("a", 200 + DEMOTION_MS - 1));
        assert!(!book.is_demoted("a", 200 + DEMOTION_MS));
        assert_eq!(book.access_etag("a", 10, 300), None);
        assert_eq!(
            book.access_etag("a", 10, 200 + DEMOTION_MS).as_deref(),
            Some("\"1\"")
        );

        // 再次拿到验签通过的文档：可疑标记解除。
        book.served_access("a", None, 12, 400);
        assert!(!book.is_demoted("a", 401));
    }

    #[test]
    fn the_book_is_bounded_and_survives_a_round_trip() {
        let mut book = SourceBook::default();
        for index in 0..MAX_TRACKED_SOURCES + 8 {
            book.confirmed(&format!("source-{index:03}"), index as i64);
        }
        assert_eq!(book.sources.len(), MAX_TRACKED_SOURCES);
        assert_eq!(book.last_success_ms("source-000"), None);
        book.touch(7);
        let text = serde_json::to_vec(&book).unwrap();
        assert_eq!(serde_json::from_slice::<SourceBook>(&text).unwrap(), book);
    }
}
