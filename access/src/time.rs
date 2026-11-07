//! 时间换算。本 crate 不读系统时钟，所有「现在」都由调用方以 Unix 毫秒传入。

use chrono::{DateTime, SecondsFormat, Utc};

pub const SECOND_MS: i64 = 1_000;
pub const MINUTE_MS: i64 = 60 * SECOND_MS;
pub const HOUR_MS: i64 = 60 * MINUTE_MS;
pub const DAY_MS: i64 = 24 * HOUR_MS;

/// 解析 RFC 3339 时间为 Unix 毫秒；格式错误返回 `None`。
pub fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|time| time.timestamp_millis())
}

/// Unix 毫秒格式化为 RFC 3339（UTC，秒精度）。超出 chrono 范围时退回纪元，
/// 只用于开发模式合成清单与签名工具，不参与校验判定。
pub fn format_rfc3339_ms(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_round_trip() {
        let ms = parse_rfc3339_ms("2026-11-04T08:00:00Z").unwrap();
        assert_eq!(ms, 1_793_779_200_000);
        assert_eq!(format_rfc3339_ms(ms), "2026-11-04T08:00:00Z");
        assert_eq!(parse_rfc3339_ms("2026-11-04 08:00"), None);
    }
}
