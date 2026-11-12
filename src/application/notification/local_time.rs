//! 设备当地时间：勿扰时段按它判断。
//!
//! 规则 crate 不认识时区，只要「一周中的第几分钟」；偏移由这里按系统时区算出（含夏令时），
//! 原生取操作系统的时区，浏览器取 `Date` 的时区偏移。

use flare_notify_rules::MinuteOfWeek;

/// `utc_ms` 这一刻设备所在时区相对 UTC 的偏移（分钟，东八区为 480）。读不出时区时按 UTC。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn utc_offset_minutes(utc_ms: i64) -> i32 {
    use chrono::{Local, TimeZone};

    Local
        .timestamp_millis_opt(utc_ms)
        .earliest()
        .map(|local| local.offset().local_minus_utc() / 60)
        .unwrap_or(0)
}

/// `utc_ms` 这一刻浏览器所在时区相对 UTC 的偏移（分钟，东八区为 480）。
#[cfg(target_arch = "wasm32")]
pub(crate) fn utc_offset_minutes(utc_ms: i64) -> i32 {
    // getTimezoneOffset 是「UTC 减当地」，东八区为 -480，方向与这里相反。
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(utc_ms as f64));
    -(date.get_timezone_offset() as i32)
}

/// 设备当地时间在一周中的位置。
pub(crate) fn local_minute_of_week(utc_ms: i64) -> MinuteOfWeek {
    MinuteOfWeek::from_utc_ms(utc_ms, utc_offset_minutes(utc_ms))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// 偏移落在真实时区的范围内（UTC-12 到 UTC+14），一周中的分钟落在一周之内。
    #[test]
    fn the_offset_is_a_real_time_zone() {
        let now = 1_700_000_000_000;
        let offset = utc_offset_minutes(now);
        assert!((-12 * 60..=14 * 60).contains(&offset), "{offset}");
        assert!(local_minute_of_week(now).get() < MinuteOfWeek::MINUTES_PER_WEEK);
    }
}
