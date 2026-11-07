//! 统一错误类型 — 基于 flare-core `common::error`，与框架一致
//!
//! 本 crate 直接使用 `FlareError` 与 `Result`，不再定义独立 `SdkError`。
//! 支持从 proto `RpcStatus`（code + message）映射为 `FlareError`。

pub use flare_core::common::error::LocalizedError;
pub use flare_core::common::error::{
    ClientError, ErrorBuilder, ErrorCode, FlareError, Result, ServerError,
};

/// Core 因租户不可用（停用、删除中）拒绝建连、踢线或拒绝请求时给出的类型化原因。
pub const TENANT_UNAVAILABLE: &str = "TENANT_UNAVAILABLE";

/// 同一账号在同一平台的新设备登录，本设备被踢下线时的类型化原因（「已在别处登录」）。
///
/// SDK 收到它就把 `KickedOff { reason: "DEVICE_CONFLICT" }` 交给应用并终止会话：不自动重连
/// （连回去会把新设备再顶下线），恢复要由应用重新登录。
pub const DEVICE_CONFLICT: &str = flare_core::common::kick::DEVICE_CONFLICT_REASON;

/// 建连 / 重连错误是否表示租户不可用。
///
/// 按错误码识别：网关以 `TENANT_UNAVAILABLE` 拒绝 CONNECT 时，flare-core 把它映射为
/// `PERMISSION_DENIED`（`connect_rejection_error`），socket 层原样保留这个错误码；网关拒绝 CONNECT
/// 不会因为别的原因用 `PERMISSION_DENIED`。以前在错误文本里找 `TENANT_UNAVAILABLE`，地址、服务端
/// 文案里碰巧带上这几个字就会误判，换了措辞又会漏判。
///
/// 它不是鉴权失败：换令牌、自动重连都无济于事，只会反复打到网关。SDK 据此停止重连并把
/// `KickedOff { reason: "TENANT_UNAVAILABLE" }` 交给应用。
pub fn is_tenant_unavailable(err: &FlareError) -> bool {
    err.code() == Some(ErrorCode::PermissionDenied)
}

/// 从 proto RpcStatus 的 code + message 映射为 FlareError
///
/// 与 `errors.proto` 中 ErrorCode 对齐；未命中时使用 `message` 构造 GeneralError。
///
/// # Example
///
/// ```ignore
/// return Err(from_rpc_status(rpc_status.code, rpc_status.message));
/// ```
#[inline]
pub fn from_rpc_status(code: i32, message: impl Into<String>) -> FlareError {
    use flare_core::common::error::ErrorCode as EC;
    let msg = message.into();
    let code = match code {
        2 => return FlareError::general_error("操作已取消"),
        10 | 12 => EC::InvalidParameter,
        11 => EC::InvalidParameter,
        13 => EC::OperationNotSupported,
        20 => EC::AuthenticationFailed,
        21 => EC::PermissionDenied,
        22 | 23 => EC::TokenExpired,
        30 => EC::UserNotFound,
        31 | 32 => EC::GeneralError,
        33 | 34 => EC::ResourceExhausted,
        40 | 41 => EC::MessageRateLimitExceeded,
        42 | 43 => EC::OperationTimeout,
        50 => EC::OperationTimeout,
        51 | 52 => EC::ServiceUnavailable,
        60 => EC::InternalError,
        61 | 62 => EC::OperationNotSupported,
        _ => return FlareError::localized(EC::GeneralError, msg),
    };
    FlareError::localized(code, msg)
}

/// Error param naming the messages an operation refused, so a host can say which ones without
/// reading the reason text: the ids joined by `,`. The binding runtime hands it over as the typed
/// list `refusedMessageIds` (`bindings/contract/errors.json`); attach it with [`refusing_messages`],
/// read it with [`refused_message_ids`].
pub const REFUSED_MESSAGE_IDS: &str = "refusedMessageIds";

/// Most ids one error names. Enough for any selection a person makes, and it bounds what an error
/// carries across a binding (at most about 64 KiB).
pub const MAX_REFUSED_MESSAGE_IDS: usize = 500;

/// Longest id an error names, in bytes.
pub const MAX_REFUSED_MESSAGE_ID_BYTES: usize = 128;

/// A message id as an error may name it: 1 to [`MAX_REFUSED_MESSAGE_ID_BYTES`] bytes of ASCII
/// letters, digits, `.`, `_`, `:` or `-` (server ids and client message ids alike), so it can
/// never split, smuggle markup or run long.
fn is_namable_message_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_REFUSED_MESSAGE_ID_BYTES
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// `error` naming the messages it refused ([`REFUSED_MESSAGE_IDS`]): in order, each once.
///
/// Names none unless there are 1 to [`MAX_REFUSED_MESSAGE_IDS`] ids and every one is well formed:
/// part of the list would misstate how many were refused. Only a localized error carries params.
pub fn refusing_messages<I, S>(error: FlareError, ids: I) -> FlareError
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut named: Vec<String> = Vec::new();
    for id in ids {
        let id = id.as_ref();
        if !is_namable_message_id(id) {
            return error;
        }
        if !named.iter().any(|seen| seen == id) {
            named.push(id.to_owned());
        }
    }
    if named.is_empty() || named.len() > MAX_REFUSED_MESSAGE_IDS {
        return error;
    }
    match error {
        FlareError::Localized {
            code,
            reason,
            details,
            params,
            timestamp,
        } => {
            let mut params = params.unwrap_or_default();
            params.insert(REFUSED_MESSAGE_IDS.to_owned(), named.join(","));
            FlareError::Localized {
                code,
                reason,
                details,
                params: Some(params),
                timestamp,
            }
        }
        other => other,
    }
}

/// The messages `error` refused, as [`refusing_messages`] named them; `None` when it names none,
/// more than [`MAX_REFUSED_MESSAGE_IDS`], or any id that is not well formed.
pub fn refused_message_ids(error: &FlareError) -> Option<Vec<String>> {
    let FlareError::Localized {
        params: Some(params),
        ..
    } = error
    else {
        return None;
    };
    let ids: Vec<String> = params
        .get(REFUSED_MESSAGE_IDS)?
        .split(',')
        .map(str::to_owned)
        .collect();
    (ids.len() <= MAX_REFUSED_MESSAGE_IDS && ids.iter().all(|id| is_namable_message_id(id)))
        .then_some(ids)
}

/// Error param carrying how long the server asked to wait before retrying, in milliseconds
/// (`OVERLOADED`: the IM gateway's `retry_after_ms`, an HTTP `Retry-After`). The binding runtime
/// hands it over as the number `retryAfterMs` (`bindings/contract/errors.json`); attach it with
/// [`retrying_after`], read it with [`retry_after_ms`].
pub const RETRY_AFTER_MS: &str = "retryAfterMs";

/// Longest wait an error passes on: a day. A server asking for more is clamped, so the value stays
/// exact in a JavaScript number and a host never schedules something absurd.
pub const MAX_RETRY_AFTER_MS: u64 = 24 * 60 * 60 * 1_000;

/// `error` carrying the server's retry wait ([`RETRY_AFTER_MS`]), clamped to
/// [`MAX_RETRY_AFTER_MS`]. Only a localized error carries params.
pub fn retrying_after(error: FlareError, retry_after_ms: u64) -> FlareError {
    match error {
        FlareError::Localized {
            code,
            reason,
            details,
            params,
            timestamp,
        } => {
            let mut params = params.unwrap_or_default();
            params.insert(
                RETRY_AFTER_MS.to_owned(),
                retry_after_ms.min(MAX_RETRY_AFTER_MS).to_string(),
            );
            FlareError::Localized {
                code,
                reason,
                details,
                params: Some(params),
                timestamp,
            }
        }
        other => other,
    }
}

/// The server's retry wait `error` carries ([`retrying_after`]); `None` when it carries none.
pub fn retry_after_ms(error: &FlareError) -> Option<u64> {
    let FlareError::Localized {
        params: Some(params),
        ..
    } = error
    else {
        return None;
    };
    params
        .get(RETRY_AFTER_MS)?
        .parse::<u64>()
        .ok()
        .map(|ms| ms.min(MAX_RETRY_AFTER_MS))
}

/// 建连失败里链路自己会恢复的那些：链路状态机保留会话、按退避重试，所以 `login` / `connect` 遇到它们
/// 照常成功，在不在线只由 `connection://status` 表达（waitingForNetwork / reconnecting）。
///
/// 没网、连不上（各类连接与网络错误，链路统一归为 `CONNECTION_FAILED`）、握手超时、服务端限流、服务暂不
/// 可用（含接入清单、令牌暂时拿不到）。鉴权失败且续期没换到、需要升级、配置错误、被踢等终态错误不在内。
/// 社交 SDK 判断「核心保留了会话」也用这一份。
pub fn link_keeps_session(error: &FlareError) -> bool {
    matches!(
        error.code(),
        Some(
            ErrorCode::NetworkUnreachable
                | ErrorCode::ConnectionFailed
                | ErrorCode::ConnectionTimeout
                | ErrorCode::ConnectionClosed
                | ErrorCode::ConnectionRefused
                | ErrorCode::NetworkError
                | ErrorCode::NetworkTimeout
                | ErrorCode::NetworkConnectionLost
                | ErrorCode::Overloaded
                | ErrorCode::ServiceUnavailable
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_failures_the_link_retries_keep_the_session() {
        for code in [
            ErrorCode::NetworkUnreachable,
            ErrorCode::ConnectionFailed,
            ErrorCode::ConnectionTimeout,
            ErrorCode::ConnectionClosed,
            ErrorCode::ConnectionRefused,
            ErrorCode::NetworkError,
            ErrorCode::NetworkTimeout,
            ErrorCode::NetworkConnectionLost,
            ErrorCode::Overloaded,
            ErrorCode::ServiceUnavailable,
        ] {
            assert!(
                link_keeps_session(&FlareError::localized(code, "x")),
                "{code:?}"
            );
        }
        for code in [
            ErrorCode::AuthenticationFailed,
            ErrorCode::TokenExpired,
            ErrorCode::UpgradeRequired,
            ErrorCode::PermissionDenied,
            ErrorCode::ConfigurationError,
            ErrorCode::NotConnected,
        ] {
            assert!(
                !link_keeps_session(&FlareError::localized(code, "x")),
                "{code:?}"
            );
        }
    }

    #[test]
    fn an_overload_carries_the_retry_wait_clamped_to_a_day() {
        let overloaded = FlareError::localized(ErrorCode::Overloaded, "OVERLOADED");
        assert_eq!(retry_after_ms(&overloaded), None);
        let waiting = retrying_after(overloaded, 1_500);
        assert_eq!(retry_after_ms(&waiting), Some(1_500));
        assert_eq!(waiting.code(), Some(ErrorCode::Overloaded));
        assert_eq!(
            retry_after_ms(&retrying_after(waiting, u64::MAX)),
            Some(MAX_RETRY_AFTER_MS)
        );
    }

    fn refusal() -> FlareError {
        FlareError::localized(ErrorCode::InvalidParameter, "sdk.example.refused")
    }

    fn param(error: &FlareError) -> Option<&str> {
        match error {
            FlareError::Localized {
                params: Some(params),
                ..
            } => params.get(REFUSED_MESSAGE_IDS).map(String::as_str),
            _ => None,
        }
    }

    #[test]
    fn an_error_names_the_messages_it_refused_in_order_and_once() {
        let error = refusing_messages(
            refusal(),
            [
                "s-2",
                "6b1f0c2e9a4d4f7e8c3b2a1d0e9f8a7b",
                "s-2",
                "msg:42.a_b",
            ],
        );
        assert_eq!(
            refused_message_ids(&error),
            Some(vec![
                "s-2".to_owned(),
                "6b1f0c2e9a4d4f7e8c3b2a1d0e9f8a7b".to_owned(),
                "msg:42.a_b".to_owned(),
            ])
        );
        assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
        assert_eq!(error.reason(), "sdk.example.refused");
    }

    #[test]
    fn a_list_it_cannot_name_whole_names_nothing() {
        let too_long = "a".repeat(MAX_REFUSED_MESSAGE_ID_BYTES + 1);
        let too_many: Vec<String> = (0..=MAX_REFUSED_MESSAGE_IDS)
            .map(|i| format!("m{i}"))
            .collect();
        let cases: [&[&str]; 6] = [
            &[],
            &["s-1", ""],
            &["s-1", "a,b"],
            &["s-1", "<b>"],
            &["s-1", "id with space"],
            &["s-1", "\u{7f5c}"],
        ];
        for ids in cases {
            assert_eq!(param(&refusing_messages(refusal(), ids)), None, "{ids:?}");
        }
        assert_eq!(
            param(&refusing_messages(refusal(), [too_long.as_str()])),
            None
        );
        assert_eq!(param(&refusing_messages(refusal(), &too_many)), None);
        let most: Vec<String> = too_many[..MAX_REFUSED_MESSAGE_IDS].to_vec();
        assert_eq!(
            refused_message_ids(&refusing_messages(refusal(), &most)).map(|ids| ids.len()),
            Some(MAX_REFUSED_MESSAGE_IDS)
        );
        let edge = "a".repeat(MAX_REFUSED_MESSAGE_ID_BYTES);
        assert!(refused_message_ids(&refusing_messages(refusal(), [edge.as_str()])).is_some());
        assert!(param(&refusing_messages(FlareError::system("boom"), ["s-1"])).is_none());
    }

    #[test]
    fn only_a_well_formed_param_reads_back() {
        for raw in ["", "s-1,", "s-1,,s-2", "s-1, s-2", "s-1;s-2"] {
            let mut params = std::collections::HashMap::new();
            params.insert(REFUSED_MESSAGE_IDS.to_owned(), raw.to_owned());
            let error = FlareError::Localized {
                code: ErrorCode::InvalidParameter,
                reason: "x".into(),
                details: None,
                params: Some(params),
                timestamp: Default::default(),
            };
            assert_eq!(refused_message_ids(&error), None, "{raw:?}");
        }
        assert_eq!(refused_message_ids(&refusal()), None);
        assert_eq!(refused_message_ids(&FlareError::system("boom")), None);
    }
}
