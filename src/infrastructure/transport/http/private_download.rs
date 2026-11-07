//! Policy shared by native and browser object-capability reads. This module
//! knows no product, tenant, user or material semantics.
use std::collections::{HashMap, HashSet};

use crate::shared::error::{ErrorCode, FlareError, Result};

const MAX_PRIVATE_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn validate(
    url: &str,
    headers: &HashMap<String, String>,
    max_bytes: usize,
) -> Result<()> {
    // 带 userinfo 的地址在解析时就被拒绝。
    let target = flare_access::ParsedUrl::parse(url).map_err(|_| invalid())?;
    let local = target.host_str() == "localhost" || target.ip().is_some_and(|ip| ip.is_loopback());
    if url.len() > 8192
        || target.scheme() != "https" && !(target.scheme() == "http" && local)
        || target.fragment().is_some()
        || max_bytes == 0
        || max_bytes > MAX_PRIVATE_BYTES
        || headers.len() > 8
    {
        return Err(invalid());
    }
    let mut seen = HashSet::new();
    let mut size = 0usize;
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            || !seen.insert(lower.clone())
            || matches!(
                lower.as_str(),
                "authorization"
                    | "cookie"
                    | "cookie2"
                    | "proxy-authorization"
                    | "origin"
                    | "referer"
                    | "referrer"
                    | "host"
                    | "connection"
                    | "content-length"
                    | "content-type"
                    | "cache-control"
                    | "pragma"
                    | "user-agent"
                    | "accept-language"
                    | "forwarded"
                    | "x-user-id"
                    | "x-tenant-id"
                    | "x-app-id"
                    | "x-application-id"
                    | "x-device-id"
                    | "x-session-id"
                    | "x-trace-id"
                    | "x-request-id"
                    | "x-real-ip"
            )
            || lower.starts_with("proxy-")
            || lower.starts_with("sec-")
            || lower.starts_with("x-forwarded-")
            || value.is_empty()
            || value.len() > 4096
            || !value.bytes().all(|b| (0x20..=0x7e).contains(&b))
        {
            return Err(invalid());
        }
        size = size.saturating_add(name.len() + value.len());
    }
    if size > 8192 {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn invalid() -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        "invalid private download input",
    )
}
pub(super) fn rejected(status: u16) -> FlareError {
    FlareError::localized(
        if status == 403 {
            ErrorCode::PermissionDenied
        } else {
            ErrorCode::ServiceUnavailable
        },
        "private download rejected",
    )
}
pub(super) fn transport(timed_out: bool) -> FlareError {
    FlareError::localized(
        if timed_out {
            ErrorCode::OperationTimeout
        } else {
            ErrorCode::ServiceUnavailable
        },
        "private download transport failed",
    )
}
pub(super) fn configuration() -> FlareError {
    FlareError::localized(
        ErrorCode::ConfigurationError,
        "private download transport unavailable",
    )
}
pub(super) fn response() -> FlareError {
    FlareError::localized(
        ErrorCode::ServiceUnavailable,
        "private download response invalid",
    )
}
pub(super) fn too_large() -> FlareError {
    FlareError::localized(
        ErrorCode::ResourceExhausted,
        "private download byte limit exceeded",
    )
}
pub(super) fn content_type(value: &str) -> Result<String> {
    if value.is_empty() || value.len() > 256 || !value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return Err(response());
    }
    Ok(value.to_owned())
}
pub(super) fn append(bytes: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> Result<()> {
    let size = bytes.len().checked_add(chunk.len()).ok_or_else(too_large)?;
    if size > max_bytes {
        return Err(too_large());
    }
    bytes.try_reserve(chunk.len()).map_err(|_| too_large())?;
    bytes.extend_from_slice(chunk);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_download_rejects_ambient_identity_headers_and_unsafe_urls() {
        for name in [
            "Authorization",
            "COOKIE",
            "Referer",
            "Origin",
            "x-user-id",
            "x-session-id",
            "x-request-id",
            "X-Forwarded-For",
            "Cache-Control",
        ] {
            let headers = HashMap::from([(name.into(), "synthetic-secret".into())]);
            let error = validate(
                "https://example.test/private?token=synthetic-url",
                &headers,
                32,
            )
            .unwrap_err();
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
            assert!(!error.to_string().contains("synthetic"));
        }
        for url in [
            "http://example.test/private",
            "https://user:password@example.test/private",
            "https://example.test/private#fragment",
            "file:///private",
        ] {
            assert!(validate(url, &HashMap::new(), 32).is_err());
        }
        let explicit =
            HashMap::from([("x-object-capability".into(), "synthetic-capability".into())]);
        for url in [
            "https://example.test/private",
            "http://127.0.0.1:7929/private",
            "http://[::1]:7929/private",
        ] {
            validate(url, &explicit, 32).unwrap();
        }
    }

    #[test]
    fn private_download_errors_are_typed_and_never_include_remote_details() {
        assert_eq!(rejected(403).code(), Some(ErrorCode::PermissionDenied));
        for status in [301, 401, 404, 429, 500] {
            assert_eq!(rejected(status).code(), Some(ErrorCode::ServiceUnavailable));
        }
        assert_eq!(transport(true).code(), Some(ErrorCode::OperationTimeout));
        let mut bytes = vec![1, 2];
        append(&mut bytes, &[3], 3).unwrap();
        assert_eq!(
            append(&mut bytes, &[4], 3).unwrap_err().code(),
            Some(ErrorCode::ResourceExhausted)
        );
        assert_eq!(bytes, [1, 2, 3]);
    }
}
