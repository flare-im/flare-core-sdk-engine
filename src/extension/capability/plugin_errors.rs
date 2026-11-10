//! Plugin call failures as localized keys.
//!
//! Core answers a failed plugin call with an HTTP status and a machine reason
//! (`PLUGIN_DISABLED`, …; see `http_error_from_response_status`). Hosts never
//! show those codes: the SDK turns each into one `sdk.plugin.*` key, the
//! `reason` of the returned [`FlareError::Localized`], and keeps the metadata
//! (`httpStatus`, `remoteCode`, `machineReason`) in its params. A plugin's own
//! business rejection (HTTP 422, reason = the plugin's error code) becomes
//! `sdk.plugin.rejected` with the plugin code in `machineReason`, for the
//! plugin contract's own error table.
//!
//! A platform code means what it says only with the HTTP status Core answers
//! it with ([`PLUGIN_ERROR_KEYS`]). The `PLUGIN_` and `CAPABILITY_` prefixes are
//! the platform's: a plugin answering such a code (Core rewrites it to
//! `PLUGIN_REJECTED`) never reads as a platform answer here either. Under 422
//! it is a plain rejection whose `machineReason` becomes `PLUGIN_REJECTED`, so
//! a plugin cannot make a client say the card was rolled back or drop its
//! catalog; under any other status it is not interpreted at all.
use crate::shared::error::{ErrorCode, FlareError};

/// Localized keys of plugin failures, by machine reason and the HTTP status
/// Core answers it with. Apps translate the keys; the list is part of the
/// client contract.
pub const PLUGIN_ERROR_KEYS: &[(&str, u16, ErrorCode, &str)] = &[
    (
        "PLUGIN_DISABLED",
        403,
        ErrorCode::PermissionDenied,
        "sdk.plugin.disabled",
    ),
    (
        "PLUGIN_FEATURE_DISABLED",
        403,
        ErrorCode::PermissionDenied,
        "sdk.plugin.feature_disabled",
    ),
    (
        "PLUGIN_NOT_INSTALLED",
        404,
        ErrorCode::OperationNotSupported,
        "sdk.plugin.not_installed",
    ),
    (
        "PLUGIN_PERMISSION_DENIED",
        403,
        ErrorCode::PermissionDenied,
        "sdk.plugin.permission_denied",
    ),
    (
        "PLUGIN_UNSUPPORTED_PLATFORM",
        403,
        ErrorCode::OperationNotSupported,
        "sdk.plugin.unsupported_platform",
    ),
    (
        "PLUGIN_CARD_UNBOUND",
        409,
        ErrorCode::PermissionDenied,
        "sdk.plugin.card_unbound",
    ),
    (
        "PLUGIN_CARD_PLATFORM_ONLY",
        403,
        ErrorCode::PermissionDenied,
        "sdk.plugin.card_platform_only",
    ),
    (
        "PLUGIN_CARD_NOT_POSTED",
        502,
        ErrorCode::MessageSendFailed,
        "sdk.plugin.card_not_posted",
    ),
    (
        "PLUGIN_MESSAGE_RECALLED",
        409,
        ErrorCode::MessageNotFound,
        "sdk.plugin.message_recalled",
    ),
    (
        PLUGIN_REJECTED,
        422,
        ErrorCode::OperationFailed,
        PLUGIN_REJECTED_KEY,
    ),
    (
        "CAPABILITY_RATE_LIMITED",
        429,
        ErrorCode::MessageRateLimitExceeded,
        "sdk.plugin.rate_limited",
    ),
    (
        "CAPABILITY_OUTCOME_UNKNOWN",
        503,
        ErrorCode::OperationTimeout,
        "sdk.plugin.outcome_unknown",
    ),
];
/// A plugin rejection without a code of its own, or with a code in the
/// platform's namespace (rewritten by Core, and by [`localize_plugin_error`]).
pub const PLUGIN_REJECTED: &str = "PLUGIN_REJECTED";
/// A plugin's own business rejection (HTTP 422).
pub const PLUGIN_REJECTED_KEY: &str = "sdk.plugin.rejected";
/// The card or action is not (or no longer) offered by the catalog.
pub const PLUGIN_UI_ACTION_UNAVAILABLE_KEY: &str = "sdk.plugin.ui_action_unavailable";
/// The plugin catalog could not be read.
pub const PLUGIN_CATALOG_UNREACHABLE_KEY: &str = "sdk.plugin.catalog_unreachable";

/// Every localized key the SDK reports for plugin calls, for apps' string
/// tables: the [`PLUGIN_ERROR_KEYS`] and the local ones.
pub fn plugin_error_keys() -> Vec<&'static str> {
    PLUGIN_ERROR_KEYS
        .iter()
        .map(|(_, _, _, key)| *key)
        .chain([
            PLUGIN_UI_ACTION_UNAVAILABLE_KEY,
            PLUGIN_CATALOG_UNREACHABLE_KEY,
            "sdk.plugin.invalid_ui_request",
            "sdk.plugin.invalid_message_state",
        ])
        .collect()
}

/// The machine reason Core gave for a failed call, if any.
pub fn machine_reason(error: &FlareError) -> Option<&str> {
    match error {
        FlareError::Localized {
            params: Some(params),
            ..
        } => params.get("machineReason").map(String::as_str),
        _ => None,
    }
}

/// The plugin's own code of a business rejection: `sdk.plugin.rejected` with
/// the plugin's code in `machineReason`. `None` for anything else, including a
/// rejection Core rewrote to `PLUGIN_REJECTED` (a code in the platform's
/// namespace).
pub fn plugin_rejection_code(error: &FlareError) -> Option<&str> {
    match error {
        FlareError::Localized { reason, .. } if reason == PLUGIN_REJECTED_KEY => {
            machine_reason(error).filter(|code| *code != PLUGIN_REJECTED)
        }
        _ => None,
    }
}

fn http_status(error: &FlareError) -> Option<u16> {
    match error {
        FlareError::Localized {
            params: Some(params),
            ..
        } => params.get("httpStatus").and_then(|s| s.parse().ok()),
        _ => None,
    }
}

/// Codes only the platform answers with.
fn is_platform_namespace(code: &str) -> bool {
    code.starts_with("PLUGIN_") || code.starts_with("CAPABILITY_")
}

/// The platform answer of `error`: its row of [`PLUGIN_ERROR_KEYS`] when the
/// machine reason came with the HTTP status Core answers it with.
fn platform_answer(
    error: &FlareError,
) -> Option<&'static (&'static str, u16, ErrorCode, &'static str)> {
    let reason = machine_reason(error)?;
    let status = http_status(error)?;
    PLUGIN_ERROR_KEYS
        .iter()
        .find(|(code, expected, _, _)| *code == reason && *expected == status)
}

/// `error` with a plugin failure turned into its localized key; any other
/// error unchanged.
pub fn localize_plugin_error(error: FlareError) -> FlareError {
    let (code, key, rejected_as_platform) = match platform_answer(&error) {
        Some((_, _, code, key)) => (*code, *key, false),
        None => match (machine_reason(&error), http_status(&error)) {
            (Some(reason), Some(422)) => (
                ErrorCode::OperationFailed,
                PLUGIN_REJECTED_KEY,
                is_platform_namespace(reason),
            ),
            _ => return error,
        },
    };
    match error {
        FlareError::Localized {
            reason,
            details,
            mut params,
            timestamp,
            ..
        } => {
            if rejected_as_platform && let Some(params) = params.as_mut() {
                params.insert("machineReason".to_owned(), PLUGIN_REJECTED.to_owned());
            }
            FlareError::Localized {
                code,
                reason: key.to_owned(),
                // The transport's diagnostic text stays available, never as the shown text.
                details: details.or(Some(reason)),
                params,
                timestamp,
            }
        }
        other => other,
    }
}

/// Failures that mean the viewer's catalog is out of date: the plugin or the
/// feature the call belongs to was disabled, the plugin was uninstalled, or is
/// no longer allowed here.
pub fn invalidates_catalog(error: &FlareError) -> bool {
    matches!(
        platform_answer(error).map(|(code, _, _, _)| *code),
        Some(
            "PLUGIN_DISABLED"
                | "PLUGIN_FEATURE_DISABLED"
                | "PLUGIN_NOT_INSTALLED"
                | "PLUGIN_PERMISSION_DENIED"
                | "PLUGIN_UNSUPPORTED_PLATFORM"
        )
    )
}

/// Whether a background call (a card's viewer layer) is worth retrying after
/// a backoff: transport failures, timeouts with an unknown outcome, rate
/// limits and server errors. A plugin's or Core's definite answer (a disabled
/// plugin or feature, not installed, no permission, unbound card, a business
/// rejection) is not.
pub fn is_retryable(error: &FlareError) -> bool {
    match platform_answer(error).map(|(code, _, _, _)| *code) {
        Some("CAPABILITY_OUTCOME_UNKNOWN" | "CAPABILITY_RATE_LIMITED") => true,
        Some(_) => false,
        None => http_status(error).is_none_or(|status| status >= 500 || status == 429),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::transport::http::http_error_from_response_status;

    fn reason_of(error: &FlareError) -> (Option<ErrorCode>, String) {
        match error {
            FlareError::Localized { code, reason, .. } => (Some(*code), reason.clone()),
            other => (None, other.to_string()),
        }
    }

    #[test]
    fn plugin_failures_become_localized_keys_and_keep_their_metadata() {
        for (status, machine, key) in [
            (403, "PLUGIN_DISABLED", "sdk.plugin.disabled"),
            (
                403,
                "PLUGIN_FEATURE_DISABLED",
                "sdk.plugin.feature_disabled",
            ),
            (404, "PLUGIN_NOT_INSTALLED", "sdk.plugin.not_installed"),
            (
                403,
                "PLUGIN_PERMISSION_DENIED",
                "sdk.plugin.permission_denied",
            ),
            (
                403,
                "PLUGIN_UNSUPPORTED_PLATFORM",
                "sdk.plugin.unsupported_platform",
            ),
            (409, "PLUGIN_CARD_UNBOUND", "sdk.plugin.card_unbound"),
            (
                403,
                "PLUGIN_CARD_PLATFORM_ONLY",
                "sdk.plugin.card_platform_only",
            ),
            (502, "PLUGIN_CARD_NOT_POSTED", "sdk.plugin.card_not_posted"),
            (
                409,
                "PLUGIN_MESSAGE_RECALLED",
                "sdk.plugin.message_recalled",
            ),
            (429, "CAPABILITY_RATE_LIMITED", "sdk.plugin.rate_limited"),
            (
                503,
                "CAPABILITY_OUTCOME_UNKNOWN",
                "sdk.plugin.outcome_unknown",
            ),
            (422, "PLUGIN_REJECTED", "sdk.plugin.rejected"),
            (422, "POLL_CLOSED", "sdk.plugin.rejected"),
        ] {
            let raw = http_error_from_response_status(
                status,
                &format!(r#"{{"code":{status},"reason":"{machine}","message":"x"}}"#),
            );
            let error = localize_plugin_error(raw);
            assert_eq!(reason_of(&error).1, key, "{machine}");
            assert_eq!(machine_reason(&error), Some(machine));
            assert!(plugin_error_keys().contains(&key));
        }
        // Not a plugin failure: untouched.
        let other = http_error_from_response_status(500, r#"{"code":500,"reason":"BOOM"}"#);
        let text = other.to_string();
        assert_eq!(localize_plugin_error(other).to_string(), text);
        let plain = FlareError::system("offline");
        assert_eq!(
            localize_plugin_error(plain).to_string(),
            "系统错误: offline"
        );
    }

    #[test]
    fn availability_failures_invalidate_the_catalog() {
        let error = |status: u16, reason: &str| {
            localize_plugin_error(http_error_from_response_status(
                status,
                &format!(r#"{{"code":1,"reason":"{reason}"}}"#),
            ))
        };
        assert!(invalidates_catalog(&error(403, "PLUGIN_DISABLED")));
        assert!(invalidates_catalog(&error(403, "PLUGIN_FEATURE_DISABLED")));
        assert!(invalidates_catalog(&error(404, "PLUGIN_NOT_INSTALLED")));
        assert!(invalidates_catalog(&error(403, "PLUGIN_PERMISSION_DENIED")));
        assert!(invalidates_catalog(&error(
            403,
            "PLUGIN_UNSUPPORTED_PLATFORM"
        )));
        assert!(!invalidates_catalog(&error(409, "PLUGIN_CARD_UNBOUND")));
        assert!(!invalidates_catalog(&error(422, "POLL_CLOSED")));
        assert!(is_retryable(&error(503, "CAPABILITY_OUTCOME_UNKNOWN")));
        assert!(is_retryable(&error(429, "CAPABILITY_RATE_LIMITED")));
        assert!(is_retryable(&FlareError::system("offline")));
        assert!(is_retryable(&http_error_from_response_status(502, "")));
        assert!(!is_retryable(&error(409, "PLUGIN_CARD_UNBOUND")));
        assert!(!is_retryable(&error(403, "PLUGIN_DISABLED")));
        assert!(!is_retryable(&error(403, "PLUGIN_FEATURE_DISABLED")));
        assert!(!is_retryable(&error(422, "POLL_CLOSED")));
    }

    #[test]
    fn platform_codes_count_only_with_their_own_status() {
        let error = |status: u16, reason: &str| {
            localize_plugin_error(http_error_from_response_status(
                status,
                &format!(r#"{{"code":1,"reason":"{reason}","message":"x"}}"#),
            ))
        };
        // A plugin answering a platform code (a 422 the rewrite in Core missed): a plain
        // rejection. It never says "rolled back", never drops the catalog, never retries.
        for code in [
            "PLUGIN_CARD_NOT_POSTED",
            "PLUGIN_DISABLED",
            "PLUGIN_FEATURE_DISABLED",
            "PLUGIN_NOT_INSTALLED",
            "PLUGIN_CARD_UNBOUND",
            "PLUGIN_SOMETHING_NEW",
            "CAPABILITY_OUTCOME_UNKNOWN",
            "CAPABILITY_RATE_LIMITED",
        ] {
            let rejected = error(422, code);
            assert_eq!(reason_of(&rejected).1, PLUGIN_REJECTED_KEY, "{code}");
            assert_eq!(machine_reason(&rejected), Some(PLUGIN_REJECTED), "{code}");
            assert!(!invalidates_catalog(&rejected), "{code}");
            assert!(!is_retryable(&rejected), "{code}");
        }
        // A platform code under another platform code's status: not interpreted at all.
        for (status, code) in [
            (409, "PLUGIN_DISABLED"),
            (409, "PLUGIN_FEATURE_DISABLED"),
            (403, "PLUGIN_CARD_NOT_POSTED"),
            (404, "PLUGIN_CARD_UNBOUND"),
            (403, "CAPABILITY_OUTCOME_UNKNOWN"),
        ] {
            let raw = error(status, code);
            assert!(
                !reason_of(&raw).1.starts_with("sdk.plugin."),
                "{status} {code}"
            );
            assert!(!invalidates_catalog(&raw), "{status} {code}");
            assert!(!is_retryable(&raw), "{status} {code}");
        }
        // Server-side statuses stay retryable whatever reason they carry.
        assert!(is_retryable(&error(503, "PLUGIN_CARD_UNBOUND")));
        // The plugin's own codes keep their machine reason for the contract's table.
        assert_eq!(
            machine_reason(&error(422, "POLL_CLOSED")),
            Some("POLL_CLOSED")
        );
        // Every key once.
        let keys = plugin_error_keys();
        let unique: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len());
    }
}
