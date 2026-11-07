//! 错误转换 - 统一错误模型
//!
//! 稳定错误码由 `contract/errors.json` 生成；本模块仅保留 FlareError 构造辅助。

pub use crate::generated::errors::*;

use crate::types::{FlareError, FlareString};
use flare_im_core_sdk::FlareError as SdkError;

/// 从 SDK FlareError 创建 C ABI FlareError；`details_json` 是结构化错误
/// `{code, reason, retryable, retryAfterMs?, httpStatus?, remoteCode?, machineReason?, refusedMessageIds?}`
/// （`contract/errors.json`），宿主据此识别错误，不解析 `message`。
#[inline]
pub fn make_error(err: &SdkError) -> FlareError {
    use std::fmt::Write;

    let code = err
        .code()
        .map(error_code_to_c)
        .unwrap_or(FLARE_ERR_INTERNAL);

    let mut message = String::new();
    let _ = write!(&mut message, "{}", err);

    FlareError {
        code,
        message: FlareString::from_rust_string(message),
        details_json: FlareString::from_rust_string(
            flare_im_core_sdk_bindings_runtime::sdk_error_details(err).to_string(),
        ),
    }
}

/// 创建简单错误
#[inline]
pub fn make_simple_error(code: i32, message: &str) -> FlareError {
    FlareError {
        code,
        message: FlareString::from_rust_string(message.to_string()),
        details_json: FlareString::default(),
    }
}

/// 创建成功结果 (null error)
#[inline]
pub fn make_success() -> *const FlareError {
    std::ptr::null()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::flare_error_free;
    use flare_im_core_sdk::ErrorCode;

    fn c_code_of(err: &SdkError) -> i32 {
        let converted = make_error(err);
        let code = converted.code;
        flare_error_free(converted);
        code
    }

    fn details_of(err: &SdkError) -> serde_json::Value {
        let converted = make_error(err);
        let details = unsafe {
            std::str::from_utf8(std::slice::from_raw_parts(
                converted.details_json.ptr as *const u8,
                converted.details_json.len,
            ))
            .expect("utf-8")
            .to_owned()
        };
        flare_error_free(converted);
        serde_json::from_str(&details).expect("json")
    }

    /// `details_json` 带着核心错误码、原因与参数：插件拒绝码（`machineReason`）不必从文本里找。
    #[test]
    fn details_json_carries_the_structured_error() {
        let details = details_of(&SdkError::localized(
            ErrorCode::PermissionDenied,
            "sdk.plugin.disabled",
        ));
        assert_eq!(details["code"], "PERMISSION_DENIED");
        assert_eq!(details["reason"], "sdk.plugin.disabled");
        assert!(details.get("refusedMessageIds").is_none());
    }

    /// 被拒的消息 id 是 `details_json` 里的字符串数组，宿主不必解析原因文本。
    #[test]
    fn details_json_names_the_refused_messages_as_a_list() {
        let err = flare_im_core_sdk::refusing_messages(
            SdkError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.forward.not_forwardable: s-2, s-3",
            ),
            ["s-2", "s-3"],
        );
        assert_eq!(c_code_of(&err), FLARE_ERR_INVALID_PARAM);
        let details = details_of(&err);
        assert_eq!(details["code"], "INVALID_PARAMETER");
        assert_eq!(
            details["refusedMessageIds"],
            serde_json::json!(["s-2", "s-3"])
        );
    }

    /// 过载（C 码 12）的 `details_json` 带着服务端要求的等待时间 `retryAfterMs`，宿主的
    /// OverloadedException 据此安排重试；服务端没给就不写，别的错误码也不写。
    #[test]
    fn an_overload_hands_the_retry_wait_to_the_host() {
        let overloaded = flare_im_core_sdk::retrying_after(
            SdkError::localized(ErrorCode::Overloaded, "OVERLOADED"),
            1_500,
        );
        assert_eq!(c_code_of(&overloaded), FLARE_ERR_OVERLOADED);
        let details = details_of(&overloaded);
        assert_eq!(details["code"], "OVERLOADED");
        assert_eq!(details["retryAfterMs"], 1_500);

        let without_wait = SdkError::localized(ErrorCode::Overloaded, "OVERLOADED");
        assert!(details_of(&without_wait).get("retryAfterMs").is_none());
        let not_overloaded = flare_im_core_sdk::retrying_after(
            SdkError::localized(ErrorCode::ServiceUnavailable, "SERVICE_UNAVAILABLE"),
            1_500,
        );
        assert!(details_of(&not_overloaded).get("retryAfterMs").is_none());
    }

    /// 连接建立失败、连接被关闭、网络连接丢失都是网络类失败：经 C ABI 交给宿主时是
    /// `FLARE_ERR_NETWORK_ERROR`，宿主据此提示「网络异常、可重试」。原先它们没有映射，落到
    /// `FLARE_ERR_INTERNAL`，与内部错误混在一起。
    #[test]
    fn connection_and_network_loss_errors_map_to_network_error() {
        for err in [
            SdkError::connection_failed("dial failed"),
            SdkError::connection_closed("peer closed"),
            SdkError::localized(ErrorCode::NetworkConnectionLost, "link lost"),
            SdkError::localized(ErrorCode::NetworkError, "network error"),
        ] {
            assert_eq!(c_code_of(&err), FLARE_ERR_NETWORK_ERROR, "{err:?}");
        }
        assert_eq!(
            c_code_of(&SdkError::localized(ErrorCode::InternalError, "bug")),
            FLARE_ERR_INTERNAL
        );
        assert_eq!(c_code_of(&SdkError::system("no code")), FLARE_ERR_INTERNAL);
    }

    /// 建连或网络层超时与操作超时同属超时：交给宿主时是 `FLARE_ERR_OPERATION_TIMEOUT`；连接被拒、
    /// 网络不可达（HTTP 请求根本没发出去）与正在重连都是网络类失败，是 `FLARE_ERR_NETWORK_ERROR`。
    /// 原先这五个核心码没有映射，落到 `FLARE_ERR_INTERNAL`。
    #[test]
    fn timeout_and_unreachable_errors_map_to_timeout_and_network_error() {
        for err in [
            SdkError::connection_timeout("negotiation timeout"),
            SdkError::localized(ErrorCode::NetworkTimeout, "read timeout"),
            SdkError::localized(ErrorCode::OperationTimeout, "operation timeout"),
        ] {
            assert_eq!(c_code_of(&err), FLARE_ERR_OPERATION_TIMEOUT, "{err:?}");
        }
        for err in [
            SdkError::localized(ErrorCode::ConnectionRefused, "refused"),
            SdkError::localized(ErrorCode::NetworkUnreachable, "offline"),
            SdkError::localized(ErrorCode::ConnectionReconnecting, "reconnecting"),
        ] {
            assert_eq!(c_code_of(&err), FLARE_ERR_NETWORK_ERROR, "{err:?}");
        }
    }

    /// 生成的映射与 `contract/errors.json` 逐条一致：契约里每个 `coreCodes` 都是真实的错误码，
    /// 且 `error_code_to_c` 把它映射到所在条目的 C 码。改了契约却没重新生成会在这里失败。
    #[test]
    fn generated_mapping_matches_the_error_contract() {
        let contract: serde_json::Value =
            serde_json::from_str(include_str!("../../contract/errors.json"))
                .expect("errors.json parses");
        let mut checked = 0;
        for entry in contract["cAbi"]["codes"].as_array().expect("cAbi.codes") {
            let c_code = entry["code"].as_i64().expect("code") as i32;
            for core in entry["coreCodes"].as_array().into_iter().flatten() {
                let name = core.as_str().expect("core code name");
                let mut wire = String::new();
                for (i, ch) in name.chars().enumerate() {
                    if ch.is_ascii_uppercase() && i > 0 {
                        wire.push('_');
                    }
                    wire.push(ch.to_ascii_uppercase());
                }
                let code: ErrorCode = serde_json::from_value(serde_json::Value::String(wire))
                    .unwrap_or_else(|e| panic!("{name} is not a core ErrorCode: {e}"));
                assert_eq!(error_code_to_c(code), c_code, "{name}");
                checked += 1;
            }
        }
        assert!(checked >= 22, "only {checked} core codes in the contract");
    }
}
