//! Core HTTP address resolution and safe retries.

use std::collections::HashMap;
use std::sync::Arc;

use super::IDEMPOTENCY_KEY_HEADER;
use crate::runtime::link::ConnectionRuntime;
use crate::shared::error::{ErrorCode, FlareError, Result};

/// 502/503/504 后重试前的退避。
pub const BAD_GATEWAY_BACKOFF_MS: u64 = 500;
/// 过载时愿意原地等的上限：服务端要求更久就直接把过载报给调用方，不让请求挂太久。
pub const MAX_OVERLOAD_WAIT_MS: u64 = 10_000;
/// 过载但没给 `Retry-After` 时的等待。
pub const DEFAULT_OVERLOAD_WAIT_MS: u64 = 1_000;

/// 请求头里是否带了幂等键（大小写不敏感）。
pub fn has_idempotency_key(headers: &HashMap<String, String>) -> bool {
    headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case(IDEMPOTENCY_KEY_HEADER) && !value.trim().is_empty()
    })
}

/// 方法对重试的安全性。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MethodSafety {
    /// GET / HEAD / PUT / DELETE：重复执行结果相同。
    Idempotent,
    /// 带 `Idempotency-Key` 的 POST / PATCH：服务端按键去重。
    KeyedWrite,
    /// 不带键的 POST / PATCH：重复执行可能产生两次副作用。
    UnkeyedWrite,
}

pub fn method_safety(method: &str, has_key: bool) -> MethodSafety {
    match method.to_ascii_uppercase().as_str() {
        "GET" | "HEAD" | "PUT" | "DELETE" | "OPTIONS" => MethodSafety::Idempotent,
        _ if has_key => MethodSafety::KeyedWrite,
        _ => MethodSafety::UnkeyedWrite,
    }
}

/// 一次请求失败的类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpFailure {
    /// 请求没发出去（DNS、TCP）。
    ConnectPhase,
    /// 本机在拨号时报告网络已断开或没有路由。可以换入口重试，健康簿不把它当作入口故障。
    NoNetworkRoute,
    /// TLS 握手或证书校验失败（含接入点 SPKI 钉扎不符）：请求同样没发出去，健康簿记作 TLS 失败。
    Tls,
    /// 发出去了但结果没回来（读超时、连接中途被重置）。
    Uncertain,
    /// 502 / 503 / 504。
    BadGateway,
    /// 429 或服务端标明 `OVERLOADED`。
    Overloaded { retry_after_ms: Option<u64> },
}

/// 重试决定（每个请求最多重试一次；401 续期另算）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRetry {
    /// 换一个接入点立即重试。
    AfterEndpointReplacement,
    /// 同一接入点等一会儿再试（过载：不换入口，防止级联迁移）。
    AfterOverload { delay_ms: u64 },
    /// 退避后重新选路重试。
    AfterBackoff { delay_ms: u64 },
    /// 不能重试，结果未知：返回 `NETWORK_UNCERTAIN`。
    Uncertain,
    /// 不重试，原样返回错误。
    GiveUp,
}

/// §3.9 重试表（纯函数）。
pub fn retry_decision(safety: MethodSafety, failure: HttpFailure) -> HttpRetry {
    match failure {
        HttpFailure::ConnectPhase | HttpFailure::NoNetworkRoute | HttpFailure::Tls => {
            HttpRetry::AfterEndpointReplacement
        }
        HttpFailure::Uncertain => match safety {
            MethodSafety::Idempotent | MethodSafety::KeyedWrite => {
                HttpRetry::AfterEndpointReplacement
            }
            MethodSafety::UnkeyedWrite => HttpRetry::Uncertain,
        },
        HttpFailure::BadGateway => match safety {
            MethodSafety::Idempotent | MethodSafety::KeyedWrite => HttpRetry::AfterBackoff {
                delay_ms: BAD_GATEWAY_BACKOFF_MS,
            },
            MethodSafety::UnkeyedWrite => HttpRetry::GiveUp,
        },
        // A rejection can arrive after an endpoint consumed a one-use input.
        // Without a replay guarantee, even Retry-After cannot make resending safe.
        HttpFailure::Overloaded { .. } if safety == MethodSafety::UnkeyedWrite => HttpRetry::GiveUp,
        HttpFailure::Overloaded { retry_after_ms } => {
            let wait = retry_after_ms.unwrap_or(DEFAULT_OVERLOAD_WAIT_MS);
            if wait > MAX_OVERLOAD_WAIT_MS {
                HttpRetry::GiveUp
            } else {
                HttpRetry::AfterOverload { delay_ms: wait }
            }
        }
    }
}

/// 按状态码与响应判断是否属于需要特殊处理的失败；其余状态码原样报给调用方。
pub fn classify_status(
    status: u16,
    retry_after: Option<&str>,
    machine_reason: Option<&str>,
) -> Option<HttpFailure> {
    let overloaded = status == 429 || machine_reason == Some("OVERLOADED");
    if overloaded {
        return Some(HttpFailure::Overloaded {
            retry_after_ms: retry_after.and_then(parse_retry_after_ms),
        });
    }
    matches!(status, 502..=504).then_some(HttpFailure::BadGateway)
}

/// 把过载（429 / `OVERLOADED`）交给调用方时的错误：错误码改为 `OVERLOADED`（宿主据此给出
/// OverloadedException 等类型化错误），HTTP 元数据原样保留，服务端给了等待时间就带上 `retryAfterMs`。
pub(crate) fn overloaded_error(error: FlareError, retry_after_ms: Option<u64>) -> FlareError {
    let error = match error {
        FlareError::Localized {
            reason,
            details,
            params,
            timestamp,
            ..
        } => FlareError::Localized {
            code: ErrorCode::Overloaded,
            reason,
            details,
            params,
            timestamp,
        },
        other => other,
    };
    match retry_after_ms {
        Some(retry_after_ms) => crate::shared::error::retrying_after(error, retry_after_ms),
        None => error,
    }
}

/// `Retry-After` 只认秒数形式（HTTP 日期形式在客户端时钟不可信时没有意义）。
pub fn parse_retry_after_ms(value: &str) -> Option<u64> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(|seconds| seconds.saturating_mul(1_000))
}

/// 结果未知时给调用方的错误。
pub fn network_uncertain(method: &str, url: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::NetworkUncertain,
        format!(
            "{method} {url}: the request may have been processed but the response was lost; \
             it is not idempotent and was not retried"
        ),
    )
}

/// 错误链里能认出 TLS 失败的字样（rustls 的证书错误、钉扎不符、握手告警）。与传输层的分类一致：
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use flare_core::common::cert::is_tls_failure;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn is_local_network_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    use std::io::{Error, ErrorKind};

    let mut current = Some(error);
    while let Some(error) = current {
        if error.downcast_ref::<Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                ErrorKind::NetworkDown | ErrorKind::NetworkUnreachable
            )
        }) {
            return true;
        }
        current = error.source();
    }
    false
}

/// 一次请求选中的入口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub revision: u32,
    pub base: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CoreHttpService {
    CoreApi,
    Media,
}

/// Core and Media resolve the latest address group.。
#[derive(Clone)]
pub struct HttpRouter {
    connection: Arc<ConnectionRuntime>,
    service: CoreHttpService,
}

impl HttpRouter {
    pub fn new(connection: Arc<ConnectionRuntime>, service: CoreHttpService) -> Self {
        Self {
            connection,
            service,
        }
    }

    /// Share the current endpoint snapshot with another Core service.。
    pub fn for_service(&self, service: CoreHttpService) -> Self {
        Self {
            connection: self.connection.clone(),
            service,
        }
    }

    /// 稳定的部署标识（不随接入点切换变化）：本地按「哪个部署」区分持久化数据时用。
    pub fn deployment_identity(&self) -> String {
        let deployment = self
            .connection
            .endpoints
            .as_ref()
            .map_or("", |store| store.deployment.as_str());
        format!("direct:{deployment}:{:?}", self.service)
    }

    pub fn route(&self, exclude: &[u32], pinned: Option<u32>) -> Result<Route> {
        if self.connection.network_unavailable() {
            return Err(FlareError::localized(
                ErrorCode::NetworkUnreachable,
                "the platform reports no network connection",
            ));
        }
        let snapshot = self.connection.endpoint_store()?.snapshot();
        let revision = snapshot.revision;
        if pinned.is_some_and(|pin| pin != revision) || exclude.contains(&revision) {
            return Err(FlareError::localized(
                ErrorCode::NetworkUnreachable,
                "the requested HTTP address revision is no longer available",
            ));
        }
        let base = match self.service {
            CoreHttpService::CoreApi => &snapshot.endpoints.base_url,
            CoreHttpService::Media => snapshot
                .endpoints
                .media_base_url
                .as_ref()
                .unwrap_or(&snapshot.endpoints.base_url),
        };
        Ok(Route {
            revision,
            base: base.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §3.9 重试表逐格。
    #[test]
    fn retry_table() {
        use HttpFailure::*;
        use MethodSafety::*;
        for safety in [Idempotent, KeyedWrite, UnkeyedWrite] {
            assert_eq!(
                retry_decision(safety, ConnectPhase),
                HttpRetry::AfterEndpointReplacement,
                "请求没发出去，任何方法都能换接入点重试"
            );
            assert_eq!(
                retry_decision(safety, NoNetworkRoute),
                HttpRetry::AfterEndpointReplacement
            );
            assert_eq!(
                retry_decision(safety, Tls),
                HttpRetry::AfterEndpointReplacement,
                "证书或钉扎不符时请求也没发出去，换接入点"
            );
        }
        for safety in [Idempotent, KeyedWrite] {
            assert_eq!(
                retry_decision(
                    safety,
                    Overloaded {
                        retry_after_ms: Some(2_000)
                    }
                ),
                HttpRetry::AfterOverload { delay_ms: 2_000 },
                "过载原地等，不换入口"
            );
        }
        assert_eq!(
            retry_decision(Idempotent, Uncertain),
            HttpRetry::AfterEndpointReplacement
        );
        assert_eq!(
            retry_decision(KeyedWrite, Uncertain),
            HttpRetry::AfterEndpointReplacement
        );
        assert_eq!(
            retry_decision(UnkeyedWrite, Uncertain),
            HttpRetry::Uncertain,
            "不带键的 POST 结果未知时绝不重发"
        );
        assert_eq!(
            retry_decision(Idempotent, BadGateway),
            HttpRetry::AfterBackoff {
                delay_ms: BAD_GATEWAY_BACKOFF_MS
            }
        );
        assert_eq!(retry_decision(UnkeyedWrite, BadGateway), HttpRetry::GiveUp);
        assert_eq!(
            retry_decision(
                Idempotent,
                Overloaded {
                    retry_after_ms: Some(60_000)
                }
            ),
            HttpRetry::GiveUp,
            "要等太久就把过载报给调用方"
        );
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn socket_without_a_network_route_is_distinct_from_a_refused_endpoint() {
        use std::io::{Error, ErrorKind};

        assert!(is_local_network_failure(&Error::from(
            ErrorKind::NetworkUnreachable
        )));
        assert!(is_local_network_failure(&Error::from(
            ErrorKind::NetworkDown
        )));
        assert!(!is_local_network_failure(&Error::from(
            ErrorKind::ConnectionRefused
        )));
    }

    #[test]
    fn overload_never_replays_an_unkeyed_write_even_with_retry_after() {
        for method in ["POST", "PATCH"] {
            let safety = method_safety(method, false);
            for retry_after in [None, Some("0"), Some("1"), Some("60")] {
                for (status, reason) in [(429, None), (503, Some("OVERLOADED"))] {
                    let failure = classify_status(status, retry_after, reason).unwrap();
                    assert_eq!(
                        retry_decision(safety, failure),
                        HttpRetry::GiveUp,
                        "{method} without a replay key must return the first overload response"
                    );
                }
            }
        }
    }

    /// 钉扎不符、证书不受信任要认成 TLS 失败；普通的连不上不算。最外层（带 URL）不参与判断。
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn tls_failures_are_recognised_in_the_error_chain() {
        #[derive(Debug)]
        struct Layer {
            text: &'static str,
            source: Option<Box<Layer>>,
        }
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.text)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.source
                    .as_deref()
                    .map(|layer| layer as &(dyn std::error::Error + 'static))
            }
        }
        let chain = |outer: &'static str, inner: &'static str| Layer {
            text: outer,
            source: Some(Box::new(Layer {
                text: "client error (Connect)",
                source: Some(Box::new(Layer {
                    text: inner,
                    source: None,
                })),
            })),
        };
        assert!(is_tls_failure(&chain(
            "error sending request for url (https://ap-1.example/ping)",
            "unexpected error: TLS certificate pin mismatch; leaf AAAA",
        )));
        assert!(is_tls_failure(&chain(
            "error sending request",
            "invalid peer certificate: UnknownIssuer",
        )));
        assert!(!is_tls_failure(&chain(
            "error sending request",
            "tcp connect error: Connection refused (os error 61)",
        )));
        assert!(
            !is_tls_failure(&chain(
                "error sending request for url (https://certificate.example/)",
                "tcp connect error: Connection refused (os error 61)",
            )),
            "the outer error carries the URL and is not inspected"
        );
    }

    /// 怎么都解析不完：建连时限到了，错误是连接阶段的超时。
    #[cfg(not(target_arch = "wasm32"))]
    struct NeverResolves;

    #[cfg(not(target_arch = "wasm32"))]
    impl reqwest::dns::Resolve for NeverResolves {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            Box::pin(std::future::pending())
        }
    }

    /// 放弃等待的过载交给调用方时是 `OVERLOADED`，带服务端给的等待时间，HTTP 元数据保留。
    #[test]
    fn a_given_up_overload_is_reported_as_overloaded_with_its_wait() {
        let error = overloaded_error(
            crate::infrastructure::transport::http::http_error_from_response_status(
                429,
                r#"{"code":6005,"reason":"OVERLOADED"}"#,
            ),
            Some(30_000),
        );
        assert_eq!(error.code(), Some(ErrorCode::Overloaded));
        assert_eq!(crate::shared::error::retry_after_ms(&error), Some(30_000));
        let params = error.as_localized().and_then(|error| error.params).unwrap();
        assert_eq!(params.get("httpStatus").map(String::as_str), Some("429"));
        assert_eq!(
            params.get("machineReason").map(String::as_str),
            Some("OVERLOADED")
        );
        let without_wait = overloaded_error(
            crate::infrastructure::transport::http::http_error_from_response_status(429, ""),
            None,
        );
        assert_eq!(crate::shared::error::retry_after_ms(&without_wait), None);
    }

    #[test]
    fn methods_and_statuses_are_classified() {
        assert_eq!(method_safety("get", false), MethodSafety::Idempotent);
        assert_eq!(method_safety("DELETE", false), MethodSafety::Idempotent);
        assert_eq!(method_safety("POST", true), MethodSafety::KeyedWrite);
        assert_eq!(method_safety("POST", false), MethodSafety::UnkeyedWrite);
        let mut headers = HashMap::new();
        assert!(!has_idempotency_key(&headers));
        headers.insert(
            "idempotency-key".to_owned(),
            crate::spi::new_idempotency_key(),
        );
        assert!(has_idempotency_key(&headers));

        assert_eq!(
            classify_status(429, Some("3"), None),
            Some(HttpFailure::Overloaded {
                retry_after_ms: Some(3_000)
            })
        );
        assert_eq!(
            classify_status(503, None, Some("OVERLOADED")),
            Some(HttpFailure::Overloaded {
                retry_after_ms: None
            })
        );
        assert_eq!(
            classify_status(502, None, None),
            Some(HttpFailure::BadGateway)
        );
        assert_eq!(classify_status(404, None, None), None);
        assert_eq!(parse_retry_after_ms("Wed, 21 Oct 2015 07:28:00 GMT"), None);
    }
}
