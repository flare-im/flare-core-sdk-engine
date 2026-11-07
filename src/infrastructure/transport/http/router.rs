//! HTTP 按服务路由与安全重试（§3.9）。
//!
//! 每次请求按服务类型（coreApi / socialApi / media）向选路器取接入点：REST 跟随 IM 当前接入点
//! （同一 Cell、复用同一入口的连接），IM 不在线时按健康簿自己选。失败后的处理以「同一个写操作绝不
//! 被执行两次」为前提：请求没发出去（连接阶段失败）任何方法都可以换接入点重试；发出去了但结果没回来，
//! 只有幂等方法或带 `Idempotency-Key` 的 POST 才能重试，不带键的 POST 返回 `NETWORK_UNCERTAIN`，
//! 由用户决定。结果回写健康簿，与 IM 共用：一条链路的失败对另一条立即可见。
//!
//! TLS：信任根（系统根 + 配置的私有 CA）全局一份；SPKI 钉扎按接入点来自接入清单，只对本次请求选中的
//! 接入点生效，清单没给钉扎时只做 CA 校验。浏览器里 TLS 由浏览器负责，钉扎不生效。
//!
//! DNS：原生端解析失败时按接入点的 `ipHints` 直连（`ip_hints` 模块），有 DoH 答复时它排最前。URL
//! 不变，TLS 仍按主机名。

use std::collections::HashMap;
use std::sync::Arc;

use flare_access::{ApId, NeutralReason, Observation, Outcome, ServiceClass, ServiceKind};

use crate::runtime::link::AccessRuntime;
use crate::shared::error::{ErrorCode, FlareError, Result};

/// 幂等键请求头。社交服务端按 `(tenant, user, key)` 保存 24 小时结果，重试同键返回首次结果。
pub const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";
/// 请求所经接入点（媒体服务据此生成该接入点公网端点上的分片预签名）。
pub const ACCESS_POINT_HEADER: &str = "x-flare-ap";
/// 502/503/504 后重试前的退避。
pub const BAD_GATEWAY_BACKOFF_MS: u64 = 500;
/// 过载时愿意原地等的上限：服务端要求更久就直接把过载报给调用方，不让请求挂太久。
pub const MAX_OVERLOAD_WAIT_MS: u64 = 10_000;
/// 过载但没给 `Retry-After` 时的等待。
pub const DEFAULT_OVERLOAD_WAIT_MS: u64 = 1_000;

/// 生成一个新的幂等键（UUID v4）。同一个写操作的所有重试必须复用同一个键。
pub fn new_idempotency_key() -> String {
    uuid::Uuid::new_v4().to_string()
}

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
    OtherAccessPoint,
    /// 同一接入点等一会儿再试（过载：不换入口，防止级联迁移）。
    SameAccessPointAfter { delay_ms: u64 },
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
        HttpFailure::ConnectPhase | HttpFailure::Tls => HttpRetry::OtherAccessPoint,
        HttpFailure::Uncertain => match safety {
            MethodSafety::Idempotent | MethodSafety::KeyedWrite => HttpRetry::OtherAccessPoint,
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
                HttpRetry::SameAccessPointAfter { delay_ms: wait }
            }
        }
    }
}

/// 服务端要求升级：这个版本被门禁拦下，更新中心进入强制更新与只读。
pub const UPGRADE_REQUIRED_STATUS: u16 = 426;

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
/// 错误类型跨了好几层库，只能看文字。不看最外层：reqwest 的外层错误带 URL，主机名里可能恰好有这些词。
#[cfg(not(target_arch = "wasm32"))]
const TLS_FAILURE_MARKERS: &[&str] = &[
    "certificate",
    "pin mismatch",
    "handshake failure",
    "tls handshake",
    "fatal alert",
    "peer is incompatible",
];

/// 连接阶段的失败是不是 TLS 失败（证书、钉扎、握手）。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn is_tls_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = error.source();
    while let Some(error) = current {
        let text = error.to_string().to_ascii_lowercase();
        if TLS_FAILURE_MARKERS
            .iter()
            .any(|marker| text.contains(marker))
        {
            return true;
        }
        current = error.source();
    }
    false
}

/// 建连超时。总时限先到时 reqwest 不报成连接错误，所以只对建连时限比总时限短的客户端有用。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn is_dial_timeout(error: &reqwest::Error) -> bool {
    error.is_connect() && error.is_timeout()
}

/// 发送失败后让 DoH 查一次主机：TLS 失败，或拨号超时（黑洞地址是 DNS 污染的常见表现）。查询在后台，
/// 这次失败照常处理。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn report_to_doh(
    doh: &Arc<flare_core::client::dial::DohCache>,
    error: &reqwest::Error,
    dial_timed_out: bool,
) {
    let Some(host) = error.url().and_then(|url| url.host_str()) else {
        return;
    };
    if is_tls_failure(error) {
        doh.tls_failed(host);
    } else if dial_timed_out {
        doh.request(host);
    }
}

/// 一次请求选中的入口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub access_point_id: ApId,
    pub base: String,
    /// 该接入点的 SPKI 钉扎（来自接入清单；空表示只做 CA 校验）。
    pub spki_pins: Vec<String>,
}

/// 某一类服务的路由器（共享接入运行时）。
#[derive(Clone)]
pub struct HttpRouter {
    access: Arc<AccessRuntime>,
    service: ServiceClass,
}

impl HttpRouter {
    pub fn new(access: Arc<AccessRuntime>, service: ServiceClass) -> Self {
        Self { access, service }
    }

    /// 同一接入运行时上的另一类服务。
    pub fn for_service(&self, service: ServiceClass) -> Self {
        Self {
            access: self.access.clone(),
            service,
        }
    }

    pub fn service(&self) -> ServiceClass {
        self.service
    }

    /// 稳定的部署标识（不随接入点切换变化）：本地按「哪个部署」区分持久化数据时用。
    pub fn deployment_identity(&self) -> String {
        let bootstrap_id = self
            .access
            .config()
            .map(|config| config.effective_bootstrap_id().to_owned())
            .unwrap_or_default();
        format!("access:{bootstrap_id}:{:?}", self.service)
    }

    /// 取本次请求的入口：`pinned` 时就用那个接入点（过载后原地重试），否则跟随 IM 当前接入点选路，
    /// 跳过 `exclude` 里已失败的。
    pub async fn route(&self, exclude: &[String], pinned: Option<&str>) -> Result<Route> {
        let kernel = self
            .access
            .kernel()
            .await
            .map_err(|error| error.to_error())?;
        let Some(kind) = self.service.http_service() else {
            return Err(FlareError::localized(
                ErrorCode::ConfigurationError,
                "IM is not an HTTP service",
            ));
        };
        if let Some(pinned) = pinned {
            let snapshot = kernel.directory().snapshot();
            if let Some((index, entry)) = snapshot.access_point(pinned)
                && let Some(base) = snapshot.endpoint(index, kind)
            {
                return Ok(Route {
                    access_point_id: entry.id.clone(),
                    base: base.to_owned(),
                    spki_pins: entry.spki_pins.clone(),
                });
            }
        }
        let excluded: Vec<&str> = exclude.iter().map(String::as_str).collect();
        let selection = self.access.select(&kernel, self.service, None, &excluded);
        let candidate = selection.first().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::NetworkUnreachable,
                format!("no reachable access point offers {}", kind.as_str()),
            )
        })?;
        let base = selection
            .http_base(candidate, self.service)
            .ok_or_else(|| {
                FlareError::localized(
                    ErrorCode::ConfigurationError,
                    format!(
                        "access point {} has no {} entry",
                        candidate.ap_id,
                        kind.as_str()
                    ),
                )
            })?;
        Ok(Route {
            access_point_id: candidate.ap_id.clone(),
            base: base.to_owned(),
            spki_pins: selection.access_point(candidate).spki_pins.clone(),
        })
    }

    /// 不经选路的完整 URL（媒体预签名直链指向某个接入点的公网入口）：按主机找到那个接入点，用它的
    /// 钉扎。主机不属于任何接入点（第三方对象存储等）时为空，只做 CA 校验。目录还没装配时同样为空。
    ///
    /// 这里用 `url` 而不是 `ParsedUrl`：真正连接的是 reqwest，它按 `url` 解析。解析失败会退回只做 CA
    /// 校验，所以两边必须对同一串字符得出同一个主机，用更严的解析器会把钉扎丢掉。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn spki_pins_for_url(&self, url: &str) -> Vec<String> {
        let Some(host) = url::Url::parse(url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
        else {
            return Vec::new();
        };
        let Some(kernel) = self.access.kernel_now() else {
            return Vec::new();
        };
        let snapshot = kernel.directory().snapshot();
        snapshot
            .access_points()
            .iter()
            .find(|entry| entry.host.eq_ignore_ascii_case(&host))
            .map(|entry| entry.spki_pins.clone())
            .unwrap_or_default()
    }

    /// 这个主机所属接入点的 `ipHints`。主机不属于任何接入点、或目录还没装配时为空。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn ip_hints_for_host(&self, host: &str) -> Vec<std::net::IpAddr> {
        let Some(kernel) = self.access.kernel_now() else {
            return Vec::new();
        };
        let snapshot = kernel.directory().snapshot();
        snapshot
            .access_points()
            .iter()
            .find(|entry| entry.host.eq_ignore_ascii_case(host))
            .map(|entry| entry.ip_hints.clone())
            .unwrap_or_default()
    }

    /// 路由客户端的 DNS 解析器。按主机现查，所以不经选路的完整 URL（媒体直链）也适用。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn ip_hint_resolver(&self) -> super::ip_hints::IpHintResolver {
        let router = self.clone();
        super::ip_hints::IpHintResolver::new(
            move |host| router.ip_hints_for_host(host),
            super::HTTP_CONNECT_TIMEOUT,
        )
        .with_doh(self.access.doh())
    }

    /// 一次发送失败了：是 TLS 失败或建连超时时让 DoH 查一次主机。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn note_send_failure(&self, error: &reqwest::Error) {
        if let Some(doh) = self.access.doh() {
            report_to_doh(&doh, error, is_dial_timeout(error));
        }
    }

    /// 成功：记录耗时（贡献 RTT）。
    pub fn record_success(&self, access_point: &str, elapsed_ms: u64) {
        self.access.record(
            access_point,
            Observation::http(Outcome::Success {
                rtt_ms: Some(elapsed_ms.min(u64::from(u32::MAX)) as u32),
            }),
        );
    }

    /// 失败：只有连接阶段失败计入熔断；服务端给出的响应（含过载）不计。
    pub fn record_failure(&self, access_point: &str, failure: HttpFailure) {
        let outcome = match failure {
            HttpFailure::ConnectPhase => {
                Outcome::Failure(flare_access::FailureClass::HttpConnectFailed)
            }
            HttpFailure::Tls => Outcome::Failure(flare_access::FailureClass::TlsFailed),
            HttpFailure::Overloaded { .. } => Outcome::Neutral(NeutralReason::Overloaded),
            HttpFailure::Uncertain | HttpFailure::BadGateway => {
                Outcome::Neutral(NeutralReason::ServerResponse)
            }
        };
        self.access.record(access_point, Observation::http(outcome));
    }

    /// 服务端给了应用层响应（业务 4xx/5xx）：入口可达，不计失败。
    pub fn record_response(&self, access_point: &str) {
        self.access.record(
            access_point,
            Observation::http(Outcome::Neutral(NeutralReason::ServerResponse)),
        );
    }

    /// 服务端说这个版本太旧（REST 426）。
    pub fn upgrade_required(&self) {
        self.access.note_upgrade_required(None);
    }

    /// 本服务的入口种类。
    pub fn service_kind(&self) -> Option<ServiceKind> {
        self.service.http_service()
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
                HttpRetry::OtherAccessPoint,
                "请求没发出去，任何方法都能换接入点重试"
            );
            assert_eq!(
                retry_decision(safety, Tls),
                HttpRetry::OtherAccessPoint,
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
                HttpRetry::SameAccessPointAfter { delay_ms: 2_000 },
                "过载原地等，不换入口"
            );
        }
        assert_eq!(
            retry_decision(Idempotent, Uncertain),
            HttpRetry::OtherAccessPoint
        );
        assert_eq!(
            retry_decision(KeyedWrite, Uncertain),
            HttpRetry::OtherAccessPoint
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

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_connect_timeout_asks_doh() {
        use crate::infrastructure::transport::http::doh::tests::{scripted_doh, settle};

        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(NeverResolves))
            .connect_timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();
        let error = client
            .get("http://ap-1.flare.invalid/")
            .send()
            .await
            .unwrap_err();
        assert!(is_dial_timeout(&error), "{error:?}");

        let (cache, lookup) = scripted_doh(vec!["198.51.100.20".parse().unwrap()]);
        report_to_doh(&cache, &error, is_dial_timeout(&error));
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(cache.addresses("ap-1.flare.invalid").len(), 1);
    }

    /// 连上了但响应没来：不是拨号的问题，不用查。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_response_timeout_leaves_doh_alone() {
        use crate::infrastructure::transport::http::doh::tests::{
            scripted_doh, settle, silent_server,
        };

        const HOST: &str = "ap-1.flare.invalid";
        let (port, server) = silent_server().await;
        let client = reqwest::Client::builder()
            .no_proxy()
            .resolve(HOST, ([127, 0, 0, 1], port).into())
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();
        let error = client
            .get(format!("http://{HOST}:{port}/"))
            .send()
            .await
            .unwrap_err();
        assert!(error.is_timeout(), "{error:?}");
        assert!(!is_dial_timeout(&error), "{error:?}");

        let (cache, lookup) = scripted_doh(vec!["198.51.100.20".parse().unwrap()]);
        report_to_doh(&cache, &error, is_dial_timeout(&error));
        settle().await;
        assert_eq!(lookup.calls(), 0);
        server.abort();
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
        headers.insert("idempotency-key".to_owned(), new_idempotency_key());
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
