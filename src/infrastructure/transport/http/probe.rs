//! 接入点探活（`GET https://{host}/.well-known/flare/ping`）的平台实现：原生 reqwest，浏览器 fetch。
//!
//! 合法性判定（204、`x-flare-ap`/`x-flare-cell` 与清单一致）在 flare-access 里；这里只负责把请求发出去、
//! 按部署的信任根与该接入点的 SPKI 钉扎做 TLS 校验，并把失败归成平台无关的几类。

use flare_access::probe::{ProbeRequest, ProbeResponse, ProbeTransport, ProbeTransportError};
use flare_core::common::config_types::TlsConfig as CoreTlsConfig;

pub struct PlatformProbeTransport {
    /// 信任根（系统根 + 配置的私有 CA）；每次探活叠加被探接入点的钉扎。浏览器里 TLS 由浏览器负责，不用它。
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    trust: CoreTlsConfig,
    /// DoH 备用解析：地址被污染时探活也要探到真实入口，否则好入口一直被判成不通。
    #[cfg(not(target_arch = "wasm32"))]
    doh: Option<std::sync::Arc<flare_core::client::dial::DohCache>>,
}

impl PlatformProbeTransport {
    pub fn new(trust: CoreTlsConfig) -> Self {
        Self {
            trust,
            #[cfg(not(target_arch = "wasm32"))]
            doh: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_doh(
        mut self,
        doh: Option<std::sync::Arc<flare_core::client::dial::DohCache>>,
    ) -> Self {
        self.doh = doh;
        self
    }
}

impl Default for PlatformProbeTransport {
    fn default() -> Self {
        Self::new(CoreTlsConfig::none())
    }
}

fn header(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use flare_core::client::dial::DohCache;
    use flare_core::common::cert::create_client_config_with_tls;

    use super::*;
    use crate::infrastructure::transport::http::ip_hints::IpHintResolver;
    use crate::infrastructure::transport::http::router::{is_tls_failure, report_to_doh};

    fn client(
        trust: &CoreTlsConfig,
        doh: Option<&Arc<DohCache>>,
        request: &ProbeRequest,
    ) -> Result<reqwest::Client, ProbeTransportError> {
        let timeout = Duration::from_millis(u64::from(request.timeout_ms));
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            // 探活必须原样拿到 204 与两个头：重定向本身就说明被劫持（强制门户、运营商页面）。
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout);
        // 清单给了钉扎就必须匹配；没给时只做 CA 校验（系统根 + 配置的私有 CA）。
        let tls = trust
            .clone()
            .with_spki_sha256_pins(request.spki_pins.clone());
        if tls.requires_custom_client_tls() {
            let config =
                create_client_config_with_tls(&tls).map_err(|_| ProbeTransportError::Tls)?;
            builder = builder.use_preconfigured_tls(config);
        }
        if doh.is_some() || !request.ip_hints.is_empty() {
            // DNS 失败或结果可疑时直连，SNI 与证书校验仍按 host：只替换解析结果，URL 不变。
            builder = builder.dns_resolver(Arc::new(
                IpHintResolver::for_host(request.host.clone(), request.ip_hints.clone(), timeout)
                    .with_doh(doh.cloned()),
            ));
        }
        builder
            .build()
            .map_err(|_| ProbeTransportError::Unreachable)
    }

    fn classify(error: &reqwest::Error) -> ProbeTransportError {
        if error.is_timeout() {
            ProbeTransportError::Timeout
        } else if is_tls_failure(error) {
            // 证书不受信任或钉扎不符：健康簿记作 TLS 失败。
            ProbeTransportError::Tls
        } else {
            ProbeTransportError::Unreachable
        }
    }

    async fn get_once(
        client: &reqwest::Client,
        request: &ProbeRequest,
    ) -> Result<ProbeResponse, reqwest::Error> {
        let started = Instant::now();
        let response = client.get(&request.url).send().await?;
        let read = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        Ok(ProbeResponse {
            status: response.status().as_u16(),
            ap_header: header(read(flare_access::probe::HEADER_AP)),
            cell_header: header(read(flare_access::probe::HEADER_CELL)),
            elapsed_ms: started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32,
        })
    }

    pub(super) async fn get(
        trust: &CoreTlsConfig,
        doh: Option<&Arc<DohCache>>,
        request: &ProbeRequest,
    ) -> Result<ProbeResponse, ProbeTransportError> {
        // 钉扎配置本身无效时直接判 TLS 失败，不发请求（绝不退回不校验钉扎的客户端）。
        let client = client(trust, doh, request)?;
        get_once(&client, request).await.map_err(|error| {
            if let Some(doh) = doh {
                // 总时限和建连时限一样长，总时限先到，认不出连接阶段：没等到响应就算。
                report_to_doh(doh, &error, error.is_timeout());
            }
            classify(&error)
        })
    }
}

#[cfg(target_arch = "wasm32")]
mod browser {
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{AbortController, Request, RequestInit, RequestMode, Response};

    use super::*;

    pub(super) async fn get(request: &ProbeRequest) -> Result<ProbeResponse, ProbeTransportError> {
        let window = web_sys::window().ok_or(ProbeTransportError::Unreachable)?;
        if !window.navigator().on_line() {
            return Err(ProbeTransportError::NoNetwork);
        }
        let init = RequestInit::new();
        init.set_method("GET");
        init.set_mode(RequestMode::Cors);
        let controller = AbortController::new().map_err(|_| ProbeTransportError::Unreachable)?;
        init.set_signal(Some(&controller.signal()));
        let started = js_sys::Date::now();
        let abort = wasm_bindgen::closure::Closure::once(
            Box::new(move || controller.abort()) as Box<dyn FnOnce()>
        );
        let timer = window
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                abort.as_ref().unchecked_ref(),
                i32::try_from(request.timeout_ms).unwrap_or(i32::MAX),
            )
            .ok();
        let fetched = match Request::new_with_str_and_init(&request.url, &init) {
            Ok(fetch_request) => JsFuture::from(window.fetch_with_request(&fetch_request)).await,
            Err(_) => return Err(ProbeTransportError::Unreachable),
        };
        if let Some(timer) = timer {
            window.clear_timeout_with_handle(timer);
        }
        drop(abort);
        let elapsed_ms = (js_sys::Date::now() - started).max(0.0) as u32;
        let response: Response = match fetched {
            Ok(value) => value
                .dyn_into()
                .map_err(|_| ProbeTransportError::Unreachable)?,
            Err(_) if elapsed_ms >= request.timeout_ms => return Err(ProbeTransportError::Timeout),
            Err(_) => return Err(ProbeTransportError::Unreachable),
        };
        // 读这两个头需要 Edge 在 CORS 里 expose（Access-Control-Expose-Headers）。
        let headers = response.headers();
        Ok(ProbeResponse {
            status: response.status(),
            ap_header: header(headers.get(flare_access::probe::HEADER_AP).ok().flatten()),
            cell_header: header(headers.get(flare_access::probe::HEADER_CELL).ok().flatten()),
            elapsed_ms,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl ProbeTransport for PlatformProbeTransport {
    async fn get(&self, request: &ProbeRequest) -> Result<ProbeResponse, ProbeTransportError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            native::get(&self.trust, self.doh.as_ref(), request).await
        }
        #[cfg(target_arch = "wasm32")]
        {
            browser::get(request).await
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::infrastructure::transport::http::doh::tests::{
        scripted_doh, scripted_doh_after, settle, silent_server, tls_alert_server,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// 本机起一个只回一次的 HTTP 服务：探活拿到状态码与两个头。
    #[tokio::test]
    async fn probe_reads_status_and_access_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 1024];
            let _ = stream.read(&mut buffer).await;
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nx-flare-ap: ap-a\r\nx-flare-cell: cell-1\r\ncontent-length: 0\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let response = PlatformProbeTransport::default()
            .get(&ProbeRequest {
                ap_id: "ap-a".into(),
                url: format!("http://{address}/.well-known/flare/ping"),
                host: "127.0.0.1".into(),
                ip_hints: vec![],
                spki_pins: vec![],
                timeout_ms: 2_000,
            })
            .await
            .unwrap();
        assert_eq!(response.status, 204);
        assert_eq!(response.ap_header.as_deref(), Some("ap-a"));
        assert_eq!(response.cell_header.as_deref(), Some("cell-1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn an_unreachable_host_is_classified() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let error = PlatformProbeTransport::default()
            .get(&ProbeRequest {
                ap_id: "ap-a".into(),
                url: format!("http://127.0.0.1:{port}/.well-known/flare/ping"),
                host: "127.0.0.1".into(),
                ip_hints: vec![],
                spki_pins: vec![],
                timeout_ms: 1_000,
            })
            .await
            .unwrap_err();
        assert_eq!(error, ProbeTransportError::Unreachable);
    }

    fn hinted_probe(scheme: &str, port: u16, timeout_ms: u32) -> ProbeRequest {
        const HOST: &str = "ap-a.flare.invalid";
        ProbeRequest {
            ap_id: "ap-a".into(),
            url: format!("{scheme}://{HOST}:{port}/.well-known/flare/ping"),
            host: HOST.into(),
            ip_hints: vec!["127.0.0.1".parse().unwrap()],
            spki_pins: vec![],
            timeout_ms,
        }
    }

    /// 没有 `ipHints` 的接入点同样走 DoH：解析不了时后台查一次，答复留给下一次。
    #[tokio::test]
    async fn a_dns_failure_asks_doh_without_hints() {
        let (cache, lookup) = scripted_doh(vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformProbeTransport::default()
            .with_doh(Some(cache.clone()))
            .get(&ProbeRequest {
                ip_hints: vec![],
                ..hinted_probe("http", 1, 1_000)
            })
            .await
            .unwrap_err();
        assert_eq!(error, ProbeTransportError::Unreachable);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(cache.addresses("ap-a.flare.invalid").len(), 1);
    }

    #[tokio::test]
    async fn a_tls_failure_asks_doh() {
        let (port, server) = tls_alert_server().await;
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformProbeTransport::default()
            .with_doh(Some(cache))
            .get(&hinted_probe("https", port, 2_000))
            .await
            .unwrap_err();
        assert_eq!(error, ProbeTransportError::Tls);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn a_stalled_probe_asks_doh() {
        let (port, server) = silent_server().await;
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformProbeTransport::default()
            .with_doh(Some(cache))
            .get(&hinted_probe("http", port, 200))
            .await
            .unwrap_err();
        assert_eq!(error, ProbeTransportError::Timeout);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn a_refused_probe_leaves_doh_alone() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformProbeTransport::default()
            .with_doh(Some(cache))
            .get(&hinted_probe("http", port, 2_000))
            .await
            .unwrap_err();
        assert_eq!(error, ProbeTransportError::Unreachable);
        settle().await;
        assert_eq!(lookup.calls(), 0);
    }

    /// 主机名解析不了：按清单给的 IP 直连，URL 的主机名与端口不变。
    #[tokio::test]
    async fn an_unresolvable_host_is_probed_through_its_ip_hint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 1024];
            let read = stream.read(&mut buffer).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8_lossy(&buffer[..read]).to_ascii_lowercase()
        });
        let response = PlatformProbeTransport::default()
            .get(&ProbeRequest {
                ap_id: "ap-a".into(),
                url: format!("http://ap-a.flare.invalid:{port}/.well-known/flare/ping"),
                host: "ap-a.flare.invalid".into(),
                ip_hints: vec!["127.0.0.1".parse().unwrap()],
                spki_pins: vec![],
                timeout_ms: 2_000,
            })
            .await
            .unwrap();
        assert_eq!(response.status, 204);
        let head = server.await.unwrap();
        assert!(
            head.contains(&format!("host: ap-a.flare.invalid:{port}")),
            "{head}"
        );
    }
}
