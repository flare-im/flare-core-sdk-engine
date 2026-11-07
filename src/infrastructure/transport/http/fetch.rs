//! 引导文档拉取（镜像、接入点 well-known、DoH）的平台实现：原生 reqwest，浏览器 fetch。
//!
//! 文档是否可信由 flare-access 验签决定，这里只负责把字节取回来：按部署的信任根与接入点的 SPKI 钉扎做
//! TLS 校验，限制响应体积，并把失败归成平台无关的几类。

use flare_access::{FetchError, FetchRequest, FetchResponse, HttpFetch};
use flare_core::common::config_types::TlsConfig as CoreTlsConfig;

pub struct PlatformHttpFetch {
    /// 信任根（系统根 + 配置的私有 CA）；每次请求叠加该接入点的钉扎。浏览器里 TLS 由浏览器负责，不用它。
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    trust: CoreTlsConfig,
    /// DoH 备用解析。查 DoH 的那个实例不带它：查询失败不能再引出查询。
    #[cfg(not(target_arch = "wasm32"))]
    doh: Option<std::sync::Arc<flare_core::client::dial::DohCache>>,
}

impl PlatformHttpFetch {
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

impl Default for PlatformHttpFetch {
    fn default() -> Self {
        Self::new(CoreTlsConfig::none())
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::io::Read;
    use std::sync::Arc;
    use std::time::Duration;

    use flare_core::client::dial::DohCache;
    use flare_core::common::cert::create_client_config_with_tls;
    use reqwest::header::{ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, ETAG, IF_NONE_MATCH};

    use super::*;
    use crate::infrastructure::transport::http::ip_hints::IpHintResolver;
    use crate::infrastructure::transport::http::router::{is_tls_failure, report_to_doh};

    /// 镜像前的 CDN 可能跳一次；再多就不正常了。文档有签名，跳到哪里都不影响可信度。
    const MAX_REDIRECTS: usize = 3;

    fn client(
        trust: &CoreTlsConfig,
        doh: Option<&Arc<DohCache>>,
        request: &FetchRequest,
    ) -> Result<reqwest::Client, FetchError> {
        let timeout = Duration::from_millis(u64::from(request.timeout_ms));
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
            .connect_timeout(timeout)
            .timeout(timeout);
        // 清单给了钉扎就必须匹配；没给时只做 CA 校验（系统根 + 配置的私有 CA）。
        let tls = trust
            .clone()
            .with_spki_sha256_pins(request.spki_pins.clone());
        if tls.requires_custom_client_tls() {
            let config = create_client_config_with_tls(&tls).map_err(|_| FetchError::Tls)?;
            builder = builder.use_preconfigured_tls(config);
        }
        if (doh.is_some() || !request.ip_hints.is_empty())
            && let Some(host) = url::Url::parse(&request.url)
                .ok()
                .and_then(|url| url.host_str().map(str::to_owned))
        {
            // DNS 失败或结果可疑时直连，SNI 与证书校验仍按 host：只替换解析结果，URL 不变。
            builder = builder.dns_resolver(Arc::new(
                IpHintResolver::for_host(host, request.ip_hints.clone(), timeout)
                    .with_doh(doh.cloned()),
            ));
        }
        builder.build().map_err(|_| FetchError::Unreachable)
    }

    fn classify(error: &reqwest::Error) -> FetchError {
        if error.is_timeout() {
            FetchError::Timeout
        } else if is_tls_failure(error) {
            FetchError::Tls
        } else {
            FetchError::Unreachable
        }
    }

    /// 读响应体，超过上限立即中止，不把攻击者给的流读完。
    async fn read_body(
        mut response: reqwest::Response,
        limit: usize,
    ) -> Result<Vec<u8>, FetchError> {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| classify(&error))? {
            if body.len() + chunk.len() > limit {
                return Err(FetchError::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// 解压。解压后同样受上限约束：压缩比高的小响应不能展开成大内存。
    pub(super) fn decode(
        encoding: Option<&str>,
        body: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<u8>, FetchError> {
        let reader: Box<dyn Read + '_> = match encoding.map(str::trim) {
            None | Some("") | Some("identity") => return Ok(body),
            Some("gzip") => Box::new(flate2::read::GzDecoder::new(body.as_slice())),
            Some("br") => Box::new(brotli_decompressor::Decompressor::new(
                body.as_slice(),
                4_096,
            )),
            Some(_) => return Err(FetchError::Unreachable),
        };
        let mut decoded = Vec::new();
        reader
            .take(limit as u64 + 1)
            .read_to_end(&mut decoded)
            .map_err(|_| FetchError::Unreachable)?;
        if decoded.len() > limit {
            return Err(FetchError::TooLarge);
        }
        Ok(decoded)
    }

    async fn fetch_once(
        client: &reqwest::Client,
        request: &FetchRequest,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut builder = client.get(&request.url).header(ACCEPT_ENCODING, "br, gzip");
        if let Some(etag) = &request.if_none_match {
            builder = builder.header(IF_NONE_MATCH, etag);
        }
        if let Some(accept) = request.accept {
            builder = builder.header(ACCEPT, accept);
        }
        builder.send().await
    }

    async fn into_response(
        response: reqwest::Response,
        limit: usize,
    ) -> Result<FetchResponse, FetchError> {
        let status = response.status().as_u16();
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|value: &reqwest::header::HeaderValue| value.to_str().ok())
                .map(str::to_owned)
        };
        let etag = header(ETAG);
        let encoding = header(CONTENT_ENCODING);
        let body = read_body(response, limit).await?;
        let body = decode(encoding.as_deref(), body, limit)?;
        Ok(FetchResponse { status, etag, body })
    }

    pub(super) async fn fetch(
        trust: &CoreTlsConfig,
        doh: Option<&Arc<DohCache>>,
        request: &FetchRequest,
    ) -> Result<FetchResponse, FetchError> {
        // 钉扎配置本身无效时直接判 TLS 失败，不发请求（绝不退回不校验钉扎的客户端）。
        let client = client(trust, doh, request)?;
        match fetch_once(&client, request).await {
            Ok(response) => into_response(response, request.max_body_bytes).await,
            Err(error) => {
                if let Some(doh) = doh {
                    // 总时限和建连时限一样长，总时限先到，认不出连接阶段：没等到响应就算。
                    report_to_doh(doh, &error, error.is_timeout());
                }
                Err(classify(&error))
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod browser {
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{
        AbortController, Headers, Request, RequestCache, RequestInit, RequestMode, Response,
    };

    use super::*;

    /// 浏览器不能指定 DNS、IP 与证书校验（§4.10），`ipHints` 与钉扎都不生效。
    ///
    /// 条件请求交给浏览器的 HTTP 缓存（`cache: no-cache` 时它自己带 `If-None-Match` 并在 304 时回放缓存）：
    /// 自己加这个头会触发 CORS 预检，要求镜像额外放行。于是这里看到的永远是 200，由调用方按 serial 比较。
    pub(super) async fn fetch(request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        let window = web_sys::window().ok_or(FetchError::Unreachable)?;
        if !window.navigator().on_line() {
            return Err(FetchError::NoNetwork);
        }
        let init = RequestInit::new();
        init.set_method("GET");
        init.set_mode(RequestMode::Cors);
        init.set_cache(RequestCache::NoCache);
        if let Some(accept) = request.accept {
            let headers = Headers::new().map_err(|_| FetchError::Unreachable)?;
            headers
                .set("Accept", accept)
                .map_err(|_| FetchError::Unreachable)?;
            init.set_headers(&headers);
        }
        let controller = AbortController::new().map_err(|_| FetchError::Unreachable)?;
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
        let result = read(&window, request, &init).await;
        if let Some(timer) = timer {
            window.clear_timeout_with_handle(timer);
        }
        drop(abort);
        let elapsed_ms = (js_sys::Date::now() - started).max(0.0) as u32;
        match result {
            Err(FetchError::Unreachable) if elapsed_ms >= request.timeout_ms => {
                Err(FetchError::Timeout)
            }
            other => other,
        }
    }

    async fn read(
        window: &web_sys::Window,
        request: &FetchRequest,
        init: &RequestInit,
    ) -> Result<FetchResponse, FetchError> {
        let fetch_request = Request::new_with_str_and_init(&request.url, init)
            .map_err(|_| FetchError::Unreachable)?;
        let response: Response = JsFuture::from(window.fetch_with_request(&fetch_request))
            .await
            .map_err(|_| FetchError::Unreachable)?
            .dyn_into()
            .map_err(|_| FetchError::Unreachable)?;
        let buffer = response
            .array_buffer()
            .map_err(|_| FetchError::Unreachable)?;
        let buffer = JsFuture::from(buffer)
            .await
            .map_err(|_| FetchError::Unreachable)?;
        let bytes = js_sys::Uint8Array::new(&buffer);
        if bytes.length() as usize > request.max_body_bytes {
            return Err(FetchError::TooLarge);
        }
        Ok(FetchResponse {
            status: response.status(),
            // 跨域时要镜像 expose 才读得到；读不到也无妨，条件请求由浏览器负责。
            etag: response.headers().get("etag").ok().flatten(),
            body: bytes.to_vec(),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl HttpFetch for PlatformHttpFetch {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            native::fetch(&self.trust, self.doh.as_ref(), request).await
        }
        #[cfg(target_arch = "wasm32")]
        {
            browser::fetch(request).await
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::io::Write;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::infrastructure::transport::http::doh::tests::{
        scripted_doh, scripted_doh_after, settle, silent_server, tls_alert_server,
    };

    /// 本机起一个只回一次的 HTTP 服务，把收到的请求头交回来。
    async fn serve_once(response: Vec<u8>) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 2_048];
            let read = stream.read(&mut buffer).await.unwrap();
            stream.write_all(&response).await.unwrap();
            stream.shutdown().await.unwrap();
            String::from_utf8_lossy(&buffer[..read]).to_ascii_lowercase()
        });
        (format!("http://{address}/flare/access.dsse.json"), server)
    }

    fn request(url: String) -> FetchRequest {
        FetchRequest {
            url,
            timeout_ms: 2_000,
            max_body_bytes: 1_024,
            ..FetchRequest::default()
        }
    }

    fn http_response(headers: &str, body: &[u8]) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    #[tokio::test]
    async fn sends_the_condition_and_reads_status_etag_and_body() {
        let (url, server) = serve_once(http_response("etag: \"v2\"\r\n", b"document")).await;
        let mut fetch = request(url);
        fetch.if_none_match = Some("\"v1\"".into());
        fetch.accept = Some("application/dns-message");
        let response = PlatformHttpFetch::default().fetch(&fetch).await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.etag.as_deref(), Some("\"v2\""));
        assert_eq!(response.body, b"document");
        let head = server.await.unwrap();
        assert!(head.contains("if-none-match: \"v1\""), "{head}");
        assert!(head.contains("accept-encoding: br, gzip"), "{head}");
        assert!(head.contains("accept: application/dns-message"), "{head}");
    }

    #[tokio::test]
    async fn not_modified_is_passed_through() {
        let (url, server) = serve_once(
            b"HTTP/1.1 304 Not Modified\r\netag: \"v1\"\r\nconnection: close\r\n\r\n".to_vec(),
        )
        .await;
        let response = PlatformHttpFetch::default()
            .fetch(&request(url))
            .await
            .unwrap();
        assert_eq!(response.status, 304);
        assert!(response.body.is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn compressed_documents_are_decoded() {
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(b"gzip document").unwrap();
        let gzip = gzip.finish().unwrap();
        let (url, server) = serve_once(http_response("content-encoding: gzip\r\n", &gzip)).await;
        let response = PlatformHttpFetch::default()
            .fetch(&request(url))
            .await
            .unwrap();
        assert_eq!(response.body, b"gzip document");
        server.await.unwrap();

        // 一段未压缩的 brotli 元块：`ISLAST=0`、长度 5、原文。
        let brotli: &[u8] = &[0x0b, 0x02, 0x80, b'h', b'e', b'l', b'l', b'o', 0x03];
        let (url, server) = serve_once(http_response("content-encoding: br\r\n", brotli)).await;
        let response = PlatformHttpFetch::default()
            .fetch(&request(url))
            .await
            .unwrap();
        assert_eq!(response.body, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bodies_are_bounded_before_and_after_decompression() {
        let (url, server) = serve_once(http_response("", &vec![b'x'; 2_000])).await;
        let error = PlatformHttpFetch::default()
            .fetch(&request(url))
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::TooLarge);
        server.abort();

        // 压缩后很小、展开超限。
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        gzip.write_all(&vec![0u8; 1_000_000]).unwrap();
        let gzip = gzip.finish().unwrap();
        assert!(gzip.len() < 2_000);
        assert_eq!(
            native::decode(Some("gzip"), gzip, 1_024).unwrap_err(),
            FetchError::TooLarge
        );
    }

    #[tokio::test]
    async fn an_unresolvable_host_falls_back_to_the_ip_hint_keeping_the_url() {
        // 主机名解析不了，清单给的 IP 能到：URL（Host 头）不变。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 1_024];
            let read = stream.read(&mut buffer).await.unwrap();
            stream
                .write_all(&http_response("", b"direct"))
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
            String::from_utf8_lossy(&buffer[..read]).to_ascii_lowercase()
        });
        let mut fetch = request(format!("http://im-ct.invalid:{port}/access.dsse.json"));
        fetch.ip_hints = vec!["127.0.0.1".parse().unwrap()];
        let response = PlatformHttpFetch::default().fetch(&fetch).await.unwrap();
        assert_eq!(response.body, b"direct");
        let head = server.await.unwrap();
        assert!(
            head.contains(&format!("host: im-ct.invalid:{port}")),
            "{head}"
        );

        // 没有 IP 提示：如实报不可达。
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let error = PlatformHttpFetch::default()
            .fetch(&request(format!("http://127.0.0.1:{port}/a")))
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::Unreachable);
    }

    /// 没有 `ipHints` 的镜像同样走 DoH：解析不了时后台查一次，答复留给下一次。
    #[tokio::test]
    async fn a_dns_failure_on_a_mirror_asks_doh() {
        let (cache, lookup) = scripted_doh(vec!["198.51.100.20".parse().unwrap()]);
        let fetch = PlatformHttpFetch::default().with_doh(Some(cache.clone()));
        let error = fetch
            .fetch(&request("http://mirror.flare.invalid/a".into()))
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::Unreachable);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(
            cache.addresses("mirror.flare.invalid"),
            ["198.51.100.20".parse::<std::net::IpAddr>().unwrap()]
        );
    }

    fn hinted_mirror(scheme: &str, port: u16, timeout_ms: u32) -> FetchRequest {
        FetchRequest {
            ip_hints: vec!["127.0.0.1".parse().unwrap()],
            timeout_ms,
            ..request(format!("{scheme}://mirror.flare.invalid:{port}/a"))
        }
    }

    #[tokio::test]
    async fn a_tls_failure_on_a_mirror_asks_doh() {
        let (port, server) = tls_alert_server().await;
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformHttpFetch::default()
            .with_doh(Some(cache))
            .fetch(&hinted_mirror("https", port, 2_000))
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::Tls);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn a_stalled_mirror_asks_doh() {
        let (port, server) = silent_server().await;
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformHttpFetch::default()
            .with_doh(Some(cache))
            .fetch(&hinted_mirror("http", port, 200))
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::Timeout);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn a_refused_mirror_leaves_doh_alone() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let error = PlatformHttpFetch::default()
            .with_doh(Some(cache))
            .fetch(&hinted_mirror("http", port, 2_000))
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::Unreachable);
        settle().await;
        assert_eq!(lookup.calls(), 0);
    }

    /// 带钉扎的接入点按 IP 直连：走的是钉扎的 TLS 配置，ClientHello 的 SNI 仍是主机名。
    /// 假服务端不说 TLS，请求必然失败，这里只看 ClientHello。
    #[tokio::test]
    async fn a_pinned_hint_keeps_sni_on_the_host() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut hello = vec![0u8; 4_096];
            let read = stream.read(&mut hello).await.unwrap();
            hello.truncate(read);
            hello
        });
        let host = "ap-1.flare.invalid";
        let mut fetch = request(format!("https://{host}:{port}/access.dsse.json"));
        fetch.ip_hints = vec!["127.0.0.1".parse().unwrap()];
        fetch.spki_pins = vec!["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()];
        assert!(PlatformHttpFetch::default().fetch(&fetch).await.is_err());
        let hello = server.await.unwrap();
        assert!(
            hello
                .windows(host.len())
                .any(|window| window == host.as_bytes()),
            "no SNI for {host} in the ClientHello"
        );
    }

    /// 钉扎配置无效时不发请求，有 IP 提示也一样。
    #[tokio::test]
    async fn an_invalid_pin_fails_before_dialing_a_hint() {
        let mut fetch = request("https://ap-1.flare.invalid/access.dsse.json".into());
        fetch.ip_hints = vec!["127.0.0.1".parse().unwrap()];
        fetch.spki_pins = vec!["not-a-pin".into()];
        assert_eq!(
            PlatformHttpFetch::default()
                .fetch(&fetch)
                .await
                .unwrap_err(),
            FetchError::Tls
        );
    }
}
