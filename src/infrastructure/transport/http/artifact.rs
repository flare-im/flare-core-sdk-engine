//! 更新包下载的 HTTP：只有原生端有。
//!
//! 与引导拉取分开：文档是几 KB，读进内存、有总时限；安装包几十 MB，要流式写盘，只限建连和两次读之间的间隔。
//! 包是否可信由清单里的 SHA-256 决定，这里只把字节取回来，所以不需要钉扎，跳转也不受限于接入点。

use std::sync::Arc;
use std::time::Duration;

use flare_access::download::{ArtifactStream, ArtifactTransport, TransportError};
use flare_core::client::dial::DohCache;
use flare_core::common::cert::create_client_config_with_tls;
use flare_core::common::config_types::TlsConfig as CoreTlsConfig;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, RANGE};

use super::ip_hints::IpHintResolver;
use super::router::{is_dial_timeout, is_tls_failure, report_to_doh};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 两次读之间的最长间隔：慢网络要能下完，死连接要能发现。
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// CDN 常跳一两次。
const MAX_REDIRECTS: usize = 5;

pub struct PlatformArtifactTransport {
    trust: CoreTlsConfig,
    /// DoH 备用解析：只有验过签的发布清单里的主机查得到，跳转到别的主机时只走系统 DNS。
    doh: Option<Arc<DohCache>>,
}

impl PlatformArtifactTransport {
    pub fn new(trust: CoreTlsConfig) -> Self {
        Self { trust, doh: None }
    }

    pub(crate) fn with_doh(mut self, doh: Option<Arc<DohCache>>) -> Self {
        self.doh = doh;
        self
    }

    fn client(&self) -> Result<reqwest::Client, TransportError> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT);
        if self.trust.requires_custom_client_tls() {
            let config =
                create_client_config_with_tls(&self.trust).map_err(|_| TransportError::Tls)?;
            builder = builder.use_preconfigured_tls(config);
        }
        if self.doh.is_some() {
            builder = builder.dns_resolver(Arc::new(
                IpHintResolver::new(|_| Vec::new(), CONNECT_TIMEOUT).with_doh(self.doh.clone()),
            ));
        }
        builder.build().map_err(|_| TransportError::Unreachable)
    }
}

fn classify(error: &reqwest::Error) -> TransportError {
    if error.is_timeout() {
        TransportError::Timeout
    } else if is_tls_failure(error) {
        TransportError::Tls
    } else {
        TransportError::Unreachable
    }
}

/// `Content-Range: bytes 100-199/200` 的起点。
fn range_start(value: &str) -> Option<u64> {
    value
        .trim()
        .strip_prefix("bytes ")?
        .split('-')
        .next()?
        .trim()
        .parse()
        .ok()
}

struct ResponseStream {
    start: u64,
    response: reqwest::Response,
}

#[async_trait::async_trait]
impl ArtifactStream for ResponseStream {
    fn start(&self) -> u64 {
        self.start
    }

    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        match self.response.chunk().await {
            Ok(chunk) => Ok(chunk.map(|bytes| bytes.to_vec())),
            Err(error) if error.is_timeout() => Err(TransportError::Timeout),
            Err(_) => Err(TransportError::Interrupted),
        }
    }
}

#[async_trait::async_trait]
impl ArtifactTransport for PlatformArtifactTransport {
    async fn open(
        &self,
        url: &str,
        offset: u64,
    ) -> Result<Box<dyn ArtifactStream>, TransportError> {
        let client = self.client()?;
        let mut request = client.get(url);
        if offset > 0 {
            request = request.header(RANGE, format!("bytes={offset}-"));
        }
        let response = request.send().await.map_err(|error| {
            if let Some(doh) = &self.doh {
                report_to_doh(doh, &error, is_dial_timeout(&error));
            }
            classify(&error)
        })?;
        let start = match response.status() {
            StatusCode::OK => 0,
            StatusCode::PARTIAL_CONTENT => response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(range_start)
                .ok_or(TransportError::Interrupted)?,
            status => return Err(TransportError::Http(status.as_u16())),
        };
        Ok(Box::new(ResponseStream { start, response }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::transport::http::doh::tests::{
        scripted_doh, scripted_doh_after, settle, tls_alert_server,
    };

    fn opened(result: Result<Box<dyn ArtifactStream>, TransportError>) -> TransportError {
        match result {
            Ok(_) => panic!("nothing listens there"),
            Err(error) => error,
        }
    }

    /// 解析走的是 DoH 的解析器：解析不了时后台查一次，答复留给下一次。
    #[tokio::test]
    async fn a_dns_failure_on_a_download_host_asks_doh() {
        let (cache, lookup) = scripted_doh(vec!["198.51.100.20".parse().unwrap()]);
        let transport =
            PlatformArtifactTransport::new(CoreTlsConfig::none()).with_doh(Some(cache.clone()));
        let error = opened(transport.open("https://dl.flare.invalid/app.pkg", 0).await);
        assert_eq!(error, TransportError::Unreachable);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(cache.addresses("dl.flare.invalid").len(), 1);
    }

    #[tokio::test]
    async fn a_tls_failure_on_a_download_host_asks_doh() {
        let (port, server) = tls_alert_server().await;
        // `localhost` 解析到回环算结果可疑，解析这一步先要求一次。
        let (cache, lookup) = scripted_doh_after(1, vec!["198.51.100.20".parse().unwrap()]);
        let transport = PlatformArtifactTransport::new(CoreTlsConfig::none()).with_doh(Some(cache));
        let url = format!("https://localhost:{port}/app.pkg");
        let error = opened(transport.open(&url, 0).await);
        assert_eq!(error, TransportError::Tls);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        server.abort();
    }

    #[test]
    fn the_range_start_comes_from_content_range() {
        assert_eq!(range_start("bytes 100-199/200"), Some(100));
        assert_eq!(range_start("bytes 0-0/*"), Some(0));
        assert_eq!(range_start("items 1-2/3"), None);
        assert_eq!(range_start("bytes */200"), None);
    }
}
