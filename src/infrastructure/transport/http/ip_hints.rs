//! reqwest 的 DNS 解析器：正常 DNS 优先，DNS 失败或结果可疑才用接入点的 `ipHints`，有 DoH 答复时
//! 它排最前（规则在 [`flare_core::client::dial`]，IM 建连用的是同一份）。
//!
//! 只替换解析结果，URL 不变：TLS SNI 与证书校验仍按主机名，SPKI 钉扎照常生效。

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use flare_core::client::dial::{self, DohCache};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

type HintLookup = Box<dyn Fn(&str) -> Vec<IpAddr> + Send + Sync>;

pub(crate) struct IpHintResolver {
    hints: HintLookup,
    doh: Option<Arc<DohCache>>,
    connect_timeout: Duration,
}

impl IpHintResolver {
    /// 每次解析时按主机名现查：清单换了，下一次解析就用新的。
    pub(crate) fn new(
        hints: impl Fn(&str) -> Vec<IpAddr> + Send + Sync + 'static,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            hints: Box::new(hints),
            doh: None,
            connect_timeout,
        }
    }

    /// 加上 DoH 备用解析：解析时读它的答复，DNS 失败或结果可疑时让它在后台查一次。
    pub(crate) fn with_doh(mut self, doh: Option<Arc<DohCache>>) -> Self {
        self.doh = doh;
        self
    }

    /// 只对 `host` 生效：请求重定向到别的主机时不用这组 IP。
    pub(crate) fn for_host(
        host: impl Into<String>,
        ip_hints: Vec<IpAddr>,
        connect_timeout: Duration,
    ) -> Self {
        let host = host.into();
        Self::new(
            move |name| {
                if name.eq_ignore_ascii_case(&host) {
                    ip_hints.clone()
                } else {
                    Vec::new()
                }
            },
            connect_timeout,
        )
    }
}

impl Resolve for IpHintResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let ip_hints = (self.hints)(name.as_str());
        let doh = self.doh.clone();
        let connect_timeout = self.connect_timeout;
        Box::pin(async move {
            // 端口填 0：reqwest 换成 URL 的端口。
            let chosen =
                dial::resolve(name.as_str(), 0, &ip_hints, doh.as_ref(), connect_timeout).await?;
            let addresses: Addrs = Box::new(chosen.addresses.into_iter());
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(addresses)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::infrastructure::transport::http::doh::tests::{scripted_doh, settle};

    const HOST: &str = "ap-1.flare.invalid";
    const LIMIT: Duration = Duration::from_secs(2);

    fn hint() -> IpAddr {
        "203.0.113.10".parse().unwrap()
    }

    fn loopback() -> IpAddr {
        "127.0.0.1".parse().unwrap()
    }

    fn private_hint() -> IpAddr {
        "10.0.0.9".parse().unwrap()
    }

    async fn resolved(resolver: &IpHintResolver, host: &str) -> Option<Vec<SocketAddr>> {
        let addresses = resolver.resolve(host.parse().unwrap()).await.ok()?;
        Some(addresses.collect())
    }

    #[tokio::test]
    async fn a_dns_failure_gives_the_hints_of_that_host_only() {
        let resolver = IpHintResolver::for_host(HOST, vec![hint()], LIMIT);
        assert_eq!(
            resolved(&resolver, HOST).await,
            Some(vec![SocketAddr::new(hint(), 0)])
        );
        assert_eq!(
            resolved(&resolver, &HOST.to_ascii_uppercase()).await,
            Some(vec![SocketAddr::new(hint(), 0)])
        );
        assert_eq!(resolved(&resolver, "other.flare.invalid").await, None);
    }

    #[tokio::test]
    async fn a_resolved_host_keeps_its_dns_answer() {
        let resolver = IpHintResolver::for_host("localhost", vec![private_hint()], LIMIT);
        let addresses = resolved(&resolver, "localhost").await.unwrap();
        assert!(!addresses.is_empty());
        assert!(
            addresses
                .iter()
                .all(|address| address.ip() != private_hint())
        );
    }

    #[tokio::test]
    async fn an_answer_with_no_public_address_gives_way_to_a_public_hint() {
        let resolver = IpHintResolver::for_host("localhost", vec![hint()], LIMIT);
        let addresses = resolved(&resolver, "localhost").await.unwrap();
        assert_eq!(addresses[0], SocketAddr::new(hint(), 0));
        assert!(addresses.len() > 1, "解析结果接在提示后面");
    }

    #[tokio::test]
    async fn a_doh_answer_leads_and_a_dns_failure_asks_for_one() {
        let doh_address: IpAddr = "198.51.100.20".parse().unwrap();
        let (cache, lookup) = scripted_doh(vec![doh_address]);
        let resolver = IpHintResolver::for_host(HOST, vec![hint()], LIMIT).with_doh(Some(cache));
        assert_eq!(
            resolved(&resolver, HOST).await,
            Some(vec![SocketAddr::new(hint(), 0)]),
            "查询在后台，这一次只有提示"
        );
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(
            resolved(&resolver, HOST).await,
            Some(vec![
                SocketAddr::new(doh_address, 0),
                SocketAddr::new(hint(), 0)
            ])
        );
    }

    #[tokio::test]
    async fn the_hints_are_looked_up_on_every_resolve() {
        let current = Arc::new(std::sync::Mutex::new(vec![hint()]));
        let source = current.clone();
        let resolver = IpHintResolver::new(move |_| source.lock().unwrap().clone(), LIMIT);
        assert_eq!(
            resolved(&resolver, HOST).await,
            Some(vec![SocketAddr::new(hint(), 0)])
        );
        current.lock().unwrap().clear();
        assert_eq!(resolved(&resolver, HOST).await, None);
    }

    fn client(ip_hints: Vec<IpAddr>) -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(IpHintResolver::for_host(HOST, ip_hints, LIMIT)))
            .build()
            .unwrap()
    }

    /// 直连 IP 时请求里的主机名不变，端口是 URL 的端口。
    #[tokio::test]
    async fn the_url_port_and_host_header_survive_a_hint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 1_024];
            let read = stream.read(&mut buffer).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8_lossy(&buffer[..read]).to_ascii_lowercase()
        });
        let response = client(vec![loopback()])
            .get(format!("http://{HOST}:{port}/ping"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 204);
        let head = server.await.unwrap();
        assert!(head.contains(&format!("host: {HOST}:{port}")), "{head}");
    }

    /// 假服务端不说 TLS，请求必然失败；只看 ClientHello：SNI 是主机名，不是 IP。
    #[tokio::test]
    async fn tls_names_the_host_when_dialing_a_hint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut hello = vec![0u8; 4_096];
            let read = stream.read(&mut hello).await.unwrap();
            hello.truncate(read);
            hello
        });
        let result = client(vec![loopback()])
            .get(format!("https://{HOST}:{port}/ping"))
            .send()
            .await;
        assert!(result.is_err());
        let hello = server.await.unwrap();
        assert!(
            hello
                .windows(HOST.len())
                .any(|window| window == HOST.as_bytes()),
            "no SNI for {HOST} in the ClientHello"
        );
    }
}
