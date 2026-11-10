//! 接入点、镜像与发布清单下载主机的 DoH 解析：解析器来自清单 `bootstrap.sources` 的 `dohTxt`。
//!
//! 这里只回答「这个主机能不能查」和「查一次」。缓存、并发、退避与地址排序都在
//! [`flare_core::client::dial`]，IM 拨号、REST、引导拉取、探活与制品下载共用。

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use flare_access::bootstrap::doh;
use flare_access::{AccessKernel, HttpFetch};
use flare_core::client::dial::{DohAnswer, DohLookup};

/// 下载主机最多记这么多：一份发布清单用不了这么多，换过几轮清单之后旧主机留着没用。
const MAX_DOWNLOAD_HOSTS: usize = 64;

pub(crate) struct DirectoryDoh {
    /// 目录装配好之前什么都不查。开发模式不装上：端点由配置决定。
    kernel: OnceLock<Arc<AccessKernel>>,
    http: Arc<dyn HttpFetch>,
    /// 发布清单里的下载主机（小写）。
    download_hosts: Mutex<HashSet<String>>,
}

impl DirectoryDoh {
    /// `http` 自己不能走 DoH：查询失败不能再引出查询。
    pub(crate) fn new(http: Arc<dyn HttpFetch>) -> Self {
        Self {
            kernel: OnceLock::new(),
            http,
            download_hosts: Mutex::default(),
        }
    }

    pub(crate) fn attach(&self, kernel: Arc<AccessKernel>) {
        let _ = self.kernel.set(kernel);
    }

    /// 记下要下载的地址的主机。只给验过签的发布清单用：制品常放在 CDN 上，不在接入清单里，而这些名字
    /// 和接入点一样是部署方写的。媒体直链不走这里，它们的第三方主机名不发给解析器。
    pub(crate) fn cover_downloads<'a>(&self, urls: impl IntoIterator<Item = &'a String>) {
        let mut hosts = self.hosts();
        for url in urls {
            // 主机名要和 reqwest 解析出来的一样，所以用 `url`。
            let Some(host) = url::Url::parse(url)
                .ok()
                .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
            else {
                continue;
            };
            if hosts.len() >= MAX_DOWNLOAD_HOSTS && !hosts.contains(&host) {
                hosts.clear();
            }
            hosts.insert(host);
        }
    }

    fn hosts(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.download_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl DohLookup for DirectoryDoh {
    fn covers(&self, host: &str) -> bool {
        self.kernel.get().is_some_and(|kernel| {
            !kernel.doh_resolvers().is_empty()
                && (kernel.is_listed_host(host)
                    || self.hosts().contains(&host.to_ascii_lowercase()))
        })
    }

    async fn lookup(&self, host: &str) -> Option<DohAnswer> {
        let resolvers = self.kernel.get()?.doh_resolvers();
        let answer = doh::resolve_host(self.http.as_ref(), &resolvers, host).await?;
        Some(DohAnswer {
            addresses: answer.addresses,
            ttl: Duration::from_secs(u64::from(answer.ttl_sec)),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use flare_core::client::dial::DohCache;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::task::JoinHandle;

    use super::*;

    /// 什么主机都查，每次都给同一组地址。
    pub(crate) struct ScriptedLookup {
        addresses: Vec<IpAddr>,
        calls: AtomicUsize,
        refused: std::sync::Mutex<usize>,
    }

    impl ScriptedLookup {
        pub(crate) fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl DohLookup for ScriptedLookup {
        fn covers(&self, _host: &str) -> bool {
            let mut refused = self.refused.lock().unwrap();
            let covered = *refused == 0;
            *refused = refused.saturating_sub(1);
            covered
        }

        async fn lookup(&self, _host: &str) -> Option<DohAnswer> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            (!self.addresses.is_empty()).then(|| DohAnswer {
                addresses: self.addresses.clone(),
                ttl: Duration::from_secs(60),
            })
        }
    }

    pub(crate) fn scripted_doh(addresses: Vec<IpAddr>) -> (Arc<DohCache>, Arc<ScriptedLookup>) {
        scripted_doh_after(0, addresses)
    }

    /// 前 `refused` 次要求都拒掉。解析这一步自己就会要求查询（`.invalid` 解析失败，`localhost` 结果
    /// 可疑），拒掉它才看得出发送失败之后有没有再要求。
    pub(crate) fn scripted_doh_after(
        refused: usize,
        addresses: Vec<IpAddr>,
    ) -> (Arc<DohCache>, Arc<ScriptedLookup>) {
        let lookup = Arc::new(ScriptedLookup {
            addresses,
            calls: AtomicUsize::new(0),
            refused: std::sync::Mutex::new(refused),
        });
        (DohCache::new(lookup.clone()), lookup)
    }

    /// 后台查询和测试在同一个线程上：让出几次就跑完了。
    pub(crate) async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    /// 读到 ClientHello 就回一条 `handshake_failure` 告警：客户端报 TLS 失败，不用准备证书。
    pub(crate) async fn tls_alert_server() -> (u16, JoinHandle<()>) {
        const HANDSHAKE_FAILURE: [u8; 7] = [0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut hello = [0u8; 4_096];
                let _ = stream.read(&mut hello).await;
                let _ = stream.write_all(&HANDSHAKE_FAILURE).await;
                let _ = stream.shutdown().await;
            }
        });
        (port, server)
    }

    /// 连接收下，一声不吭。
    pub(crate) async fn silent_server() -> (u16, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                open.push(stream);
            }
        });
        (port, server)
    }
}
