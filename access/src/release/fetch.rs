//! 拉取发布清单：`{source}/release/{platform}/{channel}.dsse.json`。
//!
//! 与接入清单同一组镜像、同一条信任链，不经 IM 节点，全部接入点不可达时照样能查。
//! 验签、防回滚与落盘在 [`AccessKernel`] 里，失败只丢弃文档，不影响手里已有的清单。

use std::sync::Arc;

use crate::bootstrap::{FetchError, FetchRequest, HttpFetch, MIRROR_TIMEOUT_MS};
use crate::kernel::{AccessKernel, ReleaseUpdate};
use crate::manifest::MAX_DOCUMENT_BYTES;
use crate::store::{AcceptError, FetchMeta, ManifestSource, ManifestStore, StoredDocument};
use crate::trust::{TrustError, VerifiedReleaseManifest};

/// 一轮的结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseFetchOutcome {
    /// 拿到了比手里新的清单。
    Adopted,
    /// 有源确认手里的就是最新。
    UpToDate,
    /// 能答复的源都说没有这份清单，手里也没有。
    NotPublished,
    /// 有源给了文档，全被拒绝。`code` 是第一个被拒的原因。
    Rejected { code: String },
    /// 没有源给出结果。
    Unavailable { code: Option<String> },
    /// 开发模式：没有信任材料。
    Skipped,
}

#[derive(Debug, Clone)]
pub struct ReleaseFetchReport {
    pub outcome: ReleaseFetchOutcome,
    /// 这一轮结束时手里最新的已验签清单。
    pub manifest: Option<VerifiedReleaseManifest>,
}

/// 一个平台渠道的拉取器。
pub struct ReleaseFetcher {
    kernel: Arc<AccessKernel>,
    http: Arc<dyn HttpFetch>,
    platform: String,
    channel: String,
    store: Option<ManifestStore>,
}

/// 一轮的进度。
#[derive(Default)]
struct Round {
    newest: Option<VerifiedReleaseManifest>,
    adopted: bool,
    confirmed: bool,
    not_found: bool,
    rejected: Option<String>,
    unreachable: Option<String>,
}

impl ReleaseFetcher {
    pub fn new(
        kernel: Arc<AccessKernel>,
        http: Arc<dyn HttpFetch>,
        platform: impl Into<String>,
        channel: impl Into<String>,
    ) -> Self {
        let platform = platform.into();
        let channel = channel.into();
        let store = kernel.release_store(&platform, &channel);
        Self {
            kernel,
            http,
            platform,
            channel,
            store,
        }
    }

    /// 手里那份清单（读回后重新验签）。
    pub async fn held(&self, now_ms: i64) -> Option<VerifiedReleaseManifest> {
        self.held_record(now_ms).await.map(|(verified, _)| verified)
    }

    async fn held_record(&self, now_ms: i64) -> Option<(VerifiedReleaseManifest, StoredDocument)> {
        let store = self.store.as_ref()?;
        self.kernel
            .load_release(store, &self.platform, &self.channel, now_ms)
            .await
    }

    /// 清单列的 https 镜像，加上内置种子里的：目录为空时后者是唯一的线索。
    /// 上次给出这份清单的源排最前，它的 ETag 才有意义。
    fn mirrors(&self, held_from: Option<&str>) -> Vec<ManifestSource> {
        let snapshot = self.kernel.directory().snapshot();
        let mut sources: Vec<ManifestSource> = Vec::new();
        let listed = snapshot
            .manifest()
            .bootstrap
            .sources
            .iter()
            .chain(self.kernel.seed_sources());
        for source in listed {
            if let ("https", Some(url)) = (source.kind.as_str(), source.url.as_deref()) {
                let source = ManifestSource::Https {
                    url: url.to_owned(),
                };
                if !sources.contains(&source) {
                    sources.push(source);
                }
            }
        }
        sources.sort_by_key(|source| Some(source.label().as_str()) != held_from);
        sources
    }

    fn request(&self, base: &str, if_none_match: Option<String>) -> FetchRequest {
        FetchRequest {
            url: format!(
                "{}/release/{}/{}.dsse.json",
                base.trim_end_matches('/'),
                self.platform,
                self.channel
            ),
            if_none_match,
            timeout_ms: MIRROR_TIMEOUT_MS,
            max_body_bytes: MAX_DOCUMENT_BYTES,
            ..FetchRequest::default()
        }
    }

    /// 跑一轮：按顺序问镜像，第一个给出可信结论就停。
    pub async fn fetch(&self, now_ms: i64) -> ReleaseFetchReport {
        let Some(store) = &self.store else {
            return ReleaseFetchReport {
                outcome: ReleaseFetchOutcome::Skipped,
                manifest: None,
            };
        };
        let held = self.held_record(now_ms).await;
        let held_from = held.as_ref().map(|(_, record)| record.source.label());
        let etag = held.as_ref().and_then(|(_, record)| record.etag.clone());
        let mut round = Round {
            newest: held.map(|(verified, _)| verified),
            ..Round::default()
        };
        for source in self.mirrors(held_from.as_deref()) {
            let ManifestSource::Https { url } = &source else {
                continue;
            };
            let if_none_match = etag.clone().filter(|_| Some(source.label()) == held_from);
            let response = self.http.fetch(&self.request(url, if_none_match)).await;
            match response {
                Err(FetchError::TooLarge) => {
                    round
                        .rejected
                        .get_or_insert_with(|| FetchError::TooLarge.code().to_owned());
                }
                Err(error) => {
                    round
                        .unreachable
                        .get_or_insert_with(|| error.code().to_owned());
                }
                Ok(response) => match response.status {
                    304 => round.confirmed = true,
                    200 => {
                        let meta = FetchMeta {
                            source: source.clone(),
                            etag: response.etag,
                            fetched_at_ms: now_ms,
                        };
                        self.ingest(store, &response.body, meta, now_ms, &mut round)
                            .await;
                    }
                    404 => round.not_found = true,
                    status => {
                        round
                            .unreachable
                            .get_or_insert_with(|| format!("http.{status}"));
                    }
                },
            }
            if round.adopted || round.confirmed {
                break;
            }
        }
        // 同一份存储上可能还有别的运行时（浏览器的另一个标签页）：开头读到的清单这时可能已经过时，
        // 按它评估的话，资源包同步会删掉新清单刚下载的包。
        if !round.adopted
            && let Some((verified, _)) = self.held_record(now_ms).await
        {
            round.newest = Some(verified);
        }
        conclude(round)
    }

    async fn ingest(
        &self,
        store: &ManifestStore,
        body: &[u8],
        meta: FetchMeta,
        now_ms: i64,
        round: &mut Round,
    ) {
        let result = self
            .kernel
            .accept_release(store, body, &self.platform, &self.channel, meta, now_ms)
            .await;
        match result {
            Ok(ReleaseUpdate::Adopted(verified)) => {
                round.newest = Some(verified);
                round.adopted = true;
            }
            Ok(ReleaseUpdate::Unchanged) => round.confirmed = true,
            // 落后的镜像：CDN 还留着旧对象，这个源只是没有新东西，问下一个。
            Err(AcceptError::Trust(TrustError::Rollback { .. })) => round.not_found = true,
            Err(AcceptError::Trust(error)) => {
                round.rejected.get_or_insert_with(|| error.code());
            }
            Err(error) => {
                round.unreachable.get_or_insert_with(|| error.to_string());
            }
        }
    }
}

fn conclude(round: Round) -> ReleaseFetchReport {
    let outcome = if round.adopted {
        ReleaseFetchOutcome::Adopted
    } else if round.confirmed {
        ReleaseFetchOutcome::UpToDate
    } else if let Some(code) = round.rejected {
        ReleaseFetchOutcome::Rejected { code }
    } else if round.not_found {
        // 手里有清单时，镜像说没有不代表撤销：清单是签过名的，撤销也要走签名。
        if round.newest.is_some() {
            ReleaseFetchOutcome::UpToDate
        } else {
            ReleaseFetchOutcome::NotPublished
        }
    } else {
        ReleaseFetchOutcome::Unavailable {
            code: round.unreachable,
        }
    };
    ReleaseFetchReport {
        outcome,
        manifest: round.newest,
    }
}

#[cfg(test)]
#[path = "fetch_tests.rs"]
mod tests;
