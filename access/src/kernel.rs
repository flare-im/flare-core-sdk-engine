//! 门面：从初始化配置装配信任材料、LKG、端点目录、健康簿与选路器。
//!
//! 引擎只需要持有一个 [`AccessKernel`]：启动时 [`AccessKernel::bootstrap`]，
//! 之后通过 [`AccessKernel::selector`] 取候选、[`AccessKernel::health`] 回写结果，
//! 引导拉取器（[`BootstrapFetcher`](crate::bootstrap::BootstrapFetcher)）把拿到的信封交给
//! [`AccessKernel::accept_manifest`]，恢复链接交给 [`AccessKernel::accept_recovery`]。

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arc_swap::ArcSwap;

use crate::bootstrap::{
    AccessConfig, ConfigError, DevelopmentError, TrustMode, development_snapshot, envelope_sources,
};
use crate::directory::{DirectoryChange, DirectorySnapshot, EndpointDirectory, ManifestOrigin};
use crate::events::AccessWarning;
use crate::health::{BreakerPhase, HealthBook, LoadReport, NetPrint};
use crate::manifest::{BootstrapSource, DocType};
use crate::parsed_url::ParsedUrl;
use crate::platform::RandomSource;
use crate::select::{Selector, SelectorOptions};
use crate::store::{
    AcceptError, AcceptOutcome, AccessStore, FetchMeta, ManifestSource, ManifestStore, StoreError,
    StoredDocument,
};
use crate::trust::{
    Envelope, TrustError, TrustRoots, TrustStore, VerifiedAccessManifest, VerifiedReleaseManifest,
};
use crate::types::{ApId, ApStatus, Protocol};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelOptions {
    /// 平台能否建 QUIC（浏览器 wasm 不能）。
    pub quic_supported: bool,
}

impl Default for KernelOptions {
    fn default() -> Self {
        Self {
            quic_supported: !cfg!(target_arch = "wasm32"),
        }
    }
}

/// 启动时采用的清单来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialOrigin {
    /// 本地 LKG（读回后已重新验签）。
    Lkg,
    /// 内置种子；`persisted = false` 表示只在内存中使用（见 `AccessWarning::SeedBelowStoredSerial`）。
    Seed {
        persisted: bool,
    },
    Development,
    /// 没有任何可信清单：目录为空，靠引导拉取或恢复链接补上（见 `AccessWarning::NoUsableManifest`）。
    Unavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BootstrapReport {
    pub origin: InitialOrigin,
    pub serial: u64,
    pub warnings: Vec<AccessWarning>,
    pub health: Option<LoadReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessInitError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("trust configuration rejected: {0}")]
    Trust(TrustError),
    #[error(transparent)]
    Development(#[from] DevelopmentError),
}

/// 接受新清单的结果。
#[derive(Debug, Clone, PartialEq)]
pub enum ManifestUpdate {
    /// 目录已替换。`persisted = false` 表示写盘失败但已在内存中生效。
    Adopted {
        change: DirectoryChange,
        stale: bool,
        persisted: bool,
    },
    /// 与当前相同（或不比内存中的新）。
    Unchanged { serial: u64 },
}

/// 接受发布清单的结果。
#[derive(Debug, Clone)]
pub enum ReleaseUpdate {
    /// 比手里的新。写盘失败时也算：已验证的清单在内存里照常可用。
    Adopted(VerifiedReleaseManifest),
    /// 与手里的 serial 相同。
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeystateUpdate {
    Adopted {
        serial: u64,
        /// 当前在用清单的签名密钥已被吊销：清单照常可用（连接有 TLS 保护），引擎应尽快刷新。
        current_manifest_revoked: bool,
    },
    Unchanged {
        serial: u64,
    },
}

/// `access.getStatus` 的数据。
#[derive(Debug, Clone, PartialEq)]
pub struct AccessStatus {
    pub serial: u64,
    pub source: String,
    pub stale: bool,
    pub development: bool,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    /// 当前清单到达本机的时刻（启动时取自 LKG 记录）；种子与开发模式为 `None`。
    pub fetched_at_ms: Option<i64>,
    pub region_id: String,
    pub cell_id: String,
    pub keystate_serial: Option<u64>,
    /// 目录里有可信清单。
    pub has_manifest: bool,
}

/// `access.listAccessPoints` 的一行。
#[derive(Debug, Clone, PartialEq)]
pub struct AccessPointStatus {
    pub ap_id: ApId,
    pub region_id: String,
    pub host: String,
    pub status: ApStatus,
    pub rtt_ms: Option<f64>,
    pub breaker: BreakerPhase,
    pub protocol_winner: Option<Protocol>,
    pub pinned: bool,
}

pub struct AccessKernel {
    mode: TrustMode,
    store: Arc<dyn AccessStore>,
    trust: Option<ArcSwap<TrustStore>>,
    manifests: Option<ManifestStore>,
    directory: Arc<EndpointDirectory>,
    health: Arc<HealthBook>,
    selector: Arc<Selector>,
    bootstrap_id: String,
    /// 内置种子里的引导源：恢复时作为线索追加，不要求种子本身还能验签通过。
    seed_sources: Vec<BootstrapSource>,
    /// 当前清单到达本机的时刻（0 表示没有）。
    fetched_at_ms: AtomicI64,
}

impl AccessKernel {
    /// 装配。生产模式：密钥状态 LKG → 接入清单 LKG（重新验签）→ 内置种子，取 serial 更大者，
    /// 都不可用时目录为空，由引导拉取或恢复链接补上；开发模式：由 `developmentEndpoints` 直接构造。
    /// 随后加载健康簿。
    pub async fn bootstrap(
        config: &AccessConfig,
        store: Arc<dyn AccessStore>,
        options: KernelOptions,
        now_ms: i64,
    ) -> Result<(Self, BootstrapReport), AccessInitError> {
        Self::bootstrap_with(config, store, options, now_ms, None).await
    }

    /// 同 [`bootstrap`](Self::bootstrap)，可注入健康簿与选路器的随机源（测试用）。
    pub async fn bootstrap_with(
        config: &AccessConfig,
        store: Arc<dyn AccessStore>,
        options: KernelOptions,
        now_ms: i64,
        random: Option<(Box<dyn RandomSource>, Box<dyn RandomSource>)>,
    ) -> Result<(Self, BootstrapReport), AccessInitError> {
        config.validate()?;
        let bootstrap_id = config.effective_bootstrap_id().to_owned();
        let mut warnings = Vec::new();
        let seed_sources = config
            .seed_bytes()
            .map(envelope_sources)
            .unwrap_or_default();
        let mut fetched_at_ms = 0;

        let (snapshot, origin, trust, manifests) = match config.trust_mode {
            TrustMode::Development => {
                let endpoints = config
                    .development_endpoints
                    .as_ref()
                    .ok_or(ConfigError::MissingDevelopmentEndpoints)?;
                warnings.push(AccessWarning::DevelopmentMode);
                (
                    development_snapshot(endpoints, &bootstrap_id, now_ms)?,
                    InitialOrigin::Development,
                    None,
                    None,
                )
            }
            TrustMode::Production => {
                let roots =
                    TrustRoots::from_config(&config.trust_roots).map_err(AccessInitError::Trust)?;
                let mut trust =
                    TrustStore::new(roots, &bootstrap_id).map_err(AccessInitError::Trust)?;
                let manifests =
                    ManifestStore::new(store.clone(), &bootstrap_id, trust.roots().fingerprint());
                trust = load_keystate(&manifests, trust, now_ms, &mut warnings).await;
                let chosen =
                    choose_initial(config, &trust, &manifests, now_ms, &mut warnings).await;
                let (snapshot, origin) = match chosen {
                    Initial::Manifest {
                        verified,
                        source,
                        origin,
                        fetched_at_ms: fetched,
                    } => {
                        fetched_at_ms = fetched;
                        (DirectorySnapshot::from_verified(&verified, source), origin)
                    }
                    Initial::Unavailable { sources } => (
                        DirectorySnapshot::unavailable(&bootstrap_id, sources),
                        InitialOrigin::Unavailable,
                    ),
                };
                (
                    snapshot,
                    origin,
                    Some(ArcSwap::from_pointee(trust)),
                    Some(manifests),
                )
            }
        };
        if snapshot.has_manifest() && snapshot.is_stale(now_ms) {
            warnings.push(AccessWarning::ManifestStale {
                serial: snapshot.serial(),
                expires_at_ms: snapshot.expires_at_ms(),
            });
        }

        let (health_rng, select_rng) = match random {
            Some((health, select)) => (Some(health), Some(select)),
            None => (None, None),
        };
        let health = Arc::new(match health_rng {
            Some(rng) => HealthBook::with_random(snapshot.policy(), rng),
            None => HealthBook::new(snapshot.policy()),
        });
        let health_report = match health.load(&*store, now_ms).await {
            Ok(report) => Some(report),
            Err(error) => {
                warnings.push(AccessWarning::StoredDocumentUnreadable {
                    doc: DocType::Access,
                    message: format!("health: {error}"),
                });
                None
            }
        };
        let serial = snapshot.serial();
        let directory = Arc::new(EndpointDirectory::new(snapshot));
        let selector_options = SelectorOptions {
            quic_supported: options.quic_supported,
        };
        let selector = Arc::new(match select_rng {
            Some(rng) => {
                Selector::with_random(directory.clone(), health.clone(), selector_options, rng)
            }
            None => Selector::new(directory.clone(), health.clone(), selector_options),
        });
        Ok((
            Self {
                mode: config.trust_mode,
                store,
                trust,
                manifests,
                directory,
                health,
                selector,
                bootstrap_id,
                seed_sources,
                fetched_at_ms: AtomicI64::new(fetched_at_ms),
            },
            BootstrapReport {
                origin,
                serial,
                warnings,
                health: health_report,
            },
        ))
    }

    pub fn is_development(&self) -> bool {
        self.mode == TrustMode::Development
    }

    pub fn directory(&self) -> &Arc<EndpointDirectory> {
        &self.directory
    }

    pub fn health(&self) -> &Arc<HealthBook> {
        &self.health
    }

    pub fn selector(&self) -> &Arc<Selector> {
        &self.selector
    }

    pub fn store(&self) -> &Arc<dyn AccessStore> {
        &self.store
    }

    /// 当前信任材料（开发模式为 `None`）。
    pub fn trust(&self) -> Option<Arc<TrustStore>> {
        self.trust.as_ref().map(ArcSwap::load_full)
    }

    pub fn bootstrap_id(&self) -> &str {
        &self.bootstrap_id
    }

    /// 内置种子里的引导源（线索，不要求种子验签通过）。
    pub fn seed_sources(&self) -> &[BootstrapSource] {
        &self.seed_sources
    }

    /// 清单与种子里 `dohTxt` 列出的解析器，去重，清单在前。
    pub fn doh_resolvers(&self) -> Vec<String> {
        let snapshot = self.directory.snapshot();
        let mut resolvers: Vec<String> = Vec::new();
        for source in snapshot
            .manifest()
            .bootstrap
            .sources
            .iter()
            .chain(&self.seed_sources)
            .filter(|source| source.kind == "dohTxt")
        {
            for resolver in &source.resolvers {
                if !resolvers.contains(resolver) {
                    resolvers.push(resolver.clone());
                }
            }
        }
        resolvers
    }

    /// 接入点或镜像的主机。发给 DoH 解析器的只能是这些名字：媒体直链之类的第三方主机不外泄。
    pub fn is_listed_host(&self, host: &str) -> bool {
        let snapshot = self.directory.snapshot();
        let access_point = snapshot
            .access_points()
            .iter()
            .any(|entry| entry.host.eq_ignore_ascii_case(host));
        access_point
            || snapshot
                .manifest()
                .bootstrap
                .sources
                .iter()
                .chain(&self.seed_sources)
                .filter_map(|source| ParsedUrl::parse(source.url.as_deref()?).ok())
                .any(|url| url.host_str().eq_ignore_ascii_case(host))
    }

    /// 接受一份网络下发的接入清单信封：验签 → 防回滚提交 → 目录替换 → 熔断参数更新。
    pub async fn accept_manifest(
        &self,
        envelope: &[u8],
        meta: FetchMeta,
        now_ms: i64,
    ) -> Result<ManifestUpdate, AcceptError> {
        let Some(trust) = &self.trust else {
            return Err(AcceptError::DevelopmentMode);
        };
        let verified = trust.load().verify_access(envelope, now_ms)?;
        self.adopt(verified, meta, now_ms).await
    }

    /// 导入恢复链接（深链或二维码内容）：验签之后与网络下发的清单走同一条采纳路径，
    /// 同样不能回退 serial。
    pub async fn accept_recovery(
        &self,
        link: &str,
        now_ms: i64,
    ) -> Result<ManifestUpdate, AcceptError> {
        let Some(trust) = &self.trust else {
            return Err(AcceptError::DevelopmentMode);
        };
        let envelope = Envelope::from_recovery_link(link)?;
        let verified = trust.load().verify_recovery(&envelope.to_json_bytes())?;
        self.adopt(
            verified,
            FetchMeta::new(ManifestSource::Recovery, now_ms),
            now_ms,
        )
        .await
    }

    async fn adopt(
        &self,
        verified: VerifiedAccessManifest,
        meta: FetchMeta,
        now_ms: i64,
    ) -> Result<ManifestUpdate, AcceptError> {
        let Some(manifests) = &self.manifests else {
            return Err(AcceptError::DevelopmentMode);
        };
        let persisted = match manifests.commit_access(&verified, &meta).await {
            Ok(AcceptOutcome::Adopted { .. }) | Ok(AcceptOutcome::Unchanged { .. }) => true,
            // 写盘失败不妨碍使用已验证的新清单；下一次成功提交时再落盘。
            Err(AcceptError::Store(_)) => false,
            Err(error) => return Err(error),
        };
        Ok(self.install(&verified, &meta, now_ms, persisted))
    }

    fn install(
        &self,
        verified: &VerifiedAccessManifest,
        meta: &FetchMeta,
        now_ms: i64,
        persisted: bool,
    ) -> ManifestUpdate {
        let snapshot = DirectorySnapshot::from_verified(verified, meta.source.clone());
        let stale = snapshot.is_stale(now_ms);
        let policy = snapshot.policy().clone();
        match self.directory.replace_if_newer(snapshot) {
            Some(change) => {
                self.fetched_at_ms
                    .store(meta.fetched_at_ms, Ordering::Relaxed);
                self.health.apply_policy(&policy);
                ManifestUpdate::Adopted {
                    change,
                    stale,
                    persisted,
                }
            }
            None => ManifestUpdate::Unchanged {
                serial: self.directory.snapshot().serial(),
            },
        }
    }

    /// 接受密钥状态文档（吊销列表）。
    pub async fn accept_keystate(
        &self,
        envelope: &[u8],
        meta: FetchMeta,
        now_ms: i64,
    ) -> Result<KeystateUpdate, AcceptError> {
        let (Some(trust), Some(manifests)) = (&self.trust, &self.manifests) else {
            return Err(AcceptError::DevelopmentMode);
        };
        let current = trust.load_full();
        let keystate = current.verify_keystate(envelope, now_ms)?;
        let outcome = match manifests.commit_keystate(&keystate, &meta).await {
            Ok(outcome) => Some(outcome),
            // 写盘失败不能让吊销失效：内存里照常生效，下一次成功提交时再落盘。
            Err(AcceptError::Store(_)) => None,
            Err(error) => return Err(error),
        };
        if let Some(AcceptOutcome::Unchanged { serial }) = outcome
            && current.keystate_serial() == Some(serial)
        {
            return Ok(KeystateUpdate::Unchanged { serial });
        }
        let next = current.with_keystate(&keystate);
        trust.store(Arc::new(next));
        Ok(KeystateUpdate::Adopted {
            serial: keystate.serial,
            current_manifest_revoked: self.current_manifest_revoked(),
        })
    }

    /// 当前在用清单的签名密钥已被吊销：清单照常可用（连接有 TLS 保护），但应尽快换一份。
    pub fn current_manifest_revoked(&self) -> bool {
        let snapshot = self.directory.snapshot();
        let ManifestOrigin::Signed { key_id, .. } = snapshot.origin() else {
            return false;
        };
        self.trust()
            .is_some_and(|trust| trust.revoked_key_ids().any(|revoked| revoked == key_id))
    }

    /// 某个平台渠道的发布清单存储。开发模式没有信任材料，返回 `None`。
    pub fn release_store(&self, platform: &str, channel: &str) -> Option<ManifestStore> {
        let trust = self.trust()?;
        Some(
            ManifestStore::new(
                self.store.clone(),
                &self.bootstrap_id,
                trust.roots().fingerprint(),
            )
            .for_release(platform, channel),
        )
    }

    /// 读本地发布清单并重新验签：被改过的副本、已吊销密钥签的副本都不算数。
    pub async fn load_release(
        &self,
        releases: &ManifestStore,
        platform: &str,
        channel: &str,
        now_ms: i64,
    ) -> Option<(VerifiedReleaseManifest, StoredDocument)> {
        let trust = self.trust()?;
        let record = releases.load_release().await.ok().flatten()?;
        let bytes = record.envelope_bytes().ok()?;
        let verified = trust
            .verify_release(&bytes, platform, channel, now_ms)
            .ok()?;
        Some((verified, record))
    }

    /// 接受一份网络下发的发布清单信封：验签 → 防回滚提交。
    pub async fn accept_release(
        &self,
        releases: &ManifestStore,
        envelope: &[u8],
        platform: &str,
        channel: &str,
        meta: FetchMeta,
        now_ms: i64,
    ) -> Result<ReleaseUpdate, AcceptError> {
        let Some(trust) = self.trust() else {
            return Err(AcceptError::DevelopmentMode);
        };
        let verified = trust.verify_release(envelope, platform, channel, now_ms)?;
        match releases.commit_release(&verified, &meta).await {
            Ok(AcceptOutcome::Adopted { .. }) | Err(AcceptError::Store(_)) => {
                Ok(ReleaseUpdate::Adopted(verified))
            }
            Ok(AcceptOutcome::Unchanged { .. }) => Ok(ReleaseUpdate::Unchanged),
            Err(error) => Err(error),
        }
    }

    /// 健康簿落盘（节流；`force` 用于进入后台或退出前）。
    pub async fn flush_health(&self, now_ms: i64, force: bool) -> Result<usize, StoreError> {
        self.health.flush(&*self.store, now_ms, force).await
    }

    pub fn status(&self, now_ms: i64) -> AccessStatus {
        let snapshot = self.directory.snapshot();
        AccessStatus {
            serial: snapshot.serial(),
            source: match snapshot.origin() {
                ManifestOrigin::Signed { source, .. } => source.label(),
                ManifestOrigin::Development => "development".into(),
                ManifestOrigin::Unavailable => "none".into(),
            },
            stale: snapshot.is_stale(now_ms),
            development: snapshot.is_development(),
            issued_at_ms: snapshot.issued_at_ms(),
            expires_at_ms: snapshot.expires_at_ms(),
            fetched_at_ms: Some(self.fetched_at_ms.load(Ordering::Relaxed))
                .filter(|fetched| *fetched > 0),
            region_id: snapshot.home_region().to_owned(),
            cell_id: snapshot.home_cell().to_owned(),
            keystate_serial: self
                .trust
                .as_ref()
                .and_then(|trust| trust.load().keystate_serial()),
            has_manifest: snapshot.has_manifest(),
        }
    }

    /// 线路诊断页数据：归属 Cell 的全部接入点及其在当前网络下的健康状况。
    pub fn list_access_points(&self, net: NetPrint, now_ms: i64) -> Vec<AccessPointStatus> {
        let snapshot = self.directory.snapshot();
        let pinned = self.selector.pinned(now_ms);
        snapshot
            .access_points()
            .iter()
            .map(|entry| {
                let report = self.health.report(&entry.id, net, now_ms);
                AccessPointStatus {
                    ap_id: entry.id.clone(),
                    region_id: entry.region_id.to_string(),
                    host: entry.host.clone(),
                    status: entry.status,
                    rtt_ms: report.as_ref().and_then(|report| report.rtt_ms),
                    breaker: report
                        .as_ref()
                        .map_or(BreakerPhase::Closed, |report| report.breaker),
                    protocol_winner: report.and_then(|report| report.protocol_winner),
                    pinned: pinned.as_ref().is_some_and(|pin| pin.ap_id == entry.id),
                }
            })
            .collect()
    }
}

async fn load_keystate(
    manifests: &ManifestStore,
    trust: TrustStore,
    now_ms: i64,
    warnings: &mut Vec<AccessWarning>,
) -> TrustStore {
    let record = match manifests.load_keystate().await {
        Ok(Some(record)) => record,
        Ok(None) => return trust,
        Err(error) => {
            warnings.push(AccessWarning::StoredDocumentUnreadable {
                doc: DocType::KeyState,
                message: error.to_string(),
            });
            return trust;
        }
    };
    let verified = record
        .envelope_bytes()
        .map_err(|error| error.to_string())
        .and_then(|bytes| {
            trust
                .verify_keystate(&bytes, now_ms)
                .map_err(|error| error.code())
        });
    match verified {
        Ok(keystate) => trust.with_keystate(&keystate),
        Err(code) => {
            warnings.push(AccessWarning::StoredDocumentRejected {
                doc: DocType::KeyState,
                code,
            });
            trust
        }
    }
}

async fn load_lkg(
    manifests: &ManifestStore,
    trust: &TrustStore,
    now_ms: i64,
    warnings: &mut Vec<AccessWarning>,
) -> (
    Option<(VerifiedAccessManifest, StoredDocument)>,
    Option<String>,
) {
    let record = match manifests.load_access().await {
        Ok(Some(record)) => record,
        Ok(None) => return (None, None),
        Err(error) => {
            warnings.push(AccessWarning::StoredDocumentUnreadable {
                doc: DocType::Access,
                message: error.to_string(),
            });
            return (None, Some("unreadable".into()));
        }
    };
    let bytes = match record.envelope_bytes() {
        Ok(bytes) => bytes,
        Err(error) => {
            warnings.push(AccessWarning::StoredDocumentUnreadable {
                doc: DocType::Access,
                message: error.to_string(),
            });
            return (None, Some("unreadable".into()));
        }
    };
    match trust.verify_stored_access(&bytes, now_ms) {
        Ok(verified) => (Some((verified, record)), None),
        Err(error) => {
            let code = error.code();
            warnings.push(AccessWarning::StoredDocumentRejected {
                doc: DocType::Access,
                code: code.clone(),
            });
            (None, Some(code))
        }
    }
}

/// 启动时选出的初始目录。
enum Initial {
    Manifest {
        verified: VerifiedAccessManifest,
        source: ManifestSource,
        origin: InitialOrigin,
        fetched_at_ms: i64,
    },
    /// 都不可用。`sources` 是从被拒的 LKG 与种子里读出的引导源（线索）。
    Unavailable { sources: Vec<BootstrapSource> },
}

async fn choose_initial(
    config: &AccessConfig,
    trust: &TrustStore,
    manifests: &ManifestStore,
    now_ms: i64,
    warnings: &mut Vec<AccessWarning>,
) -> Initial {
    let (lkg, lkg_error) = load_lkg(manifests, trust, now_ms, warnings).await;
    let mut seed_error = None;
    let seed = match config.seed_bytes() {
        None => None,
        Some(bytes) => match trust.verify_access(bytes, now_ms) {
            Ok(verified) => Some(verified),
            Err(error) => {
                let code = error.code();
                warnings.push(AccessWarning::SeedRejected { code: code.clone() });
                seed_error = Some(code);
                None
            }
        },
    };
    match (lkg, seed) {
        (Some((lkg, _)), Some(seed)) if seed.serial > lkg.serial => {
            initial_from_seed(seed, manifests, now_ms, warnings).await
        }
        (Some((lkg, record)), _) => Initial::Manifest {
            verified: lkg,
            source: record.source,
            origin: InitialOrigin::Lkg,
            fetched_at_ms: record.fetched_at_ms,
        },
        (None, Some(seed)) => initial_from_seed(seed, manifests, now_ms, warnings).await,
        (None, None) => {
            warnings.push(AccessWarning::NoUsableManifest {
                lkg: lkg_error,
                seed: seed_error.or_else(|| config.seed_bytes().is_none().then(|| "absent".into())),
            });
            // 证书过期、吊销都可能让副本验签失败，但里面的引导源仍然是最好的线索。
            let mut sources = match manifests.load_access().await {
                Ok(Some(record)) => record
                    .envelope_bytes()
                    .map(|bytes| envelope_sources(&bytes))
                    .unwrap_or_default(),
                _ => Vec::new(),
            };
            for source in config
                .seed_bytes()
                .map(envelope_sources)
                .unwrap_or_default()
            {
                if !sources.contains(&source) {
                    sources.push(source);
                }
            }
            Initial::Unavailable { sources }
        }
    }
}

async fn initial_from_seed(
    seed: VerifiedAccessManifest,
    manifests: &ManifestStore,
    now_ms: i64,
    warnings: &mut Vec<AccessWarning>,
) -> Initial {
    let origin = adopt_seed(manifests, &seed, now_ms, warnings).await;
    Initial::Manifest {
        verified: seed,
        source: ManifestSource::Seed,
        origin,
        fetched_at_ms: 0,
    }
}

/// 种子写入 LKG。比已接受 serial 旧时只在内存中使用：种子来自安装包这一可信渠道，
/// 拒绝它会让 LKG 损坏的设备彻底失联；但它不能写回、也不能降低 maxSerial。
async fn adopt_seed(
    manifests: &ManifestStore,
    seed: &VerifiedAccessManifest,
    now_ms: i64,
    warnings: &mut Vec<AccessWarning>,
) -> InitialOrigin {
    match manifests
        .commit_access(seed, &FetchMeta::new(ManifestSource::Seed, now_ms))
        .await
    {
        Ok(_) => InitialOrigin::Seed { persisted: true },
        Err(AcceptError::Trust(TrustError::Rollback { serial, max_serial })) => {
            warnings.push(AccessWarning::SeedBelowStoredSerial {
                seed_serial: serial,
                stored_serial: max_serial,
            });
            InitialOrigin::Seed { persisted: false }
        }
        Err(error) => {
            warnings.push(AccessWarning::PersistFailed {
                message: error.to_string(),
            });
            InitialOrigin::Seed { persisted: false }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::DevelopmentEndpoints;
    use crate::health::Observation;
    use crate::select::SelectRequest;
    use crate::store::{MemoryAccessStore, keys};
    use crate::trust::sign::{Signer, encode_public_key, sign_keystate};
    use crate::trust::testkit::*;
    use crate::trust::{KeyState, TrustRootConfig};
    use crate::types::ServiceClass;

    fn config(seed_serial: Option<u64>) -> AccessConfig {
        AccessConfig::production(
            BOOTSTRAP_ID,
            vec![TrustRootConfig {
                key_id: ROOT_ID.into(),
                public_key: encode_public_key(&root_key().verifying_key()),
            }],
            seed_serial.map(|serial| {
                String::from_utf8(signed_manifest(&manifest_with_serial(serial))).unwrap()
            }),
        )
    }

    async fn boot(
        config: &AccessConfig,
        store: &Arc<MemoryAccessStore>,
    ) -> Result<(AccessKernel, BootstrapReport), AccessInitError> {
        AccessKernel::bootstrap(
            config,
            store.clone(),
            KernelOptions {
                quic_supported: true,
            },
            NOW_MS,
        )
        .await
    }

    #[tokio::test]
    async fn seed_then_lkg_then_newer_seed() {
        let store = Arc::new(MemoryAccessStore::new());
        let (kernel, report) = boot(&config(Some(5)), &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Seed { persisted: true });
        assert_eq!(report.serial, 5);
        assert!(report.warnings.is_empty());
        assert_eq!(kernel.status(NOW_MS).source, "seed");

        // 新清单经网络到达。
        let meta = FetchMeta::new(
            ManifestSource::Https {
                url: "https://m.example.com/".into(),
            },
            NOW_MS,
        );
        let update = kernel
            .accept_manifest(
                &signed_manifest(&manifest_with_serial(8)),
                meta.clone(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert!(matches!(
            update,
            ManifestUpdate::Adopted {
                persisted: true,
                stale: false,
                ..
            }
        ));
        let rollback = kernel
            .accept_manifest(&signed_manifest(&manifest_with_serial(7)), meta, NOW_MS)
            .await
            .unwrap_err();
        assert_eq!(
            rollback,
            AcceptError::Trust(TrustError::Rollback {
                serial: 7,
                max_serial: 8
            })
        );

        // 重启：LKG(8) 胜过种子(5)，来源保留为原始镜像。
        let (kernel, report) = boot(&config(Some(5)), &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Lkg);
        assert_eq!(report.serial, 8);
        assert_eq!(kernel.status(NOW_MS).source, "https:https://m.example.com/");

        // 新版本 App 带更新的种子(9)。
        let (_, report) = boot(&config(Some(9)), &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Seed { persisted: true });
        assert_eq!(report.serial, 9);
    }

    #[tokio::test]
    async fn corrupt_lkg_falls_back_to_older_seed_in_memory() {
        let store = Arc::new(MemoryAccessStore::new());
        boot(&config(Some(9)), &store).await.unwrap();
        store
            .put(&keys::access_current(BOOTSTRAP_ID), b"garbage")
            .await
            .unwrap();
        let (kernel, report) = boot(&config(Some(5)), &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Seed { persisted: false });
        assert!(
            report
                .warnings
                .contains(&AccessWarning::SeedBelowStoredSerial {
                    seed_serial: 5,
                    stored_serial: 9
                })
        );
        assert_eq!(kernel.directory().snapshot().serial(), 5);
        // maxSerial 未被降低。
        assert_eq!(
            store.get(&keys::access_serial(BOOTSTRAP_ID)).await.unwrap(),
            Some(b"9".to_vec())
        );
    }

    #[tokio::test]
    async fn no_manifest_boots_an_empty_directory() {
        let store = Arc::new(MemoryAccessStore::new());
        let (kernel, report) = boot(&config(None), &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Unavailable);
        assert_eq!(
            report.warnings,
            vec![AccessWarning::NoUsableManifest {
                lkg: None,
                seed: Some("absent".into())
            }]
        );
        let status = kernel.status(NOW_MS);
        assert_eq!((status.serial, status.source.as_str()), (0, "none"));
        assert!(!status.has_manifest);
        let selection = kernel.selector().select(&SelectRequest {
            service: ServiceClass::Im,
            net: NetPrint::UNKNOWN,
            net_tags: &[],
            current: None,
            exclude: &[],
            now_ms: NOW_MS,
        });
        assert!(selection.candidates.is_empty());

        // 空目录照常接受网络下发的清单。
        let update = kernel
            .accept_manifest(
                &signed_manifest(&manifest_with_serial(3)),
                FetchMeta::new(ManifestSource::Seed, NOW_MS),
                NOW_MS,
            )
            .await
            .unwrap();
        assert!(matches!(update, ManifestUpdate::Adopted { .. }));
        assert_eq!(kernel.directory().snapshot().serial(), 3);
    }

    #[tokio::test]
    async fn keystate_revocation_flags_current_manifest() {
        let store = Arc::new(MemoryAccessStore::new());
        let (kernel, _) = boot(&config(Some(5)), &store).await.unwrap();
        let root = root_key();
        let keystate = KeyState {
            schema: "flare.keystate.v1".into(),
            serial: 1,
            issued_at: "2026-11-04T00:00:00Z".into(),
            certificates: vec![],
            revoked_key_ids: vec![ONLINE_ID.into()],
        };
        let bytes = sign_keystate(
            &keystate,
            &Signer {
                key_id: ROOT_ID,
                key: &root,
            },
        );
        let update = kernel
            .accept_keystate(&bytes, FetchMeta::new(ManifestSource::Seed, NOW_MS), NOW_MS)
            .await
            .unwrap();
        assert_eq!(
            update,
            KeystateUpdate::Adopted {
                serial: 1,
                current_manifest_revoked: true
            }
        );
        let error = kernel
            .accept_manifest(
                &signed_manifest(&manifest_with_serial(6)),
                FetchMeta::new(ManifestSource::Seed, NOW_MS),
                NOW_MS,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            AcceptError::Trust(TrustError::Revoked {
                key_id: ONLINE_ID.into()
            })
        );
        // 重启后吊销仍生效：LKG 被拒，种子同样被拒；目录为空，但引导源作为线索留下。
        let (kernel, report) = boot(&config(Some(5)), &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Unavailable);
        let snapshot = kernel.directory().snapshot();
        assert!(!snapshot.has_manifest());
        assert_eq!(snapshot.manifest().bootstrap.sources.len(), 4);
    }

    #[tokio::test]
    async fn development_mode_selects_configured_endpoints() {
        let store = Arc::new(MemoryAccessStore::new());
        let config = AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051".into()),
            core_api_url: Some("http://127.0.0.1:60061".into()),
            ..Default::default()
        });
        let (kernel, report) = boot(&config, &store).await.unwrap();
        assert_eq!(report.origin, InitialOrigin::Development);
        assert_eq!(report.warnings, vec![AccessWarning::DevelopmentMode]);
        assert!(kernel.is_development());
        let selection = kernel.selector().select(&SelectRequest {
            service: ServiceClass::Im,
            net: NetPrint::UNKNOWN,
            net_tags: &[],
            current: None,
            exclude: &[],
            now_ms: NOW_MS,
        });
        let first = selection.first().unwrap();
        assert_eq!(first.protocols.as_ref().unwrap().order, vec![Protocol::Ws]);
        assert_eq!(
            selection.endpoint(first, crate::types::ServiceKind::ImWs),
            Some("ws://127.0.0.1:60051")
        );
        assert_eq!(
            kernel
                .accept_manifest(b"{}", FetchMeta::new(ManifestSource::Seed, NOW_MS), NOW_MS)
                .await
                .unwrap_err(),
            AcceptError::DevelopmentMode
        );
        kernel.health().record(
            "development",
            NetPrint::UNKNOWN,
            Observation::heartbeat_rtt(5),
            NOW_MS,
        );
        assert_eq!(kernel.flush_health(NOW_MS, true).await.unwrap(), 2);
        assert_eq!(
            kernel.list_access_points(NetPrint::UNKNOWN, NOW_MS)[0].rtt_ms,
            Some(5.0)
        );
    }
}
