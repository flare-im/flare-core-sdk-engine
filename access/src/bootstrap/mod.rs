//! 引导：初始化配置、构建期种子、开发模式与分层拉取。
//!
//! 分层拉取（镜像、Edge well-known、DoH TXT）在 [`BootstrapFetcher`]：网络能力经 [`HttpFetch`] 由
//! 引擎注入，拿到的信封交给 [`AccessKernel::accept_manifest`](crate::kernel::AccessKernel::accept_manifest)。
//! 何时刷新（周期、通知、恢复退避）由引擎决定。

pub mod development;
pub mod doh;
pub mod fetcher;
mod sources;

use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use development::{
    DEVELOPMENT_ID, DevelopmentAccessPoint, DevelopmentEndpoints, DevelopmentError,
    development_snapshot,
};
pub use fetcher::{
    Adoption, AttemptResult, BootstrapClock, BootstrapFetcher, DEMOTION_MS, DOH_TIMEOUT_MS,
    MAX_IN_FLIGHT, MIRROR_TIMEOUT_MS, NOTIFIED_RETRY_DELAY_MS, RefreshMode, RefreshOutcome,
    RefreshReport, STAGGER_MS, SourceAttempt, SourceTier, WELL_KNOWN_TIMEOUT_MS,
};

use crate::manifest::access::validate_source;
use crate::manifest::schema::MAX_BOOTSTRAP_SOURCES;
use crate::manifest::{BootstrapSection, BootstrapSource, MAX_DOCUMENT_BYTES, is_valid_id};
use crate::platform::MaybeSendSync;
use crate::trust::{Envelope, TrustRootConfig, decode_b64};

/// 信任模式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TrustMode {
    /// 只接受信任链验证通过的清单。
    #[default]
    Production,
    /// 允许用 `developmentEndpoints` 直接构造未签名清单（本地联调与 E2E）。
    Development,
}

/// 初始化配置中的 `access` 对象（§7.6）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessConfig {
    /// 本应用的引导 ID；开发模式可省略（取 `development`）。
    #[serde(default)]
    pub bootstrap_id: String,
    /// 内置根公钥（构建期注入）。
    #[serde(default)]
    pub trust_roots: Vec<TrustRootConfig>,
    /// 构建期写入的已签名接入清单（DSSE 信封 JSON 原文）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<String>,
    #[serde(default)]
    pub trust_mode: TrustMode,
    /// 仅开发模式。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub development_endpoints: Option<DevelopmentEndpoints>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("access.bootstrapId is missing or invalid")]
    InvalidBootstrapId,
    #[error("access.trustRoots is required in production mode")]
    MissingTrustRoots,
    #[error("access.developmentEndpoints is only allowed when trustMode is development")]
    DevelopmentEndpointsInProduction,
    #[error("access.developmentEndpoints is required when trustMode is development")]
    MissingDevelopmentEndpoints,
    #[error("access.seed exceeds {0} bytes")]
    SeedTooLarge(usize),
}

impl AccessConfig {
    /// 生产配置。
    pub fn production(
        bootstrap_id: impl Into<String>,
        trust_roots: Vec<TrustRootConfig>,
        seed: Option<String>,
    ) -> Self {
        Self {
            bootstrap_id: bootstrap_id.into(),
            trust_roots,
            seed,
            trust_mode: TrustMode::Production,
            development_endpoints: None,
        }
    }

    /// 开发配置。
    pub fn development(endpoints: DevelopmentEndpoints) -> Self {
        Self {
            bootstrap_id: DEVELOPMENT_ID.into(),
            trust_roots: Vec::new(),
            seed: None,
            trust_mode: TrustMode::Development,
            development_endpoints: Some(endpoints),
        }
    }

    /// 生效的 bootstrapId（开发模式缺省为 `development`）。
    pub fn effective_bootstrap_id(&self) -> &str {
        if self.bootstrap_id.is_empty() && self.trust_mode == TrustMode::Development {
            DEVELOPMENT_ID
        } else {
            &self.bootstrap_id
        }
    }

    /// 配置自检。生产模式下出现开发端点直接报错，而不是静默忽略：
    /// 这是发布构建误带调试地址的最后一道防线。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !is_valid_id(self.effective_bootstrap_id()) {
            return Err(ConfigError::InvalidBootstrapId);
        }
        match self.trust_mode {
            TrustMode::Production => {
                if self.development_endpoints.is_some() {
                    return Err(ConfigError::DevelopmentEndpointsInProduction);
                }
                if self.trust_roots.is_empty() {
                    return Err(ConfigError::MissingTrustRoots);
                }
            }
            TrustMode::Development => {
                if self.development_endpoints.is_none() {
                    return Err(ConfigError::MissingDevelopmentEndpoints);
                }
            }
        }
        if let Some(seed) = &self.seed
            && seed.len() > MAX_DOCUMENT_BYTES
        {
            return Err(ConfigError::SeedTooLarge(MAX_DOCUMENT_BYTES));
        }
        Ok(())
    }

    /// 种子信封字节（空串视为没有种子）。
    pub fn seed_bytes(&self) -> Option<&[u8]> {
        self.seed
            .as_deref()
            .map(str::trim)
            .filter(|seed| !seed.is_empty())
            .map(str::as_bytes)
    }
}

/// 从文档信封里读出引导源，不验签：这些只是「去哪儿拉」的线索，拉到的清单仍要完整验签，
/// 被篡改的线索最多造成拒绝服务（与 DoH TXT 指针同样的道理）。证书过期或密钥被吊销的副本
/// 不能再当目录用，但这是全新安装的老版本、长期离线的设备手里唯一的地址。
pub fn envelope_sources(bytes: &[u8]) -> Vec<BootstrapSource> {
    #[derive(Deserialize)]
    struct Fragment {
        #[serde(default)]
        bootstrap: BootstrapSection,
    }
    let Some(payload) = Envelope::parse(bytes)
        .ok()
        .and_then(|envelope| decode_b64(&envelope.payload))
    else {
        return Vec::new();
    };
    let Ok(fragment) = serde_json::from_slice::<Fragment>(&payload) else {
        return Vec::new();
    };
    fragment
        .bootstrap
        .sources
        .into_iter()
        .filter(|source| validate_source(source, "source").is_ok())
        .take(MAX_BOOTSTRAP_SOURCES)
        .collect()
}

/// 拉取引导源时使用的 HTTP 能力，由引擎注入（原生 reqwest、wasm fetch）。
/// 本 crate 不直接依赖任何 HTTP 栈。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchRequest {
    pub url: String,
    /// 条件请求：上次的 ETag。浏览器里由它自己的 HTTP 缓存负责，实现可以忽略。
    pub if_none_match: Option<String>,
    /// `Accept` 请求头（DoH 用）。
    pub accept: Option<&'static str>,
    pub timeout_ms: u32,
    /// 响应体上限（解压后），超出即中止（清单 64 KiB）。
    pub max_body_bytes: usize,
    /// 仅原生端：DNS 失败或结果可疑时直连，SNI 与证书校验仍按 URL 的主机名。
    pub ip_hints: Vec<IpAddr>,
    /// 非空时证书链中必须有匹配的 SPKI SHA-256（来自接入清单）。
    pub spki_pins: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    pub status: u16,
    pub etag: Option<String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    #[error("device has no network")]
    NoNetwork,
    #[error("host unreachable")]
    Unreachable,
    #[error("TLS verification failed")]
    Tls,
    #[error("request timed out")]
    Timeout,
    #[error("response body exceeds the limit")]
    TooLarge,
    #[error("request was cancelled")]
    Cancelled,
}

impl FetchError {
    /// 跨端稳定的错误码（诊断导出使用）。
    pub fn code(self) -> &'static str {
        match self {
            FetchError::NoNetwork => "noNetwork",
            FetchError::Unreachable => "unreachable",
            FetchError::Tls => "tls",
            FetchError::Timeout => "timeout",
            FetchError::TooLarge => "tooLarge",
            FetchError::Cancelled => "cancelled",
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait HttpFetch: MaybeSendSync {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_modes() {
        let roots = vec![TrustRootConfig {
            key_id: "root-a".into(),
            public_key: "x".into(),
        }];
        assert_eq!(
            AccessConfig::production("b7f3c9a1e2d4b6a8", roots.clone(), None).validate(),
            Ok(())
        );
        assert_eq!(
            AccessConfig::production("b7f3c9a1e2d4b6a8", vec![], None).validate(),
            Err(ConfigError::MissingTrustRoots)
        );
        assert_eq!(
            AccessConfig::production("bad/id", roots.clone(), None).validate(),
            Err(ConfigError::InvalidBootstrapId)
        );
        let mut leaked = AccessConfig::production("b7f3c9a1e2d4b6a8", roots, None);
        leaked.development_endpoints = Some(DevelopmentEndpoints::default());
        assert_eq!(
            leaked.validate(),
            Err(ConfigError::DevelopmentEndpointsInProduction)
        );
        let development = AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051".into()),
            ..Default::default()
        });
        assert_eq!(development.validate(), Ok(()));
        let parsed: AccessConfig = serde_json::from_value(serde_json::json!({
            "trustMode": "development",
            "developmentEndpoints": { "wsUrl": "ws://127.0.0.1:60051" }
        }))
        .unwrap();
        assert_eq!(parsed.effective_bootstrap_id(), "development");
        assert_eq!(parsed.validate(), Ok(()));
    }
}
