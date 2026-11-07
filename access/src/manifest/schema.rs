//! 文档类型、schema 名与体积/数量上限。
//!
//! 上限的意义：清单来自网络（镜像、Edge、恢复链接），在验签之前就要解析信封，
//! 所有可由攻击者放大的维度（字节数、签名数、证书数）都必须先有界。

use serde::Serialize;

/// 签名后信封的最大字节数；更大的一律拒收。
pub const MAX_DOCUMENT_BYTES: usize = 64 * 1024;
/// 单个信封最多处理的签名条数（轮换重叠期两签即可，留余量）。
pub const MAX_SIGNATURES: usize = 8;
/// 单个信封最多携带的在线密钥证书数。
pub const MAX_KEY_CERTS: usize = 8;
/// 一张在线密钥证书的作用范围里最多列出的 bootstrapId 数。
pub const MAX_KEY_CERT_BOOTSTRAP_IDS: usize = 256;
/// 内置信任根上限。
pub const MAX_TRUST_ROOTS: usize = 8;

pub const MAX_REGIONS: usize = 32;
pub const MAX_CELLS_PER_REGION: usize = 64;
/// 全清单接入点总数上限；也是健康簿每个网络维度的条目上限。
pub const MAX_ACCESS_POINTS: usize = 256;
pub const MAX_TAGS_PER_AP: usize = 16;
pub const MAX_IP_HINTS_PER_AP: usize = 8;
pub const MAX_SPKI_PINS_PER_AP: usize = 8;
pub const MAX_BOOTSTRAP_SOURCES: usize = 16;

/// 同渠道发布清单保留的版本数：最新灰度版、当前全量版、上一个全量版。
pub const MAX_RELEASES: usize = 3;
pub const MAX_BLOCKED_BUILD_RANGES: usize = 64;
pub const MAX_RELEASE_ARTIFACTS: usize = 32;
pub const MAX_RELEASE_TARGETS: usize = 16;
pub const MAX_ARTIFACT_URLS: usize = 8;
pub const MAX_RESOURCE_PACKS: usize = 64;

pub const ACCESS_SCHEMA: &str = "flare.access.v1";
pub const KEYCERT_SCHEMA: &str = "flare.keycert.v1";
pub const KEYSTATE_SCHEMA: &str = "flare.keystate.v1";
pub const RECOVERY_SCHEMA: &str = "flare.recovery.v1";
pub const RELEASE_SCHEMA: &str = "flare.release.v1";

pub const ACCESS_PAYLOAD_TYPE: &str = "application/vnd.flare.access.v1+json";
pub const KEYCERT_PAYLOAD_TYPE: &str = "application/vnd.flare.keycert.v1+json";
pub const KEYSTATE_PAYLOAD_TYPE: &str = "application/vnd.flare.keystate.v1+json";
pub const RELEASE_PAYLOAD_TYPE: &str = "application/vnd.flare.release.v1+json";
pub const RECOVERY_PAYLOAD_TYPE: &str = "application/vnd.flare.recovery.v1+json";

/// 受 serial 防回滚保护的文档类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DocType {
    /// 接入清单：在线密钥签名。
    Access,
    /// 密钥状态文档（吊销）：根密钥签名。
    KeyState,
    /// 发布清单：在线密钥签名。
    Release,
}

impl DocType {
    pub fn payload_type(self) -> &'static str {
        match self {
            DocType::Access => ACCESS_PAYLOAD_TYPE,
            DocType::KeyState => KEYSTATE_PAYLOAD_TYPE,
            DocType::Release => RELEASE_PAYLOAD_TYPE,
        }
    }

    /// 在线密钥证书 `usages` 中必须出现的用途；根签名的文档返回 `None`。
    pub fn cert_usage(self) -> Option<&'static str> {
        match self {
            DocType::Access => Some("access"),
            DocType::Release => Some("release"),
            DocType::KeyState => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            DocType::Access => "access",
            DocType::KeyState => "keys",
            DocType::Release => "release",
        }
    }
}
