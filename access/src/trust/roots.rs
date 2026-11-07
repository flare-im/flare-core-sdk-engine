//! 编译进 App 的信任根。

use ed25519_dalek::VerifyingKey;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::envelope::decode_b64;
use super::error::TrustError;
use crate::manifest::is_valid_id;
use crate::manifest::schema::MAX_TRUST_ROOTS;

/// 初始化配置里的一条根公钥（构建期注入，与 `tlsCaCert` 同一机制）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustRootConfig {
    pub key_id: String,
    /// base64url(32 字节 Ed25519 公钥)。
    pub public_key: String,
}

/// 根公钥集合。根只签「在线密钥证书」与「密钥状态文档」，从不直接签接入清单。
#[derive(Debug, Clone)]
pub struct TrustRoots {
    roots: Vec<(String, VerifyingKey)>,
    fingerprint: String,
}

impl TrustRoots {
    pub fn from_config(entries: &[TrustRootConfig]) -> Result<Self, TrustError> {
        let mut keys = Vec::with_capacity(entries.len());
        for entry in entries {
            let bytes = decode_b64(&entry.public_key)
                .ok_or(TrustError::Config("trust root publicKey is not base64"))?;
            let key: [u8; 32] = bytes
                .try_into()
                .map_err(|_| TrustError::Config("trust root publicKey must be 32 bytes"))?;
            keys.push((entry.key_id.clone(), key));
        }
        Self::from_keys(keys)
    }

    pub fn from_keys(
        entries: impl IntoIterator<Item = (String, [u8; 32])>,
    ) -> Result<Self, TrustError> {
        let mut roots: Vec<(String, VerifyingKey)> = Vec::new();
        for (key_id, bytes) in entries {
            if !is_valid_id(&key_id) {
                return Err(TrustError::Config("trust root keyId is invalid"));
            }
            if roots.iter().any(|(existing, _)| *existing == key_id) {
                return Err(TrustError::Config("duplicate trust root keyId"));
            }
            let key = VerifyingKey::from_bytes(&bytes).map_err(|_| {
                TrustError::Config("trust root publicKey is not a valid Ed25519 key")
            })?;
            if key.is_weak() {
                return Err(TrustError::Config("trust root publicKey is a weak key"));
            }
            roots.push((key_id, key));
        }
        if roots.is_empty() {
            return Err(TrustError::Config(
                "production mode needs at least one trust root",
            ));
        }
        if roots.len() > MAX_TRUST_ROOTS {
            return Err(TrustError::Config("too many trust roots"));
        }
        let fingerprint = fingerprint(&roots);
        Ok(Self { roots, fingerprint })
    }

    pub fn get(&self, key_id: &str) -> Option<&VerifyingKey> {
        self.roots
            .iter()
            .find(|(id, _)| id == key_id)
            .map(|(_, key)| key)
    }

    pub fn contains(&self, key_id: &str) -> bool {
        self.get(key_id).is_some()
    }

    pub fn len(&self) -> usize {
        self.roots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    /// 根集合指纹（16 位十六进制）。密钥状态文档与它的 serial 按指纹分区存放：
    /// 同一设备上换了一套根（例如测试包与正式包共用数据目录），旧根体系的 serial 不会挡住新体系。
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

fn fingerprint(roots: &[(String, VerifyingKey)]) -> String {
    let mut sorted: Vec<&(String, VerifyingKey)> = roots.iter().collect();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = Sha256::new();
    for (key_id, key) in sorted {
        hasher.update(key_id.as_bytes());
        hasher.update([0u8]);
        hasher.update(key.as_bytes());
    }
    let digest = hasher.finalize();
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
