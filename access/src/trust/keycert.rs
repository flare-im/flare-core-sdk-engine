//! 在线密钥证书：根密钥为在线签名密钥背书（有效期、用途、作用范围）。

use std::collections::BTreeSet;

use ed25519_dalek::VerifyingKey;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::envelope::{Envelope, decode_b64, verify_signature};
use super::error::KeyCertError;
use super::roots::TrustRoots;
use crate::manifest::schema::{KEYCERT_PAYLOAD_TYPE, KEYCERT_SCHEMA, MAX_KEY_CERT_BOOTSTRAP_IDS};
use crate::manifest::{DocType, ManifestError, is_valid_id};
use crate::time::{DAY_MS, parse_rfc3339_ms};

/// 证书有效期判定允许的时钟偏差：设备时钟常有误差，过严会让时钟稍快/稍慢的设备整体失联。
pub const CERT_CLOCK_SKEW_MS: i64 = DAY_MS;

/// 在线密钥证书的最长有效期（到第 120 天轮换，重叠期两证共存）。
/// 它同时是密钥泄露且吊销文档被全部镜像扣住时的暴露窗口上限，所以签发时强制。
pub const MAX_CERT_VALIDITY_DAYS: i64 = 180;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeyCert {
    /// 固定为 `flare.keycert.v1`。
    #[schemars(extend("const" = "flare.keycert.v1"))]
    pub schema: String,
    #[serde(rename = "keyid")]
    pub key_id: String,
    /// base64url(32 字节 Ed25519 公钥)。
    pub public_key: String,
    /// `access`、`release`。
    pub usages: Vec<String>,
    pub scope: KeyScope,
    /// RFC 3339。
    #[schemars(extend("format" = "date-time"))]
    pub not_before: String,
    /// RFC 3339；与 `notBefore` 相距不超过 180 天。
    #[schemars(extend("format" = "date-time"))]
    pub not_after: String,
    pub root_key_id: String,
}

impl KeyCert {
    /// 待签发的证书。`public_key` 是在线公钥的 base64url，见 [`super::sign::encode_public_key`]。
    pub fn new(
        key_id: impl Into<String>,
        public_key: impl Into<String>,
        usages: Vec<String>,
        scope: KeyScope,
        not_before: impl Into<String>,
        not_after: impl Into<String>,
        root_key_id: impl Into<String>,
    ) -> Self {
        Self {
            schema: KEYCERT_SCHEMA.to_owned(),
            key_id: key_id.into(),
            public_key: public_key.into(),
            usages,
            scope,
            not_before: not_before.into(),
            not_after: not_after.into(),
            root_key_id: root_key_id.into(),
        }
    }

    /// 签发前的结构校验。比客户端验证书时更严：客户端只判定证书此刻能否用，
    /// 这里还要拦下根本不该签出去的证书（超过最长有效期、用途不认识、公钥不合法）。
    pub fn validate(&self) -> Result<(), ManifestError> {
        let invalid = |field: &str, reason: &'static str| ManifestError::Invalid {
            field: field.to_owned(),
            reason,
        };
        if self.schema != KEYCERT_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(self.schema.clone()));
        }
        if !is_valid_id(&self.key_id) {
            return Err(invalid("keyid", "must be 1-64 chars of [A-Za-z0-9._:-]"));
        }
        if !is_valid_id(&self.root_key_id) {
            return Err(invalid(
                "rootKeyId",
                "must be 1-64 chars of [A-Za-z0-9._:-]",
            ));
        }
        decode_public_key(&self.public_key)
            .ok_or_else(|| invalid("publicKey", "not a usable Ed25519 public key"))?;
        let usages: BTreeSet<&str> = self.usages.iter().map(String::as_str).collect();
        if usages.is_empty()
            || usages.len() != self.usages.len()
            || !usages.iter().all(|usage| {
                [DocType::Access, DocType::Release]
                    .iter()
                    .any(|doc| doc.cert_usage() == Some(usage))
            })
        {
            return Err(invalid(
                "usages",
                "must list access and/or release, each once",
            ));
        }
        let bootstrap_ids = &self.scope.bootstrap_ids;
        if bootstrap_ids.is_empty()
            || bootstrap_ids.len() > MAX_KEY_CERT_BOOTSTRAP_IDS
            || !bootstrap_ids
                .iter()
                .all(|scope| scope == "*" || is_valid_id(scope))
        {
            return Err(invalid(
                "scope.bootstrapIds",
                "must list \"*\" or 1-256 bootstrap ids",
            ));
        }
        let not_before = parse_rfc3339_ms(&self.not_before)
            .ok_or_else(|| invalid("notBefore", "not RFC 3339"))?;
        let not_after =
            parse_rfc3339_ms(&self.not_after).ok_or_else(|| invalid("notAfter", "not RFC 3339"))?;
        if not_after <= not_before {
            return Err(invalid("notAfter", "must be later than notBefore"));
        }
        if not_after - not_before > MAX_CERT_VALIDITY_DAYS * DAY_MS {
            return Err(invalid("notAfter", "validity must not exceed 180 days"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeyScope {
    /// `"*"` 表示全部；否则只覆盖列出的 bootstrapId。
    pub bootstrap_ids: Vec<String>,
}

impl KeyScope {
    /// 覆盖全部应用。
    pub fn all() -> Self {
        Self {
            bootstrap_ids: vec!["*".to_owned()],
        }
    }

    pub fn covers(&self, bootstrap_id: &str) -> bool {
        self.bootstrap_ids
            .iter()
            .any(|scope| scope == "*" || scope == bootstrap_id)
    }
}

/// 解析配置与证书里的公钥（base64url），拒绝非法点与弱公钥。
pub fn decode_public_key(encoded: &str) -> Option<VerifyingKey> {
    let bytes: [u8; 32] = decode_b64(encoded)?.try_into().ok()?;
    let key = VerifyingKey::from_bytes(&bytes).ok()?;
    (!key.is_weak()).then_some(key)
}

/// 一张已拆开、尚未验根签名的证书。
struct OpenedCert {
    envelope: Envelope,
    payload: Vec<u8>,
    cert: KeyCert,
}

fn open_cert(encoded: &str) -> Result<OpenedCert, KeyCertError> {
    let bytes = decode_b64(encoded).ok_or(KeyCertError::Malformed)?;
    let envelope = Envelope::parse(&bytes).map_err(|_| KeyCertError::Malformed)?;
    if envelope.payload_type != KEYCERT_PAYLOAD_TYPE {
        return Err(KeyCertError::WrongPayloadType);
    }
    let payload = decode_b64(&envelope.payload).ok_or(KeyCertError::Malformed)?;
    let cert = serde_json::from_slice(&payload).map_err(|_| KeyCertError::Malformed)?;
    Ok(OpenedCert {
        envelope,
        payload,
        cert,
    })
}

/// 至少一条签名来自内置根且验签通过，并与证书声明的 `rootKeyId` 一致。
fn check_root_signature(opened: &OpenedCert, roots: &TrustRoots) -> Result<(), KeyCertError> {
    let mut signed_by: Option<&str> = None;
    let mut saw_root = false;
    for signature in &opened.envelope.signatures {
        if let Some(root) = roots.get(&signature.keyid) {
            saw_root = true;
            if verify_signature(root, KEYCERT_PAYLOAD_TYPE, &opened.payload, &signature.sig) {
                signed_by = Some(signature.keyid.as_str());
                break;
            }
        }
    }
    let Some(root_id) = signed_by else {
        return Err(if saw_root {
            KeyCertError::BadRootSignature
        } else {
            KeyCertError::UnknownRoot
        });
    };
    if opened.cert.root_key_id != root_id {
        return Err(KeyCertError::RootMismatch);
    }
    Ok(())
}

/// 验根签名并返回证书内容，不看时间、用途与范围。
/// 控制面接收离线仪式签回的证书时用它：证书可能尚未生效，也要先确认它确实出自根密钥。
pub fn verify_key_cert(encoded: &str, roots: &TrustRoots) -> Result<KeyCert, KeyCertError> {
    let opened = open_cert(encoded)?;
    if opened.cert.schema != KEYCERT_SCHEMA {
        return Err(KeyCertError::UnsupportedSchema);
    }
    check_root_signature(&opened, roots)?;
    Ok(opened.cert)
}

/// 一张已通过全部检查的证书。
#[derive(Debug, Clone)]
pub(crate) struct ValidCert {
    pub key_id: String,
    pub key: VerifyingKey,
}

/// 校验一张证书（`keyCerts` 中的一项，或密钥状态文档携带的证书）。
///
/// 返回值里的 keyid 在失败时也尽量给出：它取自未验签的载荷，只用于把错误归到具体的签名上，
/// 不参与任何信任判定。
pub(crate) fn validate_cert(
    encoded: &str,
    roots: &TrustRoots,
    usage: &str,
    bootstrap_id: &str,
    revoked: &BTreeSet<String>,
    now_ms: i64,
) -> Result<ValidCert, (Option<String>, CertRejection)> {
    let opened = open_cert(encoded).map_err(|reason| (None, CertRejection::Cert(reason)))?;
    let claimed = Some(opened.cert.key_id.clone());
    let reject = |reason: KeyCertError| (claimed.clone(), CertRejection::Cert(reason));

    if opened.cert.schema != KEYCERT_SCHEMA {
        return Err(reject(KeyCertError::UnsupportedSchema));
    }
    check_root_signature(&opened, roots).map_err(reject)?;
    let cert = opened.cert;

    let not_before =
        parse_rfc3339_ms(&cert.not_before).ok_or_else(|| reject(KeyCertError::Malformed))?;
    let not_after =
        parse_rfc3339_ms(&cert.not_after).ok_or_else(|| reject(KeyCertError::Malformed))?;
    if not_before > now_ms.saturating_add(CERT_CLOCK_SKEW_MS) {
        return Err(reject(KeyCertError::NotYetValid));
    }
    if not_after < now_ms.saturating_sub(CERT_CLOCK_SKEW_MS) {
        return Err(reject(KeyCertError::Expired));
    }
    if !cert.usages.iter().any(|candidate| candidate == usage) {
        return Err(reject(KeyCertError::UsageNotAllowed));
    }
    if !cert.scope.covers(bootstrap_id) {
        return Err(reject(KeyCertError::ScopeMismatch));
    }
    if revoked.contains(&cert.key_id) {
        return Err((claimed.clone(), CertRejection::Revoked));
    }
    let key = decode_public_key(&cert.public_key).ok_or_else(|| reject(KeyCertError::Malformed))?;
    Ok(ValidCert {
        key_id: cert.key_id,
        key,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CertRejection {
    Cert(KeyCertError),
    Revoked,
}
