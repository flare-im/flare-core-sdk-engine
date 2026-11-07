//! 验签与信任链错误。
//!
//! 错误按「为什么不可信」细分，`code()` 给出跨端稳定的字符串（向量与诊断导出使用），
//! `kind()` 归成粗类别，供引擎决定是否降权引导源、是否上报。

use serde::Serialize;

use crate::manifest::ManifestError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrustError {
    #[error("document is {actual} bytes, larger than the {limit}-byte limit")]
    TooLarge { actual: usize, limit: usize },
    #[error("malformed envelope: {0}")]
    MalformedEnvelope(&'static str),
    #[error("payloadType {actual:?} does not match the expected {expected:?}")]
    WrongPayloadType {
        expected: &'static str,
        actual: String,
    },
    /// 签名对不上：载荷被改动，或签名伪造。
    #[error("signature by key {key_id:?} does not verify (document altered or forged)")]
    BadSignature { key_id: String },
    /// 签名所用的 keyid 没有任何（无论有效与否）证书。
    #[error("no certificate for signing key {key_id:?}")]
    UnknownKey { key_id: String },
    #[error("certificate for key {key_id:?} rejected: {reason}")]
    KeyCert {
        key_id: String,
        reason: KeyCertError,
    },
    #[error("signing key {key_id:?} has been revoked")]
    Revoked { key_id: String },
    /// 只在调用方要求「过期即拒收」时出现；接入清单过期只标记 stale。
    #[error("document expired at {expires_at_ms}")]
    Expired { expires_at_ms: i64 },
    #[error("serial {serial} is older than the stored serial {max_serial}")]
    Rollback { serial: u64, max_serial: u64 },
    #[error("bootstrapId {actual:?} does not belong to this application ({expected:?})")]
    WrongBootstrapId { expected: String, actual: String },
    /// 发布清单是真的，但不是本平台本渠道的：别的渠道的清单不能顶替这一份。
    #[error("release manifest is for {actual:?}, expected {expected:?}")]
    WrongReleaseTarget { expected: String, actual: String },
    #[error("unsupported document schema {0:?}")]
    UnsupportedSchema(String),
    #[error("invalid document: {0}")]
    InvalidPayload(ManifestError),
    #[error("invalid trust configuration: {0}")]
    Config(&'static str),
}

/// 在线密钥证书被拒的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyCertError {
    #[error("certificate is malformed")]
    Malformed,
    #[error("certificate payloadType is wrong")]
    WrongPayloadType,
    #[error("certificate schema is unsupported")]
    UnsupportedSchema,
    #[error("certificate is not signed by a built-in root")]
    UnknownRoot,
    #[error("root signature on the certificate does not verify")]
    BadRootSignature,
    #[error("rootKeyId does not name the root that signed it")]
    RootMismatch,
    #[error("certificate is not valid yet")]
    NotYetValid,
    #[error("certificate has expired")]
    Expired,
    #[error("certificate usages do not cover this document type")]
    UsageNotAllowed,
    #[error("certificate scope does not cover this bootstrapId")]
    ScopeMismatch,
}

impl KeyCertError {
    pub fn code(self) -> &'static str {
        match self {
            KeyCertError::Malformed => "malformed",
            KeyCertError::WrongPayloadType => "wrongPayloadType",
            KeyCertError::UnsupportedSchema => "unsupportedSchema",
            KeyCertError::UnknownRoot => "unknownRoot",
            KeyCertError::BadRootSignature => "badRootSignature",
            KeyCertError::RootMismatch => "rootMismatch",
            KeyCertError::NotYetValid => "notYetValid",
            KeyCertError::Expired => "expired",
            KeyCertError::UsageNotAllowed => "usageNotAllowed",
            KeyCertError::ScopeMismatch => "scopeMismatch",
        }
    }
}

/// 粗类别。引擎据此决定：篡改/回退 → 该源降权；过期/吊销 → 提示刷新信任材料；
/// 其余结构性问题 → 视为源失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TrustFailureKind {
    TooLarge,
    Malformed,
    WrongPayloadType,
    Tampered,
    Untrusted,
    Revoked,
    Expired,
    Rollback,
    WrongBootstrapId,
    UnsupportedSchema,
    InvalidPayload,
    Config,
}

impl TrustFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TrustFailureKind::TooLarge => "tooLarge",
            TrustFailureKind::Malformed => "malformed",
            TrustFailureKind::WrongPayloadType => "wrongPayloadType",
            TrustFailureKind::Tampered => "tampered",
            TrustFailureKind::Untrusted => "untrusted",
            TrustFailureKind::Revoked => "revoked",
            TrustFailureKind::Expired => "expired",
            TrustFailureKind::Rollback => "rollback",
            TrustFailureKind::WrongBootstrapId => "wrongBootstrapId",
            TrustFailureKind::UnsupportedSchema => "unsupportedSchema",
            TrustFailureKind::InvalidPayload => "invalidPayload",
            TrustFailureKind::Config => "config",
        }
    }
}

impl TrustError {
    pub fn kind(&self) -> TrustFailureKind {
        match self {
            TrustError::TooLarge { .. } => TrustFailureKind::TooLarge,
            TrustError::MalformedEnvelope(_) => TrustFailureKind::Malformed,
            TrustError::WrongPayloadType { .. } => TrustFailureKind::WrongPayloadType,
            TrustError::BadSignature { .. } => TrustFailureKind::Tampered,
            TrustError::UnknownKey { .. } => TrustFailureKind::Untrusted,
            TrustError::KeyCert { reason, .. } => match reason {
                KeyCertError::BadRootSignature => TrustFailureKind::Tampered,
                KeyCertError::NotYetValid | KeyCertError::Expired => TrustFailureKind::Expired,
                KeyCertError::UnknownRoot
                | KeyCertError::RootMismatch
                | KeyCertError::UsageNotAllowed
                | KeyCertError::ScopeMismatch => TrustFailureKind::Untrusted,
                KeyCertError::Malformed
                | KeyCertError::WrongPayloadType
                | KeyCertError::UnsupportedSchema => TrustFailureKind::Malformed,
            },
            TrustError::Revoked { .. } => TrustFailureKind::Revoked,
            TrustError::Expired { .. } => TrustFailureKind::Expired,
            TrustError::Rollback { .. } => TrustFailureKind::Rollback,
            TrustError::WrongBootstrapId { .. } | TrustError::WrongReleaseTarget { .. } => {
                TrustFailureKind::WrongBootstrapId
            }
            TrustError::UnsupportedSchema(_) => TrustFailureKind::UnsupportedSchema,
            TrustError::InvalidPayload(_) => TrustFailureKind::InvalidPayload,
            TrustError::Config(_) => TrustFailureKind::Config,
        }
    }

    /// 跨端稳定的错误码，例如 `badSignature`、`keyCert.expired`。
    pub fn code(&self) -> String {
        match self {
            TrustError::TooLarge { .. } => "tooLarge".into(),
            TrustError::MalformedEnvelope(_) => "malformedEnvelope".into(),
            TrustError::WrongPayloadType { .. } => "wrongPayloadType".into(),
            TrustError::BadSignature { .. } => "badSignature".into(),
            TrustError::UnknownKey { .. } => "unknownKey".into(),
            TrustError::KeyCert { reason, .. } => format!("keyCert.{}", reason.code()),
            TrustError::Revoked { .. } => "revoked".into(),
            TrustError::Expired { .. } => "expired".into(),
            TrustError::Rollback { .. } => "rollback".into(),
            TrustError::WrongBootstrapId { .. } => "wrongBootstrapId".into(),
            TrustError::WrongReleaseTarget { .. } => "wrongReleaseTarget".into(),
            TrustError::UnsupportedSchema(_) => "unsupportedSchema".into(),
            TrustError::InvalidPayload(_) => "invalidPayload".into(),
            TrustError::Config(_) => "config".into(),
        }
    }
}
