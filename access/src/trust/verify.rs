//! 验签流程（§4.2）：信封 → payloadType → 证书链 → 签名 → 载荷结构 → bootstrapId → 过期判定。
//! serial 防回滚依赖持久化的 `maxSerial`，由 `store::ManifestStore` 在提交时用 [`check_serial`] 完成。

use std::collections::BTreeSet;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::envelope::{Envelope, verify_signature};
use super::error::TrustError;
use super::keycert::{CertRejection, ValidCert, validate_cert};
use super::keystate::{KeyState, MAX_REVOKED_KEY_IDS, VerifiedKeyState};
use super::roots::TrustRoots;
use crate::manifest::schema::{DocType, KEYSTATE_SCHEMA, MAX_KEY_CERTS, RECOVERY_PAYLOAD_TYPE};
use crate::manifest::{
    AccessManifest, MAX_RECOVERY_LINK_BYTES, ManifestError, RecoveryManifest, ReleaseManifest,
    is_valid_id,
};

/// 验签通过的接入清单。
#[derive(Debug, Clone)]
pub struct VerifiedAccessManifest {
    pub manifest: Arc<AccessManifest>,
    pub serial: u64,
    /// 签名所用的在线密钥。
    pub key_id: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    /// 原始信封字节：持久化时原样写入，读回后重新验签。
    pub envelope: Arc<[u8]>,
    /// `sha256:<hex>`，用于诊断与日志，不参与判定。
    pub digest: String,
}

impl VerifiedAccessManifest {
    /// 过期的接入清单仍可用于连接（连接本身有 TLS 保护，过期只说明拓扑可能陈旧），
    /// 调用方据此标记 stale 并提高刷新频率。
    pub fn is_stale(&self, now_ms: i64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

/// 验签通过的发布清单。
#[derive(Debug, Clone)]
pub struct VerifiedReleaseManifest {
    pub manifest: Arc<ReleaseManifest>,
    pub serial: u64,
    /// 签名所用的在线密钥。
    pub key_id: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    /// 原始信封字节：持久化时原样写入，读回后重新验签。
    pub envelope: Arc<[u8]>,
    /// `sha256:<hex>`，用于诊断与日志，不参与判定。
    pub digest: String,
}

impl VerifiedReleaseManifest {
    /// 过期的发布清单只能给出可选提示，不能据此强制更新（以服务端门禁为准）。
    pub fn is_stale(&self, now_ms: i64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

/// 文档过期时的处理方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryMode {
    /// 接受并由调用方标记 stale（接入清单）。
    MarkStale,
    /// 直接拒收（发布清单等过期后不得据此行动的文档）。
    Reject,
}

/// serial 比较结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialCheck {
    /// 比已存的新，应采纳。
    Newer,
    /// 与已存的相同：视为同一份文档，直接复用已有副本。
    Same,
}

/// 防回滚：只接受不小于 `maxSerial` 的文档。清单内容的「回滚」由控制面以更大的 serial 重新签发。
pub fn check_serial(serial: u64, max_serial: Option<u64>) -> Result<SerialCheck, TrustError> {
    match max_serial {
        None => Ok(SerialCheck::Newer),
        Some(max) if serial > max => Ok(SerialCheck::Newer),
        Some(max) if serial == max => Ok(SerialCheck::Same),
        Some(max) => Err(TrustError::Rollback {
            serial,
            max_serial: max,
        }),
    }
}

/// 信任材料：根公钥、本应用的 bootstrapId、最近一份密钥状态（吊销列表与随附证书）。
///
/// 不可变：装入新的密钥状态得到一个新实例（[`TrustStore::with_keystate`]），
/// 引擎可以放在 `ArcSwap` 里原子替换，验签过程不持锁。
#[derive(Debug, Clone)]
pub struct TrustStore {
    roots: TrustRoots,
    bootstrap_id: String,
    revoked: BTreeSet<String>,
    keystate_certs: Vec<String>,
    keystate_serial: Option<u64>,
}

impl TrustStore {
    pub fn new(roots: TrustRoots, bootstrap_id: impl Into<String>) -> Result<Self, TrustError> {
        let bootstrap_id = bootstrap_id.into();
        if !is_valid_id(&bootstrap_id) {
            return Err(TrustError::Config("bootstrapId is invalid"));
        }
        Ok(Self {
            roots,
            bootstrap_id,
            revoked: BTreeSet::new(),
            keystate_certs: Vec::new(),
            keystate_serial: None,
        })
    }

    pub fn roots(&self) -> &TrustRoots {
        &self.roots
    }

    pub fn bootstrap_id(&self) -> &str {
        &self.bootstrap_id
    }

    pub fn keystate_serial(&self) -> Option<u64> {
        self.keystate_serial
    }

    pub fn revoked_key_ids(&self) -> impl Iterator<Item = &str> {
        self.revoked.iter().map(String::as_str)
    }

    /// 装入一份已验签的密钥状态。比当前更旧的直接忽略（返回原样副本），
    /// 这样即使调用方顺序出错也不会把吊销列表倒退。
    pub fn with_keystate(&self, keystate: &VerifiedKeyState) -> TrustStore {
        if self
            .keystate_serial
            .is_some_and(|current| keystate.serial < current)
        {
            return self.clone();
        }
        TrustStore {
            roots: self.roots.clone(),
            bootstrap_id: self.bootstrap_id.clone(),
            revoked: keystate.revoked.clone(),
            keystate_certs: keystate.certificates.clone(),
            keystate_serial: Some(keystate.serial),
        }
    }

    /// 校验接入清单；过期只标记，不拒收。
    pub fn verify_access(
        &self,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<VerifiedAccessManifest, TrustError> {
        self.verify_access_with(bytes, now_ms, ExpiryMode::MarkStale)
    }

    pub fn verify_access_with(
        &self,
        bytes: &[u8],
        now_ms: i64,
        expiry: ExpiryMode,
    ) -> Result<VerifiedAccessManifest, TrustError> {
        let envelope = Envelope::parse(bytes)?;
        let payload = envelope.open_payload(DocType::Access.payload_type())?;
        // 签名先于载荷解析：未验签的内容只经过信封层的限长 JSON 解析。
        let key_id = self.verify_cert_signed(&envelope, DocType::Access, &payload, now_ms)?;
        let (manifest, times) =
            AccessManifest::from_slice(&payload).map_err(|error| match error {
                ManifestError::UnsupportedSchema(schema) => TrustError::UnsupportedSchema(schema),
                other => TrustError::InvalidPayload(other),
            })?;
        if manifest.bootstrap_id != self.bootstrap_id {
            return Err(TrustError::WrongBootstrapId {
                expected: self.bootstrap_id.clone(),
                actual: manifest.bootstrap_id,
            });
        }
        if expiry == ExpiryMode::Reject && now_ms >= times.expires_at_ms {
            return Err(TrustError::Expired {
                expires_at_ms: times.expires_at_ms,
            });
        }
        Ok(VerifiedAccessManifest {
            serial: manifest.serial,
            manifest: Arc::new(manifest),
            key_id,
            issued_at_ms: times.issued_at_ms,
            expires_at_ms: times.expires_at_ms,
            envelope: Arc::from(bytes),
            digest: digest(bytes),
        })
    }

    /// 校验发布清单：与接入清单同一条信任链，另外要求它正是本平台本渠道的那一份。
    /// 过期不拒收：是否据此强制由调用方按 [`VerifiedReleaseManifest::is_stale`] 判断。
    pub fn verify_release(
        &self,
        bytes: &[u8],
        platform: &str,
        channel: &str,
        now_ms: i64,
    ) -> Result<VerifiedReleaseManifest, TrustError> {
        let envelope = Envelope::parse(bytes)?;
        let payload = envelope.open_payload(DocType::Release.payload_type())?;
        // 签名先于载荷解析：未验签的内容只经过信封层的限长 JSON 解析。
        let key_id = self.verify_cert_signed(&envelope, DocType::Release, &payload, now_ms)?;
        let (manifest, times) =
            ReleaseManifest::from_slice(&payload).map_err(|error| match error {
                ManifestError::UnsupportedSchema(schema) => TrustError::UnsupportedSchema(schema),
                other => TrustError::InvalidPayload(other),
            })?;
        if manifest.bootstrap_id != self.bootstrap_id {
            return Err(TrustError::WrongBootstrapId {
                expected: self.bootstrap_id.clone(),
                actual: manifest.bootstrap_id,
            });
        }
        if manifest.platform != platform || manifest.channel != channel {
            return Err(TrustError::WrongReleaseTarget {
                expected: format!("{platform}/{channel}"),
                actual: format!("{}/{}", manifest.platform, manifest.channel),
            });
        }
        Ok(VerifiedReleaseManifest {
            serial: manifest.serial,
            manifest: Arc::new(manifest),
            key_id,
            issued_at_ms: times.issued_at_ms,
            expires_at_ms: times.expires_at_ms,
            envelope: Arc::from(bytes),
            digest: digest(bytes),
        })
    }

    /// 校验恢复清单（[`Envelope::from_recovery_link`] 的结果或本地保存的副本）并展开成接入清单：
    /// 由内置根直接签名，不依赖在线密钥与它的吊销状态，正是在线密钥出事时也要能用。
    pub fn verify_recovery(&self, bytes: &[u8]) -> Result<VerifiedAccessManifest, TrustError> {
        let envelope = Envelope::parse(bytes)?;
        let payload = envelope.open_payload(RECOVERY_PAYLOAD_TYPE)?;
        if payload.len() > MAX_RECOVERY_LINK_BYTES {
            return Err(TrustError::TooLarge {
                actual: payload.len(),
                limit: MAX_RECOVERY_LINK_BYTES,
            });
        }
        let key_id = self.verify_root_signed(&envelope, RECOVERY_PAYLOAD_TYPE, &payload)?;
        let recovery =
            RecoveryManifest::from_slice(&payload).map_err(TrustError::InvalidPayload)?;
        let (manifest, times) = recovery.to_access_manifest().map_err(|error| match error {
            ManifestError::UnsupportedSchema(schema) => TrustError::UnsupportedSchema(schema),
            other => TrustError::InvalidPayload(other),
        })?;
        if manifest.bootstrap_id != self.bootstrap_id {
            return Err(TrustError::WrongBootstrapId {
                expected: self.bootstrap_id.clone(),
                actual: manifest.bootstrap_id,
            });
        }
        Ok(VerifiedAccessManifest {
            serial: manifest.serial,
            manifest: Arc::new(manifest),
            key_id,
            issued_at_ms: times.issued_at_ms,
            expires_at_ms: times.expires_at_ms,
            envelope: Arc::from(bytes),
            digest: digest(bytes),
        })
    }

    /// 校验本地保存的 LKG：通常是接入清单，导入过恢复链接后可能是恢复清单。
    /// 网络下发的文档走 [`Self::verify_access`]，镜像不能拿恢复清单顶替接入清单。
    pub fn verify_stored_access(
        &self,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<VerifiedAccessManifest, TrustError> {
        if Envelope::parse(bytes)?.payload_type == RECOVERY_PAYLOAD_TYPE {
            self.verify_recovery(bytes)
        } else {
            self.verify_access(bytes, now_ms)
        }
    }

    /// 校验密钥状态文档：必须由内置根直接签名。
    pub fn verify_keystate(
        &self,
        bytes: &[u8],
        _now_ms: i64,
    ) -> Result<VerifiedKeyState, TrustError> {
        let envelope = Envelope::parse(bytes)?;
        let payload = envelope.open_payload(DocType::KeyState.payload_type())?;
        let signed_by =
            self.verify_root_signed(&envelope, DocType::KeyState.payload_type(), &payload)?;
        let header: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|error| TrustError::InvalidPayload(ManifestError::Json(error.to_string())))?;
        let schema = header
            .get("schema")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if schema != KEYSTATE_SCHEMA {
            return Err(TrustError::UnsupportedSchema(schema.to_owned()));
        }
        let keystate: KeyState = serde_json::from_value(header)
            .map_err(|error| TrustError::InvalidPayload(ManifestError::Json(error.to_string())))?;
        if keystate.revoked_key_ids.len() > MAX_REVOKED_KEY_IDS {
            return Err(TrustError::InvalidPayload(ManifestError::Invalid {
                field: "revokedKeyIds".into(),
                reason: "too many entries",
            }));
        }
        if keystate.certificates.len() > MAX_KEY_CERTS {
            return Err(TrustError::InvalidPayload(ManifestError::Invalid {
                field: "certificates".into(),
                reason: "too many entries",
            }));
        }
        Ok(VerifiedKeyState {
            serial: keystate.serial,
            signed_by,
            revoked: keystate.revoked_key_ids.into_iter().collect(),
            certificates: keystate.certificates,
            envelope: Arc::from(bytes),
        })
    }

    /// 根密钥直接签名的文档：至少一条签名出自内置根且验签通过，返回该根的 keyid。
    fn verify_root_signed(
        &self,
        envelope: &Envelope,
        payload_type: &str,
        payload: &[u8],
    ) -> Result<String, TrustError> {
        let mut first_root_failure: Option<&str> = None;
        for signature in &envelope.signatures {
            if let Some(root) = self.roots.get(&signature.keyid) {
                if verify_signature(root, payload_type, payload, &signature.sig) {
                    return Ok(signature.keyid.clone());
                }
                first_root_failure.get_or_insert(signature.keyid.as_str());
            }
        }
        Err(match first_root_failure {
            Some(key_id) => TrustError::BadSignature {
                key_id: key_id.to_owned(),
            },
            None => TrustError::UnknownKey {
                key_id: envelope.signatures[0].keyid.clone(),
            },
        })
    }

    /// 在线密钥签名的文档：至少一条签名的 keyid 对应一张有效证书且验签通过。
    ///
    /// 全部失败时报告第一条签名的失败原因，保证同一输入在各端得到同一个错误码。
    fn verify_cert_signed(
        &self,
        envelope: &Envelope,
        doc: DocType,
        payload: &[u8],
        now_ms: i64,
    ) -> Result<String, TrustError> {
        let usage = doc.cert_usage().unwrap_or_default();
        let mut valid: Vec<ValidCert> = Vec::new();
        let mut rejected: Vec<(Option<String>, CertRejection)> = Vec::new();
        for encoded in envelope.key_certs.iter().chain(self.keystate_certs.iter()) {
            match validate_cert(
                encoded,
                &self.roots,
                usage,
                &self.bootstrap_id,
                &self.revoked,
                now_ms,
            ) {
                Ok(cert) => valid.push(cert),
                Err(rejection) => rejected.push(rejection),
            }
        }

        let mut first_error: Option<TrustError> = None;
        for signature in &envelope.signatures {
            let key_id = signature.keyid.clone();
            let error = if self.revoked.contains(&key_id) {
                TrustError::Revoked { key_id }
            } else if let Some(cert) = valid.iter().find(|cert| cert.key_id == key_id) {
                if verify_signature(&cert.key, doc.payload_type(), payload, &signature.sig) {
                    return Ok(key_id);
                }
                TrustError::BadSignature { key_id }
            } else if let Some((_, rejection)) = rejected
                .iter()
                .find(|(claimed, _)| claimed.as_deref() == Some(key_id.as_str()))
                // 连 keyid 都读不出的证书（例如被改坏）归到这条签名上：它是唯一可能的证书来源。
                .or_else(|| rejected.iter().find(|(claimed, _)| claimed.is_none()))
            {
                match rejection {
                    CertRejection::Cert(reason) => TrustError::KeyCert {
                        key_id,
                        reason: *reason,
                    },
                    CertRejection::Revoked => TrustError::Revoked { key_id },
                }
            } else {
                TrustError::UnknownKey { key_id }
            };
            first_error.get_or_insert(error);
        }
        Err(first_error.unwrap_or(TrustError::MalformedEnvelope("no signatures")))
    }
}

fn digest(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in hash {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
