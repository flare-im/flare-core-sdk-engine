//! 密钥状态文档（吊销列表）：由根密钥直接签名，与接入清单一同拉取，同样受 serial 防回滚保护。

use std::collections::BTreeSet;
use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::manifest::schema::{KEYSTATE_SCHEMA, MAX_KEY_CERTS};
use crate::manifest::{ManifestError, is_valid_id};
use crate::time::parse_rfc3339_ms;

/// 吊销列表与随附证书的条数上限：文档整体已限 64 KiB，这里再限条数让内存占用可预期。
pub const MAX_REVOKED_KEY_IDS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeyState {
    /// 固定为 `flare.keystate.v1`。
    #[schemars(extend("const" = "flare.keystate.v1"))]
    pub schema: String,
    pub serial: u64,
    /// RFC 3339。
    #[schemars(extend("format" = "date-time"))]
    pub issued_at: String,
    /// 当前有效的在线密钥证书（与清单 `keyCerts` 同格式）。清单轮换时客户端可以从这里拿到新证书。
    #[serde(default)]
    pub certificates: Vec<String>,
    /// 已吊销的在线密钥 keyid。
    #[serde(default)]
    pub revoked_key_ids: Vec<String>,
}

impl KeyState {
    pub fn new(
        serial: u64,
        issued_at: impl Into<String>,
        certificates: Vec<String>,
        revoked_key_ids: Vec<String>,
    ) -> Self {
        Self {
            schema: KEYSTATE_SCHEMA.to_owned(),
            serial,
            issued_at: issued_at.into(),
            certificates,
            revoked_key_ids,
        }
    }

    /// 签发前的结构校验，覆盖客户端验签时的条数上限。
    pub fn validate(&self) -> Result<(), ManifestError> {
        let invalid = |field: &str, reason: &'static str| ManifestError::Invalid {
            field: field.to_owned(),
            reason,
        };
        if self.schema != KEYSTATE_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(self.schema.clone()));
        }
        if parse_rfc3339_ms(&self.issued_at).is_none() {
            return Err(invalid("issuedAt", "not RFC 3339"));
        }
        if self.certificates.len() > MAX_KEY_CERTS {
            return Err(invalid("certificates", "too many entries"));
        }
        if self.revoked_key_ids.len() > MAX_REVOKED_KEY_IDS {
            return Err(invalid("revokedKeyIds", "too many entries"));
        }
        if !self.revoked_key_ids.iter().all(|id| is_valid_id(id)) {
            return Err(invalid(
                "revokedKeyIds",
                "must be 1-64 chars of [A-Za-z0-9._:-]",
            ));
        }
        Ok(())
    }
}

/// 验签通过的密钥状态文档。
#[derive(Debug, Clone)]
pub struct VerifiedKeyState {
    pub serial: u64,
    pub signed_by: String,
    pub revoked: BTreeSet<String>,
    pub certificates: Vec<String>,
    /// 原始信封字节，持久化时原样写入，读回后重新验签。
    pub envelope: Arc<[u8]>,
}
