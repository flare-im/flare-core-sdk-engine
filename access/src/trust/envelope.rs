//! DSSE（Dead Simple Signing Envelope）信封。
//!
//! 签名对象是载荷原始字节经 PAE 编码后的结果，不依赖 JSON 规范化：
//! 控制面用什么字节签，客户端就验什么字节，序列化差异不会让签名失效，也无法被利用。

use base64::Engine;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};

use super::error::TrustError;
use crate::manifest::MAX_RECOVERY_LINK_BYTES;
use crate::manifest::schema::{
    MAX_DOCUMENT_BYTES, MAX_KEY_CERTS, MAX_SIGNATURES, RECOVERY_PAYLOAD_TYPE,
};
use crate::parsed_url::ParsedUrl;

/// 生成时一律输出 base64url 无填充；解析时按 DSSE 规范同时接受标准与 URL 安全字母表、有无填充。
/// 编码差异不影响安全性：签名覆盖的是解码后的字节。
const URL_SAFE_LENIENT: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);
const STANDARD_LENIENT: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);
const URL_SAFE_NO_PAD: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new().with_encode_padding(false),
);

pub const RECOVERY_LINK_SCHEME: &str = "flare-recover";
const RECOVERY_LINK_PREFIX: &str = "flare-recover://v1";

pub(crate) fn decode_b64(value: &str) -> Option<Vec<u8>> {
    if value.contains(['+', '/']) {
        STANDARD_LENIENT.decode(value).ok()
    } else {
        URL_SAFE_LENIENT.decode(value).ok()
    }
}

pub(crate) fn encode_b64(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Envelope {
    pub payload_type: String,
    /// base64url(载荷原始字节)。
    pub payload: String,
    pub signatures: Vec<EnvelopeSignature>,
    /// base64url(在线密钥证书的 DSSE 信封 JSON)；根签名的文档（密钥状态）为空。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_certs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeSignature {
    pub keyid: String,
    /// base64url(Ed25519 签名)。
    pub sig: String,
}

/// DSSE 的预认证编码：`"DSSEv1" SP len(type) SP type SP len(body) SP body`。
pub fn pae(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let type_len = payload_type.len().to_string();
    let body_len = payload.len().to_string();
    let mut out = Vec::with_capacity(
        6 + 4 + type_len.len() + payload_type.len() + body_len.len() + payload.len(),
    );
    out.extend_from_slice(b"DSSEv1 ");
    out.extend_from_slice(type_len.as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload_type.as_bytes());
    out.push(b' ');
    out.extend_from_slice(body_len.as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload);
    out
}

impl Envelope {
    /// 解析信封：先限长再解 JSON，并给签名与证书条数设上限，
    /// 防止超大或超多条目的文档在验签前就消耗大量 CPU。
    pub fn parse(bytes: &[u8]) -> Result<Self, TrustError> {
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(TrustError::TooLarge {
                actual: bytes.len(),
                limit: MAX_DOCUMENT_BYTES,
            });
        }
        let envelope: Envelope = serde_json::from_slice(bytes)
            .map_err(|_| TrustError::MalformedEnvelope("not a DSSE envelope"))?;
        if envelope.signatures.is_empty() {
            return Err(TrustError::MalformedEnvelope("no signatures"));
        }
        if envelope.signatures.len() > MAX_SIGNATURES {
            return Err(TrustError::MalformedEnvelope("too many signatures"));
        }
        if envelope.key_certs.len() > MAX_KEY_CERTS {
            return Err(TrustError::MalformedEnvelope("too many key certificates"));
        }
        Ok(envelope)
    }

    /// 校验 payloadType 并解码载荷。
    pub fn open_payload(&self, expected_type: &'static str) -> Result<Vec<u8>, TrustError> {
        if self.payload_type != expected_type {
            return Err(TrustError::WrongPayloadType {
                expected: expected_type,
                actual: self.payload_type.clone(),
            });
        }
        decode_b64(&self.payload).ok_or(TrustError::MalformedEnvelope("payload is not base64"))
    }

    pub fn to_json_bytes(&self) -> Vec<u8> {
        // 结构体只含字符串与数组，序列化不会失败。
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// 恢复链接：`flare-recover://v1?p=<载荷>&k=<根 keyid>&s=<签名>`，只有一条根签名，其余字段都是定值。
    /// 载荷与签名已是 base64url，不做二次编码：整条链接要装进一个二维码。
    pub fn to_recovery_link(&self) -> Option<String> {
        let [signature] = self.signatures.as_slice() else {
            return None;
        };
        let mut link = ParsedUrl::parse(RECOVERY_LINK_PREFIX).ok()?;
        link.append_query_pair("p", &self.payload);
        link.append_query_pair("k", &signature.keyid);
        link.append_query_pair("s", &signature.sig);
        Some(link.to_string())
    }

    /// 解析恢复链接（深链或二维码内容）。签名在 `TrustStore::verify_recovery` 里校验。
    pub fn from_recovery_link(text: &str) -> Result<Self, TrustError> {
        let text = text.trim();
        if text.len() > MAX_RECOVERY_LINK_BYTES {
            return Err(TrustError::TooLarge {
                actual: text.len(),
                limit: MAX_RECOVERY_LINK_BYTES,
            });
        }
        let link = ParsedUrl::parse(text)
            .map_err(|_| TrustError::MalformedEnvelope("not a recovery link"))?;
        if link.scheme() != RECOVERY_LINK_SCHEME || link.host_str() != "v1" {
            return Err(TrustError::MalformedEnvelope("not a recovery link"));
        }
        let field = |name: &str| {
            link.query_pairs()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value)
                .filter(|value| !value.is_empty())
                .ok_or(TrustError::MalformedEnvelope("recovery link is incomplete"))
        };
        Ok(Self {
            payload_type: RECOVERY_PAYLOAD_TYPE.to_owned(),
            payload: field("p")?,
            signatures: vec![EnvelopeSignature {
                keyid: field("k")?,
                sig: field("s")?,
            }],
            key_certs: Vec::new(),
        })
    }
}

/// 用 `verify_strict` 校验一条签名：拒绝非规范 S 与小阶公钥，
/// 与 `flare-plugin-protocol` 发布者签名使用同一实现与同一严格度。
pub(crate) fn verify_signature(
    key: &VerifyingKey,
    payload_type: &str,
    payload: &[u8],
    signature_b64: &str,
) -> bool {
    // 64 字节签名的 base64 不超过 88 字符；先限长再解码攻击者可控的输入。
    if signature_b64.len() > 128 {
        return false;
    }
    let Some(bytes) = decode_b64(signature_b64) else {
        return false;
    };
    let Ok(signature) = Signature::from_slice(&bytes) else {
        return false;
    };
    key.verify_strict(&pae(payload_type, payload), &signature)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pae_matches_dsse_spec_example() {
        // DSSE 规范 protocol.md 中的示例。
        assert_eq!(
            pae("http://example.com/HelloWorld", b"hello world"),
            b"DSSEv1 29 http://example.com/HelloWorld 11 hello world".to_vec()
        );
    }

    #[test]
    fn base64_accepts_both_alphabets_and_padding() {
        let bytes = [0xfbu8, 0xff, 0x01];
        assert_eq!(decode_b64("-_8B").unwrap(), bytes);
        assert_eq!(decode_b64("+/8B").unwrap(), bytes);
        assert_eq!(decode_b64("AQ").unwrap(), vec![1]);
        assert_eq!(decode_b64("AQ==").unwrap(), vec![1]);
        assert_eq!(encode_b64(&bytes), "-_8B");
    }

    #[test]
    fn oversized_and_empty_envelopes_are_rejected() {
        let big = vec![b' '; MAX_DOCUMENT_BYTES + 1];
        assert!(matches!(
            Envelope::parse(&big),
            Err(TrustError::TooLarge { .. })
        ));
        let empty = br#"{"payloadType":"x","payload":"","signatures":[]}"#;
        assert_eq!(
            Envelope::parse(empty),
            Err(TrustError::MalformedEnvelope("no signatures"))
        );
    }
}
