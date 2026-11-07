//! 签名工具：生成 DSSE 信封与在线密钥证书。
//!
//! 客户端不持有任何私钥；这里服务于控制面编排器、`flarectl trust` 仪式、构建期种子校验工具
//! 和本 crate 的测试向量。验签与签名放在同一处，保证两侧对 PAE 与编码的理解一致。

use ed25519_dalek::Signer as _;

pub use ed25519_dalek::{SigningKey, VerifyingKey};

use super::envelope::{Envelope, EnvelopeSignature, encode_b64, pae};
use super::keycert::KeyCert;
use super::keystate::KeyState;
use crate::manifest::RecoveryManifest;
use crate::manifest::schema::{
    ACCESS_PAYLOAD_TYPE, KEYCERT_PAYLOAD_TYPE, KEYSTATE_PAYLOAD_TYPE, RECOVERY_PAYLOAD_TYPE,
    RELEASE_PAYLOAD_TYPE,
};

/// 一把签名密钥及其 keyid。
pub struct Signer<'a> {
    pub key_id: &'a str,
    pub key: &'a SigningKey,
}

/// 对载荷字节签名并封装成 DSSE 信封。
pub fn sign_envelope(
    payload_type: &str,
    payload: &[u8],
    signers: &[Signer<'_>],
    key_certs: &[String],
) -> Envelope {
    let message = pae(payload_type, payload);
    Envelope {
        payload_type: payload_type.to_owned(),
        payload: encode_b64(payload),
        signatures: signers
            .iter()
            .map(|signer| EnvelopeSignature {
                keyid: signer.key_id.to_owned(),
                sig: encode_b64(&signer.key.sign(&message).to_bytes()),
            })
            .collect(),
        key_certs: key_certs.to_vec(),
    }
}

/// 根密钥签发在线密钥证书，返回可直接放进 `keyCerts` 的字符串。
pub fn issue_key_cert(root: &Signer<'_>, cert: &KeyCert) -> String {
    let payload = serde_json::to_vec(cert).unwrap_or_default();
    let envelope = sign_envelope(
        KEYCERT_PAYLOAD_TYPE,
        &payload,
        std::slice::from_ref(root),
        &[],
    );
    encode_b64(&envelope.to_json_bytes())
}

/// 在线密钥签接入清单载荷（原始 JSON 字节）。
pub fn sign_access_manifest(payload: &[u8], online: &Signer<'_>, key_certs: &[String]) -> Vec<u8> {
    sign_envelope(
        ACCESS_PAYLOAD_TYPE,
        payload,
        std::slice::from_ref(online),
        key_certs,
    )
    .to_json_bytes()
}

/// 在线密钥签发布清单载荷（原始 JSON 字节）。
pub fn sign_release_manifest(payload: &[u8], online: &Signer<'_>, key_certs: &[String]) -> Vec<u8> {
    sign_envelope(
        RELEASE_PAYLOAD_TYPE,
        payload,
        std::slice::from_ref(online),
        key_certs,
    )
    .to_json_bytes()
}

/// 根密钥签密钥状态文档。
pub fn sign_keystate(keystate: &KeyState, root: &Signer<'_>) -> Vec<u8> {
    let payload = serde_json::to_vec(keystate).unwrap_or_default();
    sign_envelope(
        KEYSTATE_PAYLOAD_TYPE,
        &payload,
        std::slice::from_ref(root),
        &[],
    )
    .to_json_bytes()
}

/// 根密钥签恢复清单，返回可直接下发的恢复链接（`flare-recover://…`）。
pub fn sign_recovery_link(recovery: &RecoveryManifest, root: &Signer<'_>) -> Option<String> {
    let payload = serde_json::to_vec(recovery).ok()?;
    sign_envelope(
        RECOVERY_PAYLOAD_TYPE,
        &payload,
        std::slice::from_ref(root),
        &[],
    )
    .to_recovery_link()
}

/// 公钥的 base64url 表示（配置与证书中使用的格式）。
pub fn encode_public_key(key: &VerifyingKey) -> String {
    encode_b64(key.as_bytes())
}
