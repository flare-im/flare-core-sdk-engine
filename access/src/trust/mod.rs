//! 信任链：DSSE 信封、内置根、在线密钥证书、密钥状态（吊销）与验签流程。
//!
//! 两级密钥：根密钥离线保存，只签「在线密钥证书」与「密钥状态文档」；
//! 在线密钥签接入清单与发布清单。客户端只内置根公钥，在线密钥轮换不需要发版。
//! 底层签名算法是 Ed25519 `verify_strict`（`ed25519-dalek`），与插件发布者签名同一实现。

mod envelope;
mod error;
mod keycert;
mod keystate;
mod roots;
pub mod sign;
mod verify;

pub use envelope::{Envelope, EnvelopeSignature, RECOVERY_LINK_SCHEME, pae};
pub use error::{KeyCertError, TrustError, TrustFailureKind};
pub use keycert::{
    CERT_CLOCK_SKEW_MS, KeyCert, KeyScope, MAX_CERT_VALIDITY_DAYS, decode_public_key,
    verify_key_cert,
};
pub use keystate::{KeyState, MAX_REVOKED_KEY_IDS, VerifiedKeyState};
pub use roots::{TrustRootConfig, TrustRoots};
pub use verify::{
    ExpiryMode, SerialCheck, TrustStore, VerifiedAccessManifest, VerifiedReleaseManifest,
    check_serial,
};

#[cfg(any(test, feature = "client"))]
pub(crate) use envelope::{decode_b64, encode_b64};

#[cfg(test)]
pub(crate) mod testkit {
    //! 单元测试用的确定性密钥与签名清单。
    use ed25519_dalek::SigningKey;

    use super::sign::{Signer, issue_key_cert, sign_access_manifest};
    use super::*;

    pub const ROOT_ID: &str = "root-a";
    pub const ONLINE_ID: &str = "ok-2026h2-01";
    pub const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
    /// 2026-11-04T08:00:00Z
    pub const NOW_MS: i64 = 1_793_779_200_000;

    pub fn root_key() -> SigningKey {
        SigningKey::from_bytes(&[0x11; 32])
    }

    pub fn online_key() -> SigningKey {
        SigningKey::from_bytes(&[0x33; 32])
    }

    pub fn roots() -> TrustRoots {
        TrustRoots::from_keys([(ROOT_ID.to_owned(), root_key().verifying_key().to_bytes())])
            .unwrap()
    }

    pub fn trust() -> TrustStore {
        TrustStore::new(roots(), BOOTSTRAP_ID).unwrap()
    }

    pub fn cert() -> KeyCert {
        KeyCert {
            schema: "flare.keycert.v1".into(),
            key_id: ONLINE_ID.into(),
            public_key: sign::encode_public_key(&online_key().verifying_key()),
            usages: vec!["access".into(), "release".into()],
            scope: KeyScope {
                bootstrap_ids: vec!["*".into()],
            },
            not_before: "2026-07-01T00:00:00Z".into(),
            not_after: "2026-12-28T00:00:00Z".into(),
            root_key_id: ROOT_ID.into(),
        }
    }

    pub fn issued_cert(cert: &KeyCert) -> String {
        let root = root_key();
        issue_key_cert(
            &Signer {
                key_id: ROOT_ID,
                key: &root,
            },
            cert,
        )
    }

    /// 用默认证书签一份清单载荷。
    pub fn signed_manifest(payload: &serde_json::Value) -> Vec<u8> {
        let online = online_key();
        sign_access_manifest(
            &serde_json::to_vec(payload).unwrap(),
            &Signer {
                key_id: ONLINE_ID,
                key: &online,
            },
            &[issued_cert(&cert())],
        )
    }

    /// 用默认证书签一份发布清单载荷。只有客户端部分的测试在用。
    #[cfg(feature = "client")]
    pub fn signed_release(payload: &serde_json::Value) -> Vec<u8> {
        let online = online_key();
        super::sign::sign_release_manifest(
            &serde_json::to_vec(payload).unwrap(),
            &Signer {
                key_id: ONLINE_ID,
                key: &online,
            },
            &[issued_cert(&cert())],
        )
    }

    pub fn manifest_with_serial(serial: u64) -> serde_json::Value {
        let mut value = crate::manifest::access::tests::sample_manifest_json();
        value["serial"] = serial.into();
        value
    }
}

#[cfg(test)]
mod tests {
    use super::sign::{Signer, sign_keystate};
    use super::testkit::*;
    use super::*;

    #[test]
    fn verifies_design_sample_through_both_levels() {
        let bytes = signed_manifest(&manifest_with_serial(1042));
        let verified = trust().verify_access(&bytes, NOW_MS).unwrap();
        assert_eq!(verified.serial, 1042);
        assert_eq!(verified.key_id, ONLINE_ID);
        assert!(!verified.is_stale(NOW_MS));
        assert!(verified.is_stale(1_796_371_200_000));
        assert!(verified.digest.starts_with("sha256:"));
    }

    #[test]
    fn expired_manifest_is_stale_or_rejected_by_mode() {
        let bytes = signed_manifest(&manifest_with_serial(1));
        let later = 1_796_371_200_000 + 1;
        // 证书 notAfter 为 12-28，12-04 之后清单过期但证书仍有效。
        assert!(
            trust()
                .verify_access(&bytes, later)
                .unwrap()
                .is_stale(later)
        );
        assert_eq!(
            trust()
                .verify_access_with(&bytes, later, ExpiryMode::Reject)
                .unwrap_err(),
            TrustError::Expired {
                expires_at_ms: 1_796_371_200_000
            }
        );
    }

    #[test]
    fn tampered_payload_fails_signature() {
        let bytes = signed_manifest(&manifest_with_serial(7));
        let mut envelope = Envelope::parse(&bytes).unwrap();
        let mut payload = decode_b64(&envelope.payload).unwrap();
        let position = payload.windows(4).position(|w| w == b"im-c").unwrap();
        payload[position] = b'x';
        envelope.payload = encode_b64(&payload);
        let error = trust()
            .verify_access(&envelope.to_json_bytes(), NOW_MS)
            .unwrap_err();
        assert_eq!(
            error,
            TrustError::BadSignature {
                key_id: ONLINE_ID.into()
            }
        );
        assert_eq!(error.kind(), TrustFailureKind::Tampered);
    }

    #[test]
    fn keystate_revocation_rejects_online_key() {
        let root = root_key();
        let keystate = KeyState {
            schema: "flare.keystate.v1".into(),
            serial: 3,
            issued_at: "2026-11-01T00:00:00Z".into(),
            certificates: vec![],
            revoked_key_ids: vec![ONLINE_ID.into()],
        };
        let ks_bytes = sign_keystate(
            &keystate,
            &Signer {
                key_id: ROOT_ID,
                key: &root,
            },
        );
        let base = trust();
        let verified = base.verify_keystate(&ks_bytes, NOW_MS).unwrap();
        assert_eq!(verified.serial, 3);
        let revoked = base.with_keystate(&verified);
        let bytes = signed_manifest(&manifest_with_serial(9));
        let error = revoked.verify_access(&bytes, NOW_MS).unwrap_err();
        assert_eq!(
            error,
            TrustError::Revoked {
                key_id: ONLINE_ID.into()
            }
        );
        // 更旧的密钥状态不能把吊销列表倒退。
        let older = KeyState {
            serial: 2,
            revoked_key_ids: vec![],
            ..keystate
        };
        let older = base
            .verify_keystate(
                &sign_keystate(
                    &older,
                    &Signer {
                        key_id: ROOT_ID,
                        key: &root,
                    },
                ),
                NOW_MS,
            )
            .unwrap();
        assert!(
            revoked
                .with_keystate(&older)
                .verify_access(&bytes, NOW_MS)
                .is_err()
        );
    }

    #[test]
    fn keystate_signed_by_online_key_is_rejected() {
        let online = online_key();
        let keystate = KeyState {
            schema: "flare.keystate.v1".into(),
            serial: 1,
            issued_at: "2026-11-01T00:00:00Z".into(),
            certificates: vec![],
            revoked_key_ids: vec![],
        };
        let bytes = sign_keystate(
            &keystate,
            &Signer {
                key_id: ONLINE_ID,
                key: &online,
            },
        );
        assert_eq!(
            trust().verify_keystate(&bytes, NOW_MS).unwrap_err(),
            TrustError::UnknownKey {
                key_id: ONLINE_ID.into()
            }
        );
    }

    #[test]
    fn key_cert_validate_stops_what_should_not_be_signed() {
        assert_eq!(cert().validate(), Ok(()));
        type Mutate = fn(&mut KeyCert);
        let cases: [(&str, Mutate); 8] = [
            ("keyid", |c| c.key_id = "ok/1".into()),
            ("publicKey", |c| c.public_key = "AQ".into()),
            ("usages", |c| c.usages = vec![]),
            ("usages", |c| c.usages = vec!["keys".into()]),
            ("usages", |c| {
                c.usages = vec!["access".into(), "access".into()]
            }),
            ("scope.bootstrapIds", |c| c.scope.bootstrap_ids = vec![]),
            ("notAfter", |c| c.not_after = c.not_before.clone()),
            // 180 天 + 1 秒。
            ("notAfter", |c| c.not_after = "2026-12-28T00:00:01Z".into()),
        ];
        for (field, mutate) in cases {
            let mut candidate = cert();
            mutate(&mut candidate);
            match candidate.validate() {
                Err(crate::manifest::ManifestError::Invalid { field: actual, .. }) => {
                    assert_eq!(actual, field);
                }
                other => panic!("{field}: {other:?}"),
            }
        }
    }

    #[test]
    fn verify_key_cert_checks_the_root_but_not_the_validity_window() {
        let mut future = cert();
        future.not_before = "2030-01-01T00:00:00Z".into();
        future.not_after = "2030-06-01T00:00:00Z".into();
        let encoded = issued_cert(&future);
        assert_eq!(verify_key_cert(&encoded, &roots()).unwrap(), future);

        let other_root = sign::SigningKey::from_bytes(&[0x55; 32]);
        let wrong =
            TrustRoots::from_keys([(ROOT_ID.to_owned(), other_root.verifying_key().to_bytes())])
                .unwrap();
        assert_eq!(
            verify_key_cert(&encoded, &wrong),
            Err(KeyCertError::BadRootSignature)
        );
        let renamed =
            TrustRoots::from_keys([("root-z".to_owned(), root_key().verifying_key().to_bytes())])
                .unwrap();
        assert_eq!(
            verify_key_cert(&encoded, &renamed),
            Err(KeyCertError::UnknownRoot)
        );
        assert_eq!(
            verify_key_cert("not-a-cert", &roots()),
            Err(KeyCertError::Malformed)
        );
    }

    #[test]
    fn key_state_validate_covers_the_client_limits() {
        let state = KeyState::new(
            1,
            "2026-11-01T00:00:00Z",
            vec![],
            vec![ONLINE_ID.to_owned()],
        );
        assert_eq!(state.validate(), Ok(()));
        let mut bad = state.clone();
        bad.issued_at = "yesterday".into();
        assert!(bad.validate().is_err());
        let mut bad = state.clone();
        bad.revoked_key_ids = vec!["ok/1".into()];
        assert!(bad.validate().is_err());
        let mut bad = state.clone();
        bad.revoked_key_ids = vec![ONLINE_ID.to_owned(); MAX_REVOKED_KEY_IDS + 1];
        assert!(bad.validate().is_err());
    }

    #[test]
    fn serial_check_orders() {
        assert_eq!(check_serial(5, None), Ok(SerialCheck::Newer));
        assert_eq!(check_serial(5, Some(4)), Ok(SerialCheck::Newer));
        assert_eq!(check_serial(5, Some(5)), Ok(SerialCheck::Same));
        assert_eq!(
            check_serial(4, Some(5)),
            Err(TrustError::Rollback {
                serial: 4,
                max_serial: 5
            })
        );
    }
}
