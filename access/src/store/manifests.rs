//! 最后可信清单（LKG）与 serial 防回滚。
//!
//! 这里只做持久化与 serial 判定，不做验签：调用方先用 `TrustStore` 验签，再把结果交给这里提交。
//! 读回的副本同样要重新验签（防止本地被篡改的副本绕过信任检查），所以读接口只返回原始信封。
//!
//! LKG 只会被 serial 更大的已验签文档替换，永远不会被种子、未签名数据或更旧的版本覆盖。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::{AccessStore, StoreError, StoreOp, keys};
use crate::manifest::DocType;
use crate::trust::{
    SerialCheck, TrustError, VerifiedAccessManifest, VerifiedKeyState, VerifiedReleaseManifest,
    check_serial, decode_b64, encode_b64,
};

/// 文档从哪里来。写进 LKG 记录，供诊断与「上次成功的源排前」使用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ManifestSource {
    /// 构建期内置种子。
    Seed,
    /// 本地 LKG 读回。
    Lkg,
    /// `bootstrap.sources` 中的 https 镜像。
    Https { url: String },
    /// 接入点上的 `/.well-known/flare/v1/...`。
    WellKnown {
        #[serde(rename = "apId")]
        ap_id: String,
    },
    /// 人工导入的恢复清单。
    Recovery,
}

impl ManifestSource {
    pub fn label(&self) -> String {
        match self {
            ManifestSource::Seed => "seed".into(),
            ManifestSource::Lkg => "lkg".into(),
            ManifestSource::Https { url } => format!("https:{url}"),
            ManifestSource::WellKnown { ap_id } => format!("wellKnown:{ap_id}"),
            ManifestSource::Recovery => "recovery".into(),
        }
    }
}

/// 拉取元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchMeta {
    pub source: ManifestSource,
    /// 条件请求用的 ETag。
    pub etag: Option<String>,
    pub fetched_at_ms: i64,
}

impl FetchMeta {
    pub fn new(source: ManifestSource, fetched_at_ms: i64) -> Self {
        Self {
            source,
            etag: None,
            fetched_at_ms,
        }
    }
}

/// LKG 记录：已验签信封原始字节 + ETag + 来源 + 拉取时间。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredDocument {
    /// base64url(信封原始字节)。
    pub envelope: String,
    pub serial: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    pub source: ManifestSource,
    pub fetched_at_ms: i64,
}

impl StoredDocument {
    pub fn envelope_bytes(&self) -> Result<Vec<u8>, StoreError> {
        decode_b64(&self.envelope)
            .ok_or_else(|| StoreError::Corrupt("envelope is not base64".into()))
    }
}

/// 提交结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// 新文档已写入 LKG。
    Adopted {
        serial: u64,
        previous_serial: Option<u64>,
    },
    /// 与已存 serial 相同，视为同一份文档。
    Unchanged { serial: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AcceptError {
    #[error(transparent)]
    Trust(#[from] TrustError),
    #[error(transparent)]
    Store(#[from] StoreError),
    /// 开发模式的端点由配置决定，不接受网络下发的清单。
    #[error("manifests are not accepted in development mode")]
    DevelopmentMode,
}

pub struct ManifestStore {
    store: Arc<dyn AccessStore>,
    bootstrap_id: String,
    rootset: String,
    /// 发布清单的 `{platform}/{channel}`：每个渠道一个 [`ManifestStore`]，serial 互不影响。
    release_target: String,
    /// 串行化提交：两份清单并发验签通过时，后写入的必须是 serial 更大的那份，否则磁盘会回退。
    gate: futures_util::lock::Mutex<()>,
    /// 已读入的 maxSerial 缓存（键不存在 = 尚未读过）。
    max_cache: Mutex<HashMap<DocType, Option<u64>>>,
}

impl ManifestStore {
    pub fn new(
        store: Arc<dyn AccessStore>,
        bootstrap_id: impl Into<String>,
        rootset: impl Into<String>,
    ) -> Self {
        Self {
            store,
            bootstrap_id: bootstrap_id.into(),
            rootset: rootset.into(),
            release_target: String::new(),
            gate: futures_util::lock::Mutex::new(()),
            max_cache: Mutex::new(HashMap::new()),
        }
    }

    /// 这一份存储管哪个平台渠道的发布清单。
    pub fn for_release(mut self, platform: &str, channel: &str) -> Self {
        self.release_target = format!("{platform}/{channel}");
        self
    }

    fn keys_for(&self, doc: DocType) -> (String, Option<String>, String) {
        match doc {
            DocType::KeyState => (
                keys::keystate_current(&self.rootset),
                None,
                keys::keystate_serial(&self.rootset),
            ),
            DocType::Access => (
                keys::access_current(&self.bootstrap_id),
                Some(keys::access_previous(&self.bootstrap_id)),
                keys::access_serial(&self.bootstrap_id),
            ),
            DocType::Release => (
                keys::release_current(&self.bootstrap_id, &self.release_target),
                None,
                keys::release_serial(&self.bootstrap_id, &self.release_target),
            ),
        }
    }

    fn cached_max(&self, doc: DocType) -> Option<Option<u64>> {
        self.max_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&doc)
            .copied()
    }

    fn set_cached_max(&self, doc: DocType, value: Option<u64>) {
        self.max_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(doc, value);
    }

    /// 某类文档已接受过的最大 serial。serial 键损坏时退回 LKG 记录里的 serial，
    /// 两者都没有才视为从未接受过。
    pub async fn max_serial(&self, doc: DocType) -> Result<Option<u64>, StoreError> {
        if let Some(cached) = self.cached_max(doc) {
            return Ok(cached);
        }
        let (current_key, _, serial_key) = self.keys_for(doc);
        let from_serial = self
            .store
            .get(&serial_key)
            .await?
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|text| text.trim().parse::<u64>().ok());
        let value = match from_serial {
            Some(serial) => Some(serial),
            None => self
                .store
                .get(&current_key)
                .await?
                .and_then(|bytes| serde_json::from_slice::<StoredDocument>(&bytes).ok())
                .map(|record| record.serial),
        };
        self.set_cached_max(doc, value);
        Ok(value)
    }

    /// 读 LKG 接入清单记录（未验签，调用方必须重新验签）。
    pub async fn load_access(&self) -> Result<Option<StoredDocument>, StoreError> {
        self.load(DocType::Access).await
    }

    /// 读密钥状态记录（未验签）。
    pub async fn load_keystate(&self) -> Result<Option<StoredDocument>, StoreError> {
        self.load(DocType::KeyState).await
    }

    /// 读发布清单记录（未验签）。
    pub async fn load_release(&self) -> Result<Option<StoredDocument>, StoreError> {
        self.load(DocType::Release).await
    }

    async fn load(&self, doc: DocType) -> Result<Option<StoredDocument>, StoreError> {
        let (current_key, _, _) = self.keys_for(doc);
        match self.store.get(&current_key).await? {
            None => Ok(None),
            Some(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| StoreError::Corrupt(format!("{current_key}: {error}"))),
        }
    }

    /// 提交一份已验签的接入清单：防回滚检查后，清单、上一份副本与 maxSerial 一次原子写入。
    pub async fn commit_access(
        &self,
        verified: &VerifiedAccessManifest,
        meta: &FetchMeta,
    ) -> Result<AcceptOutcome, AcceptError> {
        self.commit(DocType::Access, verified.serial, &verified.envelope, meta)
            .await
    }

    pub async fn commit_keystate(
        &self,
        verified: &VerifiedKeyState,
        meta: &FetchMeta,
    ) -> Result<AcceptOutcome, AcceptError> {
        self.commit(DocType::KeyState, verified.serial, &verified.envelope, meta)
            .await
    }

    /// 提交一份已验签的发布清单，防回滚规则与接入清单相同。
    pub async fn commit_release(
        &self,
        verified: &VerifiedReleaseManifest,
        meta: &FetchMeta,
    ) -> Result<AcceptOutcome, AcceptError> {
        self.commit(DocType::Release, verified.serial, &verified.envelope, meta)
            .await
    }

    async fn commit(
        &self,
        doc: DocType,
        serial: u64,
        envelope: &[u8],
        meta: &FetchMeta,
    ) -> Result<AcceptOutcome, AcceptError> {
        let _guard = self.gate.lock().await;
        let max = self.max_serial(doc).await?;
        let check = check_serial(serial, max)?;
        let (current_key, previous_key, serial_key) = self.keys_for(doc);
        let current_bytes = self.store.get(&current_key).await?;
        let current_serial = current_bytes
            .as_deref()
            .and_then(|bytes| serde_json::from_slice::<StoredDocument>(bytes).ok())
            .map(|record| record.serial);
        if check == SerialCheck::Same && current_serial == Some(serial) {
            return Ok(AcceptOutcome::Unchanged { serial });
        }

        let record = StoredDocument {
            envelope: encode_b64(envelope),
            serial,
            etag: meta.etag.clone(),
            source: meta.source.clone(),
            fetched_at_ms: meta.fetched_at_ms,
        };
        let record_bytes = serde_json::to_vec(&record)
            .map_err(|error| StoreError::Backend(format!("encode record: {error}")))?;
        let mut ops = Vec::with_capacity(3);
        if let (Some(previous_key), Some(bytes)) = (previous_key, current_bytes)
            && current_serial != Some(serial)
        {
            ops.push(StoreOp::Put {
                key: previous_key,
                value: bytes,
            });
        }
        ops.push(StoreOp::Put {
            key: current_key,
            value: record_bytes,
        });
        ops.push(StoreOp::Put {
            key: serial_key,
            value: serial.to_string().into_bytes(),
        });
        self.store.apply(&ops).await?;
        self.set_cached_max(doc, Some(max.map_or(serial, |max| max.max(serial))));
        Ok(match check {
            // serial 相同但 LKG 记录缺失或损坏：已就地修复，对调用方仍是同一份文档。
            SerialCheck::Same => AcceptOutcome::Unchanged { serial },
            SerialCheck::Newer => AcceptOutcome::Adopted {
                serial,
                previous_serial: max,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryAccessStore;
    use crate::trust::testkit::*;

    fn verified(serial: u64) -> VerifiedAccessManifest {
        trust()
            .verify_access(&signed_manifest(&manifest_with_serial(serial)), NOW_MS)
            .unwrap()
    }

    #[tokio::test]
    async fn adopts_newer_rejects_older_and_keeps_previous() {
        let memory = Arc::new(MemoryAccessStore::new());
        let store = ManifestStore::new(memory.clone(), BOOTSTRAP_ID, "rootset");
        let meta = FetchMeta::new(ManifestSource::Seed, NOW_MS);
        assert_eq!(
            store.commit_access(&verified(10), &meta).await.unwrap(),
            AcceptOutcome::Adopted {
                serial: 10,
                previous_serial: None
            }
        );
        assert_eq!(
            store.commit_access(&verified(10), &meta).await.unwrap(),
            AcceptOutcome::Unchanged { serial: 10 }
        );
        let https = FetchMeta::new(
            ManifestSource::Https {
                url: "https://m.example.com/".into(),
            },
            NOW_MS + 1,
        );
        assert_eq!(
            store.commit_access(&verified(12), &https).await.unwrap(),
            AcceptOutcome::Adopted {
                serial: 12,
                previous_serial: Some(10)
            }
        );
        assert_eq!(
            store.commit_access(&verified(11), &meta).await.unwrap_err(),
            AcceptError::Trust(TrustError::Rollback {
                serial: 11,
                max_serial: 12
            })
        );
        let current = store.load_access().await.unwrap().unwrap();
        assert_eq!(current.serial, 12);
        assert_eq!(current.source, https.source);
        let previous: StoredDocument = serde_json::from_slice(
            &memory
                .get(&keys::access_previous(BOOTSTRAP_ID))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(previous.serial, 10);

        // 新实例从磁盘读 maxSerial，同样拒绝回退。
        let reopened = ManifestStore::new(memory.clone(), BOOTSTRAP_ID, "rootset");
        assert_eq!(
            reopened.max_serial(DocType::Access).await.unwrap(),
            Some(12)
        );
        assert!(reopened.commit_access(&verified(5), &meta).await.is_err());
    }

    #[tokio::test]
    async fn same_serial_repairs_missing_record() {
        let memory = Arc::new(MemoryAccessStore::new());
        memory
            .put(&keys::access_serial(BOOTSTRAP_ID), b"10")
            .await
            .unwrap();
        let store = ManifestStore::new(memory.clone(), BOOTSTRAP_ID, "rootset");
        let meta = FetchMeta::new(ManifestSource::Seed, NOW_MS);
        assert_eq!(
            store.commit_access(&verified(10), &meta).await.unwrap(),
            AcceptOutcome::Unchanged { serial: 10 }
        );
        assert_eq!(store.load_access().await.unwrap().unwrap().serial, 10);
    }

    fn verified_release(serial: u64, channel: &str) -> VerifiedReleaseManifest {
        let payload = serde_json::json!({
            "schema": "flare.release.v1",
            "bootstrapId": BOOTSTRAP_ID,
            "platform": "android",
            "channel": channel,
            "serial": serial,
            "issuedAt": "2026-11-04T08:00:00Z",
            "expiresAt": "2026-11-18T08:00:00Z",
            "policy": {},
            "releases": []
        });
        trust()
            .verify_release(&signed_release(&payload), "android", channel, NOW_MS)
            .unwrap()
    }

    #[tokio::test]
    async fn release_serials_stay_apart_per_channel_and_from_the_access_manifest() {
        let memory = Arc::new(MemoryAccessStore::new());
        let meta = FetchMeta::new(ManifestSource::Seed, NOW_MS);
        let stable = ManifestStore::new(memory.clone(), BOOTSTRAP_ID, "rootset")
            .for_release("android", "stable");
        let beta = ManifestStore::new(memory.clone(), BOOTSTRAP_ID, "rootset")
            .for_release("android", "beta");
        assert_eq!(
            stable
                .commit_release(&verified_release(88, "stable"), &meta)
                .await
                .unwrap(),
            AcceptOutcome::Adopted {
                serial: 88,
                previous_serial: None
            }
        );
        assert_eq!(
            beta.commit_release(&verified_release(3, "beta"), &meta)
                .await
                .unwrap(),
            AcceptOutcome::Adopted {
                serial: 3,
                previous_serial: None
            }
        );
        assert_eq!(
            stable
                .commit_release(&verified_release(87, "stable"), &meta)
                .await
                .unwrap_err(),
            AcceptError::Trust(TrustError::Rollback {
                serial: 87,
                max_serial: 88
            })
        );
        assert_eq!(stable.load_release().await.unwrap().unwrap().serial, 88);

        // 接入清单的 serial 不受发布清单影响。
        let access = ManifestStore::new(memory, BOOTSTRAP_ID, "rootset");
        assert_eq!(access.max_serial(DocType::Access).await.unwrap(), None);
        assert_eq!(
            access.commit_access(&verified(10), &meta).await.unwrap(),
            AcceptOutcome::Adopted {
                serial: 10,
                previous_serial: None
            }
        );
    }

    #[tokio::test]
    async fn failed_write_does_not_advance_serial() {
        let memory = Arc::new(MemoryAccessStore::new());
        let store = ManifestStore::new(memory.clone(), BOOTSTRAP_ID, "rootset");
        memory.set_fail_writes(true);
        let meta = FetchMeta::new(ManifestSource::Seed, NOW_MS);
        assert!(matches!(
            store.commit_access(&verified(10), &meta).await,
            Err(AcceptError::Store(_))
        ));
        assert_eq!(store.max_serial(DocType::Access).await.unwrap(), None);
    }
}
