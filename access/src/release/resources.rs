//! 资源包（§8.12）：选包、校验与本地存取。
//!
//! 包只是数据：这里只保证字节是清单签过的那份，内容由已审核版本里的 Kit 解释，不加载包里的代码。
//!
//! 选包结果是唯一真相：本地和它比 `sha256`，相同保留，不同就下载替换（版本更低也换），结果里没有的
//! `id` 删掉。发布者从清单里删掉坏版本、留下旧版本时靠这条撤回；防降级靠发布清单的签名和 serial。
//!
//! 新下载和新撤回都是下次启动才生效：本次运行的快照由引擎在第一次写入之前读定。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::evaluate::bucket;
use crate::bootstrap::{FetchRequest, HttpFetch};
use crate::manifest::{ReleaseEntry, ReleaseManifest, ResourcePack};
use crate::store::{AccessStore, StoreError, StoreOp, keys};

/// 包文件的 `schema`。
pub const RESOURCE_SCHEMA: &str = "flare.resource.v1";
/// 单包上限。
pub const MAX_RESOURCE_PACK_BYTES: u64 = 4 * 1024 * 1024;
/// 单个地址的超时：比清单长，4 MiB 在弱网上要一阵子。
pub const RESOURCE_FETCH_TIMEOUT_MS: u32 = 30_000;

/// 这个版本的 Kit 解释得了的类型。新版控制面可以加类型，旧客户端不下载。
const KNOWN_KINDS: [&str; 4] = ["strings", "tokens", "emoji", "help"];

/// 选包要的本机信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackInput {
    /// 当前构建号。Web 可能没有，这时只有没要求 `minBuild` 的包可用。
    pub current_build: Option<u64>,
    pub install_id: String,
}

/// 不知道当前构建时证明不了 `minBuild` 满足，只放行没要求的包。
fn build_allows(min_build: u64, current_build: Option<u64>) -> bool {
    min_build == 0 || current_build.is_some_and(|build| build >= min_build)
}

fn release_reaches(release: &ReleaseEntry, install_id: &str) -> bool {
    !release.rollout.halted
        && bucket(install_id, &release.rollout.salt) < release.rollout.basis_points
}

/// 选包：跳过暂停和分桶未命中的版本，跳过 `minBuild` 不满足的包，同 `id` 取最高 `packVersion`，
/// 版本相同取清单里靠前的。结果按 `id` 排序。
pub fn select_packs<'a>(manifest: &'a ReleaseManifest, input: &PackInput) -> Vec<&'a ResourcePack> {
    let mut selected: BTreeMap<&str, &ResourcePack> = BTreeMap::new();
    let reachable = manifest
        .releases
        .iter()
        .filter(|release| release_reaches(release, &input.install_id));
    for release in reachable {
        for pack in &release.resource_packs {
            if !KNOWN_KINDS.contains(&pack.kind.as_str())
                || !build_allows(pack.min_build, input.current_build)
            {
                continue;
            }
            let newer = selected
                .get(pack.id.as_str())
                .is_none_or(|held| pack.pack_version > held.pack_version);
            if newer {
                selected.insert(&pack.id, pack);
            }
        }
    }
    selected.into_values().collect()
}

/// 本地已存的包，同步时只看这两项。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HeldPack {
    pub id: String,
    pub sha256: String,
}

/// 这一轮要做的事。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackPlan<'a> {
    /// 本地没有或和选包结果不同的包，按 `id` 排序。
    pub fetch: Vec<&'a ResourcePack>,
    /// 不在选包结果里的已存包的 `id`，排好序。
    pub remove: Vec<String>,
}

/// 把本地和选包结果对齐。
pub fn plan_packs<'a>(selected: &[&'a ResourcePack], held: &[HeldPack]) -> PackPlan<'a> {
    let current = |pack: &ResourcePack| {
        held.iter()
            .any(|stored| stored.id == pack.id && stored.sha256 == pack.sha256)
    };
    let fetch = selected
        .iter()
        .copied()
        .filter(|pack| !current(pack))
        .collect();
    let mut remove: Vec<String> = held
        .iter()
        .filter(|stored| !selected.iter().any(|pack| pack.id == stored.id))
        .map(|stored| stored.id.clone())
        .collect();
    remove.sort();
    PackPlan { fetch, remove }
}

/// 包文件为什么不收。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PackError {
    #[error("the pack exceeds the size limit")]
    TooLarge,
    #[error("the pack size differs from the manifest")]
    SizeMismatch,
    #[error("the pack hash differs from the manifest")]
    HashMismatch,
    #[error("the pack is not JSON")]
    Malformed,
    #[error("the pack schema is not flare.resource.v1")]
    WrongSchema,
    #[error("the pack kind differs from the manifest")]
    WrongKind,
}

impl PackError {
    /// 跨端稳定的错误码（向量与日志使用）。
    pub fn code(self) -> &'static str {
        match self {
            PackError::TooLarge => "tooLarge",
            PackError::SizeMismatch => "sizeMismatch",
            PackError::HashMismatch => "hashMismatch",
            PackError::Malformed => "malformed",
            PackError::WrongSchema => "wrongSchema",
            PackError::WrongKind => "wrongKind",
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 校验下载到的文件，返回它的原文。
pub fn verify_pack(pack: &ResourcePack, body: &[u8]) -> Result<String, PackError> {
    if body.len() as u64 > MAX_RESOURCE_PACK_BYTES {
        return Err(PackError::TooLarge);
    }
    if body.len() as u64 != pack.size_bytes {
        return Err(PackError::SizeMismatch);
    }
    if sha256_hex(body) != pack.sha256 {
        return Err(PackError::HashMismatch);
    }
    let content = std::str::from_utf8(body).map_err(|_| PackError::Malformed)?;
    let document: Value = serde_json::from_str(content).map_err(|_| PackError::Malformed)?;
    if document.get("schema").and_then(Value::as_str) != Some(RESOURCE_SCHEMA) {
        return Err(PackError::WrongSchema);
    }
    if document.get("kind").and_then(Value::as_str) != Some(pack.kind.as_str()) {
        return Err(PackError::WrongKind);
    }
    Ok(content.to_owned())
}

/// 存在 AccessStore 里的一个包。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredPack {
    pub id: String,
    pub kind: String,
    pub pack_version: u64,
    pub min_build: u64,
    pub sha256: String,
    /// 包文件原文。哈希算在这些字节上。
    pub content: String,
}

impl StoredPack {
    fn new(pack: &ResourcePack, content: String) -> Self {
        Self {
            id: pack.id.clone(),
            kind: pack.kind.clone(),
            pack_version: pack.pack_version,
            min_build: pack.min_build,
            sha256: pack.sha256.clone(),
            content,
        }
    }

    fn usable(&self, current_build: Option<u64>) -> bool {
        sha256_hex(self.content.as_bytes()) == self.sha256
            && build_allows(self.min_build, current_build)
    }
}

/// 启动时读一次：解不开、哈希不符、`minBuild` 不满足的删掉，其余按 `id` 排序，形成本次运行的快照。
pub async fn load_packs(
    store: &dyn AccessStore,
    bootstrap_id: &str,
    current_build: Option<u64>,
) -> Result<Vec<StoredPack>, StoreError> {
    let mut packs = Vec::new();
    let mut spoiled = Vec::new();
    let entries = store
        .scan_prefix(&keys::resource_prefix(bootstrap_id))
        .await?;
    for (key, bytes) in entries {
        let pack = serde_json::from_slice::<StoredPack>(&bytes)
            .ok()
            .filter(|pack| key == keys::resource_pack(bootstrap_id, &pack.id))
            .filter(|pack| pack.usable(current_build));
        match pack {
            Some(pack) => packs.push(pack),
            None => spoiled.push(StoreOp::Delete { key }),
        }
    }
    if !spoiled.is_empty() {
        // 删不掉也不影响这次的快照，下次启动再删。
        let _ = store.apply(&spoiled).await;
    }
    packs.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(packs)
}

/// 一轮同步做了什么，给引擎记日志。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackSyncReport {
    pub fetched: Vec<String>,
    pub removed: Vec<String>,
    /// 计费网络上没下的包，下次检查再试。
    pub postponed: Vec<String>,
    /// 失败的包和原因，下次检查重试。
    pub failed: Vec<(String, String)>,
}

/// 按地址顺序故障转移，第一个校验通过的算数。失败时给最后一个地址的原因。
async fn fetch_pack(http: &dyn HttpFetch, pack: &ResourcePack) -> Result<String, String> {
    if pack.size_bytes > MAX_RESOURCE_PACK_BYTES {
        return Err(PackError::TooLarge.code().to_owned());
    }
    let mut failure = String::new();
    for url in &pack.url {
        let request = FetchRequest {
            url: url.clone(),
            timeout_ms: RESOURCE_FETCH_TIMEOUT_MS,
            max_body_bytes: pack.size_bytes as usize,
            ..FetchRequest::default()
        };
        failure = match http.fetch(&request).await {
            Ok(response) if response.status == 200 => match verify_pack(pack, &response.body) {
                Ok(content) => return Ok(content),
                Err(error) => error.code().to_owned(),
            },
            Ok(response) => format!("http.{}", response.status),
            Err(error) => error.code().to_owned(),
        };
    }
    Err(failure)
}

/// 同步一轮：先删撤回的，再下要换的。一个包失败不影响其它包；计费网络只删不下。
/// 替换在下载成功后才发生，下载失败时旧包留到下次检查。
pub async fn sync_packs(
    store: &dyn AccessStore,
    http: &dyn HttpFetch,
    bootstrap_id: &str,
    manifest: &ReleaseManifest,
    input: &PackInput,
    metered: bool,
) -> Result<PackSyncReport, StoreError> {
    let held: Vec<HeldPack> = store
        .scan_prefix(&keys::resource_prefix(bootstrap_id))
        .await?
        .into_iter()
        .filter_map(|(_, bytes)| serde_json::from_slice(&bytes).ok())
        .collect();
    let selected = select_packs(manifest, input);
    let plan = plan_packs(&selected, &held);
    let mut report = PackSyncReport::default();
    for id in plan.remove {
        match store.delete(&keys::resource_pack(bootstrap_id, &id)).await {
            Ok(()) => report.removed.push(id),
            Err(error) => report.failed.push((id, error.to_string())),
        }
    }
    for pack in plan.fetch {
        if metered {
            report.postponed.push(pack.id.clone());
            continue;
        }
        let saved = match fetch_pack(http, pack).await {
            Ok(content) => save_pack(store, bootstrap_id, pack, content).await,
            Err(code) => Err(code),
        };
        match saved {
            Ok(()) => report.fetched.push(pack.id.clone()),
            Err(code) => report.failed.push((pack.id.clone(), code)),
        }
    }
    Ok(report)
}

async fn save_pack(
    store: &dyn AccessStore,
    bootstrap_id: &str,
    pack: &ResourcePack,
    content: String,
) -> Result<(), String> {
    let bytes =
        serde_json::to_vec(&StoredPack::new(pack, content)).map_err(|error| error.to_string())?;
    store
        .put(&keys::resource_pack(bootstrap_id, &pack.id), &bytes)
        .await
        .map_err(|error| error.to_string())
}

/// `update.get_resources` 里的一个包。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResourcePack {
    pub id: String,
    pub kind: String,
    pub pack_version: u64,
    /// 包文件原文。以字符串穿过绑定，由 Kit 和宿主解析。
    pub content_json: String,
}

impl From<StoredPack> for UpdateResourcePack {
    fn from(pack: StoredPack) -> Self {
        Self {
            id: pack.id,
            kind: pack.kind,
            pack_version: pack.pack_version,
            content_json: pack.content,
        }
    }
}

/// `update.get_resources` 的返回值，按 `id` 排序。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UpdateResources {
    pub packs: Vec<UpdateResourcePack>,
}

#[cfg(test)]
#[path = "resources_tests.rs"]
mod tests;
