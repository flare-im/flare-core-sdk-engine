//! 端点目录：把已验证清单展开成「服务 → 归属 Cell 的候选接入点」不可变快照，`ArcSwap` 原子替换。
//!
//! 读路径（选路、HTTP 取 base URL）只做一次原子加载，不持锁；
//! 清单更新时整体换一份新快照，正在使用旧快照的调用方不受影响。

use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::manifest::schema::ACCESS_SCHEMA;
use crate::manifest::{AccessManifest, BootstrapSection, BootstrapSource, ClientPolicy, Placement};
use crate::store::ManifestSource;
use crate::trust::VerifiedAccessManifest;
use crate::types::{ApId, ApStatus, ServiceKind};

/// 快照的来源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestOrigin {
    /// 验签通过的清单。
    Signed {
        key_id: String,
        source: ManifestSource,
        digest: String,
    },
    /// 开发模式由配置端点直接构造，未签名。上层应持续发出警告并显示「开发模式」角标。
    Development,
    /// 还没有任何可信清单（LKG 与种子都不可用）：目录为空，等引导拉取或恢复链接。
    Unavailable,
}

/// 归属 Cell 中的一个接入点，服务入口已展开为完整 URL。
#[derive(Debug, Clone, PartialEq)]
pub struct AccessPointEntry {
    pub id: ApId,
    pub cell_id: Arc<str>,
    pub region_id: Arc<str>,
    pub host: String,
    /// 仅原生端使用：DNS 失败或结果可疑时直连，TLS SNI 与证书校验仍用 `host`。
    pub ip_hints: Vec<IpAddr>,
    pub spki_pins: Vec<String>,
    pub weight: u32,
    pub priority: u32,
    pub status: ApStatus,
    pub tags: Vec<String>,
    pub endpoints: ServiceEndpoints,
}

/// 各服务的完整入口。生产清单按约定拼接：`wss://{host}{path}`、`quic://{host}:{port}`、
/// `https://{host}{path}`；开发模式直接使用配置给出的 URL。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceEndpoints {
    urls: [Option<String>; ServiceKind::COUNT],
}

impl ServiceEndpoints {
    pub fn get(&self, kind: ServiceKind) -> Option<&str> {
        self.urls[kind.index()].as_deref()
    }

    pub fn has(&self, kind: ServiceKind) -> bool {
        self.urls[kind.index()].is_some()
    }

    pub(crate) fn set(&mut self, kind: ServiceKind, url: Option<String>) {
        self.urls[kind.index()] = url;
    }
}

impl AccessPointEntry {
    pub fn endpoint(&self, kind: ServiceKind) -> Option<&str> {
        self.endpoints.get(kind)
    }

    pub fn is_active(&self) -> bool {
        self.status == ApStatus::Active
    }
}

#[derive(Debug, Clone)]
pub struct DirectorySnapshot {
    origin: ManifestOrigin,
    manifest: Arc<AccessManifest>,
    serial: u64,
    issued_at_ms: i64,
    expires_at_ms: i64,
    home_cell: Arc<str>,
    home_region: Arc<str>,
    policy: Arc<ClientPolicy>,
    access_points: Vec<AccessPointEntry>,
}

impl DirectorySnapshot {
    /// 由验签通过的清单构造。只展开归属 Cell 的接入点：客户端不跨 Cell 切换。
    pub fn from_verified(verified: &VerifiedAccessManifest, source: ManifestSource) -> Self {
        let manifest = verified.manifest.clone();
        let (home_region, access_points) = expand_home_cell(&manifest);
        Self {
            origin: ManifestOrigin::Signed {
                key_id: verified.key_id.clone(),
                source,
                digest: verified.digest.clone(),
            },
            serial: verified.serial,
            issued_at_ms: verified.issued_at_ms,
            expires_at_ms: verified.expires_at_ms,
            home_cell: Arc::from(manifest.placement.home_cell.as_str()),
            home_region,
            policy: Arc::new(manifest.client_policy()),
            access_points,
            manifest,
        }
    }

    /// 开发模式：清单与端点由 `bootstrap::development` 生成。
    pub(crate) fn development(
        manifest: AccessManifest,
        issued_at_ms: i64,
        expires_at_ms: i64,
        access_points: Vec<AccessPointEntry>,
    ) -> Self {
        let manifest = Arc::new(manifest);
        let home_region = manifest
            .home_cell()
            .map(|(region, _)| Arc::from(region.id.as_str()))
            .unwrap_or_else(|| Arc::from(""));
        Self {
            origin: ManifestOrigin::Development,
            serial: manifest.serial,
            issued_at_ms,
            expires_at_ms,
            home_cell: Arc::from(manifest.placement.home_cell.as_str()),
            home_region,
            policy: Arc::new(manifest.client_policy()),
            access_points,
            manifest,
        }
    }

    /// 没有任何可信清单时的空目录。`sources` 是从验签失败的种子或 LKG 里读出的引导源：
    /// 它们只是「去哪儿拉」的线索，拉到的清单仍要完整验签，所以不要求可信。
    pub(crate) fn unavailable(bootstrap_id: &str, sources: Vec<BootstrapSource>) -> Self {
        let manifest = AccessManifest {
            schema: ACCESS_SCHEMA.to_owned(),
            bootstrap_id: bootstrap_id.to_owned(),
            app_id: String::new(),
            serial: 0,
            issued_at: String::new(),
            expires_at: String::new(),
            regions: Vec::new(),
            placement: Placement {
                home_cell: String::new(),
                allowed_regions: Vec::new(),
            },
            bootstrap: BootstrapSection {
                sources,
                refresh_interval_sec: None,
            },
            client_policy: serde_json::Value::Null,
            release: None,
        };
        Self {
            origin: ManifestOrigin::Unavailable,
            serial: 0,
            issued_at_ms: 0,
            expires_at_ms: 0,
            home_cell: Arc::from(""),
            home_region: Arc::from(""),
            policy: Arc::new(manifest.client_policy()),
            access_points: Vec::new(),
            manifest: Arc::new(manifest),
        }
    }

    pub fn origin(&self) -> &ManifestOrigin {
        &self.origin
    }

    /// 目录里有可信清单（开发模式也算）。
    pub fn has_manifest(&self) -> bool {
        !matches!(self.origin, ManifestOrigin::Unavailable)
    }

    pub fn is_development(&self) -> bool {
        matches!(self.origin, ManifestOrigin::Development)
    }

    pub fn manifest(&self) -> &Arc<AccessManifest> {
        &self.manifest
    }

    pub fn serial(&self) -> u64 {
        self.serial
    }

    pub fn issued_at_ms(&self) -> i64 {
        self.issued_at_ms
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }

    /// 过期（stale）：仍可用于连接，引擎应提高刷新频率。开发模式永不 stale。
    pub fn is_stale(&self, now_ms: i64) -> bool {
        !self.is_development() && now_ms >= self.expires_at_ms
    }

    pub fn home_cell(&self) -> &str {
        &self.home_cell
    }

    pub fn home_region(&self) -> &str {
        &self.home_region
    }

    pub fn policy(&self) -> &ClientPolicy {
        &self.policy
    }

    /// 归属 Cell 的全部接入点（含非 active），清单顺序。
    pub fn access_points(&self) -> &[AccessPointEntry] {
        &self.access_points
    }

    pub fn access_point(&self, id: &str) -> Option<(usize, &AccessPointEntry)> {
        self.access_points
            .iter()
            .enumerate()
            .find(|(_, entry)| &*entry.id == id)
    }

    /// 提供某个服务的接入点（含非 active，状态过滤由选路完成）。
    pub fn offering(&self, kind: ServiceKind) -> impl Iterator<Item = (usize, &AccessPointEntry)> {
        self.access_points
            .iter()
            .enumerate()
            .filter(move |(_, entry)| entry.endpoints.has(kind))
    }

    pub fn endpoint(&self, ap_index: usize, kind: ServiceKind) -> Option<&str> {
        self.access_points.get(ap_index)?.endpoint(kind)
    }
}

fn expand_home_cell(manifest: &AccessManifest) -> (Arc<str>, Vec<AccessPointEntry>) {
    let Some((region, cell)) = manifest.home_cell() else {
        // 结构校验保证归属 Cell 存在；这里防御性地返回空目录而不是 panic。
        return (Arc::from(""), Vec::new());
    };
    let region_id: Arc<str> = Arc::from(region.id.as_str());
    let cell_id: Arc<str> = Arc::from(cell.id.as_str());
    let entries = cell
        .access_points
        .iter()
        .map(|ap| {
            let mut endpoints = ServiceEndpoints::default();
            let services = &ap.services;
            let https = |path: Option<&str>| path.map(|path| format!("https://{}{path}", ap.host));
            endpoints.set(
                ServiceKind::ImWs,
                services
                    .path(ServiceKind::ImWs)
                    .map(|path| format!("wss://{}{path}", ap.host)),
            );
            endpoints.set(
                ServiceKind::ImQuic,
                services
                    .im_quic
                    .as_ref()
                    .map(|quic| format!("quic://{}:{}", ap.host, quic.port)),
            );
            for kind in [
                ServiceKind::CoreApi,
                ServiceKind::SocialApi,
                ServiceKind::Media,
                ServiceKind::Ping,
            ] {
                endpoints.set(kind, https(services.path(kind)));
            }
            AccessPointEntry {
                id: Arc::from(ap.id.as_str()),
                cell_id: cell_id.clone(),
                region_id: region_id.clone(),
                host: ap.host.clone(),
                ip_hints: ap
                    .ip_hints
                    .iter()
                    .filter_map(|hint| hint.parse().ok())
                    .collect(),
                spki_pins: ap.spki_pins.clone(),
                weight: ap.weight,
                priority: ap.priority,
                status: ap.status,
                tags: ap.tags.clone(),
                endpoints,
            }
        })
        .collect();
    (region_id, entries)
}

/// 两份快照之间的差异：供 `access://changed` 事件与「收到更高 serial 清单」的切换决策使用。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DirectoryChange {
    pub from_serial: u64,
    pub to_serial: u64,
    pub added: Vec<ApId>,
    pub removed: Vec<ApId>,
    /// 状态或服务入口变化的接入点（新状态）。当前在用的接入点变为 draining/disabled 时引擎按排空规则迁移。
    pub changed: Vec<(ApId, ApStatus)>,
    pub policy_changed: bool,
}

impl DirectoryChange {
    pub fn between(old: &DirectorySnapshot, new: &DirectorySnapshot) -> Self {
        let mut change = DirectoryChange {
            from_serial: old.serial,
            to_serial: new.serial,
            policy_changed: old.policy != new.policy,
            ..Default::default()
        };
        for entry in &new.access_points {
            match old.access_point(&entry.id) {
                None => change.added.push(entry.id.clone()),
                Some((_, previous)) if previous != entry => {
                    change.changed.push((entry.id.clone(), entry.status));
                }
                Some(_) => {}
            }
        }
        for entry in &old.access_points {
            if new.access_point(&entry.id).is_none() {
                change.removed.push(entry.id.clone());
            }
        }
        change
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.changed.is_empty()
            && !self.policy_changed
    }
}

/// 当前目录。
pub struct EndpointDirectory {
    current: ArcSwap<DirectorySnapshot>,
}

impl EndpointDirectory {
    pub fn new(snapshot: DirectorySnapshot) -> Self {
        Self {
            current: ArcSwap::from_pointee(snapshot),
        }
    }

    /// 当前快照（一次原子加载）。
    pub fn snapshot(&self) -> Arc<DirectorySnapshot> {
        self.current.load_full()
    }

    /// 原子替换并返回差异。
    pub fn replace(&self, snapshot: DirectorySnapshot) -> DirectoryChange {
        let next = Arc::new(snapshot);
        let previous = self.current.swap(next.clone());
        DirectoryChange::between(&previous, &next)
    }

    /// 仅当新快照 serial 更大时替换（CAS 循环）。两份清单并发验签提交时，
    /// 后到的旧清单不能把内存中的目录换回去。
    pub fn replace_if_newer(&self, snapshot: DirectorySnapshot) -> Option<DirectoryChange> {
        let next = Arc::new(snapshot);
        loop {
            let current = self.current.load_full();
            if next.serial <= current.serial && !current.is_development() && current.has_manifest()
            {
                return None;
            }
            let previous = self.current.compare_and_swap(&current, next.clone());
            if Arc::ptr_eq(&*previous, &current) {
                return Some(DirectoryChange::between(&current, &next));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::testkit::*;

    fn snapshot(value: &serde_json::Value) -> DirectorySnapshot {
        let verified = trust()
            .verify_access(&signed_manifest(value), NOW_MS)
            .unwrap();
        DirectorySnapshot::from_verified(&verified, ManifestSource::Seed)
    }

    #[test]
    fn expands_service_urls_for_home_cell() {
        let snap = snapshot(&manifest_with_serial(1));
        assert_eq!(snap.home_cell(), "cell-cne-1");
        assert_eq!(snap.home_region(), "cn-east");
        let (index, ap) = snap.access_point("ap-cne1-ct").unwrap();
        assert_eq!(
            ap.endpoint(ServiceKind::ImWs),
            Some("wss://im-ct.example.com/ws")
        );
        assert_eq!(
            ap.endpoint(ServiceKind::ImQuic),
            Some("quic://im-ct.example.com:443")
        );
        assert_eq!(
            snap.endpoint(index, ServiceKind::CoreApi),
            Some("https://im-ct.example.com/api")
        );
        assert_eq!(
            ap.endpoint(ServiceKind::Ping),
            Some("https://im-ct.example.com/.well-known/flare/ping")
        );
        assert_eq!(ap.ip_hints.len(), 2);
        assert_eq!(snap.offering(ServiceKind::Media).count(), 1);
        assert!(!snap.is_stale(NOW_MS));
        assert!(!snap.is_development());
    }

    #[test]
    fn replace_reports_added_removed_and_changed() {
        let first = manifest_with_serial(1);
        let mut second = manifest_with_serial(2);
        let mut extra = second["regions"][0]["cells"][0]["accessPoints"][0].clone();
        extra["id"] = "ap-cne1-cm".into();
        extra["host"] = "im-cm.example.com".into();
        second["regions"][0]["cells"][0]["accessPoints"][0]["status"] = "draining".into();
        second["regions"][0]["cells"][0]["accessPoints"]
            .as_array_mut()
            .unwrap()
            .push(extra);
        second["clientPolicy"] = serde_json::json!({ "raceStaggerMs": 500 });

        let directory = EndpointDirectory::new(snapshot(&first));
        let change = directory.replace(snapshot(&second));
        assert_eq!((change.from_serial, change.to_serial), (1, 2));
        assert_eq!(change.added, vec![ApId::from("ap-cne1-cm")]);
        assert_eq!(
            change.changed,
            vec![(ApId::from("ap-cne1-ct"), ApStatus::Draining)]
        );
        assert!(change.removed.is_empty());
        assert!(change.policy_changed);
        assert_eq!(directory.snapshot().serial(), 2);
    }
}
