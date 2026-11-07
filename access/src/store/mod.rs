//! 登录前的全局存储端口与实现。
//!
//! 与按用户分库的 IM 数据完全分开：接入清单、密钥状态与健康簿在登录之前就要可用，
//! 也不应随某个用户退出登录而被清掉。

mod host;
mod install_id;
mod manifests;
mod memory;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
mod sqlite;

pub use host::{GlobalStorageHost, HostAccessStore};
pub use install_id::install_id;
pub use manifests::{
    AcceptError, AcceptOutcome, FetchMeta, ManifestSource, ManifestStore, StoredDocument,
};
pub use memory::MemoryAccessStore;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub use sqlite::SqliteAccessStore;

use crate::platform::MaybeSendSync;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("access store backend failed: {0}")]
    Backend(String),
    #[error("access store value is corrupt: {0}")]
    Corrupt(String),
}

/// 一条写操作；`apply` 把一批操作作为一个原子单元提交。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreOp {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

impl StoreOp {
    pub fn key(&self) -> &str {
        match self {
            StoreOp::Put { key, .. } | StoreOp::Delete { key } => key,
        }
    }
}

/// 登录前全局键值存储。原生默认实现为 SQLite（[`SqliteAccessStore`]），
/// wasm 由宿主经 [`GlobalStorageHost`] 提供（IndexedDB）。
///
/// 语义要求：`put`/`delete`/`apply` 各自原子；`apply` 中的多条操作要么全部生效、要么都不生效
/// （清单与它的 `maxSerial` 必须一起落盘，否则崩溃后可能出现「新清单 + 旧 serial」或相反）。
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait AccessStore: MaybeSendSync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;

    async fn put(&self, key: &str, value: &[u8]) -> Result<(), StoreError>;

    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    /// 原子批量写。
    async fn apply(&self, ops: &[StoreOp]) -> Result<(), StoreError>;

    /// 列出某前缀下的全部键值（健康簿启动加载与过期清理使用）。
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError>;
}

/// 存储键。集中定义，宿主实现与诊断工具都以这里为准。
///
/// 接入清单与它的 serial 按 bootstrapId 分区、密钥状态按根集合指纹分区：
/// 同一数据目录先后跑过两个应用（或测试包与正式包）时，一方的 serial 不会挡住另一方。
pub mod keys {
    pub fn access_current(bootstrap_id: &str) -> String {
        format!("manifest/access/{bootstrap_id}/current")
    }

    pub fn access_previous(bootstrap_id: &str) -> String {
        format!("manifest/access/{bootstrap_id}/previous")
    }

    pub fn access_serial(bootstrap_id: &str) -> String {
        format!("serial/access/{bootstrap_id}")
    }

    pub fn keystate_current(rootset: &str) -> String {
        format!("manifest/keys/{rootset}/current")
    }

    pub fn keystate_serial(rootset: &str) -> String {
        format!("serial/keys/{rootset}")
    }

    /// 发布清单按「应用 × 平台 × 渠道」各一份，serial 也各走各的。
    pub fn release_current(bootstrap_id: &str, target: &str) -> String {
        format!("manifest/release/{bootstrap_id}/{target}/current")
    }

    pub fn release_serial(bootstrap_id: &str, target: &str) -> String {
        format!("serial/release/{bootstrap_id}/{target}")
    }

    /// 安装级随机 ID：灰度分桶用，不关联设备标识。属于这次安装，不按应用分区。
    pub const INSTALL_ID: &str = "install/id";

    /// 更新状态机的持久部分（按 bootstrapId 分区）。
    pub fn update_state(bootstrap_id: &str) -> String {
        format!("update/state/{bootstrap_id}")
    }

    /// 资源包（按 bootstrapId 分区）。
    pub fn resource_pack(bootstrap_id: &str, id: &str) -> String {
        format!("resource/{bootstrap_id}/{id}")
    }

    pub fn resource_prefix(bootstrap_id: &str) -> String {
        format!("resource/{bootstrap_id}/")
    }

    /// 引导源簿：每个源的 ETag、上次成功、降权到期（按 bootstrapId 分区）。
    pub fn bootstrap_sources(bootstrap_id: &str) -> String {
        format!("bootstrap/sources/{bootstrap_id}")
    }

    pub const HEALTH_PREFIX: &str = "health/";
    pub const HEALTH_AP_PREFIX: &str = "health/ap/";
    pub const HEALTH_NET_PREFIX: &str = "health/net/";

    pub fn health_ap(ap_id: &str, netprint: &str) -> String {
        format!("{HEALTH_AP_PREFIX}{ap_id}/{netprint}")
    }

    pub fn health_net(netprint: &str) -> String {
        format!("{HEALTH_NET_PREFIX}{netprint}")
    }
}
