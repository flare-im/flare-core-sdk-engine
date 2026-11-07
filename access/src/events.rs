//! 需要上层知晓的告警。引擎把它们映射为事件（如开发模式角标、清单陈旧、诊断记录），
//! 本 crate 不直接发事件，避免依赖引擎的事件总线。

use serde::Serialize;

use crate::manifest::DocType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AccessWarning {
    /// 开发模式：端点来自配置、未经签名。上层应持续提示并显示「开发模式」角标。
    DevelopmentMode,
    /// 当前清单已过期：仍可连接，但应提高刷新频率。
    ManifestStale { serial: u64, expires_at_ms: i64 },
    /// 本地保存的文档读回后验签失败（例如签名密钥已被吊销），已弃用。
    StoredDocumentRejected { doc: DocType, code: String },
    /// 本地保存的文档无法读取或解析。
    StoredDocumentUnreadable { doc: DocType, message: String },
    /// 内置种子验签失败。
    SeedRejected { code: String },
    /// 本地 LKG 不可用、只能使用比已接受 serial 更旧的内置种子：
    /// 种子来自安装包本身（可信渠道），在内存中使用但不写回，也不降低 maxSerial。
    SeedBelowStoredSerial {
        seed_serial: u64,
        stored_serial: u64,
    },
    /// 既没有可用的 LKG，也没有可用的种子：目录为空，等引导拉取或恢复链接。
    NoUsableManifest {
        lkg: Option<String>,
        seed: Option<String>,
    },
    /// 持久化失败（内存中的已验证状态照常生效）。
    PersistFailed { message: String },
}
