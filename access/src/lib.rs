//! Flare IM 客户端链路核心：接入清单、信任链、端点目录、健康簿与选路。
//!
//! 这个 crate 只回答三个问题：
//!
//! 1. **哪些入口可信**：`trust` 校验 DSSE 信封与「根密钥 → 在线密钥证书 → 文档签名」两级链，
//!    `store` 负责最后可信清单（LKG）与 serial 防回滚。
//! 2. **有哪些入口**：`directory` 把已验证清单展开成按服务索引的不可变快照，原子替换。
//! 3. **现在该连哪个**：`health` 记录每个（接入点, 网络指纹）的 RTT、失败率、熔断与协议胜者，
//!    `select` 在此之上打分、分层随机、粘性与恢复切回判定，`probe` 管探活合法性与预算。
//!
//! `release` 是更新中心的决策核心：发布清单评估、提示频率与状态机；`download` 是原生端的制品下载。
//!
//! 连接生命周期（何时重连、退避多久）属于引擎的 `LinkSupervisor`，不在这里；
//! 网络 I/O（探活、拉取清单）通过 [`probe::ProbeTransport`] 与 [`bootstrap::HttpFetch`] 由引擎注入，
//! 这样本 crate 不依赖任何 HTTP 栈，原生与 wasm 共用同一份逻辑。
//!
//! 所有涉及时间的接口都显式接收 `now_ms`（Unix 毫秒）：wasm 上 `std::time` 会 panic，
//! 而且显式时间让熔断、过期、恢复切回这些规则可以用黄金向量逐步回放。
//!
//! `manifest` 与 `trust` 也是控制面的编排与签名基础：控制面以 `default-features = false` 依赖，
//! 只得到文档类型、结构校验、签名与验签；其余模块属于 `client` feature。

#[cfg(feature = "client")]
pub mod bootstrap;
#[cfg(feature = "client")]
pub mod directory;
#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
pub mod download;
#[cfg(feature = "client")]
pub mod events;
#[cfg(feature = "client")]
pub mod health;
#[cfg(feature = "client")]
pub mod kernel;
pub mod manifest;
pub mod parsed_url;
#[cfg(feature = "client")]
pub mod platform;
#[cfg(feature = "client")]
pub mod probe;
#[cfg(feature = "client")]
pub mod release;
#[cfg(feature = "client")]
pub mod select;
#[cfg(feature = "client")]
pub mod store;
pub mod time;
pub mod trust;
pub mod types;

#[cfg(feature = "client")]
pub use bootstrap::{
    AccessConfig, BootstrapClock, BootstrapFetcher, DevelopmentAccessPoint, DevelopmentEndpoints,
    FetchError, FetchRequest, FetchResponse, HttpFetch, RefreshMode, RefreshOutcome, RefreshReport,
    TrustMode,
};
#[cfg(feature = "client")]
pub use directory::{AccessPointEntry, DirectoryChange, DirectorySnapshot, EndpointDirectory};
#[cfg(feature = "client")]
pub use events::AccessWarning;
#[cfg(feature = "client")]
pub use health::{
    AttemptKind, FailureClass, HealthBook, NetPrint, NeutralReason, Observation, Outcome,
};
#[cfg(feature = "client")]
pub use kernel::{AccessKernel, BootstrapReport, KernelOptions};
pub use manifest::{AccessManifest, ClientPolicy, ReleaseManifest};
pub use parsed_url::{ParsedUrl, UrlError};
#[cfg(feature = "client")]
pub use platform::{MaybeSendSync, RandomSource};
#[cfg(feature = "client")]
pub use select::{Candidate, SelectRequest, Selection, Selector};
#[cfg(feature = "client")]
pub use store::AccessStore;
pub use trust::{TrustError, TrustStore};
pub use types::{ApId, ApStatus, Protocol, ServiceClass, ServiceKind};
