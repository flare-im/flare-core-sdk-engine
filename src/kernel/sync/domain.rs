//! 领域无关同步 SPI：任意领域（消息/会话/业务扩展的自定义领域）实现 [SyncDomain] 并注册进收敛内核，
//! 即复用冷启/热启/重连/追赶/优先级/缺口修复。**内核只提供机制；领域提供语义**（守 flare-im-spec 4/5：
//! `im.messages`/`im.conversations` 为内置领域，`example.items` 之类业务领域由扩展实现注册，core 不认识其业务规则）。

use std::future::Future;
use std::pin::Pin;

use crate::infrastructure::persistence::StoreProvider;
use crate::kernel::event::EventBus;
use crate::shared::error::Result;

use super::task::{SyncTrigger, SyncVisibility};

/// 领域标识：**开放集**稳定字符串（如 `im.messages`、`example.items`、`biz.orders`），
/// 取代闭枚举 SyncScope，使新业务零改引擎接入。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DomainId(pub String);

impl DomainId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 领域在收敛内核中的调度阶段（对应三层就绪模型）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainPhase {
    /// T0：本地水合即可满足，无需网络。
    Hydrate,
    /// T1：前台新鲜度，冷启可 gate。
    Foreground,
    /// T2：环境收敛，静默后台。
    Ambient,
}

/// 收敛优先级（注意力驱动）。派生 `Ord` 使 `P0Foreground < P3Ambient`，
/// 即排序后前台优先——调度器据此从"眼睛所在处"向外补齐。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConvergencePriority {
    /// P0 前台会话/领域。
    P0Foreground,
    /// P1 可见列表窗口。
    P1VisibleWindow,
    /// P2 近期活跃。
    P2Recent,
    /// P3 长尾 / 维护。
    P3Ambient,
}

/// 领域调度声明：注册时由领域给出，调度器据此编排。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LaneSpec {
    pub phase: DomainPhase,
    pub priority: ConvergencePriority,
    pub visibility: SyncVisibility,
    pub trigger: SyncTrigger,
}

/// 领域子游标：挂在设备全局同步点下的单调位点（不透明字符串，领域自解释）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainCursor(pub Option<String>);

impl DomainCursor {
    pub fn empty() -> Self {
        Self(None)
    }
    pub fn at(cursor: impl Into<String>) -> Self {
        Self(Some(cursor.into()))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

/// 一项领域增量：单调 `entity_seq`（供内核统一排序 + 缺口检测）+ 原始字节（领域自解码）。
#[derive(Clone, Debug)]
pub struct DomainItem {
    pub entity_seq: u64,
    pub payload: Vec<u8>,
}

/// 一批领域增量：`DomainStreamRouter` 从全局流多路复用后按 `DomainId` 投喂，或由领域 `pull` 直接返回。
#[derive(Clone, Debug, Default)]
pub struct DomainDelta {
    pub items: Vec<DomainItem>,
    pub next_cursor: DomainCursor,
    pub has_more: bool,
}

/// 应用一项增量后的结果：沿用现有 applier 的 LWW/去重/乱序语义精神。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// 已应用（推进游标）。
    Applied,
    /// 陈旧被忽略（LWW，游标可推进）。
    IgnoredStale,
    /// 目标暂不可用，保留后续重放（缺口/乱序）。
    Deferred,
}

/// 同步领域运行期上下文：builder 期注册的领域在真正 pull/apply 时才能拿到用户、
/// store 和 event bus。外部领域可忽略它；内置 `im.messages` 领域需要它复用
/// `SyncEventApplier` / `IncomingMessageConverger`。
#[derive(Clone, Default)]
pub struct SyncDomainContext {
    pub user_id: Option<String>,
    pub store: Option<StoreProvider>,
    pub bus: Option<EventBus>,
}

impl SyncDomainContext {
    pub fn detached() -> Self {
        Self::default()
    }

    pub fn for_user(user_id: impl Into<String>, store: StoreProvider, bus: EventBus) -> Self {
        Self {
            user_id: Some(user_id.into()),
            store: Some(store),
            bus: Some(bus),
        }
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 同步领域：任意业务领域实现本 trait 并经 `IMClientBuilder::add_sync_domain` 注册即接入收敛内核。
/// `im.messages`/`im.conversations` 为内置实现；`example.items` 之类业务领域由扩展实现。
pub trait SyncDomain: Send + Sync {
    /// 稳定领域标识（开放集）。
    fn id(&self) -> DomainId;

    /// 调度声明（阶段 × 优先级 × 可见性）。
    fn lane(&self) -> LaneSpec;

    /// 拉取自 `since` 之后的增量。返回空 `items` 表示"随全局流多路复用"，由 `DomainStreamRouter` 投喂。
    fn pull(
        &self,
        context: SyncDomainContext,
        since: DomainCursor,
    ) -> BoxFuture<'_, Result<DomainDelta>>;

    /// 应用一项增量：领域自持 LWW / 去重 / 乱序重放。
    fn apply(
        &self,
        context: SyncDomainContext,
        item: &DomainItem,
    ) -> BoxFuture<'_, Result<ApplyOutcome>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_orders_foreground_before_ambient() {
        let mut lanes = [
            ConvergencePriority::P3Ambient,
            ConvergencePriority::P0Foreground,
            ConvergencePriority::P2Recent,
            ConvergencePriority::P1VisibleWindow,
        ];
        lanes.sort();
        assert_eq!(
            lanes,
            [
                ConvergencePriority::P0Foreground,
                ConvergencePriority::P1VisibleWindow,
                ConvergencePriority::P2Recent,
                ConvergencePriority::P3Ambient,
            ]
        );
    }

    #[test]
    fn domain_id_is_open_set_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(DomainId::new("im.messages"));
        set.insert(DomainId::new("example.items"));
        set.insert(DomainId::new("example.items"));
        assert_eq!(set.len(), 2);
        assert!(set.contains(&DomainId::new("im.messages")));
    }

    #[test]
    fn domain_cursor_empty_vs_at() {
        assert!(DomainCursor::empty().is_empty());
        assert!(!DomainCursor::at("global-42").is_empty());
    }

    #[test]
    fn lane_spec_declares_trigger_dimension() {
        let lane = LaneSpec {
            phase: DomainPhase::Foreground,
            priority: ConvergencePriority::P0Foreground,
            visibility: SyncVisibility::Silent,
            trigger: SyncTrigger::Reconnect,
        };

        assert_eq!(lane.trigger, SyncTrigger::Reconnect);
    }

    #[test]
    fn domain_context_can_be_detached_for_pure_router_tests() {
        let context = SyncDomainContext::detached();

        assert!(context.user_id.is_none());
        assert!(context.store.is_none());
        assert!(context.bus.is_none());
    }
}
