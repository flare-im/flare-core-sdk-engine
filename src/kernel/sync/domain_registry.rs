//! 领域路由与注册表：`DomainStreamRouter` 的核心。把（未来来自设备全局同步点的）多路复用增量
//! 按 [DomainId] 路由到已注册的 [SyncDomain]，按 `entity_seq` 有序应用，并汇总缺口（Deferred）供修复。
//!
//! 注册在 builder 期完成（不可变后），路由热路径**无锁**（性能红线）。`im.messages`/`im.conversations`
//! 为内置领域；`example.items` 等任意业务领域实现 [SyncDomain] 注册即接入，
//! 白拿冷启/热启/重连/追赶/优先级/缺口修复——core 不认识其业务规则。

use std::collections::HashMap;
use std::sync::Arc;

use super::domain::{ApplyOutcome, DomainId, DomainItem, SyncDomain, SyncDomainContext};

const MAX_REPORTED_GLOBAL_GAPS: usize = 128;

/// 一次路由的结果汇总：应用/陈旧忽略计数 + 需重放的 `entity_seq`（缺口）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteReport {
    pub applied: usize,
    pub ignored_stale: usize,
    /// 目标暂不可用被推迟的 entity_seq（乱序/缺口），供缺口修复重放。
    pub deferred_seqs: Vec<u64>,
    /// 收到但无对应注册领域的 items 数（诊断用；不阻断其余领域）。
    pub unrouted: usize,
}

#[derive(Clone, Debug)]
pub struct GlobalDomainItem {
    pub global_seq: u64,
    pub domain_id: DomainId,
    pub item: DomainItem,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GlobalStreamRouteReport {
    pub domains: HashMap<DomainId, RouteReport>,
    pub highest_global_seq: u64,
    /// 设备全局游标**唯一允许推进到**的水位（I1/I3 契约）：连续、且已注册领域全部应用成功的
    /// 前缀（未注册领域不阻塞——其接入后由自身 domain cursor 管辖）。缺口/Deferred 之后的
    /// 增量即使已应用也不计入——游标停在这里，`missing_global_seqs` 截断因此无害：
    /// 下次从 safe 水位重拉会重新检出全部缺口，绝不静默丢失。
    pub safe_global_seq: u64,
    pub missing_global_seqs: Vec<u64>,
    pub missing_global_seqs_truncated: bool,
}

/// 领域注册表：builder 期注册，运行期只读路由。
#[derive(Default)]
pub struct SyncDomainRegistry {
    domains: HashMap<DomainId, Arc<dyn SyncDomain>>,
}

impl SyncDomainRegistry {
    pub fn new() -> Self {
        Self {
            domains: HashMap::new(),
        }
    }

    /// 注册一个领域（builder 期）。同 id 覆盖——一条干净路径，无并存。
    pub fn register(&mut self, domain: Arc<dyn SyncDomain>) {
        self.domains.insert(domain.id(), domain);
    }

    pub fn get(&self, id: &DomainId) -> Option<Arc<dyn SyncDomain>> {
        self.domains.get(id).cloned()
    }

    pub fn contains(&self, id: &DomainId) -> bool {
        self.domains.contains_key(id)
    }

    pub fn ids(&self) -> Vec<DomainId> {
        self.domains.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.domains.len()
    }

    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// 路由一批同领域增量：按 `entity_seq` 升序应用到该领域，汇总结果。
    /// 未注册领域整批记为 `unrouted`（不阻断——多路复用流里其它领域照常处理）。
    pub async fn route(&self, domain_id: &DomainId, items: &[DomainItem]) -> RouteReport {
        self.route_with_context(SyncDomainContext::detached(), domain_id, items)
            .await
    }

    /// 带运行期上下文的领域路由。内置领域可通过 context 复用 store/bus/user；
    /// 纯业务领域和测试领域可忽略该参数。
    pub async fn route_with_context(
        &self,
        context: SyncDomainContext,
        domain_id: &DomainId,
        items: &[DomainItem],
    ) -> RouteReport {
        let Some(domain) = self.get(domain_id) else {
            return RouteReport {
                unrouted: items.len(),
                ..Default::default()
            };
        };
        // 统一按 entity_seq 有序应用（缺口检测/LWW 依赖单调序）。
        let mut ordered: Vec<&DomainItem> = items.iter().collect();
        ordered.sort_by_key(|item| item.entity_seq);

        let mut report = RouteReport::default();
        for item in ordered {
            match domain.apply(context.clone(), item).await {
                Ok(ApplyOutcome::Applied) => report.applied += 1,
                Ok(ApplyOutcome::IgnoredStale) => report.ignored_stale += 1,
                Ok(ApplyOutcome::Deferred) => report.deferred_seqs.push(item.entity_seq),
                Err(error) => {
                    tracing::warn!(
                        domain = %domain_id.as_str(),
                        entity_seq = item.entity_seq,
                        error = %error,
                        "领域增量应用失败，记为待重放缺口"
                    );
                    report.deferred_seqs.push(item.entity_seq);
                }
            }
        }
        report
    }

    /// 路由一批全局增量流 item：先按 `global_seq` 检测设备级缺口，再按 `domain_id`
    /// 多路复用到领域路由器；每个领域内部仍按 `entity_seq` 升序应用。
    pub async fn route_global_stream(
        &self,
        since_global_seq: u64,
        items: &[GlobalDomainItem],
    ) -> GlobalStreamRouteReport {
        self.route_global_stream_with_context(
            SyncDomainContext::detached(),
            since_global_seq,
            items,
        )
        .await
    }

    /// 带运行期上下文的全局流路由。
    pub async fn route_global_stream_with_context(
        &self,
        context: SyncDomainContext,
        since_global_seq: u64,
        items: &[GlobalDomainItem],
    ) -> GlobalStreamRouteReport {
        let mut ordered: Vec<&GlobalDomainItem> = items
            .iter()
            .filter(|item| item.global_seq > since_global_seq)
            .collect();
        ordered.sort_by_key(|item| item.global_seq);

        let mut report = GlobalStreamRouteReport {
            highest_global_seq: since_global_seq,
            safe_global_seq: since_global_seq,
            ..Default::default()
        };
        let mut expected_global_seq = since_global_seq.saturating_add(1);
        let mut by_domain: HashMap<DomainId, Vec<DomainItem>> = HashMap::new();

        for item in &ordered {
            if item.global_seq > expected_global_seq {
                Self::record_global_gap(
                    &mut report,
                    expected_global_seq,
                    item.global_seq.saturating_sub(1),
                );
            }

            expected_global_seq = item.global_seq.saturating_add(1);
            report.highest_global_seq = report.highest_global_seq.max(item.global_seq);
            by_domain
                .entry(item.domain_id.clone())
                .or_default()
                .push(item.item.clone());
        }

        for (domain_id, domain_items) in by_domain {
            let domain_report = self
                .route_with_context(context.clone(), &domain_id, &domain_items)
                .await;
            report.domains.insert(domain_id, domain_report);
        }

        // safe 水位 = 连续前缀 ∩ 应用成功前缀：遇缺口或已注册领域的 Deferred 即停；
        // 未注册领域（unrouted）不阻塞。游标推进以此为唯一依据（I1 顺序契约的全局流版）。
        let mut safe_global_seq = since_global_seq;
        let mut expected = since_global_seq.saturating_add(1);
        for item in &ordered {
            if item.global_seq != expected {
                break;
            }
            let deferred = report
                .domains
                .get(&item.domain_id)
                .map(|domain_report| domain_report.deferred_seqs.contains(&item.item.entity_seq))
                .unwrap_or(false);
            if deferred {
                break;
            }
            safe_global_seq = item.global_seq;
            expected = expected.saturating_add(1);
        }
        report.safe_global_seq = safe_global_seq;

        report
    }

    fn record_global_gap(report: &mut GlobalStreamRouteReport, start: u64, end: u64) {
        for seq in start..=end {
            if report.missing_global_seqs.len() >= MAX_REPORTED_GLOBAL_GAPS {
                report.missing_global_seqs_truncated = true;
                return;
            }
            report.missing_global_seqs.push(seq);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{
        ConvergencePriority, DomainCursor, DomainDelta, DomainPhase, LaneSpec, SyncTrigger,
        SyncVisibility,
    };
    use crate::shared::error::Result;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    /// 玩具领域：记录被应用的 seq；seq 为 0 视为"目标不可用"→ Deferred，验证缺口汇总。
    struct ToyDomain {
        id: DomainId,
        applied: Arc<Mutex<Vec<u64>>>,
    }

    impl SyncDomain for ToyDomain {
        fn id(&self) -> DomainId {
            self.id.clone()
        }
        fn lane(&self) -> LaneSpec {
            LaneSpec {
                phase: DomainPhase::Ambient,
                priority: ConvergencePriority::P3Ambient,
                visibility: SyncVisibility::Silent,
                trigger: SyncTrigger::BackgroundMaintenance,
            }
        }
        fn pull(
            &self,
            _context: crate::kernel::sync::SyncDomainContext,
            _since: DomainCursor,
        ) -> Pin<Box<dyn Future<Output = Result<DomainDelta>> + Send + '_>> {
            Box::pin(async { Ok(DomainDelta::default()) })
        }
        fn apply(
            &self,
            _context: crate::kernel::sync::SyncDomainContext,
            item: &DomainItem,
        ) -> Pin<Box<dyn Future<Output = Result<ApplyOutcome>> + Send + '_>> {
            let applied = self.applied.clone();
            let seq = item.entity_seq;
            Box::pin(async move {
                if seq == 0 {
                    return Ok(ApplyOutcome::Deferred);
                }
                applied.lock().unwrap().push(seq);
                Ok(ApplyOutcome::Applied)
            })
        }
    }

    fn item(entity_seq: u64) -> DomainItem {
        DomainItem {
            entity_seq,
            payload: Vec::new(),
        }
    }

    fn global_item(global_seq: u64, domain_id: &str, entity_seq: u64) -> GlobalDomainItem {
        GlobalDomainItem {
            global_seq,
            domain_id: DomainId::new(domain_id),
            item: item(entity_seq),
        }
    }

    #[tokio::test]
    async fn routes_items_to_registered_domain_in_seq_order() {
        let applied = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SyncDomainRegistry::new();
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("example.items"),
            applied: applied.clone(),
        }));

        // 乱序投喂，路由应按 entity_seq 升序应用。
        let report = registry
            .route(
                &DomainId::new("example.items"),
                &[item(3), item(1), item(0), item(2)],
            )
            .await;

        assert_eq!(*applied.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(report.applied, 3);
        assert_eq!(report.deferred_seqs, vec![0]); // seq 0 → Deferred（缺口）
        assert_eq!(report.unrouted, 0);
    }

    #[tokio::test]
    async fn unregistered_domain_is_unrouted_not_error() {
        let registry = SyncDomainRegistry::new();
        let report = registry
            .route(&DomainId::new("biz.unknown"), &[item(1), item(2)])
            .await;
        assert_eq!(report.unrouted, 2);
        assert_eq!(report.applied, 0);
    }

    #[test]
    fn register_same_id_replaces_single_clean_path() {
        let mut registry = SyncDomainRegistry::new();
        let a = Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: Arc::new(Mutex::new(Vec::new())),
        });
        let b = Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: Arc::new(Mutex::new(Vec::new())),
        });
        registry.register(a);
        registry.register(b);
        assert_eq!(registry.len(), 1);
        assert!(registry.contains(&DomainId::new("im.messages")));
    }

    #[tokio::test]
    async fn routes_global_stream_by_domain_and_entity_order() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let conversations = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SyncDomainRegistry::new();
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: messages.clone(),
        }));
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("im.conversations"),
            applied: conversations.clone(),
        }));

        let report = registry
            .route_global_stream(
                0,
                &[
                    global_item(3, "im.messages", 2),
                    global_item(1, "im.conversations", 7),
                    global_item(2, "im.messages", 1),
                ],
            )
            .await;

        assert_eq!(*messages.lock().unwrap(), vec![1, 2]);
        assert_eq!(*conversations.lock().unwrap(), vec![7]);
        assert_eq!(report.highest_global_seq, 3);
        assert_eq!(
            report.safe_global_seq, 3,
            "contiguous fully-applied stream advances safe watermark to highest"
        );
        assert!(report.missing_global_seqs.is_empty());
        assert_eq!(
            report
                .domains
                .get(&DomainId::new("im.messages"))
                .map(|r| r.applied),
            Some(2)
        );
    }

    #[tokio::test]
    async fn global_stream_reports_global_gaps_without_blocking_known_domains() {
        let applied = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SyncDomainRegistry::new();
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: applied.clone(),
        }));

        let report = registry
            .route_global_stream(
                0,
                &[
                    global_item(1, "im.messages", 1),
                    global_item(3, "biz.unknown", 1),
                ],
            )
            .await;

        assert_eq!(*applied.lock().unwrap(), vec![1]);
        assert_eq!(report.highest_global_seq, 3);
        assert_eq!(
            report.safe_global_seq, 1,
            "global cursor must stop at the contiguous point before the gap"
        );
        assert_eq!(report.missing_global_seqs, vec![2]);
        assert_eq!(
            report
                .domains
                .get(&DomainId::new("biz.unknown"))
                .map(|r| r.unrouted),
            Some(1)
        );
    }

    #[tokio::test]
    async fn safe_global_seq_stops_before_deferred_item_of_registered_domain() {
        // I3：已注册领域应用失败（Deferred）的项必须挡住全局游标——越过它=永久丢失该增量。
        let applied = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SyncDomainRegistry::new();
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: applied.clone(),
        }));

        let report = registry
            .route_global_stream(
                0,
                &[
                    global_item(1, "im.messages", 5),
                    global_item(2, "im.messages", 0), // entity_seq 0 → ToyDomain Deferred
                    global_item(3, "im.messages", 7),
                ],
            )
            .await;

        assert_eq!(*applied.lock().unwrap(), vec![5, 7]);
        assert_eq!(report.highest_global_seq, 3);
        assert_eq!(
            report.safe_global_seq, 1,
            "deferred item must block the safe watermark so it gets replayed"
        );
    }

    #[tokio::test]
    async fn safe_global_seq_skips_unrouted_domains_without_blocking() {
        // 未注册领域不阻塞全局游标：该领域接入后由自身 domain cursor 补历史。
        let applied = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SyncDomainRegistry::new();
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: applied.clone(),
        }));

        let report = registry
            .route_global_stream(
                0,
                &[
                    global_item(1, "im.messages", 1),
                    global_item(2, "biz.unknown", 9),
                    global_item(3, "im.messages", 2),
                ],
            )
            .await;

        assert_eq!(*applied.lock().unwrap(), vec![1, 2]);
        assert_eq!(
            report.safe_global_seq, 3,
            "unrouted domain must not block the safe watermark"
        );
    }

    #[tokio::test]
    async fn safe_global_seq_makes_gap_truncation_harmless() {
        // I3：缺口报告截断（>128）时，safe 水位仍停在连续前缀——重拉自动重检全部缺口。
        let applied = Arc::new(Mutex::new(Vec::new()));
        let mut registry = SyncDomainRegistry::new();
        registry.register(Arc::new(ToyDomain {
            id: DomainId::new("im.messages"),
            applied: applied.clone(),
        }));

        // 1 之后直接跳到 200：缺口 2..=199（198 个 > 128 上限触发截断）。
        let report = registry
            .route_global_stream(
                0,
                &[
                    global_item(1, "im.messages", 1),
                    global_item(200, "im.messages", 2),
                ],
            )
            .await;

        assert!(report.missing_global_seqs_truncated);
        assert_eq!(
            report.safe_global_seq, 1,
            "cursor stays at contiguous prefix; truncated gaps are re-detected on next pull"
        );
    }
}
