//! 选路：在端点目录与健康簿之上给出候选顺序、协议计划、手动固定与恢复切回判定。
//!
//! 引擎的 `LinkSupervisor`（IM）与 `HttpRouter`（REST）都通过 [`Selector::select`] 取候选；
//! 打分、分层与恢复切回的规则本身在纯函数模块 [`score`]、[`failback`]、[`protocol`] 中。

pub mod failback;
pub mod protocol;
pub mod score;

use std::sync::{Arc, Mutex, MutexGuard};

pub use failback::{FailbackDecision, FailbackHold, FailbackInput, evaluate_failback};
pub use protocol::{ProtocolPlan, plan_protocols};
pub use score::{ApInput, FilterReason, PinOutcome, RankParams, Ranking, TagGroup, pick, rank};

use crate::directory::{AccessPointEntry, DirectorySnapshot, EndpointDirectory};
use crate::health::{HealthBook, NetPrint};
use crate::platform::{DefaultRandom, RandomSource};
use crate::time::HOUR_MS;
use crate::types::{ApId, ServiceClass, ServiceKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectorOptions {
    /// 当前平台能否建 QUIC（浏览器 wasm 不能）。
    pub quic_supported: bool,
}

impl Default for SelectorOptions {
    fn default() -> Self {
        Self {
            quic_supported: !cfg!(target_arch = "wasm32"),
        }
    }
}

/// 一次选路请求。
#[derive(Debug, Clone, Copy)]
pub struct SelectRequest<'a> {
    pub service: ServiceClass,
    pub net: NetPrint,
    /// 本机网络特征标签（平台提供，如 `isp:ct`）；没有就传空。
    pub net_tags: &'a [String],
    /// 当前在用的接入点（IM 连接所在的，或 REST 希望跟随的）。
    pub current: Option<&'a str>,
    /// 本轮已经试过、要跳过的接入点。
    pub exclude: &'a [&'a str],
    pub now_ms: i64,
}

/// 一个候选。
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    /// 在 `Selection::snapshot.access_points()` 中的下标。
    pub ap_index: usize,
    pub ap_id: ApId,
    pub score: f64,
    /// 属于最优档。
    pub top_tier: bool,
    /// 处于 half-open：连接前必须 `HealthBook::claim_trial` 认领成功，否则跳到下一个候选。
    pub half_open: bool,
    /// IM 的协议计划；REST 为 `None`。
    pub protocols: Option<ProtocolPlan>,
}

/// 选路结果。`candidates[0]` 是本次选择，其余按分数排好，作为「立即换下一个」的顺序。
#[derive(Debug, Clone)]
pub struct Selection {
    pub snapshot: Arc<DirectorySnapshot>,
    pub candidates: Vec<Candidate>,
    pub pin: PinOutcome,
    /// 未入选的接入点与原因（诊断用）。
    pub filtered: Vec<(ApId, FilterReason)>,
    /// 候选为空且原因是熔断时，最早进入 half-open 的时刻（引擎据此安排退避到期）。
    pub next_half_open_at_ms: Option<i64>,
}

impl Selection {
    pub fn first(&self) -> Option<&Candidate> {
        self.candidates.first()
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    pub fn access_point(&self, candidate: &Candidate) -> &AccessPointEntry {
        &self.snapshot.access_points()[candidate.ap_index]
    }

    /// 候选在某服务上的完整 URL。
    pub fn endpoint(&self, candidate: &Candidate, kind: ServiceKind) -> Option<&str> {
        self.snapshot.endpoint(candidate.ap_index, kind)
    }

    /// REST 请求的 base URL（IM 请用 `protocols` 决定入口）。
    pub fn http_base(&self, candidate: &Candidate, service: ServiceClass) -> Option<&str> {
        self.endpoint(candidate, service.http_service()?)
    }
}

/// 手动固定的接入点。只在内存里：App 重启自动解除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedAccessPoint {
    pub ap_id: ApId,
    pub pinned_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PinError {
    #[error("access point {0:?} is not in the current directory")]
    UnknownAccessPoint(String),
}

/// 恢复切回判定的请求。
#[derive(Debug, Clone, Copy)]
pub struct FailbackRequest<'a> {
    pub net: NetPrint,
    pub net_tags: &'a [String],
    /// 当前 IM 连接所在接入点。
    pub current: &'a str,
    pub foregrounded: bool,
    pub last_activity_ms: i64,
    pub last_switch_ms: i64,
    pub now_ms: i64,
}

/// 恢复切回判定结果。
#[derive(Debug, Clone, PartialEq)]
pub struct FailbackVerdict {
    pub decision: FailbackDecision,
    /// 首选接入点（不考虑粘性时的最优者）。
    pub preferred: Option<ApId>,
}

pub struct Selector {
    directory: Arc<EndpointDirectory>,
    health: Arc<HealthBook>,
    options: SelectorOptions,
    rng: Mutex<Box<dyn RandomSource>>,
    pin: Mutex<Option<PinnedAccessPoint>>,
}

impl Selector {
    pub fn new(
        directory: Arc<EndpointDirectory>,
        health: Arc<HealthBook>,
        options: SelectorOptions,
    ) -> Self {
        Self::with_random(directory, health, options, Box::new(DefaultRandom::new()))
    }

    pub fn with_random(
        directory: Arc<EndpointDirectory>,
        health: Arc<HealthBook>,
        options: SelectorOptions,
        rng: Box<dyn RandomSource>,
    ) -> Self {
        Self {
            directory,
            health,
            options,
            rng: Mutex::new(rng),
            pin: Mutex::new(None),
        }
    }

    pub fn options(&self) -> SelectorOptions {
        self.options
    }

    fn pin_guard(&self) -> MutexGuard<'_, Option<PinnedAccessPoint>> {
        self.pin
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 手动固定（线路诊断页点选）。有效期取清单 `pinTtlHours`。
    pub fn pin(&self, ap_id: &str, now_ms: i64) -> Result<PinnedAccessPoint, PinError> {
        let snapshot = self.directory.snapshot();
        let (_, entry) = snapshot
            .access_point(ap_id)
            .ok_or_else(|| PinError::UnknownAccessPoint(ap_id.to_owned()))?;
        let pinned = PinnedAccessPoint {
            ap_id: entry.id.clone(),
            pinned_at_ms: now_ms,
            expires_at_ms: now_ms + i64::from(snapshot.policy().pin_ttl_hours) * HOUR_MS,
        };
        *self.pin_guard() = Some(pinned.clone());
        Ok(pinned)
    }

    pub fn unpin(&self) {
        *self.pin_guard() = None;
    }

    /// 当前有效的固定（过期即自动清除）。
    pub fn pinned(&self, now_ms: i64) -> Option<PinnedAccessPoint> {
        let mut guard = self.pin_guard();
        if guard
            .as_ref()
            .is_some_and(|pin| now_ms >= pin.expires_at_ms)
        {
            *guard = None;
        }
        guard.clone()
    }

    /// 给某个接入点算协议计划（握手前调用；`select` 对 IM 已附带）。
    pub fn protocol_plan(
        &self,
        entry: &AccessPointEntry,
        policy: &crate::manifest::ClientPolicy,
        net: NetPrint,
        now_ms: i64,
    ) -> ProtocolPlan {
        plan_protocols(
            entry,
            policy,
            self.health.protocol_winner(&entry.id, net),
            self.health.udp_blocked_until(net, now_ms).is_some(),
            self.options.quic_supported,
        )
    }

    /// 取候选。热路径：一次目录加载、一次健康簿加锁、一次随机抽样，分配量与接入点数成正比。
    pub fn select(&self, request: &SelectRequest<'_>) -> Selection {
        let snapshot = self.directory.snapshot();
        let pinned = self.pinned(request.now_ms);
        let unit = self
            .rng
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .next_unit();
        self.select_with_unit(
            snapshot,
            request,
            pinned.as_ref().map(|pin| &*pin.ap_id),
            unit,
        )
    }

    fn select_with_unit(
        &self,
        snapshot: Arc<DirectorySnapshot>,
        request: &SelectRequest<'_>,
        pinned: Option<&str>,
        unit: f64,
    ) -> Selection {
        let entries = snapshot.access_points();
        let views = self.health.views(
            entries.iter().map(|entry| &*entry.id),
            request.net,
            request.now_ms,
        );
        let udp_blocked = self
            .health
            .udp_blocked_until(request.net, request.now_ms)
            .is_some();
        let policy = snapshot.policy();
        let plans: Vec<Option<ProtocolPlan>> = entries
            .iter()
            .map(|entry| {
                (request.service == ServiceClass::Im).then(|| {
                    plan_protocols(
                        entry,
                        policy,
                        self.health.protocol_winner(&entry.id, request.net),
                        udp_blocked,
                        self.options.quic_supported,
                    )
                })
            })
            .collect();
        let inputs: Vec<ApInput<'_>> = entries
            .iter()
            .zip(views.iter())
            .zip(plans.iter())
            .map(|((entry, view), plan)| ApInput {
                id: &entry.id,
                weight: entry.weight,
                priority: entry.priority,
                status: entry.status,
                tags: &entry.tags,
                offers_service: match request.service.http_service() {
                    Some(kind) => entry.endpoints.has(kind),
                    None => plan.as_ref().is_some_and(|plan| !plan.is_empty()),
                },
                excluded: request.exclude.contains(&&*entry.id),
                health: *view,
            })
            .collect();
        let ranking = rank(
            &inputs,
            &RankParams {
                net_tags: request.net_tags,
                current: request.current,
                pinned,
                stickiness: true,
            },
        );
        let chosen = pick(&ranking, &inputs, request.current, unit);

        let mut order: Vec<usize> = (0..ranking.entries.len()).collect();
        if let Some(chosen) = chosen {
            order.remove(chosen);
            order.insert(0, chosen);
        }
        let candidates = order
            .into_iter()
            .map(|position| {
                let ranked = ranking.entries[position];
                Candidate {
                    ap_index: ranked.index,
                    ap_id: entries[ranked.index].id.clone(),
                    score: ranked.score,
                    top_tier: position < ranking.tier_len,
                    half_open: ranked.half_open,
                    protocols: plans[ranked.index].clone(),
                }
            })
            .collect::<Vec<_>>();
        let next_half_open_at_ms = if candidates.is_empty() {
            ranking
                .filtered
                .iter()
                .filter_map(|(index, _)| match inputs[*index].health.breaker {
                    crate::health::BreakerView::Open { until_ms } => Some(until_ms),
                    _ => None,
                })
                .min()
        } else {
            None
        };
        Selection {
            filtered: ranking
                .filtered
                .iter()
                .map(|(index, reason)| (entries[*index].id.clone(), *reason))
                .collect(),
            candidates,
            pin: ranking.pin,
            next_half_open_at_ms,
            snapshot,
        }
    }

    /// 恢复切回判定：找出不带粘性时的最优 IM 接入点，与当前接入点比较。
    pub fn evaluate_failback(&self, request: &FailbackRequest<'_>) -> FailbackVerdict {
        let snapshot = self.directory.snapshot();
        let entries = snapshot.access_points();
        let views = self.health.views(
            entries.iter().map(|entry| &*entry.id),
            request.net,
            request.now_ms,
        );
        let udp_blocked = self
            .health
            .udp_blocked_until(request.net, request.now_ms)
            .is_some();
        let inputs: Vec<ApInput<'_>> = entries
            .iter()
            .zip(views.iter())
            .map(|(entry, view)| ApInput {
                id: &entry.id,
                weight: entry.weight,
                priority: entry.priority,
                status: entry.status,
                tags: &entry.tags,
                offers_service: !plan_protocols(
                    entry,
                    snapshot.policy(),
                    None,
                    udp_blocked,
                    self.options.quic_supported,
                )
                .is_empty(),
                excluded: false,
                health: *view,
            })
            .collect();
        let ranking = rank(
            &inputs,
            &RankParams {
                net_tags: request.net_tags,
                current: Some(request.current),
                pinned: None,
                stickiness: false,
            },
        );
        let Some(best) = ranking.entries.first() else {
            return FailbackVerdict {
                decision: FailbackDecision::Stay(FailbackHold::NoPreferred),
                preferred: None,
            };
        };
        let preferred = entries[best.index].id.clone();
        if &*preferred == request.current {
            return FailbackVerdict {
                decision: FailbackDecision::Stay(FailbackHold::AlreadyPreferred),
                preferred: Some(preferred),
            };
        }
        // 当前接入点自己已不在候选（熔断/下线）属于故障转移，不是切回；这里按「需要离开」处理。
        let current_score = ranking
            .entries
            .iter()
            .find(|entry| inputs[entry.index].id == request.current)
            .map_or(f64::INFINITY, |entry| entry.score);
        let streak = self.health.probe_streak(&preferred, request.net);
        let decision = evaluate_failback(
            &FailbackInput {
                probe_successes: streak.successes,
                probe_span_ms: if streak.successes == 0 {
                    0
                } else {
                    request.now_ms.saturating_sub(streak.since_ms)
                },
                preferred_score: best.score,
                current_score,
                foregrounded: request.foregrounded,
                idle_ms: request.now_ms.saturating_sub(request.last_activity_ms),
                since_last_switch_ms: request.now_ms.saturating_sub(request.last_switch_ms),
            },
            &snapshot.policy().failback,
        );
        FailbackVerdict {
            decision,
            preferred: Some(preferred),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{FailureClass, Observation, Outcome};
    use crate::platform::SequenceRandom;
    use crate::store::ManifestSource;
    use crate::trust::testkit::*;
    use crate::types::Protocol;

    /// 三个接入点：ct（有标签）、cm（有标签）、bgp（无标签，无 QUIC）。
    fn directory() -> Arc<EndpointDirectory> {
        let mut value = manifest_with_serial(1);
        let template = value["regions"][0]["cells"][0]["accessPoints"][0].clone();
        let mut cm = template.clone();
        cm["id"] = "ap-cne1-cm".into();
        cm["host"] = "im-cm.example.com".into();
        cm["tags"] = serde_json::json!(["isp:cm"]);
        let mut bgp = template;
        bgp["id"] = "ap-cne1-bgp".into();
        bgp["host"] = "im-bgp.example.com".into();
        bgp["tags"] = serde_json::json!([]);
        bgp["services"].as_object_mut().unwrap().remove("imQuic");
        let aps = value["regions"][0]["cells"][0]["accessPoints"]
            .as_array_mut()
            .unwrap();
        aps.push(cm);
        aps.push(bgp);
        let verified = trust()
            .verify_access(&signed_manifest(&value), NOW_MS)
            .unwrap();
        Arc::new(EndpointDirectory::new(DirectorySnapshot::from_verified(
            &verified,
            ManifestSource::Seed,
        )))
    }

    fn selector(draws: Vec<f64>) -> (Selector, Arc<HealthBook>) {
        let directory = directory();
        let health = Arc::new(HealthBook::with_random(
            directory.snapshot().policy(),
            Box::new(SequenceRandom::new([0.5; 8])),
        ));
        let selector = Selector::with_random(
            directory,
            health.clone(),
            SelectorOptions {
                quic_supported: true,
            },
            Box::new(SequenceRandom::new(draws)),
        );
        (selector, health)
    }

    fn request<'a>(
        service: ServiceClass,
        tags: &'a [String],
        current: Option<&'a str>,
        exclude: &'a [&'a str],
        now_ms: i64,
    ) -> SelectRequest<'a> {
        SelectRequest {
            service,
            net: NetPrint::UNKNOWN,
            net_tags: tags,
            current,
            exclude,
            now_ms,
        }
    }

    #[test]
    fn im_selection_prefers_tag_and_plans_protocols() {
        let (selector, health) = selector(vec![0.0]);
        let tags = ["isp:cm".to_owned()];
        health.record(
            "ap-cne1-cm",
            NetPrint::UNKNOWN,
            Observation::handshake_ok(Protocol::Ws, 90),
            NOW_MS,
        );
        let selection = selector.select(&request(ServiceClass::Im, &tags, None, &[], NOW_MS));
        let first = selection.first().unwrap();
        assert_eq!(&*first.ap_id, "ap-cne1-cm");
        let plan = first.protocols.as_ref().unwrap();
        // 历史胜者 ws 提前。
        assert_eq!(plan.order, vec![Protocol::Ws, Protocol::Quic]);
        assert!(plan.from_winner);
        assert_eq!(selection.candidates.len(), 3);
        assert_eq!(
            selection.endpoint(first, ServiceKind::ImWs),
            Some("wss://im-cm.example.com/ws")
        );
        let bgp = selection
            .candidates
            .iter()
            .find(|c| &*c.ap_id == "ap-cne1-bgp")
            .unwrap();
        assert_eq!(bgp.protocols.as_ref().unwrap().order, vec![Protocol::Ws]);
    }

    #[test]
    fn rest_follows_current_and_excludes_tried() {
        let (selector, _) = selector(vec![0.99, 0.99]);
        let selection = selector.select(&request(
            ServiceClass::CoreApi,
            &[],
            Some("ap-cne1-bgp"),
            &[],
            NOW_MS,
        ));
        assert_eq!(&*selection.first().unwrap().ap_id, "ap-cne1-bgp");
        assert_eq!(
            selection.http_base(selection.first().unwrap(), ServiceClass::CoreApi),
            Some("https://im-bgp.example.com/api")
        );
        let exclude = ["ap-cne1-bgp"];
        let selection = selector.select(&request(
            ServiceClass::CoreApi,
            &[],
            Some("ap-cne1-bgp"),
            &exclude,
            NOW_MS,
        ));
        assert!(
            selection
                .candidates
                .iter()
                .all(|c| &*c.ap_id != "ap-cne1-bgp")
        );
        assert_eq!(
            selection.filtered,
            vec![(ApId::from("ap-cne1-bgp"), FilterReason::Excluded)]
        );
    }

    #[test]
    fn all_open_reports_next_half_open() {
        let (selector, health) = selector(vec![0.5]);
        let fail = Observation::handshake_failed(Protocol::Ws, FailureClass::Unreachable);
        for ap in ["ap-cne1-ct", "ap-cne1-cm", "ap-cne1-bgp"] {
            for step in 0..3 {
                health.record(ap, NetPrint::UNKNOWN, fail, NOW_MS + step);
            }
        }
        let selection = selector.select(&request(ServiceClass::Im, &[], None, &[], NOW_MS + 10));
        assert!(selection.is_empty());
        // 冷却 30s × 1.0（抽样 0.5），最早的一个在 NOW+2 打开。
        assert_eq!(selection.next_half_open_at_ms, Some(NOW_MS + 2 + 30_000));
        let later = selector.select(&request(
            ServiceClass::Im,
            &[],
            None,
            &[],
            NOW_MS + 2 + 30_000,
        ));
        assert!(later.first().unwrap().half_open);
    }

    #[test]
    fn pin_applies_then_expires_and_falls_back_when_open() {
        let (selector, health) = selector(vec![0.0; 4]);
        assert!(selector.pin("nope", NOW_MS).is_err());
        let pinned = selector.pin("ap-cne1-bgp", NOW_MS).unwrap();
        assert_eq!(pinned.expires_at_ms, NOW_MS + 24 * HOUR_MS);
        let selection = selector.select(&request(ServiceClass::Im, &[], None, &[], NOW_MS));
        assert_eq!(selection.pin, PinOutcome::Applied);
        assert_eq!(selection.candidates.len(), 1);
        let fail = Observation::handshake_failed(Protocol::Ws, FailureClass::TlsFailed);
        for step in 0..3 {
            health.record("ap-cne1-bgp", NetPrint::UNKNOWN, fail, NOW_MS + step);
        }
        let selection = selector.select(&request(ServiceClass::Im, &[], None, &[], NOW_MS + 5));
        assert_eq!(selection.pin, PinOutcome::Unavailable);
        assert_eq!(selection.candidates.len(), 2);
        assert!(selector.pinned(NOW_MS + 24 * HOUR_MS).is_none());
    }

    #[test]
    fn failback_needs_probe_streak_and_better_score() {
        let (selector, health) = selector(vec![]);
        let net = NetPrint::UNKNOWN;
        // 当前在 bgp（RTT 200），首选 ct（RTT 40）。
        health.record(
            "ap-cne1-bgp",
            net,
            Observation::handshake_ok(Protocol::Ws, 200),
            NOW_MS,
        );
        health.record(
            "ap-cne1-cm",
            net,
            Observation::handshake_ok(Protocol::Ws, 150),
            NOW_MS,
        );
        let probe_ok = Observation::probe(Outcome::Success { rtt_ms: Some(40) });
        health.record("ap-cne1-ct", net, probe_ok, NOW_MS);
        // 本机在电信网络：ct 是标签匹配的首选。
        let tags = ["isp:ct".to_owned()];
        let base = FailbackRequest {
            net,
            net_tags: &tags,
            current: "ap-cne1-bgp",
            foregrounded: false,
            last_activity_ms: NOW_MS,
            last_switch_ms: NOW_MS,
            now_ms: NOW_MS + 60_000,
        };
        let verdict = selector.evaluate_failback(&base);
        assert_eq!(verdict.preferred.as_deref(), Some("ap-cne1-ct"));
        assert_eq!(
            verdict.decision,
            FailbackDecision::Stay(FailbackHold::NotEnoughProbes)
        );
        health.record("ap-cne1-ct", net, probe_ok, NOW_MS + 150_000);
        health.record("ap-cne1-ct", net, probe_ok, NOW_MS + 300_000);
        let at = NOW_MS + 600_000;
        let verdict = selector.evaluate_failback(&FailbackRequest { now_ms: at, ..base });
        assert_eq!(verdict.decision, FailbackDecision::Switch);
        let verdict = selector.evaluate_failback(&FailbackRequest {
            current: "ap-cne1-ct",
            now_ms: at,
            ..base
        });
        assert_eq!(
            verdict.decision,
            FailbackDecision::Stay(FailbackHold::AlreadyPreferred)
        );
    }
}
