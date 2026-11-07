//! 探活：`GET https://{host}/.well-known/flare/ping` → `204`，响应头 `x-flare-ap`、`x-flare-cell`
//! 必须与清单一致。任何一项不满足都记为失败，从而排除强制门户、运营商劫持页与 DNS 污染。
//!
//! TLS 校验（含 SPKI 钉扎、`ipHints` 直连时按 host 校验证书）由引擎注入的 [`ProbeTransport`] 负责；
//! 这里只做与平台无关的部分：合法性判定、结果分类、预算与探测计划。

mod budget;

use std::sync::{Arc, Mutex};

pub use budget::{MAX_BUDGET_PER_HOUR, ProbeBudget, ProbeTrigger};

use crate::directory::{AccessPointEntry, DirectorySnapshot};
use crate::health::{FailureClass, HealthBook, NetPrint, NeutralReason, Observation, Outcome};
use crate::platform::MaybeSendSync;
use crate::select::Selection;
use crate::types::{ApId, ServiceKind};

pub const HEADER_AP: &str = "x-flare-ap";
pub const HEADER_CELL: &str = "x-flare-cell";
/// 冷启动与故障切换时一轮最多探几个接入点。
pub const MAX_PROBES_PER_ROUND: usize = 3;
/// 故障切换探测的交错间隔。
pub const FAILOVER_PROBE_STAGGER_MS: u32 = 250;
/// 单次探活超时。
pub const PROBE_TIMEOUT_MS: u32 = 4_000;

/// 交给传输实现的请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeRequest {
    pub ap_id: ApId,
    /// 完整 URL（`https://{host}{ping.path}`）。
    pub url: String,
    /// SNI 与证书校验用的主机名。
    pub host: String,
    /// 原生端 DNS 失败或结果可疑时可直连的地址。
    pub ip_hints: Vec<std::net::IpAddr>,
    /// 非空时证书链中必须有匹配的 SPKI SHA-256。
    pub spki_pins: Vec<String>,
    pub timeout_ms: u32,
}

/// 传输实现返回的响应摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResponse {
    pub status: u16,
    /// `x-flare-ap` 响应头。
    pub ap_header: Option<String>,
    /// `x-flare-cell` 响应头。
    pub cell_header: Option<String>,
    pub elapsed_ms: u32,
}

/// 传输层失败（请求没有拿到 HTTP 响应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProbeTransportError {
    #[error("device has no network")]
    NoNetwork,
    #[error("host unreachable")]
    Unreachable,
    #[error("TLS or certificate pin verification failed")]
    Tls,
    #[error("probe timed out")]
    Timeout,
    #[error("probe was cancelled")]
    Cancelled,
}

/// 引擎注入的探活传输：发一个 GET，按 `ProbeRequest` 做 TLS 校验，返回状态码与两个响应头。
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait ProbeTransport: MaybeSendSync {
    async fn get(&self, request: &ProbeRequest) -> Result<ProbeResponse, ProbeTransportError>;
}

/// 探活响应不合法的原因。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProbeInvalid {
    #[error("expected 204, got {0}")]
    Status(u16),
    #[error("x-flare-ap is missing")]
    MissingAp,
    #[error("x-flare-ap {0:?} does not match the manifest")]
    ApMismatch(String),
    #[error("x-flare-cell is missing")]
    MissingCell,
    #[error("x-flare-cell {0:?} does not match the manifest")]
    CellMismatch(String),
}

/// 合法性判定：204 且两个头与清单一致（去掉首尾空白后精确比较，ID 区分大小写）。
pub fn validate_response(
    ap: &AccessPointEntry,
    response: &ProbeResponse,
) -> Result<(), ProbeInvalid> {
    if response.status != 204 {
        return Err(ProbeInvalid::Status(response.status));
    }
    match response.ap_header.as_deref().map(str::trim) {
        None | Some("") => return Err(ProbeInvalid::MissingAp),
        Some(value) if value != &*ap.id => return Err(ProbeInvalid::ApMismatch(value.to_owned())),
        Some(_) => {}
    }
    match response.cell_header.as_deref().map(str::trim) {
        None | Some("") => Err(ProbeInvalid::MissingCell),
        Some(value) if value != &*ap.cell_id => Err(ProbeInvalid::CellMismatch(value.to_owned())),
        Some(_) => Ok(()),
    }
}

/// 把一次探活结果分类成健康簿可记录的结果。无网络与取消不计失败。
pub fn classify(
    ap: &AccessPointEntry,
    result: &Result<ProbeResponse, ProbeTransportError>,
) -> Outcome {
    match result {
        Ok(response) => match validate_response(ap, response) {
            Ok(()) => Outcome::Success {
                rtt_ms: Some(response.elapsed_ms),
            },
            Err(_) => Outcome::Failure(FailureClass::ProbeInvalid),
        },
        Err(ProbeTransportError::NoNetwork) => Outcome::Neutral(NeutralReason::NoNetwork),
        Err(ProbeTransportError::Cancelled) => Outcome::Neutral(NeutralReason::Cancelled),
        Err(ProbeTransportError::Unreachable) => Outcome::Failure(FailureClass::Unreachable),
        Err(ProbeTransportError::Tls) => Outcome::Failure(FailureClass::TlsFailed),
        Err(ProbeTransportError::Timeout) => Outcome::Failure(FailureClass::HandshakeTimeout),
    }
}

/// 一次探活的结局。
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeReport {
    /// 已探测并回写健康簿。
    Done {
        outcome: Outcome,
        invalid: Option<ProbeInvalid>,
    },
    /// 预算用尽，未探测。
    BudgetExhausted,
    /// 该接入点没有 ping 入口（例如开发模式），未探测。
    NoPingEndpoint,
}

/// 计划中的一次探测：对哪个候选、相对本轮开始延迟多久发起。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedProbe {
    pub ap_index: usize,
    pub delay_ms: u32,
}

/// 按触发原因给出一轮探测计划：冷启动最多 3 个并行，故障切换最多 3 个每 250ms 交错，
/// 其它触发只探第一个候选。没有 ping 入口的候选跳过。
pub fn plan_round(selection: &Selection, trigger: ProbeTrigger) -> Vec<PlannedProbe> {
    let (limit, stagger) = match trigger {
        ProbeTrigger::ColdStart => (MAX_PROBES_PER_ROUND, 0),
        ProbeTrigger::Failover => (MAX_PROBES_PER_ROUND, FAILOVER_PROBE_STAGGER_MS),
        ProbeTrigger::Background | ProbeTrigger::Failback | ProbeTrigger::UserInitiated => (1, 0),
    };
    selection
        .candidates
        .iter()
        .filter(|candidate| selection.endpoint(candidate, ServiceKind::Ping).is_some())
        .take(limit)
        .enumerate()
        .map(|(order, candidate)| PlannedProbe {
            ap_index: candidate.ap_index,
            delay_ms: stagger * order as u32,
        })
        .collect()
}

/// 探活执行器：预算 → 传输 → 合法性 → 回写健康簿。
pub struct Prober {
    transport: Arc<dyn ProbeTransport>,
    health: Arc<HealthBook>,
    budget: Mutex<ProbeBudget>,
}

impl Prober {
    pub fn new(transport: Arc<dyn ProbeTransport>, health: Arc<HealthBook>) -> Self {
        Self {
            transport,
            health,
            budget: Mutex::new(ProbeBudget::new()),
        }
    }

    /// 本小时剩余预算。
    pub fn remaining_budget(&self, snapshot: &DirectorySnapshot, now_ms: i64) -> u32 {
        self.budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remaining(now_ms, snapshot.policy().probe.budget_per_hour)
    }

    /// 探一个接入点。`now_ms` 为发起时刻，结果按 `now_ms + elapsed` 回写。
    pub async fn probe(
        &self,
        snapshot: &DirectorySnapshot,
        ap_index: usize,
        net: NetPrint,
        trigger: ProbeTrigger,
        now_ms: i64,
    ) -> ProbeReport {
        let Some(ap) = snapshot.access_points().get(ap_index) else {
            return ProbeReport::NoPingEndpoint;
        };
        let Some(url) = ap.endpoint(ServiceKind::Ping) else {
            return ProbeReport::NoPingEndpoint;
        };
        let allowed = self
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .try_consume(now_ms, snapshot.policy().probe.budget_per_hour, trigger);
        if !allowed {
            return ProbeReport::BudgetExhausted;
        }
        let request = ProbeRequest {
            ap_id: ap.id.clone(),
            url: url.to_owned(),
            host: ap.host.clone(),
            ip_hints: ap.ip_hints.clone(),
            spki_pins: ap.spki_pins.clone(),
            timeout_ms: PROBE_TIMEOUT_MS,
        };
        let result = self.transport.get(&request).await;
        let outcome = classify(ap, &result);
        let invalid = result
            .as_ref()
            .ok()
            .and_then(|response| validate_response(ap, response).err());
        let elapsed = result.as_ref().map_or(0, |response| response.elapsed_ms);
        self.health.record(
            &ap.id,
            net,
            Observation::probe(outcome),
            now_ms + i64::from(elapsed),
        );
        ProbeReport::Done { outcome, invalid }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::EndpointDirectory;
    use crate::health::BreakerView;
    use crate::manifest::ClientPolicy;
    use crate::select::{SelectRequest, Selector, SelectorOptions};
    use crate::store::ManifestSource;
    use crate::trust::testkit::*;
    use crate::types::ServiceClass;

    struct FixedTransport(Mutex<Vec<Result<ProbeResponse, ProbeTransportError>>>);

    #[async_trait::async_trait]
    impl ProbeTransport for FixedTransport {
        async fn get(&self, _request: &ProbeRequest) -> Result<ProbeResponse, ProbeTransportError> {
            self.0.lock().unwrap().remove(0)
        }
    }

    fn ok(ap: &str, cell: &str) -> Result<ProbeResponse, ProbeTransportError> {
        Ok(ProbeResponse {
            status: 204,
            ap_header: Some(ap.into()),
            cell_header: Some(cell.into()),
            elapsed_ms: 35,
        })
    }

    fn snapshot() -> Arc<DirectorySnapshot> {
        let verified = trust()
            .verify_access(&signed_manifest(&manifest_with_serial(1)), NOW_MS)
            .unwrap();
        Arc::new(DirectorySnapshot::from_verified(
            &verified,
            ManifestSource::Seed,
        ))
    }

    #[test]
    fn validation_rejects_portals_and_mismatches() {
        let snap = snapshot();
        let ap = &snap.access_points()[0];
        let valid = ok("ap-cne1-ct", "cell-cne-1").unwrap();
        assert_eq!(validate_response(ap, &valid), Ok(()));
        let portal = ProbeResponse {
            status: 200,
            ..valid.clone()
        };
        assert_eq!(
            validate_response(ap, &portal),
            Err(ProbeInvalid::Status(200))
        );
        let hijack = ProbeResponse {
            ap_header: Some("ap-evil".into()),
            ..valid.clone()
        };
        assert_eq!(
            validate_response(ap, &hijack),
            Err(ProbeInvalid::ApMismatch("ap-evil".into()))
        );
        let no_cell = ProbeResponse {
            cell_header: None,
            ..valid.clone()
        };
        assert_eq!(
            validate_response(ap, &no_cell),
            Err(ProbeInvalid::MissingCell)
        );
        assert_eq!(
            classify(ap, &Err(ProbeTransportError::NoNetwork)),
            Outcome::Neutral(NeutralReason::NoNetwork)
        );
        assert_eq!(
            classify(ap, &Ok(hijack)),
            Outcome::Failure(FailureClass::ProbeInvalid)
        );
    }

    #[tokio::test]
    async fn probe_records_and_respects_budget() {
        let snap = snapshot();
        let health = Arc::new(HealthBook::new(&ClientPolicy::default()));
        let transport = Arc::new(FixedTransport(Mutex::new(vec![
            ok("ap-cne1-ct", "cell-cne-1"),
            ok("wrong", "cell-cne-1"),
            ok("wrong", "cell-cne-1"),
            ok("wrong", "cell-cne-1"),
        ])));
        let prober = Prober::new(transport, health.clone());
        let net = NetPrint::UNKNOWN;
        let report = prober
            .probe(&snap, 0, net, ProbeTrigger::ColdStart, NOW_MS)
            .await;
        assert!(matches!(
            report,
            ProbeReport::Done {
                outcome: Outcome::Success { .. },
                invalid: None
            }
        ));
        assert_eq!(
            health.view("ap-cne1-ct", net, NOW_MS + 35).rtt_ms,
            Some(35.0)
        );
        for step in 1..=3 {
            prober
                .probe(&snap, 0, net, ProbeTrigger::Failover, NOW_MS + step)
                .await;
        }
        // 连续 3 次不合法 → 熔断。
        assert!(matches!(
            health.view("ap-cne1-ct", net, NOW_MS + 100).breaker,
            BreakerView::Open { .. }
        ));
        assert_eq!(prober.remaining_budget(&snap, NOW_MS + 100), 26);
    }

    #[test]
    fn plans_rounds_by_trigger() {
        let snap = snapshot();
        let directory = Arc::new(EndpointDirectory::new((*snap).clone()));
        let health = Arc::new(HealthBook::new(snap.policy()));
        let selector = Selector::new(
            directory,
            health,
            SelectorOptions {
                quic_supported: true,
            },
        );
        let selection = selector.select(&SelectRequest {
            service: ServiceClass::Im,
            net: NetPrint::UNKNOWN,
            net_tags: &[],
            current: None,
            exclude: &[],
            now_ms: NOW_MS,
        });
        assert_eq!(
            plan_round(&selection, ProbeTrigger::Failover),
            vec![PlannedProbe {
                ap_index: 0,
                delay_ms: 0
            }]
        );
    }
}
