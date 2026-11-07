//! 候选过滤、打分与分层随机（§3.3）。纯函数：输入是接入点属性与健康视图，输出是排序与选择，
//! 不读时钟、不持锁，黄金向量直接驱动这一层。
//!
//! ```text
//! score(ap) = rtt_ewma_ms × (1 + 3 × failure_ratio) + 50 × priority − stickiness_bonus
//! 无 RTT 样本时取同 Cell 已知样本的中位数，再无则 300ms；当前在用的接入点减 30ms。
//! 同档：score ≤ s_min + 0.2 × max(s_min, 0) + 20ms，档内按清单 weight 加权随机。
//! ```
//!
//! 分层随机让客户端既选到近的入口，又不会全部挤向同一个「最快」的入口（单样本最低延迟排序的羊群效应）。

use crate::health::{BreakerView, HealthView};
use crate::platform::clamp_unit;
use crate::types::ApStatus;

pub const UNKNOWN_RTT_MS: f64 = 300.0;
pub const FAILURE_PENALTY: f64 = 3.0;
pub const PRIORITY_STEP_MS: f64 = 50.0;
pub const STICKINESS_BONUS_MS: f64 = 30.0;
pub const TIER_RATIO: f64 = 0.2;
pub const TIER_SLACK_MS: f64 = 20.0;
/// 浮点比较容差，避免 `s_min × 1.2 + 20` 这类边界值因舍入落到档外。
const EPSILON: f64 = 1e-9;

/// 一个接入点的打分输入。
#[derive(Debug, Clone)]
pub struct ApInput<'a> {
    pub id: &'a str,
    pub weight: u32,
    pub priority: u32,
    pub status: ApStatus,
    pub tags: &'a [String],
    /// 提供所需服务（IM 还要求协议计划非空）。
    pub offers_service: bool,
    /// 调用方本轮已尝试过、要跳过的。
    pub excluded: bool,
    pub health: HealthView,
}

pub struct RankParams<'a> {
    /// 本机网络特征标签（如 `isp:ct`、`net:ipv6`、`udp:ok`）。
    pub net_tags: &'a [String],
    /// 当前在用的接入点。
    pub current: Option<&'a str>,
    /// 用户手动固定的接入点。
    pub pinned: Option<&'a str>,
    /// 是否给当前接入点粘性加成。恢复切回比较「首选 vs 当前」时关闭，避免加成掩盖真实差距。
    pub stickiness: bool,
}

/// 标签分组：匹配本机网络标签的排最前，其次是无标签的通用入口，最后是为其它网络准备的入口。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TagGroup {
    Matched,
    Untagged,
    Other,
}

impl TagGroup {
    pub fn as_str(self) -> &'static str {
        match self {
            TagGroup::Matched => "matched",
            TagGroup::Untagged => "untagged",
            TagGroup::Other => "other",
        }
    }
}

/// 未进入候选的原因（诊断与向量使用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterReason {
    NotActive,
    NoService,
    Excluded,
    BreakerOpen,
    /// half-open 且已有试探在途。
    TrialInFlight,
    /// 手动固定生效，其它接入点让位。
    NotPinned,
}

impl FilterReason {
    pub fn as_str(self) -> &'static str {
        match self {
            FilterReason::NotActive => "notActive",
            FilterReason::NoService => "noService",
            FilterReason::Excluded => "excluded",
            FilterReason::BreakerOpen => "breakerOpen",
            FilterReason::TrialInFlight => "trialInFlight",
            FilterReason::NotPinned => "notPinned",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinOutcome {
    /// 没有手动固定。
    None,
    /// 固定的接入点可用，只用它。
    Applied,
    /// 固定的接入点不可用（熔断、下线、已尝试），临时回退自动选路；UI 应提示「所选线路不可用」。
    Unavailable,
}

impl PinOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            PinOutcome::None => "none",
            PinOutcome::Applied => "applied",
            PinOutcome::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RankedEntry {
    /// 在输入切片中的下标。
    pub index: usize,
    pub score: f64,
    pub group: TagGroup,
    pub half_open: bool,
    /// 参与打分的 RTT（已知样本或回退值）。
    pub rtt_ms: f64,
    pub rtt_known: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ranking {
    /// 按（标签分组, 分数, id）升序。
    pub entries: Vec<RankedEntry>,
    /// `entries` 前多少个属于最优档。
    pub tier_len: usize,
    pub filtered: Vec<(usize, FilterReason)>,
    pub pin: PinOutcome,
    /// 无样本接入点使用的 RTT。
    pub fallback_rtt_ms: f64,
}

impl Ranking {
    pub fn tier(&self) -> &[RankedEntry] {
        &self.entries[..self.tier_len]
    }
}

fn filter_reason(ap: &ApInput<'_>) -> Option<FilterReason> {
    if ap.status != ApStatus::Active {
        Some(FilterReason::NotActive)
    } else if !ap.offers_service {
        Some(FilterReason::NoService)
    } else if ap.excluded {
        Some(FilterReason::Excluded)
    } else {
        match ap.health.breaker {
            BreakerView::Open { .. } => Some(FilterReason::BreakerOpen),
            BreakerView::HalfOpen {
                trial_available: false,
            } => Some(FilterReason::TrialInFlight),
            _ => None,
        }
    }
}

/// 同 Cell 已知 RTT 的中位数（偶数个取中间两个的平均），没有样本时 300ms。
pub fn fallback_rtt(aps: &[ApInput<'_>]) -> f64 {
    let mut known: Vec<f64> = aps.iter().filter_map(|ap| ap.health.rtt_ms).collect();
    if known.is_empty() {
        return UNKNOWN_RTT_MS;
    }
    known.sort_by(f64::total_cmp);
    let middle = known.len() / 2;
    if known.len().is_multiple_of(2) {
        (known[middle - 1] + known[middle]) / 2.0
    } else {
        known[middle]
    }
}

pub fn score(
    rtt_ms: f64,
    failure_ratio: f64,
    priority: u32,
    is_current: bool,
    stickiness: bool,
) -> f64 {
    let bonus = if is_current && stickiness {
        STICKINESS_BONUS_MS
    } else {
        0.0
    };
    rtt_ms * (1.0 + FAILURE_PENALTY * failure_ratio) + PRIORITY_STEP_MS * f64::from(priority)
        - bonus
}

/// 档位阈值。`s_min` 为负（极低 RTT 加粘性）时不再乘系数，避免档位反而收窄。
pub fn tier_threshold(s_min: f64) -> f64 {
    s_min + TIER_RATIO * s_min.max(0.0) + TIER_SLACK_MS
}

pub fn rank(aps: &[ApInput<'_>], params: &RankParams<'_>) -> Ranking {
    let fallback_rtt_ms = fallback_rtt(aps);
    let mut filtered = Vec::new();
    let mut pin = PinOutcome::None;
    let mut pinned_index = None;
    if let Some(pinned) = params.pinned {
        match aps.iter().position(|ap| ap.id == pinned) {
            Some(index) if filter_reason(&aps[index]).is_none() => {
                pin = PinOutcome::Applied;
                pinned_index = Some(index);
            }
            _ => pin = PinOutcome::Unavailable,
        }
    }

    let mut entries = Vec::with_capacity(aps.len());
    for (index, ap) in aps.iter().enumerate() {
        if let Some(reason) = filter_reason(ap) {
            filtered.push((index, reason));
            continue;
        }
        if pinned_index.is_some_and(|pinned| pinned != index) {
            filtered.push((index, FilterReason::NotPinned));
            continue;
        }
        let group = if ap.tags.iter().any(|tag| params.net_tags.contains(tag)) {
            TagGroup::Matched
        } else if ap.tags.is_empty() {
            TagGroup::Untagged
        } else {
            TagGroup::Other
        };
        let rtt_known = ap.health.rtt_ms.is_some();
        let rtt_ms = ap.health.rtt_ms.unwrap_or(fallback_rtt_ms);
        entries.push(RankedEntry {
            index,
            score: score(
                rtt_ms,
                ap.health.failure_ratio,
                ap.priority,
                params.current == Some(ap.id),
                params.stickiness,
            ),
            group,
            half_open: matches!(ap.health.breaker, BreakerView::HalfOpen { .. }),
            rtt_ms,
            rtt_known,
        });
    }
    entries.sort_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then(left.score.total_cmp(&right.score))
            .then_with(|| aps[left.index].id.cmp(aps[right.index].id))
    });

    let tier_len = match entries.first() {
        None => 0,
        Some(best) => {
            let threshold = tier_threshold(best.score);
            entries
                .iter()
                .take_while(|entry| entry.group == best.group && entry.score <= threshold + EPSILON)
                .count()
        }
    };
    Ranking {
        entries,
        tier_len,
        filtered,
        pin,
        fallback_rtt_ms,
    }
}

/// 在最优档内选一个，返回它在 `ranking.entries` 中的位置。
///
/// 粘性：当前接入点仍在最优档内就继续用它（IM 与 REST 留在同一个入口，也避免无谓切换）。
/// 否则按 weight 加权随机；档内权重全为 0 时均匀随机。`unit` 为 `[0, 1)` 的随机数。
pub fn pick(
    ranking: &Ranking,
    aps: &[ApInput<'_>],
    current: Option<&str>,
    unit: f64,
) -> Option<usize> {
    let tier = ranking.tier();
    if tier.is_empty() {
        return None;
    }
    if let Some(current) = current
        && let Some(position) = tier.iter().position(|entry| aps[entry.index].id == current)
    {
        return Some(position);
    }
    let unit = clamp_unit(unit);
    let total: u64 = tier
        .iter()
        .map(|entry| u64::from(aps[entry.index].weight))
        .sum();
    if total == 0 {
        return Some(((unit * tier.len() as f64) as usize).min(tier.len() - 1));
    }
    let target = unit * total as f64;
    let mut cumulative = 0u64;
    for (position, entry) in tier.iter().enumerate() {
        cumulative += u64::from(aps[entry.index].weight);
        if target < cumulative as f64 {
            return Some(position);
        }
    }
    Some(tier.len() - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input<'a>(
        id: &'a str,
        rtt: Option<f64>,
        ratio: f64,
        priority: u32,
        weight: u32,
    ) -> ApInput<'a> {
        ApInput {
            id,
            weight,
            priority,
            status: ApStatus::Active,
            tags: &[],
            offers_service: true,
            excluded: false,
            health: HealthView {
                rtt_ms: rtt,
                failure_ratio: ratio,
                breaker: BreakerView::Closed,
            },
        }
    }

    fn params<'a>(current: Option<&'a str>) -> RankParams<'a> {
        RankParams {
            net_tags: &[],
            current,
            pinned: None,
            stickiness: true,
        }
    }

    #[test]
    fn score_formula_and_tiering() {
        let aps = [
            input("a", Some(40.0), 0.0, 0, 100),
            // 60 × (1 + 3 × 0.1) = 78
            input("b", Some(60.0), 0.1, 0, 100),
            // 无样本 → 中位数 (40+60+100)/3 的中位 = 60；+50 优先级 = 110
            input("c", None, 0.0, 1, 100),
            input("d", Some(100.0), 0.0, 0, 100),
        ];
        let ranking = rank(&aps, &params(None));
        let order: Vec<&str> = ranking.entries.iter().map(|e| aps[e.index].id).collect();
        assert_eq!(order, ["a", "b", "d", "c"]);
        assert_eq!(ranking.fallback_rtt_ms, 60.0);
        assert!((ranking.entries[1].score - 78.0).abs() < 1e-9);
        assert_eq!(ranking.entries[3].score, 110.0);
        // 阈值 40 × 1.2 + 20 = 68：只有 a。
        assert_eq!(ranking.tier_len, 1);

        // 当前在用 b：78 − 30 = 48，落进 a 的档（阈值 68）。
        let ranking = rank(&aps, &params(Some("b")));
        assert_eq!(ranking.tier_len, 2);
        assert_eq!(aps[ranking.entries[0].index].id, "a");
        // 粘性：当前在档内就选当前。
        let chosen = pick(&ranking, &aps, Some("b"), 0.0).unwrap();
        assert_eq!(aps[ranking.entries[chosen].index].id, "b");
    }

    #[test]
    fn weighted_pick_within_tier() {
        let aps = [
            input("a", Some(50.0), 0.0, 0, 100),
            input("b", Some(55.0), 0.0, 0, 300),
            input("c", Some(500.0), 0.0, 0, 100),
        ];
        let ranking = rank(&aps, &params(None));
        assert_eq!(ranking.tier_len, 2);
        let id_at =
            |unit: f64| aps[ranking.entries[pick(&ranking, &aps, None, unit).unwrap()].index].id;
        assert_eq!(id_at(0.0), "a");
        assert_eq!(id_at(0.249), "a");
        assert_eq!(id_at(0.25), "b");
        assert_eq!(id_at(0.999), "b");
    }

    #[test]
    fn filters_open_inactive_and_busy_trials() {
        let mut aps = vec![
            input("a", Some(10.0), 0.0, 0, 100),
            input("b", Some(20.0), 0.0, 0, 100),
            input("c", Some(30.0), 0.0, 0, 100),
            input("d", Some(40.0), 0.0, 0, 100),
        ];
        aps[0].health.breaker = BreakerView::Open { until_ms: 99 };
        aps[1].status = ApStatus::Draining;
        aps[2].health.breaker = BreakerView::HalfOpen {
            trial_available: false,
        };
        let ranking = rank(&aps, &params(None));
        assert_eq!(ranking.entries.len(), 1);
        assert_eq!(
            ranking.filtered,
            vec![
                (0, FilterReason::BreakerOpen),
                (1, FilterReason::NotActive),
                (2, FilterReason::TrialInFlight),
            ]
        );
    }

    #[test]
    fn tags_and_pins() {
        let matched = ["isp:ct".to_owned()];
        let other = ["isp:cm".to_owned()];
        let mut aps = vec![
            input("fast-other", Some(10.0), 0.0, 0, 100),
            input("slow-matched", Some(200.0), 0.0, 0, 100),
            input("plain", Some(50.0), 0.0, 0, 100),
        ];
        aps[0].tags = &other;
        aps[1].tags = &matched;
        let net_tags = ["isp:ct".to_owned()];
        let ranking = rank(
            &aps,
            &RankParams {
                net_tags: &net_tags,
                current: None,
                pinned: None,
                stickiness: true,
            },
        );
        let order: Vec<&str> = ranking.entries.iter().map(|e| aps[e.index].id).collect();
        assert_eq!(order, ["slow-matched", "plain", "fast-other"]);

        let ranking = rank(
            &aps,
            &RankParams {
                net_tags: &[],
                current: None,
                pinned: Some("plain"),
                stickiness: true,
            },
        );
        assert_eq!(ranking.pin, PinOutcome::Applied);
        assert_eq!(ranking.entries.len(), 1);
        aps[2].health.breaker = BreakerView::Open { until_ms: 5 };
        let ranking = rank(
            &aps,
            &RankParams {
                net_tags: &[],
                current: None,
                pinned: Some("plain"),
                stickiness: true,
            },
        );
        assert_eq!(ranking.pin, PinOutcome::Unavailable);
        assert_eq!(ranking.entries.len(), 2);
    }
}
