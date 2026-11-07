//! 客户端链路参数（清单 `clientPolicy`）：默认值与硬上下限。
//!
//! 解析是宽松的：缺字段、类型不对都取默认值，越界取边界值，不会因为一个字段让整份清单失效。
//! 硬上下限是安全边界——一份错误（或被盗签的）清单最多把参数推到边界，
//! 不能把全网客户端变成对入口或镜像的攻击流量。

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::Protocol;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClientPolicy {
    /// 无历史胜者时的协议顺序。
    pub protocol_order: Vec<Protocol>,
    /// 交错竞速间隔。
    pub race_stagger_ms: u32,
    /// 单次握手（含 CONNECT_ACK）超时。
    pub connect_timeout_ms: u32,
    pub heartbeat: HeartbeatPolicy,
    /// 一次即时验活（回前台、网络变化、发送迟迟没有回执）等 PONG 的窗口，窗口内没有 PONG 即判死。
    /// 发送停滞 1.5 s 才验活，所以有发送时发现半开连接的上限约是它 + 1.5 s。
    pub probe_wake_timeout_ms: u32,
    pub breaker: BreakerPolicy,
    pub backoff: BackoffPolicy,
    pub probe: ProbePolicy,
    pub failback: FailbackPolicy,
    /// UDP 封锁记忆时长。
    pub udp_blocked_ttl_hours: u32,
    /// 手动固定接入点的有效期。
    pub pin_ttl_hours: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatPolicy {
    pub interval_sec: u32,
    pub timeout_sec: u32,
    pub background_interval_sec: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BreakerPolicy {
    /// 连续失败次数阈值。
    pub failure_threshold: u32,
    /// 近 20 次失败率阈值。
    pub failure_ratio: f64,
    pub cooldown_base_sec: u32,
    pub cooldown_max_sec: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackoffPolicy {
    pub base_ms: u32,
    pub cap_foreground_ms: u32,
    pub cap_background_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProbePolicy {
    /// 每设备每小时主动探活次数上限。
    pub budget_per_hour: u32,
    /// Wi-Fi 下后台复评的最小间隔。
    pub background_interval_min: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailbackPolicy {
    /// 首选接入点连续探活成功需要覆盖的最短时间跨度。
    pub min_healthy_span_sec: u32,
    /// 首选接入点评分需要比当前好多少（0.2 = 好 20%）。
    pub min_improvement_ratio: f64,
    /// 两次切换之间的最短间隔。
    pub min_interval_sec: u32,
}

/// 一个数值参数的（下限, 默认, 上限）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bound {
    pub min: f64,
    pub default: f64,
    pub max: f64,
}

const fn bound(min: f64, default: f64, max: f64) -> Bound {
    Bound { min, default, max }
}

/// 参数表：清单路径 → 上下限。导出供文档与诊断展示，也是向量的依据。
pub const POLICY_BOUNDS: &[(&str, Bound)] = &[
    ("raceStaggerMs", bound(100.0, 300.0, 3_000.0)),
    ("connectTimeoutMs", bound(2_000.0, 8_000.0, 30_000.0)),
    ("heartbeat.intervalSec", bound(10.0, 30.0, 300.0)),
    ("heartbeat.timeoutSec", bound(20.0, 90.0, 900.0)),
    ("heartbeat.backgroundIntervalSec", bound(30.0, 120.0, 900.0)),
    ("probeWakeTimeoutMs", bound(2_000.0, 5_000.0, 30_000.0)),
    ("breaker.failureThreshold", bound(2.0, 3.0, 20.0)),
    ("breaker.failureRatio", bound(0.2, 0.5, 1.0)),
    ("breaker.cooldownBaseSec", bound(5.0, 30.0, 300.0)),
    ("breaker.cooldownMaxSec", bound(60.0, 600.0, 3_600.0)),
    ("backoff.baseMs", bound(500.0, 1_000.0, 10_000.0)),
    (
        "backoff.capForegroundMs",
        bound(5_000.0, 60_000.0, 600_000.0),
    ),
    (
        "backoff.capBackgroundMs",
        bound(30_000.0, 300_000.0, 1_800_000.0),
    ),
    ("probe.budgetPerHour", bound(0.0, 30.0, 120.0)),
    ("probe.backgroundIntervalMin", bound(5.0, 30.0, 1_440.0)),
    ("failback.minHealthySpanSec", bound(60.0, 300.0, 3_600.0)),
    ("failback.minImprovementRatio", bound(0.05, 0.2, 0.9)),
    ("failback.minIntervalSec", bound(60.0, 600.0, 7_200.0)),
    ("udpBlockedTtlHours", bound(1.0, 24.0, 168.0)),
    ("pinTtlHours", bound(1.0, 24.0, 168.0)),
];

fn bound_of(path: &str) -> Bound {
    POLICY_BOUNDS
        .iter()
        .find(|(name, _)| *name == path)
        .map(|(_, bound)| *bound)
        .unwrap_or(bound(0.0, 0.0, 0.0))
}

fn lookup<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(root, |node, key| node.get(key))
}

/// 读取数值并夹紧；非数值或缺失取默认。
fn read_f64(root: &Value, path: &str) -> f64 {
    let limits = bound_of(path);
    match lookup(root, path).and_then(Value::as_f64) {
        Some(value) if value.is_finite() => value.clamp(limits.min, limits.max),
        _ => limits.default,
    }
}

fn read_u32(root: &Value, path: &str) -> u32 {
    // 先四舍五入再转整数；边界本身都是整数，不会越界。
    read_f64(root, path).round() as u32
}

impl Default for ClientPolicy {
    fn default() -> Self {
        Self::resolve(&Value::Null)
    }
}

impl ClientPolicy {
    /// 从清单的 `clientPolicy` 原始 JSON 得到夹紧后的参数。
    ///
    /// 除逐项夹紧外还保证几条跨字段关系，否则单项合法的组合仍可能让客户端行为失常：
    /// 心跳超时 ≥ 2 × 心跳间隔、后台心跳 ≥ 前台心跳、熔断冷却上限 ≥ 起始值、
    /// 退避上限 ≥ 基数、后台退避上限 ≥ 前台。
    pub fn resolve(raw: &Value) -> Self {
        let interval_sec = read_u32(raw, "heartbeat.intervalSec");
        let timeout_sec = read_u32(raw, "heartbeat.timeoutSec")
            .max(interval_sec.saturating_mul(2))
            .min(bound_of("heartbeat.timeoutSec").max as u32);
        let background_interval_sec =
            read_u32(raw, "heartbeat.backgroundIntervalSec").max(interval_sec);

        let cooldown_base_sec = read_u32(raw, "breaker.cooldownBaseSec");
        let cooldown_max_sec = read_u32(raw, "breaker.cooldownMaxSec").max(cooldown_base_sec);

        let base_ms = read_u32(raw, "backoff.baseMs");
        let cap_foreground_ms = read_u32(raw, "backoff.capForegroundMs").max(base_ms);
        let cap_background_ms = read_u32(raw, "backoff.capBackgroundMs").max(cap_foreground_ms);

        Self {
            protocol_order: resolve_protocol_order(lookup(raw, "protocolOrder")),
            race_stagger_ms: read_u32(raw, "raceStaggerMs"),
            connect_timeout_ms: read_u32(raw, "connectTimeoutMs"),
            heartbeat: HeartbeatPolicy {
                interval_sec,
                timeout_sec,
                background_interval_sec,
            },
            probe_wake_timeout_ms: read_u32(raw, "probeWakeTimeoutMs"),
            breaker: BreakerPolicy {
                failure_threshold: read_u32(raw, "breaker.failureThreshold"),
                failure_ratio: read_f64(raw, "breaker.failureRatio"),
                cooldown_base_sec,
                cooldown_max_sec,
            },
            backoff: BackoffPolicy {
                base_ms,
                cap_foreground_ms,
                cap_background_ms,
            },
            probe: ProbePolicy {
                budget_per_hour: read_u32(raw, "probe.budgetPerHour"),
                background_interval_min: read_u32(raw, "probe.backgroundIntervalMin"),
            },
            failback: FailbackPolicy {
                min_healthy_span_sec: read_u32(raw, "failback.minHealthySpanSec"),
                min_improvement_ratio: read_f64(raw, "failback.minImprovementRatio"),
                min_interval_sec: read_u32(raw, "failback.minIntervalSec"),
            },
            udp_blocked_ttl_hours: read_u32(raw, "udpBlockedTtlHours"),
            pin_ttl_hours: read_u32(raw, "pinTtlHours"),
        }
    }
}

/// 只保留认识的协议并去重；为空（或类型不对）时退回 `[quic, ws]`。
/// 清单只写 `["ws"]` 是合法的运维意图（全网停用 QUIC），保持原样。
fn resolve_protocol_order(raw: Option<&Value>) -> Vec<Protocol> {
    let mut order = Vec::with_capacity(2);
    if let Some(items) = raw.and_then(Value::as_array) {
        for protocol in items
            .iter()
            .filter_map(Value::as_str)
            .filter_map(Protocol::parse)
        {
            if !order.contains(&protocol) {
                order.push(protocol);
            }
        }
    }
    if order.is_empty() {
        order = vec![Protocol::Quic, Protocol::Ws];
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_match_design_table() {
        let policy = ClientPolicy::default();
        assert_eq!(policy.protocol_order, vec![Protocol::Quic, Protocol::Ws]);
        assert_eq!(policy.race_stagger_ms, 300);
        assert_eq!(policy.connect_timeout_ms, 8_000);
        assert_eq!(
            policy.heartbeat,
            HeartbeatPolicy {
                interval_sec: 30,
                timeout_sec: 90,
                background_interval_sec: 120
            }
        );
        assert_eq!(policy.breaker.failure_threshold, 3);
        assert_eq!(policy.breaker.failure_ratio, 0.5);
        assert_eq!(
            (
                policy.breaker.cooldown_base_sec,
                policy.breaker.cooldown_max_sec
            ),
            (30, 600)
        );
        assert_eq!(
            (
                policy.backoff.base_ms,
                policy.backoff.cap_foreground_ms,
                policy.backoff.cap_background_ms
            ),
            (1_000, 60_000, 300_000)
        );
        assert_eq!(
            (
                policy.probe.budget_per_hour,
                policy.probe.background_interval_min
            ),
            (30, 30)
        );
        assert_eq!(policy.failback.min_healthy_span_sec, 300);
        assert_eq!(policy.failback.min_improvement_ratio, 0.2);
        assert_eq!(policy.failback.min_interval_sec, 600);
        assert_eq!(
            (policy.udp_blocked_ttl_hours, policy.pin_ttl_hours),
            (24, 24)
        );
    }

    #[test]
    fn out_of_range_values_clamp_to_bounds() {
        let policy = ClientPolicy::resolve(&json!({
            "raceStaggerMs": 0,
            "backoff": { "baseMs": 10, "capForegroundMs": 1, "capBackgroundMs": 99999999 },
            "probe": { "budgetPerHour": 100000 },
            "breaker": { "failureRatio": -3, "cooldownBaseSec": 300, "cooldownMaxSec": 60 },
            "heartbeat": { "intervalSec": 200, "timeoutSec": 30 },
            "protocolOrder": ["ws", "ws", "tcp"],
            "connectTimeoutMs": "fast"
        }));
        assert_eq!(policy.race_stagger_ms, 100);
        assert_eq!(policy.backoff.base_ms, 500);
        assert_eq!(policy.backoff.cap_foreground_ms, 5_000);
        assert_eq!(policy.backoff.cap_background_ms, 1_800_000);
        assert_eq!(policy.probe.budget_per_hour, 120);
        assert_eq!(policy.breaker.failure_ratio, 0.2);
        // 冷却上限被抬到起始值，保证 ×2 递增不会倒退。
        assert_eq!(
            (
                policy.breaker.cooldown_base_sec,
                policy.breaker.cooldown_max_sec
            ),
            (300, 300)
        );
        // 心跳超时被抬到间隔的两倍。
        assert_eq!(policy.heartbeat.timeout_sec, 400);
        assert_eq!(policy.heartbeat.background_interval_sec, 200);
        assert_eq!(policy.protocol_order, vec![Protocol::Ws]);
        assert_eq!(policy.connect_timeout_ms, 8_000);
    }
}
