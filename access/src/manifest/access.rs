//! 接入清单（Access Manifest）的载荷模型与结构校验。
//!
//! 结构校验与签名无关：控制面在签发前、客户端在验签后跑同一套规则，
//! 这样一份结构有问题的清单在发布前就会被拦下，而不是到客户端才被拒收。

use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::policy::ClientPolicy;
use super::schema::{
    ACCESS_SCHEMA, MAX_ACCESS_POINTS, MAX_BOOTSTRAP_SOURCES, MAX_CELLS_PER_REGION,
    MAX_IP_HINTS_PER_AP, MAX_REGIONS, MAX_SPKI_PINS_PER_AP, MAX_TAGS_PER_AP,
};
use crate::parsed_url::ParsedUrl;
use crate::time::parse_rfc3339_ms;
use crate::types::{ApStatus, ServiceKind};

/// 引导刷新间隔的默认值与上下限（秒）。下限防止错误清单让全网客户端高频拉镜像。
pub const DEFAULT_REFRESH_INTERVAL_SEC: u64 = 21_600;
pub const MIN_REFRESH_INTERVAL_SEC: u64 = 600;
pub const MAX_REFRESH_INTERVAL_SEC: u64 = 86_400;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessManifest {
    /// 固定为 `flare.access.v1`。
    #[schemars(extend("const" = "flare.access.v1"))]
    pub schema: String,
    /// 每个应用随机生成的引导 ID，与 `appId` 解耦（镜像路径用它，避免枚举租户）。
    pub bootstrap_id: String,
    pub app_id: String,
    /// 同一 `bootstrapId` 单调递增。
    pub serial: u64,
    /// RFC 3339。
    #[schemars(extend("format" = "date-time"))]
    pub issued_at: String,
    /// RFC 3339；过期后仍可用于连接，但标记 stale 并提高刷新频率。
    #[schemars(extend("format" = "date-time"))]
    pub expires_at: String,
    pub regions: Vec<Region>,
    pub placement: Placement,
    #[serde(default)]
    pub bootstrap: BootstrapSection,
    /// 原样保留：解析时逐字段宽松读取并夹紧到硬上下限，见 [`ClientPolicy::resolve`]。
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub client_policy: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<ReleaseSection>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Region {
    pub id: String,
    /// 语言标签 → 显示名。
    #[serde(default)]
    pub display_name: BTreeMap<String, String>,
    pub cells: Vec<Cell>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Cell {
    pub id: String,
    pub access_points: Vec<AccessPoint>,
}

fn default_weight() -> u32 {
    100
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessPoint {
    pub id: String,
    /// 公网可签发证书的域名；禁止裸 IP。
    pub host: String,
    /// 仅原生端使用：DNS 失败或可疑时直连，SNI 与证书校验仍按 `host`。
    #[serde(default)]
    pub ip_hints: Vec<String>,
    /// 可选的 SPKI SHA-256 钉扎（base64）。
    #[serde(default)]
    pub spki_pins: Vec<String>,
    /// 同档内加权随机的权重，0–1000。
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// 0 最高；每级在打分里加 50ms。
    #[serde(default)]
    pub priority: u32,
    pub status: ApStatus,
    /// 网络适配标签，例如 `isp:ct`、`net:ipv6`、`udp:ok`。
    #[serde(default)]
    pub tags: Vec<String>,
    pub services: Services,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Services {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub im_ws: Option<PathService>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub im_quic: Option<PortService>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_api: Option<PathService>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub social_api: Option<PathService>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<PathService>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ping: Option<PathService>,
}

impl Services {
    pub fn path(&self, kind: ServiceKind) -> Option<&str> {
        let entry = match kind {
            ServiceKind::ImWs => &self.im_ws,
            ServiceKind::CoreApi => &self.core_api,
            ServiceKind::SocialApi => &self.social_api,
            ServiceKind::Media => &self.media,
            ServiceKind::Ping => &self.ping,
            ServiceKind::ImQuic => return None,
        };
        entry.as_ref().map(|service| service.path.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PathService {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PortService {
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Placement {
    /// 应用归属 Cell：客户端只在该 Cell 的接入点之间切换。
    pub home_cell: String,
    #[serde(default)]
    pub allowed_regions: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapSection {
    #[serde(default)]
    pub sources: Vec<BootstrapSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval_sec: Option<u64>,
}

/// 引导源。`kind` 用字符串而不是封闭枚举：新版控制面可以加源类型，旧客户端忽略即可。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapSource {
    /// `https` | `wellKnown` | `dohTxt`。
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolvers: Vec<String>,
    /// 仅 `https` 镜像、仅原生端使用：规则同接入点的 `ipHints`，SNI 与证书校验仍按 URL 的主机名。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ip_hints: Vec<String>,
}

impl BootstrapSource {
    pub fn https(url: impl Into<String>) -> Self {
        Self {
            kind: "https".to_owned(),
            url: Some(url.into()),
            name: None,
            resolvers: Vec::new(),
            ip_hints: Vec::new(),
        }
    }

    pub fn well_known() -> Self {
        Self {
            kind: "wellKnown".to_owned(),
            url: None,
            name: None,
            resolvers: Vec::new(),
            ip_hints: Vec::new(),
        }
    }

    pub fn doh_txt(name: impl Into<String>, resolvers: Vec<String>) -> Self {
        Self {
            kind: "dohTxt".to_owned(),
            url: None,
            name: Some(name.into()),
            resolvers,
            ip_hints: Vec::new(),
        }
    }

    /// 给 `https` 镜像带上 `ipHints`。
    pub fn with_ip_hints(mut self, ip_hints: Vec<String>) -> Self {
        self.ip_hints = ip_hints;
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseSection {
    #[serde(default)]
    pub channels: Vec<String>,
}

/// 校验通过后解析出的时间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestTimes {
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest JSON is invalid: {0}")]
    Json(String),
    #[error("unsupported manifest schema {0:?}")]
    UnsupportedSchema(String),
    #[error("invalid manifest field {field}: {reason}")]
    Invalid { field: String, reason: &'static str },
}

pub(super) fn invalid(field: impl Into<String>, reason: &'static str) -> ManifestError {
    ManifestError::Invalid {
        field: field.into(),
        reason,
    }
}

#[derive(Deserialize)]
pub(super) struct SchemaHeader {
    pub(super) schema: String,
}

impl AccessManifest {
    /// 控制面编排的起点：写好 `schema`，其余可选段留空，由调用方继续填。
    pub fn new(
        bootstrap_id: impl Into<String>,
        app_id: impl Into<String>,
        serial: u64,
        issued_at: impl Into<String>,
        expires_at: impl Into<String>,
        regions: Vec<Region>,
        placement: Placement,
    ) -> Self {
        Self {
            schema: ACCESS_SCHEMA.to_owned(),
            bootstrap_id: bootstrap_id.into(),
            app_id: app_id.into(),
            serial,
            issued_at: issued_at.into(),
            expires_at: expires_at.into(),
            regions,
            placement,
            bootstrap: BootstrapSection::default(),
            client_policy: serde_json::Value::Null,
            release: None,
        }
    }

    /// 校验并序列化成待签名的载荷字节。控制面只应签这里产出的字节：
    /// 除了与客户端相同的结构校验，还要求 bootstrapId 是 Edge 与镜像路径接受的形式。
    pub fn to_payload(&self) -> Result<Vec<u8>, ManifestError> {
        self.validate()?;
        if !is_valid_bootstrap_id(&self.bootstrap_id) {
            return Err(invalid("bootstrapId", "must be 16 lowercase hex chars"));
        }
        serde_json::to_vec(self).map_err(|error| ManifestError::Json(error.to_string()))
    }

    /// 解析并做结构校验。先只读 `schema`：将来的 v2 结构可能完全不同，
    /// 这时应该报「不支持的 schema」，而不是一个难以理解的 JSON 字段错误。
    pub fn from_slice(bytes: &[u8]) -> Result<(Self, ManifestTimes), ManifestError> {
        let header: SchemaHeader = serde_json::from_slice(bytes)
            .map_err(|error| ManifestError::Json(error.to_string()))?;
        if header.schema != ACCESS_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(header.schema));
        }
        let manifest: AccessManifest = serde_json::from_slice(bytes)
            .map_err(|error| ManifestError::Json(error.to_string()))?;
        let times = manifest.validate()?;
        Ok((manifest, times))
    }

    /// 结构校验：ID 合法且唯一、归属 Cell 存在、主机名不是 IP、服务路径合法、时间可解析。
    pub fn validate(&self) -> Result<ManifestTimes, ManifestError> {
        if self.schema != ACCESS_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(self.schema.clone()));
        }
        if !is_valid_id(&self.bootstrap_id) {
            return Err(invalid(
                "bootstrapId",
                "must be 1-64 chars of [A-Za-z0-9._:-]",
            ));
        }
        if self.app_id.is_empty() || self.app_id.len() > 128 {
            return Err(invalid("appId", "must be 1-128 chars"));
        }
        let issued_at_ms =
            parse_rfc3339_ms(&self.issued_at).ok_or_else(|| invalid("issuedAt", "not RFC 3339"))?;
        let expires_at_ms = parse_rfc3339_ms(&self.expires_at)
            .ok_or_else(|| invalid("expiresAt", "not RFC 3339"))?;
        if expires_at_ms <= issued_at_ms {
            return Err(invalid("expiresAt", "must be later than issuedAt"));
        }

        if self.regions.is_empty() || self.regions.len() > MAX_REGIONS {
            return Err(invalid("regions", "must hold 1-32 regions"));
        }
        let mut region_ids = HashSet::new();
        let mut cell_ids = HashSet::new();
        let mut ap_ids = HashSet::new();
        let mut ap_total = 0usize;
        for (region_index, region) in self.regions.iter().enumerate() {
            let field = format!("regions[{region_index}]");
            if !is_valid_id(&region.id) || !region_ids.insert(region.id.as_str()) {
                return Err(invalid(format!("{field}.id"), "invalid or duplicate id"));
            }
            if region.cells.len() > MAX_CELLS_PER_REGION {
                return Err(invalid(format!("{field}.cells"), "too many cells"));
            }
            for (cell_index, cell) in region.cells.iter().enumerate() {
                let field = format!("{field}.cells[{cell_index}]");
                if !is_valid_id(&cell.id) || !cell_ids.insert(cell.id.as_str()) {
                    return Err(invalid(format!("{field}.id"), "invalid or duplicate id"));
                }
                ap_total += cell.access_points.len();
                if ap_total > MAX_ACCESS_POINTS {
                    return Err(invalid("regions", "too many access points"));
                }
                for (ap_index, ap) in cell.access_points.iter().enumerate() {
                    let field = format!("{field}.accessPoints[{ap_index}]");
                    if !is_valid_id(&ap.id) || !ap_ids.insert(ap.id.as_str()) {
                        return Err(invalid(format!("{field}.id"), "invalid or duplicate id"));
                    }
                    validate_access_point(ap, &field)?;
                }
            }
        }

        if !cell_ids.contains(self.placement.home_cell.as_str()) {
            return Err(invalid(
                "placement.homeCell",
                "must reference a cell in regions",
            ));
        }
        for region in &self.placement.allowed_regions {
            if !region_ids.contains(region.as_str()) {
                return Err(invalid("placement.allowedRegions", "unknown region"));
            }
        }

        if self.bootstrap.sources.len() > MAX_BOOTSTRAP_SOURCES {
            return Err(invalid("bootstrap.sources", "too many sources"));
        }
        for (index, source) in self.bootstrap.sources.iter().enumerate() {
            validate_source(source, &format!("bootstrap.sources[{index}]"))?;
        }
        Ok(ManifestTimes {
            issued_at_ms,
            expires_at_ms,
        })
    }

    /// 归属 Cell 及其所在区域。
    pub fn home_cell(&self) -> Option<(&Region, &Cell)> {
        self.regions.iter().find_map(|region| {
            region
                .cells
                .iter()
                .find(|cell| cell.id == self.placement.home_cell)
                .map(|cell| (region, cell))
        })
    }

    /// 已夹紧的客户端链路参数。
    pub fn client_policy(&self) -> ClientPolicy {
        ClientPolicy::resolve(&self.client_policy)
    }

    /// 引导刷新间隔（秒），夹在 [600, 86400]。
    pub fn refresh_interval_sec(&self) -> u64 {
        self.bootstrap
            .refresh_interval_sec
            .unwrap_or(DEFAULT_REFRESH_INTERVAL_SEC)
            .clamp(MIN_REFRESH_INTERVAL_SEC, MAX_REFRESH_INTERVAL_SEC)
    }
}

fn validate_access_point(ap: &AccessPoint, field: &str) -> Result<(), ManifestError> {
    if !is_valid_public_host(&ap.host) {
        return Err(invalid(
            format!("{field}.host"),
            "must be a DNS name with at least one dot, not an IP literal",
        ));
    }
    if ap.weight > 1000 {
        return Err(invalid(format!("{field}.weight"), "must be 0-1000"));
    }
    if ap.priority > 100 {
        return Err(invalid(format!("{field}.priority"), "must be 0-100"));
    }
    if ap.tags.len() > MAX_TAGS_PER_AP
        || ap
            .tags
            .iter()
            .any(|tag| tag.is_empty() || tag.len() > 64 || tag.chars().any(char::is_whitespace))
    {
        return Err(invalid(
            format!("{field}.tags"),
            "too many or malformed tags",
        ));
    }
    validate_ip_hints(&ap.ip_hints, field)?;
    if ap.spki_pins.len() > MAX_SPKI_PINS_PER_AP
        || ap
            .spki_pins
            .iter()
            .any(|pin| pin.is_empty() || pin.len() > 128)
    {
        return Err(invalid(
            format!("{field}.spkiPins"),
            "too many or malformed pins",
        ));
    }
    let services = &ap.services;
    for kind in ServiceKind::ALL {
        if let Some(path) = services.path(kind)
            && !is_valid_path(path)
        {
            return Err(invalid(
                format!("{field}.services.{}", kind.as_str()),
                "path must start with '/' and contain no whitespace, '?' or '#'",
            ));
        }
    }
    if let Some(quic) = &services.im_quic
        && quic.port == 0
    {
        return Err(invalid(
            format!("{field}.services.imQuic.port"),
            "must be 1-65535",
        ));
    }
    Ok(())
}

fn validate_ip_hints(ip_hints: &[String], field: &str) -> Result<(), ManifestError> {
    if ip_hints.len() > MAX_IP_HINTS_PER_AP
        || ip_hints.iter().any(|hint| hint.parse::<IpAddr>().is_err())
    {
        return Err(invalid(
            format!("{field}.ipHints"),
            "too many or not IP addresses",
        ));
    }
    Ok(())
}

pub(crate) fn validate_source(source: &BootstrapSource, field: &str) -> Result<(), ManifestError> {
    match source.kind.as_str() {
        "https" => {
            let ok = source
                .url
                .as_deref()
                .and_then(|value| ParsedUrl::parse(value).ok())
                .is_some_and(|url| url.scheme() == "https");
            if !ok {
                return Err(invalid(
                    format!("{field}.url"),
                    "https source needs an https URL",
                ));
            }
            validate_ip_hints(&source.ip_hints, field)?;
        }
        "dohTxt" => {
            let name_ok = source
                .name
                .as_deref()
                .is_some_and(|name| !name.is_empty() && name.len() <= 253);
            let resolvers_ok = !source.resolvers.is_empty()
                && source.resolvers.len() <= 8
                && source.resolvers.iter().all(|resolver| {
                    ParsedUrl::parse(resolver).is_ok_and(|url| url.scheme() == "https")
                });
            if !name_ok || !resolvers_ok {
                return Err(invalid(
                    field,
                    "dohTxt source needs a name and https resolvers",
                ));
            }
        }
        // `wellKnown` 无参数；未知类型留给新版客户端，旧客户端忽略。
        _ => {}
    }
    Ok(())
}

/// ID 规则：1–64 字符，首字符为字母数字，其余为字母数字或 `._:-`。
/// 不允许 `/`：ID 会拼进本地存储键与镜像路径。
pub fn is_valid_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// 控制面分配的 bootstrapId：随机 64 位的 16 位小写十六进制。
/// Edge 的镜像路由只放行这个形式；客户端配置里的开发用 ID 走 [`is_valid_id`]。
pub fn is_valid_bootstrap_id(value: &str) -> bool {
    value.len() == 16
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// 生产清单的主机名：至少两级、每级 1–63 字符、不是 IP 字面量。
fn is_valid_public_host(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 || host.parse::<IpAddr>().is_ok() {
        return false;
    }
    let mut labels = 0;
    for label in host.split('.') {
        labels += 1;
        let bytes = label.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 63
            || bytes[0] == b'-'
            || bytes[bytes.len() - 1] == b'-'
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
        {
            return false;
        }
    }
    labels >= 2
}

pub(super) fn is_valid_path(path: &str) -> bool {
    path.starts_with('/')
        && path.len() <= 256
        && !path
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control() || ch == '?' || ch == '#')
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// 设计文档 §4.1 的示例清单（载荷）。
    pub(crate) fn sample_manifest_json() -> serde_json::Value {
        serde_json::json!({
            "schema": "flare.access.v1",
            "bootstrapId": "b7f3c9a1e2d4b6a8",
            "appId": "app_01J9ZK3X",
            "serial": 1042,
            "issuedAt": "2026-11-04T08:00:00Z",
            "expiresAt": "2026-12-04T08:00:00Z",
            "regions": [{
                "id": "cn-east",
                "displayName": { "zh-CN": "华东", "en": "East China" },
                "cells": [{
                    "id": "cell-cne-1",
                    "accessPoints": [{
                        "id": "ap-cne1-ct",
                        "host": "im-ct.example.com",
                        "ipHints": ["203.0.113.10", "203.0.113.11"],
                        "spkiPins": [],
                        "weight": 100,
                        "priority": 0,
                        "status": "active",
                        "tags": ["isp:ct"],
                        "services": {
                            "imWs": { "path": "/ws" },
                            "imQuic": { "port": 443 },
                            "coreApi": { "path": "/api" },
                            "socialApi": { "path": "/social-api" },
                            "media": { "path": "/flare-media" },
                            "ping": { "path": "/.well-known/flare/ping" }
                        }
                    }]
                }]
            }],
            "placement": { "homeCell": "cell-cne-1", "allowedRegions": ["cn-east"] },
            "bootstrap": {
                "sources": [
                    { "kind": "https", "url": "https://cfg-a.example-cdn.com/flare/v1/b7f3c9a1e2d4b6a8/" },
                    { "kind": "https", "url": "https://cfg-b.other-cloud.net/flare/v1/b7f3c9a1e2d4b6a8/" },
                    { "kind": "wellKnown" },
                    { "kind": "dohTxt", "name": "_flare.b7f3c9a1e2d4b6a8.example-boot.org",
                      "resolvers": ["https://1.1.1.1/dns-query", "https://223.5.5.5/dns-query"] }
                ],
                "refreshIntervalSec": 21600
            },
            "clientPolicy": { "raceStaggerMs": 300, "breaker": { "failureThreshold": 3 } },
            "release": { "channels": ["stable", "beta"] }
        })
    }

    #[test]
    fn parses_design_sample() {
        let bytes = serde_json::to_vec(&sample_manifest_json()).unwrap();
        let (manifest, times) = AccessManifest::from_slice(&bytes).unwrap();
        assert_eq!(manifest.serial, 1042);
        assert_eq!(times.issued_at_ms, 1_793_779_200_000);
        assert_eq!(times.expires_at_ms, 1_796_371_200_000);
        let (region, cell) = manifest.home_cell().unwrap();
        assert_eq!(region.id, "cn-east");
        assert_eq!(
            cell.access_points[0].services.path(ServiceKind::Ping),
            Some("/.well-known/flare/ping")
        );
        assert_eq!(manifest.refresh_interval_sec(), 21_600);
        assert_eq!(manifest.client_policy().race_stagger_ms, 300);
    }

    #[test]
    fn unknown_status_and_fields_are_tolerated() {
        let mut value = sample_manifest_json();
        value["regions"][0]["cells"][0]["accessPoints"][0]["status"] = "quarantined".into();
        value["futureField"] = serde_json::json!({ "x": 1 });
        let bytes = serde_json::to_vec(&value).unwrap();
        let (manifest, _) = AccessManifest::from_slice(&bytes).unwrap();
        assert_eq!(
            manifest.regions[0].cells[0].access_points[0].status,
            ApStatus::Unknown
        );
    }

    type Mutation = Box<dyn Fn(&mut serde_json::Value)>;

    #[test]
    fn rejects_structural_problems() {
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "ip host",
                Box::new(|v: &mut serde_json::Value| {
                    v["regions"][0]["cells"][0]["accessPoints"][0]["host"] = "203.0.113.10".into()
                }),
            ),
            (
                "single label host",
                Box::new(|v: &mut serde_json::Value| {
                    v["regions"][0]["cells"][0]["accessPoints"][0]["host"] = "localhost".into()
                }),
            ),
            (
                "bad home cell",
                Box::new(|v: &mut serde_json::Value| v["placement"]["homeCell"] = "cell-x".into()),
            ),
            (
                "slash id",
                Box::new(|v: &mut serde_json::Value| {
                    v["regions"][0]["cells"][0]["accessPoints"][0]["id"] = "ap/1".into()
                }),
            ),
            (
                "bad path",
                Box::new(|v: &mut serde_json::Value| {
                    v["regions"][0]["cells"][0]["accessPoints"][0]["services"]["imWs"]["path"] =
                        "ws".into()
                }),
            ),
            (
                "bad hint",
                Box::new(|v: &mut serde_json::Value| {
                    v["regions"][0]["cells"][0]["accessPoints"][0]["ipHints"] =
                        serde_json::json!(["not-ip"])
                }),
            ),
            (
                "expiry before issue",
                Box::new(|v: &mut serde_json::Value| {
                    v["expiresAt"] = "2026-11-01T00:00:00Z".into()
                }),
            ),
            (
                "http mirror",
                Box::new(|v: &mut serde_json::Value| {
                    v["bootstrap"]["sources"][0]["url"] = "http://cfg.example.com/".into()
                }),
            ),
            (
                "bad mirror hint",
                Box::new(|v: &mut serde_json::Value| {
                    v["bootstrap"]["sources"][0]["ipHints"] = serde_json::json!(["not-ip"])
                }),
            ),
            (
                "too many mirror hints",
                Box::new(|v: &mut serde_json::Value| {
                    v["bootstrap"]["sources"][0]["ipHints"] =
                        serde_json::json!(vec!["203.0.113.10"; MAX_IP_HINTS_PER_AP + 1])
                }),
            ),
            (
                "weight",
                Box::new(|v: &mut serde_json::Value| {
                    v["regions"][0]["cells"][0]["accessPoints"][0]["weight"] = 5000.into()
                }),
            ),
        ];
        for (name, mutate) in cases {
            let mut value = sample_manifest_json();
            mutate(&mut value);
            let bytes = serde_json::to_vec(&value).unwrap();
            assert!(
                matches!(
                    AccessManifest::from_slice(&bytes),
                    Err(ManifestError::Invalid { .. })
                ),
                "{name} should be rejected"
            );
        }
    }

    #[test]
    fn control_plane_builds_a_manifest_the_client_accepts() {
        let mut manifest = AccessManifest::new(
            "b7f3c9a1e2d4b6a8",
            "app_01J9ZK3X",
            7,
            "2026-11-04T08:00:00Z",
            "2026-12-04T08:00:00Z",
            vec![Region {
                id: "cn-east".into(),
                display_name: BTreeMap::new(),
                cells: vec![Cell {
                    id: "cell-cne-1".into(),
                    access_points: vec![AccessPoint {
                        id: "ap-cne1-ct".into(),
                        host: "im-ct.example.com".into(),
                        ip_hints: Vec::new(),
                        spki_pins: Vec::new(),
                        weight: 100,
                        priority: 0,
                        status: ApStatus::Active,
                        tags: Vec::new(),
                        services: Services {
                            im_ws: Some(PathService { path: "/ws".into() }),
                            ping: Some(PathService {
                                path: "/.well-known/flare/ping".into(),
                            }),
                            ..Services::default()
                        },
                    }],
                }],
            }],
            Placement {
                home_cell: "cell-cne-1".into(),
                allowed_regions: vec!["cn-east".into()],
            },
        );
        manifest.bootstrap.sources = vec![
            BootstrapSource::https("https://cfg-a.example-cdn.com/flare/v1/b7f3c9a1e2d4b6a8/"),
            BootstrapSource::well_known(),
            BootstrapSource::doh_txt(
                "_flare.b7f3c9a1e2d4b6a8.example-boot.org",
                vec!["https://1.1.1.1/dns-query".into()],
            ),
        ];
        let payload = manifest.to_payload().unwrap();
        let (parsed, _) = AccessManifest::from_slice(&payload).unwrap();
        assert_eq!(parsed, manifest);

        // 客户端的 ID 规则放行，但控制面不签 Edge 路由不了的 bootstrapId。
        manifest.bootstrap_id = "development".into();
        assert!(manifest.validate().is_ok());
        assert!(matches!(
            manifest.to_payload(),
            Err(ManifestError::Invalid { .. })
        ));
    }

    #[test]
    fn mirror_hints_round_trip_and_stay_off_the_wire_when_empty() {
        let plain = BootstrapSource::https("https://cfg-a.example-cdn.com/flare/");
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            serde_json::json!({ "kind": "https", "url": "https://cfg-a.example-cdn.com/flare/" })
        );

        let hinted = plain.with_ip_hints(vec!["203.0.113.10".into(), "2001:db8::10".into()]);
        let value = serde_json::to_value(&hinted).unwrap();
        assert_eq!(
            value["ipHints"],
            serde_json::json!(["203.0.113.10", "2001:db8::10"])
        );
        assert_eq!(
            serde_json::from_value::<BootstrapSource>(value).unwrap(),
            hinted
        );
        assert!(validate_source(&hinted, "source").is_ok());
    }

    /// 只有 `https` 镜像用这个字段；未知类型留给新版客户端，这里不替它判断。
    #[test]
    fn only_https_sources_check_their_hints() {
        let source = BootstrapSource {
            kind: "ipfs".into(),
            ..BootstrapSource::https("https://cfg-a.example-cdn.com/flare/")
                .with_ip_hints(vec!["not-ip".into()])
        };
        assert!(validate_source(&source, "source").is_ok());
    }

    #[test]
    fn bootstrap_id_is_sixteen_lowercase_hex_chars() {
        assert!(is_valid_bootstrap_id("b7f3c9a1e2d4b6a8"));
        for value in [
            "",
            "b7f3c9a1e2d4b6a",
            "b7f3c9a1e2d4b6a80",
            "B7F3C9A1E2D4B6A8",
            "b7f3c9a1e2d4b6g8",
            "../../etc/passwd",
        ] {
            assert!(!is_valid_bootstrap_id(value), "{value}");
        }
    }

    #[test]
    fn unsupported_schema_is_reported_before_structure() {
        let bytes = br#"{"schema":"flare.access.v2","totally":"different"}"#;
        assert_eq!(
            AccessManifest::from_slice(bytes).unwrap_err(),
            ManifestError::UnsupportedSchema("flare.access.v2".into())
        );
    }
}
