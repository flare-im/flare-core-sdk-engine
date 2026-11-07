//! 开发模式：由配置的端点直接构造未签名的单接入点清单，便于本地联调与 E2E。
//!
//! 生成的快照来源标记为 [`ManifestOrigin::Development`](crate::directory::ManifestOrigin)，
//! 上层据此持续发出警告事件并显示「开发模式」角标；发布构建由 CI 门禁拦截 `development`。

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::directory::{AccessPointEntry, DirectorySnapshot, ServiceEndpoints};
use crate::manifest::schema::ACCESS_SCHEMA;
use crate::manifest::{
    AccessManifest, AccessPoint, Cell, PathService, Placement, PortService, Region, Services,
    is_valid_id,
};
use crate::parsed_url::ParsedUrl;
use crate::time::{DAY_MS, format_rfc3339_ms};
use crate::types::{ApStatus, ServiceKind};

/// 开发模式合成清单里 Region / Cell / 接入点共用的 ID。
pub const DEVELOPMENT_ID: &str = "development";

/// 开发模式端点（完整 URL）。IM 至少给一个；REST 入口缺省时对应服务不可用。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DevelopmentEndpoints {
    /// `ws://` 或 `wss://`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws_url: Option<String>,
    /// `quic://host:port`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_url: Option<String>,
    /// `http://` 或 `https://`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub social_api_url: Option<String>,
    /// 可选：媒体入口；缺省时媒体服务不可用。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_url: Option<String>,
    /// 可选：额外的接入点（本地双入口故障演练、集成测试）。上面几项是 ID 为 `development` 的主接入点，
    /// 这里每一项是同一 Cell 里的另一个接入点，选路、熔断、切换与生产清单完全一样。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_points: Vec<DevelopmentAccessPoint>,
    /// 可选：开发期的 `clientPolicy` 覆盖（结构同清单，照常按硬上下限夹紧），用来在演练里缩短退避、心跳等。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_policy: Option<serde_json::Value>,
}

/// 开发模式的额外接入点：自带 ID 与一组完整 URL（字段含义同 [`DevelopmentEndpoints`]）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DevelopmentAccessPoint {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub social_api_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DevelopmentError {
    #[error("development mode needs wsUrl or quicUrl")]
    MissingImEndpoint,
    #[error("development access point id {0:?} is invalid or duplicated")]
    InvalidAccessPointId(String),
    #[error("development endpoint {field} is invalid: {reason}")]
    InvalidUrl { field: &'static str, reason: String },
}

/// 校验一个端点并返回（解析结果, 原样字符串）。保留原样字符串是因为 URL 规范化会给
/// `ws://host:port` 补上 `/`，而传输层对路径敏感，开发者写什么就用什么。
fn check(
    field: &'static str,
    value: &Option<String>,
    schemes: &[&str],
) -> Result<Option<(ParsedUrl, String)>, DevelopmentError> {
    let Some(value) = value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let parsed = ParsedUrl::parse(value).map_err(|error| DevelopmentError::InvalidUrl {
        field,
        reason: error.to_string(),
    })?;
    if !schemes.contains(&parsed.scheme()) {
        return Err(DevelopmentError::InvalidUrl {
            field,
            reason: format!("scheme must be one of {schemes:?}"),
        });
    }
    Ok(Some((parsed, value.to_owned())))
}

/// 一个开发接入点的已校验入口。
struct CheckedAccessPoint {
    id: String,
    host: String,
    services: Services,
    urls: ServiceEndpoints,
}

#[allow(clippy::too_many_arguments)]
fn check_access_point(
    id: &str,
    ws_url: &Option<String>,
    quic_url: &Option<String>,
    core_api_url: &Option<String>,
    social_api_url: &Option<String>,
    media_url: &Option<String>,
) -> Result<CheckedAccessPoint, DevelopmentError> {
    let ws = check("wsUrl", ws_url, &["ws", "wss"])?;
    let quic = check("quicUrl", quic_url, &["quic"])?;
    let core = check("coreApiUrl", core_api_url, &["http", "https"])?;
    let social = check("socialApiUrl", social_api_url, &["http", "https"])?;
    let media = check("mediaUrl", media_url, &["http", "https"])?;
    let Some((primary, _)) = ws.as_ref().or(quic.as_ref()) else {
        return Err(DevelopmentError::MissingImEndpoint);
    };
    let host = primary.host_str().to_owned();

    let path_of = |endpoint: &Option<(ParsedUrl, String)>| {
        endpoint.as_ref().map(|(url, _)| PathService {
            path: if url.path().is_empty() {
                "/".to_owned()
            } else {
                url.path().to_owned()
            },
        })
    };
    let services = Services {
        im_ws: path_of(&ws),
        im_quic: quic.as_ref().map(|(url, _)| PortService {
            port: url.port().unwrap_or(443),
        }),
        core_api: path_of(&core),
        social_api: path_of(&social),
        media: path_of(&media),
        ping: None,
    };
    // 端点直接用配置给出的 URL（端口、协议各不相同，无法从单个 host 推导）。
    let mut urls = ServiceEndpoints::default();
    let as_string =
        |endpoint: &Option<(ParsedUrl, String)>| endpoint.as_ref().map(|(_, raw)| raw.clone());
    urls.set(ServiceKind::ImWs, as_string(&ws));
    urls.set(ServiceKind::ImQuic, as_string(&quic));
    urls.set(ServiceKind::CoreApi, as_string(&core));
    urls.set(ServiceKind::SocialApi, as_string(&social));
    urls.set(ServiceKind::Media, as_string(&media));
    Ok(CheckedAccessPoint {
        id: id.to_owned(),
        host,
        services,
        urls,
    })
}

/// 构造开发模式快照。`now_ms` 用作 issuedAt；有效期给一年，开发模式也永不标记 stale。
pub fn development_snapshot(
    endpoints: &DevelopmentEndpoints,
    bootstrap_id: &str,
    now_ms: i64,
) -> Result<DirectorySnapshot, DevelopmentError> {
    let mut checked = vec![check_access_point(
        DEVELOPMENT_ID,
        &endpoints.ws_url,
        &endpoints.quic_url,
        &endpoints.core_api_url,
        &endpoints.social_api_url,
        &endpoints.media_url,
    )?];
    for extra in &endpoints.access_points {
        let id = extra.id.trim();
        if !is_valid_id(id) || checked.iter().any(|ap| ap.id == id) {
            return Err(DevelopmentError::InvalidAccessPointId(extra.id.clone()));
        }
        checked.push(check_access_point(
            id,
            &extra.ws_url,
            &extra.quic_url,
            &extra.core_api_url,
            &extra.social_api_url,
            &extra.media_url,
        )?);
    }

    let expires_at_ms = now_ms + 365 * DAY_MS;
    let manifest = AccessManifest {
        schema: ACCESS_SCHEMA.into(),
        bootstrap_id: bootstrap_id.to_owned(),
        app_id: DEVELOPMENT_ID.into(),
        serial: 0,
        issued_at: format_rfc3339_ms(now_ms),
        expires_at: format_rfc3339_ms(expires_at_ms),
        regions: vec![Region {
            id: DEVELOPMENT_ID.into(),
            display_name: [("en".to_owned(), "Development".to_owned())]
                .into_iter()
                .collect(),
            cells: vec![Cell {
                id: DEVELOPMENT_ID.into(),
                access_points: checked
                    .iter()
                    .map(|ap| AccessPoint {
                        id: ap.id.clone(),
                        host: ap.host.clone(),
                        ip_hints: vec![],
                        spki_pins: vec![],
                        weight: 100,
                        priority: 0,
                        status: ApStatus::Active,
                        tags: vec![],
                        services: ap.services.clone(),
                    })
                    .collect(),
            }],
        }],
        placement: Placement {
            home_cell: DEVELOPMENT_ID.into(),
            allowed_regions: vec![DEVELOPMENT_ID.into()],
        },
        bootstrap: Default::default(),
        client_policy: endpoints
            .client_policy
            .clone()
            .unwrap_or(serde_json::Value::Null),
        release: None,
    };

    let entries = checked
        .into_iter()
        .map(|ap| AccessPointEntry {
            id: ap.id.as_str().into(),
            cell_id: DEVELOPMENT_ID.into(),
            region_id: DEVELOPMENT_ID.into(),
            host: ap.host,
            ip_hints: vec![],
            spki_pins: vec![],
            weight: 100,
            priority: 0,
            status: ApStatus::Active,
            tags: vec![],
            endpoints: ap.urls,
        })
        .collect();
    Ok(DirectorySnapshot::development(
        manifest,
        now_ms,
        expires_at_ms,
        entries,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_single_unsigned_access_point() {
        let endpoints = DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051".into()),
            quic_url: Some("quic://127.0.0.1:60052".into()),
            core_api_url: Some("http://127.0.0.1:60061".into()),
            social_api_url: Some("http://localhost:8080/social-api".into()),
            media_url: None,
            ..Default::default()
        };
        let snapshot = development_snapshot(&endpoints, "development", 0).unwrap();
        assert!(snapshot.is_development());
        assert!(!snapshot.is_stale(i64::MAX));
        let ap = &snapshot.access_points()[0];
        assert_eq!(ap.endpoint(ServiceKind::ImWs), Some("ws://127.0.0.1:60051"));
        assert_eq!(
            ap.endpoint(ServiceKind::ImQuic),
            Some("quic://127.0.0.1:60052")
        );
        assert_eq!(
            ap.endpoint(ServiceKind::SocialApi),
            Some("http://localhost:8080/social-api")
        );
        assert_eq!(ap.endpoint(ServiceKind::Media), None);
        assert_eq!(ap.endpoint(ServiceKind::Ping), None);
        assert_eq!(
            snapshot.manifest().regions[0].cells[0].access_points[0]
                .services
                .im_quic,
            Some(PortService { port: 60052 })
        );
    }

    #[test]
    fn rejects_missing_im_and_bad_schemes() {
        assert_eq!(
            development_snapshot(&DevelopmentEndpoints::default(), "development", 0).unwrap_err(),
            DevelopmentError::MissingImEndpoint
        );
        let bad = DevelopmentEndpoints {
            ws_url: Some("http://127.0.0.1:60051".into()),
            ..Default::default()
        };
        assert!(matches!(
            development_snapshot(&bad, "development", 0),
            Err(DevelopmentError::InvalidUrl { field: "wsUrl", .. })
        ));
    }

    #[test]
    fn extra_access_points_join_the_same_cell() {
        let endpoints = DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051/ws".into()),
            access_points: vec![DevelopmentAccessPoint {
                id: "ap-b".into(),
                ws_url: Some("ws://127.0.0.1:61051/ws".into()),
                core_api_url: Some("http://127.0.0.1:61061".into()),
                ..Default::default()
            }],
            client_policy: Some(serde_json::json!({ "backoff": { "baseMs": 500 } })),
            ..Default::default()
        };
        let snapshot = development_snapshot(&endpoints, "development", 0).unwrap();
        let ids: Vec<&str> = snapshot.access_points().iter().map(|ap| &*ap.id).collect();
        assert_eq!(ids, vec!["development", "ap-b"]);
        assert_eq!(
            snapshot.access_points()[1].endpoint(ServiceKind::ImWs),
            Some("ws://127.0.0.1:61051/ws")
        );
        assert_eq!(snapshot.policy().backoff.base_ms, 500);

        let duplicated = DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051".into()),
            access_points: vec![DevelopmentAccessPoint {
                id: DEVELOPMENT_ID.into(),
                ws_url: Some("ws://127.0.0.1:61051".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            development_snapshot(&duplicated, "development", 0),
            Err(DevelopmentError::InvalidAccessPointId(_))
        ));
    }

    /// 开发模式的 clientPolicy 与签名清单走同一套夹紧：越界取边界，缺省取默认。
    #[test]
    fn development_client_policy_is_clamped_like_a_manifest() {
        let endpoints = |client_policy: Option<serde_json::Value>| DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:60051/ws".into()),
            client_policy,
            ..Default::default()
        };
        let snapshot = development_snapshot(
            &endpoints(Some(serde_json::json!({
                "probeWakeTimeoutMs": 100,
                "connectTimeoutMs": 999_999,
                "backoff": { "baseMs": 1 }
            }))),
            "development",
            0,
        )
        .unwrap();
        let policy = snapshot.policy();
        assert_eq!(policy.probe_wake_timeout_ms, 2_000);
        assert_eq!(policy.connect_timeout_ms, 30_000);
        assert_eq!(policy.backoff.base_ms, 500);
        assert_eq!(
            policy,
            &crate::manifest::ClientPolicy::resolve(&serde_json::json!({
                "probeWakeTimeoutMs": 100,
                "connectTimeoutMs": 999_999,
                "backoff": { "baseMs": 1 }
            }))
        );

        let defaults = development_snapshot(&endpoints(None), "development", 0).unwrap();
        assert_eq!(defaults.policy().probe_wake_timeout_ms, 5_000);
    }
}
