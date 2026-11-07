//! 恢复清单：全部引导源与接入点都失效时，经带外渠道（官网、短信、邮件、客服）下发的精简签名清单。
//!
//! 整条恢复链接要装进一个二维码（≤ 1 KiB），所以它不是完整的接入清单：只列 1–2 个接入点和
//! 引导源，由根密钥直接签名（带不下在线密钥证书），服务入口缺省按 Edge 的标准布局展开。
//! 展开后就是一份普通接入清单，验证与采纳走同一条路径，同样受 serial 防回滚约束；
//! 客户端拿到新地址后会很快从引导源换回完整清单。

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::access::{
    AccessManifest, AccessPoint, BootstrapSection, BootstrapSource, Cell, MIN_REFRESH_INTERVAL_SEC,
    ManifestError, ManifestTimes, PathService, Placement, PortService, Region, Services,
};
use super::schema::{ACCESS_SCHEMA, RECOVERY_SCHEMA};
use crate::types::ApStatus;

/// 恢复链接（含 scheme）的字节数上限。
pub const MAX_RECOVERY_LINK_BYTES: usize = 1024;
pub const MAX_RECOVERY_ACCESS_POINTS: usize = 2;
pub const MAX_RECOVERY_SOURCES: usize = 4;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryManifest {
    /// 固定为 `flare.recovery.v1`。
    pub schema: String,
    pub bootstrap_id: String,
    pub app_id: String,
    /// 与接入清单共用同一条 serial 序列：导入后必须不小于本机已接受的最大值。
    pub serial: u64,
    /// RFC 3339。
    pub issued_at: String,
    /// RFC 3339；过期只标记 stale，与接入清单相同。
    pub expires_at: String,
    pub region_id: String,
    /// 这些接入点所属的 Cell（即应用的归属 Cell）。
    pub cell_id: String,
    pub access_points: Vec<RecoveryAccessPoint>,
    /// 引导源（https 镜像基址）。客户端随后从这里换回完整清单。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryAccessPoint {
    pub id: String,
    pub host: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ip_hints: Vec<String>,
    /// 缺省按 Edge 的标准布局展开；只有非标准部署才需要写。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub services: Option<Services>,
}

/// Edge 的标准入口布局（`flare-deploy` 的路由）。
fn standard_services() -> Services {
    let path = |path: &str| {
        Some(PathService {
            path: path.to_owned(),
        })
    };
    Services {
        im_ws: path("/ws"),
        im_quic: Some(PortService { port: 443 }),
        core_api: path("/api"),
        social_api: path("/social-api"),
        media: path("/flare-media"),
        ping: path("/.well-known/flare/ping"),
    }
}

impl RecoveryManifest {
    pub fn from_slice(bytes: &[u8]) -> Result<Self, ManifestError> {
        serde_json::from_slice(bytes).map_err(|error| ManifestError::Json(error.to_string()))
    }

    /// 校验并展开成接入清单。结构规则与接入清单相同，这里只多限制条数。
    pub fn to_access_manifest(&self) -> Result<(AccessManifest, ManifestTimes), ManifestError> {
        if self.schema != RECOVERY_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(self.schema.clone()));
        }
        if self.access_points.is_empty() || self.access_points.len() > MAX_RECOVERY_ACCESS_POINTS {
            return Err(ManifestError::Invalid {
                field: "accessPoints".into(),
                reason: "must hold 1-2 access points",
            });
        }
        if self.sources.len() > MAX_RECOVERY_SOURCES {
            return Err(ManifestError::Invalid {
                field: "sources".into(),
                reason: "too many sources",
            });
        }
        let mut sources: Vec<BootstrapSource> = self
            .sources
            .iter()
            .map(|url| BootstrapSource::https(url.clone()))
            .collect();
        sources.push(BootstrapSource::well_known());
        let manifest = AccessManifest {
            schema: ACCESS_SCHEMA.to_owned(),
            bootstrap_id: self.bootstrap_id.clone(),
            app_id: self.app_id.clone(),
            serial: self.serial,
            issued_at: self.issued_at.clone(),
            expires_at: self.expires_at.clone(),
            regions: vec![Region {
                id: self.region_id.clone(),
                display_name: BTreeMap::new(),
                cells: vec![Cell {
                    id: self.cell_id.clone(),
                    access_points: self
                        .access_points
                        .iter()
                        .map(|ap| AccessPoint {
                            id: ap.id.clone(),
                            host: ap.host.clone(),
                            ip_hints: ap.ip_hints.clone(),
                            spki_pins: Vec::new(),
                            weight: 100,
                            priority: 0,
                            status: ApStatus::Active,
                            tags: Vec::new(),
                            services: ap.services.clone().unwrap_or_else(standard_services),
                        })
                        .collect(),
                }],
            }],
            placement: Placement {
                home_cell: self.cell_id.clone(),
                allowed_regions: Vec::new(),
            },
            bootstrap: BootstrapSection {
                sources,
                // 这只是临时目录：拿到网络就尽快换回完整清单。
                refresh_interval_sec: Some(MIN_REFRESH_INTERVAL_SEC),
            },
            client_policy: serde_json::Value::Null,
            release: None,
        };
        let times = manifest.validate()?;
        Ok((manifest, times))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ServiceKind;

    pub(crate) fn sample() -> RecoveryManifest {
        RecoveryManifest {
            schema: RECOVERY_SCHEMA.into(),
            bootstrap_id: "b7f3c9a1e2d4b6a8".into(),
            app_id: "app_01J9ZK3X".into(),
            serial: 2000,
            issued_at: "2026-11-04T08:00:00Z".into(),
            expires_at: "2026-11-18T08:00:00Z".into(),
            region_id: "cn-east".into(),
            cell_id: "cell-cne-1".into(),
            access_points: vec![RecoveryAccessPoint {
                id: "ap-rescue-1".into(),
                host: "rescue-a.example.net".into(),
                ip_hints: vec!["203.0.113.20".into()],
                services: None,
            }],
            sources: vec!["https://cfg-c.example.org/flare/v1/b7f3c9a1e2d4b6a8/".into()],
        }
    }

    #[test]
    fn expands_into_a_valid_manifest_with_the_standard_layout() {
        let (manifest, _) = sample().to_access_manifest().unwrap();
        assert_eq!(manifest.serial, 2000);
        assert_eq!(manifest.placement.home_cell, "cell-cne-1");
        let (_, cell) = manifest.home_cell().unwrap();
        let services = &cell.access_points[0].services;
        assert_eq!(services.path(ServiceKind::ImWs), Some("/ws"));
        assert_eq!(services.im_quic.as_ref().unwrap().port, 443);
        assert_eq!(manifest.refresh_interval_sec(), MIN_REFRESH_INTERVAL_SEC);
        let kinds: Vec<_> = manifest
            .bootstrap
            .sources
            .iter()
            .map(|source| source.kind.as_str())
            .collect();
        assert_eq!(kinds, ["https", "wellKnown"]);
    }

    #[test]
    fn holds_one_or_two_access_points_on_public_hosts() {
        let mut none = sample();
        none.access_points.clear();
        assert!(none.to_access_manifest().is_err());

        let mut three = sample();
        for index in 2..=3 {
            three.access_points.push(RecoveryAccessPoint {
                id: format!("ap-rescue-{index}"),
                host: format!("rescue-{index}.example.net"),
                ip_hints: vec![],
                services: None,
            });
        }
        assert!(three.to_access_manifest().is_err());

        let mut bare_ip = sample();
        bare_ip.access_points[0].host = "203.0.113.20".into();
        assert!(bare_ip.to_access_manifest().is_err());

        let mut plain_http = sample();
        plain_http.sources = vec!["http://cfg-c.example.org/".into()];
        assert!(plain_http.to_access_manifest().is_err());

        let mut other = sample();
        other.schema = "flare.access.v1".into();
        assert_eq!(
            other.to_access_manifest().unwrap_err(),
            ManifestError::UnsupportedSchema("flare.access.v1".into())
        );
    }
}
