//! 发布清单（Release Manifest）的载荷模型与结构校验。
//!
//! 每个「应用 × 平台 × 渠道」一份，签名方式与接入清单相同。这里只定义文档本身：
//! 控制面在签发前、客户端在验签后跑同一套结构校验；是否提示更新的评估规则不在这里。

use std::collections::{BTreeMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::access::{ManifestError, ManifestTimes, SchemaHeader, invalid, is_valid_id};
use super::schema::{
    MAX_ARTIFACT_URLS, MAX_BLOCKED_BUILD_RANGES, MAX_RELEASE_ARTIFACTS, MAX_RELEASE_TARGETS,
    MAX_RELEASES, MAX_RESOURCE_PACKS, RELEASE_SCHEMA,
};
use crate::parsed_url::ParsedUrl;
use crate::time::parse_rfc3339_ms;

/// 灰度比例的满额：10000 个基点 = 100%。
pub const FULL_ROLLOUT_BASIS_POINTS: u32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseManifest {
    /// 固定为 `flare.release.v1`。
    #[schemars(extend("const" = "flare.release.v1"))]
    pub schema: String,
    pub bootstrap_id: String,
    /// `android`、`ios`、`web`、`tauri` 等；进入镜像路径。
    pub platform: String,
    /// `stable`、`beta`、`internal` 等；进入镜像路径。
    pub channel: String,
    /// 同一（`bootstrapId`, `platform`, `channel`）单调递增。
    pub serial: u64,
    /// RFC 3339。
    #[schemars(extend("format" = "date-time"))]
    pub issued_at: String,
    /// RFC 3339；过期后不再据此给出强制更新。
    #[schemars(extend("format" = "date-time"))]
    pub expires_at: String,
    pub policy: ReleasePolicy,
    /// 同渠道最近的版本：最新灰度版、当前全量版、上一个全量版。
    pub releases: Vec<ReleaseEntry>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReleasePolicy {
    /// 低于此 build 必须更新。
    #[serde(default)]
    pub min_build: u64,
    /// 低于此 build 标记为推荐更新。
    #[serde(default)]
    pub recommend_below_build: u64,
    #[serde(default)]
    pub blocked_builds: Vec<BlockedBuilds>,
    /// 强制更新时阻断界面。
    #[serde(default)]
    pub block_ui: bool,
}

/// 被封禁的 build 闭区间。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BlockedBuilds {
    pub from: u64,
    pub to: u64,
    /// 语言标签 → 提示语。
    #[serde(default)]
    pub reason: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseEntry {
    pub release_id: String,
    /// 只用于显示；版本比较只用 `build`。
    pub version: String,
    /// 单调整数。
    pub build: u64,
    /// RFC 3339。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("format" = "date-time"))]
    pub published_at: Option<String>,
    pub rollout: Rollout,
    /// 语言标签 → 更新说明条目。
    #[serde(default)]
    pub notes: BTreeMap<String, Vec<String>>,
    /// 分发渠道（`play`、`appgallery`、`direct` 等）→ 制品。
    pub artifacts: BTreeMap<String, ReleaseArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_packs: Vec<ResourcePack>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rollout {
    /// 灰度比例，0–10000。
    pub basis_points: u32,
    /// 分桶盐：同一安装在同一版本的灰度中结果稳定。
    pub salt: String,
    #[serde(default)]
    pub halted: bool,
}

/// 一个分发渠道的制品。`kind` 用字符串而不是封闭枚举：新版控制面可以加制品类型，
/// 旧客户端找不到自己认识的类型时按「没有制品」处理。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseArtifact {
    /// `store` | `apk` | `tauri` | `electron` | `web_bundle`。
    pub kind: String,
    /// 商店包名。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_name: Option<String>,
    /// 商店内的应用 ID。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    /// 商店详情页。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_url: Option<String>,
    /// 下载地址，按顺序故障转移。`web_bundle` 时是资产清单的地址。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// 小写十六进制。`web_bundle` 时是资产清单的哈希。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Web 构建标识，`web_bundle` 时必填。Web 的 `build` 不单调可比，是否更新看它。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    /// APK 签名证书指纹，小写十六进制。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_cert_sha256: Option<String>,
    /// 平台自己的包签名，例如 Tauri 的 minisign。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    /// 桌面平台目标（`darwin-aarch64` 等）→ 该目标的制品。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub targets: BTreeMap<String, ArtifactTarget>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactTarget {
    pub urls: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// 资源包：只含数据（词条、表情、token、帮助文档），不含可执行内容。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourcePack {
    pub id: String,
    /// `strings` | `emoji` | `tokens` | `help`。
    pub kind: String,
    pub pack_version: u64,
    #[serde(default)]
    pub min_build: u64,
    pub url: Vec<String>,
    /// 小写十六进制。
    pub sha256: String,
    pub size_bytes: u64,
}

impl ReleaseManifest {
    /// 控制面编排的起点：写好 `schema`，策略与版本由调用方继续填。
    pub fn new(
        bootstrap_id: impl Into<String>,
        platform: impl Into<String>,
        channel: impl Into<String>,
        serial: u64,
        issued_at: impl Into<String>,
        expires_at: impl Into<String>,
    ) -> Self {
        Self {
            schema: RELEASE_SCHEMA.to_owned(),
            bootstrap_id: bootstrap_id.into(),
            platform: platform.into(),
            channel: channel.into(),
            serial,
            issued_at: issued_at.into(),
            expires_at: expires_at.into(),
            policy: ReleasePolicy::default(),
            releases: Vec::new(),
        }
    }

    /// 解析并做结构校验。与接入清单一样先只读 `schema`。
    pub fn from_slice(bytes: &[u8]) -> Result<(Self, ManifestTimes), ManifestError> {
        let header: SchemaHeader = serde_json::from_slice(bytes)
            .map_err(|error| ManifestError::Json(error.to_string()))?;
        if header.schema != RELEASE_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(header.schema));
        }
        let manifest: ReleaseManifest = serde_json::from_slice(bytes)
            .map_err(|error| ManifestError::Json(error.to_string()))?;
        let times = manifest.validate()?;
        Ok((manifest, times))
    }

    /// 校验并序列化成待签名的载荷字节。
    pub fn to_payload(&self) -> Result<Vec<u8>, ManifestError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| ManifestError::Json(error.to_string()))
    }

    /// 结构校验：ID 与路径段合法、时间可解析、版本不重复、灰度比例在范围内、已知制品类型字段完整。
    pub fn validate(&self) -> Result<ManifestTimes, ManifestError> {
        if self.schema != RELEASE_SCHEMA {
            return Err(ManifestError::UnsupportedSchema(self.schema.clone()));
        }
        if !is_valid_id(&self.bootstrap_id) {
            return Err(invalid(
                "bootstrapId",
                "must be 1-64 chars of [A-Za-z0-9._:-]",
            ));
        }
        if !is_valid_path_segment(&self.platform) {
            return Err(invalid("platform", "must be 1-32 chars of [a-z0-9-]"));
        }
        if !is_valid_path_segment(&self.channel) {
            return Err(invalid("channel", "must be 1-32 chars of [a-z0-9-]"));
        }
        let issued_at_ms =
            parse_rfc3339_ms(&self.issued_at).ok_or_else(|| invalid("issuedAt", "not RFC 3339"))?;
        let expires_at_ms = parse_rfc3339_ms(&self.expires_at)
            .ok_or_else(|| invalid("expiresAt", "not RFC 3339"))?;
        if expires_at_ms <= issued_at_ms {
            return Err(invalid("expiresAt", "must be later than issuedAt"));
        }
        self.policy.validate()?;

        if self.releases.len() > MAX_RELEASES {
            return Err(invalid("releases", "must hold at most 3 releases"));
        }
        let mut release_ids = HashSet::new();
        let mut builds = HashSet::new();
        for (index, release) in self.releases.iter().enumerate() {
            let field = format!("releases[{index}]");
            if !is_valid_id(&release.release_id) || !release_ids.insert(&release.release_id) {
                return Err(invalid(
                    format!("{field}.releaseId"),
                    "invalid or duplicate id",
                ));
            }
            if !builds.insert(release.build) {
                return Err(invalid(format!("{field}.build"), "duplicate build"));
            }
            release.validate(&field)?;
        }
        Ok(ManifestTimes {
            issued_at_ms,
            expires_at_ms,
        })
    }
}

impl ReleasePolicy {
    fn validate(&self) -> Result<(), ManifestError> {
        if self.blocked_builds.len() > MAX_BLOCKED_BUILD_RANGES {
            return Err(invalid("policy.blockedBuilds", "too many ranges"));
        }
        for (index, range) in self.blocked_builds.iter().enumerate() {
            if range.from > range.to {
                return Err(invalid(
                    format!("policy.blockedBuilds[{index}]"),
                    "from must not exceed to",
                ));
            }
        }
        Ok(())
    }
}

impl ReleaseEntry {
    fn validate(&self, field: &str) -> Result<(), ManifestError> {
        if self.version.is_empty() || self.version.len() > 64 {
            return Err(invalid(format!("{field}.version"), "must be 1-64 chars"));
        }
        if let Some(published_at) = &self.published_at
            && parse_rfc3339_ms(published_at).is_none()
        {
            return Err(invalid(format!("{field}.publishedAt"), "not RFC 3339"));
        }
        if self.rollout.basis_points > FULL_ROLLOUT_BASIS_POINTS {
            return Err(invalid(
                format!("{field}.rollout.basisPoints"),
                "must be 0-10000",
            ));
        }
        if self.rollout.salt.is_empty() || self.rollout.salt.len() > 128 {
            return Err(invalid(
                format!("{field}.rollout.salt"),
                "must be 1-128 chars",
            ));
        }
        if self.artifacts.is_empty() || self.artifacts.len() > MAX_RELEASE_ARTIFACTS {
            return Err(invalid(
                format!("{field}.artifacts"),
                "must hold 1-32 distributions",
            ));
        }
        for (distribution, artifact) in &self.artifacts {
            let field = format!("{field}.artifacts.{distribution}");
            if !is_valid_id(distribution) {
                return Err(invalid(field, "invalid distribution"));
            }
            artifact.validate(&field)?;
        }
        if self.resource_packs.len() > MAX_RESOURCE_PACKS {
            return Err(invalid(format!("{field}.resourcePacks"), "too many packs"));
        }
        for (index, pack) in self.resource_packs.iter().enumerate() {
            let field = format!("{field}.resourcePacks[{index}]");
            if !is_valid_id(&pack.id) {
                return Err(invalid(format!("{field}.id"), "invalid id"));
            }
            validate_urls(&pack.url, &format!("{field}.url"))?;
            if !is_sha256_hex(&pack.sha256) {
                return Err(invalid(format!("{field}.sha256"), "not 64 hex chars"));
            }
        }
        Ok(())
    }
}

impl ReleaseArtifact {
    fn validate(&self, field: &str) -> Result<(), ManifestError> {
        match self.kind.as_str() {
            "store" => {
                let reference = [&self.package_name, &self.app_id, &self.store_url];
                if reference.iter().all(|value| value.is_none()) {
                    return Err(invalid(
                        field,
                        "store artifact needs packageName, appId or storeUrl",
                    ));
                }
            }
            "apk" => {
                validate_download(&self.urls, self.size_bytes, self.sha256.as_deref(), field)?;
                if !self
                    .signing_cert_sha256
                    .as_deref()
                    .is_some_and(is_sha256_hex)
                {
                    return Err(invalid(
                        format!("{field}.signingCertSha256"),
                        "not 64 hex chars",
                    ));
                }
            }
            "tauri" | "electron" => {
                if self.targets.is_empty() || self.targets.len() > MAX_RELEASE_TARGETS {
                    return Err(invalid(
                        format!("{field}.targets"),
                        "must hold 1-16 targets",
                    ));
                }
                for (target, artifact) in &self.targets {
                    let field = format!("{field}.targets.{target}");
                    if !is_valid_id(target) {
                        return Err(invalid(field, "invalid target"));
                    }
                    validate_download(
                        &artifact.urls,
                        artifact.size_bytes,
                        artifact.sha256.as_deref(),
                        &field,
                    )?;
                }
            }
            "web_bundle" => {
                if !self.build_id.as_deref().is_some_and(is_valid_id) {
                    return Err(invalid(
                        format!("{field}.buildId"),
                        "must be 1-64 chars of [A-Za-z0-9._:-]",
                    ));
                }
                validate_urls(&self.urls, &format!("{field}.urls"))?;
                if !self.sha256.as_deref().is_some_and(is_sha256_hex) {
                    return Err(invalid(format!("{field}.sha256"), "not 64 hex chars"));
                }
            }
            // 未知类型留给新版客户端，旧客户端忽略。
            _ => {}
        }
        Ok(())
    }
}

fn validate_download(
    urls: &[String],
    size_bytes: Option<u64>,
    sha256: Option<&str>,
    field: &str,
) -> Result<(), ManifestError> {
    validate_urls(urls, &format!("{field}.urls"))?;
    if size_bytes.is_none_or(|size| size == 0) {
        return Err(invalid(format!("{field}.sizeBytes"), "must be positive"));
    }
    if !sha256.is_some_and(is_sha256_hex) {
        return Err(invalid(format!("{field}.sha256"), "not 64 hex chars"));
    }
    Ok(())
}

fn validate_urls(urls: &[String], field: &str) -> Result<(), ManifestError> {
    if urls.is_empty() || urls.len() > MAX_ARTIFACT_URLS {
        return Err(invalid(field, "must hold 1-8 URLs"));
    }
    let https = |value: &String| ParsedUrl::parse(value).is_ok_and(|url| url.scheme() == "https");
    if !urls.iter().all(https) {
        return Err(invalid(field, "must be https URLs"));
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// 拼进镜像路径的段（`platform`、`channel`）：1–32 字符的 `[a-z0-9-]`，首尾不是 `-`。
/// Edge 的镜像路由按同一规则放行。
pub fn is_valid_path_segment(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 32
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
        && bytes
            .iter()
            .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'-'))
}

/// 把设备平台的各种写法归一成 CONNECT 词汇：`web`、`h5`、`pc`、`android`、`ios`、`harmonyos`。
///
/// 别名与 flare-core 的 `DevicePlatform::from_str` 一致（`desktop`→`pc`，`mobile_web`→`h5`，
/// `iphone`/`ipad`→`ios`，`harmony`/`openharmony`→`harmonyos`），另外把桌面操作系统名
/// `windows`、`macos`、`linux` 也当作 `pc`。Core 的 CONNECT 门禁和 Social 的 HTTP 门禁都先过这一步，
/// 同一个客户端无论在哪边报哪种写法，判定都一样。认不出的返回 `None`。
pub fn normalize_device_platform(device_platform: &str) -> Option<&'static str> {
    let platform = device_platform.trim().to_ascii_lowercase();
    Some(match platform.as_str() {
        "web" => "web",
        "h5" | "mobile_web" => "h5",
        "pc" | "desktop" | "windows" | "macos" | "linux" => "pc",
        "android" => "android",
        "ios" | "iphone" | "ipad" => "ios",
        "harmonyos" | "harmony" | "openharmony" => "harmonyos",
        _ => return None,
    })
}

/// CONNECT 设备平台 → 发布清单平台段（Core 门禁、Social 门禁、引擎查更新共用这一处）。
///
/// 设备平台先经 [`normalize_device_platform`] 归一。桌面分两种外壳，只看 `distribution`：
/// `tauri` 是 `tauri`，`electron` 是 `electron`；其余（没给、`direct` 等）认不出外壳，返回 `None`——
/// 不设版本门禁，也不查发布清单，免得把 Electron 客户端按 Tauri 的清单拦下或升级。没有发布清单的
/// 平台（鸿蒙等）同样返回 `None`。
pub fn release_platform(device_platform: &str, distribution: Option<&str>) -> Option<&'static str> {
    match normalize_device_platform(device_platform)? {
        "web" | "h5" => Some("web"),
        "android" => Some("android"),
        "ios" => Some("ios"),
        "pc" => {
            let shell = distribution.map(|value| value.trim().to_ascii_lowercase());
            match shell.as_deref() {
                Some("tauri") => Some("tauri"),
                Some("electron") => Some("electron"),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn device_platforms_map_to_release_platform_segments() {
        for (device, distribution, expected) in [
            ("web", None, Some("web")),
            ("h5", None, Some("web")),
            ("mobile_web", None, Some("web")),
            ("android", Some("play"), Some("android")),
            ("ios", Some("appstore"), Some("ios")),
            ("IOS", None, Some("ios")),
            ("iphone", None, Some("ios")),
            ("iPad", Some("appstore"), Some("ios")),
            ("pc", None, None),
            ("pc", Some("direct"), None),
            ("pc", Some("tauri"), Some("tauri")),
            ("pc", Some(" Tauri "), Some("tauri")),
            ("pc", Some("electron"), Some("electron")),
            ("desktop", Some("Electron"), Some("electron")),
            ("desktop", Some("tauri"), Some("tauri")),
            ("windows", Some("direct"), None),
            ("macos", Some("electron"), Some("electron")),
            ("linux", None, None),
            ("harmonyos", Some("appgallery"), None),
            ("harmony", None, None),
            ("openharmony", None, None),
            ("", None, None),
            ("other", None, None),
        ] {
            assert_eq!(
                release_platform(device, distribution),
                expected,
                "{device} / {distribution:?}"
            );
        }
        for (raw, normalized) in [
            ("desktop", Some("pc")),
            ("Windows", Some("pc")),
            ("mobile_web", Some("h5")),
            ("iphone", Some("ios")),
            ("ipad", Some("ios")),
            ("harmony", Some("harmonyos")),
            ("OpenHarmony", Some("harmonyos")),
            (" web ", Some("web")),
            ("fridge", None),
        ] {
            assert_eq!(normalize_device_platform(raw), normalized, "{raw}");
        }
        for segment in ["web", "android", "ios", "tauri", "electron"] {
            assert!(is_valid_path_segment(segment));
        }
    }

    /// 设计文档的示例发布清单（载荷）。
    fn sample() -> serde_json::Value {
        serde_json::json!({
            "schema": "flare.release.v1",
            "bootstrapId": "b7f3c9a1e2d4b6a8",
            "platform": "android",
            "channel": "stable",
            "serial": 88,
            "issuedAt": "2026-11-04T08:00:00Z",
            "expiresAt": "2026-11-18T08:00:00Z",
            "policy": {
                "minBuild": 20100,
                "recommendBelowBuild": 20300,
                "blockedBuilds": [{
                    "from": 20210,
                    "to": 20212,
                    "reason": { "zh-CN": "该版本存在数据同步缺陷，请更新" }
                }],
                "blockUi": false
            },
            "releases": [
                {
                    "releaseId": "rel_01JC8W",
                    "version": "2.4.0",
                    "build": 20400,
                    "publishedAt": "2026-11-04T08:00:00Z",
                    "rollout": { "basisPoints": 500, "salt": "rel_01JC8W", "halted": false },
                    "notes": { "zh-CN": ["新增消息定时发送"], "en": ["Scheduled messages"] },
                    "artifacts": {
                        "play": { "kind": "store", "packageName": "com.acme.chat" },
                        "appgallery": { "kind": "store", "appId": "C107654321" },
                        "direct": {
                            "kind": "apk",
                            "urls": ["https://dl-a.example-cdn.com/acme/2.4.0-20400.apk"],
                            "sizeBytes": 52428800,
                            "sha256": DIGEST,
                            "signingCertSha256": DIGEST
                        }
                    },
                    "resourcePacks": []
                },
                {
                    "releaseId": "rel_01JB2Q",
                    "version": "2.3.2",
                    "build": 20302,
                    "rollout": { "basisPoints": 10000, "salt": "rel_01JB2Q", "halted": false },
                    "notes": { "zh-CN": ["稳定性改进"] },
                    "artifacts": { "play": { "kind": "store", "packageName": "com.acme.chat" } }
                }
            ]
        })
    }

    fn parse(value: &serde_json::Value) -> Result<ReleaseManifest, ManifestError> {
        ReleaseManifest::from_slice(&serde_json::to_vec(value).unwrap())
            .map(|(manifest, _)| manifest)
    }

    #[test]
    fn parses_design_sample_and_round_trips() {
        let manifest = parse(&sample()).unwrap();
        assert_eq!(manifest.serial, 88);
        assert_eq!(manifest.policy.min_build, 20100);
        assert_eq!(manifest.releases[0].rollout.basis_points, 500);
        assert_eq!(
            manifest.releases[0].artifacts["direct"].size_bytes,
            Some(52_428_800)
        );
        assert_eq!(parse_slice(&manifest.to_payload().unwrap()), manifest);
    }

    fn parse_slice(bytes: &[u8]) -> ReleaseManifest {
        ReleaseManifest::from_slice(bytes).unwrap().0
    }

    #[test]
    fn unknown_artifact_kinds_and_fields_are_tolerated() {
        let mut value = sample();
        value["futureField"] = serde_json::json!({ "x": 1 });
        value["releases"][1]["artifacts"]["future"] = serde_json::json!({ "kind": "hologram" });
        assert!(parse(&value).is_ok());
    }

    #[test]
    fn web_bundles_name_their_build_and_asset_manifest() {
        let mut value = sample();
        value["releases"][1]["artifacts"]["web"] = serde_json::json!({
            "kind": "web_bundle",
            "buildId": "20261105-abc",
            "urls": ["https://app.example.com/assets/20261105-abc.json"],
            "sha256": DIGEST
        });
        let manifest = parse(&value).unwrap();
        assert_eq!(
            manifest.releases[1].artifacts["web"].build_id.as_deref(),
            Some("20261105-abc")
        );
        assert_eq!(parse_slice(&manifest.to_payload().unwrap()), manifest);

        for field in ["buildId", "urls", "sha256"] {
            let mut missing = value.clone();
            missing["releases"][1]["artifacts"]["web"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(
                matches!(parse(&missing), Err(ManifestError::Invalid { .. })),
                "web_bundle without {field} should be rejected"
            );
        }
    }

    #[test]
    fn desktop_artifacts_list_targets() {
        let mut value = sample();
        value["releases"][1]["artifacts"]["tauri"] = serde_json::json!({
            "kind": "tauri",
            "targets": {
                "darwin-aarch64": {
                    "urls": ["https://dl-a.example-cdn.com/acme/app.tar.gz"],
                    "sizeBytes": 1024,
                    "sha256": DIGEST,
                    "signature": "minisign"
                }
            }
        });
        assert!(parse(&value).is_ok());
        value["releases"][1]["artifacts"]["tauri"]["targets"] = serde_json::json!({});
        assert!(matches!(parse(&value), Err(ManifestError::Invalid { .. })));
    }

    type Mutation = Box<dyn Fn(&mut serde_json::Value)>;

    #[test]
    fn rejects_structural_problems() {
        let cases: Vec<(&str, Mutation)> = vec![
            (
                "bad platform",
                Box::new(|v| v["platform"] = "../android".into()),
            ),
            (
                "upper channel",
                Box::new(|v| v["channel"] = "Stable".into()),
            ),
            (
                "expiry before issue",
                Box::new(|v| v["expiresAt"] = "2026-11-01T00:00:00Z".into()),
            ),
            (
                "inverted blocked range",
                Box::new(|v| v["policy"]["blockedBuilds"][0]["from"] = 20213.into()),
            ),
            (
                "rollout over 100%",
                Box::new(|v| v["releases"][0]["rollout"]["basisPoints"] = 10001.into()),
            ),
            (
                "empty salt",
                Box::new(|v| v["releases"][0]["rollout"]["salt"] = "".into()),
            ),
            (
                "duplicate build",
                Box::new(|v| v["releases"][1]["build"] = 20400.into()),
            ),
            (
                "duplicate release id",
                Box::new(|v| v["releases"][1]["releaseId"] = "rel_01JC8W".into()),
            ),
            (
                "too many releases",
                Box::new(|v| {
                    let extra = v["releases"][1].clone();
                    let releases = v["releases"].as_array_mut().unwrap();
                    for build in 1..=2u64 {
                        let mut release = extra.clone();
                        release["releaseId"] = format!("rel_{build}").into();
                        release["build"] = build.into();
                        releases.push(release);
                    }
                }),
            ),
            (
                "no artifacts",
                Box::new(|v| v["releases"][1]["artifacts"] = serde_json::json!({})),
            ),
            (
                "store without reference",
                Box::new(|v| {
                    v["releases"][1]["artifacts"]["play"] = serde_json::json!({ "kind": "store" })
                }),
            ),
            (
                "http download",
                Box::new(|v| {
                    v["releases"][0]["artifacts"]["direct"]["urls"] =
                        serde_json::json!(["http://dl.example.com/a.apk"])
                }),
            ),
            (
                "apk without size",
                Box::new(|v| {
                    v["releases"][0]["artifacts"]["direct"]
                        .as_object_mut()
                        .unwrap()
                        .remove("sizeBytes");
                }),
            ),
            (
                "bad digest",
                Box::new(|v| v["releases"][0]["artifacts"]["direct"]["sha256"] = "abc".into()),
            ),
            (
                "bad signing cert",
                Box::new(|v| {
                    v["releases"][0]["artifacts"]["direct"]["signingCertSha256"] =
                        DIGEST.to_uppercase().into()
                }),
            ),
        ];
        for (name, mutate) in cases {
            let mut value = sample();
            mutate(&mut value);
            assert!(
                matches!(parse(&value), Err(ManifestError::Invalid { .. })),
                "{name} should be rejected"
            );
        }
    }

    #[test]
    fn unsupported_schema_is_reported_before_structure() {
        assert_eq!(
            ReleaseManifest::from_slice(br#"{"schema":"flare.release.v2","other":1}"#).unwrap_err(),
            ManifestError::UnsupportedSchema("flare.release.v2".into())
        );
    }

    #[test]
    fn path_segments_match_the_edge_route() {
        for value in ["android", "stable", "beta-2", "a"] {
            assert!(is_valid_path_segment(value), "{value}");
        }
        let long = "a".repeat(33);
        for value in ["", "-a", "a-", "A", "a/b", "a.b", "..", long.as_str()] {
            assert!(!is_valid_path_segment(value), "{value}");
        }
    }

    #[test]
    fn builder_produces_a_valid_empty_manifest() {
        let manifest = ReleaseManifest::new(
            "b7f3c9a1e2d4b6a8",
            "ios",
            "beta",
            1,
            "2026-11-04T08:00:00Z",
            "2026-11-18T08:00:00Z",
        );
        assert_eq!(parse_slice(&manifest.to_payload().unwrap()), manifest);
    }
}
