//! 发布清单评估（§8.3）：纯函数，输入一份已验签的清单和本机信息，输出该提示什么。
//!
//! 原生只比 `build`（单调整数）。Web 的 `build` 不单调可比，看 `buildId` 是否与运行中的不同，
//! 这样回滚（把最新版本指回旧 `buildId`）和升级走同一条路。

use std::cmp::Reverse;

use sha2::{Digest, Sha256};

use super::locale::select_text;
use crate::manifest::release::FULL_ROLLOUT_BASIS_POINTS;
use crate::manifest::{
    BlockedBuilds, ReleaseArtifact, ReleaseEntry, ReleaseManifest, ReleasePolicy,
};

/// 灰度分桶：`u32_be(SHA-256(installId ":" salt)[0..4]) mod 10000`。
/// 同一安装在同一版本的灰度中结果稳定，各版本的盐不同，所以不总是同一批人先拿到新版本。
pub fn bucket(install_id: &str, salt: &str) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(install_id.as_bytes());
    hasher.update(b":");
    hasher.update(salt.as_bytes());
    let digest = hasher.finalize();
    u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % FULL_ROLLOUT_BASIS_POINTS
}

/// 服务端已经拒绝了这个版本（网关 `UPGRADE_REQUIRED` 或 REST 426）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerGate {
    /// 服务端要求的最低构建号，不一定有。
    pub min_build: Option<u64>,
}

/// 本机信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationInput<'a> {
    /// 当前构建号。原生必须有；Web 有就用来判断强制更新。
    pub current_build: Option<u64>,
    /// 运行中的 Web 构建。有值即按 Web 规则评估。
    pub current_build_id: Option<&'a str>,
    pub distribution: &'a str,
    /// 桌面目标，例如 `darwin-aarch64`。
    pub target: Option<&'a str>,
    pub install_id: &'a str,
    /// 清单已过期：只给可选提示，不据此强制（以服务端门禁为准）。
    pub stale: bool,
    pub server: Option<ServerGate>,
}

/// 更新的紧迫程度，决定提示形态与能否跳过。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Urgency {
    Optional,
    Recommended,
    Required { block_ui: bool },
}

impl Urgency {
    pub fn is_required(self) -> bool {
        matches!(self, Urgency::Required { .. })
    }
}

/// 评估结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision<'a> {
    UpToDate,
    Available {
        release: &'a ReleaseEntry,
        recommended: bool,
    },
    /// `release` 为空：必须更新，但清单里没有本机能用的版本。
    Required {
        release: Option<&'a ReleaseEntry>,
        block_ui: bool,
    },
}

/// 本机能用的制品。类型不认识、桌面目标缺失都算没有。
pub fn artifact_for<'a>(
    release: &'a ReleaseEntry,
    distribution: &str,
    target: Option<&str>,
) -> Option<&'a ReleaseArtifact> {
    let artifact = release.artifacts.get(distribution)?;
    let usable = match artifact.kind.as_str() {
        "store" | "apk" => true,
        "web_bundle" => artifact.build_id.is_some(),
        "tauri" | "electron" => target.is_some_and(|target| artifact.targets.contains_key(target)),
        _ => false,
    };
    usable.then_some(artifact)
}

fn usable(release: &ReleaseEntry, input: &EvaluationInput<'_>) -> bool {
    !release.rollout.halted && artifact_for(release, input.distribution, input.target).is_some()
}

fn in_rollout(release: &ReleaseEntry, input: &EvaluationInput<'_>) -> bool {
    bucket(input.install_id, &release.rollout.salt) < release.rollout.basis_points
}

fn covers(range: &BlockedBuilds, build: u64) -> bool {
    (range.from..=range.to).contains(&build)
}

fn is_blocked(policy: &ReleasePolicy, build: u64) -> bool {
    policy
        .blocked_builds
        .iter()
        .any(|range| covers(range, build))
}

/// 这个构建被封禁的原因，已按语言选好。区间没写原因时看下一个命中的区间，都没有时为空，
/// 由界面用固定文案。
pub fn block_reason(policy: &ReleasePolicy, build: u64, locale: Option<&str>) -> Option<String> {
    policy
        .blocked_builds
        .iter()
        .filter(|range| covers(range, build))
        .find_map(|range| {
            select_text(&range.reason, locale).filter(|reason| !reason.trim().is_empty())
        })
}

fn policy_requires(policy: &ReleasePolicy, build: u64) -> bool {
    build < policy.min_build || is_blocked(policy, build)
}

/// 评估。
pub fn evaluate<'a>(manifest: &'a ReleaseManifest, input: &EvaluationInput<'_>) -> Decision<'a> {
    match input.current_build_id {
        Some(current) => evaluate_web(manifest, input, current),
        None => evaluate_native(manifest, input),
    }
}

fn evaluate_native<'a>(manifest: &'a ReleaseManifest, input: &EvaluationInput<'_>) -> Decision<'a> {
    let Some(current) = input.current_build else {
        return Decision::UpToDate;
    };
    let policy = &manifest.policy;
    let mut newer: Vec<&ReleaseEntry> = manifest
        .releases
        .iter()
        .filter(|release| release.build > current && usable(release, input))
        .collect();
    newer.sort_by_key(|release| Reverse(release.build));

    if input.server.is_some() || (!input.stale && policy_requires(policy, current)) {
        let floor = input
            .server
            .and_then(|server| server.min_build)
            .unwrap_or(0)
            .max(policy.min_build);
        return Decision::Required {
            release: required_target(&newer, policy, floor, input),
            block_ui: policy.block_ui,
        };
    }
    match newer
        .iter()
        .copied()
        .find(|release| in_rollout(release, input))
    {
        Some(release) => Decision::Available {
            release,
            recommended: current < policy.recommend_below_build,
        },
        None => Decision::UpToDate,
    }
}

/// 强制更新的目标：优先灰度命中的；都没命中也要给一个，灰度只决定提示谁，不能把必须更新的人挡在外面。
fn required_target<'a>(
    newer: &[&'a ReleaseEntry],
    policy: &ReleasePolicy,
    floor: u64,
    input: &EvaluationInput<'_>,
) -> Option<&'a ReleaseEntry> {
    let acceptable =
        |release: &ReleaseEntry| release.build >= floor && !is_blocked(policy, release.build);
    newer
        .iter()
        .copied()
        .find(|release| acceptable(release) && in_rollout(release, input))
        .or_else(|| newer.iter().copied().find(|release| acceptable(release)))
        .or_else(|| newer.first().copied())
}

fn evaluate_web<'a>(
    manifest: &'a ReleaseManifest,
    input: &EvaluationInput<'_>,
    current: &str,
) -> Decision<'a> {
    let policy = &manifest.policy;
    let eligible: Vec<&ReleaseEntry> = manifest
        .releases
        .iter()
        .filter(|release| usable(release, input))
        .collect();
    // 第一个命中灰度的版本就是这台设备该跑的版本；清单顺序由控制面决定，回滚时旧 buildId 排在最前。
    let hit = eligible
        .iter()
        .copied()
        .find(|release| in_rollout(release, input));
    let differs = |release: &ReleaseEntry| {
        artifact_for(release, input.distribution, input.target)
            .and_then(|artifact| artifact.build_id.as_deref())
            != Some(current)
    };
    let required = input.server.is_some()
        || (!input.stale
            && input
                .current_build
                .is_some_and(|build| policy_requires(policy, build)));
    if required {
        return Decision::Required {
            release: hit
                .filter(|release| differs(release))
                .or_else(|| eligible.iter().copied().find(|release| differs(release))),
            block_ui: policy.block_ui,
        };
    }
    match hit.filter(|release| differs(release)) {
        Some(release) => Decision::Available {
            release,
            recommended: input
                .current_build
                .is_some_and(|build| build < policy.recommend_below_build),
        },
        None => Decision::UpToDate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn manifest(value: serde_json::Value) -> ReleaseManifest {
        ReleaseManifest::from_slice(&serde_json::to_vec(&value).unwrap())
            .unwrap()
            .0
    }

    fn release(id: &str, build: u64, basis_points: u32) -> serde_json::Value {
        serde_json::json!({
            "releaseId": id,
            "version": format!("2.{build}"),
            "build": build,
            "rollout": { "basisPoints": basis_points, "salt": id, "halted": false },
            "artifacts": { "play": { "kind": "store", "packageName": "com.acme.chat" } }
        })
    }

    fn native(policy: serde_json::Value, releases: Vec<serde_json::Value>) -> ReleaseManifest {
        manifest(serde_json::json!({
            "schema": "flare.release.v1",
            "bootstrapId": "b7f3c9a1e2d4b6a8",
            "platform": "android",
            "channel": "stable",
            "serial": 1,
            "issuedAt": "2026-11-04T08:00:00Z",
            "expiresAt": "2026-11-18T08:00:00Z",
            "policy": policy,
            "releases": releases
        }))
    }

    fn input(build: u64) -> EvaluationInput<'static> {
        EvaluationInput {
            current_build: Some(build),
            current_build_id: None,
            distribution: "play",
            target: None,
            install_id: "install-1",
            stale: false,
            server: None,
        }
    }

    #[test]
    fn the_bucket_is_stable_and_in_range() {
        assert_eq!(bucket("install-1", "rel_a"), bucket("install-1", "rel_a"));
        assert_ne!(bucket("install-1", "rel_a"), bucket("install-1", "rel_b"));
        let mut hits = 0;
        for index in 0..10_000 {
            let value = bucket(&format!("install-{index}"), "rel_01JC8W");
            assert!(value < FULL_ROLLOUT_BASIS_POINTS);
            if value < 500 {
                hits += 1;
            }
        }
        // 验收：1 万个安装在 5% 灰度下命中 5%±0.5%。
        assert!((450..=550).contains(&hits), "hits {hits}");
    }

    #[test]
    fn users_outside_the_rollout_fall_back_to_the_fully_rolled_out_release() {
        let manifest = native(
            serde_json::json!({}),
            vec![release("rel_new", 300, 0), release("rel_old", 200, 10_000)],
        );
        match evaluate(&manifest, &input(100)) {
            Decision::Available { release, .. } => assert_eq!(release.build, 200),
            other => panic!("{other:?}"),
        }
        assert_eq!(evaluate(&manifest, &input(200)), Decision::UpToDate);
    }

    #[test]
    fn halted_releases_and_missing_distributions_are_skipped() {
        let mut halted = release("rel_new", 300, 10_000);
        halted["rollout"]["halted"] = true.into();
        let manifest = native(serde_json::json!({}), vec![halted]);
        assert_eq!(evaluate(&manifest, &input(100)), Decision::UpToDate);

        let manifest = native(serde_json::json!({}), vec![release("rel_new", 300, 10_000)]);
        let other = EvaluationInput {
            distribution: "direct",
            ..input(100)
        };
        assert_eq!(evaluate(&manifest, &other), Decision::UpToDate);
    }

    #[test]
    fn the_policy_forces_old_and_blocked_builds_unless_the_manifest_expired() {
        let manifest = native(
            serde_json::json!({
                "minBuild": 150,
                "recommendBelowBuild": 250,
                "blockedBuilds": [{ "from": 180, "to": 190 }],
                "blockUi": true
            }),
            vec![release("rel_new", 300, 10_000)],
        );
        for build in [100, 185] {
            match evaluate(&manifest, &input(build)) {
                Decision::Required { release, block_ui } => {
                    assert_eq!(release.map(|release| release.build), Some(300));
                    assert!(block_ui);
                }
                other => panic!("{other:?}"),
            }
        }
        match evaluate(&manifest, &input(200)) {
            Decision::Available { recommended, .. } => assert!(recommended),
            other => panic!("{other:?}"),
        }
        match evaluate(&manifest, &input(260)) {
            Decision::Available { recommended, .. } => assert!(!recommended),
            other => panic!("{other:?}"),
        }
        let stale = EvaluationInput {
            stale: true,
            ..input(100)
        };
        assert!(matches!(
            evaluate(&manifest, &stale),
            Decision::Available { .. }
        ));
    }

    #[test]
    fn the_reason_of_a_blocked_build_comes_in_the_chosen_language() {
        let policy: ReleasePolicy = serde_json::from_value(serde_json::json!({
            "blockedBuilds": [
                { "from": 100, "to": 110 },
                { "from": 105, "to": 120, "reason": { "en": "Data loss", "zh-CN": "数据丢失" } },
                { "from": 130, "to": 130, "reason": { "en": "  " } }
            ]
        }))
        .unwrap();
        assert_eq!(block_reason(&policy, 99, Some("en")), None);
        assert_eq!(
            block_reason(&policy, 102, Some("en")),
            None,
            "命中但没写原因"
        );
        assert_eq!(
            block_reason(&policy, 107, Some("zh-Hans-CN")).as_deref(),
            Some("数据丢失"),
            "第一个命中的区间没写原因，看下一个"
        );
        assert_eq!(
            block_reason(&policy, 120, None).as_deref(),
            Some("Data loss")
        );
        assert_eq!(block_reason(&policy, 130, Some("en")), None, "空白不算原因");
    }

    #[test]
    fn a_server_rejection_forces_even_past_the_rollout_and_the_expiry() {
        let manifest = native(serde_json::json!({}), vec![release("rel_new", 300, 0)]);
        let rejected = EvaluationInput {
            stale: true,
            server: Some(ServerGate {
                min_build: Some(250),
            }),
            ..input(100)
        };
        match evaluate(&manifest, &rejected) {
            Decision::Required { release, .. } => {
                assert_eq!(release.map(|release| release.build), Some(300));
            }
            other => panic!("{other:?}"),
        }
        let nothing = native(serde_json::json!({}), Vec::new());
        assert_eq!(
            evaluate(&nothing, &rejected),
            Decision::Required {
                release: None,
                block_ui: false
            }
        );
    }

    fn web_release(id: &str, build: u64, build_id: &str, basis_points: u32) -> serde_json::Value {
        serde_json::json!({
            "releaseId": id,
            "version": "2.4.0",
            "build": build,
            "rollout": { "basisPoints": basis_points, "salt": id, "halted": false },
            "artifacts": { "web": {
                "kind": "web_bundle",
                "buildId": build_id,
                "urls": ["https://app.example.com/assets.json"],
                "sha256": DIGEST
            } }
        })
    }

    #[test]
    fn web_compares_build_ids_so_a_rollback_is_an_update_too() {
        let manifest = native(
            serde_json::json!({}),
            vec![
                web_release("rel_back", 10, "bundle-old", 10_000),
                web_release("rel_bad", 20, "bundle-bad", 10_000),
            ],
        );
        let web = |current| EvaluationInput {
            current_build: None,
            current_build_id: Some(current),
            distribution: "web",
            target: None,
            install_id: "install-1",
            stale: false,
            server: None,
        };
        match evaluate(&manifest, &web("bundle-bad")) {
            Decision::Available { release, .. } => assert_eq!(release.build, 10),
            other => panic!("{other:?}"),
        }
        assert_eq!(evaluate(&manifest, &web("bundle-old")), Decision::UpToDate);
    }
}
