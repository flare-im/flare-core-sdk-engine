//! 安装计划：Rust 决定装什么、是否可信，平台包执行计划，再用 `update.report_install` 回报结果。

use serde::Serialize;

use crate::manifest::ReleaseArtifact;

/// `kind` 判别的安装计划。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum UpdateInstallPlan {
    OpenStore {
        /// `play`、`appstore`、`testflight`、`appgallery` 或国内商店渠道名。
        store: String,
        url: Option<String>,
        package_name: Option<String>,
        app_id: Option<String>,
        /// Play 上强制更新走 immediate，其余走 flexible。
        immediate: bool,
    },
    InstallPackage(PackagePlan),
    ActivateWebBundle {
        build_id: String,
        asset_manifest_urls: Vec<String>,
        asset_manifest_sha256: String,
        mandatory: bool,
        block_ui: bool,
    },
    DesktopUpdater {
        version: String,
        target: String,
        sha256: Option<String>,
    },
}

/// 已校验的安装包。字段只在 crate 内可写：这份计划只能从下载模块的 `VerifiedArtifact` 产生。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackagePlan {
    pub(crate) path: String,
    pub(crate) sha256: String,
    pub(crate) signing_cert_sha256: Option<String>,
    pub(crate) build: u64,
    pub(crate) size_bytes: u64,
}

impl PackagePlan {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn build(&self) -> u64 {
        self.build
    }
}

/// 商店详情页：清单给了就用，否则按商店的固定格式拼。
pub(crate) fn store_url(store: &str, artifact: &ReleaseArtifact) -> Option<String> {
    artifact.store_url.clone().or_else(|| match store {
        "play" => artifact
            .package_name
            .as_ref()
            .map(|package| format!("https://play.google.com/store/apps/details?id={package}")),
        "appstore" => artifact
            .app_id
            .as_ref()
            .map(|app_id| format!("itms-apps://apps.apple.com/app/id{app_id}")),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_serialize_with_the_wire_names() {
        let plan = UpdateInstallPlan::OpenStore {
            store: "play".into(),
            url: Some("https://x".into()),
            package_name: Some("com.acme.chat".into()),
            app_id: None,
            immediate: false,
        };
        assert_eq!(
            serde_json::to_value(&plan).unwrap(),
            serde_json::json!({
                "kind": "openStore",
                "store": "play",
                "url": "https://x",
                "packageName": "com.acme.chat",
                "appId": null,
                "immediate": false
            })
        );
        let plan = UpdateInstallPlan::InstallPackage(PackagePlan {
            path: "/data/2.4.0-20400.apk".into(),
            sha256: "ab".into(),
            signing_cert_sha256: Some("cd".into()),
            build: 20400,
            size_bytes: 10,
        });
        assert_eq!(
            serde_json::to_value(&plan).unwrap(),
            serde_json::json!({
                "kind": "installPackage",
                "path": "/data/2.4.0-20400.apk",
                "sha256": "ab",
                "signingCertSha256": "cd",
                "build": 20400,
                "sizeBytes": 10
            })
        );
        let plan = UpdateInstallPlan::ActivateWebBundle {
            build_id: "20261105-abc".into(),
            asset_manifest_urls: vec!["https://x".into()],
            asset_manifest_sha256: "ab".into(),
            mandatory: false,
            block_ui: false,
        };
        assert_eq!(
            serde_json::to_value(&plan).unwrap()["assetManifestUrls"],
            serde_json::json!(["https://x"])
        );
    }

    #[test]
    fn store_links_are_derived_when_the_manifest_gives_none() {
        let play = ReleaseArtifact {
            kind: "store".into(),
            package_name: Some("com.acme.chat".into()),
            ..Default::default()
        };
        assert_eq!(
            store_url("play", &play).as_deref(),
            Some("https://play.google.com/store/apps/details?id=com.acme.chat")
        );
        let appstore = ReleaseArtifact {
            kind: "store".into(),
            app_id: Some("123".into()),
            ..Default::default()
        };
        assert_eq!(
            store_url("appstore", &appstore).as_deref(),
            Some("itms-apps://apps.apple.com/app/id123")
        );
        assert_eq!(store_url("appgallery", &appstore), None);
    }
}
