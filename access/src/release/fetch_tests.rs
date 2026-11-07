//! 发布清单拉取：假镜像，走真实的验签、防回滚与落盘。

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{Value, json};

use super::*;
use crate::bootstrap::{AccessConfig, FetchResponse};
use crate::kernel::KernelOptions;
use crate::store::MemoryAccessStore;
use crate::trust::TrustRootConfig;
use crate::trust::sign::encode_public_key;
use crate::trust::testkit::*;

const MIRROR_A: &str = "https://mirror-a.example.com/flare/v1/b7f3c9a1e2d4b6a8/";
const MIRROR_B: &str = "https://mirror-b.example.net/flare/v1/b7f3c9a1e2d4b6a8/";
const RELEASE_FILE: &str = "release/android/stable.dsse.json";

#[derive(Default)]
struct FakeMirrors {
    routes: Mutex<HashMap<String, Result<FetchResponse, FetchError>>>,
    log: Mutex<Vec<FetchRequest>>,
}

impl FakeMirrors {
    fn serve(&self, base: &str, etag: &str, body: Vec<u8>) {
        self.routes.lock().unwrap().insert(
            format!("{}/{RELEASE_FILE}", base.trim_end_matches('/')),
            Ok(FetchResponse {
                status: 200,
                etag: Some(etag.to_owned()),
                body,
            }),
        );
    }

    fn fail(&self, base: &str, error: FetchError) {
        self.routes.lock().unwrap().insert(
            format!("{}/{RELEASE_FILE}", base.trim_end_matches('/')),
            Err(error),
        );
    }
}

#[async_trait::async_trait]
impl HttpFetch for FakeMirrors {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        self.log.lock().unwrap().push(request.clone());
        let route = self.routes.lock().unwrap().get(&request.url).cloned();
        match route {
            Some(Ok(response))
                if response.etag.is_some() && response.etag == request.if_none_match =>
            {
                Ok(FetchResponse {
                    status: 304,
                    etag: response.etag,
                    body: Vec::new(),
                })
            }
            Some(route) => route,
            None => Ok(FetchResponse {
                status: 404,
                etag: None,
                body: Vec::new(),
            }),
        }
    }
}

fn release(serial: u64, channel: &str) -> Value {
    json!({
        "schema": "flare.release.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "platform": "android",
        "channel": channel,
        "serial": serial,
        "issuedAt": "2026-11-04T08:00:00Z",
        "expiresAt": "2026-11-18T08:00:00Z",
        "policy": {},
        "releases": []
    })
}

fn signed(serial: u64) -> Vec<u8> {
    signed_release(&release(serial, "stable"))
}

async fn lab() -> (Arc<AccessKernel>, Arc<FakeMirrors>, ReleaseFetcher) {
    let mut seed = manifest_with_serial(10);
    seed["bootstrap"]["sources"] = json!([
        { "kind": "https", "url": MIRROR_A },
        { "kind": "https", "url": MIRROR_B }
    ]);
    let config = AccessConfig::production(
        BOOTSTRAP_ID,
        vec![TrustRootConfig {
            key_id: ROOT_ID.into(),
            public_key: encode_public_key(&root_key().verifying_key()),
        }],
        Some(String::from_utf8(signed_manifest(&seed)).unwrap()),
    );
    let (kernel, _) = AccessKernel::bootstrap(
        &config,
        Arc::new(MemoryAccessStore::new()),
        KernelOptions {
            quic_supported: true,
        },
        NOW_MS,
    )
    .await
    .unwrap();
    let kernel = Arc::new(kernel);
    let net = Arc::new(FakeMirrors::default());
    let fetcher = ReleaseFetcher::new(kernel.clone(), net.clone(), "android", "stable");
    (kernel, net, fetcher)
}

#[tokio::test]
async fn the_first_valid_document_is_adopted_and_kept() {
    let (_, net, fetcher) = lab().await;
    net.fail(MIRROR_A, FetchError::Unreachable);
    net.serve(MIRROR_B, "\"88\"", signed(88));

    let report = fetcher.fetch(NOW_MS).await;
    assert_eq!(report.outcome, ReleaseFetchOutcome::Adopted);
    assert_eq!(report.manifest.unwrap().serial, 88);
    assert_eq!(fetcher.held(NOW_MS).await.unwrap().serial, 88);
    let log = net.log.lock().unwrap();
    assert_eq!(
        log[1].url,
        format!("{}/{RELEASE_FILE}", MIRROR_B.trim_end_matches('/'))
    );
    assert_eq!(log[1].max_body_bytes, MAX_DOCUMENT_BYTES);
}

#[tokio::test]
async fn the_next_round_asks_the_same_source_conditionally() {
    let (_, net, fetcher) = lab().await;
    net.fail(MIRROR_A, FetchError::Unreachable);
    net.serve(MIRROR_B, "\"88\"", signed(88));
    fetcher.fetch(NOW_MS).await;
    net.log.lock().unwrap().clear();

    let report = fetcher.fetch(NOW_MS).await;
    assert_eq!(report.outcome, ReleaseFetchOutcome::UpToDate);
    assert_eq!(report.manifest.unwrap().serial, 88);
    let log = net.log.lock().unwrap();
    assert_eq!(log.len(), 1, "上次的源排最前，304 就结束");
    assert_eq!(log[0].if_none_match.as_deref(), Some("\"88\""));
}

#[tokio::test]
async fn a_rollback_never_replaces_the_manifest_in_hand() {
    let (_, net, fetcher) = lab().await;
    net.serve(MIRROR_A, "\"88\"", signed(88));
    fetcher.fetch(NOW_MS).await;

    net.serve(MIRROR_A, "\"87\"", signed(87));
    net.serve(MIRROR_B, "\"87\"", signed(87));
    let report = fetcher.fetch(NOW_MS).await;
    assert_eq!(report.outcome, ReleaseFetchOutcome::UpToDate);
    assert_eq!(report.manifest.unwrap().serial, 88);
    assert_eq!(fetcher.held(NOW_MS).await.unwrap().serial, 88);
}

#[tokio::test]
async fn forged_and_misdirected_documents_are_rejected() {
    let (_, net, fetcher) = lab().await;
    let mut forged = crate::trust::Envelope::parse(&signed(88)).unwrap();
    forged.payload = crate::trust::encode_b64(&serde_json::to_vec(&release(89, "stable")).unwrap());
    net.serve(MIRROR_A, "\"x\"", forged.to_json_bytes());
    net.serve(MIRROR_B, "\"y\"", signed_release(&release(90, "beta")));

    let report = fetcher.fetch(NOW_MS).await;
    assert_eq!(
        report.outcome,
        ReleaseFetchOutcome::Rejected {
            code: "badSignature".into()
        }
    );
    assert!(report.manifest.is_none());
    assert!(fetcher.held(NOW_MS).await.is_none());
}

#[tokio::test]
async fn nothing_published_and_nothing_reachable_are_different_answers() {
    let (_, net, fetcher) = lab().await;
    assert_eq!(
        fetcher.fetch(NOW_MS).await.outcome,
        ReleaseFetchOutcome::NotPublished
    );
    net.fail(MIRROR_A, FetchError::Timeout);
    net.fail(MIRROR_B, FetchError::Timeout);
    assert_eq!(
        fetcher.fetch(NOW_MS).await.outcome,
        ReleaseFetchOutcome::Unavailable {
            code: Some("timeout".into())
        }
    );
}

/// 另一个运行时抢在这一轮之前存下同一份新清单。
struct RacedMirrors {
    kernel: Arc<AccessKernel>,
    mirrors: Arc<FakeMirrors>,
}

#[async_trait::async_trait]
impl HttpFetch for RacedMirrors {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        ReleaseFetcher::new(
            self.kernel.clone(),
            self.mirrors.clone(),
            "android",
            "stable",
        )
        .fetch(NOW_MS)
        .await;
        self.mirrors.fetch(request).await
    }
}

/// 浏览器里每个标签页一个运行时，共用一份存储。这一轮开头读到的清单可能已经过时：按它评估的话，
/// 资源包同步会把新清单刚下载的包删掉。
#[tokio::test]
async fn the_round_answers_with_what_another_runtime_stored_meanwhile() {
    let (kernel, http, fetcher) = lab().await;
    http.serve(MIRROR_A, "\"v1\"", signed(1));
    fetcher.fetch(NOW_MS).await;
    http.serve(MIRROR_A, "\"v2\"", signed(2));

    let raced = ReleaseFetcher::new(
        kernel.clone(),
        Arc::new(RacedMirrors {
            kernel,
            mirrors: http,
        }),
        "android",
        "stable",
    );
    let report = raced.fetch(NOW_MS).await;

    assert_eq!(report.outcome, ReleaseFetchOutcome::UpToDate);
    assert_eq!(report.manifest.expect("manifest").manifest.serial, 2);
}
