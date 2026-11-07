//! 运行时：假镜像，走真实的状态机、验签与本地存储。

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use flare_access::AccessConfig;
use flare_access::bootstrap::{FetchError, FetchRequest, FetchResponse};
use flare_access::release::{ReleaseKind, UpdateState};
use flare_access::store::MemoryAccessStore;
use flare_access::time::{DAY_MS, format_rfc3339_ms};
use flare_access::trust::sign::{
    Signer, SigningKey, encode_public_key, issue_key_cert, sign_access_manifest,
    sign_release_manifest,
};
use flare_access::trust::{KeyCert, KeyScope, TrustRootConfig};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::*;
use crate::runtime::link::AccessStoreSource;

const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
const MIRROR: &str = "https://mirror-a.example.com/flare/v1/b7f3c9a1e2d4b6a8/";

fn root_key() -> SigningKey {
    SigningKey::from_bytes(&[0x11; 32])
}

fn online_key() -> SigningKey {
    SigningKey::from_bytes(&[0x33; 32])
}

fn cert() -> String {
    let root = root_key();
    issue_key_cert(
        &Signer {
            key_id: "root-a",
            key: &root,
        },
        &KeyCert {
            schema: "flare.keycert.v1".into(),
            key_id: "ok-1".into(),
            public_key: encode_public_key(&online_key().verifying_key()),
            usages: vec!["access".into(), "release".into()],
            scope: KeyScope {
                bootstrap_ids: vec!["*".into()],
            },
            not_before: format_rfc3339_ms(now_ms() - DAY_MS),
            not_after: format_rfc3339_ms(now_ms() + 90 * DAY_MS),
            root_key_id: "root-a".into(),
        },
    )
}

fn signer<'a>(key: &'a SigningKey) -> Signer<'a> {
    Signer {
        key_id: "ok-1",
        key,
    }
}

fn seed() -> String {
    seed_with(json!([{ "kind": "https", "url": MIRROR }]))
}

fn seed_with(sources: Value) -> String {
    let payload = json!({
        "schema": "flare.access.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "appId": "app_01J9ZK3X",
        "serial": 10,
        "issuedAt": format_rfc3339_ms(now_ms() - DAY_MS),
        "expiresAt": format_rfc3339_ms(now_ms() + 30 * DAY_MS),
        "regions": [],
        "bootstrap": { "sources": sources }
    });
    let key = online_key();
    String::from_utf8(sign_access_manifest(
        &serde_json::to_vec(&payload).unwrap(),
        &signer(&key),
        &[cert()],
    ))
    .unwrap()
}

fn release_document(policy: Value) -> Vec<u8> {
    let payload = json!({
        "schema": "flare.release.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "platform": release_platform(Some("play")),
        "channel": "stable",
        "serial": 88,
        "issuedAt": format_rfc3339_ms(now_ms() - DAY_MS),
        "expiresAt": format_rfc3339_ms(now_ms() + 14 * DAY_MS),
        "policy": policy,
        "releases": [{
            "releaseId": "rel_01JC8W",
            "version": "2.4.0",
            "build": 20400,
            "rollout": { "basisPoints": 10000, "salt": "rel_01JC8W", "halted": false },
            "notes": { "en": ["Scheduled messages"], "zh-CN": ["定时发送"] },
            "artifacts": { "play": { "kind": "store", "packageName": "com.acme.chat" } }
        }]
    });
    let key = online_key();
    sign_release_manifest(
        &serde_json::to_vec(&payload).unwrap(),
        &signer(&key),
        &[cert()],
    )
}

#[derive(Default)]
struct FakeMirror {
    release: StdMutex<Option<Vec<u8>>>,
    /// 资源包文件：地址 → 内容。
    files: StdMutex<HashMap<String, Vec<u8>>>,
    down: AtomicBool,
    requests: StdMutex<Vec<String>>,
}

impl FakeMirror {
    /// 问过几次发布清单。接入清单的刷新任务在后台跑，不算。
    fn release_requests(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|url| url.contains("/release/"))
            .count()
    }
}

#[async_trait::async_trait]
impl HttpFetch for FakeMirror {
    async fn fetch(
        &self,
        request: &FetchRequest,
    ) -> std::result::Result<FetchResponse, FetchError> {
        self.requests.lock().unwrap().push(request.url.clone());
        if self.down.load(Ordering::SeqCst) {
            return Err(FetchError::Unreachable);
        }
        let body = request
            .url
            .contains("/release/")
            .then(|| self.release.lock().unwrap().clone())
            .flatten()
            .or_else(|| self.files.lock().unwrap().get(&request.url).cloned());
        Ok(match body {
            Some(body) => FetchResponse {
                status: 200,
                etag: None,
                body,
            },
            None => FetchResponse {
                status: 404,
                etag: None,
                body: Vec::new(),
            },
        })
    }
}

struct Lab {
    access: Arc<AccessRuntime>,
    update: Arc<UpdateRuntime>,
    mirror: Arc<FakeMirror>,
}

struct NoArtifacts;

#[async_trait::async_trait]
impl flare_access::download::ArtifactTransport for NoArtifacts {
    async fn open(
        &self,
        _url: &str,
        _offset: u64,
    ) -> std::result::Result<
        Box<dyn flare_access::download::ArtifactStream>,
        flare_access::download::TransportError,
    > {
        Err(flare_access::download::TransportError::Unreachable)
    }
}

fn app() -> UpdateApp {
    UpdateApp {
        platform: release_platform(Some("play")).to_owned(),
        channel: "stable".into(),
        version: Some("2.3.2".into()),
        build: Some(20302),
        distribution: Some("play".into()),
        locale: Some("en".into()),
        ..UpdateApp::default()
    }
}

fn lab_with(store: Arc<MemoryAccessStore>, app: UpdateApp) -> Lab {
    lab_from(store, app, seed(), |access| access)
}

fn lab_from(
    store: Arc<MemoryAccessStore>,
    app: UpdateApp,
    seed: String,
    finish: impl FnOnce(AccessRuntime) -> AccessRuntime,
) -> Lab {
    let config = AccessConfig::production(
        BOOTSTRAP_ID,
        vec![TrustRootConfig {
            key_id: "root-a".into(),
            public_key: encode_public_key(&root_key().verifying_key()),
        }],
        Some(seed),
    );
    let mirror = Arc::new(FakeMirror::default());
    let access = Arc::new(finish(
        AccessRuntime::new(
            Some(config),
            AccessStoreSource::Ready(store as Arc<dyn AccessStore>),
        )
        .with_http_fetch(mirror.clone()),
    ));
    let update = Arc::new(UpdateRuntime::new(
        Arc::downgrade(&access),
        Some(mirror.clone()),
        UpdateOptions {
            app,
            download_dir: std::env::temp_dir().join("flare-update-runtime-test"),
            artifacts: Arc::new(NoArtifacts),
        },
    ));
    Lab {
        access,
        update,
        mirror,
    }
}

fn lab() -> Lab {
    lab_with(Arc::new(MemoryAccessStore::new()), app())
}

#[tokio::test]
async fn the_state_is_readable_before_any_session_and_a_manual_check_finds_the_release() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    assert_eq!(lab.update.state(false).await.state, UpdateState::Idle);

    let status = lab.update.check(true).await;
    assert_eq!(status.state, UpdateState::Available);
    let release = status.release.unwrap();
    assert_eq!(release.build, 20400);
    assert_eq!(release.kind, ReleaseKind::Store);
    assert!(status.last_checked_at.is_some());
    assert_eq!(lab.mirror.release_requests(), 1);
}

#[tokio::test]
async fn a_scheduled_check_respects_the_six_hour_window() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    lab.update.check(false).await;
    let asked = lab.mirror.release_requests();
    assert!(asked > 0);
    lab.update.check(false).await;
    assert_eq!(lab.mirror.release_requests(), asked);
    lab.update.check(true).await;
    assert!(lab.mirror.release_requests() > asked);
}

#[tokio::test]
async fn an_unreachable_mirror_shows_up_only_on_a_manual_check() {
    let lab = lab();
    lab.mirror.down.store(true, Ordering::SeqCst);
    assert_eq!(lab.update.check(false).await.error, None);
    let status = lab.update.check(true).await;
    assert_eq!(status.error.unwrap().code, codes::MANIFEST_UNAVAILABLE);
}

#[tokio::test]
async fn prompts_and_the_cached_decision_survive_a_restart() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    *first.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    let status = first.update.check(true).await;
    assert_eq!(status.prompt, UpdatePrompt::Sheet);
    first
        .update
        .mark_prompted(Some(20400), "sheet")
        .await
        .unwrap();

    let second = lab_with(store, app());
    let status = second.update.state(false).await;
    assert_eq!(status.state, UpdateState::Available, "用本地清单评估");
    assert_eq!(status.prompt, UpdatePrompt::Badge);
    assert_eq!(second.mirror.release_requests(), 0);
}

#[tokio::test]
async fn a_rejection_goes_required_and_read_only_then_finds_a_target() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({ "blockUi": true })));
    let mut states = lab.update.subscribe();
    lab.update.rejected(Some(20400));
    let status = loop {
        let status = states.recv().await.unwrap();
        if status.release.is_some() {
            break status;
        }
        assert_eq!(status.state, UpdateState::Required);
        assert!(status.read_only);
    };
    assert_eq!(status.prompt, UpdatePrompt::Fullscreen);
    assert!(status.block_ui);
    assert!(status.read_only);
}

#[tokio::test]
async fn a_store_release_installs_through_a_plan_and_a_report() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    lab.update.check(true).await;
    let plan = serde_json::to_value(lab.update.install().await.unwrap()).unwrap();
    assert_eq!(plan["kind"], "openStore");
    assert_eq!(plan["store"], "play");
    assert_eq!(lab.update.state(false).await.state, UpdateState::Installing);
    let status = lab.update.report_install("completed", None).await.unwrap();
    assert_eq!(status.state, UpdateState::Available);
    assert!(lab.update.report_install("nope", None).await.is_err());
}

#[tokio::test]
async fn install_without_an_offer_names_the_code_in_the_error() {
    let lab = lab();
    let error = lab.update.install().await.unwrap_err();
    assert!(error.to_string().contains(codes::NO_ARTIFACT), "{error}");
}

#[tokio::test]
async fn a_build_less_app_stays_idle_and_a_manual_check_says_why() {
    let lab = lab_with(
        Arc::new(MemoryAccessStore::new()),
        UpdateApp {
            build: None,
            ..app()
        },
    );
    assert_eq!(lab.update.check(false).await.error, None);
    let status = lab.update.check(true).await;
    assert_eq!(status.state, UpdateState::Idle);
    assert_eq!(status.error.unwrap().code, codes::MANIFEST_UNAVAILABLE);
    drop(lab.access);
}

/// 等状态离开 `checking`。
async fn settled(states: &mut broadcast::Receiver<UpdateStatus>) -> UpdateStatus {
    loop {
        let status = states.recv().await.unwrap();
        if status.state != UpdateState::Checking {
            return status;
        }
    }
}

#[tokio::test]
async fn a_check_answers_checking_at_once_and_settles_in_the_background() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    let mut states = lab.update.subscribe();
    assert_eq!(lab.update.request_check(true).state, UpdateState::Checking);
    assert_eq!(lab.mirror.release_requests(), 0, "路由不等这一轮");

    let status = settled(&mut states).await;
    assert_eq!(status.state, UpdateState::Available);
    assert_eq!(status.release.unwrap().build, 20400);
    assert_eq!(lab.mirror.release_requests(), 1);
}

#[tokio::test]
async fn a_request_during_a_round_does_not_start_another() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    let mut states = lab.update.subscribe();
    assert_eq!(lab.update.request_check(true).state, UpdateState::Checking);
    assert_eq!(lab.update.request_check(true).state, UpdateState::Checking);
    assert_eq!(lab.update.request_check(false).state, UpdateState::Checking);
    settled(&mut states).await;
    assert_eq!(lab.mirror.release_requests(), 1);
}

#[tokio::test]
async fn a_check_that_is_not_due_answers_the_current_state_and_asks_nobody() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({})));
    lab.update.check(true).await;
    let asked = lab.mirror.release_requests();
    assert_eq!(
        lab.update.request_check(false).state,
        UpdateState::Available
    );
    tokio::task::yield_now().await;
    assert_eq!(lab.mirror.release_requests(), asked);
}

#[tokio::test]
async fn a_check_that_cannot_run_does_not_stay_in_checking() {
    let lab = lab_with(
        Arc::new(MemoryAccessStore::new()),
        UpdateApp {
            build: None,
            ..app()
        },
    );
    let mut states = lab.update.subscribe();
    assert_eq!(lab.update.request_check(true).state, UpdateState::Checking);
    let status = settled(&mut states).await;
    assert_eq!(status.state, UpdateState::Idle);
    assert_eq!(status.error.unwrap().code, codes::MANIFEST_UNAVAILABLE);
}

#[tokio::test]
async fn reading_the_state_on_the_way_back_counts_as_the_return_and_only_once() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({ "minBuild": 20400 })));
    lab.update.check(true).await;
    lab.update
        .mark_prompted(Some(20400), "modal")
        .await
        .unwrap();
    assert_eq!(lab.update.defer().await.prompt, UpdatePrompt::Badge);
    assert_eq!(lab.update.state(false).await.prompt, UpdatePrompt::Badge);
    assert_eq!(lab.update.state(true).await.prompt, UpdatePrompt::Modal);

    lab.update
        .mark_prompted(Some(20400), "modal")
        .await
        .unwrap();
    lab.update.foregrounded();
    assert_eq!(
        lab.update.defer().await.prompt,
        UpdatePrompt::Badge,
        "`lifecycle.set_app_state` 晚到不算第二次"
    );
}

#[tokio::test]
async fn a_new_locale_applies_to_the_notes_and_the_block_reason_at_once() {
    let lab = lab();
    *lab.mirror.release.lock().unwrap() = Some(release_document(json!({
        "blockedBuilds": [{
            "from": 20300,
            "to": 20310,
            "reason": { "en": "Data loss", "zh-CN": "数据丢失" }
        }]
    })));
    let status = lab.update.check(true).await;
    assert_eq!(status.block_reason.as_deref(), Some("Data loss"));
    assert_eq!(status.release.unwrap().notes, vec!["Scheduled messages"]);

    let status = lab.update.set_locale("zh-CN".into()).await;
    assert_eq!(status.block_reason.as_deref(), Some("数据丢失"));
    assert_eq!(status.release.unwrap().notes, vec!["定时发送"]);
}

#[tokio::test]
async fn a_second_init_keeps_the_prompt_record_the_read_only_choice_and_the_locale() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    *first.mirror.release.lock().unwrap() = Some(release_document(json!({ "minBuild": 20400 })));
    first.update.check(true).await;
    first
        .update
        .mark_prompted(Some(20400), "modal")
        .await
        .unwrap();
    first.update.defer().await;
    first.update.set_locale("zh-CN".into()).await;

    let second = lab_with(store, app());
    second.update.adopt_session(&first.update);
    let status = second.update.state(false).await;
    assert_eq!(status.state, UpdateState::Required);
    assert_eq!(status.prompt, UpdatePrompt::Badge);
    assert!(status.read_only);
    assert_eq!(status.release.unwrap().notes, vec!["定时发送"]);
}

#[test]
fn the_platform_segment_follows_the_runtime() {
    assert_eq!(
        release_platform(Some("electron")),
        match std::env::consts::OS {
            "android" => "android",
            "ios" => "ios",
            _ => "electron",
        }
    );
}

const PACK_ID: &str = "strings-main";
const PACK_URL: &str = "https://cdn.example.com/packs/strings-main.json";

fn pack_text(marker: &str) -> String {
    json!({ "schema": "flare.resource.v1", "kind": "strings", "marker": marker }).to_string()
}

fn pack_entry(version: u64, text: &str) -> Value {
    json!({
        "id": PACK_ID,
        "kind": "strings",
        "packVersion": version,
        "url": [PACK_URL],
        "sha256": hex::encode(Sha256::digest(text.as_bytes())),
        "sizeBytes": text.len()
    })
}

fn pack_document(serial: u64, packs: Vec<Value>) -> Vec<u8> {
    let payload = json!({
        "schema": "flare.release.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "platform": release_platform(Some("play")),
        "channel": "stable",
        "serial": serial,
        "issuedAt": format_rfc3339_ms(now_ms() - DAY_MS),
        "expiresAt": format_rfc3339_ms(now_ms() + 14 * DAY_MS),
        "policy": {},
        "releases": [{
            "releaseId": "rel_01JC8W",
            "version": "2.4.0",
            "build": 20400,
            "rollout": { "basisPoints": 10000, "salt": "rel_01JC8W", "halted": false },
            "artifacts": { "play": { "kind": "store", "packageName": "com.acme.chat" } },
            "resourcePacks": packs
        }]
    });
    let key = online_key();
    sign_release_manifest(
        &serde_json::to_vec(&payload).unwrap(),
        &signer(&key),
        &[cert()],
    )
}

impl Lab {
    fn serve_packs(&self, serial: u64, packs: Vec<Value>) {
        *self.mirror.release.lock().unwrap() = Some(pack_document(serial, packs));
    }

    fn serve_file(&self, text: &str) {
        self.mirror
            .files
            .lock()
            .unwrap()
            .insert(PACK_URL.to_owned(), text.as_bytes().to_vec());
    }

    /// 查一次更新，再等这一轮同步做完。后台那一轮也在跑，同步是幂等的，先到的做事。
    async fn check_and_sync(&self) -> UpdateStatus {
        let status = self.update.check(true).await;
        let kernel = self.update.ready().await.unwrap();
        self.update
            .sync_resources(kernel, self.mirror.clone())
            .await;
        status
    }

    fn pack_requests(&self) -> usize {
        self.mirror
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|url| url.as_str() == PACK_URL)
            .count()
    }
}

/// 清单列了 DoH 解析器的实验台。
fn doh_lab() -> Lab {
    use crate::infrastructure::transport::http::doh::DirectoryDoh;

    let seed = seed_with(json!([
        { "kind": "https", "url": MIRROR },
        { "kind": "dohTxt", "name": "_flare.example.org", "resolvers": ["https://1.1.1.1/dns-query"] }
    ]));
    lab_from(Arc::new(MemoryAccessStore::new()), app(), seed, |access| {
        access.with_doh(Arc::new(DirectoryDoh::new(Arc::new(FakeMirror::default()))))
    })
}

fn apk_document(url: &str) -> Vec<u8> {
    let payload = json!({
        "schema": "flare.release.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "platform": release_platform(Some("play")),
        "channel": "stable",
        "serial": 88,
        "issuedAt": format_rfc3339_ms(now_ms() - DAY_MS),
        "expiresAt": format_rfc3339_ms(now_ms() + 14 * DAY_MS),
        "policy": {},
        "releases": [{
            "releaseId": "rel_01JC8W",
            "version": "2.4.0",
            "build": 20400,
            "rollout": { "basisPoints": 10000, "salt": "rel_01JC8W", "halted": false },
            "artifacts": { "play": {
                "kind": "apk",
                "urls": [url],
                "sizeBytes": 4,
                "sha256": "a".repeat(64),
                "signingCertSha256": "b".repeat(64)
            } }
        }]
    });
    let key = online_key();
    sign_release_manifest(
        &serde_json::to_vec(&payload).unwrap(),
        &signer(&key),
        &[cert()],
    )
}

/// 制品放在 CDN 上，不在接入清单里：开始下载时交给 DoH，否则查询到不了这个主机。
#[tokio::test]
async fn a_download_lets_doh_ask_about_its_host() {
    const HOST: &str = "dl-a.example-cdn.com";
    let lab = doh_lab();
    *lab.mirror.release.lock().unwrap() = Some(apk_document(&format!("https://{HOST}/app.apk")));
    assert_eq!(lab.update.check(true).await.state, UpdateState::Available);
    assert!(!lab.access.doh_covers(HOST));

    lab.update.download(false).await;

    assert!(lab.access.doh_covers(HOST));
}

#[tokio::test]
async fn a_pack_sync_lets_doh_ask_about_its_host() {
    let lab = doh_lab();
    let text = pack_text("one");
    lab.serve_file(&text);
    lab.serve_packs(88, vec![pack_entry(1, &text)]);

    lab.check_and_sync().await;

    assert!(lab.access.doh_covers("cdn.example.com"));
}

fn pack_keys(store: &MemoryAccessStore) -> Vec<String> {
    store
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("resource/"))
        .collect()
}

#[tokio::test]
async fn a_new_pack_takes_effect_on_the_next_start() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    let text = pack_text("a");
    first.serve_packs(88, vec![pack_entry(1, &text)]);
    first.serve_file(&text);

    assert!(first.update.resources().await.packs.is_empty());
    first.check_and_sync().await;
    assert_eq!(
        pack_keys(&store),
        vec![format!("resource/{BOOTSTRAP_ID}/{PACK_ID}")]
    );
    assert!(
        first.update.resources().await.packs.is_empty(),
        "运行中不变"
    );

    let second = lab_with(store, app());
    let packs = second.update.resources().await.packs;
    assert_eq!(packs.len(), 1);
    assert_eq!(packs[0].id, PACK_ID);
    assert_eq!(packs[0].kind, "strings");
    assert_eq!(packs[0].pack_version, 1);
    assert_eq!(packs[0].content_json, text);
}

#[tokio::test]
async fn the_first_sync_cannot_slip_into_a_snapshot_nobody_asked_for_yet() {
    let store = Arc::new(MemoryAccessStore::new());
    let lab = lab_with(store, app());
    let text = pack_text("a");
    lab.serve_packs(88, vec![pack_entry(1, &text)]);
    lab.serve_file(&text);
    lab.check_and_sync().await;
    assert!(lab.update.resources().await.packs.is_empty());
}

#[tokio::test]
async fn a_withdrawn_pack_stays_for_this_run_and_is_gone_on_the_next_start() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    let text = pack_text("a");
    first.serve_packs(88, vec![pack_entry(1, &text)]);
    first.serve_file(&text);
    first.check_and_sync().await;

    let second = lab_with(store.clone(), app());
    second.serve_packs(89, Vec::new());
    assert_eq!(second.update.resources().await.packs.len(), 1);
    second.check_and_sync().await;
    assert!(pack_keys(&store).is_empty());
    assert_eq!(second.update.resources().await.packs.len(), 1, "运行中不变");

    let third = lab_with(store, app());
    assert!(third.update.resources().await.packs.is_empty());
}

#[tokio::test]
async fn a_lower_version_with_another_hash_replaces_the_stored_pack() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    let bad = pack_text("bad");
    first.serve_packs(88, vec![pack_entry(5, &bad)]);
    first.serve_file(&bad);
    first.check_and_sync().await;

    let second = lab_with(store.clone(), app());
    let good = pack_text("good");
    second.serve_packs(89, vec![pack_entry(3, &good)]);
    second.serve_file(&good);
    second.check_and_sync().await;

    let third = lab_with(store, app());
    let packs = third.update.resources().await.packs;
    assert_eq!(packs[0].pack_version, 3);
    assert_eq!(packs[0].content_json, good);
}

#[tokio::test]
async fn a_second_init_keeps_the_snapshot_of_this_run() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    let text = pack_text("a");
    first.serve_packs(88, vec![pack_entry(1, &text)]);
    first.serve_file(&text);
    first.check_and_sync().await;

    let second = lab_with(store, app());
    second.update.adopt_session(&first.update);
    assert!(second.update.resources().await.packs.is_empty());
}

#[tokio::test]
async fn a_metered_network_waits_for_a_later_check() {
    let store = Arc::new(MemoryAccessStore::new());
    let lab = lab_with(store.clone(), app());
    let text = pack_text("a");
    lab.serve_packs(88, vec![pack_entry(1, &text)]);
    lab.serve_file(&text);
    lab.update.metered.store(true, Ordering::SeqCst);
    lab.check_and_sync().await;
    assert_eq!(lab.pack_requests(), 0);
    assert!(pack_keys(&store).is_empty());

    lab.update.metered.store(false, Ordering::SeqCst);
    lab.check_and_sync().await;
    assert_eq!(lab.pack_requests(), 1);
    assert_eq!(pack_keys(&store).len(), 1);
}

#[tokio::test]
async fn a_failing_pack_is_invisible_to_the_update_status() {
    let store = Arc::new(MemoryAccessStore::new());
    let lab = lab_with(store.clone(), app());
    lab.serve_packs(88, vec![pack_entry(1, &pack_text("a"))]);
    let status = lab.check_and_sync().await;
    assert_eq!(status.state, UpdateState::Available);
    assert_eq!(status.error, None);
    assert!(pack_keys(&store).is_empty());
    assert_eq!(lab.update.state(false).await.error, None);
}

#[tokio::test]
async fn a_manifest_that_stopped_answering_withdraws_nothing() {
    let store = Arc::new(MemoryAccessStore::new());
    let first = lab_with(store.clone(), app());
    let text = pack_text("a");
    first.serve_packs(88, vec![pack_entry(1, &text)]);
    first.serve_file(&text);
    first.check_and_sync().await;

    let second = lab_with(store.clone(), app());
    second.mirror.down.store(true, Ordering::SeqCst);
    second.update.check(true).await;
    assert_eq!(pack_keys(&store).len(), 1, "没有新清单就不动本地");
}
