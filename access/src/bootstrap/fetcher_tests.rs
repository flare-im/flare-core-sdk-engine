//! 分层拉取：本地假镜像与假 DoH 服务，走真实的验签、防回滚与 LKG。

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use base64::Engine;
use serde_json::{Value, json};

use super::*;
use crate::bootstrap::AccessConfig;
use crate::kernel::{BootstrapReport, InitialOrigin, KernelOptions};
use crate::manifest::schema::RELEASE_PAYLOAD_TYPE;
use crate::store::{AccessStore, MemoryAccessStore};
use crate::trust::sign::{
    Signer, encode_public_key, sign_access_manifest, sign_envelope, sign_keystate,
};
use crate::trust::testkit::*;
use crate::trust::{Envelope, KeyState, TrustRootConfig, decode_b64};

const MIRROR_A: &str = "https://mirror-a.example.com/flare/v1/b7f3c9a1e2d4b6a8/";
const MIRROR_B: &str = "https://mirror-b.example.net/flare/v1/b7f3c9a1e2d4b6a8/";
const MIRROR_C: &str = "https://mirror-c.example.org/flare/v1/b7f3c9a1e2d4b6a8/";
const RESOLVER_A: &str = "https://dns-a.example.com/dns-query";
const RESOLVER_B: &str = "https://dns-b.example.net/dns-query";
const POINTER_NAME: &str = "_flare.b7f3c9a1e2d4b6a8.example-boot.org";
const WELL_KNOWN: &str = "https://im-ct.example.com/.well-known/flare/v1/b7f3c9a1e2d4b6a8/";

#[derive(Clone)]
enum Route {
    Body { etag: Option<String>, body: Vec<u8> },
    Status(u16),
    Error(FetchError),
}

/// 假网络：按完整 URL 应答，带 ETag 的文档支持条件请求；DoH 解析器按 `txt` 表回答。
#[derive(Default)]
struct FakeNet {
    routes: Mutex<HashMap<String, Route>>,
    txt: Mutex<HashMap<String, Vec<String>>>,
    log: Mutex<Vec<FetchRequest>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    /// 每个请求要花的（真实）毫秒数。
    latency_ms: AtomicUsize,
}

impl FakeNet {
    fn serve(&self, base: &str, file: &str, body: Vec<u8>) {
        let url = format!("{base}{file}");
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        let etag = Some(format!("\"{:x}\"", hasher.finish()));
        self.routes
            .lock()
            .unwrap()
            .insert(url, Route::Body { etag, body });
    }

    fn fail(&self, base: &str, error: FetchError) {
        for file in [ACCESS_FILE, KEYS_FILE] {
            self.routes
                .lock()
                .unwrap()
                .insert(format!("{base}{file}"), Route::Error(error));
        }
    }

    fn status(&self, base: &str, file: &str, status: u16) {
        self.routes
            .lock()
            .unwrap()
            .insert(format!("{base}{file}"), Route::Status(status));
    }

    fn pointer(&self, resolver: &str, records: &[&str]) {
        self.txt.lock().unwrap().insert(
            resolver.to_owned(),
            records.iter().map(|record| (*record).to_owned()).collect(),
        );
    }

    fn requests(&self) -> Vec<FetchRequest> {
        self.log.lock().unwrap().clone()
    }

    fn requested(&self, prefix: &str) -> usize {
        self.requests()
            .iter()
            .filter(|request| request.url.starts_with(prefix))
            .count()
    }

    fn clear_log(&self) {
        self.log.lock().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl HttpFetch for FakeNet {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        self.log.lock().unwrap().push(request.clone());
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let latency = self.latency_ms.load(Ordering::SeqCst);
        if latency > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(latency as u64)).await;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);

        if let Some((resolver, query)) = request.url.split_once("?dns=") {
            let records = self.txt.lock().unwrap().get(resolver).cloned();
            return match records {
                Some(records) => {
                    assert_eq!(request.accept, Some(doh::DNS_MESSAGE));
                    assert_eq!(request.timeout_ms, DOH_TIMEOUT_MS);
                    let query = base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .decode(query)
                        .unwrap();
                    assert_eq!(query, doh::encode_query(POINTER_NAME).unwrap());
                    let records: Vec<&str> = records.iter().map(String::as_str).collect();
                    Ok(FetchResponse {
                        status: 200,
                        etag: None,
                        body: doh::encode_txt_response(&query, &records),
                    })
                }
                None => Err(FetchError::Unreachable),
            };
        }
        let route = self.routes.lock().unwrap().get(&request.url).cloned();
        match route {
            Some(Route::Body { etag, body }) => {
                if etag.is_some() && request.if_none_match == etag {
                    return Ok(FetchResponse {
                        status: 304,
                        etag,
                        body: Vec::new(),
                    });
                }
                Ok(FetchResponse {
                    status: 200,
                    etag,
                    body,
                })
            }
            Some(Route::Status(status)) => Ok(FetchResponse {
                status,
                etag: None,
                body: Vec::new(),
            }),
            Some(Route::Error(error)) => Err(error),
            None => Ok(FetchResponse {
                status: 404,
                etag: None,
                body: Vec::new(),
            }),
        }
    }
}

/// 虚拟时钟：`sleep` 推进时间，真实只等几毫秒（让在途请求有机会完成）。
struct FakeClock {
    now: AtomicI64,
    sleeps: Mutex<Vec<u64>>,
}

impl FakeClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            now: AtomicI64::new(NOW_MS),
            sleeps: Mutex::new(Vec::new()),
        })
    }

    fn advance(&self, ms: i64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl BootstrapClock for FakeClock {
    fn now_ms(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }

    async fn sleep(&self, ms: u64) {
        self.sleeps.lock().unwrap().push(ms);
        self.advance(ms as i64);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}

fn sources() -> Value {
    json!([
        { "kind": "https", "url": MIRROR_A },
        { "kind": "https", "url": MIRROR_B },
        { "kind": "wellKnown" },
        { "kind": "dohTxt", "name": POINTER_NAME, "resolvers": [RESOLVER_A, RESOLVER_B] }
    ])
}

fn manifest(serial: u64) -> Value {
    let mut value = manifest_with_serial(serial);
    value["bootstrap"]["sources"] = sources();
    value
}

fn signed(serial: u64) -> Vec<u8> {
    signed_manifest(&manifest(serial))
}

fn config(seed_serial: Option<u64>) -> AccessConfig {
    AccessConfig::production(
        BOOTSTRAP_ID,
        vec![TrustRootConfig {
            key_id: ROOT_ID.into(),
            public_key: encode_public_key(&root_key().verifying_key()),
        }],
        seed_serial.map(|serial| String::from_utf8(signed(serial)).unwrap()),
    )
}

fn config_with_seed(seed: &Value) -> AccessConfig {
    let mut config = config(None);
    config.seed = Some(String::from_utf8(signed_manifest(seed)).unwrap());
    config
}

async fn boot(
    config: &AccessConfig,
    store: &Arc<MemoryAccessStore>,
) -> (Arc<AccessKernel>, BootstrapReport) {
    let (kernel, report) = AccessKernel::bootstrap(
        config,
        store.clone(),
        KernelOptions {
            quic_supported: true,
        },
        NOW_MS,
    )
    .await
    .unwrap();
    (Arc::new(kernel), report)
}

struct Lab {
    store: Arc<MemoryAccessStore>,
    net: Arc<FakeNet>,
    clock: Arc<FakeClock>,
    kernel: Arc<AccessKernel>,
    fetcher: BootstrapFetcher,
}

impl Lab {
    async fn start(seed_serial: u64) -> Self {
        Self::with_config(config(Some(seed_serial))).await
    }

    async fn with_config(config: AccessConfig) -> Self {
        let store = Arc::new(MemoryAccessStore::new());
        let net = Arc::new(FakeNet::default());
        let clock = FakeClock::new();
        let (kernel, _) = boot(&config, &store).await;
        let fetcher = BootstrapFetcher::new(kernel.clone(), net.clone(), clock.clone());
        Self {
            store,
            net,
            clock,
            kernel,
            fetcher,
        }
    }

    fn serial(&self) -> u64 {
        self.kernel.directory().snapshot().serial()
    }

    async fn lkg_bytes(&self) -> Vec<u8> {
        self.store
            .get(&keys::access_current(BOOTSTRAP_ID))
            .await
            .unwrap()
            .unwrap()
    }

    async fn refresh(&self, mode: RefreshMode) -> RefreshReport {
        self.fetcher.refresh(mode, None).await
    }
}

fn attempt<'a>(report: &'a RefreshReport, source: &str) -> &'a SourceAttempt {
    report
        .attempts
        .iter()
        .find(|attempt| attempt.source == format!("https:{source}") && attempt.document == "access")
        .unwrap_or_else(|| panic!("no attempt for {source}: {:?}", report.attempts))
}

/// 镜像的 `ipHints` 和接入点一样带到请求上；DoH 指针给出的镜像没有清单背书，也就没有提示。
#[tokio::test]
async fn mirror_ip_hints_reach_the_fetch_request() {
    let mut seed = manifest(10);
    seed["bootstrap"]["sources"][0]["ipHints"] = json!(["203.0.113.30", "2001:db8::30"]);
    let lab = Lab::with_config(config_with_seed(&seed)).await;
    lab.net.fail(MIRROR_A, FetchError::Unreachable);
    lab.net.fail(MIRROR_B, FetchError::Unreachable);
    lab.net.fail(WELL_KNOWN, FetchError::Unreachable);
    let pointer = format!("v=flare1;u={MIRROR_C}");
    lab.net.pointer(RESOLVER_A, &[&pointer]);
    lab.net.fail(MIRROR_C, FetchError::Unreachable);
    lab.refresh(RefreshMode::Recovering).await;

    let hints = |prefix: &str| {
        lab.net
            .requests()
            .into_iter()
            .find(|request| request.url.starts_with(prefix))
            .unwrap_or_else(|| panic!("nothing asked {prefix}"))
            .ip_hints
    };
    let listed: Vec<IpAddr> = vec![
        "203.0.113.30".parse().unwrap(),
        "2001:db8::30".parse().unwrap(),
    ];
    assert_eq!(hints(MIRROR_A), listed);
    assert!(hints(MIRROR_B).is_empty());
    assert!(hints(MIRROR_C).is_empty());
}

#[tokio::test]
async fn doh_resolvers_and_listed_hosts_come_from_the_manifest() {
    let lab = Lab::start(10).await;
    assert_eq!(lab.kernel.doh_resolvers(), [RESOLVER_A, RESOLVER_B]);
    assert!(lab.kernel.is_listed_host("IM-CT.example.com"));
    assert!(lab.kernel.is_listed_host("mirror-a.example.com"));
    assert!(!lab.kernel.is_listed_host("dns-a.example.com"));
    assert!(!lab.kernel.is_listed_host("cdn.third-party.example"));
}

#[tokio::test]
async fn a_newer_manifest_on_the_first_mirror_is_adopted_and_persisted() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(11));

    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(report.serial, 11);
    assert_eq!(lab.serial(), 11);
    let adoption = &report.adoptions[0];
    assert_eq!(
        adoption.source,
        ManifestSource::Https {
            url: MIRROR_A.into()
        }
    );
    assert!(adoption.persisted && !adoption.stale);
    assert_eq!(report.retry_after_ms, None);
    assert_eq!(lab.kernel.status(NOW_MS).fetched_at_ms, Some(NOW_MS));

    // 重启后 LKG 重新验签，来源保留。
    let (kernel, report) = boot(&config(Some(10)), &lab.store).await;
    assert_eq!(report.origin, InitialOrigin::Lkg);
    assert_eq!(report.serial, 11);
    assert_eq!(
        kernel.status(NOW_MS).source,
        format!("https:{MIRROR_A}"),
        "来源写进 LKG 记录"
    );
    assert_eq!(kernel.status(NOW_MS).fetched_at_ms, Some(NOW_MS));
}

/// §13.4 负向用例：全部被拒，LKG 与目录不动，能判定为不可信的源被降权。
#[tokio::test]
async fn hostile_documents_are_rejected_and_never_reach_the_lkg() {
    let root = root_key();
    let online = online_key();
    let online_signer = Signer {
        key_id: ONLINE_ID,
        key: &online,
    };
    let root_signer = Signer {
        key_id: ROOT_ID,
        key: &root,
    };

    let tampered = {
        let mut envelope = Envelope::parse(&signed(20)).unwrap();
        let mut payload = decode_b64(&envelope.payload).unwrap();
        let at = payload.windows(5).position(|w| w == b"im-ct").unwrap();
        payload[at] = b'x';
        envelope.payload = crate::trust::encode_b64(&payload);
        envelope.to_json_bytes()
    };
    let expired_cert = {
        let mut expired = cert();
        expired.not_before = "2026-01-01T00:00:00Z".into();
        expired.not_after = "2026-06-01T00:00:00Z".into();
        sign_access_manifest(
            &serde_json::to_vec(&manifest(20)).unwrap(),
            &online_signer,
            &[issued_cert(&expired)],
        )
    };
    let wrong_payload_type = sign_envelope(
        RELEASE_PAYLOAD_TYPE,
        &serde_json::to_vec(&manifest(20)).unwrap(),
        std::slice::from_ref(&online_signer),
        &[issued_cert(&cert())],
    )
    .to_json_bytes();
    let wrong_bootstrap = {
        let mut other = manifest(20);
        other["bootstrapId"] = "0000000000000000".into();
        signed_manifest(&other)
    };
    let oversized = {
        let mut padded = manifest(20);
        padded["clientPolicy"] = json!({ "pad": "x".repeat(MAX_DOCUMENT_BYTES) });
        signed_manifest(&padded)
    };
    let cases: Vec<(&str, Vec<u8>, &str, AttemptResult, bool)> = vec![
        (
            "tampered payload",
            tampered,
            "badSignature",
            AttemptResult::Rejected,
            true,
        ),
        (
            "serial rollback",
            signed(9),
            "rollback",
            AttemptResult::OlderSerial,
            true,
        ),
        (
            "expired online certificate",
            expired_cert,
            "keyCert.expired",
            AttemptResult::Rejected,
            false,
        ),
        (
            "wrong payloadType",
            wrong_payload_type,
            "wrongPayloadType",
            AttemptResult::Rejected,
            true,
        ),
        (
            "wrong bootstrapId",
            wrong_bootstrap,
            "wrongBootstrapId",
            AttemptResult::Rejected,
            true,
        ),
        (
            "larger than 64 KiB",
            oversized,
            "tooLarge",
            AttemptResult::Rejected,
            true,
        ),
    ];
    for (name, body, code, result, demoted) in cases {
        let lab = Lab::start(10).await;
        let before = lab.lkg_bytes().await;
        lab.net.serve(MIRROR_A, ACCESS_FILE, body);
        let report = lab.refresh(RefreshMode::Background).await;
        assert_eq!(report.outcome, RefreshOutcome::Failed, "{name}");
        let seen = attempt(&report, MIRROR_A);
        assert_eq!(seen.code.as_deref(), Some(code), "{name}");
        assert_eq!(seen.result, result, "{name}");
        assert_eq!(lab.serial(), 10, "{name}");
        assert_eq!(lab.lkg_bytes().await, before, "{name}: LKG 不动");
        let book = lab.fetcher.book.lock().await;
        assert_eq!(
            book.as_ref()
                .unwrap()
                .is_demoted(&format!("https:{MIRROR_A}"), NOW_MS),
            demoted,
            "{name}"
        );
    }

    // 传输层就截断的超大响应同样按「被拒」算。
    let lab = Lab::start(10).await;
    lab.net.fail(MIRROR_A, FetchError::TooLarge);
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(attempt(&report, MIRROR_A).code.as_deref(), Some("tooLarge"));

    // 被吊销的 keyid：密钥状态与清单从同一个镜像到达，先生效的吊销挡住清单。
    let lab = Lab::start(10).await;
    let before = lab.lkg_bytes().await;
    let keystate = KeyState {
        schema: "flare.keystate.v1".into(),
        serial: 5,
        issued_at: "2026-11-04T00:00:00Z".into(),
        certificates: vec![],
        revoked_key_ids: vec![ONLINE_ID.into()],
    };
    lab.net
        .serve(MIRROR_A, KEYS_FILE, sign_keystate(&keystate, &root_signer));
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(20));
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::Failed);
    assert_eq!(attempt(&report, MIRROR_A).code.as_deref(), Some("revoked"));
    assert!(lab.kernel.current_manifest_revoked());
    assert_eq!(lab.serial(), 10);
    assert_eq!(lab.lkg_bytes().await, before);
    assert_eq!(lab.kernel.status(NOW_MS).keystate_serial, Some(5));

    // 换上新密钥签的清单之前一直算被吊销：刷新频率不会退回去。
    lab.refresh(RefreshMode::Background).await;
    assert!(lab.kernel.current_manifest_revoked());

    // 写盘失败时吊销同样要在内存里生效。
    let lab = Lab::start(10).await;
    lab.net
        .serve(MIRROR_A, KEYS_FILE, sign_keystate(&keystate, &root_signer));
    lab.store.set_fail_writes(true);
    lab.refresh(RefreshMode::Background).await;
    assert!(lab.kernel.current_manifest_revoked());
    assert_eq!(lab.kernel.status(NOW_MS).keystate_serial, Some(5));
}

#[tokio::test]
async fn the_keystate_is_protected_against_rollback_too() {
    let root = root_key();
    let signer = Signer {
        key_id: ROOT_ID,
        key: &root,
    };
    let keystate = |serial: u64, revoked: &[&str]| KeyState {
        schema: "flare.keystate.v1".into(),
        serial,
        issued_at: "2026-11-04T00:00:00Z".into(),
        certificates: vec![],
        revoked_key_ids: revoked.iter().map(|id| (*id).to_owned()).collect(),
    };
    let lab = Lab::start(10).await;
    lab.net.serve(
        MIRROR_A,
        KEYS_FILE,
        sign_keystate(&keystate(7, &["ok-old"]), &signer),
    );
    lab.refresh(RefreshMode::Background).await;
    let trust = lab.kernel.trust().unwrap();
    assert_eq!(trust.keystate_serial(), Some(7));

    // 镜像换回旧的密钥状态：丢弃，吊销列表不倒退，该源降权。
    lab.net.serve(
        MIRROR_A,
        KEYS_FILE,
        sign_keystate(&keystate(6, &[]), &signer),
    );
    lab.net.status(MIRROR_A, ACCESS_FILE, 503);
    let report = lab.refresh(RefreshMode::Background).await;
    let rejected = report
        .attempts
        .iter()
        .find(|attempt| attempt.document == "keys")
        .unwrap();
    assert_eq!(rejected.result, AttemptResult::OlderSerial);
    assert_eq!(rejected.code.as_deref(), Some("rollback"));
    let trust = lab.kernel.trust().unwrap();
    assert_eq!(trust.keystate_serial(), Some(7));
    assert_eq!(trust.revoked_key_ids().collect::<Vec<_>>(), ["ok-old"]);
}

#[tokio::test]
async fn a_rotated_certificate_is_taken_from_the_keystate_on_the_same_mirror() {
    let root = root_key();
    let online = online_key();
    let lab = Lab::start(10).await;
    let keystate = KeyState {
        schema: "flare.keystate.v1".into(),
        serial: 1,
        issued_at: "2026-11-04T00:00:00Z".into(),
        certificates: vec![issued_cert(&cert())],
        revoked_key_ids: vec![],
    };
    lab.net.serve(
        MIRROR_A,
        KEYS_FILE,
        sign_keystate(
            &keystate,
            &Signer {
                key_id: ROOT_ID,
                key: &root,
            },
        ),
    );
    // 清单本身不带证书：只有先读密钥状态才验得过。
    lab.net.serve(
        MIRROR_A,
        ACCESS_FILE,
        sign_access_manifest(
            &serde_json::to_vec(&manifest(11)).unwrap(),
            &Signer {
                key_id: ONLINE_ID,
                key: &online,
            },
            &[],
        ),
    );
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
}

#[tokio::test]
async fn conditional_requests_use_the_etag_of_the_document_held() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(11));
    lab.refresh(RefreshMode::Background).await;
    lab.net.clear_log();

    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::UpToDate);
    assert_eq!(
        attempt(&report, MIRROR_A).result,
        AttemptResult::NotModified
    );
    let access = lab
        .net
        .requests()
        .into_iter()
        .find(|request| request.url.ends_with(ACCESS_FILE))
        .unwrap();
    assert!(access.if_none_match.is_some());
    assert_eq!(access.max_body_bytes, MAX_DOCUMENT_BYTES);
    assert_eq!(access.timeout_ms, MIRROR_TIMEOUT_MS);

    // 304 之后 Background 就结束了：不再问后面的源。
    assert_eq!(lab.net.requested(MIRROR_B), 0);

    // 重启：源簿在存储里，条件请求照常发出。
    let (kernel, _) = boot(&config(Some(10)), &lab.store).await;
    let fetcher = BootstrapFetcher::new(kernel, lab.net.clone(), lab.clock.clone());
    lab.net.clear_log();
    let report = fetcher.refresh(RefreshMode::Background, None).await;
    assert_eq!(
        attempt(&report, MIRROR_A).result,
        AttemptResult::NotModified
    );
    assert!(fetcher.last_refresh_ms().await.is_some());

    // 200 且 serial 相同视同 304。
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(10));
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::UpToDate);
    assert_eq!(attempt(&report, MIRROR_A).result, AttemptResult::SameSerial);
}

#[tokio::test]
async fn an_etag_is_not_sent_for_a_document_this_device_no_longer_holds() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(11));
    lab.refresh(RefreshMode::Background).await;
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(12));
    lab.net.fail(MIRROR_A, FetchError::Unreachable);
    lab.refresh(RefreshMode::Background).await;
    assert_eq!(lab.serial(), 12);

    // 镜像 A 回来，还是 serial 11 的旧对象：本机持有的是 12，不能用它的 ETag 换来一个 304。
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(11));
    lab.net.clear_log();
    let report = lab.refresh(RefreshMode::Background).await;
    let sent: Vec<_> = lab
        .net
        .requests()
        .into_iter()
        .filter(|request| request.url == format!("{MIRROR_A}{ACCESS_FILE}"))
        .collect();
    if let Some(request) = sent.first() {
        assert_eq!(request.if_none_match, None);
    }
    assert_eq!(report.serial, 12);
}

#[tokio::test]
async fn a_source_that_rolled_back_stays_demoted_for_a_day() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(9));
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(11));
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(
        attempt(&report, MIRROR_A).result,
        AttemptResult::OlderSerial
    );
    assert_eq!(report.outcome, RefreshOutcome::Adopted);

    // 下一轮：B 排到最前（成功过），A 降权排在 well-known 之后。
    lab.net.clear_log();
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(12));
    lab.refresh(RefreshMode::Background).await;
    assert_eq!(lab.serial(), 12);
    assert_eq!(
        lab.net.requested(MIRROR_A),
        0,
        "降权的源排在最后，这轮没问到"
    );

    let keyed = |fetcher: &BootstrapFetcher| {
        let snapshot = lab.kernel.directory().snapshot();
        let book = fetcher.book.try_lock().unwrap();
        fetcher
            .candidates(
                &snapshot,
                RefreshMode::Background,
                None,
                book.as_ref().unwrap(),
            )
            .into_iter()
            .map(|candidate| candidate.key)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        keyed(&lab.fetcher),
        vec![
            format!("https:{MIRROR_B}"),
            "wellKnown:ap-cne1-ct".to_owned(),
            format!("https:{MIRROR_A}"),
        ]
    );

    // 24 小时后恢复原位（A 没成功过，所以在 B 之后、well-known 之前）。
    lab.clock.advance(DEMOTION_MS);
    assert_eq!(
        keyed(&lab.fetcher),
        vec![
            format!("https:{MIRROR_B}"),
            format!("https:{MIRROR_A}"),
            "wellKnown:ap-cne1-ct".to_owned(),
        ]
    );
}

#[tokio::test]
async fn t2_failures_fall_back_to_the_access_point_then_to_doh_pointers() {
    // T2 全失败 → T3。
    let lab = Lab::start(10).await;
    lab.net.fail(MIRROR_A, FetchError::Timeout);
    lab.net.fail(MIRROR_B, FetchError::Unreachable);
    lab.net.serve(WELL_KNOWN, ACCESS_FILE, signed(11));
    let report = lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(
        report.adoptions[0].source,
        ManifestSource::WellKnown {
            ap_id: "ap-cne1-ct".into()
        }
    );
    assert_eq!(
        lab.net.requested(RESOLVER_A) + lab.net.requested(RESOLVER_B),
        0,
        "T3 拿到了就不用问 DNS"
    );
    let well_known = lab
        .net
        .requests()
        .into_iter()
        .find(|request| request.url.starts_with(WELL_KNOWN))
        .unwrap();
    assert_eq!(well_known.timeout_ms, WELL_KNOWN_TIMEOUT_MS);
    assert_eq!(
        well_known.ip_hints.len(),
        2,
        "DNS 失败时可以按 ipHints 直连"
    );

    // T2、T3 全失败 → 并行问全部解析器，取并集。
    let lab = Lab::start(10).await;
    lab.net.fail(MIRROR_A, FetchError::Timeout);
    lab.net.fail(MIRROR_B, FetchError::Unreachable);
    lab.net.fail(WELL_KNOWN, FetchError::Tls);
    let pointer_a = format!("v=flare1;u={MIRROR_C}");
    lab.net.pointer(RESOLVER_A, &[&pointer_a]);
    lab.net
        .pointer(RESOLVER_B, &["v=flare1;u=https://mirror-d.example.org/"]);
    lab.net.serve(MIRROR_C, ACCESS_FILE, signed(11));
    let report = lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(report.serial, 11);
    assert_eq!(
        report.adoptions[0].source,
        ManifestSource::Https {
            url: MIRROR_C.into()
        }
    );
    assert_eq!(lab.net.requested(RESOLVER_A), 1);
    assert_eq!(lab.net.requested(RESOLVER_B), 1);
    let resolvers: Vec<_> = report
        .attempts
        .iter()
        .filter(|attempt| attempt.tier == SourceTier::Resolver)
        .collect();
    assert_eq!(resolvers.len(), 2);

    // 被篡改的指针最多造成拒绝服务：指向的文档照样要验签。
    let lab = Lab::start(10).await;
    lab.net.fail(MIRROR_A, FetchError::Timeout);
    lab.net.fail(MIRROR_B, FetchError::Timeout);
    lab.net.fail(WELL_KNOWN, FetchError::Timeout);
    lab.net.pointer(RESOLVER_A, &[&pointer_a]);
    lab.net
        .serve(MIRROR_C, ACCESS_FILE, b"{\"forged\":true}".to_vec());
    let report = lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(report.outcome, RefreshOutcome::Failed);
    assert_eq!(lab.serial(), 10);

    // 周期刷新不问 DNS。
    let lab = Lab::start(10).await;
    lab.net.fail(MIRROR_A, FetchError::Timeout);
    lab.net.fail(MIRROR_B, FetchError::Timeout);
    lab.net.fail(WELL_KNOWN, FetchError::Timeout);
    lab.net.pointer(RESOLVER_A, &[&pointer_a]);
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::Failed);
    assert_eq!(lab.net.requested(RESOLVER_A), 0);
}

#[tokio::test]
async fn recovering_keeps_asking_past_a_stale_mirror_and_skips_dns_when_sources_answer() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(10));
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(11));
    let report = lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(report.serial, 11, "同 serial 不结束恢复，继续问后面的源");

    // 所有源都确认没有更新：不是引导面的问题，不去问 DNS。
    lab.net.serve(WELL_KNOWN, ACCESS_FILE, signed(11));
    let report = lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(report.outcome, RefreshOutcome::UpToDate);
    assert_eq!(lab.net.requested(RESOLVER_A), 0);
}

#[tokio::test]
async fn notified_waits_for_the_announced_serial_to_propagate() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(10));
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(10));

    // CDN 还没传播：全部是旧值，30 秒后再来一轮。
    let report = lab.refresh(RefreshMode::Notified(12)).await;
    assert_eq!(report.outcome, RefreshOutcome::OldValues);
    assert_eq!(report.retry_after_ms, Some(NOTIFIED_RETRY_DELAY_MS));
    assert_eq!(lab.serial(), 10);

    // 第二轮只传播到一半：B（最近成功，排在前面）只有 11，先采纳，继续问，拿到 12 才算完。
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(11));
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(12));
    let report = lab.refresh(RefreshMode::Notified(12)).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(report.retry_after_ms, None);
    assert_eq!(report.serial, 12);
    assert_eq!(report.adoptions.len(), 2);

    // 本机已不低于通知值：不发请求。
    lab.net.clear_log();
    let report = lab.refresh(RefreshMode::Notified(12)).await;
    assert_eq!(report.outcome, RefreshOutcome::UpToDate);
    assert!(lab.net.requests().is_empty());

    // 全部失败时同样留着重试。
    lab.net.fail(MIRROR_A, FetchError::Timeout);
    lab.net.fail(MIRROR_B, FetchError::Timeout);
    let report = lab.refresh(RefreshMode::Notified(13)).await;
    assert_eq!(report.outcome, RefreshOutcome::Failed);
    assert_eq!(report.retry_after_ms, Some(NOTIFIED_RETRY_DELAY_MS));
}

#[tokio::test]
async fn sources_start_staggered_with_at_most_three_in_flight() {
    let mut value = manifest(10);
    value["bootstrap"]["sources"] =
        json!((0..6)
        .map(|index| json!({ "kind": "https", "url": format!("https://m{index}.example.com/") }))
        .collect::<Vec<_>>());
    let lab = Lab::with_config(AccessConfig::production(
        BOOTSTRAP_ID,
        vec![TrustRootConfig {
            key_id: ROOT_ID.into(),
            public_key: encode_public_key(&root_key().verifying_key()),
        }],
        Some(String::from_utf8(signed_manifest(&value)).unwrap()),
    ))
    .await;
    lab.net.latency_ms.store(25, Ordering::SeqCst);

    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::Failed);
    assert_eq!(report.attempts.len(), 6);
    assert!(
        lab.clock
            .sleeps
            .lock()
            .unwrap()
            .iter()
            .all(|ms| *ms == STAGGER_MS),
        "交错间隔"
    );
    // 每个源同时拉 access 与 keys 两个文件。
    let peak = lab.net.peak.load(Ordering::SeqCst);
    assert!(peak > 2, "确实并发了：{peak}");
    assert!(peak <= MAX_IN_FLIGHT * 2, "最多 3 个源在途：{peak}");
}

#[tokio::test]
async fn the_lkg_is_replaced_only_by_a_newer_verified_manifest() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(15));
    lab.refresh(RefreshMode::Background).await;
    let lkg = lab.lkg_bytes().await;

    // 未签名数据、旧版本：不动。
    lab.net.serve(MIRROR_A, ACCESS_FILE, b"{}".to_vec());
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(14));
    lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(lab.lkg_bytes().await, lkg);
    assert_eq!(lab.serial(), 15);

    // 带旧种子的新版本启动：LKG 胜出且不被种子覆盖。
    let (kernel, report) = boot(&config(Some(12)), &lab.store).await;
    assert_eq!(report.origin, InitialOrigin::Lkg);
    assert_eq!(kernel.directory().snapshot().serial(), 15);
    assert_eq!(lab.lkg_bytes().await, lkg);
}

#[tokio::test]
async fn a_tampered_local_copy_fails_verification_after_a_restart() {
    let lab = Lab::start(10).await;
    lab.net.serve(MIRROR_A, ACCESS_FILE, signed(11));
    lab.refresh(RefreshMode::Background).await;

    let key = keys::access_current(BOOTSTRAP_ID);
    let mut record: serde_json::Value = serde_json::from_slice(&lab.lkg_bytes().await).unwrap();
    let mut envelope =
        Envelope::parse(&decode_b64(record["envelope"].as_str().unwrap()).unwrap()).unwrap();
    let mut payload = decode_b64(&envelope.payload).unwrap();
    let at = payload.windows(5).position(|w| w == b"im-ct").unwrap();
    payload[at] = b'x';
    envelope.payload = crate::trust::encode_b64(&payload);
    record["envelope"] = crate::trust::encode_b64(&envelope.to_json_bytes()).into();
    lab.store
        .put(&key, &serde_json::to_vec(&record).unwrap())
        .await
        .unwrap();

    let (kernel, report) = boot(&config(Some(10)), &lab.store).await;
    assert_eq!(
        report.origin,
        InitialOrigin::Seed { persisted: false },
        "被改过的 LKG 重新验签失败，退回种子，且不降低 maxSerial"
    );
    assert_eq!(kernel.directory().snapshot().serial(), 10);
}

#[tokio::test]
async fn a_device_without_any_valid_copy_recovers_from_the_seed_sources_as_hints() {
    // 全新安装的老版本：种子的在线证书已过期，验不过；但它列的镜像还活着。
    let online = online_key();
    let mut expired = cert();
    expired.not_before = "2026-01-01T00:00:00Z".into();
    expired.not_after = "2026-06-01T00:00:00Z".into();
    let seed = sign_access_manifest(
        &serde_json::to_vec(&manifest(3)).unwrap(),
        &Signer {
            key_id: ONLINE_ID,
            key: &online,
        },
        &[issued_cert(&expired)],
    );
    let lab = Lab::with_config(AccessConfig::production(
        BOOTSTRAP_ID,
        vec![TrustRootConfig {
            key_id: ROOT_ID.into(),
            public_key: encode_public_key(&root_key().verifying_key()),
        }],
        Some(String::from_utf8(seed).unwrap()),
    ))
    .await;
    assert!(!lab.kernel.directory().snapshot().has_manifest());
    lab.net.fail(MIRROR_A, FetchError::Timeout);
    lab.net.serve(MIRROR_B, ACCESS_FILE, signed(20));

    let report = lab.refresh(RefreshMode::Recovering).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(lab.serial(), 20);
    assert!(lab.kernel.directory().snapshot().has_manifest());
    assert!(lab.kernel.status(NOW_MS).has_manifest);
}

#[tokio::test]
async fn development_mode_never_fetches() {
    let store = Arc::new(MemoryAccessStore::new());
    let net = Arc::new(FakeNet::default());
    let config = AccessConfig::development(crate::DevelopmentEndpoints {
        ws_url: Some("ws://127.0.0.1:60051".into()),
        ..Default::default()
    });
    let (kernel, _) = boot(&config, &store).await;
    let fetcher = BootstrapFetcher::new(kernel, net.clone(), FakeClock::new());
    let report = fetcher.refresh(RefreshMode::Recovering, None).await;
    assert_eq!(report.outcome, RefreshOutcome::Skipped);
    assert!(net.requests().is_empty());
}

fn recovery(serial: u64) -> crate::manifest::RecoveryManifest {
    crate::manifest::RecoveryManifest {
        schema: "flare.recovery.v1".into(),
        bootstrap_id: BOOTSTRAP_ID.into(),
        app_id: "app_01J9ZK3X".into(),
        serial,
        issued_at: "2026-11-04T08:00:00Z".into(),
        expires_at: "2026-11-18T08:00:00Z".into(),
        region_id: "cn-east".into(),
        cell_id: "cell-cne-1".into(),
        access_points: vec![
            crate::manifest::RecoveryAccessPoint {
                id: "ap-rescue-1".into(),
                host: "rescue-a.example.net".into(),
                ip_hints: vec!["203.0.113.20".into()],
                services: None,
            },
            crate::manifest::RecoveryAccessPoint {
                id: "ap-rescue-2".into(),
                host: "rescue-b.example.org".into(),
                ip_hints: vec![],
                services: None,
            },
        ],
        sources: vec![
            MIRROR_C.into(),
            "https://mirror-e.example.net/flare/".into(),
        ],
    }
}

fn recovery_link(recovery: &crate::manifest::RecoveryManifest) -> String {
    let root = root_key();
    crate::trust::sign::sign_recovery_link(
        recovery,
        &Signer {
            key_id: ROOT_ID,
            key: &root,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn a_recovery_link_takes_the_same_adoption_path() {
    let lab = Lab::start(10).await;
    let link = recovery_link(&recovery(30));
    assert!(link.starts_with("flare-recover://v1?p="));
    assert!(
        link.len() <= crate::manifest::MAX_RECOVERY_LINK_BYTES,
        "两个接入点加两个引导源要装进 1 KiB：{}",
        link.len()
    );

    let update = lab.kernel.accept_recovery(&link, NOW_MS).await.unwrap();
    let ManifestUpdate::Adopted { change, .. } = update else {
        panic!("{update:?}");
    };
    assert_eq!(change.to_serial, 30);
    assert_eq!(lab.serial(), 30);
    let snapshot = lab.kernel.directory().snapshot();
    let (_, entry) = snapshot.access_point("ap-rescue-1").unwrap();
    assert_eq!(
        entry.endpoint(crate::ServiceKind::ImWs),
        Some("wss://rescue-a.example.net/ws")
    );
    assert_eq!(lab.kernel.status(NOW_MS).source, "recovery");

    // 重复导入同一条：不变。
    let again = lab.kernel.accept_recovery(&link, NOW_MS).await.unwrap();
    assert_eq!(again, ManifestUpdate::Unchanged { serial: 30 });

    // 重启：恢复清单作为 LKG 重新验签。
    let (kernel, report) = boot(&config(Some(10)), &lab.store).await;
    assert_eq!(report.origin, InitialOrigin::Lkg);
    assert_eq!(report.serial, 30);
    assert_eq!(kernel.status(NOW_MS).source, "recovery");

    // 拿到网络后从恢复清单列的引导源换回完整清单。
    lab.net.serve(MIRROR_C, ACCESS_FILE, signed(31));
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(report.outcome, RefreshOutcome::Adopted);
    assert_eq!(lab.serial(), 31);

    // 旧的恢复链接受防回滚约束。
    let error = lab.kernel.accept_recovery(&link, NOW_MS).await.unwrap_err();
    assert_eq!(
        error,
        AcceptError::Trust(TrustError::Rollback {
            serial: 30,
            max_serial: 31
        })
    );
    assert_eq!(lab.serial(), 31);
}

#[tokio::test]
async fn a_recovery_link_rescues_a_device_with_no_directory_at_all() {
    let lab = Lab::with_config(config(None)).await;
    assert!(!lab.kernel.directory().snapshot().has_manifest());
    let update = lab
        .kernel
        .accept_recovery(&recovery_link(&recovery(30)), NOW_MS)
        .await
        .unwrap();
    assert!(matches!(update, ManifestUpdate::Adopted { .. }));
    assert_eq!(lab.kernel.directory().snapshot().access_points().len(), 2);
}

#[tokio::test]
async fn forged_or_oversized_recovery_links_are_rejected() {
    let lab = Lab::start(10).await;
    let good = recovery_link(&recovery(30));
    let rejected = async |link: &str| {
        let error = lab.kernel.accept_recovery(link, NOW_MS).await.unwrap_err();
        match error {
            AcceptError::Trust(error) => error.code(),
            other => panic!("{other:?}"),
        }
    };

    // 改动载荷。
    let forged = {
        let mut other = recovery(30);
        other.access_points[0].host = "evil.example.net".into();
        let payload = crate::trust::encode_b64(&serde_json::to_vec(&other).unwrap());
        let signature = good.split("&s=").nth(1).unwrap();
        format!("flare-recover://v1?p={payload}&k={ROOT_ID}&s={signature}")
    };
    assert_eq!(rejected(&forged).await, "badSignature");

    // 在线密钥签的不算：恢复清单只认内置根。
    let online = online_key();
    let by_online_key = crate::trust::sign::sign_recovery_link(
        &recovery(30),
        &Signer {
            key_id: ONLINE_ID,
            key: &online,
        },
    )
    .unwrap();
    assert_eq!(rejected(&by_online_key).await, "unknownKey");

    // 另一个应用的恢复清单。
    let mut other_app = recovery(30);
    other_app.bootstrap_id = "0000000000000000".into();
    assert_eq!(
        rejected(&recovery_link(&other_app)).await,
        "wrongBootstrapId"
    );

    // 三个接入点。
    let mut three = recovery(30);
    three
        .access_points
        .push(crate::manifest::RecoveryAccessPoint {
            id: "ap-3".into(),
            host: "c.example.net".into(),
            ip_hints: vec![],
            services: None,
        });
    three.sources.clear();
    assert_eq!(rejected(&recovery_link(&three)).await, "invalidPayload");

    // 超过 1 KiB 的链接。
    let mut large = recovery(30);
    large.sources = vec![format!("https://{}.example.net/", "m".repeat(60)); 4];
    large.access_points[0].ip_hints = vec!["203.0.113.20".into(); 8];
    let link = recovery_link(&large);
    assert!(link.len() > crate::manifest::MAX_RECOVERY_LINK_BYTES);
    assert_eq!(rejected(&link).await, "tooLarge");

    // 普通链接、缺字段。
    assert_eq!(
        rejected("https://example.com/?p=a&k=b&s=c").await,
        "malformedEnvelope"
    );
    assert_eq!(
        rejected("flare-recover://v1?p=abc").await,
        "malformedEnvelope"
    );
    assert_eq!(rejected("not a link").await, "malformedEnvelope");
    assert_eq!(lab.serial(), 10);

    // 镜像不能拿恢复清单顶替接入清单：只有用户导入的才走根签名路径。
    let envelope = Envelope::from_recovery_link(&good).unwrap();
    lab.net
        .serve(MIRROR_A, ACCESS_FILE, envelope.to_json_bytes());
    let report = lab.refresh(RefreshMode::Background).await;
    assert_eq!(
        attempt(&report, MIRROR_A).code.as_deref(),
        Some("wrongPayloadType")
    );
    assert_eq!(lab.serial(), 10);
}
