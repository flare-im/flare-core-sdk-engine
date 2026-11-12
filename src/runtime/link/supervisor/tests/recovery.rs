//! 全故障恢复：生产模式的目录（真实验签与 LKG）、假镜像、假驱动。

use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use flare_access::store::{AccessStore, MemoryAccessStore, keys};
use flare_access::trust::sign::{
    Signer, SigningKey, encode_public_key, issue_key_cert, sign_access_manifest, sign_recovery_link,
};
use flare_access::trust::{KeyCert, KeyScope, TrustRootConfig};
use flare_access::{FetchError, FetchRequest, FetchResponse, HttpFetch};
use serde_json::{Value, json};

use super::*;
use crate::kernel::event::AccessChanged;
use crate::runtime::link::access::now_ms;

const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
const MIRROR: &str = "https://mirror.example.com/flare/v1/b7f3c9a1e2d4b6a8/";

fn rfc3339(offset: ChronoDuration) -> String {
    (Utc::now() + offset).to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn root_key() -> SigningKey {
    SigningKey::from_bytes(&[0x11; 32])
}

fn online_key() -> SigningKey {
    SigningKey::from_bytes(&[0x33; 32])
}

/// 一份清单：`access_points` 是（ID，状态）。
fn manifest(serial: u64, access_points: &[(&str, &str)]) -> Value {
    let access_points: Vec<Value> = access_points
        .iter()
        .map(|(id, status)| {
            json!({
                "id": id,
                "host": format!("{id}.example.com"),
                "status": status,
                "services": { "imWs": { "path": "/ws" } }
            })
        })
        .collect();
    json!({
        "schema": "flare.access.v1",
        "bootstrapId": BOOTSTRAP_ID,
        "appId": "app_1",
        "serial": serial,
        "issuedAt": rfc3339(ChronoDuration::hours(-1)),
        "expiresAt": rfc3339(ChronoDuration::days(30)),
        "regions": [{ "id": "region-1", "cells": [{ "id": "cell-1", "accessPoints": access_points }] }],
        "placement": { "homeCell": "cell-1" },
        "bootstrap": { "sources": [{ "kind": "https", "url": MIRROR }] },
        "clientPolicy": { "backoff": { "baseMs": 500, "capForegroundMs": 5_000 } }
    })
}

fn signed(value: &Value) -> Vec<u8> {
    let root = root_key();
    let online = online_key();
    let cert = issue_key_cert(
        &Signer {
            key_id: "root-a",
            key: &root,
        },
        &KeyCert::new(
            "ok-1",
            encode_public_key(&online.verifying_key()),
            vec!["access".into()],
            KeyScope {
                bootstrap_ids: vec!["*".into()],
            },
            rfc3339(ChronoDuration::days(-10)),
            rfc3339(ChronoDuration::days(100)),
            "root-a",
        ),
    );
    sign_access_manifest(
        &serde_json::to_vec(value).unwrap(),
        &Signer {
            key_id: "ok-1",
            key: &online,
        },
        &[cert],
    )
}

fn config(seed: Option<&Value>) -> AccessConfig {
    AccessConfig::production(
        BOOTSTRAP_ID,
        vec![TrustRootConfig {
            key_id: "root-a".into(),
            public_key: encode_public_key(&root_key().verifying_key()),
        }],
        seed.map(|seed| String::from_utf8(signed(seed)).unwrap()),
    )
}

/// 假镜像：有文档就回 200，没有就连不上。
#[derive(Default)]
struct FakeMirror {
    document: Mutex<Option<Vec<u8>>>,
    fetches: AtomicUsize,
}

impl FakeMirror {
    fn publish(&self, value: &Value) {
        *self.document.lock().unwrap() = Some(signed(value));
    }

    fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl HttpFetch for FakeMirror {
    async fn fetch(
        &self,
        request: &FetchRequest,
    ) -> std::result::Result<FetchResponse, FetchError> {
        if !request.url.ends_with("access.dsse.json") {
            return Ok(FetchResponse {
                status: 404,
                etag: None,
                body: Vec::new(),
            });
        }
        self.fetches.fetch_add(1, Ordering::SeqCst);
        match self.document.lock().unwrap().clone() {
            Some(body) => Ok(FetchResponse {
                status: 200,
                etag: None,
                body,
            }),
            None => Err(FetchError::Unreachable),
        }
    }
}

struct Lab {
    h: Harness,
    access: Arc<AccessRuntime>,
    mirror: Arc<FakeMirror>,
}

/// 刚刷新过的设备：周期刷新要等一个间隔，这段时间里只有被测的路径会去拉清单。
async fn lab(seed: Option<&Value>) -> Lab {
    let store = Arc::new(MemoryAccessStore::new());
    store
        .put(
            &keys::bootstrap_sources(BOOTSTRAP_ID),
            json!({ "lastRefreshMs": now_ms() }).to_string().as_bytes(),
        )
        .await
        .unwrap();
    let mirror = Arc::new(FakeMirror::default());
    let access = Arc::new(
        AccessRuntime::new(Some(config(seed)), AccessStoreSource::Ready(store))
            .with_http_fetch(mirror.clone()),
    );
    let driver = FakeDriver::default();
    let bus = EventBus::new();
    let rx = bus.subscribe_filter(SdkEventKind::Connection);
    let reach = reach_on("pc");
    let link = LinkHandle::spawn(
        Arc::new(driver.clone()),
        access.clone(),
        bus.clone(),
        true,
        reach.clone(),
    );
    Lab {
        h: Harness {
            driver,
            link,
            bus,
            rx,
            reach,
        },
        access,
        mirror,
    }
}

impl Lab {
    async fn next_changed(&mut self, serial: u64, within: Duration) -> AccessChanged {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let event = tokio::time::timeout(remaining, self.h.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("no access://changed for serial {serial}"))
                .expect("bus open");
            if let SdkEvent::Connection(ConnectionEvent::AccessChanged(changed)) = event
                && changed.serial == serial
            {
                return changed;
            }
        }
    }

    async fn wait_opened(&self, access_point: &str, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        while !self
            .h
            .driver
            .opened()
            .iter()
            .any(|(ap, _, _)| ap == access_point)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{access_point} was never dialled; opened {:?}, status {:?}",
                self.h.driver.opened(),
                self.h.link.status()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_fetches(&self, at_least: usize, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        while self.mirror.fetches() < at_least {
            assert!(
                tokio::time::Instant::now() < deadline,
                "only {} fetches",
                self.mirror.fetches()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn fail_forever(&self, access_point: &str) {
        self.h.driver.script(
            access_point,
            (0..200).map(|_| Err(unreachable())).collect::<Vec<_>>(),
        );
    }
}

const RECOVERY_DEADLINE: Duration = Duration::from_secs(10);

/// 连续两轮全部失败进入恢复，才去拉清单；采纳后不等退避，按新拓扑选路，连上即退出恢复。
#[tokio::test]
async fn two_failed_rounds_fetch_the_new_topology_and_connect_to_it() {
    let old = manifest(10, &[("ap-old", "active")]);
    let mut lab = lab(Some(&old)).await;
    lab.fail_forever("ap-old");
    lab.mirror.publish(&manifest(11, &[("ap-new", "active")]));

    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h.wait_phase(ConnectionPhase::Reconnecting, SHORT).await;
    assert_eq!(lab.mirror.fetches(), 0, "一轮失败还不到拉清单的时候");

    let changed = lab.next_changed(11, RECOVERY_DEADLINE).await;
    assert_eq!(changed.changes.access_points_added, ["ap-new"]);
    assert_eq!(changed.changes.access_points_removed, ["ap-old"]);
    assert_eq!(changed.source, format!("https:{MIRROR}"));
    assert!(!changed.stale);

    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(lab.h.driver.opened().last().unwrap().0, "ap-new");
    let status = lab.access.status();
    assert_eq!(status.serial, 11);
    assert_eq!(status.access_point_id.as_deref(), Some("ap-new"));
    // 退出恢复是发给刷新任务的命令，稍后生效。
    let deadline = tokio::time::Instant::now() + SHORT;
    while lab.access.status().recovering.is_some() {
        assert!(tokio::time::Instant::now() < deadline, "still recovering");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// 引导面也不通：恢复循环继续；网络变化、回到前台、点重试都立即来一轮，不等 30 秒的退避。
#[tokio::test]
async fn network_changes_and_the_foreground_restart_a_failed_recovery_round() {
    let old = manifest(10, &[("ap-old", "active")]);
    let mut lab = lab(Some(&old)).await;
    lab.fail_forever("ap-old");
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h
        .wait_phase(ConnectionPhase::ServiceUnavailable, RECOVERY_DEADLINE)
        .await;
    lab.wait_fetches(1, SHORT).await;
    let status = lab.access.status();
    let recovering = status.recovering.expect("recovering status");
    assert!(recovering.since_ms > 0);

    lab.h.link.network_changed(true, true).await;
    lab.wait_fetches(2, SHORT).await;

    lab.h.link.app_state(false).await;
    lab.h.link.app_state(true).await;
    lab.wait_fetches(3, SHORT).await;

    lab.mirror.publish(&manifest(11, &[("ap-new", "active")]));
    lab.h.link.reconnect_now().await;
    lab.wait_opened("ap-new", RECOVERY_DEADLINE).await;
}

#[tokio::test]
async fn a_lost_network_pauses_the_recovery_loop() {
    let old = manifest(10, &[("ap-old", "active")]);
    let mut lab = lab(Some(&old)).await;
    lab.fail_forever("ap-old");
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h
        .wait_phase(ConnectionPhase::ServiceUnavailable, RECOVERY_DEADLINE)
        .await;
    lab.wait_fetches(1, SHORT).await;

    lab.h.link.network_changed(false, false).await;
    lab.h
        .wait_phase(ConnectionPhase::WaitingForNetwork, SHORT)
        .await;
    let deadline = tokio::time::Instant::now() + SHORT;
    loop {
        let recovering = lab.access.status().recovering.expect("still recovering");
        if recovering.next_round_at_ms.is_none() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "not paused");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// CONNECT_ACK 报的 serial 比本地新：刷新；当前接入点还在清单里且 active，不动它。
#[tokio::test]
async fn a_newer_serial_in_the_connect_ack_triggers_a_refresh() {
    let old = manifest(10, &[("ap-a", "active")]);
    let mut lab = lab(Some(&old)).await;
    let mut report = ok_report();
    report.ack.access_serial = Some(11);
    lab.h.driver.script("ap-a", [Ok(report)]);
    lab.mirror
        .publish(&manifest(11, &[("ap-a", "active"), ("ap-b", "active")]));

    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    let changed = lab.next_changed(11, RECOVERY_DEADLINE).await;
    assert_eq!(changed.changes.access_points_added, ["ap-b"]);
    assert!(changed.changes.access_points_removed.is_empty());
    assert_eq!(lab.h.driver.opened().len(), 1, "只更新候选，不换线路");
}

#[tokio::test]
async fn an_equal_serial_in_the_connect_ack_fetches_nothing() {
    let old = manifest(10, &[("ap-a", "active")]);
    let mut lab = lab(Some(&old)).await;
    let mut report = ok_report();
    report.ack.access_serial = Some(10);
    lab.h.driver.script("ap-a", [Ok(report)]);
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(lab.mirror.fetches(), 0);
}

/// `flare.access` 通知：拉到新清单，当前接入点被摘除，立即切到新的（不重登）。
#[tokio::test]
async fn a_notice_adopts_the_manifest_and_moves_off_a_removed_access_point() {
    let old = manifest(10, &[("ap-old", "active")]);
    let mut lab = lab(Some(&old)).await;
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;
    lab.mirror.publish(&manifest(11, &[("ap-new", "active")]));

    lab.access.notify_access_serial(11);
    lab.next_changed(11, RECOVERY_DEADLINE).await;
    let switched = lab.h.next_switched(RECOVERY_DEADLINE).await;
    assert_eq!(switched.from.ap, "ap-old");
    assert_eq!(switched.to.ap, "ap-new");
    assert_eq!(
        switched.reason,
        crate::runtime::link::SwitchReason::Manifest
    );
    assert!(
        matches!(
            lab.h.driver.link_ups().last(),
            Some(LinkUp::Reconnected { .. })
        ),
        "切换走断线重连的路径：发件箱恢复、补同步，不重登"
    );
}

/// 新清单只留下不接新连接的接入点：没有别处可去，留着这条还能用的连接。
#[tokio::test]
async fn a_retired_access_point_stays_in_use_when_nothing_else_is_on_offer() {
    let old = manifest(10, &[("ap-old", "active")]);
    let mut lab = lab(Some(&old)).await;
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;
    lab.mirror
        .publish(&manifest(11, &[("ap-other", "draining")]));

    lab.access.notify_access_serial(11);
    lab.next_changed(11, RECOVERY_DEADLINE).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(lab.h.driver.opened().len(), 1);
    assert_eq!(
        lab.h.link.status().phase,
        ConnectionPhase::Online,
        "没有候选时不主动断开"
    );
}

/// 新清单把当前接入点改成 draining：不是立刻走，窗口内随机时刻再迁（防止全网同时重连）。
#[tokio::test]
async fn a_draining_access_point_is_left_inside_a_window_not_at_once() {
    let old = manifest(10, &[("ap-a", "active"), ("ap-b", "active")]);
    let mut lab = lab(Some(&old)).await;
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;
    let dialled = lab.h.driver.opened().len();
    let current = lab.access.status().access_point_id.unwrap();
    let other = if current == "ap-a" { "ap-b" } else { "ap-a" };
    lab.mirror.publish(&manifest(
        11,
        &[(current.as_str(), "draining"), (other, "active")],
    ));

    lab.access.notify_access_serial(11);
    lab.next_changed(11, RECOVERY_DEADLINE).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(lab.h.link.status().phase, ConnectionPhase::Online);
    assert_eq!(lab.h.driver.opened().len(), dialled);
}

fn recovery_link(serial: u64) -> String {
    let root = root_key();
    sign_recovery_link(
        &flare_access::manifest::RecoveryManifest {
            schema: "flare.recovery.v1".into(),
            bootstrap_id: BOOTSTRAP_ID.into(),
            app_id: "app_1".into(),
            serial,
            issued_at: rfc3339(ChronoDuration::hours(-1)),
            expires_at: rfc3339(ChronoDuration::days(14)),
            region_id: "region-1".into(),
            cell_id: "cell-1".into(),
            access_points: vec![flare_access::manifest::RecoveryAccessPoint {
                id: "ap-rescue".into(),
                host: "rescue.example.net".into(),
                ip_hints: vec![],
                services: None,
            }],
            sources: vec![MIRROR.into()],
        },
        &Signer {
            key_id: "root-a",
            key: &root,
        },
    )
    .unwrap()
}

/// 全新安装、种子不可用：目录为空，第一轮就是恢复；导入恢复链接后立刻连上。
#[tokio::test]
async fn a_recovery_link_rescues_a_session_with_an_empty_directory() {
    let mut lab = lab(None).await;
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .expect("没有清单也保留会话，由状态表达");
    lab.h
        .wait_phase(ConnectionPhase::ServiceUnavailable, SHORT)
        .await;
    assert!(!lab.access.status().loaded);

    let forged = lab
        .access
        .import_recovery("flare-recover://v1?p=e30&k=root-a&s=AAAA")
        .await
        .unwrap();
    assert_eq!(forged.outcome, "rejected");
    assert_eq!(forged.code.as_deref(), Some("badSignature"));
    assert_eq!(forged.serial, 0);

    let imported = lab
        .access
        .import_recovery(&recovery_link(30))
        .await
        .unwrap();
    assert_eq!(imported.outcome, "adopted");
    assert_eq!(imported.code, None);
    assert_eq!(imported.serial, 30);
    let changed = lab.next_changed(30, SHORT).await;
    assert_eq!(changed.source, "recovery");
    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(lab.h.driver.opened().last().unwrap().0, "ap-rescue");

    let again = lab
        .access
        .import_recovery(&recovery_link(30))
        .await
        .unwrap();
    assert_eq!(again.outcome, "unchanged");
    let older = lab
        .access
        .import_recovery(&recovery_link(29))
        .await
        .unwrap();
    assert_eq!(older.outcome, "rejected");
    assert_eq!(older.code.as_deref(), Some("rollback"));

    // 恢复清单只是临时目录：导入后马上去引导源换完整清单。
    lab.mirror
        .publish(&manifest(31, &[("ap-rescue", "active")]));
    lab.wait_fetches(1, SHORT).await;
}

/// `access.refresh`：立即一轮，结果里有每个源的情况。
#[tokio::test]
async fn a_manual_refresh_reports_each_source() {
    let old = manifest(10, &[("ap-a", "active")]);
    let lab = lab(Some(&old)).await;
    let report = lab.access.refresh().await.unwrap();
    assert_eq!(report.outcome, "failed");
    assert_eq!(report.mode, "background");
    assert_eq!(report.attempts[0].source, format!("https:{MIRROR}"));
    assert_eq!(report.attempts[0].tier, "mirror");
    assert_eq!(report.attempts[0].result, "failed");
    assert_eq!(report.attempts[0].code.as_deref(), Some("unreachable"));

    lab.mirror.publish(&manifest(12, &[("ap-a", "active")]));
    let report = lab.access.refresh().await.unwrap();
    assert_eq!(report.outcome, "adopted");
    assert_eq!(report.serial, 12);
    assert_eq!(
        report.source.as_deref(),
        Some(format!("https:{MIRROR}").as_str())
    );
    let status = lab.access.status();
    assert_eq!(status.serial, 12);
    let refresh = status.refresh.unwrap();
    assert_eq!(refresh.last_outcome.as_deref(), Some("adopted"));
    assert!(refresh.last_success_at_ms.is_some());
    assert!(refresh.next_at_ms.is_some());
    assert!(status.fetched_at_ms.is_some());
}

/// 清单里的 `ipHints` 一路带到建连目标与 REST 的解析器；不属于接入点的主机没有。
#[tokio::test]
async fn ip_hints_reach_the_dial_target_and_the_http_router() {
    let mut seed = manifest(10, &[("ap-a", "active")]);
    seed["regions"][0]["cells"][0]["accessPoints"][0]["ipHints"] =
        json!(["203.0.113.10", "203.0.113.11"]);
    let mut lab = lab(Some(&seed)).await;
    lab.h
        .link
        .start("alice", Tokens::new("t1", []))
        .await
        .unwrap();
    lab.h.wait_phase(ConnectionPhase::Online, SHORT).await;

    let hints: Vec<std::net::IpAddr> = vec![
        "203.0.113.10".parse().unwrap(),
        "203.0.113.11".parse().unwrap(),
    ];
    assert_eq!(lab.h.driver.ip_hints(), hints);

    let router = crate::infrastructure::transport::http::router::HttpRouter::new(
        lab.access.clone(),
        flare_access::ServiceClass::CoreApi,
    );
    assert_eq!(router.ip_hints_for_host("AP-A.example.com"), hints);
    assert!(router.ip_hints_for_host("mirror.example.com").is_empty());
}

const DOH_RESOLVER: &str = "https://1.1.1.1/dns-query";
const DOH_HOST: &str = "ap-a.example.com";

/// 假解析器：A 与 AAAA 各答一个地址。
#[derive(Default)]
struct FakeResolver {
    asked: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl HttpFetch for FakeResolver {
    async fn fetch(
        &self,
        request: &FetchRequest,
    ) -> std::result::Result<FetchResponse, FetchError> {
        use flare_access::bootstrap::doh::{self, AddressRecord};

        self.asked.lock().unwrap().push(request.url.clone());
        let v4 = doh::address_query_url(DOH_RESOLVER, DOH_HOST, AddressRecord::A);
        let (record, address) = if v4.as_deref() == Some(request.url.as_str()) {
            (AddressRecord::A, "198.51.100.20")
        } else {
            (AddressRecord::Aaaa, "2001:db8::20")
        };
        let query = doh::encode_address_query(DOH_HOST, record).unwrap();
        Ok(FetchResponse {
            status: 200,
            etag: None,
            body: doh::encode_address_response(&query, &[address.parse().unwrap()], 120),
        })
    }
}

fn with_doh_resolver(mut seed: Value) -> Value {
    seed["bootstrap"]["sources"] = json!([
        { "kind": "https", "url": MIRROR },
        { "kind": "dohTxt", "name": "_flare.example.org", "resolvers": [DOH_RESOLVER] }
    ]);
    seed
}

/// 清单列了解析器：接入点和镜像的主机能查，别的主机不发给解析器。答复从清单的解析器来，带 TTL。
#[tokio::test]
async fn doh_asks_the_manifest_resolvers_about_listed_hosts_only() {
    use flare_core::client::dial::DohLookup;

    use crate::infrastructure::transport::http::doh::DirectoryDoh;

    let seed = with_doh_resolver(manifest(10, &[("ap-a", "active")]));
    let access = AccessRuntime::new(Some(config(Some(&seed))), AccessStoreSource::memory());
    let resolver = Arc::new(FakeResolver::default());
    let lookup = DirectoryDoh::new(resolver.clone());
    assert!(!lookup.covers(DOH_HOST), "目录没装配时什么都不查");
    assert_eq!(lookup.lookup(DOH_HOST).await, None);

    lookup.attach(access.kernel().await.unwrap());
    assert!(lookup.covers("AP-A.example.com"));
    assert!(lookup.covers("mirror.example.com"));
    assert!(!lookup.covers("objects.third-party.example"));
    assert!(!lookup.covers("1.1.1.1"));

    let answer = lookup.lookup(DOH_HOST).await.unwrap();
    assert_eq!(
        answer.addresses,
        [
            "198.51.100.20".parse::<std::net::IpAddr>().unwrap(),
            "2001:db8::20".parse().unwrap(),
        ]
    );
    assert_eq!(answer.ttl, Duration::from_secs(120));
    assert_eq!(resolver.asked.lock().unwrap().len(), 2);
}

/// 发布清单的下载主机记下来才查，媒体直链的主机一直不查。
#[tokio::test]
async fn doh_covers_signed_download_hosts_only_once_they_are_named() {
    use flare_core::client::dial::DohLookup;

    use crate::infrastructure::transport::http::doh::DirectoryDoh;

    let seed = with_doh_resolver(manifest(10, &[("ap-a", "active")]));
    let access = AccessRuntime::new(Some(config(Some(&seed))), AccessStoreSource::memory());
    let lookup = DirectoryDoh::new(Arc::new(FakeResolver::default()));
    lookup.attach(access.kernel().await.unwrap());
    assert!(!lookup.covers("dl.example-cdn.com"));

    let urls = [
        "https://DL.example-cdn.com/acme/app.apk".to_owned(),
        "not a url".to_owned(),
    ];
    lookup.cover_downloads(&urls);
    assert!(lookup.covers("dl.example-cdn.com"));
    assert!(lookup.covers("DL.EXAMPLE-CDN.COM"));
    assert!(!lookup.covers("objects.third-party.example"));
}

/// 记的主机有上限，满了就从头记，刚记的一定在。
#[tokio::test]
async fn the_download_hosts_are_bounded() {
    use flare_core::client::dial::DohLookup;

    use crate::infrastructure::transport::http::doh::DirectoryDoh;

    let seed = with_doh_resolver(manifest(10, &[("ap-a", "active")]));
    let access = AccessRuntime::new(Some(config(Some(&seed))), AccessStoreSource::memory());
    let lookup = DirectoryDoh::new(Arc::new(FakeResolver::default()));
    lookup.attach(access.kernel().await.unwrap());

    let url = |index: usize| format!("https://dl-{index}.example-cdn.com/app.apk");
    for index in 0..1_000 {
        lookup.cover_downloads(&[url(index)]);
    }
    assert!(lookup.covers("dl-999.example-cdn.com"));
    assert!(!lookup.covers("dl-0.example-cdn.com"));
}

/// 清单没列解析器：记了下载主机也不查。
#[tokio::test]
async fn doh_needs_a_resolver_in_the_manifest() {
    use flare_core::client::dial::DohLookup;

    use crate::infrastructure::transport::http::doh::DirectoryDoh;

    let seed = manifest(10, &[("ap-a", "active")]);
    let access = AccessRuntime::new(Some(config(Some(&seed))), AccessStoreSource::memory());
    let lookup = DirectoryDoh::new(Arc::new(FakeResolver::default()));
    lookup.attach(access.kernel().await.unwrap());
    assert!(!lookup.covers(DOH_HOST));

    lookup.cover_downloads(&["https://dl.example-cdn.com/app.apk".to_owned()]);
    assert!(!lookup.covers("dl.example-cdn.com"));
}

/// 运行时把解析器接到目录上，网络变了就忘掉答复和退避；开发模式端点由配置决定，不查。
#[tokio::test]
async fn the_runtime_wires_doh_to_the_directory_and_the_network() {
    use crate::infrastructure::transport::http::doh::DirectoryDoh;
    use crate::infrastructure::transport::http::doh::tests::settle;

    let seed = with_doh_resolver(manifest(10, &[("ap-a", "active")]));
    let resolver = Arc::new(FakeResolver::default());
    let access = AccessRuntime::new(Some(config(Some(&seed))), AccessStoreSource::memory())
        .with_doh(Arc::new(DirectoryDoh::new(resolver.clone())));
    let doh = access.doh().unwrap();
    doh.request(DOH_HOST);
    settle().await;
    assert!(resolver.asked.lock().unwrap().is_empty(), "目录还没装配");

    access.kernel().await.unwrap();
    doh.request(DOH_HOST);
    settle().await;
    assert_eq!(doh.addresses(DOH_HOST).len(), 2);

    assert!(access.set_network(Some("wifi"), Some("office")));
    assert!(doh.addresses(DOH_HOST).is_empty());

    let resolver = Arc::new(FakeResolver::default());
    let access = AccessRuntime::new(Some(two_access_points()), AccessStoreSource::memory())
        .with_doh(Arc::new(DirectoryDoh::new(resolver.clone())));
    let doh = access.doh().unwrap();
    access.kernel().await.unwrap();
    doh.request("ap-a.example.com");
    settle().await;
    assert!(resolver.asked.lock().unwrap().is_empty());
}

/// 开发模式：端点由配置决定，不起刷新任务。
#[tokio::test]
async fn development_mode_skips_refresh_and_recovery_links() {
    let access = Arc::new(
        AccessRuntime::new(Some(two_access_points()), AccessStoreSource::memory())
            .with_http_fetch(Arc::new(FakeMirror::default())),
    );
    let report = access.refresh().await.unwrap();
    assert_eq!(report.outcome, "skipped");
    let imported = access.import_recovery(&recovery_link(30)).await.unwrap();
    assert_eq!(imported.outcome, "rejected");
    assert_eq!(imported.code.as_deref(), Some("developmentMode"));
    assert!(access.status().refresh.is_none());
}
