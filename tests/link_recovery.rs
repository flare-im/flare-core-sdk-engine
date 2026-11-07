//! 全故障恢复（实验台）：生产模式的目录与信任链，真实的 WebSocket 服务端、真实的 HTTP 假镜像。
//!
//! 全部接入点失效时客户端从引导面拿到新地址并连上；发件箱里的消息续发不丢、不重。

#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_im_core_sdk::event::{ConnectionEvent, SdkEvent, SdkEventKind};
use flare_im_core_sdk::link::flare_access::store::MemoryAccessStore;
use flare_im_core_sdk::prelude::*;

use link_lab::bootstrap::*;
use link_lab::*;

const CONNECT_DEADLINE: Duration = Duration::from_secs(15);
/// 从全部接入点失效到新接入点 CONNECT_ACK：两轮退避 + 一次拉取。方案的目标是 P95 ≤ 60 s。
const RECOVERY_DEADLINE: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_every_access_point_recovers_from_a_mirror_and_resends_the_outbox() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let old_a = AccessPoint::start("ap-old-a", ledger.clone(), ServerOptions::default()).await;
    let old_b = AccessPoint::start("ap-old-b", ledger.clone(), ServerOptions::default()).await;
    let new = AccessPoint::start("ap-new", ledger.clone(), ServerOptions::default()).await;
    let mirror_a = Mirror::start("mirror-a.lab.example").await;
    let mirror_b = Mirror::start("mirror-b.lab.example").await;
    let authority = Authority::new(&[&mirror_a, &mirror_b]);

    let seed = authority.manifest(10, &[&old_a, &old_b]);
    mirror_a.publish(ACCESS_FILE, authority.sign(&seed));
    mirror_b.publish(ACCESS_FILE, authority.sign(&seed));

    let root = data_root("link-recovery", "all-access-points");
    let device = Arc::new(MemoryAccessStore::new());
    let client = lab_client_with(
        authority.access(&seed),
        &root,
        |_| {},
        Some(lab_runtime(
            authority.access(&seed),
            &device,
            lab_network(&[&mirror_a, &mirror_b], &[&old_a, &old_b, &new]),
        )),
    )
    .await;
    let (observed, _subscriptions) = Observed::record(&client);
    let changed: Arc<Mutex<Vec<AccessChanged>>> = Arc::default();
    let sink = changed.clone();
    let _access_events = client
        .on_event_kind(SdkEventKind::Connection, move |event| {
            if let SdkEvent::Connection(ConnectionEvent::AccessChanged(payload)) = event.as_ref() {
                sink.lock().unwrap().push(payload.clone());
            }
        })
        .expect("subscribe access events");

    connect(&client).await;
    let online = wait_connected(&client, CONNECT_DEADLINE).await;
    let current = online.access_point_id.clone().unwrap();
    assert!(current.starts_with("ap-old"), "{current}");
    let generation = client.session_generation().await;

    // 刚才的周期刷新已确认清单是最新的：之后只有恢复才会去拉。
    let refreshed = client.refresh_access().await.expect("refresh");
    assert_eq!(refreshed.outcome, "upToDate");
    assert_eq!(refreshed.serial, 10);

    let mut sent = Vec::new();
    for index in 0..5 {
        sent.push(send_text(&client, &format!("before {index}")).await);
    }
    wait_until("the first messages accepted", CONNECT_DEADLINE, || async {
        observed.accepted_distinct().len() == 5
    })
    .await;

    // 全部接入点下线；运维把新地址发布到一个镜像，另一个镜像同时挂了。
    mirror_a.take_down();
    mirror_b.publish(
        ACCESS_FILE,
        authority.sign(&authority.manifest(11, &[&new])),
    );
    old_a.relay.take_down().await;
    old_b.relay.take_down().await;
    for index in 0..5 {
        sent.push(send_text(&client, &format!("during {index}")).await);
    }

    let reconnected = wait_until_on(&client, "ap-new", RECOVERY_DEADLINE).await;
    assert_eq!(reconnected.access_point_id.as_deref(), Some("ap-new"));
    wait_until("the outbox drained", RECOVERY_DEADLINE, || async {
        observed.accepted_distinct().len() == sent.len()
    })
    .await;

    assert!(
        observed
            .phases()
            .contains(&ConnectionPhase::ServiceUnavailable),
        "经过了恢复：{:?}",
        observed.phases()
    );
    assert!(observed.failed.lock().unwrap().is_empty());
    assert_eq!(client.session_generation().await, generation, "恢复不重登");
    let arrivals = ledger.lock().unwrap().first_arrivals.clone();
    assert_eq!(arrivals, sent, "零丢失、零重复、会话内有序");
    assert_eq!(
        new.gate
            .last_metadata
            .lock()
            .unwrap()
            .get("access_serial")
            .map(String::as_str),
        Some("11"),
        "新连接报的是刚采纳的清单"
    );

    let changes = changed.lock().unwrap().clone();
    let adopted = changes
        .iter()
        .find(|changed| changed.serial == 11)
        .expect("access://changed for the new manifest");
    assert_eq!(adopted.source, format!("https:{}", mirror_b.base_url()));
    assert_eq!(adopted.changes.access_points_added, ["ap-new"]);

    let status = client.access_status().await.expect("status");
    assert_eq!(status.serial, 11);
    assert_eq!(status.source.as_deref(), Some(adopted.source.as_str()));
    assert!(!status.stale);
    assert_eq!(status.recovering, None);
    let _ = client.logout().await;

    // 重启：新地址已写进 LKG，种子再旧也不会把它盖掉。
    let restarted = lab_client_with(
        authority.access(&seed),
        &root,
        |_| {},
        Some(lab_runtime(
            authority.access(&seed),
            &device,
            lab_network(&[&mirror_b], &[&new]),
        )),
    )
    .await;
    connect(&restarted).await;
    let online = wait_connected(&restarted, CONNECT_DEADLINE).await;
    assert_eq!(online.access_point_id.as_deref(), Some("ap-new"));
    let _ = restarted.logout().await;
}

/// CONNECT 报本地 serial，CONNECT_ACK 报服务端的：服务端更新时客户端自己去拉，不等通知。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_newer_serial_in_the_connect_ack_pulls_the_manifest() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let access_point = AccessPoint::start("ap-a", ledger.clone(), ServerOptions::default()).await;
    let extra = AccessPoint::start("ap-b", ledger.clone(), ServerOptions::default()).await;
    let mirror = Mirror::start("mirror.lab.example").await;
    let authority = Authority::new(&[&mirror]);
    let seed = authority.manifest(10, &[&access_point]);
    mirror.publish(
        ACCESS_FILE,
        authority.sign(&authority.manifest(12, &[&access_point, &extra])),
    );
    access_point.gate.access_serial.store(12, Ordering::SeqCst);

    let root = data_root("link-recovery", "connect-ack");
    let client = lab_client_with(
        authority.access(&seed),
        &root,
        |_| {},
        Some(lab_runtime(
            authority.access(&seed),
            &Arc::new(MemoryAccessStore::new()),
            lab_network(&[&mirror], &[&access_point, &extra]),
        )),
    )
    .await;
    connect(&client).await;
    wait_connected(&client, CONNECT_DEADLINE).await;
    assert_eq!(
        access_point
            .gate
            .last_metadata
            .lock()
            .unwrap()
            .get("access_serial")
            .map(String::as_str),
        Some("10")
    );
    wait_until("serial 12 adopted", Duration::from_secs(20), || async {
        client.access_status().await.expect("status").serial == 12
    })
    .await;
    assert!(mirror.requests() > 0);
    let status = client.access_status().await.expect("status");
    assert_eq!(
        status.access_point_id.as_deref(),
        Some("ap-a"),
        "当前接入点仍 active：不动"
    );
    assert_eq!(
        status.connect_ack.and_then(|ack| ack.access_serial),
        Some(12)
    );
    let _ = client.logout().await;
}

/// 等到连上指定的接入点。
async fn wait_until_on(
    client: &IMClient,
    access_point: &str,
    deadline: Duration,
) -> ConnectionStatus {
    let started = std::time::Instant::now();
    loop {
        let status = client.connection_status().await;
        if matches!(
            status.phase,
            ConnectionPhase::Syncing | ConnectionPhase::Online
        ) && status.access_point_id.as_deref() == Some(access_point)
        {
            return status;
        }
        assert!(
            started.elapsed() < deadline,
            "not on {access_point} within {deadline:?}; last {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
