//! 多接入点故障转移：切换不重登、过载不迁移、排空窗口内重连、半开连接被戳穿、发件箱跨会话恢复。
//!
//! 本机两个「接入点」（真实 flare-core WebSocket 服务端 + 可控中继），SDK 以开发模式多接入点接入，
//! 共享的服务端账本模拟集群的消息幂等（同一 `client_msg_id` 永远同一个序号）。

#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flare_core::client::builder::flare::{FlareClientBuilder, MessageListener};
use flare_core::client::link::{CloseKind, DisconnectReason, ServerClose};
use flare_core::common::protocol::{Frame, Reliability, frame_with_system_command};
use flare_im_core_sdk::prelude::*;

use link_lab::*;

async fn two_access_points(
    case: &str,
    first: ServerOptions,
) -> (
    AccessPoint,
    AccessPoint,
    Arc<Mutex<Ledger>>,
    std::path::PathBuf,
) {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start("development", ledger.clone(), first).await;
    let b = AccessPoint::start("ap-b", ledger.clone(), ServerOptions::default()).await;
    (a, b, ledger, data_root("link-failover", case))
}

fn access_of(a: &AccessPoint, b: &AccessPoint) -> AccessConfig {
    development_access(&[(&a.id, a.ws_url()), (&b.id, b.ws_url())])
}

fn by_id<'a>(
    id: &str,
    a: &'a AccessPoint,
    b: &'a AccessPoint,
) -> (&'a AccessPoint, &'a AccessPoint) {
    if id == a.id { (a, b) } else { (b, a) }
}

/// 健康连接上背靠背连发 100 条，在途窗口用默认值（> 1，等回执是并行的）：服务端首次收到的顺序必须就是
/// 发送顺序。往连接上写帧是串行的，写完一条才写下一条；窗口只决定能同时等多少条的回执。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn back_to_back_sends_on_a_healthy_link_arrive_in_order() {
    let (a, b, ledger, root) = two_access_points("healthy-100", ServerOptions::default()).await;
    let client = lab_client(access_of(&a, &b), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(15)).await;

    let mut sent = Vec::new();
    for index in 0..100 {
        sent.push(send_text(&client, &format!("message {index}")).await);
    }
    wait_until(
        "all 100 messages accepted",
        Duration::from_secs(60),
        || async { observed.accepted_distinct().len() == 100 },
    )
    .await;
    let ledger = ledger.lock().unwrap().first_arrivals.clone();
    assert_eq!(ledger.len(), 100, "every message arrives once");
    let swapped: Vec<usize> = (0..100)
        .filter(|&index| ledger[index] != sent[index])
        .collect();
    assert!(
        swapped.is_empty(),
        "the server saw them in the order they were sent; out of place at {swapped:?}"
    );
    let _ = client.logout().await;
}

/// 当前接入点挂掉：切到另一个接入点，不重登（会话世代不变），期间背靠背（不留间隔）连续发 100 条：
/// 零失败、零重复，且服务端首次收到的顺序就是发送顺序（会话内有序）。同一毫秒里入队的几条也不例外。
/// 在途窗口用默认值：已写出没回执的那些在新连接上按入队顺序重发。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_an_access_point_switches_without_relogin_and_loses_nothing() {
    let (a, b, ledger, root) = two_access_points("switch-100", ServerOptions::default()).await;
    let client = lab_client(access_of(&a, &b), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    let online = wait_connected(&client, Duration::from_secs(15)).await;
    let generation = client.session_generation().await;
    let (current, other) = by_id(online.access_point_id.as_deref().unwrap(), &a, &b);

    let mut sent = Vec::new();
    for index in 0..100 {
        sent.push(send_text(&client, &format!("message {index}")).await);
        if index == 30 {
            // 第 31 条入队后，当前接入点整体不可用（现有连接断开、新连接被拒）。
            current.relay.take_down().await;
        }
    }

    wait_until(
        "all 100 messages accepted",
        Duration::from_secs(60),
        || async { observed.accepted_distinct().len() == 100 },
    )
    .await;
    assert!(
        observed.failed.lock().unwrap().is_empty(),
        "no message may fail: {:?}",
        observed.failed.lock().unwrap()
    );
    {
        let ledger = ledger.lock().unwrap();
        assert_eq!(ledger.first_arrivals.len(), 100, "every message arrives");
        assert_eq!(
            ledger.seq_of.len(),
            100,
            "and exactly once (by client_msg_id)"
        );
        assert_eq!(ledger.first_arrivals, sent, "in the order they were sent");
    }

    let switched = observed.switched.lock().unwrap().clone();
    assert_eq!(switched.len(), 1, "{switched:?}");
    assert_eq!(switched[0].1.from.ap, current.id);
    assert_eq!(switched[0].1.to.ap, other.id);
    assert_eq!(switched[0].1.reason, SwitchReason::Failure);
    assert_eq!(
        client.session_generation().await,
        generation,
        "a switch is not a relogin"
    );
    assert_eq!(other.gate.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(
        client.connection_status().await.access_point_id.as_deref(),
        Some(other.id.as_str())
    );
    let _ = client.logout().await;
    let _ = tokio::fs::remove_dir_all(root).await;
}

struct Silent;

#[async_trait::async_trait]
impl MessageListener for Silent {
    async fn on_message(&self, _frame: &Frame) -> flare_core::common::error::Result<Option<Frame>> {
        Ok(None)
    }

    async fn on_disconnect(
        &self,
        _reason: &DisconnectReason,
    ) -> flare_core::common::error::Result<()> {
        Ok(())
    }
}

/// 服务端过载（连接数已满，握手回 OVERLOADED）：在同一个接入点等它给的窗口再试，不迁移到别的接入点。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_waits_on_the_same_access_point() {
    let (a, b, _ledger, root) = two_access_points(
        "overload",
        ServerOptions {
            max_connections: Some(1),
            overload_retry_after: Some(Duration::from_millis(800)),
            ..Default::default()
        },
    )
    .await;
    // B 的服务端停掉（中继在，连接一建立就被关）：选到 B 只会失败。
    b.server.stop().await.expect("stop B");
    // 另一个客户端占满 A 的唯一名额。
    let occupant = FlareClientBuilder::new(format!("ws://{}", a.server_address))
        .with_user_id("occupant".into())
        .with_token("token".into())
        .with_listener(Arc::new(Silent))
        .build_with_race()
        .await
        .expect("occupant connects");

    let client = lab_client(access_of(&a, &b), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    let result = client.connect(USER_ID, Some("token")).await;
    let overloaded_at = Instant::now();
    assert!(
        result.is_ok(),
        "overloaded: the link keeps the session and waits, so connect succeeds"
    );
    let rejections_on_b = b.relay.accepted();

    // 名额让出来之后，A 上的重试成功。
    tokio::time::sleep(Duration::from_millis(300)).await;
    occupant.disconnect().await.expect("occupant leaves");
    let online = wait_connected(&client, Duration::from_secs(15)).await;
    assert_eq!(online.access_point_id.as_deref(), Some("development"));
    assert!(
        observed.phases().contains(&ConnectionPhase::Reconnecting),
        "the wait shows as reconnecting with a countdown"
    );
    // 原地等的时候报的就是在等的那个接入点（不是 null）。
    let waits: Vec<ConnectionStatus> = observed
        .statuses
        .lock()
        .unwrap()
        .iter()
        .map(|(_, status)| status.clone())
        .filter(|status| status.phase == ConnectionPhase::Reconnecting)
        .collect();
    assert!(
        waits.iter().all(
            |status| status.access_point_id.as_deref() == Some("development")
                && status.cell_id.is_some()
                && status.region_id.is_some()
        ),
        "reconnecting names the access point it waits on: {waits:?}"
    );
    assert_eq!(
        b.relay.accepted(),
        rejections_on_b,
        "overload must not migrate to another access point"
    );
    assert!(a.relay.accepted() >= 2);
    assert!(overloaded_at.elapsed() < Duration::from_secs(15));
    let _ = client.logout().await;
}

/// 排空 / 停机（CLOSE 不带优先接入点）：到点后直接连另一个接入点，发出 CLOSE 的那个一次都不碰；
/// 等的时候状态里报的就是要去的那个接入点。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_access_point_is_left_alone_on_reconnect() {
    for kind in [CloseKind::Drain, CloseKind::ServerShutdown] {
        let case = format!("drain-avoid-{kind:?}").to_lowercase();
        let (a, b, _ledger, root) = two_access_points(&case, ServerOptions::default()).await;
        let client = lab_client(access_of(&a, &b), &root, |_| {}).await;
        let (observed, _subscriptions) = Observed::record(&client);
        connect(&client).await;
        let online = wait_connected(&client, Duration::from_secs(15)).await;
        let (current, other) = by_id(online.access_point_id.as_deref().unwrap(), &a, &b);
        wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(10)).await;
        let accepted_on_current = current.relay.accepted();
        let statuses_before = observed.statuses.lock().unwrap().len();

        let close = ServerClose::new(kind)
            .with_retry_window(Duration::from_millis(400), Duration::from_millis(900));
        current
            .server
            .broadcast(&frame_with_system_command(
                close.to_command(),
                Reliability::AtLeastOnce,
            ))
            .await
            .expect("broadcast CLOSE");
        wait_until(
            "switched to the other access point",
            Duration::from_secs(10),
            || async { !observed.switched.lock().unwrap().is_empty() },
        )
        .await;
        let switched = observed.switched.lock().unwrap()[0].1.clone();
        assert_eq!(switched.to.ap, other.id, "{kind:?}");
        assert_eq!(switched.reason, SwitchReason::Drain, "{kind:?}");
        assert_eq!(
            current.relay.accepted(),
            accepted_on_current,
            "{kind:?}: the access point that sent CLOSE is never dialled again"
        );
        let waiting: Vec<ConnectionStatus> = observed.statuses.lock().unwrap()[statuses_before..]
            .iter()
            .map(|(_, status)| status.clone())
            .filter(|status| status.phase == ConnectionPhase::Switching)
            .collect();
        assert!(
            !waiting.is_empty()
                && waiting
                    .iter()
                    .all(|status| status.access_point_id.as_deref() == Some(other.id.as_str())),
            "{kind:?}: switching names the access point it goes to: {waiting:?}"
        );
        let _ = client.logout().await;
    }
}

/// 计划性关闭（DRAIN，带窗口与优先接入点）：在窗口内随机时刻重连，连到服务端建议的接入点。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_reconnects_inside_the_window_to_the_preferred_access_point() {
    let (a, b, _ledger, root) = two_access_points("drain", ServerOptions::default()).await;
    let client = lab_client(access_of(&a, &b), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    let online = wait_connected(&client, Duration::from_secs(15)).await;
    let (current, other) = by_id(online.access_point_id.as_deref().unwrap(), &a, &b);
    // 补同步收尾后再排空：之后出现的第一个非在线阶段就是客户端对 CLOSE 的反应。
    wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(10)).await;
    let accepted_before = other.relay.accepted();
    let statuses_before = observed.statuses.lock().unwrap().len();

    let close = ServerClose::new(CloseKind::Drain)
        .with_retry_window(Duration::from_millis(400), Duration::from_millis(900))
        .with_prefer_ap([other.id.clone()]);
    current
        .server
        .broadcast(&frame_with_system_command(
            close.to_command(),
            Reliability::AtLeastOnce,
        ))
        .await
        .expect("broadcast CLOSE(DRAIN)");
    let closed_at = Instant::now();
    // 与真实服务端一致：下发 CLOSE 后给客户端一段宽限自行断开，期满才强关剩下的连接。
    // 立刻强关会和 CLOSE 帧赛跑，客户端可能只看到对端关闭，那就成了一次普通故障。
    wait_until(
        "the client leaves on CLOSE(DRAIN)",
        Duration::from_secs(2),
        || async {
            observed.phases()[statuses_before..]
                .iter()
                .any(|phase| !matches!(phase, ConnectionPhase::Online | ConnectionPhase::Syncing))
        },
    )
    .await;
    let connections = current.gate.connections.lock().unwrap().clone();
    for connection in connections {
        let _ = current.server.disconnect(&connection).await;
    }

    wait_until(
        "switched to the preferred access point",
        Duration::from_secs(10),
        || async { !observed.switched.lock().unwrap().is_empty() },
    )
    .await;
    let reconnected_at = other.relay.accepted_at()[accepted_before];
    let waited = reconnected_at.duration_since(closed_at);
    assert!(
        waited >= Duration::from_millis(350),
        "reconnects inside the window, not at once: {waited:?}"
    );
    assert!(waited < Duration::from_secs(5), "{waited:?}");
    let switched = observed.switched.lock().unwrap()[0].1.clone();
    assert_eq!(switched.to.ap, other.id);
    assert_eq!(switched.reason, SwitchReason::Drain);
    let _ = client.logout().await;
}

/// 半开连接（两端套接字都在、字节再也不通）：消息发出后迟迟没有回执即验活，验活窗口内判死，
/// 切到另一个接入点，消息以原 client_msg_id 重发、恰好一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_half_open_link_is_found_while_sending_and_switched() {
    let (a, b, ledger, root) = two_access_points("half-open", ServerOptions::default()).await;
    let client = lab_client(access_of(&a, &b), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    let online = wait_connected(&client, Duration::from_secs(15)).await;
    let (current, other) = by_id(online.access_point_id.as_deref().unwrap(), &a, &b);

    current.relay.freeze();
    let sent_at = Instant::now();
    let client_msg_id = send_text(&client, "into the void").await;
    wait_until(
        "accepted on the other access point",
        Duration::from_secs(20),
        || async { observed.accepted_distinct().contains(&client_msg_id) },
    )
    .await;
    let found = sent_at.elapsed();
    // 发出未回执 1.5 s 触发验活，验活窗口（probeWakeTimeoutMs）缺省 5 s：有发送时 ≤ 8 s 发现并切换。
    assert!(found <= Duration::from_secs(8), "found after {found:?}");
    assert_eq!(
        client.connection_status().await.access_point_id.as_deref(),
        Some(other.id.as_str())
    );
    assert_eq!(ledger.lock().unwrap().seq_of.len(), 1, "exactly once");
    assert!(observed.failed.lock().unwrap().is_empty());
    let _ = client.logout().await;
}

/// 同一用户重新 prepare（含进程重启后的冷启动）：发件箱原样恢复，上线后继续发出；
/// 连接从未建立过的离线消息也在上线后发出，且只发一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_user_resumes_the_outbox_after_prepare() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start("development", ledger.clone(), ServerOptions::default()).await;
    let root = data_root("link-failover", "outbox-resume");
    let access = development_access(&[(&a.id, a.ws_url())]);
    let stores = in_memory_im_provider();

    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                access: Some(access.clone()),
                ..Default::default()
            }),
        )
        .await
        .expect("init");
    client
        .prepare(USER_ID, LoginDbKind::IndexedDb(stores.clone()))
        .await
        .expect("prepare");
    // 还没连：消息入队、乐观上屏，不发。
    let offline = send_text(&client, "queued before connecting").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(ledger.lock().unwrap().first_arrivals.is_empty());
    // 「重启」：同一用户再 prepare 一次（同一份本地库）。
    client
        .prepare(USER_ID, LoginDbKind::IndexedDb(stores.clone()))
        .await
        .expect("prepare again");
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    wait_until(
        "the resumed message is accepted",
        Duration::from_secs(15),
        || async { observed.accepted_distinct().contains(&offline) },
    )
    .await;
    {
        let ledger = ledger.lock().unwrap();
        assert_eq!(ledger.first_arrivals, vec![offline]);
        assert_eq!(ledger.received_total, 1);
    }
    let _ = client.logout().await;
}

/// QUIC 被静默丢弃（UDP 黑洞）：新网络上 QUIC 先发、交错后 WebSocket 胜出；胜者记在该网络下，
/// 之后的建连 WebSocket 打头，赶在交错之前握手成功，不再往黑洞里发包。
///
/// 累计三次静默才记住 UDP 封锁、把 QUIC 整个剔出计划的那条规则，要求每次都是 QUIC 打头（新网络或
/// 胜者记忆失效）；它由 flare-access 健康簿与 `report_records` 的单测覆盖。
#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_quic_loses_once_and_later_connects_lead_with_websocket() {
    use std::sync::atomic::AtomicUsize;

    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start("development", ledger, ServerOptions::default()).await;
    let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let quic_port = blackhole.local_addr().unwrap().port();
    let datagrams = Arc::new(AtomicUsize::new(0));
    let swallow = tokio::spawn({
        let datagrams = datagrams.clone();
        async move {
            let mut buffer = [0u8; 2048];
            while blackhole.recv_from(&mut buffer).await.is_ok() {
                datagrams.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    let mut access = development_access(&[(&a.id, a.ws_url())]);
    if let Some(endpoints) = access.development_endpoints.as_mut() {
        endpoints.quic_url = Some(format!("quic://127.0.0.1:{quic_port}"));
    }
    let root = data_root("link-failover", "udp-silent");
    let client = lab_client(access, &root, |_| {}).await;
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(15)).await;
    assert_eq!(
        client.connection_status().await.protocol.as_deref(),
        Some("ws"),
        "WebSocket wins over the silent QUIC"
    );
    assert!(
        datagrams.load(Ordering::SeqCst) > 0,
        "QUIC led the first race on a new network"
    );
    let points = client.list_access_points().await.unwrap();
    assert_eq!(
        points[0].protocol_winner.as_deref(),
        Some("ws"),
        "{points:?}"
    );

    // 被取消的 QUIC 尝试还会补发一两个包（重传、关闭）：等计数稳定再取基线。
    let mut before = datagrams.load(Ordering::SeqCst);
    let settle_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let now = datagrams.load(Ordering::SeqCst);
        if now == before || Instant::now() >= settle_deadline {
            before = now;
            break;
        }
        before = now;
    }
    for round in 1..=2 {
        a.relay.cut().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        wait_connected(&client, Duration::from_secs(15)).await;
        assert_eq!(
            client.connection_status().await.protocol.as_deref(),
            Some("ws"),
            "round {round}"
        );
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        datagrams.load(Ordering::SeqCst),
        before,
        "no more QUIC packets once WebSocket leads"
    );
    swallow.abort();
    let _ = client.logout().await;
}
