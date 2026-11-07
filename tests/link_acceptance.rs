//! 链路验收矩阵（实验台）：接入点故障的恢复时间与期间收发、半开连接的发现与切换、慢而活着的链路
//! 不被误判、UDP 封锁记忆跨重启。
//!
//! 每一轮都是全新的两个接入点与全新的客户端，轮与轮之间互不影响（熔断、协议记忆不串）。
//! 测量逐轮打印到测试输出（`cargo test --test link_acceptance -- --nocapture`），阈值按方案的验收标准：
//! 单接入点故障恢复 P95 ≤ 10 s；有发送时半开连接 ≤ 8 s 发现并切换。

#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flare_im_core_sdk::prelude::*;

use link_lab::*;

/// 故障转移轮数（验收要求 ≥ 20）。
const FAILOVER_RUNS: usize = 20;
/// 每轮的消息：切断前发出 `BEFORE_CUT` 条，切断后立刻再发其余的，20 轮共 100 条。
const MESSAGES_PER_RUN: usize = 5;
const BEFORE_CUT: usize = 2;
/// 半开轮数（验收要求 ≥ 10）。
const HALF_OPEN_RUNS: usize = 10;

/// 慢链路的单向时延：往返 4 s，比验活窗口（缺省 5 s）短。
const SLOW_ONE_WAY: Duration = Duration::from_millis(2_000);

const RECOVERY_P95_LIMIT: Duration = Duration::from_secs(10);
const HALF_OPEN_LIMIT: Duration = Duration::from_secs(8);

fn runs(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// 最近秩百分位：排序后第 ⌈p·n⌉ 个。
fn percentile(samples: &[Duration], p: f64) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort();
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn ms(duration: Duration) -> String {
    format!("{:.1}ms", duration.as_secs_f64() * 1_000.0)
}

fn summary(label: &str, samples: &[Duration]) {
    let mut sorted = samples.to_vec();
    sorted.sort();
    println!(
        "{label}: n={} min={} p50={} p95={} max={} all=[{}]",
        samples.len(),
        ms(sorted[0]),
        ms(percentile(samples, 0.50)),
        ms(percentile(samples, 0.95)),
        ms(*sorted.last().unwrap()),
        samples
            .iter()
            .map(|s| ms(*s))
            .collect::<Vec<_>>()
            .join(", ")
    );
}

struct Pair {
    a: AccessPoint,
    b: AccessPoint,
    ledger: Arc<Mutex<Ledger>>,
    root: std::path::PathBuf,
}

impl Pair {
    async fn start(case: &str) -> Self {
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let a = AccessPoint::start("development", ledger.clone(), ServerOptions::default()).await;
        let b = AccessPoint::start("ap-b", ledger.clone(), ServerOptions::default()).await;
        Self {
            a,
            b,
            ledger,
            root: data_root("link-acceptance", case),
        }
    }

    fn access(&self) -> AccessConfig {
        development_access(&[(&self.a.id, self.a.ws_url()), (&self.b.id, self.b.ws_url())])
    }

    fn split(&self, current: &str) -> (&AccessPoint, &AccessPoint) {
        if current == self.a.id {
            (&self.a, &self.b)
        } else {
            (&self.b, &self.a)
        }
    }
}

/// 两个接入点指向同一 Cell，切断当前接入点（现有连接断开、新连接被拒，等同它的 Edge 被杀）：
/// 从切断到新接入点 CONNECT_ACK 的恢复时间跑 20 轮取 P50/P95，P95 ≤ 10 s。
/// 每轮切断前后连续发 5 条（共 100 条，背靠背、不留间隔）：零失败、零重复（按 client_msg_id 恰好一次）、
/// 会话内有序，切换不重登。乱序不中断测量：逐轮记下，全部跑完再判。
/// `FLARE_LINK_SEND_GAP_MS` 可在两条之间留间隔（诊断用，验收按默认 0 判）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_an_access_point_recovers_within_ten_seconds_at_p95() {
    let rounds = runs("FLARE_LINK_FAILOVER_RUNS", FAILOVER_RUNS).max(1);
    let mut recoveries = Vec::with_capacity(rounds);
    let mut delivered = Vec::with_capacity(rounds);
    let gap = Duration::from_millis(runs("FLARE_LINK_SEND_GAP_MS", 0) as u64);
    let mut total_sent = 0;
    let mut absorbed_resends = 0;
    let mut out_of_order = Vec::new();
    for run in 0..rounds {
        let pair = Pair::start(&format!("failover-{run}")).await;
        // 在途窗口用默认值（> 1）：写帧串行、等回执并行，会话内照样有序。
        let client = lab_client(pair.access(), &pair.root, |_| {}).await;
        let (observed, _subscriptions) = Observed::record(&client);
        connect(&client).await;
        let online = wait_connected(&client, Duration::from_secs(15)).await;
        let generation = client.session_generation().await;
        let (current, other) = pair.split(online.access_point_id.as_deref().unwrap());

        let mut sent = Vec::with_capacity(MESSAGES_PER_RUN);
        for index in 0..BEFORE_CUT {
            sent.push(send_text(&client, &format!("run {run} message {index}")).await);
            tokio::time::sleep(gap).await;
        }
        current.relay.take_down().await;
        let cut_at = Instant::now();
        for index in BEFORE_CUT..MESSAGES_PER_RUN {
            sent.push(send_text(&client, &format!("run {run} message {index}")).await);
            tokio::time::sleep(gap).await;
        }

        wait_until(
            "switched to the other access point",
            Duration::from_secs(30),
            || async { !observed.switched.lock().unwrap().is_empty() },
        )
        .await;
        let switched_at = observed.switched.lock().unwrap()[0].0;
        let recovery = switched_at.duration_since(cut_at);
        wait_until(
            "every message accepted",
            Duration::from_secs(30),
            || async {
                sent.iter()
                    .all(|id| observed.accepted_distinct().contains(id))
            },
        )
        .await;
        let all_accepted = cut_at.elapsed();

        assert!(
            observed.failed.lock().unwrap().is_empty(),
            "run {run}: no message may fail: {:?}",
            observed.failed.lock().unwrap()
        );
        {
            let ledger = pair.ledger.lock().unwrap();
            assert_eq!(ledger.seq_of.len(), sent.len(), "run {run}: exactly once");
            if ledger.first_arrivals != sent {
                let position = |id: &String| sent.iter().position(|sent| sent == id).unwrap();
                let order: Vec<usize> = ledger.first_arrivals.iter().map(position).collect();
                println!("failover run {run:2}: OUT OF ORDER, server saw messages {order:?}");
                out_of_order.push((run, order));
            }
            absorbed_resends += ledger.received_total - ledger.seq_of.len();
        }
        let switched = observed.switched.lock().unwrap().clone();
        assert_eq!(switched.len(), 1, "run {run}: {switched:?}");
        assert_eq!(switched[0].1.from.ap, current.id);
        assert_eq!(switched[0].1.to.ap, other.id);
        assert_eq!(switched[0].1.reason, SwitchReason::Failure);
        assert_eq!(
            client.session_generation().await,
            generation,
            "run {run}: a switch is not a relogin"
        );
        println!(
            "failover run {run:2}: {} -> {}: CONNECT_ACK after {}, all {} messages accepted after {}",
            current.id,
            other.id,
            ms(recovery),
            sent.len(),
            ms(all_accepted)
        );
        recoveries.push(recovery);
        delivered.push(all_accepted);
        total_sent += sent.len();
        let _ = client.logout().await;
        let _ = tokio::fs::remove_dir_all(&pair.root).await;
    }
    summary("failover: cut -> new CONNECT_ACK", &recoveries);
    summary("failover: cut -> every message accepted", &delivered);
    println!(
        "failover: {total_sent} messages in {rounds} runs (gap {gap:?}), 0 failed, 0 duplicated, \
         {} runs out of order; {absorbed_resends} resends absorbed by the idempotent ledger",
        out_of_order.len()
    );
    let p95 = percentile(&recoveries, 0.95);
    assert!(
        p95 <= RECOVERY_P95_LIMIT,
        "recovery P95 {} exceeds {}",
        ms(p95),
        ms(RECOVERY_P95_LIMIT)
    );
    assert!(
        out_of_order.is_empty(),
        "messages must reach the server in sending order: {out_of_order:?}"
    );
}

/// 半开连接（两端套接字都在、字节再也不通）：上线后冻结当前接入点，立刻发一条。
/// 从发出到在另一个接入点 CONNECT_ACK（发现并切换）跑 10 轮，每轮 ≤ 8 s；消息以原 client_msg_id
/// 在新接入点送达、恰好一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_half_open_link_is_found_and_switched_within_eight_seconds() {
    let rounds = runs("FLARE_LINK_HALF_OPEN_RUNS", HALF_OPEN_RUNS).max(1);
    let mut found = Vec::with_capacity(rounds);
    let mut delivered = Vec::with_capacity(rounds);
    for run in 0..rounds {
        let pair = Pair::start(&format!("half-open-{run}")).await;
        let client = lab_client(pair.access(), &pair.root, |_| {}).await;
        let (observed, _subscriptions) = Observed::record(&client);
        connect(&client).await;
        let online = wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(30)).await;
        let (current, other) = pair.split(online.access_point_id.as_deref().unwrap());

        current.relay.freeze();
        let sent_at = Instant::now();
        let client_msg_id = send_text(&client, &format!("run {run} into the void")).await;
        wait_until(
            "switched away from the half-open link",
            Duration::from_secs(30),
            || async { !observed.switched.lock().unwrap().is_empty() },
        )
        .await;
        let switch = observed.switched.lock().unwrap()[0].clone();
        let switched = switch.0.duration_since(sent_at);
        wait_until(
            "accepted on the other access point",
            Duration::from_secs(30),
            || async { observed.accepted_distinct().contains(&client_msg_id) },
        )
        .await;
        let accepted = sent_at.elapsed();

        assert_eq!(switch.1.from.ap, current.id);
        assert_eq!(switch.1.to.ap, other.id);
        assert_eq!(switch.1.reason, SwitchReason::Failure);
        assert_eq!(pair.ledger.lock().unwrap().seq_of.len(), 1, "exactly once");
        assert!(observed.failed.lock().unwrap().is_empty());
        println!(
            "half-open run {run:2}: found and switched {} -> {} after {}, message accepted after {}",
            current.id,
            other.id,
            ms(switched),
            ms(accepted)
        );
        found.push(switched);
        delivered.push(accepted);
        let _ = client.logout().await;
        let _ = tokio::fs::remove_dir_all(&pair.root).await;
    }
    summary("half-open: send -> CONNECT_ACK elsewhere", &found);
    summary("half-open: send -> message accepted", &delivered);
    let worst = *found.iter().max().unwrap();
    assert!(
        worst <= HALF_OPEN_LIMIT,
        "half-open found and switched after {} (> {})",
        ms(worst),
        ms(HALF_OPEN_LIMIT)
    );
}

/// 慢而活着的链路（往返 4 s，短于验活窗口 5 s）：发送停滞 1.5 s 触发的验活在窗口内收到 PONG，
/// 连接不被拆、不切换线路、不重连，消息在原接入点送达一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_but_live_link_is_not_torn_down() {
    let pair = Pair::start("slow-link").await;
    let client = lab_client(pair.access(), &pair.root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    let online = wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(30)).await;
    let (current, other) = pair.split(online.access_point_id.as_deref().unwrap());
    let admitted = current
        .gate
        .admitted
        .load(std::sync::atomic::Ordering::SeqCst);
    let connections = current.relay.accepted();

    current.relay.set_latency(SLOW_ONE_WAY);
    let sent_at = Instant::now();
    let client_msg_id = send_text(&client, "slow but alive").await;
    wait_until(
        "accepted over the slow link",
        Duration::from_secs(30),
        || async { observed.accepted_distinct().contains(&client_msg_id) },
    )
    .await;
    let accepted = sent_at.elapsed();
    // 看过停滞（1.5 s）+ 验活窗口（5 s）再留余量：期间不得判死。
    tokio::time::sleep_until((sent_at + Duration::from_secs(9)).into()).await;

    println!(
        "slow link: one-way {} (round trip {}), message accepted after {}, still on {} after {}",
        ms(SLOW_ONE_WAY),
        ms(SLOW_ONE_WAY * 2),
        ms(accepted),
        current.id,
        ms(sent_at.elapsed())
    );
    assert!(
        accepted >= SLOW_ONE_WAY * 2 - Duration::from_millis(100),
        "the round trip really was slow: {}",
        ms(accepted)
    );
    assert!(
        observed.switched.lock().unwrap().is_empty(),
        "a live link is not switched away from"
    );
    assert_eq!(current.relay.accepted(), connections, "no reconnect");
    assert_eq!(
        current
            .gate
            .admitted
            .load(std::sync::atomic::Ordering::SeqCst),
        admitted
    );
    assert_eq!(other.relay.accepted(), 0);
    let status = client.connection_status().await;
    assert_eq!(status.phase, ConnectionPhase::Online);
    assert_eq!(status.access_point_id.as_deref(), Some(current.id.as_str()));
    assert_eq!(pair.ledger.lock().unwrap().received_total, 1, "sent once");
    assert!(observed.failed.lock().unwrap().is_empty());
    let _ = client.logout().await;
    let _ = tokio::fs::remove_dir_all(&pair.root).await;
}

/// UDP 黑洞（QUIC 包被静默丢弃）：同一网络上第一次 QUIC 打头、交错后 WebSocket 胜出；胜者记在
/// `{data_root}/access/access.db`。用同一个数据目录重建客户端（等同 App 重启）：第一次建连就由
/// WebSocket 打头、赶在交错之前握手成功，一个 QUIC 包也不发；换到另一个网络则重新评估（QUIC 再打头）。
#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_websocket_winner_survives_a_restart_on_the_same_network() {
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    let root = data_root("link-acceptance", "udp-restart");
    let _ = tokio::fs::remove_dir_all(&root).await;

    // 一次「启动」：新客户端、同一个数据目录、报告所在网络，登录（持久化的接入运行时）。
    let launch = |fingerprint: &'static str| {
        let access = access.clone();
        let root = root.clone();
        async move {
            let client = IMClient::new();
            client
                .init(
                    None,
                    Some(SdkConfigOverlay {
                        data_url: Some(root.display().to_string()),
                        access: Some(access),
                        ..Default::default()
                    }),
                )
                .await
                .expect("init");
            client
                .notify_network_change(NetworkChangeEvent {
                    available: true,
                    interface: Some("wifi".into()),
                    fingerprint_hint: Some(fingerprint.into()),
                    ..Default::default()
                })
                .await
                .expect("report the network");
            client
                .login(
                    USER_ID,
                    Some("token"),
                    LoginDbKind::IndexedDb(in_memory_im_provider()),
                    |_, _| {},
                )
                .await
                .expect("login");
            wait_connected(&client, Duration::from_secs(15)).await;
            client
        }
    };
    // 被取消的 QUIC 尝试还会补发一两个包（重传、关闭）：等计数稳定。
    let settle = || async {
        let mut before = datagrams.load(Ordering::SeqCst);
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(1_000)).await;
            let now = datagrams.load(Ordering::SeqCst);
            if now == before {
                break;
            }
            before = now;
        }
        before
    };

    let first = launch("lab-home").await;
    assert_eq!(
        first.connection_status().await.protocol.as_deref(),
        Some("ws"),
        "WebSocket wins over the silent QUIC"
    );
    let led_with_quic = settle().await;
    assert!(
        led_with_quic > 0,
        "QUIC led the first race on a new network"
    );
    first.logout().await.expect("logout");
    drop(first);
    assert!(
        root.join("access").join("access.db").exists(),
        "the health book lives in {{data_root}}/access/access.db"
    );

    let restarted = launch("lab-home").await;
    assert_eq!(
        restarted.connection_status().await.protocol.as_deref(),
        Some("ws")
    );
    let points = restarted.list_access_points().await.unwrap();
    assert_eq!(
        points[0].protocol_winner.as_deref(),
        Some("ws"),
        "{points:?}"
    );
    a.relay.cut().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    wait_connected(&restarted, Duration::from_secs(15)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_restart = datagrams.load(Ordering::SeqCst);
    println!(
        "udp blackhole: {led_with_quic} QUIC datagrams on the first launch, {} after the restart \
         (connect + reconnect on the same network)",
        after_restart - led_with_quic
    );
    assert_eq!(
        after_restart, led_with_quic,
        "after a restart on the same network WebSocket leads: no QUIC packet at all"
    );
    restarted.logout().await.expect("logout");
    drop(restarted);

    // 另一个网络没有记录：重新评估，QUIC 再次打头。
    let elsewhere = launch("lab-office").await;
    assert_eq!(
        elsewhere.connection_status().await.protocol.as_deref(),
        Some("ws")
    );
    let other_network = settle().await;
    println!(
        "udp blackhole: {} QUIC datagrams on a different network",
        other_network - after_restart
    );
    assert!(
        other_network > after_restart,
        "a different network is evaluated afresh: QUIC leads again"
    );
    elsewhere.logout().await.expect("logout");
    swallow.abort();
    let _ = tokio::fs::remove_dir_all(&root).await;
}
